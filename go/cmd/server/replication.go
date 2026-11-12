package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"bbs/internal/proto"
	"bbs/internal/store"
)

const (
	pushTimeout     = time.Second
	forwardTimeout  = 3 * time.Second
	catchupTimeout  = 3 * time.Second
	catchupBatch    = 200
	serversMaxAge   = 5 * time.Second
	maxHistoryLimit = 100
)

// updateServers guarda a lista de servidores ativos e a hora em que ela foi pega.
func (c *cluster) updateServers(servers []proto.ServerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.servers = servers
	c.serversAt = time.Now()
}

// activeServers devolve a lista de servidores ativos, pedindo uma nova à referência se a guardada tiver mais de 5 segundos.
func (c *cluster) activeServers() []proto.ServerInfo {
	c.mu.Lock()
	fresh := time.Since(c.serversAt) <= serversMaxAge
	servers := append([]proto.ServerInfo(nil), c.servers...)
	c.mu.Unlock()
	if fresh {
		return servers
	}
	return c.fetchServers()
}

// write executa uma escrita: o coordenador numera e replica, os outros servidores encaminham ao coordenador.
func (c *cluster) write(ctx context.Context, op proto.Op) proto.Data {
	if c.getCoordinator() == "" {
		c.elect()
	}
	coordinator := c.getCoordinator()
	if coordinator == c.process.Name {
		return c.execute(ctx, op)
	}
	if coordinator == "" {
		return errorData("no_coordinator", "ainda não há coordenador, tente de novo")
	}
	reply, ok := c.callPeer(coordinator, "forward", proto.Data{Ops: []proto.Op{op}}, forwardTimeout)
	if !ok || reply.Data.Code == "not_coordinator" {
		c.setCoordinator("")
		go c.elect()
		return errorData("coordinator_unavailable", "coordenador indisponível, tente de novo")
	}
	return proto.Data{Status: reply.Data.Status, Code: reply.Data.Code, Message: reply.Data.Message}
}

// execute, como coordenador, confere as regras do canal, numera e grava a operação, replica nos outros servidores e só então devolve ok.
func (c *cluster) execute(ctx context.Context, op proto.Op) proto.Data {
	c.opMu.Lock()
	defer c.opMu.Unlock()

	if op.Type == "channel" || op.Type == "publish" {
		exists, err := c.store.ChannelExists(ctx, op.Channel)
		if err != nil {
			return errorData("internal", "erro ao acessar o banco")
		}
		if op.Type == "channel" && exists {
			return errorData("already_exists", fmt.Sprintf("o canal %s já existe", op.Channel))
		}
		if op.Type == "publish" && !exists {
			return errorData("channel_not_found", fmt.Sprintf("o canal %s não existe", op.Channel))
		}
	}
	op.At = c.process.Now()
	op, err := c.store.Append(ctx, op)
	if err != nil {
		return errorData("internal", "erro ao acessar o banco")
	}
	c.push(ctx, op)
	return proto.Data{Status: "ok"}
}

// push manda a operação para todos os outros servidores ativos; quem estiver atrasado recebe também as que perdeu.
func (c *cluster) push(ctx context.Context, op proto.Op) {
	for _, server := range c.activeServers() {
		if server.Name == c.process.Name || c.isDown(server.Name) {
			continue
		}
		reply, ok := c.callPeer(server.Name, "replicate", proto.Data{Ops: []proto.Op{op}}, pushTimeout)
		if !ok {
			c.markDown(server.Name)
			continue
		}
		if reply.Data.Code == "gap" {
			missing, err := c.store.OpsAfter(ctx, reply.Data.Last, catchupBatch)
			if err == nil {
				c.callPeer(server.Name, "replicate", proto.Data{Ops: missing}, catchupTimeout)
			}
		}
	}
}

// catchUp puxa de outro servidor as operações que faltam no log deste, em lotes, até ficar em dia.
func (c *cluster) catchUp(ctx context.Context, name string) {
	for {
		reply, ok := c.callPeer(name, "catchup", proto.Data{After: c.store.LastSeq()}, catchupTimeout)
		if !ok || reply.Data.Status != "ok" {
			return
		}
		for _, op := range reply.Data.Ops {
			if _, err := c.store.ApplyReplicated(ctx, op); err != nil {
				return
			}
		}
		if len(reply.Data.Ops) < catchupBatch {
			return
		}
	}
}

// handleForward executa a escrita que outro servidor encaminhou, se este for o coordenador.
func (c *cluster) handleForward(ctx context.Context, request proto.Message) proto.Data {
	if c.getCoordinator() != c.process.Name {
		return errorData("not_coordinator", "este servidor não é o coordenador")
	}
	ops := request.Data.Ops
	if len(ops) != 1 || (ops[0].Type != "login" && ops[0].Type != "channel" && ops[0].Type != "publish") {
		return errorData("bad_request", "operação inválida")
	}
	return c.execute(ctx, ops[0])
}

// handleReplicate aplica em ordem as operações recebidas do coordenador; se faltar alguma antes, responde gap com o último número que tem.
func (c *cluster) handleReplicate(ctx context.Context, request proto.Message) proto.Data {
	ops := append([]proto.Op(nil), request.Data.Ops...)
	sort.Slice(ops, func(i, j int) bool { return ops[i].Seq < ops[j].Seq })
	for _, op := range ops {
		_, err := c.store.ApplyReplicated(ctx, op)
		if errors.Is(err, store.ErrGap) {
			return proto.Data{Status: "error", Code: "gap", Last: c.store.LastSeq()}
		}
		if err != nil {
			return errorData("internal", "erro ao acessar o banco")
		}
	}
	return proto.Data{Status: "ok", Last: c.store.LastSeq()}
}

// handleCatchup devolve um lote de operações com número maior que o pedido.
func (c *cluster) handleCatchup(ctx context.Context, request proto.Message) proto.Data {
	ops, err := c.store.OpsAfter(ctx, request.Data.After, catchupBatch)
	if err != nil {
		return errorData("internal", "erro ao acessar o banco")
	}
	return proto.Data{Status: "ok", Ops: ops}
}
