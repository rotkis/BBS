package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
	"bbs/internal/req"
	"bbs/internal/store"
)

const (
	electionTimeout = time.Second
	timeTimeout     = 500 * time.Millisecond
	clockTimeout    = 2 * time.Second
	syncEvery       = 15
	serversTopic    = "servers"
	downFor         = 10 * time.Second
	peerWorkers     = 8
)

type cluster struct {
	process      *proto.Process
	reference    *req.Requester
	publisher    *req.Publisher
	store        store.Store
	peerTemplate string
	peerBind     string
	subAddr      string

	mu          sync.Mutex
	rank        int
	coordinator string
	servers     []proto.ServerInfo
	serversAt   time.Time
	downUntil   map[string]time.Time

	opMu       sync.Mutex
	electionMu sync.Mutex
	syncMu     sync.Mutex
	nextSync   int64
}

// start liga a goroutine que atende os outros servidores e a que escuta os anúncios de coordenador.
func (c *cluster) start() {
	go c.servePeers()
	go c.listenAnnouncements()
}

// peerAddress monta o endereço do socket de outro servidor a partir do nome dele.
func (c *cluster) peerAddress(name string) string {
	return fmt.Sprintf(c.peerTemplate, name)
}

// callPeer faz uma requisição a outro servidor.
func (c *cluster) callPeer(name, service string, data proto.Data, timeout time.Duration) (proto.Message, bool) {
	return req.Once(c.process, c.peerAddress(name), timeout, service, data)
}

// getRank devolve o rank deste servidor.
func (c *cluster) getRank() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rank
}

// getCoordinator devolve o nome do coordenador conhecido, ou vazio se não houver.
func (c *cluster) getCoordinator() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.coordinator
}

// setCoordinator guarda o nome do coordenador.
func (c *cluster) setCoordinator(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.coordinator = name
}

// fetchServers pede à referência a lista de servidores ativos e, se ela não responder, usa a última que tinha.
func (c *cluster) fetchServers() []proto.ServerInfo {
	reply, ok := c.reference.Call("list", proto.Data{})
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok && reply.Data.Status == "ok" {
		c.servers = reply.Data.Servers
		c.serversAt = time.Now()
	}
	return append([]proto.ServerInfo(nil), c.servers...)
}

// elect faz a eleição do valentão: pede eleição aos servidores de rank maior e, se nenhum responder, vira coordenador.
func (c *cluster) elect() {
	rank := c.getRank()
	if rank == 0 || !c.electionMu.TryLock() {
		return
	}
	defer c.electionMu.Unlock()

	answered := false
	for _, server := range c.fetchServers() {
		if server.Rank <= rank {
			continue
		}
		reply, ok := c.callPeer(server.Name, "election", proto.Data{}, electionTimeout)
		if ok && reply.Data.Status == "ok" {
			answered = true
		}
	}
	if answered {
		c.setCoordinator("")
		fmt.Printf("[%s] um servidor de rank maior respondeu, ele assume a eleição\n", c.process.Name)
		return
	}
	c.becomeCoordinator()
}

// becomeCoordinator busca com os outros servidores o que falta no próprio log, assume como coordenador, avisa os outros no tópico servers e acerta o relógio de todos.
func (c *cluster) becomeCoordinator() {
	for _, server := range c.fetchServers() {
		if server.Name != c.process.Name && !c.isDown(server.Name) {
			c.catchUp(context.Background(), server.Name)
		}
	}
	c.setCoordinator(c.process.Name)
	rank := c.getRank()
	fmt.Printf("[%s] eleito coordenador (rank %d)\n", c.process.Name, rank)
	message := c.process.New("coordinator", proto.Data{Coordinator: c.process.Name, Rank: rank})
	if err := c.publisher.Publish(serversTopic, message); err != nil {
		fmt.Printf("[%s] erro ao anunciar: %v\n", c.process.Name, err)
	}
	c.berkeleyRound()
}

// isDown diz se o servidor foi marcado como fora do ar há pouco tempo.
func (c *cluster) isDown(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Before(c.downUntil[name])
}

// markDown marca o servidor como fora do ar para as próximas rodadas de Berkeley pularem ele por alguns segundos.
func (c *cluster) markDown(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.downUntil[name] = time.Now().Add(downFor)
}

// berkeleyRound pergunta a hora dos outros servidores, calcula a média com a sua e ajusta o próprio relógio para ela.
func (c *cluster) berkeleyRound() {
	diffs := []int64{0}
	for _, server := range c.fetchServers() {
		if server.Name == c.process.Name || c.isDown(server.Name) {
			continue
		}
		t0 := c.process.Now()
		reply, ok := c.callPeer(server.Name, "time", proto.Data{}, timeTimeout)
		t1 := c.process.Now()
		if !ok || reply.Data.Status != "ok" {
			c.markDown(server.Name)
			continue
		}
		diffs = append(diffs, reply.Data.Time+(t1-t0)/2-t1)
	}
	var sum int64
	for _, diff := range diffs {
		sum += diff
	}
	average := sum / int64(len(diffs))
	c.process.Shift(average)
	fmt.Printf("[%s] Berkeley: média de %d relógios, ajuste de %+dms\n", c.process.Name, len(diffs), average)
}

// syncClock pede a hora correta ao coordenador; se ele não responder, começa uma eleição.
func (c *cluster) syncClock() {
	if !c.syncMu.TryLock() {
		return
	}
	defer c.syncMu.Unlock()

	coordinator := c.getCoordinator()
	if coordinator == "" {
		c.elect()
		return
	}
	if coordinator == c.process.Name {
		c.berkeleyRound()
		return
	}
	t0 := c.process.RawNow()
	reply, ok := c.callPeer(coordinator, "clock", proto.Data{}, clockTimeout)
	t1 := c.process.RawNow()
	if !ok || reply.Data.Status != "ok" {
		fmt.Printf("[%s] coordenador %s indisponível, pedindo eleição\n", c.process.Name, coordinator)
		c.setCoordinator("")
		c.elect()
		return
	}
	offset, rtt := c.process.Adjust(reply.Data.Time, reply.Data.Elapsed, t0, t1)
	fmt.Printf("[%s] relógio sincronizado com %s: offset=%+dms rtt=%dms\n", c.process.Name, coordinator, offset, rtt)
}

// afterRequest pede a hora ao coordenador a cada 15 mensagens trocadas.
func (c *cluster) afterRequest() {
	if c.process.Exchanged() >= c.nextSync {
		c.syncClock()
		c.nextSync = c.process.Exchanged() + syncEvery
	}
}

// handleElection responde OK a quem pediu eleição e começa a própria eleição em outra goroutine.
func (c *cluster) handleElection() proto.Data {
	if c.getRank() == 0 {
		return errorData("not_ready", "servidor ainda não tem rank")
	}
	go c.elect()
	return proto.Data{Status: "ok"}
}

// handleClock, se for o coordenador, roda uma rodada de Berkeley e devolve a hora correta.
func (c *cluster) handleClock() proto.Data {
	if c.getCoordinator() != c.process.Name {
		return errorData("not_coordinator", "este servidor não é o coordenador")
	}
	started := time.Now()
	c.berkeleyRound()
	return proto.Data{Status: "ok", Time: c.process.Now(), Elapsed: time.Since(started).Milliseconds()}
}

// handlePeer trata uma requisição de outro servidor e devolve a resposta já serializada.
func (c *cluster) handlePeer(raw []byte) []byte {
	var service string
	var data proto.Data
	request, err := proto.Decode(raw)
	if err != nil {
		service, data = "error", errorData("bad_request", err.Error())
	} else {
		c.process.Receive(request)
		proto.Show(c.process.Name, "RECV", request)
		service = request.Service
		switch service {
		case "election":
			data = c.handleElection()
		case "clock":
			data = c.handleClock()
		case "time":
			data = proto.Data{Status: "ok", Time: c.process.Now()}
		case "forward":
			data = c.handleForward(context.Background(), request)
		case "replicate":
			data = c.handleReplicate(context.Background(), request)
		case "catchup":
			data = c.handleCatchup(context.Background(), request)
		default:
			data = errorData("unknown_service", fmt.Sprintf("serviço %s não existe", service))
		}
	}
	reply := c.process.New(service, data)
	proto.Show(c.process.Name, "SEND", reply)
	encoded, err := proto.Encode(reply)
	if err != nil {
		panic(err)
	}
	return encoded
}

// servePeers recebe as requisições dos outros servidores em um ROUTER e reparte entre as goroutines de trabalho, para um pedido demorado não travar os demais.
func (c *cluster) servePeers() {
	frontend, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		panic(err)
	}
	if err := frontend.Bind(c.peerBind); err != nil {
		panic(err)
	}
	backend, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		panic(err)
	}
	if err := backend.Bind("inproc://peers"); err != nil {
		panic(err)
	}
	for i := 0; i < peerWorkers; i++ {
		go c.peerWorker()
	}
	panic(zmq.Proxy(frontend, backend, nil))
}

// peerWorker atende requisições de outros servidores, uma por vez, em um socket REP próprio.
func (c *cluster) peerWorker() {
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		panic(err)
	}
	if err := socket.Connect("inproc://peers"); err != nil {
		panic(err)
	}
	for {
		raw, err := socket.RecvBytes(0)
		if err != nil {
			panic(err)
		}
		if _, err := socket.SendBytes(c.handlePeer(raw), 0); err != nil {
			panic(err)
		}
	}
}

// listenAnnouncements escuta o tópico servers; quem anuncia vira o coordenador, a não ser que tenha rank menor que o deste servidor.
func (c *cluster) listenAnnouncements() {
	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		panic(err)
	}
	if err := socket.Connect(c.subAddr); err != nil {
		panic(err)
	}
	socket.SetSubscribe(serversTopic)
	for {
		frames, err := socket.RecvMessageBytes(0)
		if err != nil || len(frames) != 2 {
			continue
		}
		message, err := proto.Decode(frames[1])
		if err != nil {
			continue
		}
		c.process.Receive(message)
		proto.Show(c.process.Name, "RECV", message)
		if message.Data.Coordinator == c.process.Name {
			continue
		}
		if rank := c.getRank(); rank != 0 && message.Data.Rank < rank {
			go c.elect()
			continue
		}
		c.setCoordinator(message.Data.Coordinator)
		fmt.Printf("[%s] coordenador agora é %s\n", c.process.Name, message.Data.Coordinator)
		c.syncClock()
		c.catchUp(context.Background(), message.Data.Coordinator)
	}
}
