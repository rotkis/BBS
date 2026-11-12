package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
	"bbs/internal/req"
	"bbs/internal/store"
)

const (
	maxTextLength    = 500
	heartbeatEvery   = 10
	referenceTimeout = 2 * time.Second
	retryDelay       = 2 * time.Second
)

type server struct {
	process   *proto.Process
	store     store.Store
	publisher *req.Publisher
	cluster   *cluster
	reference *req.Requester
	rank      int
	servers   []proto.ServerInfo
	received  int
}

// env devolve a variável de ambiente ou o valor padrão quando ela não existe.
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// errorData monta o corpo de uma resposta de erro.
func errorData(code, message string) proto.Data {
	return proto.Data{Status: "error", Code: code, Message: message}
}

// register pede o rank ao serviço de referência, esperando ele ficar disponível, e já sincroniza o relógio.
func (s *server) register() {
	for {
		reply, ok := s.reference.Call("rank", proto.Data{Name: s.process.Name})
		if ok && reply.Data.Status == "ok" {
			s.rank = reply.Data.Rank
			break
		}
		time.Sleep(retryDelay)
	}
	s.cluster.mu.Lock()
	s.cluster.rank = s.rank
	s.cluster.mu.Unlock()
	fmt.Printf("[%s] registrado na referência com rank %d\n", s.process.Name, s.rank)
	s.heartbeat()
}

// heartbeat avisa a referência que o servidor está vivo, corrige o relógio com a hora devolvida e atualiza a lista de servidores.
func (s *server) heartbeat() {
	reply, ok := s.reference.Call("heartbeat", proto.Data{Name: s.process.Name})
	if !ok {
		return
	}
	if reply.Data.Status != "ok" {
		if reply.Data.Code == "unknown_server" {
			s.register()
		}
		return
	}

	reply, ok = s.reference.Call("list", proto.Data{})
	if ok && reply.Data.Status == "ok" {
		s.servers = reply.Data.Servers
		s.cluster.updateServers(s.servers)
		names := make([]string, 0, len(s.servers))
		for _, info := range s.servers {
			names = append(names, info.Name+"(rank "+strconv.Itoa(info.Rank)+")")
		}
		fmt.Printf("[%s] servidores ativos: %s\n", s.process.Name, strings.Join(names, ", "))
	}
}

// afterRequest conta a requisição de cliente, manda o heartbeat a cada 10 recebidas e acerta o relógio a cada 15 mensagens trocadas.
func (s *server) afterRequest() {
	s.received++
	if s.received%heartbeatEvery == 0 {
		s.heartbeat()
	}
	s.cluster.afterRequest()
}

// handleLogin valida o nome e manda o login para ser gravado e replicado.
func (s *server) handleLogin(ctx context.Context, request proto.Message) (proto.Data, error) {
	user := request.Data.User
	if !proto.ValidName(user) {
		return errorData("invalid_name", "usuário deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	return s.cluster.write(ctx, proto.Op{Type: "login", User: user, Timestamp: request.Timestamp}), nil
}

// handleChannel valida o nome e manda a criação do canal para ser gravada e replicada.
func (s *server) handleChannel(ctx context.Context, request proto.Message) (proto.Data, error) {
	name := request.Data.Channel
	if !proto.ValidName(name) {
		return errorData("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	op := proto.Op{Type: "channel", User: request.Sender, Channel: name, Timestamp: request.Timestamp}
	return s.cluster.write(ctx, op), nil
}

// handleList devolve a lista com o nome de todos os canais.
func (s *server) handleList(ctx context.Context, request proto.Message) (proto.Data, error) {
	channels, err := s.store.ListChannels(ctx)
	if err != nil {
		return proto.Data{}, err
	}
	return proto.Data{Status: "ok", Channels: channels}, nil
}

// handleHistory devolve o total de publicações do canal e as últimas que este servidor tem.
func (s *server) handleHistory(ctx context.Context, request proto.Message) (proto.Data, error) {
	channel := request.Data.Channel
	limit := request.Data.Limit
	if limit == 0 {
		limit = 20
	}
	if !proto.ValidName(channel) {
		return errorData("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	if limit < 1 || limit > maxHistoryLimit {
		return errorData("invalid_limit", fmt.Sprintf("limit deve ficar entre 1 e %d", maxHistoryLimit)), nil
	}
	total, entries, err := s.store.History(ctx, channel, limit)
	if err != nil {
		return proto.Data{}, err
	}
	return proto.Data{Status: "ok", Channel: channel, Total: total, Messages: entries}, nil
}

// handlePublish valida a publicação, manda gravar e replicar e, se deu certo, publica no tópico do canal.
func (s *server) handlePublish(ctx context.Context, request proto.Message) (proto.Data, error) {
	channel := request.Data.Channel
	text := request.Data.Text
	if !proto.ValidName(channel) {
		return errorData("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	if strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > maxTextLength {
		return errorData("invalid_text", fmt.Sprintf("o texto deve ter de 1 a %d caracteres", maxTextLength)), nil
	}

	op := proto.Op{Type: "publish", User: request.Sender, Channel: channel, Text: text, Timestamp: request.Timestamp}
	result := s.cluster.write(ctx, op)
	if result.Status == "ok" {
		message := s.process.New("publish", proto.Data{Channel: channel, User: request.Sender, Text: text})
		if err := s.publisher.Publish(channel, message); err != nil {
			return proto.Data{}, err
		}
	}
	return result, nil
}

// handle trata uma requisição e devolve a resposta já serializada.
func (s *server) handle(ctx context.Context, raw []byte) []byte {
	request, err := proto.Decode(raw)
	if err != nil {
		return s.reply("error", errorData("bad_request", err.Error()))
	}
	s.process.Receive(request)
	proto.Show(s.process.Name, "RECV", request)

	var data proto.Data
	switch request.Service {
	case "login":
		data, err = s.handleLogin(ctx, request)
	case "channel":
		data, err = s.handleChannel(ctx, request)
	case "list":
		data, err = s.handleList(ctx, request)
	case "publish":
		data, err = s.handlePublish(ctx, request)
	case "history":
		data, err = s.handleHistory(ctx, request)
	default:
		data = errorData("unknown_service", fmt.Sprintf("serviço %s não existe", request.Service))
	}
	if err != nil {
		log.Printf("erro ao tratar %s: %v", request.Service, err)
		data = errorData("internal", "erro ao acessar o banco ou publicar")
	}
	return s.reply(request.Service, data)
}

// reply monta a resposta, mostra na tela e devolve os bytes para enviar.
func (s *server) reply(service string, data proto.Data) []byte {
	message := s.process.New(service, data)
	proto.Show(s.process.Name, "SEND", message)
	raw, err := proto.Encode(message)
	if err != nil {
		log.Fatal(err)
	}
	return raw
}

// main registra o servidor na referência, conecta no banco, no broker e no proxy e responde requisições em loop.
func main() {
	ctx := context.Background()
	name := os.Getenv("SERVER_NAME")
	uri := os.Getenv("MONGO_URI")
	if name == "" || uri == "" {
		log.Fatal("SERVER_NAME e MONGO_URI são obrigatórios")
	}
	broker := env("BROKER_ADDR", "tcp://broker:5556")
	proxy := env("PROXY_ADDR", "tcp://proxy:5557")
	skew, err := strconv.ParseInt(env("CLOCK_SKEW_MS", "0"), 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	st, err := store.NewMongo(ctx, uri, "bbs")
	if err != nil {
		log.Fatal(err)
	}

	process := proto.NewProcess(name, skew)
	reference := req.New(process, env("REFERENCE_ADDR", "tcp://reference:5559"), referenceTimeout)

	pubSocket, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatal(err)
	}
	if err := pubSocket.Connect(proxy); err != nil {
		log.Fatal(err)
	}
	publisher := req.NewPublisher(process, pubSocket)
	c := &cluster{
		process:      process,
		reference:    reference,
		publisher:    publisher,
		store:        st,
		peerTemplate: env("PEER_ADDR", "tcp://%s:6000"),
		peerBind:     env("PEER_BIND", "tcp://*:6000"),
		subAddr:      env("SUB_ADDR", "tcp://proxy:5558"),
		nextSync:     syncEvery,
		downUntil:    map[string]time.Time{},
	}
	c.start()
	s := &server{process: process, store: st, publisher: publisher, cluster: c, reference: reference}
	s.register()
	c.elect()

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	if err := socket.Connect(broker); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[%s] servidor pronto, broker em %s, proxy em %s\n", name, broker, proxy)

	for {
		raw, err := socket.RecvBytes(0)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := socket.SendBytes(s.handle(ctx, raw), 0); err != nil {
			log.Fatal(err)
		}
		s.afterRequest()
	}
}
