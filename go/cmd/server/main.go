package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode/utf8"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
	"bbs/internal/store"
)

const maxTextLength = 500

type server struct {
	name      string
	store     store.Store
	publisher *zmq.Socket
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

// handleLogin registra o login do bot, recusando nomes fora do padrão.
func (s *server) handleLogin(ctx context.Context, request proto.Message) (proto.Data, error) {
	user := request.Data.User
	if !proto.ValidName(user) {
		return errorData("invalid_name", "usuário deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	if err := s.store.SaveLogin(ctx, user, request.Timestamp); err != nil {
		return proto.Data{}, err
	}
	return proto.Data{Status: "ok"}, nil
}

// handleChannel cria um canal novo, recusando nomes inválidos e repetidos.
func (s *server) handleChannel(ctx context.Context, request proto.Message) (proto.Data, error) {
	name := request.Data.Channel
	if !proto.ValidName(name) {
		return errorData("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	created, err := s.store.CreateChannel(ctx, name, request.Sender, request.Timestamp)
	if err != nil {
		return proto.Data{}, err
	}
	if !created {
		return errorData("already_exists", fmt.Sprintf("o canal %s já existe", name)), nil
	}
	return proto.Data{Status: "ok"}, nil
}

// handleList devolve a lista com o nome de todos os canais.
func (s *server) handleList(ctx context.Context, request proto.Message) (proto.Data, error) {
	channels, err := s.store.ListChannels(ctx)
	if err != nil {
		return proto.Data{}, err
	}
	return proto.Data{Status: "ok", Channels: channels}, nil
}

// handlePublish grava a publicação, publica no tópico do canal e devolve o status.
func (s *server) handlePublish(ctx context.Context, request proto.Message) (proto.Data, error) {
	channel := request.Data.Channel
	text := request.Data.Text
	if !proto.ValidName(channel) {
		return errorData("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	if strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > maxTextLength {
		return errorData("invalid_text", fmt.Sprintf("o texto deve ter de 1 a %d caracteres", maxTextLength)), nil
	}

	message := proto.New("publish", s.name, proto.Data{Channel: channel, User: request.Sender, Text: text})
	raw, err := proto.Encode(message)
	if err != nil {
		return proto.Data{}, err
	}
	if err := s.store.SavePublication(ctx, channel, request.Sender, text, request.Timestamp, message.Timestamp); err != nil {
		return proto.Data{}, err
	}
	if _, err := s.publisher.SendMessage(channel, raw); err != nil {
		return proto.Data{}, err
	}
	proto.Show(s.name, "PUB ", message)
	return proto.Data{Status: "ok"}, nil
}

// process trata uma requisição e devolve a resposta já serializada.
func (s *server) process(ctx context.Context, raw []byte) []byte {
	request, err := proto.Decode(raw)
	if err != nil {
		return s.reply("error", errorData("bad_request", err.Error()))
	}
	proto.Show(s.name, "RECV", request)

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
	message := proto.New(service, s.name, data)
	proto.Show(s.name, "SEND", message)
	raw, err := proto.Encode(message)
	if err != nil {
		log.Fatal(err)
	}
	return raw
}

// main conecta no banco, no broker e no proxy e responde requisições em loop.
func main() {
	ctx := context.Background()
	name := os.Getenv("SERVER_NAME")
	uri := os.Getenv("MONGO_URI")
	if name == "" || uri == "" {
		log.Fatal("SERVER_NAME e MONGO_URI são obrigatórios")
	}
	broker := env("BROKER_ADDR", "tcp://broker:5556")
	proxy := env("PROXY_ADDR", "tcp://proxy:5557")

	st, err := store.NewMongo(ctx, uri, "bbs")
	if err != nil {
		log.Fatal(err)
	}

	publisher, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatal(err)
	}
	if err := publisher.Connect(proxy); err != nil {
		log.Fatal(err)
	}
	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	if err := socket.Connect(broker); err != nil {
		log.Fatal(err)
	}
	s := &server{name: name, store: st, publisher: publisher}
	fmt.Printf("[%s] servidor pronto, broker em %s, proxy em %s\n", name, broker, proxy)

	for {
		raw, err := socket.RecvBytes(0)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := socket.SendBytes(s.process(ctx, raw), 0); err != nil {
			log.Fatal(err)
		}
	}
}
