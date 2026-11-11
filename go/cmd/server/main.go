package main

import (
	"context"
	"fmt"
	"log"
	"os"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
	"bbs/internal/store"
)

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
func handleLogin(ctx context.Context, st store.Store, request proto.Message) (proto.Data, error) {
	user := request.Data.User
	if !proto.ValidName(user) {
		return errorData("invalid_name", "usuário deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	if err := st.SaveLogin(ctx, user, request.Timestamp); err != nil {
		return proto.Data{}, err
	}
	return proto.Data{Status: "ok"}, nil
}

// handleChannel cria um canal novo, recusando nomes inválidos e repetidos.
func handleChannel(ctx context.Context, st store.Store, request proto.Message) (proto.Data, error) {
	name := request.Data.Channel
	if !proto.ValidName(name) {
		return errorData("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _"), nil
	}
	created, err := st.CreateChannel(ctx, name, request.Sender, request.Timestamp)
	if err != nil {
		return proto.Data{}, err
	}
	if !created {
		return errorData("already_exists", fmt.Sprintf("o canal %s já existe", name)), nil
	}
	return proto.Data{Status: "ok"}, nil
}

// handleList devolve a lista com o nome de todos os canais.
func handleList(ctx context.Context, st store.Store, request proto.Message) (proto.Data, error) {
	channels, err := st.ListChannels(ctx)
	if err != nil {
		return proto.Data{}, err
	}
	return proto.Data{Status: "ok", Channels: channels}, nil
}

// process trata uma requisição e devolve a resposta já serializada.
func process(ctx context.Context, st store.Store, name string, raw []byte) []byte {
	request, err := proto.Decode(raw)
	if err != nil {
		return send(name, "error", errorData("bad_request", err.Error()))
	}
	proto.Show(name, "RECV", request)

	var data proto.Data
	switch request.Service {
	case "login":
		data, err = handleLogin(ctx, st, request)
	case "channel":
		data, err = handleChannel(ctx, st, request)
	case "list":
		data, err = handleList(ctx, st, request)
	default:
		data = errorData("unknown_service", fmt.Sprintf("serviço %s não existe", request.Service))
	}
	if err != nil {
		log.Printf("erro no banco: %v", err)
		data = errorData("internal", "erro ao acessar o banco")
	}
	return send(name, request.Service, data)
}

// send monta a resposta, mostra na tela e devolve os bytes para enviar.
func send(name, service string, data proto.Data) []byte {
	reply := proto.New(service, name, data)
	proto.Show(name, "SEND", reply)
	raw, err := proto.Encode(reply)
	if err != nil {
		log.Fatal(err)
	}
	return raw
}

// main conecta no banco e no broker e responde requisições em loop.
func main() {
	ctx := context.Background()
	name := os.Getenv("SERVER_NAME")
	uri := os.Getenv("MONGO_URI")
	if name == "" || uri == "" {
		log.Fatal("SERVER_NAME e MONGO_URI são obrigatórios")
	}
	broker := env("BROKER_ADDR", "tcp://broker:5556")

	st, err := store.NewMongo(ctx, uri, "bbs")
	if err != nil {
		log.Fatal(err)
	}

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	if err := socket.Connect(broker); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[%s] servidor pronto, broker em %s\n", name, broker)

	for {
		raw, err := socket.RecvBytes(0)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := socket.SendBytes(process(ctx, st, name, raw), 0); err != nil {
			log.Fatal(err)
		}
	}
}
