package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
)

const (
	requestTimeout = 5 * time.Second
	retryDelay     = 2 * time.Second
	listInterval   = 5 * time.Second
	minChannels    = 5
)

var channelWords = []string{
	"general", "random", "music", "games", "news",
	"tech", "sports", "movies", "books", "travel",
}

type requester struct {
	name    string
	address string
	socket  *zmq.Socket
}

// connect abre um socket REQ novo, na partida e depois de cada timeout.
func (r *requester) connect() {
	if r.socket != nil {
		r.socket.Close()
	}
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		log.Fatal(err)
	}
	socket.SetLinger(0)
	socket.SetRcvtimeo(requestTimeout)
	if err := socket.Connect(r.address); err != nil {
		log.Fatal(err)
	}
	r.socket = socket
}

// call envia uma requisição e espera a resposta; devolve false se der timeout ou vier algo inválido.
func (r *requester) call(service string, data proto.Data) (proto.Message, bool) {
	request := proto.New(service, r.name, data)
	proto.Show(r.name, "SEND", request)
	raw, err := proto.Encode(request)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := r.socket.SendBytes(raw, 0); err != nil {
		log.Fatal(err)
	}

	rawReply, err := r.socket.RecvBytes(0)
	if err != nil {
		fmt.Printf("[%s] sem resposta para %s, reconectando\n", r.name, service)
		r.connect()
		return proto.Message{}, false
	}
	reply, err := proto.Decode(rawReply)
	if err != nil {
		fmt.Printf("[%s] resposta inválida: %v\n", r.name, err)
		return proto.Message{}, false
	}
	proto.Show(r.name, "RECV", reply)
	return reply, true
}

// login repete o login até o servidor aceitar.
func (r *requester) login() {
	for {
		reply, ok := r.call("login", proto.Data{User: r.name})
		if ok && reply.Data.Status == "ok" {
			return
		}
		time.Sleep(retryDelay)
	}
}

// listChannels pede a lista de canais e tenta de novo até receber.
func (r *requester) listChannels() []string {
	for {
		reply, ok := r.call("list", proto.Data{})
		if ok && reply.Data.Status == "ok" {
			return reply.Data.Channels
		}
		time.Sleep(retryDelay)
	}
}

// pickChannelName escolhe um nome que ainda não esteja na lista, ou sorteia um com sufixo se todos já existirem.
func pickChannelName(existing []string) string {
	taken := make(map[string]bool, len(existing))
	for _, name := range existing {
		taken[name] = true
	}
	var free []string
	for _, word := range channelWords {
		if !taken[word] {
			free = append(free, word)
		}
	}
	if len(free) > 0 {
		return free[rand.Intn(len(free))]
	}
	return fmt.Sprintf("%s-%d", channelWords[rand.Intn(len(channelWords))], rand.Intn(999)+1)
}

// main faz login, cria canais até ter o mínimo e depois lista os canais de tempos em tempos.
func main() {
	name := os.Getenv("BOT_NAME")
	if name == "" {
		log.Fatal("BOT_NAME é obrigatório")
	}
	address := os.Getenv("BROKER_ADDR")
	if address == "" {
		address = "tcp://broker:5555"
	}

	r := &requester{name: name, address: address}
	r.connect()
	r.login()

	channels := r.listChannels()
	if len(channels) < minChannels {
		r.call("channel", proto.Data{Channel: pickChannelName(channels)})
	}

	for {
		time.Sleep(listInterval)
		r.listChannels()
	}
}
