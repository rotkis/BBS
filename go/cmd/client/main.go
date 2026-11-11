package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
	"bbs/internal/req"
)

const (
	requestTimeout    = 5 * time.Second
	retryDelay        = 2 * time.Second
	messageInterval   = time.Second
	minChannels       = 5
	minSubscriptions  = 3
	messagesPerRound  = 10
	subscriberTimeout = 100 * time.Millisecond
)

var channelWords = []string{
	"general", "random", "music", "games", "news",
	"tech", "sports", "movies", "books", "travel",
}

var messageTexts = []string{
	"bom dia", "alguém online?", "vamos testar isso", "mensagem aleatória",
	"olá, mundo", "tudo certo por aqui", "ping", "qualquer novidade?",
	"teste de publicação", "até mais",
}

type subscriber struct {
	process *proto.Process
	address string
	topics  chan string
}

// login repete o login até o servidor aceitar.
func login(r *req.Requester, name string) {
	for {
		reply, ok := r.Call("login", proto.Data{User: name})
		if ok && reply.Data.Status == "ok" {
			return
		}
		time.Sleep(retryDelay)
	}
}

// listChannels pede a lista de canais e tenta de novo até receber.
func listChannels(r *req.Requester) []string {
	for {
		reply, ok := r.Call("list", proto.Data{})
		if ok && reply.Data.Status == "ok" {
			return reply.Data.Channels
		}
		time.Sleep(retryDelay)
	}
}

// publishRound publica 10 mensagens aleatórias no canal, uma por segundo.
func publishRound(r *req.Requester, channel string) {
	for number := 1; number <= messagesPerRound; number++ {
		text := fmt.Sprintf("%s [%d/%d]", messageTexts[rand.Intn(len(messageTexts))], number, messagesPerRound)
		r.Call("publish", proto.Data{Channel: channel, Text: text})
		time.Sleep(messageInterval)
	}
}

// run assina os canais pedidos e imprime tudo o que chega nos canais assinados; o socket SUB só é usado aqui.
func (s *subscriber) run() {
	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		log.Fatal(err)
	}
	socket.SetRcvtimeo(subscriberTimeout)
	if err := socket.Connect(s.address); err != nil {
		log.Fatal(err)
	}

	subscribed := map[string]bool{}
	for {
		for pending := true; pending; {
			select {
			case channel := <-s.topics:
				socket.SetSubscribe(channel)
				subscribed[channel] = true
			default:
				pending = false
			}
		}

		frames, err := socket.RecvMessageBytes(0)
		if err != nil || len(frames) != 2 {
			continue
		}
		if !subscribed[string(frames[0])] {
			continue
		}
		message, err := proto.Decode(frames[1])
		if err != nil {
			fmt.Printf("[%s] publicação inválida: %v\n", s.process.Name, err)
			continue
		}
		s.process.Receive(message)
		proto.ShowPublication(s.process.Name, message)
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

// pickUnsubscribed sorteia um canal da lista em que o bot ainda não está inscrito.
func pickUnsubscribed(channels []string, subscribed map[string]bool) (string, bool) {
	var options []string
	for _, channel := range channels {
		if !subscribed[channel] {
			options = append(options, channel)
		}
	}
	if len(options) == 0 {
		return "", false
	}
	return options[rand.Intn(len(options))], true
}

// env devolve a variável de ambiente ou o valor padrão quando ela não existe.
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// main faz login e, em loop, completa canais e inscrições e publica 10 mensagens em um canal sorteado.
func main() {
	name := os.Getenv("BOT_NAME")
	if name == "" {
		log.Fatal("BOT_NAME é obrigatório")
	}

	process := proto.NewProcess(name, 0)
	r := req.New(process, env("BROKER_ADDR", "tcp://broker:5555"), requestTimeout)
	sub := &subscriber{process: process, address: env("SUB_ADDR", "tcp://proxy:5558"), topics: make(chan string, 16)}
	go sub.run()

	login(r, name)

	subscribed := map[string]bool{}
	for {
		channels := listChannels(r)
		if len(channels) < minChannels {
			r.Call("channel", proto.Data{Channel: pickChannelName(channels)})
			channels = listChannels(r)
		}

		if len(subscribed) < minSubscriptions {
			if channel, ok := pickUnsubscribed(channels, subscribed); ok {
				sub.topics <- channel
				subscribed[channel] = true
			}
		}

		if len(channels) == 0 {
			time.Sleep(retryDelay)
			continue
		}
		publishRound(r, channels[rand.Intn(len(channels))])
	}
}
