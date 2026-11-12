package req

import (
	"fmt"
	"log"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
)

type Requester struct {
	mu      sync.Mutex
	process *proto.Process
	address string
	timeout time.Duration
	socket  *zmq.Socket
}

// New cria o requisitante e abre o primeiro socket REQ.
func New(process *proto.Process, address string, timeout time.Duration) *Requester {
	r := &Requester{process: process, address: address, timeout: timeout}
	r.connect()
	return r
}

// connect abre um socket REQ novo, na partida e depois de cada timeout.
func (r *Requester) connect() {
	if r.socket != nil {
		r.socket.Close()
	}
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		log.Fatal(err)
	}
	socket.SetLinger(0)
	socket.SetRcvtimeo(r.timeout)
	if err := socket.Connect(r.address); err != nil {
		log.Fatal(err)
	}
	r.socket = socket
}

// Call envia uma requisição e espera a resposta; devolve false se der timeout ou vier algo inválido.
func (r *Requester) Call(service string, data proto.Data) (proto.Message, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	request := r.process.New(service, data)
	proto.Show(r.process.Name, "SEND", request)
	raw, err := proto.Encode(request)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := r.socket.SendBytes(raw, 0); err != nil {
		log.Fatal(err)
	}

	rawReply, err := r.socket.RecvBytes(0)
	if err != nil {
		fmt.Printf("[%s] sem resposta para %s, reconectando\n", r.process.Name, service)
		r.connect()
		return proto.Message{}, false
	}
	reply, err := proto.Decode(rawReply)
	if err != nil {
		fmt.Printf("[%s] resposta inválida: %v\n", r.process.Name, err)
		return proto.Message{}, false
	}
	r.process.Receive(reply)
	proto.Show(r.process.Name, "RECV", reply)
	return reply, true
}

type Publisher struct {
	mu      sync.Mutex
	process *proto.Process
	socket  *zmq.Socket
}

// NewPublisher guarda o socket PUB e o lock que deixa só uma goroutine publicar por vez.
func NewPublisher(process *proto.Process, socket *zmq.Socket) *Publisher {
	return &Publisher{process: process, socket: socket}
}

// Publish publica a mensagem no tópico e mostra na tela.
func (p *Publisher) Publish(topic string, message proto.Message) error {
	raw, err := proto.Encode(message)
	if err != nil {
		return err
	}
	p.mu.Lock()
	_, err = p.socket.SendMessage(topic, raw)
	p.mu.Unlock()
	if err == nil {
		proto.Show(p.process.Name, "PUB ", message)
	}
	return err
}

// Once faz uma requisição com um socket REQ novo, usado para falar com outros servidores; devolve false se der timeout.
func Once(process *proto.Process, address string, timeout time.Duration, service string, data proto.Data) (proto.Message, bool) {
	request := process.New(service, data)
	proto.Show(process.Name, "SEND", request)
	raw, err := proto.Encode(request)
	if err != nil {
		log.Fatal(err)
	}
	socket, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()
	socket.SetLinger(0)
	socket.SetRcvtimeo(timeout)
	socket.SetSndtimeo(timeout)
	if err := socket.Connect(address); err != nil {
		return proto.Message{}, false
	}
	if _, err := socket.SendBytes(raw, 0); err != nil {
		fmt.Printf("[%s] sem resposta de %s para %s\n", process.Name, address, service)
		return proto.Message{}, false
	}
	rawReply, err := socket.RecvBytes(0)
	if err != nil {
		fmt.Printf("[%s] sem resposta de %s para %s\n", process.Name, address, service)
		return proto.Message{}, false
	}
	reply, err := proto.Decode(rawReply)
	if err != nil {
		fmt.Printf("[%s] resposta inválida: %v\n", process.Name, err)
		return proto.Message{}, false
	}
	process.Receive(reply)
	proto.Show(process.Name, "RECV", reply)
	return reply, true
}
