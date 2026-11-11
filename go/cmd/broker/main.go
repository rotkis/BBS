package main

import (
	"log"
	"os"

	zmq "github.com/pebbe/zmq4"
)

// env devolve a variável de ambiente ou o valor padrão quando ela não existe.
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// main liga o lado dos clientes (ROUTER) ao lado dos servidores (DEALER) e repassa as mensagens.
func main() {
	frontend, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Fatal(err)
	}
	backend, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal(err)
	}

	frontendAddr := env("FRONTEND_ADDR", "tcp://*:5555")
	backendAddr := env("BACKEND_ADDR", "tcp://*:5556")
	if err := frontend.Bind(frontendAddr); err != nil {
		log.Fatal(err)
	}
	if err := backend.Bind(backendAddr); err != nil {
		log.Fatal(err)
	}

	log.Printf("broker pronto: clientes em %s, servidores em %s", frontendAddr, backendAddr)
	log.Fatal(zmq.Proxy(frontend, backend, nil))
}
