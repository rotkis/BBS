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

// main liga o lado dos publicadores (XSUB) ao lado dos assinantes (XPUB) e repassa publicações e inscrições.
func main() {
	xsub, err := zmq.NewSocket(zmq.XSUB)
	if err != nil {
		log.Fatal(err)
	}
	xpub, err := zmq.NewSocket(zmq.XPUB)
	if err != nil {
		log.Fatal(err)
	}

	xsubAddr := env("XSUB_ADDR", "tcp://*:5557")
	xpubAddr := env("XPUB_ADDR", "tcp://*:5558")
	if err := xsub.Bind(xsubAddr); err != nil {
		log.Fatal(err)
	}
	if err := xpub.Bind(xpubAddr); err != nil {
		log.Fatal(err)
	}

	log.Printf("proxy pronto: publicadores em %s, assinantes em %s", xsubAddr, xpubAddr)
	log.Fatal(zmq.Proxy(xsub, xpub, nil))
}
