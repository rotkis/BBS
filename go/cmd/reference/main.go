package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"

	"bbs/internal/proto"
)

type reference struct {
	process  *proto.Process
	ranks    map[string]int
	lastSeen map[string]time.Time
	timeout  time.Duration
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

// handleRank devolve o rank do servidor, criando um novo na primeira vez que o nome aparece.
func (r *reference) handleRank(request proto.Message) proto.Data {
	name := request.Data.Name
	if !proto.ValidName(name) {
		return errorData("invalid_name", "nome do servidor deve ter de 1 a 32 caracteres entre letras, números, - e _")
	}
	rank, known := r.ranks[name]
	if !known {
		rank = len(r.ranks) + 1
		r.ranks[name] = rank
	}
	r.lastSeen[name] = time.Now()
	fmt.Printf("%s [%s] servidor %s registrado com rank %d\n", time.Now().Format("15:04:05.000"), r.process.Name, name, rank)
	return proto.Data{Status: "ok", Rank: rank}
}

// handleHeartbeat renova a presença do servidor na lista.
func (r *reference) handleHeartbeat(request proto.Message) proto.Data {
	name := request.Data.Name
	if _, known := r.ranks[name]; !known {
		return errorData("unknown_server", fmt.Sprintf("servidor %s não está registrado", name))
	}
	if _, active := r.lastSeen[name]; !active {
		fmt.Printf("%s [%s] servidor %s voltou à lista\n", time.Now().Format("15:04:05.000"), r.process.Name, name)
	}
	r.lastSeen[name] = time.Now()
	return proto.Data{Status: "ok"}
}

// handleList devolve o nome e o rank dos servidores ativos, em ordem de rank.
func (r *reference) handleList() proto.Data {
	servers := make([]proto.ServerInfo, 0, len(r.lastSeen))
	for name := range r.lastSeen {
		servers = append(servers, proto.ServerInfo{Name: name, Rank: r.ranks[name]})
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i].Rank < servers[j].Rank })
	return proto.Data{Status: "ok", Servers: servers}
}

// purge tira da lista os servidores que passaram do tempo limite sem mandar heartbeat.
func (r *reference) purge() {
	for name, seen := range r.lastSeen {
		if time.Since(seen) > r.timeout {
			delete(r.lastSeen, name)
			fmt.Printf("%s [%s] servidor %s removido: sem heartbeat há mais de %s\n",
				time.Now().Format("15:04:05.000"), r.process.Name, name, r.timeout)
		}
	}
}

// handle trata uma requisição e devolve a resposta já serializada.
func (r *reference) handle(raw []byte) []byte {
	request, err := proto.Decode(raw)
	var service string
	var data proto.Data
	if err != nil {
		service, data = "error", errorData("bad_request", err.Error())
	} else {
		r.process.Receive(request)
		proto.Show(r.process.Name, "RECV", request)
		service = request.Service
		switch request.Service {
		case "rank":
			data = r.handleRank(request)
		case "heartbeat":
			data = r.handleHeartbeat(request)
		case "list":
			data = r.handleList()
		default:
			data = errorData("unknown_service", fmt.Sprintf("serviço %s não existe", request.Service))
		}
	}

	reply := r.process.New(service, data)
	proto.Show(r.process.Name, "SEND", reply)
	encoded, err := proto.Encode(reply)
	if err != nil {
		log.Fatal(err)
	}
	return encoded
}

// main atende os servidores e, a cada segundo sem requisições, confere quem ficou sem mandar heartbeat.
func main() {
	timeoutSeconds, err := strconv.Atoi(env("HEARTBEAT_TIMEOUT_S", "30"))
	if err != nil {
		log.Fatal(err)
	}
	r := &reference{
		process:  proto.NewProcess("reference", 0),
		ranks:    map[string]int{},
		lastSeen: map[string]time.Time{},
		timeout:  time.Duration(timeoutSeconds) * time.Second,
	}

	socket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		log.Fatal(err)
	}
	socket.SetRcvtimeo(time.Second)
	address := env("REFERENCE_BIND", "tcp://*:5559")
	if err := socket.Bind(address); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[reference] pronto em %s, timeout de heartbeat de %s\n", address, r.timeout)

	for {
		raw, err := socket.RecvBytes(0)
		if err != nil {
			if zmq.AsErrno(err) != zmq.Errno(syscall.EAGAIN) {
				log.Fatal(err)
			}
			r.purge()
			continue
		}
		if _, err := socket.SendBytes(r.handle(raw), 0); err != nil {
			log.Fatal(err)
		}
		r.purge()
	}
}
