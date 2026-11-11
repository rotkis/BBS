package proto

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

type ServerInfo struct {
	Name string `msgpack:"name" json:"name"`
	Rank int    `msgpack:"rank" json:"rank"`
}

type Data struct {
	User     string       `msgpack:"user,omitempty" json:"user,omitempty"`
	Channel  string       `msgpack:"channel,omitempty" json:"channel,omitempty"`
	Channels []string     `msgpack:"channels,omitempty" json:"channels,omitempty"`
	Text     string       `msgpack:"text,omitempty" json:"text,omitempty"`
	Status   string       `msgpack:"status,omitempty" json:"status,omitempty"`
	Code     string       `msgpack:"code,omitempty" json:"code,omitempty"`
	Message  string       `msgpack:"message,omitempty" json:"message,omitempty"`
	Name     string       `msgpack:"name,omitempty" json:"name,omitempty"`
	Rank     int          `msgpack:"rank,omitempty" json:"rank,omitempty"`
	Time     int64        `msgpack:"time,omitempty" json:"time,omitempty"`
	Servers  []ServerInfo `msgpack:"servers,omitempty" json:"servers,omitempty"`
}

type Message struct {
	Service   string `msgpack:"service"`
	Sender    string `msgpack:"sender"`
	Timestamp int64  `msgpack:"timestamp"`
	Clock     int64  `msgpack:"clock"`
	Data      Data   `msgpack:"data"`
}

type Process struct {
	Name    string
	mu      sync.Mutex
	logical int64
	skew    int64
	offset  int64
}

// NewProcess cria o estado de relógios de um processo, com o desvio simulado do relógio físico em milissegundos.
func NewProcess(name string, skewMillis int64) *Process {
	return &Process{Name: name, skew: skewMillis}
}

// RawNow devolve o horário local em milissegundos, já somado ao desvio simulado.
func (p *Process) RawNow() int64 {
	return time.Now().UnixMilli() + p.skew
}

// Now devolve o horário físico do processo, corrigido pela última sincronização.
func (p *Process) Now() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return time.Now().UnixMilli() + p.skew + p.offset
}

// Receive atualiza o contador lógico para o maior valor entre o próprio e o da mensagem recebida.
func (p *Process) Receive(message Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if message.Clock > p.logical {
		p.logical = message.Clock
	}
}

// Adjust ajusta o relógio físico pelo algoritmo de Cristian e devolve o deslocamento e o RTT em ms.
func (p *Process) Adjust(referenceTime, t0, t1 int64) (offset, rtt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rtt = t1 - t0
	p.offset = referenceTime + rtt/2 - t1
	return p.offset, rtt
}

// New monta uma mensagem com o timestamp do envio e o contador lógico já incrementado.
func (p *Process) New(service string, data Data) Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logical++
	return Message{
		Service:   service,
		Sender:    p.Name,
		Timestamp: time.Now().UnixMilli() + p.skew + p.offset,
		Clock:     p.logical,
		Data:      data,
	}
}

// ValidName confere se um nome de usuário, canal ou servidor segue o padrão aceito.
func ValidName(value string) bool {
	return namePattern.MatchString(value)
}

// Encode serializa a mensagem em MessagePack.
func Encode(message Message) ([]byte, error) {
	return msgpack.Marshal(message)
}

// Decode desserializa uma mensagem e confere os campos obrigatórios.
func Decode(raw []byte) (Message, error) {
	var message Message
	if err := msgpack.Unmarshal(raw, &message); err != nil {
		return Message{}, errors.New("mensagem não é um mapa MessagePack válido")
	}
	if message.Service == "" {
		return Message{}, errors.New("campo service ausente")
	}
	if message.Sender == "" {
		return Message{}, errors.New("campo sender ausente")
	}
	if message.Timestamp == 0 {
		return Message{}, errors.New("campo timestamp ausente")
	}
	if message.Clock == 0 {
		return Message{}, errors.New("campo clock ausente")
	}
	return message, nil
}

// Show imprime a mensagem enviada ou recebida em uma linha só.
func Show(me, direction string, message Message) {
	data, _ := json.Marshal(message.Data)
	fmt.Printf("%s [%s] %s %-9s sender=%s ts=%d clock=%d data=%s\n",
		time.Now().Format("15:04:05.000"), me, direction, message.Service,
		message.Sender, message.Timestamp, message.Clock, data)
}

// ShowPublication imprime uma publicação recebida com o canal, o texto, o timestamp do envio e o do recebimento.
func ShowPublication(me string, message Message) {
	now := time.Now()
	text, _ := json.Marshal(message.Data.Text)
	fmt.Printf("%s [%s] RECV publish   channel=%s user=%s text=%s sent_at=%d received_at=%d clock=%d\n",
		now.Format("15:04:05.000"), me, message.Data.Channel, message.Data.User,
		text, message.Timestamp, now.UnixMilli(), message.Clock)
}
