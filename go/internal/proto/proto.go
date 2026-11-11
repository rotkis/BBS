package proto

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

type Data struct {
	User     string   `msgpack:"user,omitempty" json:"user,omitempty"`
	Channel  string   `msgpack:"channel,omitempty" json:"channel,omitempty"`
	Channels []string `msgpack:"channels,omitempty" json:"channels,omitempty"`
	Text     string   `msgpack:"text,omitempty" json:"text,omitempty"`
	Status   string   `msgpack:"status,omitempty" json:"status,omitempty"`
	Code     string   `msgpack:"code,omitempty" json:"code,omitempty"`
	Message  string   `msgpack:"message,omitempty" json:"message,omitempty"`
}

type Message struct {
	Service   string `msgpack:"service"`
	Sender    string `msgpack:"sender"`
	Timestamp int64  `msgpack:"timestamp"`
	Data      Data   `msgpack:"data"`
}

// ValidName confere se um nome de usuário ou de canal segue o padrão aceito.
func ValidName(value string) bool {
	return namePattern.MatchString(value)
}

// New monta uma mensagem já com o timestamp do envio.
func New(service, sender string, data Data) Message {
	return Message{
		Service:   service,
		Sender:    sender,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}
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
	return message, nil
}

// Show imprime a mensagem enviada ou recebida em uma linha só.
func Show(me, direction string, message Message) {
	data, _ := json.Marshal(message.Data)
	fmt.Printf("%s [%s] %s %-8s sender=%s ts=%d data=%s\n",
		time.Now().Format("15:04:05.000"), me, direction, message.Service,
		message.Sender, message.Timestamp, data)
}

// ShowPublication imprime uma publicação recebida com o canal, o texto, o timestamp do envio e o do recebimento.
func ShowPublication(me string, message Message) {
	now := time.Now()
	text, _ := json.Marshal(message.Data.Text)
	fmt.Printf("%s [%s] RECV publish  channel=%s user=%s text=%s sent_at=%d received_at=%d\n",
		now.Format("15:04:05.000"), me, message.Data.Channel, message.Data.User,
		text, message.Timestamp, now.UnixMilli())
}
