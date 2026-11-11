import json
import re
import threading
import time

import msgpack
import zmq

NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def now_ms():
    """Devolve o horário atual em milissegundos desde a época Unix."""
    return time.time_ns() // 1_000_000


def valid_name(value):
    """Confere se um nome de usuário, canal ou servidor segue o padrão aceito."""
    return isinstance(value, str) and NAME_PATTERN.match(value) is not None


class Node:
    def __init__(self, name, skew_ms=0):
        """Guarda o nome do processo, o desvio simulado do relógio físico e o contador lógico."""
        self.name = name
        self.skew_ms = skew_ms
        self.offset_ms = 0
        self.logical = 0
        self.lock = threading.Lock()

    def raw_now(self):
        """Devolve o horário local em milissegundos, já somado ao desvio simulado."""
        return now_ms() + self.skew_ms

    def now(self):
        """Devolve o horário físico do processo, corrigido pela última sincronização."""
        return self.raw_now() + self.offset_ms

    def tick(self):
        """Incrementa o contador lógico antes de um envio e devolve o novo valor."""
        with self.lock:
            self.logical += 1
            return self.logical

    def receive(self, message):
        """Atualiza o contador lógico para o maior valor entre o próprio e o da mensagem recebida."""
        with self.lock:
            self.logical = max(self.logical, message["clock"])

    def adjust(self, reference_time, t0, t1):
        """Ajusta o relógio físico pelo algoritmo de Cristian e devolve o deslocamento e o RTT em ms."""
        rtt = t1 - t0
        self.offset_ms = reference_time + rtt // 2 - t1
        return self.offset_ms, rtt

    def build(self, service, data):
        """Monta uma mensagem com o timestamp do envio e o contador lógico já incrementado."""
        return {
            "service": service,
            "sender": self.name,
            "timestamp": self.now(),
            "clock": self.tick(),
            "data": data,
        }


def encode(message):
    """Serializa a mensagem em MessagePack."""
    return msgpack.packb(message, use_bin_type=True)


def decode(raw):
    """Desserializa uma mensagem e confere os campos obrigatórios; levanta ValueError se algo estiver errado."""
    try:
        message = msgpack.unpackb(raw, raw=False)
    except Exception as exc:
        raise ValueError("mensagem não é MessagePack válido") from exc
    if not isinstance(message, dict):
        raise ValueError("a mensagem precisa ser um mapa")
    if not isinstance(message.get("service"), str) or not message["service"]:
        raise ValueError("campo service ausente")
    if not isinstance(message.get("sender"), str):
        raise ValueError("campo sender ausente")
    if not isinstance(message.get("timestamp"), int):
        raise ValueError("campo timestamp ausente")
    if not isinstance(message.get("clock"), int):
        raise ValueError("campo clock ausente")
    if message.get("data") is None:
        message["data"] = {}
    if not isinstance(message["data"], dict):
        raise ValueError("campo data precisa ser um mapa")
    return message


def show(me, direction, message):
    """Imprime a mensagem enviada ou recebida em uma linha só."""
    now = time.localtime()
    clock = time.strftime("%H:%M:%S", now) + f".{now_ms() % 1000:03d}"
    data = json.dumps(message["data"], ensure_ascii=False, separators=(",", ":"))
    print(
        f"{clock} [{me}] {direction} {message['service']:<9} "
        f"sender={message['sender']} ts={message['timestamp']} clock={message['clock']} data={data}",
        flush=True,
    )


def show_publication(me, message):
    """Imprime uma publicação recebida com o canal, o texto, o timestamp do envio e o do recebimento."""
    now = time.localtime()
    received_at = now_ms()
    clock = time.strftime("%H:%M:%S", now) + f".{received_at % 1000:03d}"
    data = message["data"]
    text = json.dumps(data.get("text", ""), ensure_ascii=False)
    print(
        f"{clock} [{me}] RECV publish   channel={data.get('channel')} "
        f"user={data.get('user')} text={text} "
        f"sent_at={message['timestamp']} received_at={received_at} clock={message['clock']}",
        flush=True,
    )


class Requester:
    def __init__(self, node, address, timeout_ms):
        """Guarda o processo, o endereço e o tempo limite e abre o primeiro socket REQ."""
        self.node = node
        self.address = address
        self.timeout_ms = timeout_ms
        self.context = zmq.Context.instance()
        self.socket = None
        self.connect()

    def connect(self):
        """Abre um socket REQ novo, na partida e depois de cada timeout."""
        if self.socket is not None:
            self.socket.close(linger=0)
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.address)

    def call(self, service, data):
        """Envia uma requisição e espera a resposta; devolve None se der timeout ou vier algo inválido."""
        message = self.node.build(service, data)
        show(self.node.name, "SEND", message)
        self.socket.send(encode(message))
        try:
            raw = self.socket.recv()
        except zmq.Again:
            print(f"[{self.node.name}] sem resposta para {service}, reconectando", flush=True)
            self.connect()
            return None
        try:
            reply = decode(raw)
        except ValueError as exc:
            print(f"[{self.node.name}] resposta inválida: {exc}", flush=True)
            return None
        self.node.receive(reply)
        show(self.node.name, "RECV", reply)
        return reply
