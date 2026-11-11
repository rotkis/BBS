import os
import random
import time

import zmq

import common

REQUEST_TIMEOUT_MS = 5000
RETRY_DELAY_S = 2
LIST_INTERVAL_S = 5
MIN_CHANNELS = 5
CHANNEL_WORDS = [
    "general", "random", "music", "games", "news",
    "tech", "sports", "movies", "books", "travel",
]


class Requester:
    def __init__(self, name, address):
        """Guarda o nome do bot e abre o primeiro socket REQ."""
        self.name = name
        self.address = address
        self.context = zmq.Context.instance()
        self.socket = None
        self.connect()

    def connect(self):
        """Abre um socket REQ novo, na partida e depois de cada timeout."""
        if self.socket is not None:
            self.socket.close(linger=0)
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.RCVTIMEO, REQUEST_TIMEOUT_MS)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.address)

    def call(self, service, data):
        """Envia uma requisição e espera a resposta; devolve None se der timeout ou vier algo inválido."""
        message = common.build(service, self.name, data)
        common.show(self.name, "SEND", message)
        self.socket.send(common.encode(message))
        try:
            raw = self.socket.recv()
        except zmq.Again:
            print(f"[{self.name}] sem resposta para {service}, reconectando", flush=True)
            self.connect()
            return None
        try:
            reply = common.decode(raw)
        except ValueError as exc:
            print(f"[{self.name}] resposta inválida: {exc}", flush=True)
            return None
        common.show(self.name, "RECV", reply)
        return reply


def is_ok(reply):
    """Diz se a resposta chegou e veio com status ok."""
    return reply is not None and reply["data"].get("status") == "ok"


def login(requester):
    """Repete o login até o servidor aceitar."""
    while not is_ok(requester.call("login", {"user": requester.name})):
        time.sleep(RETRY_DELAY_S)


def list_channels(requester):
    """Pede a lista de canais e tenta de novo até receber."""
    while True:
        reply = requester.call("list", {})
        if is_ok(reply):
            return reply["data"].get("channels", [])
        time.sleep(RETRY_DELAY_S)


def pick_channel_name(existing):
    """Escolhe um nome que ainda não esteja na lista, ou sorteia um com sufixo se todos já existirem."""
    free = [word for word in CHANNEL_WORDS if word not in existing]
    if free:
        return random.choice(free)
    return f"{random.choice(CHANNEL_WORDS)}-{random.randint(1, 999)}"


def main():
    """Faz login, cria canais até ter o mínimo e depois lista os canais de tempos em tempos."""
    name = os.environ["BOT_NAME"]
    broker = os.environ.get("BROKER_ADDR", "tcp://broker:5555")
    requester = Requester(name, broker)

    login(requester)

    channels = list_channels(requester)
    if len(channels) < MIN_CHANNELS:
        requester.call("channel", {"channel": pick_channel_name(channels)})

    while True:
        time.sleep(LIST_INTERVAL_S)
        list_channels(requester)


if __name__ == "__main__":
    main()
