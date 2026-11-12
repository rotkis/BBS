import os
import queue
import random
import threading
import time

import zmq

import common

REQUEST_TIMEOUT_MS = 5000
RETRY_DELAY_S = 2
MIN_CHANNELS = 5
MIN_SUBSCRIPTIONS = 3
MESSAGES_PER_ROUND = 10
HISTORY_LIMIT = 3
MESSAGE_INTERVAL_S = 1
CHANNEL_WORDS = [
    "general", "random", "music", "games", "news",
    "tech", "sports", "movies", "books", "travel",
]
MESSAGE_TEXTS = [
    "bom dia", "alguém online?", "vamos testar isso", "mensagem aleatória",
    "olá, mundo", "tudo certo por aqui", "ping", "qualquer novidade?",
    "teste de publicação", "até mais",
]


class Subscriber(threading.Thread):
    def __init__(self, node, address):
        """Cria a thread que recebe as publicações; o socket SUB só é usado dentro dela."""
        super().__init__(daemon=True)
        self.node = node
        self.address = address
        self.topics = queue.Queue()
        self.subscribed = set()

    def subscribe(self, channel):
        """Pede para a thread assinar o canal."""
        self.topics.put(channel)

    def run(self):
        """Assina os canais pedidos e imprime tudo o que chega nos canais assinados."""
        socket = zmq.Context.instance().socket(zmq.SUB)
        socket.setsockopt(zmq.RCVTIMEO, 100)
        socket.connect(self.address)
        while True:
            while not self.topics.empty():
                channel = self.topics.get()
                socket.setsockopt_string(zmq.SUBSCRIBE, channel)
                self.subscribed.add(channel)
            try:
                topic, raw = socket.recv_multipart()
            except zmq.Again:
                continue
            except ValueError:
                continue
            if topic.decode() not in self.subscribed:
                continue
            try:
                message = common.decode(raw)
            except ValueError as exc:
                print(f"[{self.node.name}] publicação inválida: {exc}", flush=True)
                continue
            self.node.receive(message)
            common.show_publication(self.node.name, message)


def is_ok(reply):
    """Diz se a resposta chegou e veio com status ok."""
    return reply is not None and reply["data"].get("status") == "ok"


def login(requester):
    """Repete o login até o servidor aceitar."""
    while not is_ok(requester.call("login", {"user": requester.node.name})):
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


def publish_round(requester, channel):
    """Publica 10 mensagens aleatórias no canal, uma por segundo."""
    for number in range(1, MESSAGES_PER_ROUND + 1):
        text = f"{random.choice(MESSAGE_TEXTS)} [{number}/{MESSAGES_PER_ROUND}]"
        requester.call("publish", {"channel": channel, "text": text})
        time.sleep(MESSAGE_INTERVAL_S)


def main():
    """Faz login e, em loop, completa canais e inscrições, publica 10 mensagens em um canal sorteado e pede o histórico dele."""
    name = os.environ["BOT_NAME"]
    broker = os.environ.get("BROKER_ADDR", "tcp://broker:5555")
    proxy = os.environ.get("SUB_ADDR", "tcp://proxy:5558")
    node = common.Node(name)
    requester = common.Requester(node, broker, REQUEST_TIMEOUT_MS)
    subscriber = Subscriber(node, proxy)
    subscriber.start()

    login(requester)

    subscribed = set()
    while True:
        channels = list_channels(requester)
        if len(channels) < MIN_CHANNELS:
            requester.call("channel", {"channel": pick_channel_name(channels)})
            channels = list_channels(requester)

        if len(subscribed) < MIN_SUBSCRIPTIONS:
            options = [channel for channel in channels if channel not in subscribed]
            if options:
                choice = random.choice(options)
                subscriber.subscribe(choice)
                subscribed.add(choice)

        if not channels:
            time.sleep(RETRY_DELAY_S)
            continue
        channel = random.choice(channels)
        publish_round(requester, channel)
        requester.call("history", {"channel": channel, "limit": HISTORY_LIMIT})


if __name__ == "__main__":
    main()
