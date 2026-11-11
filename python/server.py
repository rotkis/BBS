import os
import time

import zmq
from pymongo.errors import PyMongoError

import common
from store import Store

MAX_TEXT_LENGTH = 500
HEARTBEAT_EVERY = 10
REFERENCE_TIMEOUT_MS = 2000
RETRY_DELAY_S = 2


def make_error(code, message):
    """Monta o corpo de uma resposta de erro."""
    return {"status": "error", "code": code, "message": message}


class Server:
    def __init__(self, node, store, publisher, reference):
        """Guarda o processo, o banco, o socket PUB e o cliente do serviço de referência e monta a tabela de serviços."""
        self.node = node
        self.store = store
        self.publisher = publisher
        self.reference = reference
        self.rank = None
        self.servers = []
        self.received = 0
        self.handlers = {
            "login": self.handle_login,
            "channel": self.handle_channel,
            "list": self.handle_list,
            "publish": self.handle_publish,
        }

    def register(self):
        """Pede o rank ao serviço de referência, esperando ele ficar disponível, e já sincroniza o relógio."""
        while True:
            reply = self.reference.call("rank", {"name": self.node.name})
            if reply is not None and reply["data"].get("status") == "ok":
                self.rank = reply["data"]["rank"]
                break
            time.sleep(RETRY_DELAY_S)
        print(f"[{self.node.name}] registrado na referência com rank {self.rank}", flush=True)
        self.heartbeat()

    def heartbeat(self):
        """Avisa a referência que o servidor está vivo, corrige o relógio com a hora devolvida e atualiza a lista de servidores."""
        t0 = self.node.raw_now()
        reply = self.reference.call("heartbeat", {"name": self.node.name})
        t1 = self.node.raw_now()
        if reply is None:
            return
        data = reply["data"]
        if data.get("status") != "ok":
            if data.get("code") == "unknown_server":
                self.register()
            return

        offset, rtt = self.node.adjust(data["time"], t0, t1)
        print(f"[{self.node.name}] relógio sincronizado: offset={offset:+d}ms rtt={rtt}ms", flush=True)

        reply = self.reference.call("list", {})
        if reply is not None and reply["data"].get("status") == "ok":
            self.servers = reply["data"].get("servers", [])
            names = ", ".join(f"{s['name']}(rank {s['rank']})" for s in self.servers)
            print(f"[{self.node.name}] servidores ativos: {names}", flush=True)

    def after_request(self):
        """Conta a requisição de cliente e manda o heartbeat a cada 10 recebidas."""
        self.received += 1
        if self.received % HEARTBEAT_EVERY == 0:
            self.heartbeat()

    def handle_login(self, request):
        """Registra o login do bot, recusando nomes fora do padrão."""
        user = request["data"].get("user")
        if not common.valid_name(user):
            return make_error("invalid_name", "usuário deve ter de 1 a 32 caracteres entre letras, números, - e _")
        self.store.save_login(user, request["timestamp"])
        return {"status": "ok"}

    def handle_channel(self, request):
        """Cria um canal novo, recusando nomes inválidos e repetidos."""
        name = request["data"].get("channel")
        if not common.valid_name(name):
            return make_error("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _")
        if not self.store.create_channel(name, request["sender"], request["timestamp"]):
            return make_error("already_exists", f"o canal {name} já existe")
        return {"status": "ok"}

    def handle_list(self, request):
        """Devolve a lista com o nome de todos os canais."""
        return {"status": "ok", "channels": self.store.list_channels()}

    def handle_publish(self, request):
        """Grava a publicação, publica no tópico do canal e devolve o status."""
        channel = request["data"].get("channel")
        text = request["data"].get("text")
        if not common.valid_name(channel):
            return make_error("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _")
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_TEXT_LENGTH:
            return make_error("invalid_text", f"o texto deve ter de 1 a {MAX_TEXT_LENGTH} caracteres")

        message = self.node.build(
            "publish", {"channel": channel, "user": request["sender"], "text": text}
        )
        self.store.save_publication(
            channel, request["sender"], text, request["timestamp"], message["timestamp"]
        )
        self.publisher.send_multipart([channel.encode(), common.encode(message)])
        common.show(self.node.name, "PUB ", message)
        return {"status": "ok"}

    def process(self, raw):
        """Trata uma requisição e devolve a resposta já serializada."""
        try:
            request = common.decode(raw)
        except ValueError as exc:
            reply = self.node.build("error", make_error("bad_request", str(exc)))
            common.show(self.node.name, "SEND", reply)
            return common.encode(reply)

        self.node.receive(request)
        common.show(self.node.name, "RECV", request)
        handler = self.handlers.get(request["service"])
        if handler is None:
            data = make_error("unknown_service", f"serviço {request['service']} não existe")
        else:
            try:
                data = handler(request)
            except PyMongoError as exc:
                print(f"erro no banco: {exc}", flush=True)
                data = make_error("internal", "erro ao acessar o banco")

        reply = self.node.build(request["service"], data)
        common.show(self.node.name, "SEND", reply)
        return common.encode(reply)


def main():
    """Registra o servidor na referência, conecta no banco, no broker e no proxy e responde requisições em loop."""
    name = os.environ["SERVER_NAME"]
    broker = os.environ.get("BROKER_ADDR", "tcp://broker:5556")
    proxy = os.environ.get("PROXY_ADDR", "tcp://proxy:5557")
    reference_addr = os.environ.get("REFERENCE_ADDR", "tcp://reference:5559")
    skew_ms = int(os.environ.get("CLOCK_SKEW_MS", "0"))
    store = Store(os.environ["MONGO_URI"], "bbs")

    node = common.Node(name, skew_ms)
    reference = common.Requester(node, reference_addr, REFERENCE_TIMEOUT_MS)

    context = zmq.Context.instance()
    publisher = context.socket(zmq.PUB)
    publisher.connect(proxy)
    server = Server(node, store, publisher, reference)
    server.register()

    socket = context.socket(zmq.REP)
    socket.connect(broker)
    print(f"[{name}] servidor pronto, broker em {broker}, proxy em {proxy}", flush=True)

    while True:
        raw = socket.recv()
        socket.send(server.process(raw))
        server.after_request()


if __name__ == "__main__":
    main()
