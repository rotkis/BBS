import os
import time

import zmq
from pymongo.errors import PyMongoError

import common
from cluster import Cluster
from store import Store

MAX_TEXT_LENGTH = 500
MAX_HISTORY = 100
HEARTBEAT_EVERY = 10
REFERENCE_TIMEOUT_MS = 2000
RETRY_DELAY_S = 2


def make_error(code, message):
    """Monta o corpo de uma resposta de erro."""
    return {"status": "error", "code": code, "message": message}


class Server:
    def __init__(self, node, store, publisher, reference, cluster):
        """Guarda o processo, o banco, o socket PUB e o cliente do serviço de referência e monta a tabela de serviços."""
        self.node = node
        self.store = store
        self.publisher = publisher
        self.reference = reference
        self.cluster = cluster
        self.rank = None
        self.servers = []
        self.received = 0
        self.handlers = {
            "login": self.handle_login,
            "channel": self.handle_channel,
            "list": self.handle_list,
            "publish": self.handle_publish,
            "history": self.handle_history,
        }

    def register(self):
        """Pede o rank ao serviço de referência, esperando ele ficar disponível, e já sincroniza o relógio."""
        while True:
            reply = self.reference.call("rank", {"name": self.node.name})
            if reply is not None and reply["data"].get("status") == "ok":
                self.rank = reply["data"]["rank"]
                break
            time.sleep(RETRY_DELAY_S)
        self.cluster.rank = self.rank
        print(f"[{self.node.name}] registrado na referência com rank {self.rank}", flush=True)
        self.heartbeat()

    def heartbeat(self):
        """Avisa a referência que o servidor está vivo e atualiza a lista de servidores."""
        reply = self.reference.call("heartbeat", {"name": self.node.name})
        if reply is None:
            return
        data = reply["data"]
        if data.get("status") != "ok":
            if data.get("code") == "unknown_server":
                self.register()
            return

        reply = self.reference.call("list", {})
        if reply is not None and reply["data"].get("status") == "ok":
            self.servers = reply["data"].get("servers", [])
            self.cluster.update_servers(self.servers)
            names = ", ".join(f"{s['name']}(rank {s['rank']})" for s in self.servers)
            print(f"[{self.node.name}] servidores ativos: {names}", flush=True)

    def after_request(self):
        """Conta a requisição de cliente, manda o heartbeat a cada 10 recebidas e acerta o relógio a cada 15 mensagens trocadas."""
        self.received += 1
        if self.received % HEARTBEAT_EVERY == 0:
            self.heartbeat()
        self.cluster.after_request()

    def handle_login(self, request):
        """Valida o nome e manda o login para ser gravado e replicado."""
        user = request["data"].get("user")
        if not common.valid_name(user):
            return make_error("invalid_name", "usuário deve ter de 1 a 32 caracteres entre letras, números, - e _")
        return self.cluster.write({"type": "login", "user": user, "timestamp": request["timestamp"]})

    def handle_channel(self, request):
        """Valida o nome e manda a criação do canal para ser gravada e replicada."""
        name = request["data"].get("channel")
        if not common.valid_name(name):
            return make_error("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _")
        return self.cluster.write(
            {"type": "channel", "user": request["sender"], "channel": name, "timestamp": request["timestamp"]}
        )

    def handle_list(self, request):
        """Devolve a lista com o nome de todos os canais."""
        return {"status": "ok", "channels": self.store.list_channels()}

    def handle_history(self, request):
        """Devolve o total de publicações do canal e as últimas que este servidor tem."""
        channel = request["data"].get("channel")
        limit = request["data"].get("limit", 20)
        if not common.valid_name(channel):
            return make_error("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _")
        if not isinstance(limit, int) or not 1 <= limit <= MAX_HISTORY:
            return make_error("invalid_limit", f"limit deve ficar entre 1 e {MAX_HISTORY}")
        total, messages = self.store.history(channel, limit)
        return {"status": "ok", "channel": channel, "total": total, "messages": messages}

    def handle_publish(self, request):
        """Valida a publicação, manda gravar e replicar e, se deu certo, publica no tópico do canal."""
        channel = request["data"].get("channel")
        text = request["data"].get("text")
        if not common.valid_name(channel):
            return make_error("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _")
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_TEXT_LENGTH:
            return make_error("invalid_text", f"o texto deve ter de 1 a {MAX_TEXT_LENGTH} caracteres")

        result = self.cluster.write(
            {
                "type": "publish",
                "channel": channel,
                "user": request["sender"],
                "text": text,
                "timestamp": request["timestamp"],
            }
        )
        if result.get("status") == "ok":
            message = self.node.build("publish", {"channel": channel, "user": request["sender"], "text": text})
            self.publisher.publish(channel, message)
        return result

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
    sub_addr = os.environ.get("SUB_ADDR", "tcp://proxy:5558")
    peer_bind = os.environ.get("PEER_BIND", "tcp://*:6000")
    peer_template = os.environ.get("PEER_ADDR", "tcp://{name}:6000")
    reference_addr = os.environ.get("REFERENCE_ADDR", "tcp://reference:5559")
    skew_ms = int(os.environ.get("CLOCK_SKEW_MS", "0"))
    store = Store(os.environ["MONGO_URI"], "bbs")

    node = common.Node(name, skew_ms)
    reference = common.Requester(node, reference_addr, REFERENCE_TIMEOUT_MS)

    context = zmq.Context.instance()
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(proxy)
    publisher = common.Publisher(pub_socket)
    cluster = Cluster(node, reference, publisher, store, peer_template, peer_bind, sub_addr)
    cluster.start()
    server = Server(node, store, publisher, reference, cluster)
    server.register()
    cluster.elect()

    socket = context.socket(zmq.REP)
    socket.connect(broker)
    print(f"[{name}] servidor pronto, broker em {broker}, proxy em {proxy}", flush=True)

    while True:
        raw = socket.recv()
        socket.send(server.process(raw))
        server.after_request()


if __name__ == "__main__":
    main()
