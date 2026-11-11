import os

import zmq
from pymongo.errors import PyMongoError

import common
from store import Store

MAX_TEXT_LENGTH = 500


def make_error(code, message):
    """Monta o corpo de uma resposta de erro."""
    return {"status": "error", "code": code, "message": message}


class Server:
    def __init__(self, name, store, publisher):
        """Guarda o nome, o banco e o socket PUB e monta a tabela de serviços."""
        self.name = name
        self.store = store
        self.publisher = publisher
        self.handlers = {
            "login": self.handle_login,
            "channel": self.handle_channel,
            "list": self.handle_list,
            "publish": self.handle_publish,
        }

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

        message = common.build(
            "publish", self.name, {"channel": channel, "user": request["sender"], "text": text}
        )
        self.store.save_publication(
            channel, request["sender"], text, request["timestamp"], message["timestamp"]
        )
        self.publisher.send_multipart([channel.encode(), common.encode(message)])
        common.show(self.name, "PUB ", message)
        return {"status": "ok"}

    def process(self, raw):
        """Trata uma requisição e devolve a resposta já serializada."""
        try:
            request = common.decode(raw)
        except ValueError as exc:
            reply = common.build("error", self.name, make_error("bad_request", str(exc)))
            common.show(self.name, "SEND", reply)
            return common.encode(reply)

        common.show(self.name, "RECV", request)
        handler = self.handlers.get(request["service"])
        if handler is None:
            data = make_error("unknown_service", f"serviço {request['service']} não existe")
        else:
            try:
                data = handler(request)
            except PyMongoError as exc:
                print(f"erro no banco: {exc}", flush=True)
                data = make_error("internal", "erro ao acessar o banco")

        reply = common.build(request["service"], self.name, data)
        common.show(self.name, "SEND", reply)
        return common.encode(reply)


def main():
    """Conecta no banco, no broker e no proxy e responde requisições em loop."""
    name = os.environ["SERVER_NAME"]
    broker = os.environ.get("BROKER_ADDR", "tcp://broker:5556")
    proxy = os.environ.get("PROXY_ADDR", "tcp://proxy:5557")
    store = Store(os.environ["MONGO_URI"], "bbs")

    context = zmq.Context.instance()
    publisher = context.socket(zmq.PUB)
    publisher.connect(proxy)
    socket = context.socket(zmq.REP)
    socket.connect(broker)
    server = Server(name, store, publisher)
    print(f"[{name}] servidor pronto, broker em {broker}, proxy em {proxy}", flush=True)

    while True:
        raw = socket.recv()
        socket.send(server.process(raw))


if __name__ == "__main__":
    main()
