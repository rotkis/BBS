import os

import zmq
from pymongo.errors import PyMongoError

import common
from store import Store


def make_error(code, message):
    """Monta o corpo de uma resposta de erro."""
    return {"status": "error", "code": code, "message": message}


def handle_login(store, request):
    """Registra o login do bot, recusando nomes fora do padrão."""
    user = request["data"].get("user")
    if not common.valid_name(user):
        return make_error("invalid_name", "usuário deve ter de 1 a 32 caracteres entre letras, números, - e _")
    store.save_login(user, request["timestamp"])
    return {"status": "ok"}


def handle_channel(store, request):
    """Cria um canal novo, recusando nomes inválidos e repetidos."""
    name = request["data"].get("channel")
    if not common.valid_name(name):
        return make_error("invalid_name", "canal deve ter de 1 a 32 caracteres entre letras, números, - e _")
    if not store.create_channel(name, request["sender"], request["timestamp"]):
        return make_error("already_exists", f"o canal {name} já existe")
    return {"status": "ok"}


def handle_list(store, request):
    """Devolve a lista com o nome de todos os canais."""
    return {"status": "ok", "channels": store.list_channels()}


HANDLERS = {
    "login": handle_login,
    "channel": handle_channel,
    "list": handle_list,
}


def process(store, name, raw):
    """Trata uma requisição e devolve a resposta já serializada."""
    try:
        request = common.decode(raw)
    except ValueError as exc:
        reply = common.build("error", name, make_error("bad_request", str(exc)))
        common.show(name, "SEND", reply)
        return common.encode(reply)

    common.show(name, "RECV", request)
    handler = HANDLERS.get(request["service"])
    if handler is None:
        data = make_error("unknown_service", f"serviço {request['service']} não existe")
    else:
        try:
            data = handler(store, request)
        except PyMongoError as exc:
            print(f"erro no banco: {exc}", flush=True)
            data = make_error("internal", "erro ao acessar o banco")

    reply = common.build(request["service"], name, data)
    common.show(name, "SEND", reply)
    return common.encode(reply)


def main():
    """Conecta no banco e no broker e responde requisições em loop."""
    name = os.environ["SERVER_NAME"]
    broker = os.environ.get("BROKER_ADDR", "tcp://broker:5556")
    store = Store(os.environ["MONGO_URI"], "bbs")

    socket = zmq.Context.instance().socket(zmq.REP)
    socket.connect(broker)
    print(f"[{name}] servidor pronto, broker em {broker}", flush=True)

    while True:
        raw = socket.recv()
        socket.send(process(store, name, raw))


if __name__ == "__main__":
    main()
