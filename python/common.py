import json
import re
import time

import msgpack

NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def now_ms():
    """Devolve o horário atual em milissegundos desde a época Unix."""
    return time.time_ns() // 1_000_000


def valid_name(value):
    """Confere se um nome de usuário ou de canal segue o padrão aceito."""
    return isinstance(value, str) and NAME_PATTERN.match(value) is not None


def build(service, sender, data):
    """Monta uma mensagem já com o timestamp do envio."""
    return {
        "service": service,
        "sender": sender,
        "timestamp": now_ms(),
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
        f"{clock} [{me}] {direction} {message['service']:<8} "
        f"sender={message['sender']} ts={message['timestamp']} data={data}",
        flush=True,
    )
