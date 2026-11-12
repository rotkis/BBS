import threading
import time

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from common import now_ms


def connect(uri, attempts=60):
    """Tenta conectar no MongoDB até ele responder, já que o container pode demorar a subir."""
    for attempt in range(1, attempts + 1):
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        try:
            client.admin.command("ping")
            return client
        except Exception as exc:
            print(f"mongo indisponível ({attempt}/{attempts}): {exc}", flush=True)
            client.close()
            time.sleep(1)
    raise RuntimeError("não consegui conectar no MongoDB")


class Store:
    def __init__(self, uri, db_name):
        """Abre a conexão, garante o índice único do nome do canal e descobre o número da última operação gravada."""
        self.client = connect(uri)
        db = self.client[db_name]
        self.logins = db["logins"]
        self.channels = db["channels"]
        self.publications = db["publications"]
        self.oplog = db["oplog"]
        self.channels.create_index("name", unique=True)
        self.lock = threading.Lock()
        last = self.oplog.find_one(sort=[("_id", -1)])
        self.last = last["_id"] if last else 0

    def last_seq(self):
        """Devolve o número da última operação aplicada neste servidor."""
        return self.last

    def apply(self, op):
        """Grava a operação na coleção do tipo e no log; só pode rodar com o lock adquirido."""
        seq = op["seq"]
        try:
            if op["type"] == "login":
                self.logins.insert_one(
                    {"_id": seq, "user": op["user"], "timestamp": op["timestamp"], "received_at": now_ms()}
                )
            elif op["type"] == "channel":
                self.channels.insert_one(
                    {"_id": seq, "name": op["channel"], "created_by": op["user"], "timestamp": op["timestamp"]}
                )
            elif op["type"] == "publish":
                self.publications.insert_one(
                    {
                        "_id": seq,
                        "channel": op["channel"],
                        "user": op["user"],
                        "text": op["text"],
                        "timestamp": op["timestamp"],
                        "published_at": op["at"],
                        "received_at": now_ms(),
                    }
                )
        except DuplicateKeyError:
            pass
        entry = {key: value for key, value in op.items() if key != "seq"}
        entry["_id"] = seq
        try:
            self.oplog.insert_one(entry)
        except DuplicateKeyError:
            pass
        self.last = seq

    def append(self, op):
        """Numera a operação com o próximo número da sequência, grava e devolve a operação numerada; é o que o coordenador usa."""
        with self.lock:
            op = dict(op)
            op["seq"] = self.last + 1
            self.apply(op)
            return op

    def apply_replicated(self, op):
        """Aplica uma operação vinda do coordenador e devolve applied, duplicate (já tinha) ou gap (falta alguma antes)."""
        with self.lock:
            if op["seq"] <= self.last:
                return "duplicate"
            if op["seq"] != self.last + 1:
                return "gap"
            self.apply(op)
            return "applied"

    def ops_after(self, seq, limit):
        """Devolve até limit operações com número maior que seq, em ordem."""
        ops = []
        for doc in self.oplog.find({"_id": {"$gt": seq}}).sort("_id", 1).limit(limit):
            op = {key: value for key, value in doc.items() if key != "_id"}
            op["seq"] = doc["_id"]
            ops.append(op)
        return ops

    def channel_exists(self, name):
        """Diz se já existe um canal com esse nome."""
        return self.channels.find_one({"name": name}) is not None

    def list_channels(self):
        """Devolve os nomes de todos os canais em ordem alfabética."""
        cursor = self.channels.find({}, {"_id": 0, "name": 1}).sort("name", 1)
        return [doc["name"] for doc in cursor]

    def history(self, channel, limit):
        """Devolve o total de publicações do canal e as últimas limit delas, da mais antiga para a mais nova."""
        total = self.publications.count_documents({"channel": channel})
        docs = self.publications.find({"channel": channel}).sort("_id", -1).limit(limit)
        messages = [
            {"seq": doc["_id"], "user": doc["user"], "text": doc["text"], "timestamp": doc["published_at"]}
            for doc in docs
        ]
        messages.reverse()
        return total, messages
