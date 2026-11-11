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
        """Abre a conexão e garante que o nome do canal seja único no banco."""
        self.client = connect(uri)
        db = self.client[db_name]
        self.logins = db["logins"]
        self.channels = db["channels"]
        self.publications = db["publications"]
        self.channels.create_index("name", unique=True)

    def save_login(self, user, timestamp):
        """Guarda o login com o timestamp enviado pelo bot e o horário em que o servidor recebeu."""
        self.logins.insert_one(
            {"user": user, "timestamp": timestamp, "received_at": now_ms()}
        )

    def create_channel(self, name, user, timestamp):
        """Cria o canal e devolve False se o nome já existia."""
        try:
            self.channels.insert_one(
                {"name": name, "created_by": user, "timestamp": timestamp}
            )
        except DuplicateKeyError:
            return False
        return True

    def list_channels(self):
        """Devolve os nomes de todos os canais em ordem alfabética."""
        cursor = self.channels.find({}, {"_id": 0, "name": 1}).sort("name", 1)
        return [doc["name"] for doc in cursor]

    def save_publication(self, channel, user, text, timestamp, published_at):
        """Guarda a publicação com o timestamp do pedido, o da publicação e o horário de gravação."""
        self.publications.insert_one(
            {
                "channel": channel,
                "user": user,
                "text": text,
                "timestamp": timestamp,
                "published_at": published_at,
                "received_at": now_ms(),
            }
        )
