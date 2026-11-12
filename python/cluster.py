import threading
import time

import zmq

import common

ELECTION_TIMEOUT_MS = 1000
TIME_TIMEOUT_MS = 500
CLOCK_TIMEOUT_MS = 2000
SYNC_EVERY = 15
SERVERS_TOPIC = "servers"
DOWN_SECONDS = 10


def make_error(code, message):
    """Monta o corpo de uma resposta de erro."""
    return {"status": "error", "code": code, "message": message}


class Cluster:
    def __init__(self, node, reference, publisher, peer_template, peer_bind, sub_addr):
        """Guarda o que os servidores precisam para falar entre si: eleição, relógio e anúncios."""
        self.node = node
        self.reference = reference
        self.publisher = publisher
        self.peer_template = peer_template
        self.peer_bind = peer_bind
        self.sub_addr = sub_addr
        self.rank = None
        self.coordinator = None
        self.servers = []
        self.down_until = {}
        self.election_lock = threading.Lock()
        self.sync_lock = threading.Lock()
        self.next_sync = SYNC_EVERY

    def start(self):
        """Liga a thread que atende os outros servidores e a que escuta os anúncios de coordenador."""
        threading.Thread(target=self.serve_peers, daemon=True).start()
        threading.Thread(target=self.listen_announcements, daemon=True).start()

    def peer_address(self, name):
        """Monta o endereço do socket de outro servidor a partir do nome dele."""
        return self.peer_template.format(name=name)

    def call_peer(self, name, service, data, timeout_ms):
        """Faz uma requisição a outro servidor."""
        return common.call_once(self.node, self.peer_address(name), timeout_ms, service, data)

    def fetch_servers(self):
        """Pede à referência a lista de servidores ativos e, se ela não responder, usa a última que tinha."""
        reply = self.reference.call("list", {})
        if reply is not None and reply["data"].get("status") == "ok":
            self.servers = reply["data"].get("servers", [])
        return list(self.servers)

    def elect(self):
        """Eleição do valentão: pede eleição aos servidores de rank maior e, se nenhum responder, vira coordenador."""
        if self.rank is None or not self.election_lock.acquire(blocking=False):
            return
        try:
            higher = [s for s in self.fetch_servers() if s["rank"] > self.rank]
            answered = False
            for server in higher:
                reply = self.call_peer(server["name"], "election", {}, ELECTION_TIMEOUT_MS)
                if reply is not None and reply["data"].get("status") == "ok":
                    answered = True
            if answered:
                self.coordinator = None
                print(f"[{self.node.name}] um servidor de rank maior respondeu, ele assume a eleição", flush=True)
                return
            self.become_coordinator()
        finally:
            self.election_lock.release()

    def become_coordinator(self):
        """Assume como coordenador, avisa os outros no tópico servers e acerta o relógio de todos."""
        self.coordinator = self.node.name
        print(f"[{self.node.name}] eleito coordenador (rank {self.rank})", flush=True)
        message = self.node.build(
            "coordinator", {"coordinator": self.node.name, "rank": self.rank}
        )
        self.publisher.publish(SERVERS_TOPIC, message)
        self.berkeley_round()

    def berkeley_round(self):
        """Berkeley: pergunta a hora dos outros servidores, calcula a média com a sua e ajusta o próprio relógio para ela."""
        diffs = [0]
        for server in self.fetch_servers():
            name = server["name"]
            if name == self.node.name or self.down_until.get(name, 0) > time.time():
                continue
            t0 = self.node.now()
            reply = self.call_peer(name, "time", {}, TIME_TIMEOUT_MS)
            t1 = self.node.now()
            if reply is None or reply["data"].get("status") != "ok":
                self.down_until[name] = time.time() + DOWN_SECONDS
                continue
            peer_time = reply["data"]["time"] + (t1 - t0) // 2
            diffs.append(peer_time - t1)
        average = round(sum(diffs) / len(diffs))
        self.node.shift(average)
        print(
            f"[{self.node.name}] Berkeley: média de {len(diffs)} relógios, ajuste de {average:+d}ms",
            flush=True,
        )

    def sync_clock(self):
        """Pede a hora correta ao coordenador; se ele não responder, começa uma eleição."""
        if not self.sync_lock.acquire(blocking=False):
            return
        try:
            coordinator = self.coordinator
            if coordinator is None:
                self.elect()
                return
            if coordinator == self.node.name:
                self.berkeley_round()
                return
            t0 = self.node.raw_now()
            reply = self.call_peer(coordinator, "clock", {}, CLOCK_TIMEOUT_MS)
            t1 = self.node.raw_now()
            if reply is None or reply["data"].get("status") != "ok":
                print(f"[{self.node.name}] coordenador {coordinator} indisponível, pedindo eleição", flush=True)
                self.coordinator = None
                self.elect()
                return
            data = reply["data"]
            offset, rtt = self.node.adjust(data["time"], data.get("elapsed", 0), t0, t1)
            print(
                f"[{self.node.name}] relógio sincronizado com {coordinator}: offset={offset:+d}ms rtt={rtt}ms",
                flush=True,
            )
        finally:
            self.sync_lock.release()

    def after_request(self):
        """Pede a hora ao coordenador a cada 15 mensagens trocadas."""
        if self.node.exchanged >= self.next_sync:
            self.sync_clock()
            self.next_sync = self.node.exchanged + SYNC_EVERY

    def handle_election(self):
        """Responde OK a quem pediu eleição e começa a própria eleição em outra thread."""
        if self.rank is None:
            return make_error("not_ready", "servidor ainda não tem rank")
        threading.Thread(target=self.elect, daemon=True).start()
        return {"status": "ok"}

    def handle_clock(self):
        """Se for o coordenador, roda uma rodada de Berkeley e devolve a hora correta."""
        if self.coordinator != self.node.name:
            return make_error("not_coordinator", "este servidor não é o coordenador")
        started = time.monotonic()
        self.berkeley_round()
        elapsed = round((time.monotonic() - started) * 1000)
        return {"status": "ok", "time": self.node.now(), "elapsed": elapsed}

    def handle_peer(self, raw):
        """Trata uma requisição de outro servidor e devolve a resposta já serializada."""
        try:
            request = common.decode(raw)
        except ValueError as exc:
            reply = self.node.build("error", make_error("bad_request", str(exc)))
            common.show(self.node.name, "SEND", reply)
            return common.encode(reply)

        self.node.receive(request)
        common.show(self.node.name, "RECV", request)
        service = request["service"]
        if service == "election":
            data = self.handle_election()
        elif service == "clock":
            data = self.handle_clock()
        elif service == "time":
            data = {"status": "ok", "time": self.node.now()}
        else:
            data = make_error("unknown_service", f"serviço {service} não existe")
        reply = self.node.build(service, data)
        common.show(self.node.name, "SEND", reply)
        return common.encode(reply)

    def serve_peers(self):
        """Atende em loop as requisições dos outros servidores."""
        socket = zmq.Context.instance().socket(zmq.REP)
        socket.bind(self.peer_bind)
        while True:
            raw = socket.recv()
            socket.send(self.handle_peer(raw))

    def listen_announcements(self):
        """Escuta o tópico servers; quem anuncia vira o coordenador, a não ser que tenha rank menor que o deste servidor."""
        socket = zmq.Context.instance().socket(zmq.SUB)
        socket.connect(self.sub_addr)
        socket.setsockopt_string(zmq.SUBSCRIBE, SERVERS_TOPIC)
        while True:
            try:
                _, raw = socket.recv_multipart()
                message = common.decode(raw)
            except (ValueError, zmq.ZMQError):
                continue
            self.node.receive(message)
            coordinator = message["data"].get("coordinator")
            rank = message["data"].get("rank", 0)
            common.show(self.node.name, "RECV", message)
            if coordinator == self.node.name:
                continue
            if self.rank is not None and rank < self.rank:
                threading.Thread(target=self.elect, daemon=True).start()
                continue
            self.coordinator = coordinator
            print(f"[{self.node.name}] coordenador agora é {coordinator}", flush=True)
            self.sync_clock()
