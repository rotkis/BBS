import threading
import time

import zmq

import common

ELECTION_TIMEOUT_MS = 1000
TIME_TIMEOUT_MS = 500
CLOCK_TIMEOUT_MS = 2000
SYNC_EVERY = 15
PUSH_TIMEOUT_MS = 1000
FORWARD_TIMEOUT_MS = 3000
CATCHUP_TIMEOUT_MS = 3000
CATCHUP_BATCH = 200
SERVERS_MAX_AGE_S = 5
PEER_WORKERS = 8
SERVERS_TOPIC = "servers"
DOWN_SECONDS = 10


def make_error(code, message):
    """Monta o corpo de uma resposta de erro."""
    return {"status": "error", "code": code, "message": message}


class Cluster:
    def __init__(self, node, reference, publisher, store, peer_template, peer_bind, sub_addr):
        """Guarda o que os servidores precisam para falar entre si: eleição, relógio e anúncios."""
        self.node = node
        self.reference = reference
        self.publisher = publisher
        self.store = store
        self.peer_template = peer_template
        self.peer_bind = peer_bind
        self.sub_addr = sub_addr
        self.rank = None
        self.coordinator = None
        self.servers = []
        self.servers_at = 0
        self.down_until = {}
        self.op_lock = threading.Lock()
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
            self.update_servers(reply["data"].get("servers", []))
        return list(self.servers)

    def update_servers(self, servers):
        """Guarda a lista de servidores ativos e a hora em que ela foi pega."""
        self.servers = servers
        self.servers_at = time.time()

    def active_servers(self):
        """Devolve a lista de servidores ativos, pedindo uma nova à referência se a guardada tiver mais de 5 segundos."""
        if time.time() - self.servers_at > SERVERS_MAX_AGE_S:
            return self.fetch_servers()
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
        """Busca com os outros servidores o que falta no próprio log, assume como coordenador, avisa no tópico servers e acerta o relógio de todos."""
        for server in self.fetch_servers():
            name = server["name"]
            if name != self.node.name and self.down_until.get(name, 0) <= time.time():
                self.catch_up(name)
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

    def write(self, op):
        """Executa uma escrita: o coordenador numera e replica, os outros servidores encaminham ao coordenador."""
        if self.coordinator is None:
            self.elect()
        coordinator = self.coordinator
        if coordinator == self.node.name:
            return self.execute(op)
        if coordinator is None:
            return make_error("no_coordinator", "ainda não há coordenador, tente de novo")
        reply = self.call_peer(coordinator, "forward", {"ops": [op]}, FORWARD_TIMEOUT_MS)
        if reply is None or reply["data"].get("code") == "not_coordinator":
            self.coordinator = None
            threading.Thread(target=self.elect, daemon=True).start()
            return make_error("coordinator_unavailable", "coordenador indisponível, tente de novo")
        data = reply["data"]
        return {key: data[key] for key in ("status", "code", "message") if key in data}

    def execute(self, op):
        """Como coordenador, confere as regras do canal, numera e grava a operação, replica nos outros servidores e só então devolve ok."""
        with self.op_lock:
            if op["type"] == "channel" and self.store.channel_exists(op["channel"]):
                return make_error("already_exists", f"o canal {op['channel']} já existe")
            if op["type"] == "publish" and not self.store.channel_exists(op["channel"]):
                return make_error("channel_not_found", f"o canal {op['channel']} não existe")
            op = dict(op)
            op["at"] = self.node.now()
            op = self.store.append(op)
            self.push(op)
        return {"status": "ok"}

    def push(self, op):
        """Manda a operação para todos os outros servidores ativos; quem estiver atrasado recebe também as que perdeu."""
        for server in self.active_servers():
            name = server["name"]
            if name == self.node.name or self.down_until.get(name, 0) > time.time():
                continue
            reply = self.call_peer(name, "replicate", {"ops": [op]}, PUSH_TIMEOUT_MS)
            if reply is None:
                self.down_until[name] = time.time() + DOWN_SECONDS
                continue
            if reply["data"].get("code") == "gap":
                missing = self.store.ops_after(reply["data"].get("last", 0), CATCHUP_BATCH)
                self.call_peer(name, "replicate", {"ops": missing}, CATCHUP_TIMEOUT_MS)

    def catch_up(self, name):
        """Puxa de outro servidor as operações que faltam no log deste, em lotes, até ficar em dia."""
        while True:
            reply = self.call_peer(name, "catchup", {"after": self.store.last_seq()}, CATCHUP_TIMEOUT_MS)
            if reply is None or reply["data"].get("status") != "ok":
                return
            ops = reply["data"].get("ops", [])
            for op in ops:
                if self.store.apply_replicated(op) == "gap":
                    return
            if len(ops) < CATCHUP_BATCH:
                return

    def handle_forward(self, request):
        """Executa a escrita que outro servidor encaminhou, se este for o coordenador."""
        if self.coordinator != self.node.name:
            return make_error("not_coordinator", "este servidor não é o coordenador")
        ops = request["data"].get("ops", [])
        if len(ops) != 1 or ops[0].get("type") not in ("login", "channel", "publish"):
            return make_error("bad_request", "operação inválida")
        return self.execute(ops[0])

    def handle_replicate(self, request):
        """Aplica em ordem as operações recebidas do coordenador; se faltar alguma antes, responde gap com o último número que tem."""
        ops = sorted(request["data"].get("ops", []), key=lambda op: op["seq"])
        for op in ops:
            if self.store.apply_replicated(op) == "gap":
                return {"status": "error", "code": "gap", "last": self.store.last_seq()}
        return {"status": "ok", "last": self.store.last_seq()}

    def handle_catchup(self, request):
        """Devolve um lote de operações com número maior que o pedido."""
        after = request["data"].get("after", 0)
        return {"status": "ok", "ops": self.store.ops_after(after, CATCHUP_BATCH)}

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
        elif service == "forward":
            data = self.handle_forward(request)
        elif service == "replicate":
            data = self.handle_replicate(request)
        elif service == "catchup":
            data = self.handle_catchup(request)
        else:
            data = make_error("unknown_service", f"serviço {service} não existe")
        reply = self.node.build(service, data)
        common.show(self.node.name, "SEND", reply)
        return common.encode(reply)

    def serve_peers(self):
        """Recebe as requisições dos outros servidores em um ROUTER e reparte entre as threads de trabalho, para um pedido demorado não travar os demais."""
        context = zmq.Context.instance()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(self.peer_bind)
        backend = context.socket(zmq.DEALER)
        backend.bind("inproc://peers")
        for _ in range(PEER_WORKERS):
            threading.Thread(target=self.peer_worker, daemon=True).start()
        zmq.proxy(frontend, backend)

    def peer_worker(self):
        """Atende requisições de outros servidores, uma por vez, em um socket REP próprio."""
        socket = zmq.Context.instance().socket(zmq.REP)
        socket.connect("inproc://peers")
        while True:
            raw = socket.recv()
            try:
                reply = self.handle_peer(raw)
            except Exception as exc:
                print(f"[{self.node.name}] erro ao tratar pedido de outro servidor: {exc}", flush=True)
                reply = common.encode(self.node.build("error", make_error("internal", "erro interno")))
            socket.send(reply)

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
            self.catch_up(coordinator)
