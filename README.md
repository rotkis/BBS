# Troca de mensagens com ZeroMQ

Trabalho de Sistemas Distribuídos: um serviço de mensagens parecido com um BBS/IRC em que os usuários são bots. Este README cobre as partes 1, 2 e 3: login, criação e listagem de canais, publicação em canais com Pub/Sub, e relógios (lógico em todas as mensagens e físico nos servidores, com um serviço de referência).

## Como rodar

```
docker compose up --build
```

Sobem 15 containers: o broker (Req/Rep), o proxy (Pub/Sub), o serviço de referência, 4 servidores (2 em Python e 2 em Go), um MongoDB para cada servidor e 4 bots (2 em Python e 2 em Go). Cada linha de log mostra uma mensagem enviada ou recebida:

```
22:12:10.982 [bot-go-1] SEND login    sender=bot-go-1 ts=1794262330982 data={"user":"bot-go-1"}
22:12:10.985 [server-go-1] RECV login    sender=bot-go-1 ts=1794262330982 data={"user":"bot-go-1"}
22:12:10.985 [server-go-1] SEND login    sender=server-go-1 ts=1794262330985 data={"status":"ok"}
22:12:10.986 [bot-go-1] RECV login    sender=server-go-1 ts=1794262330985 data={"status":"ok"}
```

As publicações aparecem nos servidores como `PUB` e nos bots inscritos como `RECV publish`, já com o canal, o texto e os dois timestamps:

```
03:29:06.560 [bot-py-1] RECV publish  channel=music user=bot-go-1 text="bom dia [2/10]" sent_at=1794281346560 received_at=1794281346560
```

Para ver só um serviço: `docker compose logs -f server-py-1`. Para zerar os dados, `docker compose down -v`.

## Estrutura

```
docker-compose.yaml
go/        broker, proxy, referência, servidor e cliente em Go
python/    servidor e cliente em Python
```

O Go usa um módulo só com cinco binários (`bbs-broker`, `bbs-proxy`, `bbs-reference`, `bbs-server` e `bbs-client`) e uma imagem só. O Python também usa uma imagem só e o compose escolhe se roda `server.py` ou `client.py`. O Dockerfile do Go roda `go mod tidy` antes de compilar, então o repositório não tem `go.sum`.

## Escolhas

Linguagens: Python e Go. Fiz o trabalho sozinho, então implementei cliente e servidor nas duas linguagens e todos conversam entre si. O broker, o proxy e o serviço de referência são em Go.

Req/Rep: os bots usam sockets REQ ligados a um ROUTER do broker na porta 5555. Os servidores usam sockets REP ligados a um DEALER do broker na porta 5556. O broker só repassa as mensagens entre os dois lados (`zmq_proxy`), e o DEALER distribui as requisições entre os servidores.

Pub/Sub: o proxy é um container separado do broker, com um XSUB na porta 5557 e um XPUB na 5558. Cada servidor tem um socket PUB ligado no XSUB, e cada bot tem um socket SUB ligado no XPUB. O tópico é o nome do canal.

Serialização: MessagePack. Usei mapas com chaves em texto, que são fáceis de ler nas duas linguagens e deixam adicionar campos nas próximas partes sem quebrar quem já existe.

Persistência: MongoDB, uma instância por servidor, para os dados de um servidor não serem compartilhados com outro. Cada instância tem um volume, então os dados continuam depois de um `docker compose down`. O banco se chama `bbs` e tem três coleções:

- `logins`, com `user`, `timestamp` (enviado pelo bot) e `received_at` (horário de recebimento no servidor). Todo login é gravado, inclusive os repetidos.
- `channels`, com `name`, `created_by` e `timestamp`. O nome tem índice único.
- `publications`, com `channel`, `user`, `text`, `timestamp` (do pedido do bot), `published_at` (de quando o servidor publicou) e `received_at` (de quando gravou).

## Mensagens

Toda mensagem tem cinco campos: `service`, `sender`, `timestamp` (milissegundos desde a época Unix), `clock` (o contador do relógio lógico de quem envia) e `data`.

| service | request em `data` | reply em `data` |
|---|---|---|
| `login` | `user` | `status` |
| `channel` | `channel` | `status` |
| `list` | vazio | `status` e `channels` |
| `publish` | `channel` e `text` | `status` |
| `rank` | `name` | `status` e `rank` |
| `heartbeat` | `name` | `status` e `time` |
| `list` (referência) | vazio | `status` e `servers` |

`status` é `ok` ou `error`. Quando é `error`, a resposta também traz `code` e `message`. Os códigos usados são:

- `invalid_name`: usuário ou canal fora do padrão. Os nomes aceitos têm de 1 a 32 caracteres entre letras, números, `-` e `_`.
- `invalid_text`: texto da publicação vazio ou com mais de 500 caracteres.
- `already_exists`: já existe um canal com esse nome.
- `unknown_service`: o `service` da requisição não existe.
- `unknown_server`: a referência recebeu heartbeat de um nome que nunca pediu rank.
- `bad_request`: a mensagem não é MessagePack válido ou falta algum campo obrigatório.
- `internal`: erro ao acessar o banco ou ao publicar.

O servidor aceita login repetido do mesmo usuário e só grava mais um registro, porque não há logout e um bot que reinicia precisa conseguir entrar de novo.

## Publicação

O bot manda `publish` com o canal e o texto. O servidor grava a publicação no banco, publica no tópico do canal e só então responde `ok`. Se gravar ou publicar falhar, a resposta é um erro.

O que vai para o proxy é uma mensagem de duas partes: o nome do canal (o tópico) e um envelope MessagePack com `service` igual a `publish`, `sender` com o nome do servidor e `data` com `channel`, `user` (o bot que pediu) e `text`. O `timestamp` do envelope é o de quando o servidor publicou, e é o "timestamp do envio" que o bot mostra. O de recebimento é o horário em que o bot leu a mensagem.

O servidor não confere se o canal existe no banco dele. Como cada servidor só conhece os canais que ele mesmo criou, essa conferência recusaria publicações em canais que existem em outro servidor.

## Inscrição

A inscrição é só do lado do bot. Ele assina o nome do canal no socket SUB, que continua aberto e recebe em uma thread (Python) ou goroutine (Go) separada. O ZeroMQ casa tópicos por prefixo, então quem assina `tech` também receberia `tech-12`. Por isso o bot compara o tópico recebido com a lista de canais que ele assinou e descarta o resto.

## Comportamento dos bots

1. Faz login. Se o servidor devolver erro ou não responder em 5 segundos, espera 2 segundos e tenta de novo.
2. Em loop:
   1. pede a lista de canais;
   2. se existirem menos de 5, cria um com nome sorteado de uma lista fixa, preferindo um que ainda não exista, e pede a lista de novo;
   3. se estiver inscrito em menos de 3 canais, assina mais um, sorteado entre os da lista em que ainda não está;
   4. sorteia um canal da lista e publica 10 mensagens nele, uma por segundo, com texto sorteado de uma lista fixa e o número da mensagem no final.

Os passos 2 e 3 rodam a cada volta, e não só quando o bot conecta, para o bot chegar a 5 canais e 3 inscrições mesmo quando um pedido cai num servidor que não tem os canais dos outros.

Quando uma resposta não chega, o bot fecha o socket REQ e abre outro, porque um REQ fica travado esperando a resposta que se perdeu.

## Relógio lógico

Todos os processos (bots, servidores e a referência) têm um contador que começa em 0 junto com o processo. O contador é incrementado antes de cada envio e vai no campo `clock`. Quando chega uma mensagem, o contador passa a valer o maior entre o dele e o da mensagem. Isso vale para as requisições e respostas, para as publicações e para as mensagens trocadas com a referência. Nos bots, a thread que recebe as publicações e a que faz as requisições usam o mesmo contador, protegido por um lock.

O campo `clock` aparece em todas as linhas de log.

## Serviço de referência

É um container só, em Go, que escuta na porta 5559 (REP) e só conversa com os servidores. Os servidores usam um socket REQ para falar com ele. Ele guarda tudo em memória:

- `rank`: devolve o rank do servidor. O primeiro nome que pede recebe 1, o seguinte 2, e assim por diante. Um nome que já pediu recebe o mesmo rank de novo, então um servidor que reinicia volta com o rank que tinha. Ranks não são reaproveitados.
- `list`: devolve o nome e o rank dos servidores que estão na lista, em ordem de rank.
- `heartbeat`: marca o servidor como vivo e devolve a hora da referência no campo `time`. Se o nome nunca pediu rank, a resposta é `unknown_server` e o servidor pede o rank de novo.

Um servidor que não manda heartbeat por mais do que `HEARTBEAT_TIMEOUT_S` segundos (30 no compose) sai da lista. Se ele mandar um heartbeat depois disso, volta para a lista com o mesmo rank.

## Heartbeat e sincronização do relógio físico

Ao iniciar, o servidor pede o rank (tentando de novo a cada 2 segundos até a referência responder), manda um heartbeat e passa a mandar um a cada 10 requisições de clientes recebidas. A resposta do heartbeat já traz a hora, então não existe uma requisição separada só para o relógio. Depois de cada heartbeat o servidor também pede a lista de servidores à referência e mostra no log.

O ajuste usa o algoritmo de Cristian. O servidor anota a hora local antes de mandar o heartbeat (t0) e quando a resposta chega (t1). Com a hora `T` devolvida pela referência, ele estima que a hora certa em t1 é `T + (t1 - t0) / 2` e guarda a diferença para a hora local como um deslocamento. Esse deslocamento é somado ao `timestamp` de todas as mensagens que o servidor envia depois disso. O log mostra a cada ajuste algo como `offset=-3000ms rtt=0ms`. Os bots não sincronizam o relógio, só os servidores.

Como todos os containers usam o relógio da mesma máquina, os relógios nunca divergem sozinhos. Para dar o que corrigir, o compose define `CLOCK_SKEW_MS` em cada servidor (+3000, -2000, +1500 e -4000), que é somado à hora local do servidor. Com `CLOCK_SKEW_MS` igual a 0 o deslocamento calculado fica perto de zero.

O broker e o proxy não mudaram.

## O que ainda não está resolvido

Cada servidor só enxerga o que gravou. Como o broker distribui as requisições entre os 4 servidores, bots diferentes podem receber listas de canais diferentes, e o mesmo nome de canal pode ser criado uma vez em cada servidor. Isso aparece nos logs como erros `already_exists` de vez em quando. Isso vem direto da regra de não compartilhar os dados entre servidores, e a replicação entre eles não está nesta parte.

O Pub/Sub não guarda nem reentrega nada. Um bot só recebe o que for publicado depois de ele assinar o canal, e um bot que entra depois não vê o que já foi publicado. Cada servidor grava as publicações que ele mesmo recebeu por requisição, então o histórico de um canal fica espalhado pelos bancos dos servidores que atenderam os pedidos.

O heartbeat é por número de requisições, não por tempo. Se nenhum bot estiver mandando requisições, os servidores não mandam heartbeat e a referência os tira da lista depois do timeout. Com os 4 bots do compose cada servidor recebe uma requisição por segundo ou mais e o timeout de 30 segundos não é atingido.

A referência guarda ranks e a lista só em memória. Se ela reiniciar, cada servidor recebe `unknown_server` no próximo heartbeat e pede o rank de novo, mas os ranks são atribuídos outra vez na ordem em que os servidores aparecerem.
