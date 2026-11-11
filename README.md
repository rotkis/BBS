# Troca de mensagens com ZeroMQ

Trabalho de Sistemas Distribuídos: um serviço de mensagens parecido com um BBS/IRC em que os usuários são bots. Este README cobre a parte 1, que é o login dos bots e a criação e listagem de canais. Os próximos passos (publicação em canais e relógios) vão entrar em outras branches em cima desta base.

## Como rodar

```
docker compose up --build
```

Sobem 13 containers: o broker, 4 servidores (2 em Python e 2 em Go), um MongoDB para cada servidor e 4 bots (2 em Python e 2 em Go). Cada linha de log mostra uma mensagem enviada (SEND) ou recebida (RECV):

```
22:12:10.982 [bot-go-1] SEND login    sender=bot-go-1 ts=1794262330982 data={"user":"bot-go-1"}
22:12:10.985 [server-go-1] RECV login    sender=bot-go-1 ts=1794262330982 data={"user":"bot-go-1"}
22:12:10.985 [server-go-1] SEND login    sender=server-go-1 ts=1794262330985 data={"status":"ok"}
22:12:10.986 [bot-go-1] RECV login    sender=server-go-1 ts=1794262330985 data={"status":"ok"}
```

Para ver só um serviço: `docker compose logs -f server-py-1`. Para zerar os dados, `docker compose down -v`.

## Estrutura

```
docker-compose.yaml
go/        broker, servidor e cliente em Go
python/    servidor e cliente em Python
```

O Go usa um módulo só com três binários (`bbs-broker`, `bbs-server` e `bbs-client`) e uma imagem só. O Python também usa uma imagem só e o compose escolhe se roda `server.py` ou `client.py`. O Dockerfile do Go roda `go mod tidy` antes de compilar, então o repositório não tem `go.sum`.

## Escolhas

Linguagens: Python e Go. Fiz o trabalho sozinho, então implementei cliente e servidor nas duas linguagens e todos conversam entre si pelo broker. O broker é em Go.

ZeroMQ: os bots usam sockets REQ ligados a um ROUTER do broker na porta 5555. Os servidores usam sockets REP ligados a um DEALER do broker na porta 5556. O broker só repassa as mensagens entre os dois lados (`zmq_proxy`), e o DEALER distribui as requisições entre os servidores em round-robin.

Serialização: MessagePack. Usei mapas com chaves em texto, que são fáceis de ler nas duas linguagens e deixam adicionar campos nas próximas partes sem quebrar quem já existe.

Persistência: MongoDB, uma instância por servidor, para os dados de um servidor não serem compartilhados com outro. Cada instância tem um volume, então os dados continuam depois de um `docker compose down`. O banco se chama `bbs` e tem duas coleções:

- `logins`, com `user`, `timestamp` (enviado pelo bot) e `received_at` (horário de recebimento no servidor). Todo login é gravado, inclusive os repetidos.
- `channels`, com `name`, `created_by` e `timestamp`. O nome tem índice único.

## Mensagens

Toda mensagem tem quatro campos: `service`, `sender`, `timestamp` (milissegundos desde a época Unix) e `data`.

| service | request em `data` | reply em `data` |
|---|---|---|
| `login` | `user` | `status` |
| `channel` | `channel` | `status` |
| `list` | vazio | `status` e `channels` |

`status` é `ok` ou `error`. Quando é `error`, a resposta também traz `code` e `message`. Os códigos usados são:

- `invalid_name`: usuário ou canal fora do padrão. Os nomes aceitos têm de 1 a 32 caracteres entre letras, números, `-` e `_`.
- `already_exists`: já existe um canal com esse nome.
- `unknown_service`: o `service` da requisição não existe.
- `bad_request`: a mensagem não é MessagePack válido ou falta algum campo obrigatório.
- `internal`: erro ao acessar o banco.

O servidor aceita login repetido do mesmo usuário e só grava mais um registro, porque não há logout e um bot que reinicia precisa conseguir entrar de novo.

## Comportamento dos bots

1. Faz login. Se o servidor devolver erro ou não responder em 5 segundos, espera 2 segundos e tenta de novo.
2. Pede a lista de canais.
3. Se existirem menos de 5 canais, cria um com um nome sorteado de uma lista fixa, preferindo um que ainda não exista.
4. A cada 5 segundos pede a lista de canais de novo, só para a troca de mensagens continuar aparecendo nos logs.

Quando uma resposta não chega, o bot fecha o socket REQ e abre outro, porque um REQ fica travado esperando a resposta que se perdeu.

## O que ainda não está resolvido

Cada servidor só enxerga o que gravou. Como o broker distribui as requisições entre os 4 servidores, a lista de canais muda de acordo com quem responde, e o mesmo nome de canal pode ser criado uma vez em cada servidor. Isso vem direto da regra de não compartilhar os dados entre servidores. A replicação entre eles não está nesta parte.
