# Troca de mensagens com ZeroMQ

Trabalho de Sistemas Distribuídos: um serviço de mensagens parecido com um BBS/IRC em que os usuários são bots. Este README cobre o projeto completo, partes 1 a 5: login, criação e listagem de canais, publicação em canais com Pub/Sub, relógios (lógico em todas as mensagens e físico nos servidores), serviço de referência, eleição de um coordenador e replicação dos dados entre todos os servidores.

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

Para ver só um serviço: `docker compose logs -f server-py-1`. Para zerar os dados, `docker compose down -v`. Quem rodou uma versão anterior do projeto precisa fazer isso antes, porque o formato dos dados gravados mudou na parte 5. Se você já rodou o projeto e os containers antigos ainda existem, rode `docker compose down` antes de subir de novo, senão o `up` falha com conflito de nome de container ou de rede.

## Parar o coordenador

Para mostrar a eleição, primeiro descubra quem é o coordenador atual. Ele é o servidor que aparece nas rodadas de Berkeley (só o coordenador faz a rodada) e também o último eleito:

```
docker compose logs --tail 500 server-py-1 server-py-2 server-go-1 server-go-2 2>&1 | grep -E "Berkeley: média|eleito coordenador" | tail -1 | sed -E 's/.*\[(server-[^]]+)\].*/\1/'
```

A saída é só o nome, por exemplo `server-py-2`. Deixe os logs dos servidores abertos em outro terminal e pare esse container:

```
docker compose logs -f server-py-1 server-py-2 server-go-1 server-go-2
docker compose stop server-py-2
```

Os outros servidores notam a falta do coordenador no pedido de relógio e começam a eleição. Os logs mostram `[server-go-2] coordenador server-py-2 indisponível, pedindo eleição` e depois `[server-py-1] eleito coordenador (rank 3)`, que é o servidor vivo de maior rank. As escritas que chegam durante a eleição voltam com `no_coordinator` ou `coordinator_unavailable`, e os bots tentam de novo na rodada seguinte. Para religar o servidor parado, use `docker compose start server-py-2`: ele volta e pede `catchup` do que perdeu.

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

Persistência: MongoDB, uma instância por servidor, cada uma com o seu volume, então os dados continuam depois de um `docker compose down`. Nenhum arquivo ou banco é compartilhado entre servidores, mas desde a parte 5 cada servidor guarda uma cópia de tudo (ver a seção sobre replicação). O banco se chama `bbs` e tem quatro coleções:

- `oplog`, com o log de operações: `_id` (o número da operação), `type` (`login`, `channel` ou `publish`), `user`, `channel`, `text`, `timestamp` (do pedido do bot) e `at` (de quando o coordenador aplicou).
- `logins`, com `user`, `timestamp` (enviado pelo bot) e `received_at` (de quando este servidor gravou). Todo login é gravado, inclusive os repetidos.
- `channels`, com `name`, `created_by` e `timestamp`. O nome tem índice único.
- `publications`, com `channel`, `user`, `text`, `timestamp` (do pedido do bot), `published_at` (de quando o coordenador aplicou) e `received_at` (de quando este servidor gravou).

Nas três últimas o `_id` é o número da operação no `oplog`. Assim aplicar de novo uma operação que já estava gravada não duplica nada.

Para ver as coleções do banco de um servidor, sem precisar entrar no shell:

```
docker exec mongo-server-py-1 mongosh bbs --quiet --eval 'db.getCollectionNames()'
```

Para contar os documentos de uma coleção, troque o `--eval` por `db.publications.countDocuments()`. E para ver os últimos documentos gravados:

```
docker exec mongo-server-py-1 mongosh bbs --quiet --eval 'db.publications.find().sort({_id:-1}).limit(5)'
```

Troque `mongo-server-py-1` por `mongo-server-py-2`, `mongo-server-go-1` ou `mongo-server-go-2` para ver o outro servidor. Para ficar no shell interativo, tire o `--eval` (e aqui use `-it`): `docker exec -it mongo-server-py-1 mongosh bbs`. Os containers do MongoDB não expõem porta para o host, então o acesso é sempre por dentro do container.

## Mensagens

Toda mensagem tem cinco campos: `service`, `sender`, `timestamp` (milissegundos desde a época Unix), `clock` (o contador do relógio lógico de quem envia) e `data`.

| service | request em `data` | reply em `data` |
|---|---|---|
| `login` | `user` | `status` |
| `channel` | `channel` | `status` |
| `list` | vazio | `status` e `channels` |
| `publish` | `channel` e `text` | `status` |
| `history` | `channel` e `limit` (opcional) | `status`, `channel`, `total` e `messages` |
| `rank` | `name` | `status` e `rank` |
| `heartbeat` | `name` | `status` |
| `list` (referência) | vazio | `status` e `servers` |

Entre os servidores há ainda `election`, `clock`, `time`, `forward`, `replicate` e `catchup`, e o anúncio `coordinator` publicado no tópico `servers`.

`status` é `ok` ou `error`. Quando é `error`, a resposta também traz `code` e `message`. Os códigos usados são:

- `invalid_name`: usuário ou canal fora do padrão. Os nomes aceitos têm de 1 a 32 caracteres entre letras, números, `-` e `_`.
- `invalid_text`: texto da publicação vazio ou com mais de 500 caracteres.
- `already_exists`: já existe um canal com esse nome.
- `channel_not_found`: publicação em um canal que não existe.
- `invalid_limit`: `limit` do histórico fora do intervalo de 1 a 100.
- `no_coordinator` e `coordinator_unavailable`: o servidor não conseguiu falar com um coordenador para gravar a escrita, por exemplo durante uma eleição. O bot pode tentar de novo.
- `unknown_service`: o `service` da requisição não existe.
- `unknown_server`: a referência recebeu heartbeat de um nome que nunca pediu rank.
- `bad_request`: a mensagem não é MessagePack válido ou falta algum campo obrigatório.
- `internal`: erro ao acessar o banco ou ao publicar.

O servidor aceita login repetido do mesmo usuário e só grava mais um registro, porque não há logout e um bot que reinicia precisa conseguir entrar de novo.

## Publicação

O bot manda `publish` com o canal e o texto. O servidor manda a publicação para ser gravada e replicada (ver a seção sobre replicação), publica no tópico do canal e só então responde `ok`. Se gravar ou publicar falhar, a resposta é um erro.

O que vai para o proxy é uma mensagem de duas partes: o nome do canal (o tópico) e um envelope MessagePack com `service` igual a `publish`, `sender` com o nome do servidor e `data` com `channel`, `user` (o bot que pediu) e `text`. O `timestamp` do envelope é o de quando o servidor que atendeu o bot publicou, e é o "timestamp do envio" que o bot mostra. O de recebimento é o horário em que o bot leu a mensagem.

Quem confere se o canal existe é o coordenador, que tem todos os canais. Uma publicação em canal que não existe volta com `channel_not_found`.

## Inscrição

A inscrição é só do lado do bot. Ele assina o nome do canal no socket SUB, que continua aberto e recebe em uma thread (Python) ou goroutine (Go) separada. O ZeroMQ casa tópicos por prefixo, então quem assina `tech` também receberia `tech-12`. Por isso o bot compara o tópico recebido com a lista de canais que ele assinou e descarta o resto.

## Comportamento dos bots

1. Faz login. Se o servidor devolver erro ou não responder em 5 segundos, espera 2 segundos e tenta de novo.
2. Em loop:
   1. pede a lista de canais;
   2. se existirem menos de 5, cria um com nome sorteado de uma lista fixa, preferindo um que ainda não exista, e pede a lista de novo;
   3. se estiver inscrito em menos de 3 canais, assina mais um, sorteado entre os da lista em que ainda não está;
   4. sorteia um canal da lista e publica 10 mensagens nele, uma por segundo, com texto sorteado de uma lista fixa e o número da mensagem no final;
   5. pede o histórico desse canal (as 3 últimas publicações e o total), que o servidor responde com o que tem no próprio banco.

Os passos 2 e 3 rodam a cada volta, e não só quando o bot conecta, para o bot chegar a 5 canais e 3 inscrições mesmo quando um pedido cai num servidor que não tem os canais dos outros.

Quando uma resposta não chega, o bot fecha o socket REQ e abre outro, porque um REQ fica travado esperando a resposta que se perdeu.

## Relógio lógico

Todos os processos (bots, servidores e a referência) têm um contador que começa em 0 junto com o processo. O contador é incrementado antes de cada envio e vai no campo `clock`. Quando chega uma mensagem, o contador passa a valer o maior entre o dele e o da mensagem. Isso vale para as requisições e respostas, para as publicações e para as mensagens trocadas com a referência. Nos bots, a thread que recebe as publicações e a que faz as requisições usam o mesmo contador, protegido por um lock.

O campo `clock` aparece em todas as linhas de log.

## Serviço de referência

É um container só, em Go, que escuta na porta 5559 (REP) e só conversa com os servidores. Os servidores usam um socket REQ para falar com ele. Ele guarda tudo em memória:

- `rank`: devolve o rank do servidor. O primeiro nome que pede recebe 1, o seguinte 2, e assim por diante. Um nome que já pediu recebe o mesmo rank de novo, então um servidor que reinicia volta com o rank que tinha. Ranks não são reaproveitados.
- `list`: devolve o nome e o rank dos servidores que estão na lista, em ordem de rank.
- `heartbeat`: marca o servidor como vivo. Se o nome nunca pediu rank, a resposta é `unknown_server` e o servidor pede o rank de novo.

Um servidor que não manda heartbeat por mais do que `HEARTBEAT_TIMEOUT_S` segundos (30 no compose) sai da lista. Se ele mandar um heartbeat depois disso, volta para a lista com o mesmo rank.

## Heartbeat

Ao iniciar, o servidor pede o rank (tentando de novo a cada 2 segundos até a referência responder), manda um heartbeat e passa a mandar um a cada 10 requisições de clientes recebidas. A resposta só confirma que ele continua na lista. Depois de cada heartbeat o servidor pede a lista de servidores à referência e mostra no log.

## Eleição

Cada servidor tem um socket REP na porta 6000, só dentro da rede do compose, para falar com os outros. O endereço de cada um vem do nome dele na rede, por exemplo `tcp://server-go-1:6000`. Uso o algoritmo do valentão, com o rank dado pela referência:

1. O servidor que inicia, ou que não consegue falar com o coordenador, manda `election` para todos os servidores de rank maior da lista da referência.
2. Quem responde OK assume a eleição e faz o mesmo com os de rank maior que o dele. O servidor que pediu para de disputar e espera o anúncio.
3. Se ninguém de rank maior responde, o servidor é o coordenador e publica o próprio nome e rank no tópico `servers`. Todos os servidores assinam o tópico e guardam o nome em uma variável. Quem ouve o anúncio de um servidor de rank menor que o seu começa uma eleição para retomar o posto.

Na subida isso gera uma cascata: cada servidor se elege quando entra e o seguinte, de rank maior, assume, até o último a entrar ficar como coordenador. Quando o coordenador cai, quem notar primeiro (o pedido de relógio dá timeout em 2 segundos) começa a eleição e o servidor vivo de maior rank vira o coordenador.

## Relógio físico com Berkeley

A cada 15 mensagens trocadas (enviadas ou recebidas, de qualquer tipo) o servidor manda `clock` ao coordenador e recebe a hora correta. Para responder, o coordenador faz uma rodada de Berkeley: pergunta a hora (`time`) dos outros servidores ativos, desconta metade do RTT de cada resposta, tira a média das diferenças incluindo a dele e ajusta o próprio relógio para essa média. Depois devolve a hora já ajustada, junto com o tempo que levou para responder. O servidor que pediu desconta esse tempo e metade do resto do RTT e guarda a diferença como deslocamento, que é somado ao `timestamp` das mensagens dele. O coordenador também faz uma rodada ao se eleger e outra a cada 15 mensagens dele. Servidores que não respondem a `time` são pulados por 10 segundos.

A latência também define a janela de aceitação da rodada: quem responde com RTT acima de `BERKELEY_MAX_RTT_MS` (100 ms) ou cuja hora difere da do coordenador em mais de `BERKELEY_MAX_DIFF_MS` (1000 ms) fica fora da média e aparece no log como `fora da média`. Esses servidores não são marcados como fora do ar: só não entram naquela rodada e podem voltar na seguinte.

Como o Berkeley usa a média dos relógios aceitos na rodada, os servidores se acertam entre si, mas não com a hora real. Com os desvios simulados do compose (`CLOCK_SKEW_MS`) eles ficam a poucos milissegundos uns dos outros; o valor absoluto do grupo depende de quais relógios entraram nas rodadas da eleição, porque a janela descarta os que estavam muito longe. Se o próprio coordenador estiver fora do intervalo, ele descarta os outros e o grupo é puxado para o relógio dele. Os bots não sincronizam o relógio.

## Replicação

Método escolhido: réplica ativa, que na aula é a propagação da operação de atualização entre as réplicas, e não a cópia dos dados. A propagação é por push, iniciada pelo servidor que tem a operação, e quem fica para trás recupera por pull.

O problema do projeto é que o broker reparte as requisições em round-robin, então cada servidor só tinha as mensagens que atendeu. Para todos terem tudo, toda escrita (login, criação de canal e publicação) vira uma operação que passa por todos os servidores, na mesma ordem.

Como funciona:

1. O servidor que recebe a requisição do bot valida e manda a operação para o coordenador eleito na parte 4. Se ele mesmo for o coordenador, executa direto. Para isso existe o serviço `forward`.
2. O coordenador confere as regras (canal duplicado, canal inexistente), dá à operação o próximo número da sequência, grava no `oplog` e nas coleções, e manda a operação a todos os outros servidores ativos com `replicate`. Ele só responde ao servidor de origem depois disso, e o bot só recebe `ok` depois que a operação está em todos os servidores que responderam.
3. Quem recebe `replicate` aplica a operação se o número for o seguinte ao último que tem. Se já tinha, ignora. Se falta alguma antes, responde `gap` com o último número que tem, e o coordenador reenvia as que faltam em um lote.
4. Leituras (`list` e `history`) são respondidas por qualquer servidor com o que tem no próprio banco.

Servidor que entra ou volta: ao saber quem é o coordenador, ele pede com `catchup` tudo o que tem número maior que o último dele, em lotes de 200 (é o pull da aula). Um servidor que sobe com o banco vazio fica igual aos outros assim. Um coordenador recém-eleito faz o mesmo com todos os outros servidores antes de assumir, para não numerar operações em cima de um log incompleto.

Consistência: como só o coordenador numera e todos aplicam na ordem dos números, as escritas têm uma ordem global única, o que dá consistência sequencial para as escritas. As leituras (`list` e `history`) são respondidas pelo servidor que o round-robin escolher, com o que ele tem no próprio banco, então um servidor que ficou fora do ar devolve dados antigos até o `catchup` alcançar os outros: para esses servidores a consistência é eventual, e leitura monotônica não é garantida (um bot pode ler algo mais novo em um servidor e depois algo mais antigo em outro). Servidores que não respondem dentro de 1 segundo são pulados por 10 segundos e se recuperam pelo `gap` ou pelo `catchup`, então eles ficam atrasados por um tempo e depois alcançam os outros.

Mudanças em relação ao método da aula:

- Na réplica ativa pura cada réplica executa a operação por conta própria, mas aqui duas requisições chegam a servidores diferentes em ordens diferentes e canais com o mesmo nome seriam criados duas vezes. Por isso o coordenador da parte 4 também é quem numera as operações, e as réplicas só aplicam.
- O servidor que atende o bot não grava a operação antes do coordenador. Ele só publica a mensagem no tópico do canal depois do `ok`.
- O push sozinho não cobre servidor que estava fora do ar, então acrescentei o `catchup` por pull.

O coordenador aplica uma operação por vez, com um lock. Cada servidor atende os pedidos dos outros em 8 threads (goroutines no Go) atrás de um ROUTER, porque o `forward` fica esperando a replicação terminar e travaria os outros pedidos se houvesse uma thread só.

## O que ainda não está resolvido

Durante a eleição, ou logo depois de o coordenador cair, as escritas voltam com `no_coordinator` ou `coordinator_unavailable` até o novo coordenador ser anunciado. O bot só tenta de novo na rodada seguinte.

Se o coordenador numerar uma operação e cair antes de enviá-la a todos, essa operação pode existir só em alguns servidores. O bot não recebeu `ok` nesse caso, e o novo coordenador junta o que os outros têm antes de assumir, mas se dois servidores se acharem coordenadores ao mesmo tempo, por exemplo no começo da eleição em cascata, eles podem numerar operações diferentes com o mesmo número e uma delas é descartada em quem já tinha aplicado a outra. Não tratei esse caso.

O Pub/Sub não guarda nem reentrega nada. Um bot só recebe o que for publicado depois de ele assinar o canal, e um bot que entra depois não vê o que já foi publicado, mas pode pedir o histórico com `history`.

Leituras não são monotônicas. `list` e `history` são respondidos pelo servidor sorteado pelo round-robin, e um servidor que estava fora do ar responde com dados antigos até terminar o `catchup`. Assim, o mesmo bot pode ler uma lista ou um histórico mais novo em um servidor e, na requisição seguinte, algo mais antigo em outro. As escritas estão em ordem global única, mas a leitura em servidor atrasado é eventual.

O heartbeat é por número de requisições, não por tempo. Se nenhum bot estiver mandando requisições, os servidores não mandam heartbeat e a referência os tira da lista depois do timeout. Com os 4 bots do compose cada servidor recebe uma requisição por segundo ou mais e o timeout de 30 segundos não é atingido.

A referência guarda ranks e a lista só em memória. Se ela reiniciar, cada servidor recebe `unknown_server` no próximo heartbeat e pede o rank de novo, mas os ranks são atribuídos outra vez na ordem em que os servidores aparecerem.
