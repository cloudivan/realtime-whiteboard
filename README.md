# Replicated whiteboard (3 nodes)

Collaborative board (rectangles + freehand lines) served by 3 equal nodes. Any client can talk to any node; all nodes converge to an identical state, survive a node going down, and keep latency to a single hop.

## How to run

```bash
docker compose up --build          # node1:8001, node2:8002, node3:8003
```

Open http://localhost:8001, http://localhost:8002, http://localhost:8003 in different tabs. Drawing in one tab shows up in all of them.

Load, chaos and convergence test (50 bots, 10 ops/s each, kills `node2` at 60s for 30s):

```bash
cd test && npm install && node bots.js
node bots.js BOTS=20 DURATION=60 NO_KILL=1        # env vars work too: BOTS=20 node bots.js
```

Options: `HOST` (localhost), `BOTS` (50), `DURATION` s (180), `KILL_AT` s (60), `KILL_FOR` s (30), `KILL_NODE` (node2), `RATE` ops/s per bot (10), `NO_KILL=1`.
At the end it checks that all three `/state` bodies have the same sha256, that no created shape or line is missing, that the `ops` count matches the number of unique ops sent, and that each shape's final position equals the max-ts move. It also prints echo and cross-node delivery latency (avg/p50/p99). It exits with code 0 on PASS.

Full reset (wipes the persisted logs): `docker compose down -v`.

## Protocol

WebSocket at `/` on each port.

Client → server:
```json
{"type":"create_shape","opId":"<uuid>","id":"r1","x":10,"y":20,"w":100,"h":60,"color":"#4af"}
{"type":"move_shape","opId":"<uuid>","id":"r1","x":200,"y":80}
{"type":"draw_line","opId":"<uuid>","id":"l1","points":[[0,0],[5,7]],"color":"#000","width":2}
```

Server → client: first `{"type":"snapshot","state":{...}}`, then every applied op from any client on any node, including the sender's own echo. Each op is the client message plus `"ts":[hlc, originId]`, with `opId` kept. The echo is the ack.

`GET /state` →
```json
{"shapes":[{"id","x","y","w","h","color","ts"}],   // sorted by id
 "lines":[{"id","points","color","width","ts"}],     // sorted by ts, then id
 "ops": 12345}                                       // number of unique ops applied
```
A shape shows up only after its `create_shape` has arrived.

## Architecture

```
   browser / bot          browser / bot          browser / bot
        │  ws                   │  ws                   │  ws
        ▼  (on disconnect: 8001 → 8002 → 8003, resend unacked)
  ┌───────────┐  peer ws  ┌───────────┐  peer ws  ┌───────────┐
  │  node1    │◄─────────►│  node2    │◄─────────►│  node3    │
  │  :8001    │           │  :8002    │           │  :8003    │
  └─────┬─────┘◄─────────────────────────────────►└─────┬─────┘
        │            full mesh: live op push +          │
        │            version vectors every 500 ms       │
   /data/ops.log           /data/ops.log           /data/ops.log
   (volume data1)          (volume data2)          (volume data3)
```

- **Internal replication ports:** nodes talk to each other on ports 9001-9003 (`/peer`), which are only reachable inside the Docker network and are not published to the host. Clients can only reach 8001-8003, and every replicated message is validated before it is applied.
- **Op flow:** a client sends an op to its node. The node gives it `ts=[hlc, originId]` and a per-origin seq, applies it, appends it to `ops.log`, broadcasts it to its clients and pushes it to its peers right away. Peers apply it and broadcast it to their own clients.
- **State:** each shape position is an LWW register keyed by `ts`. Lines are a grow-only set keyed by id. Ops are deduplicated by `opId`.
- **Anti-entropy:** every 500 ms nodes swap version vectors (`origin → max seq`) and send each other the missing ops for every origin. This includes ops from origins that are offline right now, so any surviving node can repair any other node.
- **Restart:** a node replays `ops.log`, picks a fresh `originId = NODE_ID-<start time>` (seqs never collide), and catches up through anti-entropy.

## Защита архитектуры (шпаргалка, ~5 минут)

**Конфликты**
- Модель: CRDT. Позиция прямоугольника хранится как LWW-регистр (Last-Writer-Wins). Линии хранятся как append-only множество по id, поэтому линии конфликтовать не могут.
- Каждой операции узел, который получил её от клиента, присваивает метку `ts = [hlc, originId]`. Здесь `hlc = max(Date.now(), lastSeen + 1)`: это гибридные логические часы. Они монотонны и не отстают от уже виденных меток.
- Метки сравниваются сначала по `hlc`, а при равенстве по строке `originId`. Это **строгий полный порядок**: двух одинаковых меток не бывает, поэтому "ничьих" нет.
- Применение операции сводится к "берём max по ts". Max коммутативен, ассоциативен и идемпотентен, а повторы дополнительно отсекаются по `opId`. Поэтому порядок доставки и дубликаты не важны.
- Отсюда сходимость: узлы, получившие одно и то же множество операций, приходят к **байт-в-байт одинаковому** состоянию. Тест это проверяет: sha256 трёх `/state` совпадают.

**Падение узла**
- Клиент при разрыве через 200 мс переподключается к следующему порту (8001 → 8002 → 8003) и **повторно шлёт все неподтверждённые операции** с теми же `opId`. Подтверждением служит эхо. Дубли безопасны благодаря идемпотентности.
- Операции, которые узел успел разослать пирам (рассылка мгновенная), остаются у выживших узлов. Остальные клиент перешлёт сам.
- Каждая операция пишется в `/data/ops.log`, который лежит на docker volume и переживает рестарт контейнера.
- После рестарта узел получает новый `originId` (новая "эпоха"), поэтому его seq не пересекаются с seq до падения.
- При возвращении узел сначала проигрывает лог с диска, затем по anti-entropy (обмен version vectors раз в 500 мс) забирает недостающие операции **любого** origin у **любого** живого пира. Через секунду он полностью догнан.

**Задержка**
- Операция рассылается сразу, без батчинга, без сжатия и без ожидания кворума: локальным клиентам немедленно, пирам по уже открытому ws.
- Пиры связаны постоянной полной mesh-сетью, поэтому до любого клиента ровно один межузловой хоп и нет затрат на handshake.
- Консенсуса и лидера нет (в отличие от Raft/Paxos), поэтому запись никогда не ждёт других узлов. Задержку и eventual consistency тест измеряет как p50/p99 доставки до бота на другом узле.

**Компромиссы (осознанные)**
- LWW **может потерять одно из двух конкурентных перемещений** одного прямоугольника: побеждает более поздний `ts`. Так задумано: в итоге все видят одно и то же, и это последнее по времени действие.
- `hlc` опирается на `Date.now()`. Все узлы работают на одном хосте, поэтому часы фактически общие. При сильном расхождении часов на разных машинах "последним" окажется узел с часами, ушедшими вперёд, но сходимость сохранится.
- Лог операций в памяти и на диске только растёт: нет компакции, снапшотов и GC. Для демо это нормально, в проде нужны снапшот плюс обрезка лога по минимальному version vector.
- Удаления нет. Для него понадобились бы tombstones (LWW-флаг deleted).
