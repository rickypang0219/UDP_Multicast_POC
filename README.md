# Feed fan-out PoC

Minimal Rust example inspired by mini-redis's explicit connection handler style.
Uses ordinary threads for blocking sockets and DB I/O: no async runtime, trait
hierarchies, custom unsafe queue, or generic broker framework.

```text
UDP JSON (:9000, optional multicast membership)
                     |
              single dispatcher
               /            
      per-user SPSC          logging SPSC
           |                       |
      TCP NDJSON              DB worker
      (:9001)              HTTP ILP batches
                               QuestDB
```

One producer and one consumer own each `rtrb` lock-free ring buffer. The bounded
standard-library channel is only for subscriber registration, outside the event
queues. Event is a Copy struct with typed Feed, sequence, integer price and Unix
nanosecond timestamp. No per-subscriber payload allocation in dispatch.

## Run

Requires a current stable Rust toolchain and QuestDB with HTTP ILP enabled.
Start a local QuestDB instance separately (default HTTP port 9000). UDP ingestion
also uses 9000, which does not conflict with TCP HTTP on the same machine.

```sh
cargo run --release
# In a second terminal; keep stdin open after sending SUB:
nc 127.0.0.1 9001
SUB trades quotes
```

Wait for `OK`, then send a datagram from a third terminal:

```sh
python3 - <<'PY'
import json, socket, time
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for seq in range(10):
    event = dict(feed='trades' if seq % 2 else 'quotes', sequence=seq,
                 price_micros=123450000, ts_ns=time.time_ns())
    sock.sendto(json.dumps(event).encode(), ('127.0.0.1', 9000))
    time.sleep(.01)
PY
```

Subscribers receive one JSON event per line. Query QuestDB:

```sql
SELECT * FROM events ORDER BY timestamp DESC;
```

The table is auto-created by ILP when the server permits it. `feed` is SYMBOL;
`sequence` and `price_micros` are LONG. Sequences must fit a signed 64-bit integer.
A different endpoint can be configured with `QUESTDB_URL`, e.g.
`http://localhost:9000/write?precision=n`. Authentication is outside this PoC.

Actual multicast ingress:

```sh
MULTICAST_GROUP=239.1.1.1 cargo run --release
```

Send the same JSON datagrams to `239.1.1.1:9000`. Membership uses the default
interface; multi-interface hosts may need an explicit interface address in code.
Fan-out to users is application-level TCP unicast, not downstream IP multicast.
Feed choices are `trades` and `quotes`; each connection sends one SUB command.

## Deliberate PoC tradeoffs

- Each ring holds 4096 events. Full rings drop the incoming event independently;
  a slow DB or user does not block other users. Totals print every five seconds.
- UDP itself can lose/reorder events. Sequence numbers expose gaps to consumers;
  there is no replay, ordering repair, acknowledgement or guaranteed delivery.
- DB batches contain at most 256 events and collect for at most 100 ms. Failed
  batches retry every second with a three-second request timeout. Ambiguous HTTP
  failures can duplicate rows. Permanent errors also retry and need intervention.
- Logs cover all valid ingress events, regardless of subscriptions. Fan-out and
  persistence are independent: receiving an event does not prove it was stored.
- At most 64 active connection handlers, including handshakes. Each has one OS
  thread. Idle consumers poll with short sleeps; this is lightweight enough for
  a small PoC, not a claim of microsecond latency or thousands of users.
- Subscription registration is bounded. OK means queued for registration; the
  dispatcher applies registrations at the next receive iteration. No history.
- Shutdown is process termination; pending events are not flushed. No WAL,
  durable recovery, TLS, user authentication or feed authorization. Feed selection
  is not an access-control system. Run on a trusted development network only.
- UDP payloads must fit a datagram. Unknown event fields, unknown feeds, negative
  timestamps and malformed packets are counted and discarded.

## Verification

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

Tests cover command validation and queue overflow/FIFO behavior. The three-terminal
example checks fan-out and DB ingestion manually. This workspace had no Rust
compiler and its download proxy was unreachable, so compilation and live QuestDB
integration have **not** been verified here.
