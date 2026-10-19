# Implementation

How pg_redis works, end to end: the process model, the three storage backends,
the shared-memory layout, pub/sub, WATCH, the limits every one of those imposes,
and how to test and benchmark a change to any of it.

This is the single reference for internals. The pages beside it are for using
the extension rather than changing it — [Installation](installation.md),
[Configuration](configuration.md), [Commands](commands.md) and the
[Command coverage](command-coverage.md) matrix, which is machine-checked
against the parser by a unit test and so has to stay a page of its own.

---

## 1. Architecture

### Process model

`shared_preload_libraries = 'pg_redis'` registers a pool of PostgreSQL
background workers (`redis.workers`, default 4). Each one opens the Redis port
(`redis.port`, default 6379) with `SO_REUSEPORT`, so the kernel spreads incoming
connections across the pool and no worker is a listener bottleneck.

A worker is two things at once:

- **Connection threads.** One per client. They own the socket, parse RESP2, and
  write replies. They are ordinary OS threads, not PostgreSQL backends.
- **A dispatcher** on the worker's main thread — the only thread with a valid
  `MyProc`, and therefore the only one allowed to touch PostgreSQL LWLocks or
  SPI.

Every data command crosses between them twice: connection thread → dispatcher
(`DispatchMsg`), dispatcher → connection thread (the reply). Those two mpsc hops
are the throughput ceiling; see [Performance](#8-performance).

Connection-local commands — `PING`, `SELECT`, `AUTH`, `MULTI`/`EXEC` queueing —
never reach the dispatcher and cost nothing but the parse.

### Command path

```
socket ─> resp::parse ─> Command::parse ─┬─> execute_mem ─┐  (memory policies)
                                         └─> execute_spi ─┤  (table policies)
                                                          └─> store::run ─> MemStore | SqlStore
```

`Command` is a single enum covering every supported command. Parsing is
separated from execution so the parser can be unit-tested without a PostgreSQL
instance, and so size limits can be checked *before* anything is written — see
[Limits](#7-limits). Both paths hand the command to `store::run`, where each
data command is written once against the database's backend; see
[Commands written once](STORAGE.md#commands-written-once-phase-5).

### The 16 databases

Databases mirror Redis's model. Each has a storage policy — `memory`,
`memory+snapshot` or `logged` — changeable while the server runs,
data and all ([Storage policies](STORAGE.md)). The defaults are two contiguous
halves:

| DB | Name | Default backend | Durability |
|---|---|---|---|
| 0–7 | `cache` (0) | shared memory | Lost on restart |
| 8–15 | `durable` (8) | WAL-logged tables | Survives crashes |

`SELECT` accepts the numbers and the two names. `redis.default_db` (default 0)
chooses where new connections start. A database in memory occupies one of the
sixteen shared-memory slots — not necessarily the slot of its own number.

### Batching and transaction isolation

The dispatcher coalesces up to `redis.batch_size` (default 64) commands from
*unrelated* connections into one SPI transaction, which is what makes the SPI
path bearable at all. That creates a hazard: one client's failing command must
not roll back another client's write.

Each command therefore runs in its own subtransaction. `worker.rs` calls
PostgreSQL's subtransaction primitives directly rather than issuing `SAVEPOINT`
/ `ROLLBACK TO` / `RELEASE` as SQL — three statements parsed, planned and
executed through SPI per command, which for a plain `SET` cost more than the
`SET`. It mirrors the save/restore dance PL/pgSQL performs around its
`EXCEPTION` blocks.

---

## 2. Storage backends

Two: the shared-memory engine, and WAL-logged tables reached through SPI. A
database's policy picks one — `memory` and `memory+snapshot` the first,
`logged` the second ([Storage policies](STORAGE.md)).

| | `memory` (default 0–7) | `logged` (default 8–15) |
|---|---|---|
| Storage | A shared-memory dictionary per database | WAL-logged tables |
| Transaction | None | `BEGIN`/`COMMIT` per batch |
| SPI overhead | None | Yes |
| WAL | No (`memory+snapshot`: the snapshot) | Yes |
| Survives crash | No (`memory+snapshot`: to the last snapshot) | Yes |
| Replication | No | Yes |
| SQL-visible | No — `SELECT * FROM redis.kv_0` returns nothing | Yes |
| Max data | `redis.maxmemory` (256 MB), backed as it fills | Disk |
| Max value | 512 MiB, as Redis | ~1 GB (TOAST) |

### SQL schema

The tables live in the `redis` schema, one per data type per database, every
one WAL-logged. A database's tables stay empty while its data is in memory.

```sql
CREATE SCHEMA redis;

-- Key-value (pattern repeats for db 0–15)
CREATE TABLE redis.kv_0 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);

-- Hash (same pattern)
CREATE TABLE redis.hash_0 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
```

Every column is `BYTEA`, never `TEXT`. Redis keys, values, fields and members
are arbitrary byte strings, and `TEXT` cannot hold a NUL at all. Two
consequences when querying directly:

```sql
-- Cast to read.
SELECT convert_from(key, 'UTF8'), convert_from(value, 'UTF8') FROM redis.kv_8;

-- Cast the other way to compare (a plain literal is coerced, so it also works).
SELECT value FROM redis.kv_8 WHERE key = convert_to('mykey', 'UTF8');
```

Ordering is bytewise, which is what Redis means by lexicographic — `ZRANGEBYLEX`
and `ORDER BY member` agree with Redis rather than with a database collation.

Every `kv_*` table carries a partial index on `expires_at` for the SQL expiry
sweep; a unit test asserts it exists on all sixteen.

---

## 3. The shared-memory backend

`src/memdb` holds the data and `src/shm.rs` the memory it lives in.
[Storage policies](STORAGE.md#growable-shared-memory-phase-6) has the design;
this is how it is put together.

### One database, one dictionary

A `memdb::Db` is a dictionary from key to `Entry { value, deadline }`, the
value one of six variants — a string's bytes, a stream's serialised bytes, a
list, a hash, a set or a sorted set. One key holds one value, so `WRONGTYPE`
is a match on the variant and replacing a key of another type (`SET` over a
list) is an ordinary insert.

| Type | Structure |
|---|---|
| Keyspace, hash, set | `dict::Dict`: entries in a dense `Vec`, found through a hashbrown `HashTable` of positions. Dense storage gives a random entry in O(1) — `RANDOMKEY`, `SRANDMEMBER`, `SPOP` and eviction sampling — and removal swaps the last entry into the hole. Keys hash with SipHash under a per-postmaster random key, so a client cannot aim keys at one bucket. |
| List | `deque::Deque`: two stacks back to back, the head reversed. Pushes and pops at either end are O(1) amortised, indexing is O(1). |
| Sorted set | `zset::ZSet`: a `Dict` from member to score beside the members in `(score, member)` order, kept as sorted blocks of up to 256. An insert moves at most one block; a rank or a score bound costs a walk over the blocks plus a binary search in one. |

Every collection is generic over its allocator (`allocator_api2`). The server
gives them `shm::Shm`; the unit tests the ordinary heap, which is how the
whole engine — down to full command lines through `store::run` — is tested
without PostgreSQL.

### The arena

`shm::startup` takes one region of the main shared segment: a header (the
hash key, the limit, the use counters, one LWLock per database and one for
the allocator, and the sixteen `Db`s) followed by the heap, which talc
manages. `Shm` is a zero-sized handle: allocating takes the allocator lock,
asks talc, and counts the bytes.

The region is `redis.maxmemory` plus headroom, reserved when PostgreSQL
starts. The operating system backs a page of it only when it is first
written, so the arena costs what the data does. Freed memory goes back to
talc for the next allocation, not to the operating system — resident size
stays at its high-water mark, as Redis's does under jemalloc. With
`huge_pages` in use, the whole region is huge pages from the start.

### What makes it sound

The collections hold plain pointers into the arena, which is safe only
because of three things `shm.rs` guarantees, and the only `unsafe` in the
engine is there:

- **One address everywhere.** The main segment is mapped by the postmaster
  before it forks, so every process sees it at the same address. The header
  records where it was built and `shm::attach` refuses to start a worker that
  finds it anywhere else.
- **One holder at a time.** A database is reached only through
  `shm::with_db`, under its exclusive LWLock, so the `&mut Db` it hands out is
  the only reference in any process. A thread-local flag refuses a nested
  `with_db` — an LWLock is not re-entrant, and the second acquire would wait
  forever. The allocator's lock is innermost: nothing holding it takes
  another.
- **Failure unwinds.** When talc has no block to give, `Shm` unwinds with an
  `OutOfMemory` payload rather than returning an error, which the
  collections' infallible methods would turn into an abort of the whole
  backend. `with_db` catches the unwind and the command answers `OOM`. The
  collections are panic-safe, so a write cut short leaves the database
  consistent, and the lock is released on the way out like any other.

### One lock per command

`Command::execute_mem` checks the limit, then runs the type check and the
command inside one `with_db`. Nothing another worker does can land between
the type check and the write, or between the two halves of an
`LMOVE`. What `store::run` does not know runs after the lock is released —
composites re-enter `execute_mem` once per sub-command.

### Expiry

An entry's deadline is microseconds since the epoch, 0 for none. Any access
to a key past its deadline removes it on the way (`Db::entry`), and the
worker's once-a-second sweep removes the rest, reporting their names when
keyspace notifications or tracking want them.

### The limit and eviction

Before a write that could grow memory — every write but the removals, Redis's
`denyoom` — `shm::make_room` compares the arena's use with the limit. Under
it, the command runs, and may overshoot into the headroom. Over it:

| `redis.maxmemory_policy` | What happens |
|---|---|
| `noeviction` (default, as in Redis) | The write is refused with `OOM command not allowed when used memory > 'maxmemory'`. Reads and removals still run. |
| `allkeys-random` | Random keys are evicted, collections whole, across the memory databases until use is under the limit. |
| `volatile-ttl` | Of a sample of keys, the one with a deadline that comes soonest is evicted, until use is under the limit or no key has a deadline. |

`CONFIG SET maxmemory` changes the limit for every worker, never past what
the arena was sized for; `CONFIG SET maxmemory-policy` changes the policy.

---

## 4. Pub/Sub

Pub/sub is independent of storage policies and always allocated — it needs
only `shared_preload_libraries`.

Each subscribing connection claims one of 256 slots. A slot holds a ring buffer
of 256 messages and the connection's subscriptions (16 channels and 16 patterns).
`PUBLISH` writes into the ring of every matching slot and bumps that slot's tail
counter.

Subscribers **wait on the tail counter**, not on the socket. A subscriber that
discovered messages by letting a 5 ms socket read time out would cost ~200
wakeups per second while idle and still delay delivery by up to 5 ms. The futex
lives in PostgreSQL's shared memory segment, so the shared (not `PRIVATE`)
operations are required — waiter and waker are different processes. The socket
poll that remains backs off from 5 ms to 250 ms while a connection stays silent;
it bounds only how long a *command* from a quiet subscriber sits unread, never
delivery latency.

Route lookups short-circuit on an atomic counter, so an installation with no
routes configured pays nothing.

### Size limits

Everything runs through fixed-size slots, so these are hard caps:

| | Limit | Over the limit |
|---|---|---|
| Channel / pattern name | 255 bytes | `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH` error |
| Message payload | 512 bytes | `PUBLISH` errors |
| Concurrent subscriber connections | 256 | `SUBSCRIBE` returns "max pub/sub subscribers reached" |
| Channels + patterns per connection | 16 each | Further subscriptions ignored |
| Queued undelivered messages per subscriber | 256 | Further messages dropped for that subscriber |
| Active routes | 64 | — |

Names and payloads are **rejected, not truncated**: a truncated subscription
would never match a publish to the same name, leaving the client waiting on a
channel it believed it was subscribed to.

The payload cap is deliberately *not* memory mode's value cap. Every slot in the
ring reserves its payload whether or not anyone publishes, so tracking a 64 KiB
cap would cost the ring 128× what it costs now. The slot table is ~66 MiB,
allocated at server start whether or not pub/sub is used.

### Table routing

Any `PUBLISH` can additionally be INSERTed into a PostgreSQL table — enough for
[Supabase Realtime broadcast from database](https://supabase.com/docs/guides/realtime/broadcast)
or any trigger-based integration. The extension is decoupled from Supabase; it
just INSERTs rows.

```sql
SELECT redis.create_pubsub_table('public', 'chat_messages');
SELECT redis.route_publish('chat', 'public', 'chat_messages');
-- redis-cli> PUBLISH chat "hello"
SELECT channel, payload, inserted_at FROM public.chat_messages;
```

| Function | Description |
|---|---|
| `redis.create_pubsub_table(schema, table)` | Create a routing target with the required columns |
| `redis.route_publish(channel, schema, table)` | Route `PUBLISH` on `channel` into `schema.table` |
| `redis.unroute_publish(channel)` | Remove the route |

Routes live in `redis.pubsub_routes`, are loaded into shared memory at startup,
and survive restart. The INSERT is dispatched fire-and-forget *after* in-memory
delivery completes and after the `PUBLISH` reply is sent.

Bring your own table if you prefer — it needs `channel BYTEA` and
`payload BYTEA`. `BYTEA` is required, not recommended: channels and payloads are
arbitrary bytes. Because the insert is fire-and-forget, a routed insert into a
wrongly-typed table fails *after* `PUBLISH` has already succeeded, and the error
goes to the PostgreSQL log rather than to the client. Check there if routed rows
stop appearing.

---

## 5. WATCH

`WATCH` snapshots a write-version counter per key and `EXEC` aborts if it moved.

The counters live in shared memory rather than per worker because of
`SO_REUSEPORT`: a client's `WATCH` is served by whichever worker accepted its
connection, while the conflicting write may be dispatched through any other
worker. Per-process counters reported "no conflict" for writes the process never
saw, and `EXEC` committed when it should have aborted.

Keys are hashed (FNV-1a over the database number followed by the key bytes) into
a fixed table of 65,536 `AtomicU64` counters — 512 KiB — rather than stored. That
keeps the structure allocation-free, lock-free and fixed-size: no eviction
policy, and no growth a client can drive by writing unique keys. The cost is that
two keys can share a counter, which makes an `EXEC` abort when it strictly did
not have to. **That direction is safe; the reverse would not be.**

---

## 6. The RESP layer

RESP2 by default; `HELLO 3` switches a connection to RESP3.

RESP3 changes how a reply is framed, not what it holds, so commands keep
answering in one shape and `resp3.rs` reframes the reply for a RESP3 client,
command by command, following Redis 7.2's reply code. `HGETALL` and
`CONFIG GET` become maps; `SMEMBERS`, `SINTER`, `SUNION`, `SDIFF` and
`SPOP` with a count become sets; scores become doubles, and `WITHSCORES`
lists become `[member, score]` pairs; `XREAD` becomes a map from stream to
entries; `INFO` and `CLIENT LIST` become verbatim text; every nil is the one
RESP3 null. Pub/sub messages are push frames. A command the table does not
name is framed the same in both protocols.

| Bound | Value | Why |
|---|---|---|
| `MAX_BULK_LEN` | 512 MiB | Matches Redis's `proto-max-bulk-len` |
| `MAX_ARRAY_LEN` | 65,536 elements | |
| `MAX_INLINE_LEN` | 8 KiB | Inline (non-array) command form |
| `BULK_CHUNK` | 64 KiB | Read granularity |
| `QUEUE_LIMIT` | 10,000 | Commands queued in one `MULTI` |

A declared bulk length does not commit memory: the parser grows the buffer as
bytes actually arrive, so a client announcing a 512 MiB bulk string and then
stalling holds only what it sent, not what it promised.

Error replies carry their own code when they have one. `write_error` prefixes
`ERR` only for messages that do not already start with a code from
`ERROR_CODES`, so an OOM goes out as `-OOM ...` — which is what clients match on.

### Scripting

`EVAL` and `FCALL` run Lua 5.1 on the dispatcher, whole, so a script is atomic
against every other command its worker serves. `redis.call` builds an
ordinary command and routes it like a composite command's sub-commands, so one
script reads and writes both storage halves. The environment is Redis's:
`cjson`, `cmsgpack`, `struct` and `bit` are Redis's own C sources, compiled by
`build.rs` from `vendor/redis-lua/`; replies convert to Lua and back by Redis's
rules; errors carry the script and line as Redis reports them. Redis's Lua is
patched to make tables read-only; here every protected table is an empty proxy
whose metatable forwards reads and refuses writes, so the same "Attempt to
modify a readonly table" comes back without patching Lua.

Each worker has its own Lua states, so what must be shared lives in SQL: the
script cache in `redis.scripts` (unlogged — it is a cache, as in Redis) and the
function libraries in `redis.functions` (logged — Redis persists them in its
RDB). A change bumps a generation counter in shared memory once its
transaction commits, and each worker rebuilds its copy when it sees the
counter move. A script that runs past `busy-reply-threshold` makes every other
client get `BUSY`, on every worker, until it ends or `SCRIPT KILL` /
`FUNCTION KILL` stops it.

### Known differences from Redis

- **Atomicity is per worker.** A script, a `MULTI`, or a composite command
  holds its own dispatcher; on the memory half its sub-commands take their own
  locks, so another worker's commands can interleave with them. Redis is one
  thread, so nothing interleaves.
- **Blocked clients are served before the writer replies, within a bound.**
  Redis serves the clients a write unblocks inside that write. Here they live
  on other threads and workers, so a write that touches a key someone is
  blocked on waits — up to 500 ms — until each of them has tried against it,
  and a served client that moves an element does the same for its
  destination. Chains of `BLMOVE`s, and cycles of them, complete before the
  original `RPUSH` returns, as in Redis; a blocked client stuck for longer
  than the bound is left to finish on its own. Writes made inside a script do
  not wait.
- **No ACL users, replicas or cluster mode.** `ACL`, `WAIT`, `REPLICAOF` and
  `CLUSTER` answer as a single-user, stand-alone server would.
- **Client tracking keeps its table in shared memory, 16,384 (key, client)
  pairs across every client.** Past that — or past `tracking-table-max-keys`
  when set lower — a tracked key picked at random is invalidated to make
  room, as Redis does at its own limit. Keys longer than 128 bytes are
  tracked by hash only; when one is picked to make room, the client holding
  it gets a flush (the "every key" invalidation) instead of the key's name.
  A RESP3 client that invalidations are redirected to receives them as push
  messages with no subscription of its own; the connection that sets up the
  redirect hands it a pub/sub slot to receive them on.
- **Eviction is silent.** A key evicted under `redis.maxmemory_policy` sends
  no `evicted` notification and no client-tracking invalidation. Keyspace
  notifications otherwise follow `notify-keyspace-events` event for
  event, `expired` included for both lazy and background expiry.
- **No access clock and no dirty counter.** `OBJECT IDLETIME` and `OBJECT
  FREQ` answer nil, because keeping a last-access time would turn every read
  on the durable half into a write; `rdb_changes_since_last_save` stays 0.
  `SAVE`, `BGSAVE` and `LASTSAVE` act on `memory+snapshot` databases' snapshots
  ([Storage policies](STORAGE.md#snapshots-phase-4)).
- **Small hashes and sets keep insertion order only until a removal.** Redis
  returns a listpack-encoded hash's fields — and a small set's members — in
  the order they were added, and keeps that order across deletions. The
  memory half keeps insertion order too, but a removal moves the last entry
  into the hole; the durable half returns the table's order. Redis documents
  both as unordered, but a client can observe the difference.

---

## 7. Limits

Everything pg_redis bounds that Redis does not, in one place.

| | Limit | Where it comes from |
|---|---|---|
| Memory-mode data | `redis.maxmemory` | The arena, sized at start ([§3](#the-limit-and-eviction)) |
| Channel / pattern | 255 bytes | Fixed pub/sub slot |
| Pub/sub payload | 512 bytes | Fixed ring slot |
| Bulk string on the wire | 512 MiB | RESP parser, matches Redis |

Keys, fields, members and values are as long as Redis allows on both halves:
512 MiB a string, at the parser. The durable half is otherwise bounded by
disk, and by PostgreSQL's ~1 GB per-field TOAST limit.

---

## 8. Performance

Requests per second, higher is better. `redis-benchmark`, 50 clients, 64-byte
values over 100,000 random keys, median of 5 runs. Each server runs alone on the
same 4 pinned CPUs with the load generator on 6 others, Docker on an Apple M5 Pro,
PostgreSQL 17 with `redis.workers=4`. `mise run bench-suite` reproduces it and
writes the full report, value sizes, client counts, pipelining and a memtier mix
included, to `docker/bench/results/`.

| Command | Redis 7.2 | Redis 7.2, AOF everysec | memory | memory+snapshot | logged |
|---------|----------:|------------------------:|-------:|----------------:|-------:|
| SET     | 225,734 | 219,539 | 182,315 | 184,843 |   6,374 |
| GET     | 229,885 | 227,273 | 187,970 | 188,501 | 121,065 |
| INCR    | 228,311 | 223,714 | 187,617 | 188,324 |   4,609 |
| LPUSH   | 217,865 | 211,640 | 186,916 | 187,793 |   3,004 |
| LPOP    | 208,117 | 215,750 | 187,793 | 189,394 |   1,323 |
| SADD    | 226,501 | 226,244 | 182,482 | 186,741 |   8,486 |
| SPOP    | 228,311 | 227,790 | 192,123 | 190,840 |  18,018 |
| HSET    | 225,989 | 221,484 | 179,211 | 178,094 |   5,890 |
| ZADD    | 226,757 | 216,920 | 175,593 | 175,439 |   6,368 |
| ZPOPMIN | 227,015 | 229,095 | 192,493 | 191,571 |   3,308 |
| MSET    | 196,657 | 146,199 | 100,959 | 103,520 |   3,391 |
| memtier 1:10, 256 B | 360,804 | 347,290 | 245,948 | 241,851 | 14,581 |

`logged` writes commit to the WAL, so they measure the disk more than this
code: on Docker Desktop the same `SET` measured 6,374 and 15,309 per second in
two groups of one run. Compare `logged` writes across runs only by their order
of magnitude.

`redis-benchmark` writes 3-byte values; `mise run bench-value-sizes` sweeps
3/63/64/65/200/512/4096/65536 bytes for larger ones. `mise run bench-high-write` (8 workers, `batch_size=256`) spreads
connections across more dispatchers.

### Pub/Sub

One publisher, varying subscriber counts. **recv/s** is total deliveries across
all subscribers.

| Subscribers | Redis 7 pub/s | Redis 7 recv/s | pg_redis pub/s | pg_redis recv/s |
|---|---|---|---|---|
| 1  | 2,509 | 2,506  | 9,451  | 9,172   |
| 4  | 2,359 | 9,433  | 12,297 | 38,086  |
| 16 | 3,865 | 61,762 | 49,810 | 348,682 |
| 32 | 2,896 | 92,516 | 8,993  | 237,891 |

recv/s scales linearly — one `PUBLISH` fans to N ring buffers in parallel.
Routing to a table roughly halves publisher throughput with no subscribers
(10,833 → 5,577) and costs progressively less as subscribers are added.

### Where the ceiling is

**Sharding the shared-memory LWLocks is not worth doing**, measured rather than
assumed:

| Measurement | Result |
|---|---|
| Throughput at 1 / 2 / 4 workers | 55k / 48k / 55k — no scaling at all |
| Worker wait events, 120 samples under `SET` load | 100% `RUNNING/cpu`, zero LWLock waits |
| `SET` (exclusive lock) vs `GET` (shared lock) | 52k vs 54k — indistinguishable |
| Pipelining `-P 1` → `-P 32` | 47.5k → 151k (3.2×) |

One worker matches four, and pipelining triples throughput without touching the
lock. The ceiling is the two mpsc thread hops per command. Memory-mode commands
need no SPI and no transaction, so a connection thread could in principle
execute them inline and skip both — the blocker is that PostgreSQL LWLocks
require a valid `MyProc`, which non-backend threads do not have. Closing that
needs a design, not a patch, and lock sharding only becomes interesting
afterwards.

### Benchmarking without fooling yourself

- **Point the benchmark at db 0.** The compose default is
  `redis.default_db=8`, the WAL-logged half — a benchmark that forgets this
  measures fsync and reports ~500 requests/second for everything.
  `bench-value-sizes` and `bench-report` set it; `bench` does not, by design.
  `bench-suite` names each policy's database explicitly.
- **`redis-benchmark` never clears `mylist`.** Its `lpush`, `rpush` and `lrange`
  tests build the list up over the whole run, and `-n 20000` is past what a list
  table holds at the default, so the run ends in the OOM the server is supposed
  to give and reports nothing at all. `docker/bench/report.sh` clears the key and
  uses 4,000 requests for those.
- **The noise band is ±6%, which is wider than most changes worth measuring.**
  Three rounds is not enough. One attempt at measuring the KV key change read as
  "SET and GET are 10% slower" while `PING`, which the change cannot touch, read
  as 8% *faster* — both noise. What works: alternate the two builds, nine rounds
  each at 50,000 requests, take medians, and include a command the change cannot
  reach as a calibration. At that sample size `PING` came back at +0.5% and
  everything else sat inside ±5% straddling zero.
- **`KEYS` needs its own measurement.** It is not a `redis-benchmark` test, and
  reading every key out of every entry is exactly what it does. Time it directly
  over a populated database.

CI runs `docker/bench/report.sh` on every pull request against memory mode and a
Redis 7 container on the same runner, and posts the result as one comment —
keyed on an HTML marker, so a push edits it in place and duplicates are deleted.
The same report goes to the job summary, which is all a fork's token can write.
It is a smoke signal, not a gate: what it catches is a collapse from tens of
thousands per second to hundreds. The ratio against the Redis beside it travels
better than the absolute numbers. `mise run bench-report` runs it locally.

---

## 9. Testing

Layers covering different code. Running one is not running the others.

| Layer | Command | What it actually covers |
|---|---|---|
| Unit | `mise run test-unit` | Pure functions: parsing, key layout, limits, SipHash, the pool — no PostgreSQL |
| pgrx | `mise run test-pg` | **The SPI path only** — never the shared-memory backend |
| End-to-end | `mise run e2e` | Everything: databases 0–7 in memory, 8–15 in tables, and moves between them |
| Parity | `mise run parity` | Every reply, diffed against a real Redis, on both halves |
| Acceptance | `mise run acceptance` | Redis's own Tcl test suite, on both halves |

**The gap in the middle is the important one.** `cargo pgrx test` never reaches
the memory engine: its tests run SQL against the tables directly, not commands
through a worker. Run the e2e suite for anything touching `src/memdb` or
`src/shm.rs`; one run covers both backends, since databases 0–7 default to
memory and 8–15 to tables.

What the e2e suite cannot reach cheaply, the unit tests do: `src/memdb` is
generic over its allocator, so `cargo test --lib` drives every collection
against a reference model, and full command lines through `store::run`
against a `Db` on the ordinary heap — no PostgreSQL, no shared memory.

**Name a `#[pg_test]` module `tests`.** pgrx resolves every `#[pg_test]` to the
`tests` schema (`framework.rs`: `let schema = "tests"`), so a module called
anything else compiles and then fails at the call. `pg_tests` is worse than
wrong: `#[pg_schema]` turns it into a schema name, and PostgreSQL reserves the
`pg_` prefix, so `CREATE EXTENSION` fails and *every* test in the run reports
"Could not obtain test mutex" instead.

One constraint on the plain `#[test]`s: they may not call anything that
reaches PostgreSQL. `cargo test --lib` links a binary with no PostgreSQL
symbols to resolve against, and it only gets away with that because
everything FFI is dead code there. `src/memdb` calls none; `src/shm.rs` is
where the FFI is, and is not unit-tested for that reason.

### Parity against a real Redis

The three layers above assert against expectations written by hand, so they can
only catch what someone already knew to expect. `docker/e2e/parity.ts` asks
Redis instead: the same commands to pg_redis and to `redis:7`, replies compared
byte for byte, on the shared-memory half and the SQL half in the same run.

```bash
mise run parity           # gate: fails on a difference not already recorded
mise run parity-update    # re-record after fixing or accepting one
```

66 cases, 1,394 replies a run, covering every command the parser accepts, the
corner cases the community trips on (empty collections vanishing, duplicate
arguments, source equal to destination, score and float formatting, what counts
as an integer, which operations keep a TTL), and the patterns people actually
deploy (rate limiter, session, `SET NX PX` lock, leaderboard, work queue, dedup,
cache-aside, tag intersection, hash-as-object, time index).

Three things it has to normalise, and nothing else:

- **Unordered replies.** `SMEMBERS`, `HGETALL`, `KEYS` and the set-algebra
  commands have no defined order in Redis, so comparing them verbatim measures
  two hash-table iteration orders. Sorted before comparison.
- **Draws and clocks.** `SPOP` without a count, `RANDOMKEY`, `TTL`, `INFO`,
  `SCAN` cursors. Only the reply *shape* is compared — the type byte, and an
  array's length.
- **A dropped connection.** A command that kills the server it was sent to never
  replies; the harness records `NO REPLY` and reconnects rather than waiting
  forever. That is how the `INCR` overflow below was found.

Differences are classified — `value`, `error-vs-value`, `error-code`,
`error-text` — and the known ones live in `docker/e2e/parity-baseline.json`.
**CI gates on new differences only.** A gate on zero differences would fail on
every run today, and one that failed always would be turned off.

### Redis's own acceptance suite

Parity diffs the commands someone thought to write down. Redis gates its own
releases on the Tcl suite under `tests/unit`, and that suite can drive any
server as an external one (`--host`/`--port`), so `docker/acceptance` runs it
against pg_redis — fetched at a pinned tag (7.2.5, the last BSD-licensed 7.x),
not vendored, with one patch: `FUNCTION FLUSH` at the top of every block is
made tolerant, because an error there aborts the whole block before its first
test.

```bash
mise run acceptance          # gate: fails when a test in the baseline stops passing
mise run acceptance-update   # re-record the baseline after tests start passing
```

Two passes, one per half: the suite's default `SELECT 9` reaches the SQL
tables, and `--singledb` (database 0) the shared-memory ones. Each unit runs
in its own process, so a unit that aborts takes only itself down.

- **A ratchet, like parity.** `docker/acceptance/baseline.json` lists the tests
  that pass today, per half and unit. The gate fails when one of them stops
  passing; the run lists the ones that started passing so the baseline moves
  forward. Most of the suite fails for a missing command (`BLPOP`, `BITOP`,
  `SSCAN`, `OBJECT FREQ`, ...), and gating on that would be gating on the
  roadmap.
- **What is left out, on purpose.** Whole units for command families that do
  not exist here (streams, geo, HyperLogLog, scripting, ...), and the units
  that reconfigure or restart the server under test, which Redis itself tags
  `external:skip`. Tests needing `DEBUG`, replication, `SAVE` or a real
  `maxmemory` are denied by tag. `--ignore-encoding` and `--ignore-digest`
  turn off the assertions about Redis's internal encodings.
- **`skip.txt` is for hangs only.** A failing test belongs in the baseline,
  where it stays visible. Two groups hang instead of failing, and each entry
  says why — the pipelining one is a real defect, not a gap.
- **No password.** The Tcl client cannot `AUTH`, so the profile runs with
  `PG_REDIS_PASSWORD` empty; CI does the same.

It found what the hand-written layers could not, because nobody had written
the expectation down: an unknown-command error echoing a newline that desynced
the connection, `SELECT` inside `MULTI` ignored, `EXPIRE`'s `NX`/`XX`/`GT`/`LT`
silently dropped, `SUNION` of a string and a set answering the set on the SQL
half, `EXPIREAT k 0` making a memory-half key persistent, and protocol errors
closing the connection without saying why.

### Running the e2e suite without Docker

`docker compose --profile e2e up` is the supported path, but `bun` runs the same
suite against any live server:

```bash
# 1. A cluster with the extension preloaded. Run as an unprivileged user —
#    initdb refuses to run as root, and a long $HOME breaks the socket path.
initdb -D "$PGDATA" -U postgres -A trust
cat >> "$PGDATA/postgresql.conf" <<'CONF'
listen_addresses = 'localhost'
shared_preload_libraries = 'pg_redis'
redis.port = 6379
redis.database = 'postgres'
redis.default_db = 8
redis.password = 'testpass'
CONF

# 2. Install the extension into that PostgreSQL, then start it.
cargo pgrx install --release --pg-config /usr/lib/postgresql/17/bin/pg_config
pg_ctl -D "$PGDATA" -o '-k /tmp' start
psql -h 127.0.0.1 -U postgres -c 'CREATE EXTENSION pg_redis;'

# 3. The suite.
cd docker/e2e && bun install
DATABASE_URL="postgres://postgres@localhost:5432/postgres" \
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=testpass \
  bun test index.test.ts
```

Re-run step 2 after every code change — the running server holds the old `.so`
until it restarts.

### Against an assert-enabled PostgreSQL

`cargo pgrx init --pgN download` configures PostgreSQL with `--enable-cassert`,
which distribution packages do not. Code that misuses a PostgreSQL internal can
pass against system packages and fail only in CI, with an assertion crash that
also takes down the backend and cascades into unrelated tests. If a test fails
in CI and passes locally, suspect this first.

Not hypothetical: `UpdateActiveSnapshotCommandId` asserts the active snapshot
has `active_count == 1` and `regd_count == 0`, which the snapshot SPI has
already pushed satisfies neither. It passed locally and failed every CI leg.

```bash
# ftp.postgresql.org may be unreachable; the Debian source package works.
echo "deb-src [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
  https://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
  > /etc/apt/sources.list.d/pgdg-src.list
apt-get update && apt-get install -y flex bison && apt-get source postgresql-17

cd postgresql-17-*/
./configure --prefix=$HOME/pgassert --enable-cassert --enable-debug \
  --without-icu --without-readline --without-zlib --with-openssl
make -j"$(nproc)" && make install

PGRX_HOME=$HOME/pgrxassert cargo pgrx init --pg17 "$HOME/pgassert/bin/pg_config"
PGRX_HOME=$HOME/pgrxassert cargo pgrx test pg17
```

Keep it under its own `PGRX_HOME` so the fast system-PostgreSQL config stays
intact, and give it its own `CARGO_TARGET_DIR` — the bindings differ, so sharing
one target directory means a full rebuild on every switch.

**Build it under the test user's home.** `$HOME/pgassert` as root plus tests as
the unprivileged user below means `pgrx install --test` cannot write to
`/root/pgassert`. That build's `pg_config --pkglibdir` also reports the *system*
lib dir, so its `.so` lands there rather than under the prefix — reinstall the
ordinary build before benchmarking anything.

### `cargo pgrx test` has to run as a non-root user

It runs `initdb`, which refuses to run as root. The failure is a wall of "Could
not obtain test mutex" from every test after the first, with the real message
only in the first one's output.

```bash
useradd -m pgu
cp -a ~/.cargo ~/.pgrx /home/pgu/ && chown -R pgu /home/pgu
chown -R pgu "$PWD"                                   # target/ is written during the run
chown -R pgu /usr/lib/postgresql/*/lib /usr/share/postgresql/*/extension
sudo -u pgu env HOME=/home/pgu PATH=/home/pgu/.cargo/bin:$PATH \
  RUSTUP_HOME=$HOME/.rustup CARGO_HOME=/home/pgu/.cargo cargo pgrx test pg17
```

`cargo pgrx init --pgN download` fetches from `ftp.postgresql.org`, not always
reachable. Pointing pgrx at the PGDG packages
(`cargo pgrx init --pg15 /usr/lib/postgresql/15/bin/pg_config …`) gets the matrix
running; only the assert-enabled build has to be compiled.

### Traps

- **A `describe` that does not `SELECT cache` runs on db 8**, the durable half
  — so it says nothing about the shared-memory backend. The "Binary safety" describe sat there for its whole life and passed
  while memory mode collapsed two NUL-containing keys onto one entry. Put
  memory-mode tests in a describe that selects `cache`, and confirm they *fail*
  against `main` before believing they cover anything.
- **`bun test -t "<filter>"` changes which database the tests run against.** The
  `SELECT isolation` describe is what leaves the pooled client on db 0; a
  filtered run skips it, so the connection stays on db 8. A list bug was
  misdiagnosed twice this way. Prefer a full run when a failure looks
  order-dependent.
- **`redis-cli -n 0` does not select the database on this server.** The
  connection stays on `redis.default_db`. Send `SELECT 0` as a command.
- **`Bun.RedisClient` encodes arguments as UTF-8.** It cannot express a `0xff`
  byte and reports 10 for a 7-byte value. Anything asserting on exact bytes or
  lengths must go through the `RawRedis` helper at the top of
  `docker/e2e/index.test.ts`.
- **A rejected `client.send()` leaves the pooled client's replies out of step**
  for later tests. Where an error reply is the expected outcome, use `RawRedis`,
  which returns the frame as data.
- **The parity harness resets with `FLUSHDB`, before every case, on both
  servers.** It used to delete only the tokens its own cases name, so keys left
  by an e2e run showed up as two `KEYS *` differences and the gate failed on
  work that was fine. That is why `FLUSHDB` was built first in phase 5, out of
  order. A run no longer needs a private server — but it does empty whatever
  database it is pointed at, on the reference too.
- **`SHOW redis.foo` names the result column `redis.foo`**, dot included.
  Destructuring by `foo` yields `undefined` with no error. Use
  `SELECT current_setting('redis.foo') AS mode`.

---

## 10. Continuous integration

`.github/workflows/ci.yml` is seven jobs. Two are trivial and gate the rest, so
a formatting mistake costs seconds rather than a full matrix:

```
Workflows ─┐
Format    ─┴─> Clippy
               Test (pg15, pg16, pg17)
               Build image ──> E2E (auto), E2E (memory)
                           └─> Benchmarks (pull requests only)
```

`Build image` exists because the end-to-end legs and the benchmark need the same
container. When they each built it they started together, so none could reuse
another's layer cache; now it is built once and the rest restore it warm.

The pgrx version comes from `Cargo.toml` — CI reads it from there rather than
keeping its own copy, because the auto-update workflow's token is not allowed to
rewrite a workflow file.

---

## 11. Local development

```bash
mise run run     # cargo pgrx run pg17, extension hot-reloaded
# then, in psql:  CREATE EXTENSION pg_redis;
# and elsewhere:  redis-cli -p 6379 ping

mise run fmt     # cargo fmt
mise run lint    # cargo clippy -D warnings
```

---

## 12. Open work

What is *left* between pg_redis and Redis — the 97 commands we do not implement,
the introspection that answers wrongly, and the two architectural blockers in
front of the rest — is measured in [Parity roadmap](PARITY.md). This section
records what is done and what is known-open in the surface we already have.

### The dispatch handoff

See [Where the ceiling is](#where-the-ceiling-is). Closing it needs a design,
not a patch.

### Redis parity

Done, and the thing everything else waited on. The audit has a machine behind
it — see [Parity against a real Redis](#parity-against-a-real-redis) — and what
it reports is recorded in `docker/e2e/parity-baseline.json`. Performance work
comes after this and is bounded by it: a change that buys throughput by
widening a parity or security gap is not a change worth making.

**2 differences over 1,394 replies**, down from 189. Every phase is done — 197
closed, and a sharper harness added 10 along the way. Both survivors are the
same accepted one, once per half: `INCRBYFLOAT` is `f64` where Redis uses
`long double` (see [phase 2](#phase-2-ranges-shapes-and-ties)).

The gate fails on a *new* difference, not on these two.

#### The plan

Six phases, ordered by what a client loses, not by effort. Every difference in
the baseline belongs to exactly one of them.

| # | Phase | Closes | Needs a decision |
|---|---|---:|---|
| 0 | [Broken, not different](#phase-0-broken-not-different) | **done** — 24 | no |
| 1 | [Input Redis refuses](#phase-1-input-redis-refuses) | **done** | no |
| 2 | [Ranges, shapes and ties](#phase-2-ranges-shapes-and-ties) | **done** | no |
| 3 | [Error wording](#phase-3-error-wording) | **done** | taken |
| 4 | [The key directory](#phase-4-the-key-directory) | **done** — 40 | taken |
| 5 | [Missing commands](#phase-5-missing-commands) | **done** — 38 | taken |

Phases 0–3 are independent of each other and of 4; 5 is last because a command
that does not exist costs a client less than one that answers wrongly. Phase 4
was the only one that changed a data structure.

Each phase ends the same way: `mise run parity-update`, the baseline shrinks by
the stated count and by nothing else, and every behaviour it changes gains an
end-to-end test on **both** halves.

#### Phase 0: broken, not different

**Done.** Not parity gaps — things that could not work at all, every one on the
SQL half, and every one a worker kill before `in_subtransaction` learned to
catch. They went first because the harness reached them by accident and a client
would reach them on a Tuesday.

| What | Why it failed | Fix |
|---|---|---|
| `HINCRBY` — **every call** | The statement inserted `$3::text` into a `bytea` column and cast `bytea` to `bigint` without `encode`, so it raised at analysis time. It had never run, and nothing tested it. | `convert_to`/`encode`, as the KV counters use |
| `MSET k v1 k v2`, `HSET h f v1 f v2`, `ZADD z 1 a 2 a` | `ON CONFLICT DO UPDATE` cannot touch a row twice in one statement | `dedup_last` before binding, which is Redis's last-wins |
| `RENAME` onto an existing key | Primary-key violation; a CTE that deletes the destination is not visible to the update beside it | The destination is cleared in its own statement, and only when the source is there, so a `no such key` leaves it alone |
| `LINDEX key <large negative>` | `OFFSET` is evaluated even when the `WHERE` excludes every row, and a negative one raises | `GREATEST(…, 0)` |
| `HSET` returned fields written | Redis returns fields *added* | `RETURNING (xmax = 0)`, counted |
| `SUNIONSTORE`/`SINTERSTORE`/`SDIFFSTORE` onto an overlapping destination lost the overlap | The insert's uniqueness check still sees rows the same statement deleted, so `ON CONFLICT DO NOTHING` skipped them and the delete kept them | Delete only what is *not* in the result, as the sorted-set stores already did, and return the result's cardinality |
| `EXISTS k k` counted 1 | `count(DISTINCT key)` | One row per argument |
| `RENAME` dropped the expiry (shared memory) | The value moved, the expiry did not | Read `mem_ttl_raw` before the value is taken |
| `GETSET` kept the expiry (SQL) | Redis discards it with the value | `expires_at = NULL`, as `SET` |

The last two came out of the harness itself: `TTL` was compared by reply *shape*,
so `-1` and `100` looked alike and every expiry bug was invisible. It compares
the class now — gone, no expiry, or some expiry — which cost nothing and
immediately showed both, plus five more that belong to phase 4.

**Also here:** a caught SQL error is logged with its message and SQLSTATE
(`pg_redis: command failed: …`). Catching it stops PostgreSQL reporting it, so
without this the operator got nothing at all — which is how `HINCRBY` had been
failing unnoticed.

#### Phase 1: input Redis refuses

**Done.** All in the parser, which is what makes it one fix per behaviour rather
than two: both backends read the same `Command`.

- **Expiry validation.** `check_expire` refuses a non-positive expiry and one
  whose deadline could not be represented, so `SET k v EX 0`, `SETEX k 0 v` and
  `EXPIRE k 9999999999999999` are errors rather than a delete or a date in the
  far future. `check_expire_deadline` allows a past deadline, since `EXPIRE k -1`
  legitimately deletes the key.
- **Count validation.** `check_count` on `SPOP`, `LPOP`, `RPOP`, `ZPOPMIN` and
  `ZPOPMAX`. Not on `SRANDMEMBER` or `ZRANDMEMBER`, where a negative count means
  "with repeats".
- **Integer canonicality.** `mem::parse_stored_int` reads a counter the way
  Redis does — no `+1`, no `007`, no `-0` — and the SQL guard was widened to
  match, including a `numeric` range check so a stored value too large for
  `bigint` reads as "not an integer" rather than raising an overflow.
- **Float validation.** A score literal that overflows to infinity is out of
  range rather than infinite, so `ZADD z 1e400 m` is refused; only the spelled
  forms mean infinity. A `ZINCRBY` landing on NaN is refused without storing it,
  and so is an `INCRBYFLOAT` whose *result* is not finite — Redis draws that line
  at the result, not the argument, and takes an infinite increment happily.

#### Phase 2: ranges, shapes and ties

**Done.**

- **Range clamping.** `range_bounds` replaced four copies of the same
  arithmetic, three of which wrapped: `(stop + len) as usize` on a stop past the
  start is enormous, clamps to the last element and selects everything, which is
  how `ZREMRANGEBYRANK key -100 -100` emptied a sorted set.
- **Reply shape.** `Response::NullArray` exists now, so `LPOP key 1` on a
  missing key answers `*-1` and `LPOP key 0` on a live list answers `*0`. A
  client asking for a list back no longer gets a nil string.
- **Tie-break.** `zset_meta_member_added` compares members when scores are
  equal, which costs one `hash_search` on the tie and nothing otherwise.
- **Float precision — accepted, not fixed.** `INCRBYFLOAT` is `f64` where Redis
  uses `long double`, so `3 + 1.000000000000000005` is `4` here and
  `4.00000000000000001` there. 80-bit arithmetic is not available in stable Rust
  without a dependency, and `float8` is what the SQL half stores. The two
  differences stay in the baseline as a record.

#### Phase 3: error wording

**Done**, and the decision taken: Redis's strings verbatim. `wrong_arity`,
`NOT_AN_INTEGER`, `NOT_A_FLOAT` and `invalid_expire` name them once each, which
replaced about sixty hand-written variants — `GET missing argument at position 0`
became `wrong number of arguments for 'get' command`, and every `X requires
integer` became `value is not an integer or out of range`.

Not every message came from a table: `LSET` on a missing key says `no such key`
rather than `index out of range`, `HINCRBY` names the hash, `SELECT 99` is out
of range rather than unparseable, and `DECRBY key -9223372036854775808` gets
Redis's own `decrement would overflow`.

#### Phase 4: the key directory

**Done**, and the decision taken: one directory per database, not a probe of the
other tables per write. It closed two things that looked separate:

- **A key could hold every type at once (30).** `SET k v` then `LPUSH k a`,
  `SADD k m`, `HSET k f v` and `ZADD k 1 m` all succeeded; `TYPE` said `string`,
  and each accessor returned its own data. `SINTERSTORE` onto a key holding a
  string was the same bug from the other side: the set appeared and the string
  stayed.
- **Collections could not expire (4).** `EXPIRE` on a list, set, hash or sorted
  set returned 0 and set nothing, so every cache-with-a-TTL pattern over a
  collection kept its data forever.

Both were the same missing object: nothing knew what a key was. Each type had
its own table, keyed independently, and no one asked the others.

**The shared-memory half** gained a tenth HTAB, a key directory — since
replaced, with the rest of the fixed tables, by one dictionary per database
([§3](#one-database-one-dictionary)). The hash table also gained the count meta the set table already had,
which is what tells the directory its last field has gone, and makes `HLEN` a
lookup rather than a scan of the whole database's hash table.

**The durable half** derives the type instead of storing it: the five tables are
already keyed by `key`, so one query answers "what holds this" for every key a
command names, in a single round trip before the command runs. Nothing to keep
in step, at the cost of that query. Only the expiry needed somewhere to live —
`redis.expiry_N`, one row per collection carrying a TTL — because a string's
`expires_at` column is the only one in the schema.

Three things fell out of building it that the parity harness had not asked for:

- `SET k v NX` over a list said OK on the durable half, because the `NX` test
  looked at `redis.kv_N` alone and a list is invisible there. So did `SETNX` and
  `MSETNX`. All three ask the whole directory now, and `XX` — whose condition a
  collection satisfies — replaces it rather than doing nothing.
- Redis discards a key's TTL whenever its value is replaced, `SUNIONSTORE` onto
  a live set included. Only the type change was clearing it.
- An expired collection has to be gone, not merely untyped: `TYPE`, `EXISTS`,
  `TTL` and `DBSIZE` on the durable half read `redis.expiry_N` and treat a
  passed deadline as absent, the way they already treat an expired string.

**What it costs the durable half.** The type query is a round trip the SQL
backend did not make before, so where it lands matters:

- A **write** is checked up front — it cannot be allowed to land and then be
  refused — so `LPUSH`, `SADD`, `HSET` and friends pay one lookup each.
- A **read** runs first and asks only when it came back empty: a key holding
  another type finds nothing, and so does a key that is not there. Every
  successful `GET`, `HGET` and `LRANGE` is unchanged.
- The commands that only **replace** a key — `SET`, `SETEX`, `PSETEX`, `MSET` —
  ask nothing at all. They clear the other tables from inside their own
  statement, as four extra index probes. Doing it as a lookup first halved
  `SET` throughput on the durable half in the first measurement, which is what
  prompted folding it in.

The benchmark numbers themselves are not worth quoting from this container: the
spread between identical rounds is two to three times, which rules out a
collapse and nothing finer.

Still open, and not part of the 40: `KEYS`, `SCAN` and `RANDOMKEY` see only the
KV table. The directory holds a key's hash, not its bytes, and no other table
stores a collection's name — so there is nothing to list them from. Fixing it
means either an inline key prefix in the directory entry (32 bytes becomes 80,
plus a sixth chunk pool) or a per-key table on each side. Neither is free, and
neither is what phase 4 was for.

#### Phase 5: missing commands

**Done.** Every command the harness asked for and did not get, plus `GETBIT`
and `FLUSHALL`, which it never asked for because neither can be tested against
a shared reference server without wrecking it.

`FLUSHDB` went first, out of the documented order, and paid for itself
immediately: the harness used to reset by deleting the tokens its own cases
mention, so anything an e2e run had written was still there and `KEYS *`
reported it as a difference on work that was fine. It cost two false failures
in one session. The harness flushes now, and its cases no longer have to
declare their keys.

| Command | Notes |
|---|---|
| `SETRANGE`, `GETRANGE` | `SETRANGE` pads the gap with NUL bytes, which `chr(0)` cannot express in PostgreSQL text — the SQL half builds the padding as hex |
| `SETBIT`, `GETBIT`, `BITCOUNT` | Redis numbers bits from the most significant of byte 0; PostgreSQL's `get_bit` counts from the least, so both halves compute in Rust and agree by construction |
| `HINCRBYFLOAT`, `HRANDFIELD` | A field entered for an increment that turns out to be invalid is removed again, rather than left behind empty |
| `RPOPLPUSH` | Parses straight to `LMove { src_left: false, dst_left: true }` — one command, not two implementations |
| `RENAMENX`, `GETEX` | |
| `SINTERCARD` | `LIMIT 0` means no limit, which is Redis's reading and not the obvious one |
| `COPY` | See below |
| `OBJECT ENCODING` | See below |
| `FLUSHDB`, `FLUSHALL` | See below |

Three of them needed a decision rather than an implementation.

**`COPY` crosses the two halves.** `COPY k k DB 8` promotes a shared-memory
cache entry to a durable row, and `DB 0` brings one back. That is the one place
`COPY` does more here than in Redis, which has no such split. It reads through
`dump_key` into a form neither backend stores and writes through `restore_key`
into whichever the destination names — the only command that has to, which is
why the pair exists at all and why `COPY` is routed to the SPI path whatever
the databases' policies.

**`OBJECT ENCODING` reports Redis's representation, not ours.** `int`,
`embstr`, `raw`, `listpack`, `quicklist`, `intset`, `hashtable`, `skiplist`,
derived from the value's shape against Redis's default thresholds. pg_redis has
an inline slot and a chunk pool, or a `bytea` column, and none of those names
describes it. Reporting our own would be truthful and would break every client
that branches on Redis's — the decision was to satisfy the client. A missing
key answers nil, which is Redis's own reply here rather than the error nearly
every other command gives.

**`FLUSHALL` empties all sixteen databases, the durable half included.** Redis's
own reach, taken deliberately over a safer default: databases 8–15 are
WAL-backed, and the Redis port bypasses PostgreSQL authentication, so any client
that can reach the port can erase them. `redis.listen_address` defaults to
loopback and `redis.password` exists; both matter more now than they did.
`FLUSHALL` is routed to the SPI path for the same reason `COPY` is — it is the
only other command that touches both halves.

Two things fell out that the harness had not asked for. `DBSIZE` on the
shared-memory half counted strings alone, reporting 1 for a database holding a
string, a list, a hash, a set and a sorted set; it reads the key directory now.
And a flush writes to keys it cannot name, so `WATCH` needs every counter
bumped rather than the ones `write_keys` returns — `watch::bump_all` aborts
watches on databases the flush did not touch, which is the direction that table
is allowed to err in.

### An expired collection outlives its own expiry

Key-level reads are exact the moment the deadline passes: `TTL`, `TYPE`,
`EXISTS` and `DBSIZE` go through the key directory on the shared-memory half and
`redis.expiry_N` on the durable one, and both read a passed expiry as absent.

The type-specific reads do not. `LLEN` reads the list's own meta, `SCARD` the
set's, `LRANGE` the entries — none consults the directory — so for up to a
second, until the sweep runs, `TYPE` answers `none` while `LLEN` still reports
the old count. Redis expires lazily on every access and has no such window.

**Fixed, on both halves.** The memory half went the way this entry suggested:
`dir_lookup_raw` distinguishes "absent" from "expired", and
`mem_key_kind_reaping` drops an expired key at the type gate every command
already passes through — the same single directory lookup as before. The
durable half took two mechanisms, decided in `Command::execute` and nowhere
else: the cached single-key collection reads carry `sql::live` in their own
predicate, an InitPlan that rides inside the round trip they were already
making, and every other collection read reaps its keys first with
`sql::reap_expired_keys`. What each choice costs, measured, is in
[PARITY.md](PARITY.md#the-stale-collection-read-was-bigger-than-this-table-said).

### `RANDOMKEY` favours strings

**Fixed, on both halves** — and the durable half turned out worse than this
entry said: it read `kv_N` alone, so a database holding only collections
answered *nil* while `DBSIZE` counted them. Both halves now draw uniformly
over every live key — the durable half with a five-table `UNION` under
`ORDER BY random()`, the memory half with a single-pass reservoir over the KV
scan and the collection names, expired-but-unswept keys excluded on both. The
write-up is in [PARITY.md](PARITY.md#tier-1--commands-we-have-that-behave-differently).


### Settled, so not on this list

- **Fixed-size shared-memory tables** — replaced by the arena and one
  dictionary per database ([§3](#3-the-shared-memory-backend)), which took the
  entry limits, the 64 KiB value cap and the per-table locks with them.
- **A SQL error killing the worker** — `INCR` past `bigint`'s ceiling raised
  out of the dispatcher and ended the process; four of those ended the Redis
  endpoint. `in_subtransaction` catches now, and the reply matches Redis.
- **Where a collection's name is stored** — in the four meta tables, not the key
  directory. Priced both ways: 6 MB against 22 MB.
- **What `OBJECT ENCODING` reports** — Redis's representation names, not ours.
  See [phase 5](#phase-5-missing-commands).
- **How far `FLUSHALL` reaches** — all sixteen databases, as in Redis, the
  durable half included. Also [phase 5](#phase-5-missing-commands).
