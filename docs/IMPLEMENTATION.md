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
socket ─> resp::parse ─> Command::parse ─┬─> execute_mem   (shared memory, db 0–7 in memory mode)
                                         └─> execute_spi   (SPI + SQL, everything else)
```

`Command` is a single enum covering every supported command. Parsing is
separated from execution so the parser can be unit-tested without a PostgreSQL
instance, and so size limits can be checked *before* anything is written — see
[Limits](#7-limits).

### The 16 databases

Databases mirror Redis's model and split into two contiguous halves.

| DB | Name | Backend | Durability |
|---|---|---|---|
| 0–7 | `cache` (0) | shared memory, or UNLOGGED tables | Lost on restart |
| 8–15 | `durable` (8) | WAL-logged tables | Survives crashes |

`SELECT` accepts the numbers and the two names. `redis.default_db` (default 0)
chooses where new connections start. The halves are contiguous rather than
odd/even because `execute_mem` indexes the shared-memory tables with the raw
database number; a unit test pins that correspondence.

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

The ephemeral half has two, chosen by `redis.storage_mode` (a `postmaster`-scope
GUC — changing it needs a restart). The durable half is always WAL-logged.

| | `memory` (default) | `auto` | durable (8–15) |
|---|---|---|---|
| Storage for db 0–7 | Shared-memory HTAB per data type | UNLOGGED tables | — |
| Transaction | None | `BEGIN`/`COMMIT` per batch | per batch |
| SPI overhead | None | Yes | Yes |
| WAL | No | No | Yes |
| Survives crash | No | No (UNLOGGED, truncated) | Yes |
| Replication | No | No | Yes |
| SQL-visible | No — `SELECT * FROM redis.kv_0` returns nothing | Yes | Yes |
| Max keys | `redis.mem_max_entries` per type per db | Unlimited (disk) | Unlimited |
| Shared memory at the default | ~70 MiB, reserved at start | None | None |
| Max value | 64 KiB | Unlimited (TOAST) | Unlimited |
| Max key | 512 bytes | Unlimited | Unlimited |
| Max hash field / set member | 128 bytes | Unlimited | Unlimited |

### SQL schema

Both SPI backends use the same shape in the `redis` schema, one table per data
type per database; 0–7 `UNLOGGED`, 8–15 logged.

```sql
CREATE SCHEMA redis;

-- Key-value (pattern repeats for db 0–15)
CREATE UNLOGGED TABLE redis.kv_0 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE redis.kv_8 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);

-- Hash (same pattern)
CREATE UNLOGGED TABLE redis.hash_0 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE redis.hash_8 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
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

All of `src/mem.rs`. Eight databases, eight tables each, and five LWLocks per
database — one per data type, with each meta table covered by its type's lock.
So a write to db 0 never blocks a write to db 2, and the hash table never blocks
the list table.

### Tables and entry layouts

Eight HTABs per database, created with `ShmemInitHash` during
`shmem_startup_hook`, all `HASH_BLOBS` and `HASH_FIXED_SIZE`.

| Table | Entries | Entry | HTAB key | Size at the default |
|---|---|---:|---|---:|
| KV | `mem_max_entries` | `KvEntry` 232 B | 16 B key hash | 18.19 MiB |
| Hash | half | `HashEntry` 216 B | 16 B hash + 128 B field | 8.50 MiB |
| Zset | half | `ZsetEntry` 152 B | 16 B hash + 128 B member | 6.00 MiB |
| Set | half | `SetEntry` 144 B | 16 B hash + 128 B member | 5.69 MiB |
| List | half | `ListEntry` 96 B | 16 B hash + 8 B position | 3.81 MiB |
| Zset meta | half | `ZsetMeta` 304 B | 16 B key hash | 11.94 MiB |
| List meta | half | `ListMeta` 40 B | 16 B key hash | 1.62 MiB |
| Set meta | half | `SetMeta` 24 B | 16 B key hash | 1.00 MiB |

"half" is `mem_max_entries / 2`, floored at 256. Sizes include dynahash's 5/4
bucket overhead and are multiplied across eight databases. A unit test pins
every entry size, because each one is multiplied by `mem_max_entries` across
eight databases and requested *before* the postmaster starts — growing one
carelessly turns "slower" into "does not boot".

The meta tables exist so `ZCARD`, `SCARD`, `LLEN`, `ZPOPMIN`/`ZPOPMAX` and
`LPUSH`/`RPUSH` are O(1) rather than a scan of the entry table.

### Key hashing

**Every table indexes on a 128-bit keyed SipHash-2-4 of the Redis key**, not on
the key bytes. The key material is drawn from `pg_strong_random` once per
postmaster, so a client cannot craft two keys that collide and merge their
contents. Shared memory does not survive a restart, so neither does the key, and
it never has to match anything on disk.

The collection tables (hash, set, zset, list) keep *only* the hash. They store
one entry per member, so a full copy of the key in each would dominate the entry
— 512 bytes against a `SetEntry`'s 144, repeated for every member — and they
never need the bytes: every access is a comparison against a key the caller
already holds.

The KV table is the exception, because `KEYS`, `SCAN` and `RANDOMKEY` hand real
keys back to clients.

No table is `HASH_STRINGS`, and none may become one: dynahash compares string
keys with `strncmp`, which makes two keys agreeing up to their first NUL byte
the same key.

### How a KV entry stores its key

`KvEntry` keys on the hash and keeps the key bytes alongside it, laid out the
way a value is — an inline prefix with the tail chained through the same chunk
pool:

```
KvEntry  = key hash 16 | key_inline 128 | value_inline 64 | expires_at 8
                       | key_len 4 | key_overflow 4 | value_len 4 | overflow 4
```

`INLINE_KEY_LEN` is 128, and the reason is the pool rather than the entry. Eight
bytes of `KvEntry` is 640 KiB of shared memory (8192 entries × 8 databases ×
5/4), so the prefix costs 10 MiB. At 64 it would cost 5 MiB — but a key of
65–128 bytes would then take a chunk, and `myapp:prod:session:<uuid>` is 60 to
90 bytes, so a full table of entirely ordinary keys would claim every chunk in
the pool and leave nothing for any value past its own 64-byte inline slot. At
128 the same cliff sits at 129 bytes, which conventional keys do not reach.

A longer key still works, at one chunk per 64 bytes over:

| Key length | Chunks per key | Keys before the pool is empty |
|---:|---:|---:|
| ≤ 128 bytes | 0 | 8,192 (the table's own limit) |
| 129–192 | 1 | 8,192, with no pool left for values |
| 193–256 | 2 | 4,096 |
| 449–512 | 6 | 1,365 |

### Lookups verify the key

Because the KV entry holds the key bytes, `kv_find` and `kv_enter` compare the
stored key against the one asked for. A mismatch reads as "absent" and refuses
the write, so a hash collision cannot return or overwrite another key's value.
It has its own error rather than an OOM, because raising `mem_max_entries` is
the obvious response to an OOM and does nothing for a collision:

```
ERR key hash collides with a different key already stored; rename the key, or use SELECT durable.
```

The cost is a length test plus, at most, a compare of the inline prefix against
bytes the lookup has already pulled into cache; only a key over 128 bytes
matching on both goes as far as walking the pool. Unreachable in practice at
`mem_max_entries` keys per database — it exists because the alternative is to
write over the key already there, silently. **The collection tables cannot do
this**: they have nowhere to put the loser of a collision.

### The chunk pool

One slab per KV, hash and list table per database — 24 in all — carved out of a
single `ShmemInitStruct` allocation and indexed by stride.

```
[ValPool header][next: u32 × capacity][data: u8 × capacity × 64]
```

`capacity` is `mem_max_entries` chunks of `CHUNK_LEN` = 64 bytes. The `next`
array doubles as the free list *and* as the chain of a stored value, so a chunk
is on exactly one of the two at any moment and no separate bitmap has to be kept
in step. Allocating *n* chunks takes a prefix of the free list — one pointer
moved, not *n*. Freeing returns a chain in one splice.

A byte string of length *n* occupies `ceil((n - inline) / 64)` chunks, where
`inline` is 64 for a value and 128 for a KV key. Anything shorter than its
inline slot costs no chunk at all — which covers most of what a cache holds;
`redis-benchmark` writes 3-byte values.

**Chunk lifecycle is the easiest thing here to get wrong.** The chain head lives
in the entry, so the release must happen while the entry is still there. Every
removal goes through `remove_valued` (or `list_remove_at`, its bare-HTAB twin).
A `KvEntry` owns *two* chains, so its `kv_free` releases the value chain and the
key chain. A path that calls `hash_search(HASH_REMOVE)` on the KV, hash or list
table directly leaks silently until the pool runs dry.

### Value and key size limits

```
max value = min(64 KiB, 64 + (redis.mem_max_entries / 8) × 64 bytes)
```

which is **65,536 bytes** at the default, where the two bounds meet:

- **An eighth of the pool.** A pool is shared by every value in its table, so one
  value must not be able to take all of it.
- **64 KiB outright.** Every read copies the value out of shared memory while the
  table's LWLock is held, so an unbounded value would hold a whole database still
  for the length of one memcpy. That ceiling holds however large the pool is
  configured, so raising `mem_max_entries` buys capacity, not longer values.

Keys are capped at 512 bytes for the same kind of reason — how much of the
shared pool one key may take — and hash fields and set members at 128 because
those still live in fixed-size arrays.

Anything over a limit is **rejected, never truncated**, and the check runs
before the command executes so a refusal never leaves a partial write behind:

```
ERR key exceeds redis.storage_mode='memory' limit of 512 bytes (use SELECT durable for unbounded values)
ERR value exceeds redis.storage_mode='memory' limit of 65536 bytes (use SELECT durable for unbounded values)
```

Truncation is the one failure mode worse than an error: two members sharing a
128-byte prefix would collapse onto one entry, so an `HGET` returns another
field's value and an `HDEL` removes it.

### When a table fills

`redis.maxmemory_policy`, named after Redis's setting:

| Policy | Behaviour |
|---|---|
| `noeviction` (default, as in Redis) | The write is refused with `OOM command not allowed when used memory > 'maxmemory'`. Nothing already stored is lost. |
| `allkeys-random` | Entries are evicted to make room. Collections go **whole** — never half-present — and the key being written is never chosen, so a single key larger than its own table still reports OOM. |
| `volatile-ttl` | Only keys carrying an expiry, soonest first. Falls back to refusing if none do. |

Expired entries are reclaimed before any live key is considered, under every
policy. Eviction costs one `hash_seq_search`, so it frees `EVICT_BATCH` = 64
entries at a time and amortises the scan over the next 64 inserts. Under
`noeviction` it returns immediately without scanning anything.

A full table has never been able to take the server down: every insert uses
`HASH_ENTER_NULL`, never `HASH_ENTER`. On a `HASH_FIXED_SIZE` table dynahash
answers a full table by raising `out of shared memory`, which unwinds by
longjmp — past the Rust error handling that looks like it covers it — and kills
the worker.

The **pool** can run dry before the entries do. That is reported as the same
OOM, and `reserve_chunks` (`kv_reserve` for the KV table, which must budget for
the key as well as the value) asks eviction for room the same way. It has to run
*before* the entry exists, because eviction may remove any entry in the table
including the one about to be written.

A refused write records *why* in a single thread-local `Refusal`, read once per
command by `execute_mem` and turned into either the OOM reply or the collision
one. Threading a result out through all 68 `mem_*` entry points — whose return
types are `bool`, `i64`, `Result<i64, String>`, `Vec<..>` and more — was the
alternative. A worker dispatches one command at a time on one thread, so the
flag cannot cross commands.

### Expiry

`KvEntry` carries `expires_at` in microseconds since the epoch, 0 for none. Reads
treat an expired entry as absent and delete it lazily; a background sweep also
walks each database once a second and removes what it finds, returning the
chunks. Only the KV table has expiry — Redis's TTLs are per key, and a
collection's key lives in its meta.

### Lists

List elements are keyed on `(key hash, position)` with positions spaced
`LIST_POS_STEP` = 1024 apart, so `LPUSH` and `RPUSH` are O(1) — they take the
next position outside the current range from the meta.

Readers walk positions as `min_pos + i * LIST_POS_STEP`, so an in-place removal
leaves a hole that silently truncates the list at the first gap while the count
still reports the full length. `LREM`, `LINSERT` and `LTRIM` therefore go
through a read-modify-write that renumbers the whole list from a fresh base —
O(n) on commands that were already O(n).

### Sizing `redis.mem_max_entries`

This one number scales the whole reservation, so it is the only knob that
decides how much memory the extension takes. Measured as `SHOW
shared_memory_size` on PostgreSQL 17 at otherwise default settings:

| `redis.mem_max_entries` | `shared_memory_size` |
|---|---|
| 256 | 214 MB |
| 4,096 | 245 MB |
| **8,192 (default)** | **280 MB** |
| 10,240 | 297 MB |
| 12,288 | 314 MB |
| 16,384 | 349 MB |
| 32,768 | 487 MB |

PostgreSQL's own share is roughly the 214 MB floor; the rest is the extension.
It also sets the size of each pool, and with it the value cap above.

The default was halved from 16,384 when the key limit grew to 512 bytes, and
three rounds of shrinking the entries have not yet paid that back: hashing the
key out of the KV table freed 28 MB, where doubling the setting costs 69 MB.
Raising it is supported — it buys pool capacity as well as key capacity — but it
is a memory-budget decision, not something the entry layout funds.

### How the reservation got here

| | `shared_memory_size` | What changed |
|---|---|---|
| Composite key hashing | 587 → 413 MB | Hash, set, zset and list entries key on a 128-bit hash instead of carrying a 512-byte copy of the Redis key |
| Chunked value pool | 413 → 308 MB | Three fixed overflow tables became one 64-byte-chunk slab per table; the value cap went 512 B → 64 KiB |
| KV key hashing | 308 → **280 MB** | The last table storing keys verbatim; `KvEntry` 592 → 232 bytes |

---

## 4. Pub/Sub

Pub/sub is independent of `storage_mode` and always allocated — it needs
`shared_preload_libraries`, not memory mode.

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

RESP2, with `HELLO 3` negotiating RESP3 push frames for pub/sub.

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

---

## 7. Limits

Everything the memory-mode backend refuses, in one place. The durable half
(8–15) has none of them and is bounded only by disk; `auto` has none of the
shared-memory ones.

| | Limit | Where it comes from |
|---|---|---|
| Key | 512 bytes | Share of the chunk pool |
| Value | 64 KiB | An eighth of the pool, capped by the copy-under-lock ceiling |
| Hash field / set member | 128 bytes | Fixed array in the entry |
| Keys per type per database | `redis.mem_max_entries` | Fixed-size HTAB |
| Channel / pattern | 255 bytes | Fixed slot |
| Pub/sub payload | 512 bytes | Fixed ring slot |
| Bulk string on the wire | 512 MiB | RESP parser, matches Redis |

Redis caps a string at 512 MiB and places no limit at all on key or field
length. pg_redis honours the protocol limit at the parser, so **the durable half
matches Redis** — values there are bounded only by PostgreSQL's ~1 GB per-field
TOAST limit. The memory-mode limits are lower because shared memory is a
fixed-size region sized at postmaster start, so nothing in it can grow on
demand. That is a property of the backend, not of the protocol: switch the
database to `auto`, or use `SELECT durable`, and the same command is stored
exactly as Redis would store it.

---

## 8. Performance

Requests/second on Docker, Apple M-series, `redis-benchmark -n 50000 -c 200`
against the default database.

| Command | Redis 7 | memory | SPI/unlogged | SPI/logged |
|---------|--------:|-------:|-------------:|-----------:|
| PING    | 234,000 | 112,000 | 113,000 | 103,000 |
| GET     | 243,000 | 106,000 |  85,000 |  92,000 |
| SET     | 224,000 | 103,000 |  22,000 |  12,000 |
| INCR    | 253,000 | 116,000 |  32,000 |  14,000 |
| HSET    | 227,000 |  83,000 |  26,000 |  13,000 |
| ZADD    | 185,000 | 118,000 |  15,000 |   7,000 |
| SADD    | 194,000 |  82,000 |  71,000 |  66,000 |
| SPOP    | 183,000 | 116,000 |  51,000 |  64,000 |
| ZPOPMIN | 183,000 | 118,000 |  48,000 |  47,000 |

`redis-benchmark` writes 3-byte values, which never leave the 64-byte inline
slot — so the table above says nothing about the chunk pool.
`mise run bench-value-sizes` sweeps 3/63/64/65/200/512/4096/65536 bytes for
that. `mise run bench-high-write` (8 workers, `batch_size=256`) spreads
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

- **Point the benchmark at db 0 with `storage_mode=memory`.** The compose default
  is `redis.default_db=8`, the WAL-logged half — a benchmark that forgets this
  measures fsync and reports ~500 requests/second for everything.
  `bench-value-sizes` and `bench-report` set both; `bench` does not, by design.
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

Three layers covering different code. Running one is not running the others.

| Layer | Command | What it actually covers |
|---|---|---|
| Unit | `mise run test-unit` | Pure functions: parsing, key layout, limits, SipHash, the pool — no PostgreSQL |
| pgrx | `mise run test-pg` | **The SPI path only** — never the shared-memory backend |
| End-to-end | `mise run e2e` | Everything, in whichever storage mode the server runs |

**The gap in the middle is the important one.** `cargo pgrx test` starts its own
throwaway cluster *without* `shared_preload_libraries`, so memory mode — the
default storage mode, and all of `mem.rs` — is invisible to it. Four bugs
reached a branch that way. Run both storage modes for anything touching
`mem.rs`; CI does:

```bash
mise run e2e                              # storage_mode=auto
PG_REDIS_STORAGE_MODE=memory mise run e2e # the shared-memory backend
```

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
redis.storage_mode = 'memory'      # or 'auto' — run both
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

- **A `describe` that does not `SELECT cache` runs on db 8**, the durable half,
  whatever `storage_mode` says — so it says nothing about the shared-memory
  backend. The "Binary safety" describe sat there for its whole life and passed
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

### The dispatch handoff

See [Where the ceiling is](#where-the-ceiling-is). Closing it needs a design,
not a patch.

### `MAX_MEMBER_LEN` is still 128 bytes

Redis has no limit on hash fields or set members. Memory mode caps them because
five fixed `[u8; MAX_MEMBER_LEN]` arrays remain — `HashEntry::field`,
`SetEntry::member`, `ZsetEntry::member`, and `ZsetMeta`'s `min_member` and
`max_member`. They are what is left of the pattern the last three changes have
been removing, and they are **25 MiB** at the default:

| Array | Bytes per entry | Cost |
|---|---:|---:|
| `ZsetMeta::min_member` + `max_member` | 256 | 10.00 MiB |
| `HashEntry::field` | 128 | 5.00 MiB |
| `ZsetEntry::member` | 128 | 5.00 MiB |
| `SetEntry::member` | 128 | 5.00 MiB |

**Do not just copy what the KV table did.** Priced out, the same move gives
three different answers, and the obvious one is the worst:

| | Change | Saves | Costs |
|---|---|---:|---|
| `HashEntry` | field → hash + 48 inline + pooled tail (216 → 160) | 2.19 MiB | nothing — the hash table already has a pool |
| `SetEntry`, `ZsetEntry` | same (144 → 88, 152 → 96) | 4.38 MiB | **8.50 MiB** — neither table has a pool, and one costs 4.25 MiB across eight databases |
| `ZsetMeta` | keep member *hashes*, not bytes (304 → 72) | **9.06 MiB** | no pool at all, but `ZPOPMIN`/`ZPOPMAX` stop reading the member straight out of the meta |

So the set and zset entries do not pay for themselves as a straight copy — the
two new pools cost about twice what the smaller entries save. Options worth
pricing first: one pool shared by both tables, a pool sized below one chunk per
entry, or leaving those two alone.

`ZsetMeta` is the opposite. At 304 bytes it is the **largest entry in the
extension**, bigger than `KvEntry`, and 256 of those bytes are two copies of a
member it stores only to name the current min and max. Replacing them with
hashes is the single biggest win left and needs no new shared memory — the
question is what `ZPOPMIN` pays, since it reads the member from the meta in O(1)
today and would have to find it by hash in the entry table instead.

One number that does not carry over: these tables hold `mem_max_entries / 2`
entries, so 8 bytes of entry costs 320 KiB rather than the KV table's 640 KiB.
Real members are also shorter than real keys, so the inline prefix probably
wants to be smaller than 128 — price it against pool pressure the way
`INLINE_KEY_LEN` is priced rather than reusing that number.

### Multi-key writes are not atomic under OOM

An `MSET` or multi-member `SADD` that fills a table part-way through stores the
earlier keys and then errors, where Redis rejects the command whole. Whether
there is room is only knowable at the insert, so this needs a capacity pre-check
against the command's key count. Commented at the check site in `execute_mem`.

### Settled, so not on this list

- **`redis.mem_max_entries` stays at 8192** — see
  [Sizing](#sizing-redismem_max_entries). Measured, not assumed.
- **Sharding the shared-memory LWLocks**, for the reason above.
