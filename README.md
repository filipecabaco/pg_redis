# pg_redis

A PostgreSQL extension that lets you connect to Postgres using the **Redis protocol (RESP2)**. Drop-in Redis wire compatibility backed by real SQL tables — with two durability modes to match your performance needs.

Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

---

## How it works

`pg_redis` starts a pool of TCP background workers inside PostgreSQL that listen for Redis clients on port 6379. Incoming RESP2 commands are parsed and translated to SPI queries against regular PostgreSQL tables inside the `pg_redis` schema, **or** handled entirely in shared memory with no transaction overhead (see [In-memory mode](#in-memory-mode)).

Data is stored across 16 databases (0–15), mirroring Redis's native database model. Even-numbered databases use unlogged tables; odd-numbered databases use WAL-logged tables:

| DB numbers | Tables | Durability |
|------------|--------|------------|
| **Odd** (1, 3, 5 … 15) | `redis.kv_1`, `redis.hash_1`, … | WAL-backed, survives crashes |
| **Even** (0, 2, 4 … 14) | `redis.kv_0`, `redis.hash_0`, … | No WAL, faster writes, truncated on crash |

Switch databases per-connection with `SELECT <db>` (0–15), or set the global default via a GUC. Multiple background workers bind to the same port with `SO_REUSEPORT`, distributing connections across the worker pool. An active expiry scan runs every second across all kv tables.

---

## Requirements

- PostgreSQL 15, 16, 17, or 18
- Rust toolchain (stable)
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx) 0.16.1

---

## Installation

### From source

```bash
# Install cargo-pgrx (skip if already installed)
cargo install cargo-pgrx --version "=0.16.1" --locked

# Point pgrx at your postgres installation
cargo pgrx init --pg17 $(which pg_config)

# Build and install into the active PostgreSQL
cargo pgrx install --release --features pg17
```

### Enable the extension

Add to `postgresql.conf`:

```
shared_preload_libraries = 'pg_redis'
```

Then restart PostgreSQL and create the extension:

```sql
CREATE EXTENSION pg_redis;
```

The background workers start automatically. Verify they are running:

```sql
SELECT pid, application_name
FROM pg_stat_activity
WHERE backend_type LIKE 'pg_redis worker%';

-- Or use the built-in helper:
SELECT redis.worker_count();
```

---

## Quick start

Once the extension is loaded, connect with any Redis client:

```bash
redis-cli -p 6379

127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET greeting "hello from postgres"
OK

127.0.0.1:6379> GET greeting
"hello from postgres"

127.0.0.1:6379> SET session:token abc123 EX 3600
OK

127.0.0.1:6379> TTL session:token
(integer) 3600
```

The data is immediately visible inside PostgreSQL:

```sql
SELECT key, value, expires_at FROM redis.kv_1;
--         key        │        value         │          expires_at
-- ────────────────────┼──────────────────────┼──────────────────────────────
--  greeting           │ hello from postgres  │ (null)
--  session:token      │ abc123               │ 2025-04-16 00:07:43+00
```

---

## Configuration

| GUC | Default | Description |
|-----|---------|-------------|
| `redis.port` | `6379` | TCP port the Redis listener binds to |
| `redis.listen_address` | `0.0.0.0` | IP address to bind on |
| `redis.use_logged` | `true` | Default database for new connections (`true` = db 1, `false` = db 0) |
| `redis.workers` | `4` | Number of background worker processes (requires server restart) |
| `redis.max_connections` | `128` | Max simultaneous Redis clients per worker |
| `redis.batch_size` | `64` | Max commands coalesced into one transaction (group commit); `1` disables batching |
| `redis.password` | _(none)_ | When set, clients must `AUTH <password>` before any command |
| `redis.storage_mode` | `auto` | Storage backend for even-numbered databases. `auto` = UNLOGGED PostgreSQL tables. `memory` = shared-memory hash tables, bypassing SPI and transactions entirely. See [In-memory mode](#in-memory-mode). Requires restart. |
| `redis.mem_max_entries` | `16384` | Maximum keys per data type per even-db in memory mode. Controls shared-memory HTAB capacity. Larger values use more RAM proportionally. Requires restart. |

Set in `postgresql.conf` or at runtime:

```sql
ALTER SYSTEM SET redis.port = 6380;
ALTER SYSTEM SET redis.listen_address = '127.0.0.1';
ALTER SYSTEM SET redis.use_logged = false;
ALTER SYSTEM SET redis.workers = 8;
ALTER SYSTEM SET redis.password = 'mysecret';
SELECT pg_reload_conf();
```

### Switching databases per connection

Any of the 16 databases (0–15) can be selected with `SELECT <db>`. Even databases use unlogged tables; odd databases use WAL-logged tables.

```bash
redis-cli> SELECT 0   # unlogged — fast, no WAL
redis-cli> SET cache:key value
redis-cli> SELECT 1   # logged — durable
redis-cli> SET user:42 '{"name":"Alice"}'
redis-cli> SELECT 14  # unlogged db 14
redis-cli> SELECT 15  # logged db 15
```

### Managing workers at runtime

Workers can be added or removed without a server restart. Dynamically added workers are not restarted if terminated; startup workers (configured via `redis.workers`) restart automatically after ~5 seconds.

```sql
-- Check how many workers are running
SELECT redis.worker_count();

-- Add 2 more workers dynamically
SELECT redis.add_workers(2);

-- Remove 2 workers (newest first)
SELECT redis.remove_workers(2);
```

---

## Supported commands

### Connection
| Command | Behaviour |
|---------|-----------|
| `PING [msg]` | Returns `PONG` or echoes `msg` |
| `ECHO msg` | Returns `msg` |
| `SELECT db` | 0–15; even = unlogged, odd = logged |
| `AUTH [password]` | Validates against `redis.password` GUC; no-op when unset |
| `INFO` | Returns server info string |
| `COMMAND` | Returns empty array (client compatibility) |
| `CLIENT ...` | No-op, returns OK |

### Key–value
| Command | Behaviour |
|---------|-----------|
| `GET key` | Returns value or nil if missing/expired |
| `SET key value [NX\|XX] [GET] [EX sec\|PX ms\|EXAT ts\|PXAT ts-ms\|KEEPTTL]` | Upsert with conditional flags and optional TTL |
| `SETEX key seconds value` | SET with seconds TTL |
| `PSETEX key ms value` | SET with milliseconds TTL |
| `MGET key [key ...]` | Bulk get, preserves nil for missing keys |
| `MSET key value [key value ...]` | Bulk upsert |
| `DEL key [key ...]` | Delete keys, returns count deleted |
| `EXISTS key [key ...]` | Returns count of existing keys |

### Expiry
| Command | Behaviour |
|---------|-----------|
| `EXPIRE key seconds` | Set TTL in seconds |
| `PEXPIRE key ms` | Set TTL in milliseconds |
| `EXPIREAT key unix-ts` | Set absolute expiry (unix seconds) |
| `PEXPIREAT key unix-ts-ms` | Set absolute expiry (unix milliseconds) |
| `TTL key` | Remaining TTL in seconds; `-1` no expiry; `-2` missing |
| `PTTL key` | Remaining TTL in milliseconds |
| `PERSIST key` | Remove TTL |
| `EXPIRETIME key` | Absolute expiry as unix timestamp (seconds) |
| `PEXPIRETIME key` | Absolute expiry as unix timestamp (milliseconds) |

### Hashes
| Command | Behaviour |
|---------|-----------|
| `HGET key field` | Returns field value or nil |
| `HSET key field value [field value ...]` | Upsert one or more fields, returns new field count |
| `HDEL key field [field ...]` | Delete fields, returns count deleted |
| `HGETALL key` | Returns interleaved field/value pairs, sorted by field |

> **Note:** Expiry is not supported on hash keys (same behaviour as Redis hash TTLs without `HEXPIRE`).

### Transactions
| Command | Behaviour |
|---------|-----------|
| `MULTI` | Begin a transaction block; subsequent commands are queued |
| `EXEC` | Execute all queued commands atomically; returns array of results; returns nil if a `WATCH`ed key changed |
| `DISCARD` | Discard the queued commands and exit the transaction block |
| `WATCH key [key ...]` | Mark keys to watch; if any are modified before `EXEC`, the transaction aborts |
| `UNWATCH` | Clear all watched keys |

Commands queued inside `MULTI` receive `QUEUED` responses. Runtime errors inside `EXEC` (e.g. `INCR` on a non-integer) are returned as per-command errors in the result array without aborting the remaining commands.

---

---

## In-memory mode

When `redis.storage_mode = 'memory'`, even-numbered databases (0, 2, 4 … 14) bypass PostgreSQL's SPI layer entirely. Commands are served from shared-memory hash tables (`ShmemInitHash`) with no transaction overhead, no WAL, and no buffer pool involvement. Odd-numbered databases continue to use WAL-logged tables unchanged.

```ini
# postgresql.conf
redis.storage_mode = 'memory'
redis.use_logged   = false    # make db 0 the default so clients land in-memory
```

### What it changes

| | `auto` (default) | `memory` |
|---|---|---|
| Even-db storage | UNLOGGED PostgreSQL tables | Shared-memory HTAB per data type |
| Transaction | `BEGIN` / `COMMIT` per batch | None |
| SPI overhead | Yes | None |
| Survives crash | No (UNLOGGED, truncated) | No (shared memory, lost) |
| SQL-visible | `SELECT * FROM redis.kv_0` returns data | Returns nothing |
| Max keys | Unlimited (disk) | `redis.mem_max_entries` per KV db (default 16,384); half that for other types |
| Max value size | Unlimited (TOAST) | 512 bytes (64 B inline + 448 B overflow HTAB) |
| Key size limit | Unlimited | 127 bytes |

### When to use it

Use `memory` for:
- Ephemeral caches, session tokens, rate-limit counters
- Benchmarking and development environments
- Any workload where Redis-like "best-effort" persistence is acceptable

Use `auto` (or odd-numbered databases) for:
- Data you need to survive a PostgreSQL restart
- Data you want queryable via SQL joins
- Keys or values exceeding the inline limits above

### Concurrent safety

All in-memory operations are protected by per-database LWLocks (one per even-db per data type). LPUSH/RPUSH/ZPOPMIN/SPOP maintain secondary metadata HTABs so those operations are O(1) rather than requiring a full table scan. Multiple workers can safely operate on the same in-memory database concurrently.

### Notes

- Data is **lost on PostgreSQL restart**, crash, or `DROP EXTENSION`. This matches Redis's behaviour with `appendfsync no` and is expected.
- `SELECT * FROM redis.kv_0` will return nothing when `storage_mode = 'memory'`. Use `redis-cli` or `redis-benchmark` to inspect in-memory data.
- `pg_dump` does not capture in-memory databases.
- `redis.storage_mode` is a `postmaster`-scope GUC — a PostgreSQL restart is required for the change to take effect.

---

## Logged vs Unlogged tables

| | Logged | Unlogged |
|---|--------|----------|
| WAL writes | Yes | No |
| Survives crash | Yes | No (truncated on recovery) |
| Replication | Yes | No |
| Write speed | Standard | ~2–3× faster |
| DB numbers | Odd (1, 3, 5 … 15) | Even (0, 2, 4 … 14) |
| Default GUC | `redis.use_logged = true` (db 1) | `redis.use_logged = false` (db 0) |

Use **logged** for anything you care about keeping. Use **unlogged** for ephemeral caches, rate-limit counters, or benchmarking where durability is not needed.

---

## Benchmarks

Benchmarks use [`redis-benchmark`](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/) — the same tool used to benchmark Redis itself. It measures throughput (ops/sec) across a pool of parallel connections with configurable pipelining.

### Run the suite

```bash
mise run bench
```

This starts the `bench` Docker profile (pg_redis on `:6379`, Redis 7 on `:6380`, both with password `testpass`), runs the full built-in `redis-benchmark` suite against each one in turn, and tears the stack down.

Both runs use identical flags — `-n 20000 -c 50 -q` — so the output is directly comparable. The built-in suite covers PING, SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP, SADD, HSET, SPOP, ZADD, ZPOPMIN, LRANGE, MSET.

### Run custom commands

For anything outside the built-in suite (ZRANGE, ZRANGEBYSCORE, HGETALL, SMEMBERS, EXPIRE, …), start the stack once and drive `redis-benchmark` yourself:

```bash
docker compose --profile bench up -d --wait

# Anything from the redis-benchmark docs, just swap the port.
redis-benchmark -h localhost -p 6379 -a testpass -n 100000 -c 50 -q -P 16 -t set,get
redis-benchmark -h localhost -p 6380 -a testpass -n 100000 -c 50 -q -P 16 -t set,get

# Arbitrary command with __rand_int__ substitution:
redis-benchmark -h localhost -p 6379 -a testpass -n 50000 -c 50 -q \
  -r 100000 ZADD leaderboard __rand_int__ user:__rand_int__

docker compose --profile bench down
```

Flags worth knowing (`redis-benchmark --help` lists everything):

```
-n  total requests       (default 100000)
-c  parallel clients     (default 50)
-P  pipeline depth       (default 1 — disables pipelining)
-d  value size in bytes  (default 3)
-t  commands to test     (comma-separated)
-r  keyspace length      (enables __rand_int__ substitution)
-q  quiet output         (one line per command)
--csv                    machine-readable output
```

### Results (Docker, Apple M-series)

#### Standard commands (`redis-benchmark`)

Four-way comparison: Redis 7 Alpine · pg_redis defaults · pg_redis high-write · pg_redis memory mode.

| | **Redis 7** | **pg_redis default** | **pg_redis high-write** | **pg_redis memory** |
|-|-------------|---------------------|------------------------|---------------------|
| Workers | — | 4 | 8 | 4 |
| Batch size | — | 64 | 256 | 64 |
| Clients | 50 | 50 | 200 | 200 |
| Requests | 20,000 | 20,000 | 50,000 | 50,000 |
| DB | — | db 1 (logged) | db 1 (logged) | db 0 (in-memory) |
| Run with | — | `mise run bench` | `mise run bench-high-write` | `PG_REDIS_STORAGE_MODE=memory PG_REDIS_USE_LOGGED=false mise run bench-high-write` |

| Command | Redis 7 | pg_redis default | pg_redis high-write | **pg_redis memory** |
|---------|---------|-----------------|---------------------|---------------------|
| PING    | 250,000 | 162,000 | — | — |
| GET     | 250,000 | 94,000 | 100,000 | **100,000** |
| SET     | 189,000 | 5,554 | 8,881 | **7,621** ² |
| INCR    | 194,000 | 6,101 | 9,557 | **11,896** |
| HSET    | 192,000 | 14,015 | 10,730 | **13,576** |
| ZADD    | 192,000 | 5,759 | 6,115 | **5,898** |
| SADD    | 196,000 | 31,008 | 77,280 | **84,890** |
| SPOP    | 206,000 | 24,096 | 70,621 | **71,531** |
| ZPOPMIN | 206,000 | 19,084 | 55,617 | **57,604** |
| LPOP    | 161,000 | 6,250 | 61,050 | **61,050** |
| RPOP    | 185,000 | 12,323 | 60,976 | **57,078** |
| RPUSH   | 278,000 | 1,431 ¹ | — | — |
| LRANGE 100 | 50,000 | 620 | — | — |
| LRANGE 300 | 28,000 | 598 | — | — |

¹ RPUSH/LPUSH bottlenecked by position-finding in the SPI list implementation under concurrent load.
² Memory mode uses even-numbered db 0; the benchmark default (USE_LOGGED=true) uses db 1 (logged), so memory mode may not show the full gain over the previous table.

All throughput figures rounded to nearest integer, requests/second.

**Reading the table:**

- **`auto` mode** is limited by PostgreSQL transaction overhead. WAL-logged tables (default db 1) pay ~8 ms commit latency per batch. Group commit amortises this but doesn't eliminate it.
- **`memory` mode** eliminates the transaction entirely for even-numbered databases. Commands go directly to shared-memory hash tables — no SPI, no transaction, no buffer pool.
- **All read commands** (GET, PING) reach ~100k rps regardless of mode because reads hit the SPI plan cache without a transaction.
- **Pop/scan writes** (SADD, SPOP, ZPOPMIN, LPOP, RPOP) reach 55–85k rps in memory mode — large gains over auto mode.

#### Pub/Sub (`bench_pubsub.ts`, awaited PUBLISH)

Measures end-to-end delivery: time from PUBLISH call to subscriber callback firing. Uses `await pub.publish(...)` (sequential) to avoid overrunning the ring buffer.

| Scenario | Redis 7 | pg_redis | Notes |
|----------|---------|---------|-------|
| 1 pub → 1 sub | 19,503 | 15,511 | 5ms poll interval dominates for sparse traffic |
| 1 pub → 4 subs (fan-out) | 31,481 | 24,751 | Total deliveries/sec |
| 1 pub → 16 subs (fan-out) | 88,265 | 131,769 | pg_redis faster: writes 16 rings under one spinlock |
| PUBLISH (no subscribers) | 11,718 | 36,178 | pg_redis spinlock scan faster than Redis's dict lookup |

Run with:
```bash
mise run bench-pubsub
```

**Reading the pub/sub table:**

- Pub/sub is **fire-and-forget** — no persistence, no ACK, at-most-once delivery, identical semantics to native Redis.
- The **5ms poll interval** in `subscribe_loop` adds up to 5ms delivery latency per message (worst case when the subscriber is mid-timeout when a message arrives). This is the dominant cost for the 1-sub scenario.
- **Fan-out at scale**: pg_redis writes to all matching ring buffers under a single spinlock acquisition, so 16-subscriber fan-out is faster than Redis's per-client linked-list walk with individual output-buffer writes.
- **PUBLISH with no subscribers** is faster on pg_redis because it acquires one cross-process AtomicU8 spinlock, scans 256 slots (mostly zeroed cache lines), and releases — cheaper than Redis's dict hash + callback dispatch overhead under the same load.

### Tuning batch size

`redis.batch_size` controls how many queued commands are coalesced per transaction. The default of 64 caps the batch; the actual fill level under load is limited by how many clients are writing concurrently.

| `redis.batch_size` | Effect |
|--------------------|--------|
| `1` | Disables batching. One transaction per command. Lowest write latency, lowest write throughput. |
| `8–16` | Light coalescing. Good balance for mixed workloads with few concurrent writers. |
| `64` (default) | Aggressive coalescing under high concurrency (50+ clients). Maximises write throughput. |
| `256` | Useful when hundreds of clients write simultaneously. Diminishing returns beyond natural channel fill rate. |

Change at runtime without restart:
```sql
ALTER SYSTEM SET redis.batch_size = 16;
SELECT pg_reload_conf();
```

Workers read the GUC at startup, so newly added workers pick up changes immediately while running workers require a `redis.remove_workers` / `redis.add_workers` cycle or server reload.

### Expected behaviour

Each background worker serialises SPI calls through a single dispatcher thread. After receiving a command, it drains up to `redis.batch_size` additional queued commands and executes them all in one PostgreSQL transaction. Write throughput scales with batch fill rate — more concurrent writers → larger batches → lower per-write commit overhead. GET throughput scales with read concurrency across workers. Adding more workers via `redis.workers` or `redis.add_workers()` increases both read parallelism and the number of independent batch queues.

---

## Running tests

### Unit tests (no postgres required)

```bash
mise run test-unit
# or:
cargo test --features pg17 --lib -- --skip pg_test
```

### Integration tests (pgrx managed postgres)

```bash
mise run test-pg
# or:
cargo pgrx test pg17
```

### End-to-end tests with Docker

Builds the extension, installs it into a fresh postgres container, and runs `redis-cli` against it:

```bash
mise run e2e
# or:
docker compose up --build --abort-on-container-exit --exit-code-from e2e
```

---

## Development

```bash
# Start a local postgres with the extension hot-reloaded
mise run run
# equivalent to:
cargo pgrx run pg17

# Inside the psql session:
CREATE EXTENSION pg_redis;

# In another terminal:
redis-cli -p 6379 ping
```

Format and lint:

```bash
mise run fmt    # cargo fmt
mise run lint   # cargo clippy -D warnings
```

---

## Schema

All tables live in the `redis` schema. There are 16 database slots (0–15): even slots use `UNLOGGED` tables, odd slots use WAL-logged tables. The pattern is the same for all slots:

```sql
-- Key-value (shown for db 0 and db 1; pattern repeats through db 15)
CREATE SCHEMA redis;
CREATE UNLOGGED TABLE redis.kv_0 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE redis.kv_1 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
-- ... kv_2 through kv_15

-- Hash (same pattern)
CREATE UNLOGGED TABLE redis.hash_0 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE redis.hash_1 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
-- ... hash_2 through hash_15
```

All tables are queryable with standard SQL, joinable with your application data, and subject to normal PostgreSQL access control.

## Worker management functions

| Function | Returns | Description |
|----------|---------|-------------|
| `redis.worker_count()` | `bigint` | Number of currently running workers |
| `redis.add_workers(n)` | `integer` | Start n additional workers dynamically (no restart needed) |
| `redis.remove_workers(n)` | `integer` | Terminate n workers (newest first), returns count terminated |

Dynamic workers (added via `add_workers`) do not restart after termination. Startup workers (configured via `redis.workers`) restart automatically after ~5 seconds. To permanently change the pool size, update `redis.workers` and restart the server.

