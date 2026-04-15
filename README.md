# pg_redis

A PostgreSQL extension that lets you connect to Postgres using the **Redis protocol (RESP2)**. Drop-in Redis wire compatibility backed by real SQL tables — with two durability modes to match your performance needs.

Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

---

## How it works

`pg_redis` starts a pool of TCP background workers inside PostgreSQL that listen for Redis clients on port 6379. Incoming RESP2 commands are parsed and translated to SPI queries against regular PostgreSQL tables inside the `pg_redis` schema.

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
| `redis.password` | _(none)_ | When set, clients must `AUTH <password>` before any command |

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
| `SET key value [EX sec] [PX ms]` | Upsert with optional TTL |
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

Benchmarks use [`redis-benchmark`](https://redis.io/docs/management/optimization/benchmarks/) — the same tool used to benchmark Redis itself. It measures throughput (ops/sec) across a pool of parallel connections with configurable pipelining.

### Run with Docker (recommended)

Builds the extension, starts a fresh postgres container, and runs `redis-benchmark` against it:

```bash
mise run bench
```

This runs five passes:

| Pass | Config | Commands |
|------|--------|----------|
| Baseline | 50 clients, 100k requests, no pipeline | PING, SET, GET |
| Pipelined | 50 clients, 100k requests, P=16 | PING, SET, GET |
| Logged tables | 50 clients, 100k requests | SET, GET |
| Unlogged tables | 50 clients, 100k requests | SET, GET |
| Hash ops | 50 clients, 100k requests | HSET, HGET |

Logged vs Unlogged is the primary comparison: unlogged tables skip WAL writes entirely, trading crash durability for throughput.

### Run against a local instance

If you have a running `cargo pgrx run` session:

```bash
redis-benchmark -h localhost -p 6379 -n 100000 -c 50 -q -t ping,set,get
```

Customise freely — `redis-benchmark --help` lists all flags. Useful ones:

```
-n  total requests       (default 100000)
-c  parallel clients     (default 50)
-P  pipeline depth       (default 1 — disables pipelining)
-d  value size in bytes  (default 3)
-t  commands to test     (comma-separated)
-q  quiet output         (one line per command)
--csv                    machine-readable output
```

### Results (4 workers, Docker on Apple M-series)

Measured with `redis-benchmark` against a Docker container running `postgres:17-alpine` with 4 background workers (`redis.workers=4`).

| Pass | Command | Throughput |
|------|---------|-----------|
| Baseline (c=50, no pipeline) | PING | ~156,000 rps |
| Baseline (c=50, no pipeline) | SET  | ~1,230 rps  |
| Baseline (c=50, no pipeline) | GET  | ~58,000 rps  |
| Pipelined (c=50, P=16) | PING | ~19,500 rps |
| Pipelined (c=50, P=16) | SET  | ~1,240 rps  |
| Pipelined (c=50, P=16) | GET  | ~19,000 rps  |
| Logged tables (db 1) | SET | ~1,210 rps |
| Logged tables (db 1) | GET | ~55,500 rps |
| Unlogged tables (db 0) | SET | ~970–1,280 rps |
| Unlogged tables (db 0) | GET | ~55,500 rps |
| Hash operations | HSET | ~1,230 rps |

**Key observations:**

- **PING / GET** are fast (~55–156k rps) because they require no WAL write or heavy locking.
- **SET / HSET** are bounded by per-transaction PostgreSQL commit overhead (~35–50 ms p50), giving ~1,200 rps regardless of pipelining.
- **Pipelining** improves PING throughput from background workers sharing the port, but cannot help SET because each write must commit before the response is sent.
- **Unlogged tables** (db 0) show similar SET throughput to logged tables in this environment; the WAL skip benefit is most visible on storage-bound hardware.

### Expected behaviour

Each background worker serialises SPI calls through a single dispatcher thread. Throughput is bounded by single-core PostgreSQL transaction commit rate for writes (~1,200 SET/s per worker). GET throughput scales with read concurrency across workers. Adding more workers via `redis.workers` or `redis.add_workers()` increases read parallelism and connection capacity without affecting per-write latency.

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

