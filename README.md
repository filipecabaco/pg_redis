<div align="center">
  <img src="logo.svg" alt="pg_redis" width="600"/>
</div>

A PostgreSQL extension that lets you connect to Postgres using the **Redis protocol (RESP2)**. Drop-in Redis wire compatibility with three storage modes to match your performance and durability needs.

Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

## How it works

`pg_redis` starts a pool of TCP background workers inside PostgreSQL that listen for Redis clients on port 6379. Incoming RESP2 commands are parsed and dispatched to one of three storage backends:

- **Memory** (default, even-numbered databases) — shared-memory hash tables, no transaction overhead, near-Redis throughput
- **SPI/unlogged** (even-numbered databases, `storage_mode=auto`) — PostgreSQL UNLOGGED tables via SPI, survives worker restarts
- **SPI/logged** (odd-numbered databases) — WAL-logged PostgreSQL tables, full crash durability and SQL visibility

Data is stored across 16 databases (0–15), mirroring Redis's native database model.

## Performance

Benchmarked with `redis-benchmark -n 50000 -c 200` on Docker, Apple M-series:

| Command | Redis 7 | pg_redis (memory) | pg_redis (SPI/logged) |
|---------|---------|-------------------|-----------------------|
| SET     | 224,000 | **103,000**       | 12,000                |
| GET     | 243,000 | **106,000**       | 92,000                |
| INCR    | 253,000 | **116,000**       | 14,000                |
| ZADD    | 185,000 | **118,000**       | 7,000                 |
| SADD    | 194,000 | 82,000            | 66,000                |

Memory mode reaches ~50–70% of Redis throughput for writes and reads. See [Benchmarks](./docs/benchmarks.md) for the full table and tuning options.

## Quick start

```bash
redis-cli -p 6379

127.0.0.1:6379> SET greeting "hello from postgres"
OK

127.0.0.1:6379> GET greeting
"hello from postgres"

127.0.0.1:6379> SET session:token abc123 EX 3600
OK
```

To use a durable, SQL-visible database, switch to an odd-numbered db:

```bash
127.0.0.1:6379> SELECT 1
OK
127.0.0.1:6379> SET greeting "hello from postgres"
OK
```

```sql
SELECT key, value, expires_at FROM redis.kv_1;
```

## Documentation

Full docs at **[filipecabaco.github.io/pg_redis](https://filipecabaco.github.io/pg_redis/)**.

- [Installation](./docs/installation.md) — requirements, building from source, enabling the extension
- [Configuration](./docs/configuration.md) — GUC reference, database selection, worker management
- [Commands](./docs/commands.md) — supported Redis commands
- [Storage modes](./docs/storage-modes.md) — in-memory mode, logged vs unlogged tables
- [Pub/Sub table routing](./docs/pubsub.md) — routing PUBLISH to PostgreSQL tables
- [Benchmarks](./docs/benchmarks.md) — performance results and batch size tuning
- [Development](./docs/development.md) — running tests, local dev workflow, schema
