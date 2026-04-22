# pg_redis

A PostgreSQL extension that lets you connect to Postgres using the **Redis protocol (RESP2)**. Drop-in Redis wire compatibility backed by real SQL tables — with two durability modes to match your performance needs.

Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

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

Switch to a durable, SQL-visible database with an odd-numbered `SELECT`:

```bash
127.0.0.1:6379> SELECT 1
OK
127.0.0.1:6379> SET greeting "durable hello"
OK
```

```sql
SELECT key, value, expires_at FROM redis.kv_1;
```

## How it works

`pg_redis` starts a pool of TCP background workers inside PostgreSQL that listen for Redis clients on port 6379. Incoming RESP2 commands are parsed and translated to SPI queries against regular PostgreSQL tables inside the `redis` schema, **or** handled entirely in shared memory with no transaction overhead.

Data is stored across 16 databases (0–15), mirroring Redis's native database model:

| DB numbers | Backend | Durability |
|------------|---------|------------|
| Even (0, 2 … 14) | Shared-memory hash tables (default) | Lost on restart |
| Odd (1, 3 … 15) | WAL-logged PostgreSQL tables | Survives crashes |

See [Storage modes](storage-modes.md) for the full breakdown.

## Next steps

- [Installation](installation.md) — build from source and enable the extension
- [Configuration](configuration.md) — GUC reference and worker tuning
- [Commands](commands.md) — supported Redis commands with behaviour notes
- [Command coverage](command-coverage.md) — full compatibility matrix
