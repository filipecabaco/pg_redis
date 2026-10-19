---
template: home.html
title: pg_redis — Redis, spoken by Postgres
hide:
  - navigation
  - toc
---

<!-- Rendered by overrides/home.html. This body is not shown on the page but
     still feeds the site search, so keep it accurate. -->

# pg_redis

A PostgreSQL extension that lets you connect to Postgres using the **Redis protocol (RESP2 and RESP3)**. Existing Redis clients work unchanged. Data lives in shared memory or in PostgreSQL tables, chosen per database to match your performance and durability needs.

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

Switch to a durable, SQL-visible database with `SELECT durable`:

```bash
127.0.0.1:6379> SELECT durable
OK
127.0.0.1:6379> SET greeting "durable hello"
OK
```

```sql
SELECT key, value, expires_at FROM redis.kv_8;
```

## How it works

`pg_redis` starts a pool of TCP background workers inside PostgreSQL that listen for Redis clients on port 6379. Incoming RESP2 commands are parsed and translated to SPI queries against regular PostgreSQL tables inside the `redis` schema, or handled entirely in shared memory with no transaction overhead.

Data is stored across 16 databases (0–15), mirroring Redis's native database model. Each has
its own storage policy, which can be changed at runtime while clients keep using it:

| Policy | Where the data is | Survives a crash | Default for |
|--------|-------------------|------------------|-------------|
| `memory` | Shared memory, bounded by `redis.maxmemory` | No | 0–7 (`SELECT cache`) |
| `memory+snapshot` | Shared memory, snapshotted to a logged table | Up to the last snapshot | |
| `logged` | WAL-logged PostgreSQL tables | Yes | 8–15 (`SELECT durable`) |

```sql
SELECT * FROM redis.policies();
SELECT redis.set_policy(3, 'logged');
```

See [Storage policies](STORAGE.md) for migration and snapshots.

## Next steps

- [Installation](installation.md): Docker, an existing server, or from source
- [Configuration](configuration.md): GUC reference and worker tuning
- [Storage policies](STORAGE.md): per-database storage, online migration, snapshots
- [Commands](commands.md): supported Redis commands with behaviour notes
- [Command coverage](command-coverage.md): full compatibility matrix
- [Implementation](IMPLEMENTATION.md): architecture, the shared-memory backend, limits, testing, open work
