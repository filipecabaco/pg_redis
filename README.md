<div align="center">
  <img src="logo.svg" alt="pg_redis" width="600"/>

  <p>
    <a href="https://filipecabaco.github.io/pg_redis/"><strong>Website</strong></a> ·
    <a href="https://filipecabaco.github.io/pg_redis/#try-it">Try it in your browser</a> ·
    <a href="https://filipecabaco.github.io/pg_redis/installation/">Install</a> ·
    <a href="https://filipecabaco.github.io/pg_redis/command-coverage/">Commands</a>
  </p>
</div>

A PostgreSQL extension that lets you connect to Postgres using the **Redis protocol (RESP2 and RESP3)**. Existing Redis clients work unchanged, and each database has its own storage policy to match its performance and durability needs.

Built with [pgrx](https://github.com/pgcentralfoundation/pgrx) in Rust.

## How it works

`pg_redis` starts a pool of TCP background workers inside PostgreSQL that listen for Redis clients on port 6379. Each of its 16 databases (0–15, Redis's own model) has a storage policy, changeable at runtime while clients keep using it ([Storage policies](./docs/STORAGE.md)):

- **`memory`** (default for 0–7): an in-memory engine in PostgreSQL shared memory, no transaction overhead, near-Redis throughput
- **`memory+snapshot`**: memory, copied to a logged table on an interval and reloaded at startup, like Redis's RDB
- **`logged`** (default for 8–15): WAL-logged PostgreSQL tables, full crash durability, replication and SQL visibility

`SELECT` accepts `cache` (0) and `durable` (8) as names for the two default halves.

Memory mode keeps its data in PostgreSQL shared memory, bounded by
`redis.maxmemory` (256 MB by default) the same way Redis is bounded by
`maxmemory`. The space is reserved at start but backed by RAM only as data
fills it. Past the limit, a write either evicts or is refused with `OOM`,
depending on `redis.maxmemory_policy`. Keys and values can be as long as Redis
allows. See
[Implementation](./docs/IMPLEMENTATION.md#3-the-shared-memory-backend).

## Performance

`memory` runs close to Redis; `memory+snapshot` keeps pace with it. `logged` reads stay fast,
and by default its writes wait for the WAL to reach disk; `CONFIG SET appendfsync everysec`
trades the last fraction of a second of writes for several times the write rate. The figures come from `mise run bench-site`, on
pinned CPUs, and are shown on the site:
[Throughput](https://filipecabaco.github.io/pg_redis/#throughput) and the full table under
[Performance](https://filipecabaco.github.io/pg_redis/IMPLEMENTATION/#8-performance).

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

To use a durable, SQL-visible database, switch to the durable half:

```bash
127.0.0.1:6379> SELECT durable
OK
127.0.0.1:6379> SET greeting "hello from postgres"
OK
```

```sql
SELECT key, value, expires_at FROM redis.strings WHERE db = 8;
```

## Documentation

The full documentation is at **[filipecabaco.github.io/pg_redis](https://filipecabaco.github.io/pg_redis/)**.

- [Installation](./docs/installation.md): requirements, building from source, enabling the extension
- [Configuration](./docs/configuration.md): GUC reference, database selection, worker management
- [Commands](./docs/commands.md): supported Redis commands
- [Command coverage](./docs/command-coverage.md): full compatibility matrix
- [Implementation](./docs/IMPLEMENTATION.md): architecture, storage backends, the shared-memory
  layout, pub/sub, WATCH, limits, performance, testing and CI
