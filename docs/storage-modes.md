# Storage modes

## Overview

Data is stored across 16 databases (0–15), mirroring Redis's native database model:

| DB numbers | Tables | Durability |
|------------|--------|------------|
| **Odd** (1, 3, 5 … 15) | `redis.kv_1`, `redis.hash_1`, … | WAL-backed, survives crashes |
| **Even** (0, 2, 4 … 14) | `redis.kv_0`, `redis.hash_0`, … | Configurable — see below |

Even-numbered databases have two storage backends controlled by `redis.storage_mode`.

## memory (default)

Even-numbered databases bypass PostgreSQL's SPI layer entirely. Commands are served from shared-memory hash tables (`ShmemInitHash`) with no transaction overhead, no WAL, and no buffer pool involvement. This is the default because it gives the closest performance profile to Redis.

`redis.storage_mode` is a `postmaster`-scope GUC — a PostgreSQL restart is required to change it.

## auto

Even-numbered databases use UNLOGGED PostgreSQL tables — same SPI/transaction path as odd databases but without WAL writes. Use this when you need even-db data to be SQL-visible or when values exceed the memory mode size limits.

```ini
# postgresql.conf
redis.storage_mode = 'auto'
redis.use_logged   = false    # keep db 0 as the default
```

## Comparison

| | `memory` (default) | `auto` |
|---|---|---|
| Even-db storage | Shared-memory HTAB per data type | UNLOGGED PostgreSQL tables |
| Transaction | None | `BEGIN` / `COMMIT` per batch |
| SPI overhead | None | Yes |
| Survives crash | No (shared memory, lost) | No (UNLOGGED, truncated) |
| SQL-visible | Returns nothing | `SELECT * FROM redis.kv_0` returns data |
| Max keys | `redis.mem_max_entries` per KV db (default 16,384) | Unlimited (disk) |
| Max value size | 512 bytes | Unlimited (TOAST) |
| Key size limit | 127 bytes | Unlimited |

## Logged tables (odd-numbered databases)

Odd-numbered databases (1, 3, 5 … 15) always use WAL-logged tables regardless of `storage_mode`.

| | Logged (odd) | memory (even, default) |
|---|---|---|
| WAL writes | Yes | No |
| Survives crash | Yes | No |
| Replication | Yes | No |
| SQL-visible | Yes | No |
| Write speed | Standard | ~Redis-level |

Use odd-numbered databases (`SELECT 1`) for data that must survive a PostgreSQL restart or needs to be queryable via SQL.

## When to use each mode

**Default (`memory`)** — ephemeral caches, session tokens, rate-limit counters. Closest to Redis behaviour.

**`auto`** — even-db data you want queryable via SQL joins, or keys/values exceeding the inline limits.

**Odd databases** — data that must survive crashes and participate in replication.

## Notes on memory mode

- Data is **lost on PostgreSQL restart**, crash, or `DROP EXTENSION`.
- `SELECT * FROM redis.kv_0` returns nothing — use `redis-cli` to inspect in-memory data.
- `pg_dump` does not capture in-memory databases.
- All in-memory operations are protected by per-database LWLocks. Multiple workers can safely operate on the same in-memory database concurrently.
