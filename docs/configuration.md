# Configuration

## GUC reference

| GUC | Default | Description |
|-----|---------|-------------|
| `redis.port` | `6379` | TCP port the Redis listener binds to |
| `redis.listen_address` | `127.0.0.1` | IP address to bind on. See [Exposing the listener](#exposing-the-listener) before widening this. |
| `redis.default_db` | `0` | Database new connections start on (0–15). `0` is ephemeral, matching Redis; set `8` to start durable. |
| `redis.workers` | `4` | Number of background worker processes (requires restart) |
| `redis.max_connections` | `128` | Max simultaneous Redis clients per worker |
| `redis.batch_size` | `64` | Max commands coalesced into one transaction; `1` disables batching |
| `redis.password` | _(none)_ | When set, clients must `AUTH <password>` before any command |
| `redis.maxmemory` | `256MB` | Redis's `maxmemory` for the memory engine: past it, a write that could grow memory is refused with `OOM` unless `redis.maxmemory_policy` evicts enough first. Reserved at startup (plus a quarter, at least 32 MB, of headroom) but backed by RAM only as data fills it. `CONFIG SET maxmemory` lowers the limit at runtime, never past this. Requires restart. |
| `redis.snapshot_interval` | `60` | Seconds between snapshots of a `memory+snapshot` database. See [Storage policies](./STORAGE.md#snapshots-phase-4). |
| `redis.maxmemory_policy` | `noeviction` | What memory mode does past `redis.maxmemory`: `noeviction` (refuse the write, as in Redis), `allkeys-random`, or `volatile-ttl`. `CONFIG SET maxmemory-policy` changes it at runtime. See [Memory limit](./STORAGE.md#growable-shared-memory-phase-6). |

Apply at runtime (no restart needed unless noted):

```sql
ALTER SYSTEM SET redis.port = 6380;
ALTER SYSTEM SET redis.listen_address = '0.0.0.0';
ALTER SYSTEM SET redis.default_db = 0;
ALTER SYSTEM SET redis.workers = 8;
ALTER SYSTEM SET redis.password = 'mysecret';
SELECT pg_reload_conf();
```

## Exposing the listener

The Redis port does not go through PostgreSQL authentication. Anything that can
reach it can read and write every `redis.*` table with the background worker's
privileges, regardless of PostgreSQL roles or `pg_hba.conf`.

`redis.listen_address` therefore defaults to `127.0.0.1`. To accept connections
from other hosts — including publishing the port out of a container — widen it
deliberately **and** set a password:

```sql
ALTER SYSTEM SET redis.listen_address = '0.0.0.0';
ALTER SYSTEM SET redis.password = 'mysecret';
SELECT pg_reload_conf();
```

Binding a non-loopback address with no password logs a warning at startup.

## Selecting a database per connection

There are 16 databases. By default they split into two contiguous halves, and
any one can be given another policy at runtime ([Storage policies](./STORAGE.md)):

| Databases | Default policy | Durability |
|-----------|----------------|------------|
| **0–7** | `memory`: shared memory | Lost on restart |
| **8–15** | `logged`: WAL-logged PostgreSQL tables | Survives crashes, replicated |

`SELECT` also accepts the name of each half, so you do not have to remember
where the boundary is — `cache` is database 0 and `durable` is database 8:

```bash
redis-cli> SELECT cache     # same as SELECT 0
redis-cli> SET cache:key value
redis-cli> SELECT durable   # same as SELECT 8
redis-cli> SET user:42 '{"name":"Alice"}'
```

Plain numbers work exactly as in Redis, and `redis.default_db` picks the one new
connections start on.

## Managing workers at runtime

Workers can be added or removed without a server restart. Dynamically added workers are not restarted if terminated; startup workers (configured via `redis.workers`) restart automatically after ~5 seconds.

```sql
SELECT redis.worker_count();   -- how many are running
SELECT redis.add_workers(2);   -- add 2 more dynamically
SELECT redis.remove_workers(2); -- remove 2 (newest first)
```

| Function | Returns | Description |
|----------|---------|-------------|
| `redis.worker_count()` | `bigint` | Number of currently running workers |
| `redis.add_workers(n)` | `integer` | Start n additional workers (no restart needed) |
| `redis.remove_workers(n)` | `integer` | Terminate n workers (newest first) |

To permanently change the pool size, update `redis.workers` and restart the server.
