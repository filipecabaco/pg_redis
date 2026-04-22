# Configuration

## GUC reference

| GUC | Default | Description |
|-----|---------|-------------|
| `redis.port` | `6379` | TCP port the Redis listener binds to |
| `redis.listen_address` | `0.0.0.0` | IP address to bind on |
| `redis.use_logged` | `false` | Default database for new connections (`true` = db 1, `false` = db 0) |
| `redis.workers` | `4` | Number of background worker processes (requires restart) |
| `redis.max_connections` | `128` | Max simultaneous Redis clients per worker |
| `redis.batch_size` | `64` | Max commands coalesced into one transaction; `1` disables batching |
| `redis.password` | _(none)_ | When set, clients must `AUTH <password>` before any command |
| `redis.storage_mode` | `memory` | Storage backend for even-numbered databases. `memory` = shared-memory hash tables (default); `auto` = UNLOGGED tables. Requires restart. See [Storage modes](./storage-modes.md). |
| `redis.mem_max_entries` | `16384` | Maximum keys per data type per even-db in memory mode. Requires restart. |

Apply at runtime (no restart needed unless noted):

```sql
ALTER SYSTEM SET redis.port = 6380;
ALTER SYSTEM SET redis.listen_address = '127.0.0.1';
ALTER SYSTEM SET redis.use_logged = false;
ALTER SYSTEM SET redis.workers = 8;
ALTER SYSTEM SET redis.password = 'mysecret';
SELECT pg_reload_conf();
```

## Selecting a database per connection

There are 16 databases (0–15). Even databases use unlogged tables; odd databases use WAL-logged tables. Switch with `SELECT <db>`:

```bash
redis-cli> SELECT 0   # unlogged — fast, no WAL
redis-cli> SET cache:key value
redis-cli> SELECT 1   # logged — durable
redis-cli> SET user:42 '{"name":"Alice"}'
```

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
