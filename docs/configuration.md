# Configuration

## GUC reference

| GUC | Default | PostgreSQL context | Description |
|-----|---------|--------------------|-------------|
| `redis.port` | `6379` | superuser | TCP port the Redis listener binds to (1024–65535) |
| `redis.listen_address` | `127.0.0.1` | superuser | IP address to bind on. See [Exposing the listener](#exposing-the-listener) before widening this. |
| `redis.password` | _(none)_ | superuser | When set, clients must `AUTH <password>` before any command. Only superusers (and members of `pg_read_all_settings`) can read it, and `SHOW ALL` leaves it out. |
| `redis.database` | `postgres` | postmaster | The database the workers connect to. Run `CREATE EXTENSION pg_redis` in this database. |
| `redis.default_db` | `0` | superuser | Database new connections start on (0–15). `0` uses the `memory` policy by default, matching Redis's own default database; `8` uses `logged` by default. |
| `redis.workers` | `4` | postmaster | Number of background worker processes (1–64). Each counts against `max_worker_processes`. |
| `redis.max_connections` | `128` | superuser | Max simultaneous Redis clients per worker, so the server accepts up to `redis.workers` times this many |
| `redis.batch_size` | `64` | superuser | Max commands coalesced into one transaction (1–256); `1` disables batching |
| `redis.maxmemory` | `256MB` | postmaster | The memory engine's equivalent of Redis's `maxmemory`. Past it, a write that could grow memory is refused with `OOM` unless `redis.maxmemory_policy` evicts enough first. Reserved at startup (plus a quarter, at least 32 MB, of headroom) but backed by RAM only as data fills it. `CONFIG SET maxmemory` changes the limit at runtime, never past this. |
| `redis.maxmemory_policy` | `noeviction` | superuser | What memory mode does past `redis.maxmemory`: `noeviction` (refuse the write, as in Redis), `allkeys-random`, or `volatile-ttl`. `CONFIG SET maxmemory-policy` changes it at runtime. See [Storage policies](./STORAGE.md). |
| `redis.snapshot_interval` | `60` | sighup | Seconds between snapshots of a `memory+snapshot` database (1–86400). See [Storage policies](./STORAGE.md). |

## When a change takes effect

Each worker reads every `redis.*` setting once, when it starts, and does not re-read the
configuration on `pg_reload_conf()`. A change takes effect when the workers next start, which
for the workers started with the server means a server restart.

The context column is what PostgreSQL itself allows. `postmaster` settings can only change
at server start. `superuser` and `sighup` settings are accepted on a reload, and `SHOW` reports
the new value, but the running workers keep the one they started with.

Change settings with `ALTER SYSTEM`, then restart:

```sql
ALTER SYSTEM SET redis.workers = 8;
ALTER SYSTEM SET redis.maxmemory = '1GB';
ALTER SYSTEM SET redis.snapshot_interval = 30;
```

```bash
sudo systemctl restart postgresql
```

Two settings can also be changed on a running server, from a Redis client. Neither is written
back to the PostgreSQL configuration, so set the GUC as well to keep the value across a
restart:

```bash
redis-cli CONFIG SET maxmemory-policy allkeys-random
redis-cli CONFIG SET maxmemory 512mb    # up to redis.maxmemory, never past it
```

`ALTER SYSTEM` writes to `postgresql.auto.conf` in the data directory in plain text, and that
includes `redis.password`.

## Exposing the listener

The Redis port does not go through PostgreSQL authentication. Anything that can
reach it can read and write every `redis.*` table with the background worker's
privileges, regardless of PostgreSQL roles or `pg_hba.conf`.

`redis.listen_address` therefore defaults to `127.0.0.1`. To accept connections
from other hosts, including publishing the port out of a container, widen it
deliberately **and** set a password, then restart:

```sql
ALTER SYSTEM SET redis.listen_address = '0.0.0.0';
ALTER SYSTEM SET redis.password = 'mysecret';
```

Binding a non-loopback address with no password logs a warning at startup.

## Selecting a database per connection

There are 16 databases. By default they split into two contiguous halves. These are only the
defaults: any database can be given another policy at runtime with `redis.set_policy`
([Storage policies](./STORAGE.md)), and `SELECT * FROM redis.policies()` shows the policy each
one has now.

| Databases | Default policy | Durability under the default |
|-----------|----------------|------------------------------|
| **0–7** | `memory`: shared memory | Lost on restart |
| **8–15** | `logged`: WAL-logged PostgreSQL tables | Survives crashes, replicated |

`SELECT` also accepts the name of each half. `cache` is database 0 and `durable` is
database 8, whatever policy those two databases have:

```bash
redis-cli> SELECT cache     # same as SELECT 0
redis-cli> SET cache:key value
redis-cli> SELECT durable   # same as SELECT 8
redis-cli> SET user:42 '{"name":"Alice"}'
```

Plain numbers work exactly as in Redis, and `redis.default_db` picks the one new
connections start on.

## Managing workers at runtime

Workers can be added or removed without a server restart.

```sql
SELECT redis.worker_count();   -- how many are running
SELECT redis.add_workers(2);   -- add 2 more dynamically
SELECT redis.remove_workers(2); -- remove 2 (newest first)
```

| Function | Returns | Description |
|----------|---------|-------------|
| `redis.worker_count()` | `bigint` | Number of currently running workers |
| `redis.add_workers(n)` | `integer` | Start n additional workers (no restart needed). Run it in the extension's database: the new workers connect to the database it is called from. |
| `redis.remove_workers(n)` | `integer` | Terminate n workers (newest first) |

No pg_redis worker is restarted by PostgreSQL once it exits, whether it was started with the
server or added with `redis.add_workers`. A worker removed with `redis.remove_workers` stays
gone until the next server restart or `redis.add_workers` call. To permanently change the pool
size, update `redis.workers` and restart the server.

`redis.add_workers`, `redis.remove_workers` and `redis.set_policy` raise
`permission denied: superuser required` for any other role.
