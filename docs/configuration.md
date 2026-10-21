# Configuration

## GUC reference

| GUC | Default | PostgreSQL context | Description |
|-----|---------|--------------------|-------------|
| `redis.port` | `6379` | postmaster | TCP port the Redis listener binds to (1024–65535) |
| `redis.listen_address` | `127.0.0.1` | postmaster | IP address to bind on. See [Exposing the listener](#exposing-the-listener) before widening this. |
| `redis.password` | _(none)_ | superuser | The `default` user's password, Redis's `requirepass`. When set, clients must `AUTH` before any command. Only superusers (and members of `pg_read_all_settings`) can read it, and `SHOW ALL` leaves it out. See [Users](#users). |
| `redis.aclfile` | _(none)_ | postmaster | Redis's `aclfile`: the ACL users loaded at start and by `ACL LOAD`, and written by `ACL SAVE`. Relative to the data directory. Unset, `ACL LOAD` and `ACL SAVE` refuse, as in Redis. See [Users](#users). |
| `redis.database` | `postgres` | postmaster | The database the workers connect to. Run `CREATE EXTENSION pg_redis` in this database. |
| `redis.default_db` | `0` | superuser | Database new connections start on (0–15). `0` uses the `memory` policy by default, matching Redis's own default database; `8` uses `logged` by default. |
| `redis.workers` | `4` | postmaster | Number of background worker processes (1–64). Each counts against `max_worker_processes`. |
| `redis.max_connections` | `128` | superuser | Max simultaneous Redis clients per worker, so the server accepts up to `redis.workers` times this many |
| `redis.batch_size` | `64` | superuser | Max commands coalesced into one transaction (1–256); `1` disables batching |
| `redis.maxmemory` | `256MB` | postmaster | The memory engine's equivalent of Redis's `maxmemory`. Past it, a write that could grow memory is refused with `OOM` unless `redis.maxmemory_policy` evicts enough first. Reserved at startup (plus a quarter, at least 32 MB, of headroom) but backed by RAM only as data fills it. `CONFIG SET maxmemory` changes the limit at runtime, never past this. |
| `redis.pubsub_memory` | `128MB` | postmaster | The shared heap for pub/sub subscriptions and undelivered messages, the client tracking table, and the ACL users and log. Nothing in it has a size limit of its own, as in Redis; a command that needs more than is left gets `OOM`. Reserved at startup but backed by RAM only as it fills. A slow subscriber is disconnected by `client-output-buffer-limit pubsub` long before that, as in Redis. |
| `redis.maxmemory_policy` | `noeviction` | superuser | What memory mode does past `redis.maxmemory`, with Redis's eight values: `noeviction` (refuse the write, as in Redis), `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random` or `volatile-ttl`. Any other value is refused. `CONFIG SET maxmemory-policy` changes it at runtime, and `CONFIG SET maxmemory-samples`, `lfu-log-factor`, `lfu-decay-time` and `maxmemory-eviction-tenacity` tune it as in Redis. See [the limit and eviction](./IMPLEMENTATION.md#the-limit-and-eviction). |
| `redis.appendfsync` | `always` | superuser | Redis's `appendfsync`, for the `logged` databases: when a write is acknowledged. `always`: once its WAL is on disk, so an acknowledged write survives a crash. `everysec` or `no`: before, as with `synchronous_commit = off`, for several times the write throughput; PostgreSQL flushes the WAL within three times `wal_writer_delay` (600 ms by default), so a crash can lose the writes acknowledged in that window, and never leaves one half done. Redis's default is `everysec`; this one is `always`, so a logged database keeps its guarantee unless you choose otherwise. `CONFIG SET appendfsync` changes it at runtime. See [the cost of logged databases](./operations.md#the-cost-of-logged-databases). |
| `redis.snapshot_interval` | `60` | sighup | Seconds between snapshots of a `memory+snapshot` database (1–86400). See [Storage policies](./STORAGE.md). |

## When a change takes effect

`postmaster` settings change only at server start: `redis.port`, `redis.listen_address`,
`redis.aclfile`, `redis.database`, `redis.workers` and `redis.maxmemory`. The shared memory and the worker
pool are sized when the server starts. As in Redis, `CONFIG SET port` moves the listener
while the server runs, after checking the new port is free (`port 0` stops listening), and
`CONFIG SET maxmemory` lowers or raises the limit up to `redis.maxmemory`. Neither outlives
a restart, which binds `redis.port` again.

Every other setting takes effect on a reload. Each worker reads the configuration again on
`pg_reload_conf()` (or `SIGHUP`), as a backend does. A new `redis.password` becomes the
`default` user's password, as `CONFIG SET requirepass` does in Redis: it applies to the next
`AUTH` and to new connections, and connections that already authenticated stay authenticated. A new `redis.default_db` and `redis.max_connections` apply to new
connections. A new `redis.batch_size`, `redis.snapshot_interval`, `redis.maxmemory_policy` or
`redis.appendfsync` applies from the worker's next round. A value set with `CONFIG SET
maxmemory-policy` or `CONFIG SET appendfsync` keeps precedence over the setting until the server
restarts.

A setting given on the `postgres` command line (`-c redis.password=…`) outranks
`postgresql.auto.conf`, so `ALTER SYSTEM` cannot change it; that is PostgreSQL's order of
precedence for every setting.

```sql
ALTER SYSTEM SET redis.password = 'new-secret';
ALTER SYSTEM SET redis.snapshot_interval = 30;
SELECT pg_reload_conf();
```

The `postmaster` settings need a restart:

```sql
ALTER SYSTEM SET redis.workers = 8;
ALTER SYSTEM SET redis.maxmemory = '1GB';
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

## Users

pg_redis has Redis 7.2's ACL users: `ACL SETUSER` creates and changes them, `AUTH <user>
<password>` and `HELLO 3 AUTH <user> <password>` log in as one, and every command is checked
against the user's rules. Every worker shares the users, and a change applies to the next
command of every connection.

```bash
redis-cli -a mysecret ACL SETUSER app on '>app-secret' '~app:*' '&app:*' +@all -@dangerous
redis-cli -a mysecret ACL SETUSER reader on '>reader-secret' '%R~*' +@read
redis-cli -a mysecret ACL SAVE
```

The `default` user is the one a connection starts as. Its password is `redis.password`: with
none set, `default` is `on nopass`, and connections need no `AUTH`, as in Redis. `CONFIG SET
requirepass` changes it at runtime.

Users are kept as Redis keeps them: in an ACL file, named by `redis.aclfile` (Redis's
`aclfile`). Without one, `ACL SETUSER` changes only the running server, and `ACL SAVE` and
`ACL LOAD` refuse with Redis's error. With one, the server loads it at start, `ACL SAVE`
writes every user to it and `ACL LOAD` reads it back. A relative path is relative to the
PostgreSQL data directory, as Redis's is to its `dir`:

```
redis.aclfile = 'users.acl'
```

The file is Redis's format, one user per line, so a file from Redis works as it is:

```
user default on #<sha256 of the password> ~* &* +@all
user app on >app-secret ~app:* &app:* +@all -@dangerous
```

`ACL SAVE` writes each password as its SHA-256 (`#<hash>`); a line written by hand may use
`>password`, which is hashed when it is loaded. `ACL LOAD` checks every line first and changes
nothing if one is wrong, with Redis's messages. A file that cannot be loaded at start (missing,
or with an error) stops the server from serving, as Redis will not start: the log says why,
and the workers try again as they restart. An empty file is a valid one.

When the file has a line for `default`, that line is the default user, its password included;
otherwise `redis.password` is. A reload that changes `redis.password` sets it again. The file is
not part of `pg_dump`: back it up with the data directory.

Users and the ACL LOG live in the shared heap `redis.pubsub_memory` sizes. As in Redis, there
is no limit on users or rules, and the log keeps `acllog-max-len` entries.

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

A worker that fails is restarted after 5 seconds, and after a crash anywhere in the cluster
the postmaster starts every worker again once it has reset shared memory. A worker removed
with `redis.remove_workers` exits cleanly and is not restarted; the next server restart brings
the pool back to `redis.workers`. Workers added with `redis.add_workers` restart the same way
but are not re-created by a server restart. To change the pool size for good, set
`redis.workers` and restart.

`redis.add_workers`, `redis.remove_workers` and `redis.set_policy` raise
`permission denied: superuser required` for any other role.
