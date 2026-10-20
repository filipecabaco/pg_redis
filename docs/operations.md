# Operating in production

This page is for the DBA running PostgreSQL with pg_redis loaded. It covers what each storage
policy keeps through restarts and failures, how pg_redis fits into backups, memory sizing, the
cost of `logged` databases, and how to read the data from SQL.

## What survives what

Each of the 16 databases has a storage policy. By default 0–7 are `memory` and 8–15 are
`logged`, and any of them can be changed with `redis.set_policy` ([Storage policies](./STORAGE.md)).
The policy assignments themselves are stored in `redis.db_policy`, a logged table, so they
survive all three events below.

| Policy | Clean restart | Crash | Failover to a physical standby |
|--------|---------------|-------|--------------------------------|
| `memory` | Lost | Lost | Lost |
| `memory+snapshot` | Kept: snapshotted at shutdown, reloaded at start | Reloaded from the last snapshot | Planned switchover: kept. Unplanned: reloaded from the last snapshot the standby had replayed |
| `logged` | Kept | Kept, by WAL replay | Kept, up to what the standby had replayed |

A `memory+snapshot` database is copied to `redis.snapshot` every `redis.snapshot_interval`
seconds (60 by default), and on demand with `SAVE` or `BGSAVE`. `redis.snapshot` is a logged
table, so it is replicated like any other. On a clean shutdown the last worker to stop takes
one more snapshot, after every worker has stopped writing, and PostgreSQL waits for it before
its shutdown checkpoint and streams it to standbys before it stops. A restart or a planned
switchover therefore keeps every write. A crash, or the unplanned loss of the primary, loses
the writes since the last snapshot. The reload happens when the workers start.

The workers are registered to start once recovery has finished, so they do not run on a
standby while it is in recovery. They start when it is promoted.

## Crashes

Each pg_redis worker is a PostgreSQL background worker with access to shared memory. If one
dies on a signal, a segfault for example, the postmaster treats it like any crashed backend:
it terminates every other server process, reinitialises shared memory and runs crash recovery.
That empties every `memory` database and the in-memory copy of every `memory+snapshot` one. A
crash of any other backend does the same.

The workers come back on their own. Each is registered with a five-second restart time, so
once the postmaster has reset shared memory it starts them again; they reload
`memory+snapshot` databases from their snapshots before serving. A worker that exits with an
error is restarted the same way, after five seconds. A worker stopped with
`redis.remove_workers` or `pg_terminate_backend` exits cleanly and is not restarted.

## Backups

`pg_dump` and `pg_dumpall` include pg_redis data. The extension marks every logged table in
the `redis` schema with `pg_extension_config_dump`, so a dump carries the `logged` databases,
the snapshots of `memory+snapshot` databases, the policy table, `FUNCTION` libraries and
pub/sub routes. Restoring it runs `CREATE EXTENSION pg_redis` and then loads those rows. The
script cache (`redis.scripts`) is left out: it is an unlogged cache, which Redis does not
persist either.

Physical backups include the same data, because the tables are ordinary relations in the
data directory: `pg_basebackup`, file-system level backups, point-in-time recovery and
physical replicas.

`memory` databases are in no backup of either kind. A `memory+snapshot` database is backed up
as of its last snapshot; run `SAVE` first to take one.

## Memory

`redis.maxmemory` (256 MB by default) is reserved in PostgreSQL's main shared memory segment at
server start, plus headroom of a quarter of it or 32 MB, whichever is larger. The reservation
counts toward `shared_memory_size`. With the defaults that is about 320 MB on top of
`shared_buffers` and PostgreSQL's own structures. Check the total after changing it:

```sql
SHOW shared_memory_size;
```

Without huge pages, the operating system backs a page of the region only once it is written,
so resident memory grows with the data. With `huge_pages` in use, the whole segment, the
pg_redis region included, is reserved from the huge page pool when the server starts. Size
`vm.nr_hugepages` from `SHOW shared_memory_size_in_huge_pages`.

`redis.maxmemory` needs a restart to change. `CONFIG SET maxmemory` can lower the limit at
runtime, and raise it again up to `redis.maxmemory`.

## The cost of logged databases

A `logged` database stores its data in WAL-logged tables, and every command runs through SQL:

- Every write is a row insert, update or delete, with the WAL record that goes with it. It is
  replicated and archived like any other table change.
- `FLUSHDB` and `FLUSHALL` run `DELETE` on the database's tables. The rows become dead tuples
  that autovacuum has to clean up, and the tables keep their size until it does.
- `LINSERT` moves every element after the insertion point, updating each of those rows twice.
  An `LREM` that removes anything renumbers the whole list, updating every row in it twice.
  Long lists that see these commands produce a lot of WAL and dead tuples.
- Each worker runs an expiry sweep about once a second, deleting expired keys from every
  `logged` database in one statement per database.
- Each worker groups the commands its clients send into one transaction, up to
  `redis.batch_size` commands (64 by default). A client gets its reply after that transaction
  commits, so with `synchronous_commit = on` an acknowledged write is on disk.

Commands on `memory` and `memory+snapshot` databases are served from shared memory and write no
table rows. The snapshot of a `memory+snapshot` database is the exception.

## Querying from SQL

Every database has its own set of tables, with the database number as the suffix. They hold
data only while that database's policy is `logged`. Keys, fields, members and values are
`bytea`.

| Table | Columns | Holds |
|-------|---------|-------|
| `redis.kv_N` | `key`, `value`, `expires_at` | Strings, with their expiry |
| `redis.hash_N` | `key`, `field`, `value`, `seq` | Hashes, one row per field |
| `redis.list_N` | `key`, `pos`, `value` | Lists, one row per element, ordered by `pos` |
| `redis.set_N` | `key`, `member`, `seq` | Sets, one row per member |
| `redis.zset_N` | `key`, `member`, `score` | Sorted sets, one row per member |
| `redis.stream_N` | `key`, `value` | Streams, one serialised value per stream |
| `redis.expiry_N` | `key`, `expires_at` | The expiry of every hash, list, set, sorted set and stream key that has one |
| `redis.encoding_N` | `key`, `encoding` | The encoding of a hash, set or sorted set whose contents no longer show it |

A string's expiry is the `expires_at` column of `redis.kv_N`, `NULL` when it has none. The
other types keep theirs in `redis.expiry_N`, and a key with no expiry has no row there. A key
past its expiry can still be in the tables until the next sweep removes it, so filter on the
expiry when you read:

```sql
-- Strings in database 8
SELECT convert_from(key, 'UTF8') AS key, convert_from(value, 'UTF8') AS value
FROM redis.kv_8
WHERE expires_at IS NULL OR expires_at > now();

-- Hash fields in database 8
SELECT convert_from(h.key, 'UTF8') AS key,
       convert_from(h.field, 'UTF8') AS field,
       convert_from(h.value, 'UTF8') AS value
FROM redis.hash_8 h
LEFT JOIN redis.expiry_8 e ON e.key = h.key
WHERE e.expires_at IS NULL OR e.expires_at > now();
```

A hash's fields and a set's members come back in the order Redis would give
them, and `seq` records it: it is set when a field or member is added and kept
when a field's value changes, so `ORDER BY seq` is insertion order. A set of
integers that Redis would keep as an intset comes back sorted by value
instead. `redis.encoding_N` holds a row only for a key whose encoding its
contents no longer show: `3` for a hash or set that grew past Redis's listpack
limits and stays a hash table, `4` for such a sorted set, and `1` for a
listpack set that holds an integer. `OBJECT ENCODING` reads it
([Order and encodings](IMPLEMENTATION.md#order-and-encodings)). Neither needs
setting by hand: a row written from SQL takes the next `seq`, and a key with
no encoding row is in the encoding its contents imply.

```sql
-- A hash's fields in database 8, in the order HGETALL returns them
SELECT convert_from(field, 'UTF8') AS field, convert_from(value, 'UTF8') AS value
FROM redis.hash_8
WHERE key = convert_to('user:1', 'UTF8')
ORDER BY seq;
```

`memory` and `memory+snapshot` databases have no rows in these tables. A `memory+snapshot`
database's snapshot is in `redis.snapshot`, as one serialised `DUMP` payload per key.

## Security

The Redis port does not go through `pg_hba.conf` or PostgreSQL roles. It binds to
`redis.listen_address`, which defaults to `127.0.0.1`, and requires `AUTH` only when the
`default` user has a password (`redis.password`) or is off. The workers connect as the
superuser created by `initdb`, so a client that logs in as a user with every permission can read
and write every `redis.*` table. The listener speaks plain TCP. pg_redis has no TLS support, so
keep the port on a trusted network.

To give applications less than that, create ACL users with the commands, keys and channels
they need ([Users](./configuration.md#users)) and save them with `ACL SAVE`. Refused commands
and failed logins are listed by `ACL LOG` and counted in `INFO stats`
(`acl_access_denied_*`). Saved users are in `redis.acl_users`, with passwords stored only as
SHA-256 hashes; restrict who can read it as you would any table holding credentials. `ACL
SETUSER`'s arguments are hidden from `SLOWLOG`, as in Redis.

`redis.password` set with `ALTER SYSTEM` is stored in plain text in `postgresql.auto.conf`, and
a reload that changes it writes the new value to the server log
(`parameter "redis.password" changed to ...`).

Before widening the listener, read [Exposing the listener](./configuration.md#exposing-the-listener).
