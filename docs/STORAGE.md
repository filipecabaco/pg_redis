# Storage policies

How a database's data is kept (in shared memory or in WAL-logged PostgreSQL
tables) and how it moves from one to the other while clients keep using it.
This replaces the fixed split where the database number decided: 0–7
ephemeral, 8–15 durable.

Status: phases 1 (one routing decision, per database), 2 (policies), 3
(moving into and out of memory), 4 (snapshots), 5 (commands written once) and
6 (growable shared memory) are built. Each section says which phase builds it.
An `unlogged` policy (`UNLOGGED` tables) existed in phase 2 and was removed:
it was slower than memory and not durable, so it was never the best choice.

## Which policy?

| Workload | Policy | Why |
|---|---|---|
| Cache, memoised query results | `memory` | Losing it on a restart costs only recomputation. Set `redis.maxmemory_policy` to `allkeys-random` or `volatile-ttl` so a full cache evicts keys; the default, `noeviction`, refuses writes past `redis.maxmemory` with `OOM`. |
| Rate limits | `memory` | Short-lived counters with a TTL. Losing them on a restart only resets the limits. |
| Sessions | `memory+snapshot`, or `logged` if losing any is unacceptable | Served from memory and copied to a logged table every `redis.snapshot_interval` seconds (60 by default), so a crash loses at most that interval of changes. `logged` loses none. |
| Job queues, anything you cannot lose | `logged` | Survives a crash, is replicated and can be queried in SQL. Every write commits to the WAL, so writes are much slower than in memory; see [Performance](IMPLEMENTATION.md#8-performance). |
| Pub/sub | any | Pub/sub does not depend on storage policies. Messages pass through ring buffers in shared memory and are not stored, unless `redis.route_publish` routes a channel into a table ([§4](IMPLEMENTATION.md#4-pubsub)). |

## Policies

Every database has one policy, mirrored in shared memory where every worker
reads it per command. `redis.db_policy` records the ones that were set; a
database with no row there keeps its default.

| Policy | Where the data is | Survives a crash | Replicated | SQL-visible |
|---|---|---|---|---|
| `memory` | a shared-memory slot | no | no | no |
| `memory+snapshot` | a shared-memory slot, copied to a logged table on an interval | up to the last snapshot | the snapshot | the snapshot |
| `logged` | WAL-logged tables | yes | yes | yes |

The defaults reproduce the old layout, so nothing changes for a server that
never sets a policy: 0–7 `memory` and 8–15 `logged`. Every database has
WAL-logged tables; they stay empty while its data is in memory.

```sql
SELECT * FROM redis.policies();             -- every database and its policy
SELECT redis.set_policy(3, 'logged');       -- db 3 becomes durable
SELECT redis.set_policy(9, 'memory');       -- db 9 becomes a cache
```

### Memory slots

The arena (phase 6) holds sixteen slots, one
database each, so every database can be in memory at once. A database takes a
slot when it becomes `memory` and gives it back when it stops being `memory`.
A slot is not tied to a database number; database 12 can live in slot 0. A
slot costs nothing until keys are written into it, because the slots share one
arena and one `redis.maxmemory`.

## Changing policy

### Between `memory` and `memory+snapshot`

Nothing moves, and the data stays in its slot. Snapshots either start, with
the first due at once, or stop, and the snapshot is dropped.

### Between shared memory and tables (phase 3)

The data has to move, and the database stays in use while it does. The move
is lazy plus a sweep:

1. `set_policy` records the change as *migrating, from A to B* and returns.
2. From then on every command on the database runs against **B**. Before it
   runs, each key it names is moved if it is still in A (*move on touch*).
3. A background sweeper moves the keys nobody touches, in batches.
4. When A is empty the policy becomes B and the migration ends.

Asking a migrating database for the policy it is leaving turns the migration
round: A and B swap, and the keys already moved are moved back. Asking for a
third policy mid-move is refused until it finishes. A restart in the middle
carries on, from `redis.db_policy.from_policy`; a memory source does not
survive the restart, so for it there is nothing left to move.

#### Moving one key

A key is moved with the same serialisation `DUMP` and `RESTORE` use, which
round-trips every type including streams, and its deadline is carried
alongside:

- If the key is live in **B**, B is authoritative. Any copy left in A is
  deleted.
- Otherwise, if it is live in **A**: restore it into B with its deadline.
- Then, either way, A's copy is deleted, including a dead one, so the source
  empties.

Every move runs in the dispatcher, outside any command's transaction, in two
transactions of its own: the copies into B commit in the first, and only then
is A deleted from, in the second. An abort can therefore leave a key in both
places but never in neither, and the next touch settles it in B's favour.
Moves of one key never run at once: a striped lock in shared memory, keyed by
the key's hash, serialises them across workers, taken in stripe order so two
movers cannot deadlock. Commands do not take it.

#### When keys move

- **Before a command runs**, its connection names the keys it touches and the
  dispatcher moves them first. The keys come from the command line through
  Redis's key specs (`COMMAND GETKEYS`'s logic), plus the destination of
  `MOVE` and `COPY … DB`. `EXEC` does this for every queued command.
- **A background sweep** moves the rest, a time-boxed slice each round of
  every worker's loop.
- **A command that cannot name its keys in advance, or has to see the whole
  database**, drains it first by running the migration to the end. These are
  scripts (`EVAL`, `FCALL` and their kin, which may `SELECT` elsewhere and
  touch any key), `SORT … BY`/`GET`, `KEYS`, `SCAN`, `DBSIZE`, `RANDOMKEY`,
  `FLUSHDB`, `FLUSHALL`, and `SWAPDB` of a migrating database. The cost is
  paid once, by whichever of them comes first.

When A is empty the migration ends: whatever dead entries A still held are
cleared (the slot flushed, or the tables truncated) and the policy is final.
A memory source that still counts keys it cannot list (a collection whose name
the pool had no room to record) keeps the migration open and writes a warning to
the server log instead of flushing them.

#### What a client can observe

Only latency. A command's first touch of an unmoved key pays for the copy, and
the first command that drains pays for the rest. Moves raise no keyspace
events, do not break a `WATCH` and do not invalidate tracking, because the
key's value is the same before and after.

## Snapshots (phase 4)

`memory+snapshot` is `memory` plus a copy of the slot in `redis.snapshot` (one
row per key, with its `DUMP` payload and its deadline), replaced whole every
`redis.snapshot_interval` seconds (60 by default) and reloaded into the slot
when the server starts. It plays the role of Redis's RDB: a crash loses at most
one interval of writes.

- **Taking one.** One worker claims the database and replaces its snapshot in
  a single transaction, so a crash mid-snapshot leaves the previous one whole.
  A `SAVE` arriving meanwhile waits for that transaction (an advisory lock
  per database) and then replaces it in turn.
  Unlike Redis's forked child the copy is not one instant: each key is read
  whole, but a key written while the snapshot runs may land on either side of
  it. Keys already past their deadline are left out.
- **Reloading.** After a start (a restart, or the crash recovery that resets
  shared memory), the first worker fills the slot back in from the snapshot
  while the others wait, so no worker serves the database half loaded. No
  snapshot is taken of a slot until that is done; otherwise the empty slot
  would be saved over the snapshot it is about to be filled from.
- **On demand.** `SAVE` snapshots every `memory+snapshot` database there and
  then; `BGSAVE` makes them due at the workers' next round. `LASTSAVE` and
  `INFO persistence`'s `rdb_last_save_time` report the last one taken. With no
  such database there is nothing to save, and they answer as before.
- **Changing policy.** Between `memory` and `memory+snapshot` nothing moves:
  the first snapshot is due at once, or the snapshot is dropped. To or from
  the tables it migrates like `memory`, and the snapshot is dropped once the
  data has left memory.

## Commands written once (phase 5)

Strings, hashes, lists, sets, sorted sets and the keyspace commands (`DEL`,
`EXISTS`, `TYPE`, the expiry family, `RENAME`, `KEYS`, `SCAN`, `DBSIZE`,
`RANDOMKEY`, `FLUSHDB`, …) are written once, in `src/store.rs`, against a
`Store` trait with two implementations: `memdb::Db` over a shared-memory slot
and `SqlStore` over a database's tables. Argument checks, edge cases and
replies live in the shared code; the backends only store.

- **Where the line is drawn.** Each `Store` operation is atomic in its backend
  (one statement, or one critical section under the database's lock). The
  trait therefore works at the level of what a command does to a key
  (`hincrby`, `zadd`, `lmove`). It does not offer separate reads and writes for
  the shared code to compose: an ordinary command holds its database's
  server-wide lock shared, so another worker's command can run between two
  such calls. Only what differs per backend sits below it.
- **What it fixed.** Writing a command twice had let the halves drift. Moving
  each family surfaced and removed the differences: SQL `HINCRBYFLOAT`,
  `ZINCRBY`/`ZADD INCR`, `INCRBYFLOAT` and `SETBIT` are now read-modify-write
  under a row lock, where before they raced; SQL `ZINCRBY` no longer stores
  `NaN`; `HMGET`/`MGET` with a repeated key, `HINCRBY` overflow, memory
  `ZRANGE`'s argument errors and float formatting now answer the same on both
  halves.
- **What was already single.** Streams share one implementation through
  their own interface. Commands built from other commands (geo,
  HyperLogLog, `SORT`, `LCS`, `BITOP`, `MOVE`, `ZUNIONSTORE` and the rest of
  `src/composite.rs`) run their parts through whichever backend serves each
  database. `FLUSHALL`, `COPY`, `DUMP` and `RESTORE` run on the SQL path,
  which reaches either half. The ones that make more than one call hold every
  database they touch exclusive for the whole command, so they are atomic
  across workers on both halves
  ([Atomicity across workers](IMPLEMENTATION.md#atomicity-across-workers)).

## Growable shared memory (phase 6)

The memory engine no longer lives in fixed-size PostgreSQL hash tables. Each
database is a `memdb::Db`: one dictionary from key to value, whose values are
ordinary collections (a hash table for hashes and sets, a double-ended list
for lists, a dictionary plus sorted blocks for sorted sets). Every buffer is
allocated from one arena in PostgreSQL's main shared segment.

- **Grows with the data, up to `redis.maxmemory`.** The arena is reserved at
  startup at `redis.maxmemory` plus headroom, but the operating system backs
  a page only once it is written, so a nearly empty server uses next to
  nothing. There is no per-type entry limit, no 64 KiB value cap and no
  key-length limit: a key or value can be as long as Redis allows (512 MB).
- **The limit, as in Redis.** Before a write that could grow memory (Redis's
  `denyoom` commands), use is checked against the limit. Past it the write is
  refused with `OOM`, unless `redis.maxmemory_policy` can evict enough first:
  `allkeys-random` drops random keys, `volatile-ttl` the soonest to expire
  among a sample of keys with a deadline. As in Redis, the limit is checked
  before the command, so one large write can overshoot it; the headroom (a
  quarter of the limit, at least 32 MB) is what it overshoots into.
  `CONFIG SET maxmemory` lowers the limit at runtime, `CONFIG GET maxmemory`
  and `INFO memory` report it, and `used_memory` is the arena's real use.
- **One lock per command.** A memory command takes its database's lock once
  and holds it for the type check and the whole operation, so no other worker
  can interleave. The fixed tables locked once per table touched. Scripts,
  `EXEC` and multi-step commands also hold the database's server-wide lock
  exclusive for their whole run, so their steps do not interleave with other
  workers' commands either.
- **Memory safety.** `src/memdb` is safe Rust, generic over its allocator and
  unit-tested on the ordinary heap. All the `unsafe` is in `src/shm.rs`: the
  arena, the allocator handle and the lock that hands out a `&mut Db`. An
  allocation the arena cannot satisfy unwinds to that lock, which turns it
  into an `OOM` reply. The collections are panic-safe, so the database stays
  consistent and is never left half-written.

Dynamic shared memory was not used because a DSM segment is mapped at a
different address in every process, so nothing inside it could hold a pointer
and every structure would have to be hand-written over offsets. The main
segment is mapped before the postmaster forks, so it has one address
everywhere (each worker checks this when it starts). That is what lets the
engine use ordinary collections.
