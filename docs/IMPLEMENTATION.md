# Implementation

How pg_redis works, end to end: the process model, the three storage backends,
the shared-memory layout, pub/sub, WATCH, the limits every one of those imposes,
and how to test and benchmark a change to any of it.

This is the single reference for internals. The pages beside it cover using
the extension: [Installation](installation.md),
[Configuration](configuration.md), [Commands](commands.md) and the
[Command coverage](command-coverage.md) matrix, which is machine-checked
against the parser by a unit test and so has to stay a page of its own.

---

## 1. Architecture

### Process model

`shared_preload_libraries = 'pg_redis'` registers a pool of PostgreSQL
background workers (`redis.workers`, default 4). Each one opens the Redis port
(`redis.port`, default 6379) with `SO_REUSEPORT`, so the kernel spreads incoming
connections across the pool and no worker is a listener bottleneck.

A worker runs two kinds of thread:

- **Connection threads.** One per client. They own the socket, parse RESP2, and
  write replies. They are ordinary OS threads, not PostgreSQL backends.
- **A dispatcher** on the worker's main thread. It is the only thread with a
  valid `MyProc`, and therefore the only one allowed to touch PostgreSQL LWLocks
  or SPI.

Every data command crosses between them twice: connection thread → dispatcher
(`DispatchMsg`), dispatcher → connection thread (the reply). Those two mpsc hops
are the throughput ceiling; see [Performance](#8-performance).

Connection-local commands (`PING`, `SELECT`, `AUTH`, `MULTI`/`EXEC` queueing)
never reach the dispatcher and cost only the parse.

### Command path

```
socket ─> resp::parse ─> Command::parse ─┬─> execute_mem ─┐  (memory policies)
                                         └─> execute_spi ─┤  (table policies)
                                                          └─> store::run ─> MemStore | SqlStore
```

`Command` is a single enum covering every supported command. Parsing is
separated from execution so the parser can be unit-tested without a PostgreSQL
instance, and so size limits can be checked *before* anything is written (see
[Limits](#7-limits)). Both paths hand the command to `store::run`, where each
data command is written once against the database's backend; see
[Commands written once](STORAGE.md#commands-written-once-phase-5).

### The 16 databases

Databases mirror Redis's model. Each has a storage policy (`memory`,
`memory+snapshot` or `logged`) that can be changed while the server runs,
data and all ([Storage policies](STORAGE.md)). The defaults are two contiguous
halves:

| DB | Name | Default backend | Durability |
|---|---|---|---|
| 0–7 | `cache` (0) | shared memory | Lost on restart |
| 8–15 | `durable` (8) | WAL-logged tables | Survives crashes |

`SELECT` accepts the numbers and the two names. `redis.default_db` (default 0)
chooses where new connections start. A database in memory occupies one of the
sixteen shared-memory slots, which is not necessarily the slot of its own number.

### Batching and transaction isolation

The dispatcher coalesces up to `redis.batch_size` (default 64) commands from
*unrelated* connections into one SPI transaction, which is what makes the SPI
path fast enough to use. Because the connections are unrelated, one client's
failing command must not roll back another client's write.

Each command therefore runs in its own subtransaction. `worker.rs` calls
PostgreSQL's subtransaction primitives directly. Issuing `SAVEPOINT` /
`ROLLBACK TO` / `RELEASE` as SQL would mean three statements parsed, planned and
executed through SPI per command, which for a plain `SET` cost more than the
`SET` itself. It mirrors the save/restore sequence PL/pgSQL performs around its
`EXCEPTION` blocks.

### Atomicity across workers

Redis runs every command on one thread, so nothing changes under a script, an
`EXEC` or a command such as `ZUNIONSTORE` while it runs. Here several
dispatchers run at once, so each of the 16 databases has a reader/writer lock
in shared memory (`src/dblock.rs`). Only the dispatchers take them.

- **Ordinary commands hold their database shared.** A batch takes each
  database it touches once, for the whole batch, so commands keep running in
  parallel on every worker.
- **Multi-step commands hold it exclusive**, from before their first step until
  their transaction has committed. While one runs, no other client's command on
  that database runs on any worker; other databases are not affected. These
  are:
  - `EXEC`;
  - `EVAL`, `EVALSHA`, `FCALL` and their `_RO` forms;
  - the commands built from other commands in `src/composite.rs`: `LCS`,
    `BITOP`, `BITFIELD` (unless every operation is a `GET`), `ZRANGESTORE`,
    `LMPOP`, `ZMPOP`, `ZUNION`, `ZINTER`, `ZDIFF`, their `STORE` forms,
    `ZINTERCARD`, `MOVE`, `SORT`, `SORT_RO`, `SWAPDB`, `PFADD`, `PFCOUNT`,
    `PFMERGE`, `PFDEBUG`, `GEODIST`, `GEOHASH`, `GEOPOS`, `GEORADIUS`,
    `GEORADIUSBYMEMBER`, `GEOSEARCH`, `GEOSEARCHSTORE` and the `_RO` forms;
  - `COPY`, `RESTORE` and `FLUSHALL`, and `FLUSHDB` on a database in tables,
    which truncates them;
  - each try of `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`,
    `BZPOPMAX` and `BZMPOP`, which checks the key's type and then pops;
  - a stream write on a table-backed database, which reads the stream and
    writes it back in a second statement.

  `BITPOS`, `GEOADD`, `HSCAN`, `SSCAN`, `ZSCAN` and `PFSELFTEST` make a single
  storage call and are held shared like any command.
- **Several databases are taken in ascending order.** `MOVE`, `COPY ... DB`,
  `SWAPDB`, `FLUSHALL` (all sixteen) and an `EXEC` whose queue contains
  `SELECT` hold every database involved, exclusive. The set is known before
  the run starts, so two runs whose sets overlap take them in the same order
  and cannot deadlock. An `EXEC` also holds the databases of the keys its
  connection `WATCH`ed, and checks the `WATCH`es once it holds them, so no
  write lands between that check and the queued commands.
- **Scripts that change database lock every database.** A script's databases
  are only known as it runs. The rule: a script whose source names `SELECT`,
  `MOVE`, `COPY`, `SWAPDB` or `FLUSHALL` as a whole word, in any case and
  anywhere (a comment counts), holds all sixteen, and so does one with a
  `redis.call` or `redis.pcall` whose command name is not a plain string
  literal (read from `ARGV`, built with `..`, or called through a local
  alias); any other script holds the database it starts on. An `EVALSHA` or
  `FCALL` this worker has not compiled yet also holds all sixteen, once. A
  script that still reaches a database it does not hold, by a call the source
  hides (`redis['call']`), gets that lock only if it is free: the call fails
  with an error rather than wait, because the script's transaction may
  already hold row locks that the other holder needs. The trade-off is that a
  script holding all sixteen runs alone on the whole server, as every script
  does in Redis.
- **Writers are preferred.** Once an exclusive request waits, no new shared
  holder gets in, so a stream of commands cannot starve a script.
- **What is never held.** A connection waiting for its client, a blocking
  command waiting for a key (each try is a dispatch of its own), and `WATCH`
  hold nothing. Connection-local commands (`PING`, `SELECT`, `AUTH`, `MULTI`
  queueing, pub/sub) never take a lock.
- **Lock order.** The database locks are taken before the run's transaction
  starts, ascending, and released after it commits. The memory engine's
  per-database LWLocks and PostgreSQL's row and advisory locks are taken and
  released inside the run. No path waits for a database lock while holding one
  of those; the script case above only tries.
- **Release.** The locks belong to a guard: an ERROR caught by
  `pg::subtransaction` or `pg::transaction_or_abort`, or a Rust panic, unwinds
  through it and releases them. A FATAL error exits without unwinding, so a
  `before_shmem_exit` callback releases what the worker still holds; a worker
  that exits with status 1 is restarted without the crash recovery that would
  reset shared memory. A crash does reset it, and a zeroed lock is free.
- **Background work.** The expiry sweep, snapshots and the key moves of a
  policy change hold each database shared, like a command, so none of them
  lands inside a script or a transaction. The sweeps and snapshots only try
  the lock and skip a held database until their next round, so a long script
  on one database does not stop a dispatcher serving the others. The keys
  moved ahead of a client's command wait for it, as the command would.
- **Batching.** A script or multi-step command joins the batch it arrives
  in; the batch then holds that command's databases exclusive and the rest
  shared. The commands run one after another on the dispatcher, so each is
  still whole, and a table-backed batch still shares one commit. An `EXEC`
  runs as a batch of its own.
- **The cost.** Ordinary commands pay two atomic operations per database per
  batch; the bench rig shows no change for them beyond noise. Commands that
  hold a database exclusive on it run on one worker at a time. In memory that
  costs little (`EVAL` of `GET` then `SET`, 50 clients: about 9% fewer per
  second). On a logged database each batch waits for its commit, and those
  commits no longer overlap across workers, so scripts and composites there
  ran about a third as fast (`EVAL` 5,500 to 1,700 per second, `LMPOP` 11,400
  to 3,100). A dispatcher waiting for a database also serves none of its other
  clients meanwhile, so a long script on database 0 delays the commands queued
  behind a database-0 command on another worker.

---

## 2. Storage backends

There are two: the shared-memory engine, and WAL-logged tables reached through
SPI. A database's policy picks one. `memory` and `memory+snapshot` use the
first, `logged` the second ([Storage policies](STORAGE.md)).

| | `memory` (default 0–7) | `logged` (default 8–15) |
|---|---|---|
| Storage | A shared-memory dictionary per database | WAL-logged tables |
| Transaction | None | `BEGIN`/`COMMIT` per batch |
| SPI overhead | None | Yes |
| WAL | No (`memory+snapshot`: the snapshot) | Yes |
| Survives crash | No (`memory+snapshot`: to the last snapshot) | Yes |
| Replication | No | Yes |
| SQL-visible | No (`SELECT * FROM redis.kv_0` returns nothing) | Yes |
| Max data | `redis.maxmemory` (256 MB), backed as it fills | Disk |
| Max value | 512 MiB, as Redis | ~1 GB (TOAST) |

### SQL schema

The tables live in the `redis` schema, one per data type per database, every
one WAL-logged. A database's tables stay empty while its data is in memory.

```sql
CREATE SCHEMA redis;

-- Key-value (pattern repeats for db 0–15)
CREATE TABLE redis.kv_0 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);

-- Hash and set (same pattern); `seq` is the order of insertion
CREATE TABLE redis.hash_0 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL,
                           seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE TABLE redis.set_0 (key BYTEA NOT NULL, member BYTEA NOT NULL,
                          seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));

-- The encoding of a hash, set or sorted set whose contents no longer show it
CREATE TABLE redis.encoding_0 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
```

Every column is `BYTEA`. Redis keys, values, fields and members are arbitrary
byte strings, and `TEXT` cannot hold a NUL. A plain literal is coerced to its
bytes, so `WHERE key = 'mykey'` compares as written. To read them as text, the
views (`redis.strings`, `redis.hashes`, ..., and `redis.keys` for a whole value
as `jsonb`) decode them with `redis.text(bytea)`, which is `NULL` where the
bytes are not UTF-8 instead of an error. They are generated by
`sql/gen_views.exs` into `sql/views.sql` ([Querying from SQL](operations.md#querying-from-sql)).

```sql
SELECT key, value FROM redis.strings WHERE db = 8;
```

Ordering is bytewise, which is what Redis means by lexicographic, so
`ZRANGEBYLEX` and `ORDER BY member` agree with Redis whatever the database
collation.

### Order and encodings

Redis keeps a small hash, set or sorted set packed, as a listpack or, for a
set of integers, an intset, and converts it to a hash table or skiplist once
it passes a threshold. It never converts one back. A client sees the encoding
in two places: `OBJECT ENCODING`, and the order the elements come back in. A
listpack keeps insertion order and deletes in place, an intset is sorted, and
a hash table has an order of its own. Both halves keep the same state for
every hash, set and sorted set and follow Redis 7.2's rules for each
transition (`src/memdb/encoding.rs`):

- A hash is a listpack while it has at most `hash-max-listpack-entries` (512)
  fields and every field and value is at most `hash-max-listpack-value` (64)
  bytes.
- A set is an intset while every member is an integer as Redis reads one (no
  `+`, no leading zero, no `-0`, within 64 bits) and it has at most
  `set-max-intset-entries` (512) members. Otherwise it is a listpack while it
  has at most `set-max-listpack-entries` (128) members of at most
  `set-max-listpack-value` (64) bytes. An intset that takes a string becomes a
  listpack with its integers first, in sorted order. An intset that passes 512
  members becomes a hash table.
- A sorted set is a listpack while it has at most `zset-max-listpack-entries`
  (128) members of at most `zset-max-listpack-value` (64) bytes.
- A command that adds several elements converts first when the number it
  names passes the threshold, repeats included, because Redis sizes the
  collection before it writes.
- `CONFIG SET` of any of these parameters applies from the next write.
  Collections already written keep their encoding until then.

The results of `SUNION`, `SDIFF`, `SINTER`, their `STORE` forms, and an
`SPOP` that takes the whole set are built as Redis builds them, so they come
back in the order Redis's result keeps: the `SUNION` of a listpack holding only
integers comes back sorted, because Redis builds the result as an intset.
`SRANDMEMBER`, `HRANDFIELD` and `ZRANDMEMBER` with a count covering the
collection return all of it in its order (a sorted set from the highest score
down), and a listpack's random choice comes in the listpack's order. `HSCAN`,
`SSCAN` and `ZSCAN` of a packed collection return all of it in one call with
cursor 0, whatever `COUNT` asks for, as Redis does.

A list's encoding is worked out from its elements: a listpack while they fit
one quicklist node, which at the default `list-max-listpack-size` of -2 is
8 KB as a listpack would store them.

**Memory.** Each collection carries its encoding beside its dictionary. A
listpack deletes in place: the entries after the hole move down one, O(n) for
the at most 512 entries a packed collection holds. An intset is sorted when it
is read, and put in sorted order once when it becomes a listpack. A hash table
keeps the dictionary's order, and a removal moves the last entry into the
hole.

**Tables.** `redis.hash_N` and `redis.set_N` have a `seq` column, set from the
sequence `redis.seq` when a row is inserted and left alone when a field's value
changes, and reads order by it. `redis.encoding_N` holds a row for a key only
when its contents no longer show its encoding: a converted hash or set (3,
hashtable), a converted sorted set (4, skiplist), or a listpack set that holds
an integer (1, listpack), which would read as an intset once its strings were
removed. A key with no row is in the encoding its contents imply. A write
reads the row and the key's size, counted no further than the threshold, in
the statement that writes, and writes the row only when the encoding changes,
so most writes run no extra statement. A write that finds the key absent
starts afresh, so a row a removed key left behind does not carry over. `DEL`,
expiry, `FLUSHDB` and `FLUSHALL` remove the row with the key, and so do
`HDEL`, `SREM`, `SMOVE` and `ZREM` when they remove the last element.

**Moving keys.** `COPY`, `MOVE`, `RENAME` and the moves between storage
policies carry a key's encoding and order with it. `DUMP` writes the payload
type Redis writes for the encoding, an intset or a listpack for a packed
collection, and `RESTORE` and snapshot loading give a payload the encoding
Redis's `rdbLoadObject` would. A hash table that has shrunk is written as a
plain hash and loads as a listpack, as it does after a Redis restart.

Every `kv_*` table carries a partial index on `expires_at` for the SQL expiry
sweep; a unit test asserts it exists on all sixteen.

### SCAN cursors

`SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` return one page per call and a cursor to
resume from, with Redis's guarantee: a full iteration, from cursor 0 until the
server returns 0, returns every element present from its start to its end at
least once. An element can come back more than once, and one added or removed
during the iteration may or may not appear. `COUNT` (10 by default) sets the
work per call. `MATCH` and `TYPE` filter the page afterwards, as in Redis, so
a call can return nothing and still not be the last. The cursor is an
unsigned 64-bit integer in decimal. Its value means something different on
each backend, and neither matches Redis's.

**Memory.** The cursor is a position in the dictionary (`Dict::scan` in
`src/memdb/dict.rs`). A call visits the `COUNT` positions below the cursor,
from the top down, and returns the lowest one it visited, or 0 once it reaches
the bottom. Cursor 0 starts at the top, and a cursor past the end is clamped to
it. An insert appends at the end, and a removal either moves the last entry
into the hole or, in a listpack, moves the entries after it down one, so an
entry only ever moves to a lower position. An entry not yet visited
therefore stays below the cursor however the dictionary changes between calls;
one that moves down from a visited position is returned again. Nothing is kept
between calls and a call costs O(`COUNT`). The keyspace, a hash's fields, a
set's members and a sorted set's member dictionary are all iterated this way,
except a packed hash, set or sorted set, which comes back whole in one call
([Order and encodings](#order-and-encodings)). A key past its deadline is
passed over.

**Tables.** A call resumes after the last key or element the previous call
returned, in bytea order, through the primary key index. For `SCAN` each of
the six tables a key can be in gives its next `COUNT` keys
(`key > $last ORDER BY key LIMIT n`), and the first `COUNT` of their union are
the page: a key not among them sorts after all of them, so the page has no gap.
A collection is read with `key = $1 AND field > $last ORDER BY field LIMIT n`,
unless it is packed, when the first call returns all of it.

A key does not fit in 64 bits, so the cursor is a token naming an entry in a
table in shared memory (`src/cursor.rs`), where every worker can find it.
Clients use connection pools, so the calls of one iteration can arrive on
different connections and workers.

- The table has 4,096 entries. Each holds a token, what it iterates (the
  database, and for a collection a hash of its key), the first 256 bytes of the
  key to resume after, and a last-use counter. One spinlock guards the table.
- A new iteration takes a free entry, or else the one used least recently.
  Each call moves its iteration's entry forward under a new token, and the
  call that returns 0 frees it.
- A key longer than 256 bytes is kept as its prefix. Resuming after a prefix
  of the last key can only return elements again. When more than `COUNT` keys
  share one 256-byte prefix, the call also returns the rest of them, so the
  iteration cannot stall on that prefix.
- A token the table does not know starts the iteration over: one evicted by
  4,096 newer iterations, one issued before a restart, one made up, one issued
  for another database or key, or a token sent a second time. That repeats
  elements and skips none.

**After a policy change.** A cursor issued by one backend and sent to the
other is not an error. The tables do not know a position, and the memory
engine clamps a token to its last position, so the iteration starts over.
`SCAN` finishes a database's migration before it runs
([Storage policies](STORAGE.md#when-keys-move)).

---

## 3. The shared-memory backend

`src/memdb` holds the data and `src/shm.rs` the memory it lives in.
[Storage policies](STORAGE.md#growable-shared-memory-phase-6) has the design;
this is how it is put together.

### One database, one dictionary

A `memdb::Db` is a dictionary from key to `Entry { value, deadline }`. The
value is one of six variants: a string's bytes, a stream's serialised bytes, a
list, a hash, a set or a sorted set. One key holds one value, so `WRONGTYPE`
is a match on the variant and replacing a key of another type (`SET` over a
list) is an ordinary insert.

| Type | Structure |
|---|---|
| Keyspace, hash, set | `dict::Dict`: entries in a dense `Vec`, found through a hashbrown `HashTable` of positions. Dense storage gives a random entry in O(1) (for `RANDOMKEY`, `SRANDMEMBER`, `SPOP` and eviction sampling), and removal swaps the last entry into the hole, except in a listpack-encoded hash or set, which keeps its order ([Order and encodings](#order-and-encodings)). Keys hash with SipHash under a per-postmaster random key, so a client cannot aim keys at one bucket. |
| List | `deque::Deque`: two stacks back to back, the head reversed. Pushes and pops at either end are O(1) amortised, indexing is O(1). |
| Sorted set | `zset::ZSet`: a `Dict` from member to score beside the members in `(score, member)` order, kept as sorted blocks of up to 256. An insert moves at most one block; a rank or a score bound costs a walk over the blocks plus a binary search in one. |

Every collection is generic over its allocator (`allocator_api2`). The server
gives them `shm::Shm` and the unit tests give them the ordinary heap, which is
how the whole engine, down to full command lines through `store::run`, is
tested without PostgreSQL.

### The arena

`shm::startup` takes one region of the main shared segment: a header (the
hash key, the limit, the use counters, one LWLock per database and one for
the allocator, and the sixteen `Db`s) followed by the heap, which talc
manages. `Shm` is a zero-sized handle: allocating takes the allocator lock,
asks talc, and counts the bytes.

The region is `redis.maxmemory` plus headroom, reserved when PostgreSQL
starts. The operating system backs a page of it only when it is first
written, so the arena costs what the data does. Freed memory goes back to
talc for the next allocation and is not returned to the operating system, so
resident size stays at its high-water mark, as Redis's does under jemalloc. With
`huge_pages` in use, the whole region is huge pages from the start.

### What makes it sound

The collections hold plain pointers into the arena. That is safe only because
of three guarantees in `shm.rs`, which also holds the only `unsafe` in the
engine:

- **One address everywhere.** The main segment is mapped by the postmaster
  before it forks, so every process sees it at the same address. The header
  records where it was built and `shm::attach` refuses to start a worker that
  finds it anywhere else.
- **One holder at a time.** A database is reached only through
  `shm::with_db`, under its exclusive LWLock, so the `&mut Db` it hands out is
  the only reference in any process. A thread-local flag refuses a nested
  `with_db`, because an LWLock is not re-entrant and the second acquire would
  wait forever. The allocator's lock is innermost: nothing holding it takes
  another.
- **Failure unwinds.** When talc has no block to give, `Shm` unwinds with an
  `OutOfMemory` payload. Returning an error instead would make the
  collections' infallible methods abort the whole backend. `with_db` catches the unwind and the command answers `OOM`. The
  collections are panic-safe, so a write cut short leaves the database
  consistent, and the lock is released on the way out like any other.

### One lock per command

`Command::execute_mem` checks the limit, then runs the type check and the
command inside one `with_db`. Nothing another worker does can land between
the type check and the write, or between the two halves of an
`LMOVE`. What `store::run` does not know runs after the lock is released;
composites re-enter `execute_mem` once per sub-command. What keeps a composite,
a script or an `EXEC` whole is the database lock its dispatcher holds
exclusive around it ([Atomicity across workers](#atomicity-across-workers)).

### Expiry

An entry's deadline is microseconds since the epoch, 0 for none. Any access
to a key past its deadline removes it on the way (`Db::entry`), and the
worker's once-a-second sweep removes the rest, reporting their names when
keyspace notifications or tracking want them.

### The limit and eviction

Before every command, as Redis's `performEvictions` runs before every
command, `evict::perform` compares the memory in use (the arena and the
shared heap) with the limit. If use is under the limit, the command runs and
may overshoot into the headroom. If it is over, keys of the memory databases
are evicted under `redis.maxmemory_policy` (or `CONFIG SET
maxmemory-policy`) until use is under the limit again. A write that could
grow memory (Redis's `denyoom`) is refused with `OOM command not allowed
when used memory > 'maxmemory'` when the policy finds nothing more it may
evict, on a `logged` database too, as the limit is the server's; reads and
removals still run. Commands the connection threads answer themselves,
`INFO` and `CONFIG` among them, ask the main thread for the round first.

| `redis.maxmemory_policy` | Evicts |
|---|---|
| `noeviction` (default, as in Redis) | Nothing. |
| `allkeys-lru` | The least recently used key, by Redis's approximation. |
| `allkeys-lfu` | The least frequently used key. |
| `allkeys-random` | A random key. |
| `volatile-lru`, `volatile-lfu`, `volatile-random` | As the `allkeys-*` policy, among keys with a deadline only. |
| `volatile-ttl` | The key with a deadline that comes soonest, among those sampled. |

The algorithm is Redis 7.2's (`evict.c`):

- **Sampling and the pool.** For the LRU, LFU and TTL policies each round
  draws `maxmemory-samples` keys (5 by default) at random from every memory
  database, scores them (idle time, `255 - frequency`, or nearness of the
  deadline), and keeps the best 16 across rounds in an eviction pool. The
  best candidate still present is evicted. The random policies take a random
  key from each database in turn. A collection is evicted whole.
- **Keys with a deadline have their own index.** Each database keeps a list
  of the positions of its keys with a deadline, as Redis keeps its `expires`
  dictionary, so the `volatile-*` policies sample only those keys and the
  expiry sweep walks only those keys (`memdb::keyspace`).
- **Time limit.** A round stops after `maxmemory-eviction-tenacity`'s limit
  (500 µs at the default 10), checked every 16 keys. The command then runs,
  and the worker's next round, every 250 ms, carries on, as Redis's
  `evictionTimeProc` does. The same round evicts after `CONFIG SET
  maxmemory` lowers the limit below what is held, with no command needed.
- **What an eviction reports.** Each evicted key increments INFO's
  `evicted_keys`, raises the `evicted` keyspace event (class `e`), sends a
  `CLIENT TRACKING` invalidation and breaks a `WATCH` on it, as Redis's
  `signalModifiedKey` does. INFO's `total_eviction_exceeded_time` and
  `current_eviction_exceeded_time` count the milliseconds spent over the
  limit; `evicted_clients` stays 0, since clients are never evicted.

`CONFIG SET maxmemory` changes the limit for every worker, never past what
the arena was sized for; `CONFIG SET maxmemory-policy`, `maxmemory-samples`,
`lfu-log-factor`, `lfu-decay-time` and `maxmemory-eviction-tenacity` take
effect from the next command.

### The access clock

Every key carries Redis's 24-bit access field (`memdb::lru`). Under an LRU or
random policy it is an LRU clock: seconds since the epoch, in 24 bits, so it
wraps every 194 days and an idle time is measured across one wrap. Under an
LFU policy it is a 16-bit minute of the last decrement and an 8-bit
logarithmic counter: a new key starts at 5, each access first takes one off
per `lfu-decay-time` minutes since the last decrement, then adds one with
probability `1 / ((counter - 5) * lfu-log-factor + 1)`. Switching between
LRU and LFU policies reinterprets the same bits, as in Redis.

A command's lookups update the field as Redis's `lookupKey` does. `EXISTS`,
`TYPE`, `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `OBJECT`, `SCAN`, `KEYS`,
`RANDOMKEY` and `DBSIZE` look without touching (`LOOKUP_NOTOUCH`), and so does
every command from a client in `CLIENT NO-TOUCH` mode except `TOUCH`. An
overwrite keeps the field its lookup just updated, as `dbSetValue` keeps the
old object's; a new key, a `COPY` destination and a `RESTORE` start fresh,
and `RESTORE ... IDLETIME | FREQ` sets it.

`OBJECT IDLETIME` answers the idle seconds, or under an LFU policy `ERR An
LFU maxmemory policy is selected, idle time not tracked. ...`. `OBJECT FREQ`
answers the decayed counter, or without an LFU policy `ERR An LFU maxmemory
policy is not selected, access frequency not tracked. ...`. `OBJECT REFCOUNT`
answers `2147483647` for a string holding an integer from 0 to 9999, which
Redis keeps as a shared object, unless an LRU or LFU policy is set with a
nonzero `maxmemory` (Redis's `MAXMEMORY_FLAG_NO_SHARED_INTEGERS`), and 1
otherwise. All three answer nil for a missing key.

A `logged` database keeps no field in its rows: writing one on every read
would turn each `GET` into a table write. Its fields live in a table in
shared memory of 65,536 slots of 8 bytes, keyed by a hash of the database,
its flush count and the key (`access.rs`). The connection that ran a
command updates the fields of the keys its arguments name, by the same key
specifications `COMMAND GETKEYS` reads; the commands a script runs do the
same, and `DEL`, `UNLINK` and `GETDEL` clear the slot, `RENAME` moves it.
The table is lossy: two keys that hash to one slot share it, the later
winning, and it starts empty after a restart, so a key with no slot gets a
fresh field the first time `OBJECT` asks. A key that leaves the database
another way (a collection emptied by a pop, an expiry) keeps its slot, so a
key written again under the same name continues its old field. Eviction never
applies to a logged database, which is bounded by disk.

---

## 4. Pub/Sub

Pub/sub is independent of storage policies and always allocated. It needs
only `shared_preload_libraries`.

Subscriptions and undelivered messages live in a shared heap, a region of
PostgreSQL's shared memory sized by `redis.pubsub_memory` (128 MB by default)
with its own allocator. Pages are backed by RAM only once they are touched.
Each subscribing connection has an entry there holding its channels, its
patterns and a queue of messages. `PUBLISH` finds the channel's subscribers
through an index, scans the patterns as Redis does, copies the message into
each subscriber's queue and bumps that subscriber's tail counter.

The client tracking table lives in the same heap: every key a tracking client
read, by name, with the clients that read it, up to `tracking-table-max-keys`
(1,000,000 by default) as in Redis. If the heap fills first, tracked keys
picked at random are invalidated to make room, as the key limit does.
Invalidations travel as pub/sub messages on a channel of the client's own; a
RESP3 client that invalidations are redirected to is handed a subscription to
it by the connection that sets up the redirect.

Subscribers **wait on the tail counter**, not on the socket. A subscriber that
discovered messages by letting a 5 ms socket read time out would cost ~200
wakeups per second while idle and still delay delivery by up to 5 ms. The futex
lives in PostgreSQL's shared memory segment, and waiter and waker are different
processes, so the shared (not `PRIVATE`) operations are required. The socket
poll that remains backs off from 5 ms to 250 ms while a connection stays silent.
It bounds only how long a *command* from a quiet subscriber sits unread, and has
no effect on delivery latency.

Route lookups short-circuit on an atomic counter, so an installation with no
routes configured pays nothing.

### Limits

As in Redis, channel and pattern names, payloads, subscribers and
subscriptions have no fixed limit. What bounds them:

| | Limit | Over the limit |
|---|---|---|
| A subscriber's undelivered messages | `client-output-buffer-limit pubsub`, 32 MB at once or 8 MB for 60 seconds by default, counted in wire bytes | The subscriber is disconnected, as in Redis |
| Everything subscribed and queued, server-wide | `redis.pubsub_memory` | The command that needs more gets `OOM pub/sub memory is full` |
| Active routes | 64, channel names up to 255 bytes | `redis.route_publish` errors |

A subscriber that is disconnected never carries on with a gap it cannot see.
`CONFIG SET client-output-buffer-limit` takes effect at once.

### Table routing

Any `PUBLISH` can also be INSERTed into a PostgreSQL table, which is enough for
[Supabase Realtime broadcast from database](https://supabase.com/docs/guides/realtime/broadcast)
or any trigger-based integration. The extension is decoupled from Supabase; it
only INSERTs rows.

```sql
SELECT redis.create_pubsub_table('public', 'chat_messages');
SELECT redis.route_publish('chat', 'public', 'chat_messages');
-- redis-cli> PUBLISH chat "hello"
SELECT channel_text, payload_text, inserted_at FROM public.chat_messages;
```

| Function | Description |
|---|---|
| `redis.create_pubsub_table(schema, table)` | Create a routing target with the required columns, and both as text in `channel_text` and `payload_text` |
| `redis.route_publish(channel, schema, table)` | Route `PUBLISH` on `channel` into `schema.table` |
| `redis.unroute_publish(channel)` | Remove the route |

Routes live in `redis.pubsub_routes`, are loaded into shared memory at startup,
and survive restart. The INSERT is dispatched fire-and-forget *after* in-memory
delivery completes and after the `PUBLISH` reply is sent.

You can bring your own table if it has `channel BYTEA` and `payload BYTEA`
columns. `BYTEA` is required, because channels and payloads are arbitrary
bytes. Because the insert is fire-and-forget, a routed insert into a
wrongly-typed table fails *after* `PUBLISH` has already succeeded, and the error
goes to the PostgreSQL log; the client never sees it. Check there if routed rows
stop appearing.

---

## 5. WATCH

`WATCH` snapshots a write-version counter per key and `EXEC` aborts if it moved.

The counters live in shared memory, not per worker, because of
`SO_REUSEPORT`. A client's `WATCH` is served by whichever worker accepted its
connection, while the conflicting write may be dispatched through any other
worker. Per-process counters reported "no conflict" for writes the process never
saw, and `EXEC` committed when it should have aborted.

Keys are not stored. They are hashed (FNV-1a over the database number followed
by the key bytes) into a fixed table of 65,536 `AtomicU64` counters (512 KiB).
That keeps the structure allocation-free, lock-free and fixed-size, with no
eviction policy and no growth a client can drive by writing unique keys. The
cost is that two keys can share a counter, which makes an `EXEC` abort when it
strictly did not have to. **An unneeded abort is safe; a missed conflict would
not be.**

---

## 6. The RESP layer

RESP2 by default; `HELLO 3` switches a connection to RESP3.

RESP3 changes how a reply is framed and leaves its contents alone, so commands keep
answering in one shape and `resp3.rs` reframes the reply for a RESP3 client,
command by command, following Redis 7.2's reply code. `HGETALL` and
`CONFIG GET` become maps; `SMEMBERS`, `SINTER`, `SUNION`, `SDIFF` and
`SPOP` with a count become sets; scores become doubles, and `WITHSCORES`
lists become `[member, score]` pairs; `XREAD` becomes a map from stream to
entries; `INFO` and `CLIENT LIST` become verbatim text; every nil is the one
RESP3 null. Pub/sub messages are push frames. A command the table does not
name is framed the same in both protocols.

| Bound | Value | Why |
|---|---|---|
| `MAX_BULK_LEN` | 512 MiB | Matches Redis's `proto-max-bulk-len` |
| `MAX_ARRAY_LEN` | 65,536 elements | |
| `MAX_INLINE_LEN` | 8 KiB | Inline (non-array) command form |
| `BULK_CHUNK` | 64 KiB | Read granularity |
| `QUEUE_LIMIT` | 10,000 | Commands queued in one `MULTI` |

A declared bulk length does not commit memory. The parser grows the buffer as
bytes arrive, so a client that announces a 512 MiB bulk string and then stalls
holds only the memory for what it sent.

Error replies carry their own code when they have one. `write_error` prefixes
`ERR` only for messages that do not already start with a code from
`ERROR_CODES`, so an OOM goes out as `-OOM ...`, which is the code clients match on.

### Scripting

`EVAL` and `FCALL` run Lua 5.1 on the dispatcher, whole, holding their
database exclusive ([Atomicity across workers](#atomicity-across-workers)), so
no other client's command on it runs on any worker until the script ends.
`redis.call` builds an
ordinary command and routes it like a composite command's sub-commands, so one
script reads and writes both storage halves. The environment is Redis's:
`cjson`, `cmsgpack`, `struct` and `bit` are Redis's own C sources, compiled by
`build.rs` from `vendor/redis-lua/`; replies convert to Lua and back by Redis's
rules; errors carry the script and line as Redis reports them. Redis's Lua is
patched to make tables read-only; here every protected table is an empty proxy
whose metatable forwards reads and refuses writes, so the same "Attempt to
modify a readonly table" comes back without patching Lua.

Each worker has its own Lua states, so what must be shared lives in SQL: the
script cache in `redis.scripts` (unlogged, because it is a cache, as in Redis)
and the function libraries in `redis.functions` (logged, because Redis persists
them in its RDB). A change bumps a generation counter in shared memory once its
transaction commits, and each worker rebuilds its copy when it sees the
counter move. A script that runs past `busy-reply-threshold` makes every other
client get `BUSY`, on every worker, until it ends or `SCRIPT KILL` /
`FUNCTION KILL` stops it.

### Known differences from Redis

- **A script that hides how it calls Redis can be refused a database.** A
  script holds only its own database when its source names every command it
  runs with a string literal and none of them is `SELECT`, `MOVE`, `COPY`,
  `SWAPDB` or `FLUSHALL`; any other script holds every database, as every
  script does in Redis. One that reaches `redis.call` some way the source
  does not show (`redis['call']`) and then another database fails that call
  while another client uses the database
  ([Atomicity across workers](#atomicity-across-workers)). Redis runs it. A RESP3 `MULTI` that subscribes runs in segments around each
  subscription command, each segment atomic on its own.
- **Blocked clients are served before the writer replies, within a bound.**
  Redis serves the clients a write unblocks inside that write. Here they live
  on other threads and workers, so a write that touches a key someone is
  blocked on waits, for up to 500 ms, until each of them has tried against it,
  and a served client that moves an element does the same for its
  destination. Chains of `BLMOVE`s, and cycles of them, complete before the
  original `RPUSH` returns, as in Redis; a blocked client stuck for longer
  than the bound is left to finish on its own. Writes made inside a script do
  not wait.
- **No replicas, cluster mode or modules.** These are out of scope. `WAIT`,
  `REPLICAOF` and `CLUSTER` answer as a stand-alone server would, and
  `MODULE` answers as a server with no modules loaded. Replication is
  PostgreSQL's.
- **A blocked command's ACL check repeats.** A blocked command is checked
  again each time it is retried (every 50 ms, and on each write to its keys),
  where Redis checks it again only when a write serves it.
- **Logged keys' access fields are approximate.** `OBJECT IDLETIME` and
  `OBJECT FREQ` on a `logged` database read a lossy table in shared memory
  ([The access clock](#the-access-clock)): two keys can share a slot, and the
  table starts empty after a restart. On memory databases they are Redis's.
- **Small-integer sharing follows the current policy.** Redis decides when a
  value is written whether it is a shared object; pg_redis answers `OBJECT
  REFCOUNT` from the policy and `maxmemory` in force when asked, so the two
  differ for a key written before the policy changed. With no `CONFIG SET
  maxmemory`, `maxmemory` reports the arena's limit, never Redis's default 0.
- **`SAVE` and `BGSAVE` snapshot memory databases.** They, and `LASTSAVE`,
  act on `memory+snapshot` databases' snapshots
  ([Storage policies](STORAGE.md#snapshots-phase-4)); there is no RDB file.
- **`SCAN` cursors on a table are kept by the server.** On the durable half
  a cursor names an entry in a shared-memory table of 4,096 iterations in
  progress ([SCAN cursors](#scan-cursors)). The cursor from the latest call,
  or from the one before it, resumes where it left off, which covers a client
  retrying a lost reply. An older cursor, or one pushed out by newer
  iterations, starts the iteration over, so elements come back again; none is
  skipped. A Redis cursor carries its whole state, so any of them resumes.
- **A list that shrinks reports `listpack` sooner.** Redis converts a list
  from quicklist back to listpack only once it is down to one node of half the
  size limit. pg_redis does not keep quicklist nodes, and reports `listpack`
  as soon as the elements fit one node.

---

## 7. Limits

Everything pg_redis bounds that Redis does not, in one place.

| | Limit | Where it comes from |
|---|---|---|
| Memory-mode data | `redis.maxmemory` | The arena, sized at start ([§3](#the-limit-and-eviction)) |
| Pub/sub subscriptions, queued messages and the client tracking table | `redis.pubsub_memory` | One shared heap, sized at start ([§4](#4-pubsub)) |
| Table route channel | 255 bytes | Fixed route entry |
| Bulk string on the wire | 512 MiB | RESP parser, matches Redis |

Keys, fields, members and values are as long as Redis allows on both halves:
512 MiB a string, at the parser. The durable half is otherwise bounded by
disk, and by PostgreSQL's ~1 GB per-field TOAST limit.

---

## 8. Performance

Requests per second, higher is better. `redis-benchmark`, 50 clients, 64-byte
values over 100,000 random keys. Each server runs alone on the same pinned CPUs,
with the load generator on others, PostgreSQL with `redis.workers=4`. The
figures are not typed in: `mise run bench-site` runs `docker/bench/suite.exs`
and writes its report to `docs/bench.json`, which the site is built from.
`mise run bench-suite` runs the same suite and writes the full report, value
sizes, client counts, pipelining and a memtier mix included, to
`docker/bench/results/`.

<!-- bench:commands -->

`logged` writes commit to the WAL, so they measure the disk more than this
code. On Docker Desktop the same `SET` measured 6,374 and 15,309 per second in
two groups of one run. Compare `logged` writes across runs only by their order
of magnitude.

The table's 64-byte values come from `mise run bench-suite`, which passes
`-d 64` to `redis-benchmark`. `mise run bench` runs `redis-benchmark` with its
default of 3-byte values, and `mise run bench-value-sizes` sweeps
3/63/64/65/200/512/4096/65536 bytes. `mise run bench-high-write` (8 workers, `batch_size=256`) spreads
connections across more dispatchers.

### Pub/Sub

One publisher, varying subscriber counts. **recv/s** is total deliveries across
all subscribers. `mise run bench-pubsub` (`docker/e2e/bench_pubsub.ts`) waits
for each of 200 `PUBLISH` replies before sending the next, with no
pipelining, and reaches Redis from the host through Docker's published port
6380 but pg_redis from a container on the compose network, so pub/s is the
inverse of one round trip and the two columns measure different network paths.

| Subscribers | Redis 7 pub/s | Redis 7 recv/s | pg_redis pub/s | pg_redis recv/s |
|---|---|---|---|---|
| 1  | 2,509 | 2,506  | 9,451  | 9,172   |
| 4  | 2,359 | 9,433  | 12,297 | 38,086  |
| 16 | 3,865 | 61,762 | 49,810 | 348,682 |
| 32 | 2,896 | 92,516 | 8,993  | 237,891 |

recv/s scales linearly, because one `PUBLISH` fans out to N ring buffers in parallel.
Routing to a table roughly halves publisher throughput with no subscribers
(10,833 → 5,577) and costs progressively less as subscribers are added.

### Where the ceiling is

These measurements were taken on the memory engine before phase 6 of
[Storage policies](STORAGE.md#growable-shared-memory-phase-6) replaced its
fixed tables with one arena, and have not been repeated on the current one.

**Sharding the shared-memory LWLocks is not worth doing.** The measurements:

| Measurement | Result |
|---|---|
| Throughput at 1 / 2 / 4 workers | 55k / 48k / 55k — no scaling at all |
| Worker wait events, 120 samples under `SET` load | 100% `RUNNING/cpu`, zero LWLock waits |
| `SET` (exclusive lock) vs `GET` (shared lock) | 52k vs 54k — indistinguishable |
| Pipelining `-P 1` → `-P 32` | 47.5k → 151k (3.2×) |

One worker matches four, and pipelining triples throughput without touching the
lock. The ceiling is the two mpsc thread hops per command. Memory-mode commands
need no SPI and no transaction, so a connection thread could in principle
execute them inline and skip both hops. The blocker is that PostgreSQL LWLocks
require a valid `MyProc`, which non-backend threads do not have. Closing that
needs its own design, and lock sharding only becomes interesting afterwards.

### Benchmarking without fooling yourself

- **Point the benchmark at db 0.** The compose default is
  `redis.default_db=8`, the WAL-logged half. A benchmark that forgets this
  measures fsync and reports ~500 requests/second for everything.
  `bench-value-sizes` and `bench-report` set it; `bench` does not, by design.
  `bench-suite` names each policy's database explicitly.
- **`redis-benchmark` never clears `mylist`.** Its `lpush`, `rpush` and `lrange`
  tests build the list up over the whole run. On the fixed tables before
  phase 6, `-n 20000` was past what a list table held at the default, so the
  run ended in the OOM the server is supposed to give and reported nothing.
  The arena has no per-type limit, but a list that grows with every test still
  skews the ones after it, so `docker/bench/pr.exs` clears the key before each
  of those tests and uses 4,000 requests for them.
- **The noise band is ±6%, which is wider than most changes worth measuring.**
  Three rounds is not enough. One attempt at measuring the KV key change read as
  "SET and GET are 10% slower" while `PING`, which the change cannot touch, read
  as 8% *faster*. Both were noise. To measure reliably, alternate the two
  builds, run nine rounds each at 50,000 requests, take medians, and include a
  command the change cannot reach as a calibration. At that sample size `PING` came back at +0.5% and
  everything else sat inside ±5% straddling zero.
- **Logged writes measure the disk, and the disk moves.** On OrbStack the same
  image gave a logged `SET` of 1 KB 11,202 and 18,219 per second in two runs, and
  fresh containers differ from each other by as much again: a suite run against
  main showed `SPOP` at half and `MSET` at 0.7× that neither reproduced. To
  compare two builds' code paths on a logged database, take the disk out
  (`ALTER SYSTEM SET synchronous_commit = off`), measure both in the same
  container where the change allows it, and alternate them. Read logged writes
  across separate runs by their order of magnitude only.
- **`KEYS` needs its own measurement.** It is not a `redis-benchmark` test, and
  reading every key out of every entry is what it does. Time it directly
  over a populated database.

CI runs `docker/bench/pr.exs` on every pull request: pg_redis built from the
pull request and from main, and Redis 7.2, on the same runner, on a memory and a
logged database. Each measurement runs on all three in turn, starting with a
different one each round, and the median of the rounds is reported with the
change from main; changes of 10% or more are marked. `PING`, which no change to
the data path reaches, is the calibration row. The result is posted as one
comment, keyed on an HTML marker, so a push edits it in place and duplicates are
deleted. The same report goes to the job summary, which is all a fork's token
can write. Nothing gates on it. `mise run bench-report` runs it locally against
the local `main`.

---

## 9. Testing

The test layers cover different code, and running one does not run the others.

| Layer | Command | What it actually covers |
|---|---|---|
| Unit | `mise run test-unit` | Pure functions: parsing, key layout, limits, SipHash, the pool; no PostgreSQL |
| pgrx | `mise run test-pg` | **The SPI path only**, never the shared-memory backend |
| End-to-end | `mise run e2e` | Everything: databases 0–7 in memory, 8–15 in tables, and moves between them |
| Parity | `mise run parity` | Every reply, diffed against a real Redis, on both halves |
| Acceptance | `mise run acceptance` | Redis's own Tcl test suite, on both halves |

**The gap in the middle is the important one.** `cargo pgrx test` never reaches
the memory engine: its tests run SQL against the tables directly and send no
commands through a worker. Run the e2e suite for anything touching `src/memdb` or
`src/shm.rs`; one run covers both backends, since databases 0–7 default to
memory and 8–15 to tables.

What the e2e suite cannot reach cheaply, the unit tests do: `src/memdb` is
generic over its allocator, so `cargo test --lib` drives every collection
against a reference model, and full command lines through `store::run`
against a `Db` on the ordinary heap, with no PostgreSQL and no shared memory.

**Name a `#[pg_test]` module `tests`.** pgrx resolves every `#[pg_test]` to the
`tests` schema (`framework.rs`: `let schema = "tests"`), so a module called
anything else compiles and then fails at the call. `pg_tests` fails more
broadly. `#[pg_schema]` turns it into a schema name, and PostgreSQL reserves the
`pg_` prefix, so `CREATE EXTENSION` fails and *every* test in the run reports
"Could not obtain test mutex" instead.

One constraint on the plain `#[test]`s: they may not call anything that
reaches PostgreSQL. `cargo test --lib` links a binary with no PostgreSQL
symbols to resolve against, which works only because everything FFI is dead
code there. `src/memdb` calls none; `src/shm.rs` is
where the FFI is, and is not unit-tested for that reason.

### Parity against a real Redis

The three layers above assert against expectations written by hand, so they can
only catch what someone already knew to expect. `docker/e2e/parity.ts` uses
Redis as the expectation. It sends the same commands to pg_redis and to
`redis:7` and compares the replies byte for byte, on the shared-memory half and
the SQL half in the same run.

```bash
mise run parity           # gate: fails on a difference not already recorded
mise run parity-update    # re-record after fixing or accepting one
```

93 cases, 2,250 replies a run, covering every command the parser accepts, the
corner cases the community trips on (empty collections vanishing, duplicate
arguments, source equal to destination, score and float formatting, what counts
as an integer, which operations keep a TTL), and the patterns people deploy (rate limiter, session, `SET NX PX` lock, leaderboard, work queue, dedup,
cache-aside, tag intersection, hash-as-object, time index).

It normalises four things and nothing else:

- **Unordered replies.** `KEYS` and the random draws have no order to
  compare, so they are sorted before comparison. A hash's or set's elements
  (`SMEMBERS`, `HGETALL`, `HKEYS`, `HVALS`, the set algebra) are compared in
  order, since both servers keep the same encodings
  ([Order and encodings](#order-and-encodings)); only a reply from a
  collection Redis keeps as a hash table, marked `anyOrder` in the case, is
  sorted first, because two hash tables iterate in their own orders.
- **Draws and clocks.** `SPOP` without a count, `RANDOMKEY`, `TTL`, `INFO`.
  Only the reply *shape* is compared: the type byte, and an array's length.
- **Scan cursors.** A `SCAN`-family cursor is a position in each server's own
  structure, so cursor values and page boundaries cannot match. A reply that
  ends the iteration (cursor 0) is compared by its elements in any order, with
  field-value and member-score pairs kept together; any other page by its
  shape.
- **A dropped connection.** A command that kills the server it was sent to never
  replies; the harness records `NO REPLY` and reconnects, so it does not wait
  forever. That is how the `INCR` overflow below was found.

Differences are classified as `value`, `error-vs-value`, `error-code` or
`error-text`, and the known ones live in `docker/e2e/parity-baseline.json`.
**CI gates on new differences only.** A gate on zero differences would fail on
every run today, and one that failed always would be turned off.

### Redis's own acceptance suite

Parity diffs the commands someone thought to write down. Redis gates its own
releases on the Tcl suite under `tests/unit`, and that suite can drive any
server as an external one (`--host`/`--port`), so `docker/acceptance` runs it
against pg_redis. The suite is fetched at a pinned tag (7.2.5, the last
BSD-licensed 7.x) instead of vendored, and carries one patch: `FUNCTION FLUSH`
at the top of every block is made tolerant, because an error there aborts the
whole block before its first test.

```bash
mise run acceptance          # gate: fails when a test in the baseline stops passing
mise run acceptance-update   # re-record the baseline after tests start passing
```

Two passes, one per half: the suite's default `SELECT 9` reaches the SQL
tables, and `--singledb` (database 0) the shared-memory ones. Each unit runs
in its own process, so a unit that aborts takes only itself down.

- **A ratchet, like parity.** `docker/acceptance/baseline.json` lists the tests
  that pass today, per half and unit. The gate fails when one of them stops
  passing; the run lists the ones that started passing so the baseline moves
  forward. Most of the suite fails for a missing command (`BLPOP`, `BITOP`,
  `SSCAN`, ...), and gating on those would mean gating on
  commands not yet built.
- **What is left out, on purpose.** The units that restart the server under
  test or need a replica, which Redis itself tags `external:skip` (aofrw,
  client-eviction, limits, shutdown, wait), and the ones that measure Redis's
  own allocator or internals (memefficiency, oom-score-adj, querybuf,
  replybufsize, violations, tls). Tests needing replication or `SAVE` are
  denied by tag; tests needing a real `maxmemory` run on the memory half only.
  Tests needing `DEBUG` run: it answers `OBJECT`, `POPULATE`, `PROTOCOL`,
  `SLEEP` and `SET-ACTIVE-EXPIRE`, and `RELOAD` and `LOADAOF` leave the data
  as it was. `info`, `maxmemory`, `obuf-limits` and `pubsubshard`, `external:skip`
  throughout, run from a copy without the tag and without the blocks that
  start a replica or read the replication stream (`REWRITTEN` in `run.ts`).
  `--ignore-encoding` and `--ignore-digest` turn off the assertions about
  Redis's internal encodings.
- **`skip.txt` is for hangs only.** A failing test belongs in the baseline,
  where it stays visible. Two groups hang instead of failing, and each entry
  says why. The pipelining one is a real defect in pg_redis, not a missing
  feature.
- **No password.** The Tcl client cannot `AUTH`, so the profile runs with
  `PG_REDIS_PASSWORD` empty; CI does the same.

It found bugs the hand-written layers missed, because nobody had written those
expectations down: an unknown-command error echoing a newline that desynced
the connection, `SELECT` inside `MULTI` ignored, `EXPIRE`'s `NX`/`XX`/`GT`/`LT`
silently dropped, `SUNION` of a string and a set answering the set on the SQL
half, `EXPIREAT k 0` making a memory-half key persistent, and protocol errors
closing the connection without saying why.

### Running the e2e suite without Docker

`docker compose --profile e2e up` is the supported path, but `bun` runs the same
suite against any live server:

```bash
# 1. A cluster with the extension preloaded. Run as an unprivileged user —
#    initdb refuses to run as root, and a long $HOME breaks the socket path.
initdb -D "$PGDATA" -U postgres -A trust
cat >> "$PGDATA/postgresql.conf" <<'CONF'
listen_addresses = 'localhost'
shared_preload_libraries = 'pg_redis'
redis.port = 6379
redis.database = 'postgres'
redis.default_db = 8
redis.password = 'testpass'
CONF

# 2. Install the extension into that PostgreSQL, then start it.
cargo pgrx install --release --pg-config /usr/lib/postgresql/17/bin/pg_config
pg_ctl -D "$PGDATA" -o '-k /tmp' start
psql -h 127.0.0.1 -U postgres -c 'CREATE EXTENSION pg_redis;'

# 3. The suite.
cd docker/e2e && bun install
DATABASE_URL="postgres://postgres@localhost:5432/postgres" \
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=testpass \
  bun test index.test.ts
```

Re-run step 2 after every code change. The running server holds the old `.so`
until it restarts.

### Against an assert-enabled PostgreSQL

`cargo pgrx init --pgN download` configures PostgreSQL with `--enable-cassert`,
which distribution packages do not. Code that misuses a PostgreSQL internal can
pass against system packages and fail only in CI, with an assertion crash that
also takes down the backend and cascades into unrelated tests. If a test fails
in CI and passes locally, suspect this first.

This has happened. `UpdateActiveSnapshotCommandId` asserts the active snapshot
has `active_count == 1` and `regd_count == 0`, and the snapshot SPI has already
pushed satisfies neither. The code passed locally and failed every CI leg.

```bash
# ftp.postgresql.org may be unreachable; the Debian source package works.
echo "deb-src [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
  https://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
  > /etc/apt/sources.list.d/pgdg-src.list
apt-get update && apt-get install -y flex bison && apt-get source postgresql-17

cd postgresql-17-*/
./configure --prefix=$HOME/pgassert --enable-cassert --enable-debug \
  --without-icu --without-readline --without-zlib --with-openssl
make -j"$(nproc)" && make install

PGRX_HOME=$HOME/pgrxassert cargo pgrx init --pg17 "$HOME/pgassert/bin/pg_config"
PGRX_HOME=$HOME/pgrxassert cargo pgrx test pg17
```

Keep it under its own `PGRX_HOME` so the fast system-PostgreSQL config stays
intact, and give it its own `CARGO_TARGET_DIR`. The bindings differ, so sharing
one target directory means a full rebuild on every switch.

**Build it under the test user's home.** `$HOME/pgassert` as root plus tests as
the unprivileged user below means `pgrx install --test` cannot write to
`/root/pgassert`. That build's `pg_config --pkglibdir` also reports the *system*
lib dir, so its `.so` lands there instead of under the prefix. Reinstall the
ordinary build before benchmarking anything.

### `cargo pgrx test` has to run as a non-root user

It runs `initdb`, which refuses to run as root. The failure is a wall of "Could
not obtain test mutex" from every test after the first, with the real message
only in the first one's output.

```bash
useradd -m pgu
cp -a ~/.cargo ~/.pgrx /home/pgu/ && chown -R pgu /home/pgu
chown -R pgu "$PWD"                                   # target/ is written during the run
chown -R pgu /usr/lib/postgresql/*/lib /usr/share/postgresql/*/extension
sudo -u pgu env HOME=/home/pgu PATH=/home/pgu/.cargo/bin:$PATH \
  RUSTUP_HOME=$HOME/.rustup CARGO_HOME=/home/pgu/.cargo cargo pgrx test pg17
```

`cargo pgrx init --pgN download` fetches from `ftp.postgresql.org`, which is not
always reachable. Pointing pgrx at the PGDG packages
(`cargo pgrx init --pg15 /usr/lib/postgresql/15/bin/pg_config …`) gets the matrix
running; only the assert-enabled build has to be compiled.

### Traps

- **A `describe` that does not `SELECT cache` runs on db 8**, the durable half,
  so it says nothing about the shared-memory backend. The "Binary safety" describe sat there for its whole life and passed
  while memory mode collapsed two NUL-containing keys onto one entry. Put
  memory-mode tests in a describe that selects `cache`, and confirm they *fail*
  against `main` before believing they cover anything.
- **`bun test -t "<filter>"` changes which database the tests run against.** The
  `SELECT isolation` describe is what leaves the pooled client on db 0; a
  filtered run skips it, so the connection stays on db 8. A list bug was
  misdiagnosed twice this way. Prefer a full run when a failure looks
  order-dependent.
- **`redis-cli -n 0` does not select the database on this server.** The
  connection stays on `redis.default_db`. Send `SELECT 0` as a command.
- **`Bun.RedisClient` encodes arguments as UTF-8.** It cannot express a `0xff`
  byte and reports 10 for a 7-byte value. Anything asserting on exact bytes or
  lengths must go through the `RawRedis` helper at the top of
  `docker/e2e/index.test.ts`.
- **A rejected `client.send()` leaves the pooled client's replies out of step**
  for later tests. Where an error reply is the expected outcome, use `RawRedis`,
  which returns the frame as data.
- **The parity harness resets with `FLUSHDB`, before every case, on both
  servers.** It used to delete only the tokens its own cases name, so keys left
  by an e2e run showed up as two `KEYS *` differences and the gate failed on
  work that was fine. That is why `FLUSHDB` was built first in phase 5, out of
  order. A run no longer needs a private server, but it empties whatever
  database it is pointed at, on the reference server too.
- **`SHOW redis.foo` names the result column `redis.foo`**, dot included.
  Destructuring by `foo` yields `undefined` with no error. Use
  `SELECT current_setting('redis.foo') AS mode`.

---

## 10. Continuous integration

`.github/workflows/ci.yml` is seven jobs. Two are trivial and gate the rest, so
a formatting mistake fails in seconds, before the full matrix runs:

```
Workflows ─┐
Format    ─┴─> Clippy
               Test (pg15, pg16, pg17)
               Build image ──> E2E (auto), E2E (memory)
                           └─> Benchmarks (pull requests only)
```

`Build image` exists because the end-to-end legs and the benchmark need the same
container. When they each built it they started together, so none could reuse
another's layer cache; now it is built once and the rest restore it warm.

CI reads the pgrx version from `Cargo.toml` and keeps no copy of its own,
because the auto-update workflow's token is not allowed to rewrite a workflow
file.

---

## 11. Local development

```bash
mise run run     # cargo pgrx run pg17, extension hot-reloaded
# then, in psql:  CREATE EXTENSION pg_redis;
# and elsewhere:  redis-cli -p 6379 ping

mise run fmt     # cargo fmt
mise run lint    # cargo clippy -D warnings
```

---

## 12. Open work

[Parity roadmap](PARITY.md) measures what is *left* between pg_redis and Redis:
the 97 commands we do not implement, the introspection that answers wrongly,
and the two architectural blockers in front of the rest. This section
records what is done and what is known-open in the surface we already have.

### The dispatch handoff

See [Where the ceiling is](#where-the-ceiling-is). Closing it needs its own
design.

### Redis parity

This is done, and everything else waited on it. The audit is automated (see
[Parity against a real Redis](#parity-against-a-real-redis)), and what it
reports is recorded in `docker/e2e/parity-baseline.json`. Performance work
comes after this and is bounded by it: a change that buys throughput by
widening a parity or security gap is not worth making.

**2 differences over 1,394 replies**, down from 189. Every phase is done: 197
closed, and a sharper harness added 10 along the way. Both survivors were the
same one, once per half: `INCRBYFLOAT` computed in `f64` where Redis uses
`long double`. That is fixed now (see
[phase 2](#phase-2-ranges-shapes-and-ties)). The harness has grown since, to
2,250 replies. Against x86-64 Redis, the build CI compares with, it finds no
difference. Against arm64 Redis it finds two per half: the last digits of a
`GEOSEARCHSTORE … STOREDIST` distance, and `INCRBYFLOAT`, because arm64 Redis
has a 128-bit `long double`.

The gate fails only on a *new* difference. The baseline keeps the two GEO
entries, noted as arm64-only, so they do not trip it. The `INCRBYFLOAT` case
is not in it, so a local run on an arm64 machine reports it as new; check a
float change against x86-64 Redis instead, in CI or with
`scripts/gen_longdouble_cases.exs`.

#### The plan

The six phases are ordered by what a client loses, regardless of effort. Every difference in
the baseline belongs to exactly one of them.

| # | Phase | Closes | Needs a decision |
|---|---|---:|---|
| 0 | [Broken, not different](#phase-0-broken-not-different) | **done** — 24 | no |
| 1 | [Input Redis refuses](#phase-1-input-redis-refuses) | **done** | no |
| 2 | [Ranges, shapes and ties](#phase-2-ranges-shapes-and-ties) | **done** | no |
| 3 | [Error wording](#phase-3-error-wording) | **done** | taken |
| 4 | [The key directory](#phase-4-the-key-directory) | **done** — 40 | taken |
| 5 | [Missing commands](#phase-5-missing-commands) | **done** — 38 | taken |

Phases 0–3 are independent of each other and of 4; 5 is last because a command
that does not exist costs a client less than one that answers wrongly. Phase 4
was the only one that changed a data structure.

Each phase ends the same way: `mise run parity-update`, the baseline shrinks by
the stated count and by nothing else, and every behaviour it changes gains an
end-to-end test on **both** halves.

#### Phase 0: broken, not different

**Done.** These were not parity gaps. They could not work at all, all of them
were on the SQL half, and each one killed the worker before `in_subtransaction`
learned to catch it. They went first because the harness reached them by
accident and a client would reach them in ordinary use.

| What | Why it failed | Fix |
|---|---|---|
| `HINCRBY` — **every call** | The statement inserted `$3::text` into a `bytea` column and cast `bytea` to `bigint` without `encode`, so it raised at analysis time. It had never run, and nothing tested it. | `convert_to`/`encode`, as the KV counters use |
| `MSET k v1 k v2`, `HSET h f v1 f v2`, `ZADD z 1 a 2 a` | `ON CONFLICT DO UPDATE` cannot touch a row twice in one statement | `dedup_last` before binding, which is Redis's last-wins |
| `RENAME` onto an existing key | Primary-key violation; a CTE that deletes the destination is not visible to the update beside it | The destination is cleared in its own statement, and only when the source is there, so a `no such key` leaves it alone |
| `LINDEX key <large negative>` | `OFFSET` is evaluated even when the `WHERE` excludes every row, and a negative one raises | `GREATEST(…, 0)` |
| `HSET` returned fields written | Redis returns fields *added* | `RETURNING (xmax = 0)`, counted |
| `SUNIONSTORE`/`SINTERSTORE`/`SDIFFSTORE` onto an overlapping destination lost the overlap | The insert's uniqueness check still sees rows the same statement deleted, so `ON CONFLICT DO NOTHING` skipped them and the delete kept them | Delete only what is *not* in the result, as the sorted-set stores already did, and return the result's cardinality |
| `EXISTS k k` counted 1 | `count(DISTINCT key)` | One row per argument |
| `RENAME` dropped the expiry (shared memory) | The value moved, the expiry did not | Read `mem_ttl_raw` before the value is taken |
| `GETSET` kept the expiry (SQL) | Redis discards it with the value | `expires_at = NULL`, as `SET` |

The last two came out of the harness itself: `TTL` was compared by reply *shape*,
so `-1` and `100` looked alike and every expiry bug was invisible. It now
compares the class (gone, no expiry, or some expiry), which cost nothing and
immediately showed both, plus five more that belong to phase 4.

**Also here:** a caught SQL error is logged with its message and SQLSTATE
(`pg_redis: command failed: …`). Catching it stops PostgreSQL reporting it, so
without this the operator got nothing, which is how `HINCRBY` had been failing
unnoticed.

#### Phase 1: input Redis refuses

**Done.** Every fix is in the parser, so each behaviour needed one fix instead
of two: both backends read the same `Command`.

- **Expiry validation.** `check_expire` refuses a non-positive expiry and one
  whose deadline could not be represented, so `SET k v EX 0`, `SETEX k 0 v` and
  `EXPIRE k 9999999999999999` are errors rather than a delete or a date in the
  far future. `check_expire_deadline` allows a past deadline, since `EXPIRE k -1`
  legitimately deletes the key.
- **Count validation.** `check_count` on `SPOP`, `LPOP`, `RPOP`, `ZPOPMIN` and
  `ZPOPMAX`. Not on `SRANDMEMBER` or `ZRANDMEMBER`, where a negative count means
  "with repeats".
- **Integer canonicality.** `mem::parse_stored_int` reads a counter the way
  Redis does (no `+1`, no `007`, no `-0`), and the SQL guard was widened to
  match, including a `numeric` range check so a stored value too large for
  `bigint` reads as "not an integer" instead of raising an overflow.
- **Float validation.** A score literal that overflows to infinity is treated
  as out of range, so `ZADD z 1e400 m` is refused; only the spelled forms mean
  infinity. A `ZINCRBY` landing on NaN is refused without storing it, and so is
  an `INCRBYFLOAT` whose *result* is not finite. Redis checks the result and not
  the argument, and accepts an infinite increment.

#### Phase 2: ranges, shapes and ties

**Done.**

- **Range clamping.** `range_bounds` replaced four copies of the same
  arithmetic, three of which wrapped: `(stop + len) as usize` on a stop past the
  start is enormous, clamps to the last element and selects everything, which is
  how `ZREMRANGEBYRANK key -100 -100` emptied a sorted set.
- **Reply shape.** `Response::NullArray` exists now, so `LPOP key 1` on a
  missing key answers `*-1` and `LPOP key 0` on a live list answers `*0`. A
  client asking for a list back no longer gets a nil string.
- **Tie-break.** `zset_meta_member_added` compares members when scores are
  equal, which costs one `hash_search` on the tie and nothing otherwise.
- **Float precision.** `INCRBYFLOAT` and `HINCRBYFLOAT` were `f64`, where
  Redis uses C `long double`, so `3 + 1.000000000000000005` was `4` here and
  `4.00000000000000001` in Redis on x86-64. They now compute as x86-64 Redis
  does, in the x87 80-bit format, in `src/longdouble.rs`, which both halves
  call. Parsing follows glibc's `strtold` as Redis's `string2ld` uses it:
  decimal, hex floats and `inf`/`infinity`, rounded to nearest by the
  `rustc_apfloat` crate, with NaN, overflow and underflow to zero refused. The
  sum is rounded to nearest, and the result is printed as `%.17Lf` prints it,
  from the exact binary value, then trimmed as `ld2string` trims it.
  `scripts/gen_longdouble_cases.exs` records about 4,900 replies from x86-64
  Redis 7.2 into `tests/fixtures/`, and a unit test replays them; every one
  matches. The reference is the Debian image (glibc). The Alpine image (musl)
  differs on 13 of them, all past the 80-bit range, where musl's `strtold`
  does not report the overflow or underflow and for some hex floats returns a
  wrong value. Redis on arm64 has a 128-bit
  `long double` and can print other last digits.
- **Increment order.** `INCRBYFLOAT` with an increment that is not a float now
  checks the key's type first, as Redis does, so a list key answers
  `WRONGTYPE`.

#### Phase 3: error wording

**Done.** The decision was to use Redis's strings verbatim. `wrong_arity`,
`NOT_AN_INTEGER`, `NOT_A_FLOAT` and `invalid_expire` name them once each, which
replaced about sixty hand-written variants. `GET missing argument at position 0`
became `wrong number of arguments for 'get' command`, and every `X requires
integer` became `value is not an integer or out of range`.

Some messages are specific to one command: `LSET` on a missing key says `no such key`
rather than `index out of range`, `HINCRBY` names the hash, `SELECT 99` is out
of range rather than unparseable, and `DECRBY key -9223372036854775808` gets
Redis's own `decrement would overflow`.

#### Phase 4: the key directory

**Done.** The decision was one key directory per database, chosen over probing
the other tables on every write. It closed two problems that looked separate:

- **A key could hold every type at once (30).** `SET k v` then `LPUSH k a`,
  `SADD k m`, `HSET k f v` and `ZADD k 1 m` all succeeded; `TYPE` said `string`,
  and each accessor returned its own data. `SINTERSTORE` onto a key holding a
  string was the same bug from the other side: the set appeared and the string
  stayed.
- **Collections could not expire (4).** `EXPIRE` on a list, set, hash or sorted
  set returned 0 and set nothing, so every cache-with-a-TTL pattern over a
  collection kept its data forever.

Both had one cause: nothing recorded what a key was. Each type had its own
table, keyed independently, and none consulted the others.

**The shared-memory half** gained a tenth HTAB, a key directory. It has since
been replaced, with the rest of the fixed tables, by one dictionary per database
([§3](#one-database-one-dictionary)). The hash table also gained the count meta the set table already had,
which is what tells the directory its last field has gone, and makes `HLEN` a
lookup rather than a scan of the whole database's hash table.

**The durable half** derives the type instead of storing it: the five tables are
already keyed by `key`, so one query answers "what holds this" for every key a
command names, in a single round trip before the command runs. There is nothing
to keep in step; the cost is that query. Only the expiry needed somewhere to
live: `redis.expiry_N`, one row per collection carrying a TTL, because a
string's `expires_at` column is the only one in the schema.

Three things fell out of building it that the parity harness had not asked for:

- `SET k v NX` over a list said OK on the durable half, because the `NX` test
  looked at `redis.kv_N` alone and a list is invisible there. So did `SETNX` and
  `MSETNX`. All three ask the whole directory now, and `XX`, whose condition a
  collection satisfies, replaces it instead of doing nothing.
- Redis discards a key's TTL whenever its value is replaced, `SUNIONSTORE` onto
  a live set included. Only the type change was clearing it.
- An expired collection has to be gone, not merely untyped: `TYPE`, `EXISTS`,
  `TTL` and `DBSIZE` on the durable half read `redis.expiry_N` and treat a
  passed deadline as absent, the way they already treat an expired string.

**What it costs the durable half.** The type query is a round trip the SQL
backend did not make before, so where it lands matters:

- A **write** is checked up front, because it cannot be allowed to land and
  then be refused. `LPUSH`, `SADD`, `HSET` and the like pay one lookup each.
- A **read** runs first and asks only when it came back empty: a key holding
  another type finds nothing, and so does a key that is not there. Every
  successful `GET`, `HGET` and `LRANGE` is unchanged.
- The commands that only **replace** a key (`SET`, `SETEX`, `PSETEX`, `MSET`)
  ask nothing. They clear the other tables from inside their own
  statement, as four extra index probes. Doing it as a lookup first halved
  `SET` throughput on the durable half in the first measurement, which is what
  prompted folding it in.

The benchmark numbers themselves are not worth quoting from this container: the
spread between identical rounds is two to three times, which rules out a
collapse and nothing finer.

Still open, and not part of the 40: `KEYS`, `SCAN` and `RANDOMKEY` see only the
KV table. The directory holds a key's hash and not its bytes, and no other table
stores a collection's name, so there is nothing to list them from. Fixing it
means either an inline key prefix in the directory entry (32 bytes becomes 80,
plus a sixth chunk pool) or a per-key table on each side. Both have a cost, and
neither was in scope for phase 4. All three have since been closed: `KEYS` and
`SCAN` list keys of every type, and `RANDOMKEY` draws from every live key (see
[below](#randomkey-favours-strings)).

#### Phase 5: missing commands

**Done.** Every command the harness asked for and did not get, plus `GETBIT`
and `FLUSHALL`, which it never asked for because neither can be tested against
a shared reference server without wrecking it.

`FLUSHDB` went first, out of the documented order, and paid for itself
immediately: the harness used to reset by deleting the tokens its own cases
mention, so anything an e2e run had written was still there and `KEYS *`
reported it as a difference on work that was fine. It cost two false failures
in one session. The harness flushes now, and its cases no longer have to
declare their keys.

| Command | Notes |
|---|---|
| `SETRANGE`, `GETRANGE` | `SETRANGE` pads the gap with NUL bytes, which `chr(0)` cannot express in PostgreSQL text; the SQL half builds the padding as hex |
| `SETBIT`, `GETBIT`, `BITCOUNT` | Redis numbers bits from the most significant of byte 0; PostgreSQL's `get_bit` counts from the least, so both halves compute in Rust and agree by construction |
| `HINCRBYFLOAT`, `HRANDFIELD` | A field entered for an increment that turns out to be invalid is removed again, rather than left behind empty |
| `RPOPLPUSH` | Parses straight to `LMove { src_left: false, dst_left: true }`, so the two commands share one implementation |
| `RENAMENX`, `GETEX` | |
| `SINTERCARD` | `LIMIT 0` means no limit, which is Redis's reading and not the obvious one |
| `COPY` | See below |
| `OBJECT ENCODING` | See below |
| `FLUSHDB`, `FLUSHALL` | See below |

Three of them needed a decision first.

**`COPY` crosses the two halves.** `COPY k k DB 8` promotes a shared-memory
cache entry to a durable row, and `DB 0` brings one back. That is the one place
`COPY` does more here than in Redis, which has no such split. It reads through
`dump_key` into a form neither backend stores and writes through `restore_key`
into whichever the destination names. It is the only command that has to, which
is why the pair exists and why `COPY` is routed to the SPI path whatever the
databases' policies.

**`OBJECT ENCODING` reports Redis's representation names.** `int`,
`embstr`, `raw`, `listpack`, `quicklist`, `intset`, `hashtable`, `skiplist`.
A string's and a list's are worked out from the value; a hash's, set's and
sorted set's are the encoding each one carries, kept as Redis keeps it
([Order and encodings](#order-and-encodings)). The memory
half used an inline slot and a chunk pool then, and has used ordinary
collections in an arena since phase 6; the durable half uses a `bytea` column.
None of those names describes either. Reporting our own would be truthful but would break every client
that branches on Redis's, so the decision was to satisfy the client. A missing
key answers nil. That is Redis's own reply for this command, where nearly every
other command gives an error.

**`FLUSHALL` empties all sixteen databases, the durable half included.** That
matches Redis and was chosen deliberately over a safer default: databases 8–15 are
WAL-backed, and the Redis port bypasses PostgreSQL authentication, so any client
that can reach the port can erase them. `redis.listen_address` defaults to
loopback and `redis.password` exists; both matter more now than they did.
`FLUSHALL` is routed to the SPI path for the same reason `COPY` is: it is the
only other command that touches both halves.

Two things fell out that the harness had not asked for. `DBSIZE` on the
shared-memory half counted strings alone, reporting 1 for a database holding a
string, a list, a hash, a set and a sorted set; it reads the key directory now.
And a flush writes to keys it cannot name, so `WATCH` needs every counter
bumped, not only the ones `write_keys` returns. `watch::bump_all` therefore also
aborts watches on databases the flush did not touch, which is the direction
that table is allowed to err in.

### An expired collection outlives its own expiry

Key-level reads are exact the moment the deadline passes: `TTL`, `TYPE`,
`EXISTS` and `DBSIZE` go through the key directory on the shared-memory half and
`redis.expiry_N` on the durable one, and both read a passed expiry as absent.

The type-specific reads do not. `LLEN` reads the list's own meta, `SCARD` the
set's, `LRANGE` the entries, and none of them consults the directory. For up to
a second, until the sweep runs, `TYPE` answers `none` while `LLEN` still reports
the old count. Redis expires lazily on every access and has no such window.

**Fixed, on both halves.** The memory half went the way this entry suggested:
`dir_lookup_raw` distinguishes "absent" from "expired", and
`mem_key_kind_reaping` drops an expired key at the type gate every command
already passes through, with the same single directory lookup as before. The
durable half took two mechanisms, decided in `Command::execute` and nowhere
else: the cached single-key collection reads carry `sql::live` in their own
predicate, an InitPlan that rides inside the round trip they were already
making, and every other collection read reaps its keys first with
`sql::reap_expired_keys`. What each choice costs, measured, is in
[PARITY.md](PARITY.md#the-stale-collection-read-was-bigger-than-this-table-said).

### `RANDOMKEY` favours strings

**Fixed, on both halves.** The durable half turned out worse than this entry
said. It read `kv_N` alone, so a database holding only collections answered
*nil* while `DBSIZE` counted them. Both halves now draw uniformly over every
live key: the durable half with a five-table `UNION` under
`ORDER BY random()`, the memory half with a single-pass reservoir over the KV
scan and the collection names, expired-but-unswept keys excluded on both. The
write-up is in [PARITY.md](PARITY.md#tier-1-commands-we-have-that-behave-differently).


### Settled, so not on this list

- **Fixed-size shared-memory tables:** replaced by the arena and one
  dictionary per database ([§3](#3-the-shared-memory-backend)), which took the
  entry limits, the 64 KiB value cap and the per-table locks with them.
- **A SQL error killing the worker:** `INCR` past `bigint`'s ceiling raised
  out of the dispatcher and ended the process; four of those ended the Redis
  endpoint. `in_subtransaction` catches now, and the reply matches Redis.
- **Where a collection's name is stored:** in the four meta tables, not the key
  directory. It was priced both ways: 6 MB against 22 MB.
- **What `OBJECT ENCODING` reports:** Redis's representation names, not ours.
  See [phase 5](#phase-5-missing-commands).
- **How far `FLUSHALL` reaches:** all sixteen databases, as in Redis, the
  durable half included. Also [phase 5](#phase-5-missing-commands).

## Users and permissions

`src/acl.rs` follows Redis 7.2's `acl.c`. A user has flags (`on`/`off`,
`nopass`, the sanitize-payload pair), SHA-256 password hashes and a list of
selectors, the root one first. A selector holds a command bitmap with one bit
per entry of the generated command table (`redis_tables.rs`) and one more
that only `+@all` sets, for "commands added later"; the first arguments
allowed with `+cmd|arg`; key patterns with read and write flags; channel
patterns; and the command rules in the order they still apply, which is how
`ACL GETUSER` and `ACL LIST` describe the bitmap back, compacted as Redis
compacts them. Categories are each command's explicit ones plus the ones its
flags imply, so `+@write` covers what Redis's does.

**Shared state.** The live users are in a shared-memory block, one per
server: each user's `ACL LIST` description and a serial that stays with the
user until it is deleted, behind a lock, with a version counter. Every
connection thread checks the counter before each command (one atomic load);
when it moved, the process parses the descriptions once and every thread
uses that copy. A change takes the lock, applies the rules to a copy of the
user (so a refused rule changes nothing), writes the users back and bumps the
counter, so the next command on every worker sees it. The ACL LOG is a second
locked region in the same block. Each connection's slot in the client table
records its user's serial, which is what `CLIENT LIST` shows as `user=`,
what `CLIENT KILL USER` matches, and how the dispatcher finds the user of a
queued command or a script.

**Enforcement.** A connection checks every command after its name and arity
and after the `NOAUTH` gate, before it is parsed further, run or queued, the
order `processCommand` uses: a refused command inside `MULTI` flags the
transaction. `EXEC` checks each queued command again and answers Redis's
"ACLs rules changed" error for the ones no longer allowed. A RESP2
subscriber's commands are checked in its subscribe loop. A script's
`redis.call` checks the calling client's user on the dispatcher, with the
error `ACL failure in script: ...`, and `redis.acl_check_cmd` answers for the
same user. Keys come from the command's key specs, read for `ACCESS` and
write for `INSERT`, `DELETE` and `UPDATE`, or from the command's own key
finder where its specs cannot be followed (`SORT`, `MIGRATE`, `EVAL` with a
bad key count), as `getKeysFromCommandWithSpecs` does. Channels are those of
`PUBLISH`, `SPUBLISH` and the subscribe commands; a `PSUBSCRIBE` pattern is
compared with the user's patterns literally. `SORT ... BY` and `GET` need
read access to every key, as in Redis.

**Changes and clients.** `ACL DELUSER` closes the deleted user's clients,
and `ACL LOAD` the clients of every user but `default`, as Redis frees them.
When `ACL SETUSER` takes channel permissions from a user, each of its
subscribers checks its own subscriptions and closes if one is no longer
allowed. `off` and `resetpass` close nothing, as in Redis. A connection
thread waits up to a second for the clients it closed to go before it
replies; inside `EXEC` the dispatcher does not wait.

**Persistence.** As in Redis, users persist only through an ACL file,
`redis.aclfile` (`aclfile`). Without one, `ACL LOAD` and `ACL SAVE` refuse
with Redis's error, and `ACL SETUSER` changes only the live users. With one,
`ACL SAVE` is `ACLSaveToFile`: every user as a `user` line, written to a
temporary file beside it, synced, renamed over it, and the directory synced.
`ACL LOAD` is `ACLLoadFromFile`: every line checked first, nothing changed if
one is wrong, with its messages. The connection thread reads and writes the
file itself. At start, `ACLLoadUsersAtStartup`: one worker loads the file
before anyone is served, and a file that cannot be loaded stops it with the
error, as Redis refuses to start; the others wait, and whichever starts next
tries again. `redis.password` is the default user's password: applied at
start unless the file has a line for `default`, again on a reload that
changes it, and `CONFIG SET requirepass` sets it at runtime, as
`updateRequirePass` does.
