# Implementation

How pg_redis works, end to end: the process model, the three storage backends,
the shared-memory layout, pub/sub, WATCH, the limits every one of those imposes,
and how to test and benchmark a change to any of it.

This is the single reference for internals. The pages beside it are for using
the extension rather than changing it — [Installation](installation.md),
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

A worker is two things at once:

- **Connection threads.** One per client. They own the socket, parse RESP2, and
  write replies. They are ordinary OS threads, not PostgreSQL backends.
- **A dispatcher** on the worker's main thread — the only thread with a valid
  `MyProc`, and therefore the only one allowed to touch PostgreSQL LWLocks or
  SPI.

Every data command crosses between them twice: connection thread → dispatcher
(`DispatchMsg`), dispatcher → connection thread (the reply). Those two mpsc hops
are the throughput ceiling; see [Performance](#8-performance).

Connection-local commands — `PING`, `SELECT`, `AUTH`, `MULTI`/`EXEC` queueing —
never reach the dispatcher and cost nothing but the parse.

### Command path

```
socket ─> resp::parse ─> Command::parse ─┬─> execute_mem   (shared memory, db 0–7 in memory mode)
                                         └─> execute_spi   (SPI + SQL, everything else)
```

`Command` is a single enum covering every supported command. Parsing is
separated from execution so the parser can be unit-tested without a PostgreSQL
instance, and so size limits can be checked *before* anything is written — see
[Limits](#7-limits).

### The 16 databases

Databases mirror Redis's model and split into two contiguous halves.

| DB | Name | Backend | Durability |
|---|---|---|---|
| 0–7 | `cache` (0) | shared memory, or UNLOGGED tables | Lost on restart |
| 8–15 | `durable` (8) | WAL-logged tables | Survives crashes |

`SELECT` accepts the numbers and the two names. `redis.default_db` (default 0)
chooses where new connections start. The halves are contiguous rather than
odd/even because `execute_mem` indexes the shared-memory tables with the raw
database number; a unit test pins that correspondence.

### Batching and transaction isolation

The dispatcher coalesces up to `redis.batch_size` (default 64) commands from
*unrelated* connections into one SPI transaction, which is what makes the SPI
path bearable at all. That creates a hazard: one client's failing command must
not roll back another client's write.

Each command therefore runs in its own subtransaction. `worker.rs` calls
PostgreSQL's subtransaction primitives directly rather than issuing `SAVEPOINT`
/ `ROLLBACK TO` / `RELEASE` as SQL — three statements parsed, planned and
executed through SPI per command, which for a plain `SET` cost more than the
`SET`. It mirrors the save/restore dance PL/pgSQL performs around its
`EXCEPTION` blocks.

---

## 2. Storage backends

The ephemeral half has two, chosen by `redis.storage_mode` (a `postmaster`-scope
GUC — changing it needs a restart). The durable half is always WAL-logged.

| | `memory` (default) | `auto` | durable (8–15) |
|---|---|---|---|
| Storage for db 0–7 | Shared-memory HTAB per data type | UNLOGGED tables | — |
| Transaction | None | `BEGIN`/`COMMIT` per batch | per batch |
| SPI overhead | None | Yes | Yes |
| WAL | No | No | Yes |
| Survives crash | No | No (UNLOGGED, truncated) | Yes |
| Replication | No | No | Yes |
| SQL-visible | No — `SELECT * FROM redis.kv_0` returns nothing | Yes | Yes |
| Max keys | `redis.mem_max_entries` per type per db | Unlimited (disk) | Unlimited |
| Shared memory at the default | ~58 MiB, reserved at start | None | None |
| Max value | 64 KiB | Unlimited (TOAST) | Unlimited |
| Max key | 512 bytes | Unlimited | Unlimited |
| Max hash field / set member | 512 bytes | Unlimited | Unlimited |

### SQL schema

Both SPI backends use the same shape in the `redis` schema, one table per data
type per database; 0–7 `UNLOGGED`, 8–15 logged.

```sql
CREATE SCHEMA redis;

-- Key-value (pattern repeats for db 0–15)
CREATE UNLOGGED TABLE redis.kv_0 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE redis.kv_8 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);

-- Hash (same pattern)
CREATE UNLOGGED TABLE redis.hash_0 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE redis.hash_8 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
```

Every column is `BYTEA`, never `TEXT`. Redis keys, values, fields and members
are arbitrary byte strings, and `TEXT` cannot hold a NUL at all. Two
consequences when querying directly:

```sql
-- Cast to read.
SELECT convert_from(key, 'UTF8'), convert_from(value, 'UTF8') FROM redis.kv_8;

-- Cast the other way to compare (a plain literal is coerced, so it also works).
SELECT value FROM redis.kv_8 WHERE key = convert_to('mykey', 'UTF8');
```

Ordering is bytewise, which is what Redis means by lexicographic — `ZRANGEBYLEX`
and `ORDER BY member` agree with Redis rather than with a database collation.

Every `kv_*` table carries a partial index on `expires_at` for the SQL expiry
sweep; a unit test asserts it exists on all sixteen.

---

## 3. The shared-memory backend

All of `src/mem.rs`. Eight databases, ten tables each, and six LWLocks per
database — one per data type, with each meta table covered by its type's lock,
plus one for the key directory. So a write to db 0 never blocks a write to db 2,
and the hash table never blocks the list table.

Every one of them is taken through `htab::LockGuard`, which releases on the way
out of scope. There were 226 hand-written acquire/release calls before, and
twenty-six functions had between two and five release points each — one per way
out. Missing one is not subtle: the worker wedges on its next command, which is
what `HINCRBY` on a non-integer field used to do. Two rules the guard does not
enforce, so they are written down instead:

- **The directory's lock is innermost** — see [The key
  directory](#the-key-directory).
- **A guard may not be held across a call that takes a lock**, because an
  LWLock is not re-entrant. `LPUSHX` checking the list exists and then calling
  `mem_lpush` is the shape that bites; it scopes the check so the guard is gone
  before the call. `KEYS` does the same before reading the four meta tables.

### Tables and entry layouts

Ten HTABs per database, created with `ShmemInitHash` during
`shmem_startup_hook`, all `HASH_BLOBS` and `HASH_FIXED_SIZE`.

| Table | Entries | Entry | HTAB key | Size at the default |
|---|---|---:|---|---:|
| KV | `mem_max_entries` | `KvEntry` 232 B | 16 B key hash | 16.06 MiB |
| Key directory | see below | `DirEntry` 32 B | 16 B key hash | 11.03 MiB |
| Hash | half | `HashEntry` 160 B | 16 B key hash + 16 B field hash | 5.83 MiB |
| Zset | half | `ZsetEntry` 96 B | 16 B key hash + 16 B member hash | 3.77 MiB |
| List | half | `ListEntry` 96 B | 16 B hash + 8 B position | 3.77 MiB |
| Set | half | `SetEntry` 88 B | 16 B key hash + 16 B member hash | 3.55 MiB |
| Zset meta | half | `ZsetMeta` 120 B | 16 B key hash | 4.56 MiB |
| List meta | half | `ListMeta` 88 B | 16 B key hash | 3.55 MiB |
| Set meta | half | `CountMeta` 72 B | 16 B key hash | 3.05 MiB |
| Hash meta | half | `CountMeta` 72 B | 16 B key hash | 3.05 MiB |

"half" is `mem_max_entries / 2`, floored at 256. Sizes are dynahash's own
`hash_estimate_size` — the bucket array, the segment directory and the
per-element header — multiplied across eight databases. That call replaced a
hand-rolled `× 5/4 + 8 KiB`, which was 20% short: the postmaster refused to
start the first time a table wanted three times the entries. A unit test pins
every entry size, because each one is multiplied by `mem_max_entries` across
eight databases and requested *before* the postmaster starts — growing one
carelessly turns "slower" into "does not boot".

The meta tables exist so `ZCARD`, `SCARD`, `HLEN`, `LLEN`, `ZPOPMIN`/`ZPOPMAX`
and `LPUSH`/`RPUSH` are O(1) rather than a scan of the entry table. The set and
hash tables need nothing per key but a count, so they share one `CountMeta`.

They also hold **the key's own name** — 40 bytes inline, the tail chained into
that type's existing pool, the way a `KvEntry` holds its key. Nothing else here
writes a collection's name down: the entry tables key on `(key hash, member)`
and the directory on a hash alone, so before this `KEYS` and `SCAN` could see
strings and nothing else while `DBSIZE` counted every type. One entry per key
per type, so the four meta tables between them name every key that is not a
string, and `KEYS` is a scan of those four plus the KV table.

The name is written best-effort: it is not the only copy of anything, so a pool
with no room costs the key its place in `KEYS` and nothing else, with a warning
logged. Refusing the write instead would have to undo members already stored.
`reserve_member_chunks` counts the name's chunks alongside the member's, so a
pool that is not already exhausted has the room.

**A meta entry owns that chain.** Every removal — `remove_count_meta`,
`remove_meta`, `remove_zset_meta`, and `remove_meta_of` on the `DEL` and
eviction paths — hands it back before the entry goes. Missing one is not a leak
that shows up later: the next occupant of that slot inherits a head pointing
into the free list, and the worker aborts. Which is what it did, once.

### The key directory

The five tables that hold data are keyed independently and none of them asks the
others, which is how one key came to hold a string and a list and a set at once,
and why only a string could carry an expiry. The directory is the one table that knows
what a key *is*: `(key hash) → {kind, expires_at}`, one entry per live key.

- `TYPE`, `EXISTS` and `TTL` are one lookup in it, where `TYPE` used to probe
  five tables under five locks in turn.
- A write to a key of another kind is refused with `WRONGTYPE`, and the handful
  of commands Redis lets through — `SET` and the `*STORE` destinations — clear
  what was there first.
- It holds every expiry, so a list, set, hash or sorted set can carry a TTL. A
  string keeps a second copy in its `KvEntry`, which is what the `GET` path
  reads without taking a second lock; every write of one goes through
  `kv_write_full_value`, so the two cannot drift.
- The expiry sweep reads it rather than the KV table, and so reaches every type.

It is sized `mem_max_entries + 4 × half` — every table can fill independently,
and a member table's worst case is one key per entry — so the directory is never
what runs out first. A refusal the operator cannot act on is worse than the
2.5 MiB that sizing costs over one entry per KV slot.

**Lock order: the directory's LWLock is the innermost one.** A path already
holding a table's lock may take it; nothing may take a table's lock while
holding it. Every `dir_*` helper acquires and releases it, so callers hold
nothing extra.

The directory does not store key bytes — a hash is all it holds. That is why
`KEYS` reads the four meta tables rather than this one: see [Tables and entry
layouts](#tables-and-entry-layouts). Storing names here instead would have cost
~22 MB against the metas' 6 MB, because the directory is sized for every table
filling independently and a third of its entries are strings, whose names the KV
table already holds.

### Key hashing

**Every table indexes on a 128-bit keyed SipHash-2-4 of the Redis key**, not on
the key bytes, and the hash, set and zset tables hash their member the same way
— their HTAB key is 32 bytes, `(key hash, member hash)`. The key material is
drawn from `pg_strong_random` once per postmaster, so a client cannot craft two
keys that collide and merge their contents. Shared memory does not survive a
restart, so neither does the key, and it never has to match anything on disk.

Nothing keys on the Redis key bytes. A full copy of the key in each entry would
dominate a collection — 512 bytes against a `SetEntry`'s 88, repeated for every
member — and the tables never need the bytes: every access is a comparison
against a key the caller already holds.

The bytes an entry *does* keep are the ones a client asks for back: the KV
table's key, because `KEYS`, `SCAN` and `RANDOMKEY` return real keys, and the
member of a hash, set or zset, because `HGETALL`, `SMEMBERS`, `SPOP` and
`ZRANGE` do. Both are stored the same way — an inline prefix and a pooled tail,
described below.

No table is `HASH_STRINGS`, and none may become one: dynahash compares string
keys with `strncmp`, which makes two keys agreeing up to their first NUL byte
the same key.

### How a KV entry stores its key

`KvEntry` keys on the hash and keeps the key bytes alongside it, laid out the
way a value is — an inline prefix with the tail chained through the same chunk
pool:

```
KvEntry  = key hash 16 | key_inline 128 | value_inline 64 | expires_at 8
                       | key_len 4 | key_overflow 4 | value_len 4 | overflow 4
```

`INLINE_KEY_LEN` is 128, and the reason is the pool rather than the entry. Eight
bytes of `KvEntry` is 640 KiB of shared memory (8192 entries × 8 databases ×
5/4), so the prefix costs 10 MiB. At 64 it would cost 5 MiB — but a key of
65–128 bytes would then take a chunk, and `myapp:prod:session:<uuid>` is 60 to
90 bytes, so a full table of entirely ordinary keys would claim every chunk in
the pool and leave nothing for any value past its own 64-byte inline slot. At
128 the same cliff sits at 129 bytes, which conventional keys do not reach.

A longer key still works, at one chunk per 64 bytes over:

| Key length | Chunks per key | Keys before the pool is empty |
|---:|---:|---:|
| ≤ 128 bytes | 0 | 8,192 (the table's own limit) |
| 129–192 | 1 | 8,192, with no pool left for values |
| 193–256 | 2 | 4,096 |
| 449–512 | 6 | 1,365 |

### How a collection entry stores its member

The same shape, at a different size:

```
SetEntry  = key hash 16 | member hash 16 | member_inline 48 | member_len 4 | member_overflow 4
ZsetEntry = the same, plus score 8
HashEntry = key hash 16 | field hash 16 | field_inline 48 | value_inline 64
                        | field_len 4 | field_overflow 4 | value_len 4 | overflow 4
```

`INLINE_MEMBER_LEN` is 48, not the key's 128, and the arithmetic behind it is
different in both directions. These tables hold `mem_max_entries / 2` entries,
so eight bytes of entry costs 320 KiB rather than the KV table's 640 KiB — but
the cliff that forced the key prefix up to 128 does not exist here. That cliff
was keys competing with values for one slab: a key past its prefix takes a chunk
the values need. The set and zset pools hold *only* members and carry one chunk
per entry slot, so a table entirely full of members of 49–112 bytes still fits —
each takes exactly the chunk its own entry paid for. The hash pool carries two
chunks per entry slot, so the same table of fields still leaves one chunk each
for the values. 48 covers what members are: a UUID is 36 bytes.

The set and zset tables had no pool before this; they have one each rather than
one shared, because those two tables are covered by *different* LWLocks and a
shared free list would have two writers that never exclude each other.

### Lookups verify the key

Because an entry holds the bytes it hashed, `kv_find` and `kv_enter` — and
`member_find` and `member_enter` for the three collection tables — compare what
is stored against what was asked for. A mismatch reads as "absent" and refuses
the write, so a hash collision cannot return or overwrite another key's value or
another member's. It has its own error rather than an OOM, because raising
`mem_max_entries` is the obvious response to an OOM and does nothing for a
collision:

```
ERR key or member hash collides with a different one already stored; rename it, or use SELECT durable.
```

The cost is a length test plus, at most, a compare of the inline prefix against
bytes the lookup has already pulled into cache; only a key or member longer than
its prefix and matching on both goes as far as walking the pool. Unreachable in
practice at `mem_max_entries` entries per database — it exists because the
alternative is to write over what is already there, silently. The collection
tables could not do this while the member *was* their HTAB key: two colliding
members were one entry, with nowhere to put the loser.

### The chunk pool

One slab per KV, hash, list, set and zset table per database — 40 in all —
carved out of a single `ShmemInitStruct` allocation and indexed by stride.

```
[ValPool header][next: u32 × capacity][data: u8 × capacity × 64]
```

`capacity` is `mem_max_entries` chunks of `CHUNK_LEN` = 64 bytes for the three
pools values spill into, and half that — one chunk per entry slot — for the two
that hold members and nothing else. The `next` array doubles as the free list
*and* as the chain of a stored value, so a chunk is on exactly one of the two at
any moment and no separate bitmap has to be kept in step. Allocating *n* chunks
takes a prefix of the free list — one pointer moved, not *n*. Freeing returns a
chain in one splice.

A byte string of length *n* occupies `ceil((n - inline) / 64)` chunks, where
`inline` is 64 for a value, 128 for a KV key and 48 for a member. Anything
shorter than its inline slot costs no chunk at all — which covers most of what a
cache holds; `redis-benchmark` writes 3-byte values.

**Chunk lifecycle is the easiest thing here to get wrong.** The chain head lives
in the entry, so the release must happen while the entry is still there. Every
removal goes through `remove_valued` (or `list_remove_at` and `zset_remove_at`,
its bare-HTAB twins). A `KvEntry` owns *two* chains, so its `kv_free` releases
the value chain and the key chain; a `HashEntry` owns two as well, its value's
and its field's. A path that calls `hash_search(HASH_REMOVE)` on any of the five
tables directly leaks silently until the pool runs dry.

### Value and key size limits

```
max value = min(64 KiB, 64 + (redis.mem_max_entries / 8) × 64 bytes)
```

which is **65,536 bytes** at the default, where the two bounds meet:

- **An eighth of the pool.** A pool is shared by every value in its table, so one
  value must not be able to take all of it.
- **64 KiB outright.** Every read copies the value out of shared memory while the
  table's LWLock is held, so an unbounded value would hold a whole database still
  for the length of one memcpy. That ceiling holds however large the pool is
  configured, so raising `mem_max_entries` buys capacity, not longer values.

Keys, hash fields and set members are capped at 512 bytes for the same kind of
reason — how much of the shared pool one of them may take. At the cap a member
is eight chunks of a pool holding nothing but members, so it can never claim
more of it than a value may claim of its own.

Anything over a limit is **rejected, never truncated**, and the check runs
before the command executes so a refusal never leaves a partial write behind:

```
ERR key exceeds redis.storage_mode='memory' limit of 512 bytes (use SELECT durable for unbounded values)
ERR hash field or set member exceeds redis.storage_mode='memory' limit of 512 bytes (use SELECT durable for unbounded values)
ERR value exceeds redis.storage_mode='memory' limit of 65536 bytes (use SELECT durable for unbounded values)
```

Truncation is the one failure mode worse than an error, and it is what the fixed
member arrays actually did. Storage was never the problem — the composite key
NUL-padded the member and dynahash compared all 128 bytes, so `SADD s "a\0one"`
and `SADD s "a\0two"` were two entries. The *readback* truncated: five sites
scanned the array for its first NUL, so `SMEMBERS` returned `"a"` twice, and
`SPOP` returned a string that was not in the set. `HGETALL` and `ZRANGE` did the
same to fields and zset members. Length-prefixing removed the failure mode along
with the array; db 8 never had it, which is why the end-to-end suite could not
see it from the durable half.

### When a table fills

`redis.maxmemory_policy`, named after Redis's setting:

| Policy | Behaviour |
|---|---|
| `noeviction` (default, as in Redis) | The write is refused with `OOM command not allowed when used memory > 'maxmemory'`. Nothing already stored is lost, and a multi-key write stores nothing at all — see below. |
| `allkeys-random` | Entries are evicted to make room. Collections go **whole** — never half-present — and the key being written is never chosen, so a single key larger than its own table still reports OOM. |
| `volatile-ttl` | Only keys carrying an expiry, soonest first. Falls back to refusing if none do. |

Expired entries are reclaimed before any live key is considered, under every
policy. Eviction costs one `hash_seq_search`, so it frees `EVICT_BATCH` = 64
entries at a time and amortises the scan over the next 64 inserts. Under
`noeviction` it returns immediately without scanning anything.

A full table has never been able to take the server down: every insert uses
`HASH_ENTER_NULL`, never `HASH_ENTER`. On a `HASH_FIXED_SIZE` table dynahash
answers a full table by raising `out of shared memory`, which unwinds by
longjmp — past the Rust error handling that looks like it covers it — and kills
the worker.

The **pool** can run dry before the entries do. That is reported as the same
OOM, and `reserve_chunks` (`kv_reserve` for the KV table, which must budget for
the key as well as the value) asks eviction for room the same way. It has to run
*before* the entry exists, because eviction may remove any entry in the table
including the one about to be written.

**A multi-key write is priced before any of it runs.** `MSET`, `SADD`,
`HSET`, `ZADD` and the pushes ask `mem_room_for_*` whether the table and its
pool can take the whole command; a table that cannot refuses it outright, where
storing the first few keys and then reporting OOM is what Redis does not do.
Whether a *single* key fits is still only knowable at the insert, and a
single-key command has nothing to be partial about. The check costs one lookup
per key, so it runs only for commands carrying more than one, and only under
`noeviction` — under the other policies the insert path makes room as it goes
and a refusal here would reject a command that would have succeeded. It counts
only the keys actually absent, so a full table still takes an `MSET` that
overwrites.

A refused write records *why* in a single thread-local `Refusal`, read once per
command by `execute_mem` and turned into either the OOM reply or the collision
one. Threading a result out through all 68 `mem_*` entry points — whose return
types are `bool`, `i64`, `Result<i64, String>`, `Vec<..>` and more — was the
alternative. A worker dispatches one command at a time on one thread, so the
flag cannot cross commands.

### Expiry

`KvEntry` carries `expires_at` in microseconds since the epoch, 0 for none. Reads
treat an expired entry as absent and delete it lazily; a background sweep also
walks each database once a second and removes what it finds, returning the
chunks. Only the KV table has expiry — Redis's TTLs are per key, and a
collection's key lives in its meta.

### Lists

List elements are keyed on `(key hash, position)` with positions spaced
`LIST_POS_STEP` = 1024 apart, so `LPUSH` and `RPUSH` are O(1) — they take the
next position outside the current range from the meta.

Readers walk positions as `min_pos + i * LIST_POS_STEP`, so an in-place removal
leaves a hole that silently truncates the list at the first gap while the count
still reports the full length. `LREM`, `LINSERT` and `LTRIM` therefore go
through a read-modify-write that renumbers the whole list from a fresh base —
O(n) on commands that were already O(n).

### Sizing `redis.mem_max_entries`

This one number scales the whole reservation, so it is the only knob that
decides how much memory the extension takes. Measured as `SHOW
shared_memory_size` on PostgreSQL 17 at otherwise default settings:

| `redis.mem_max_entries` | `shared_memory_size` |
|---|---|
| 256 | 215 MB |
| 4,096 | 248 MB |
| **8,192 (default)** | **286 MB** |
| 10,240 | 306 MB |
| 12,288 | 325 MB |
| 16,384 | 360 MB |
| 32,768 | 510 MB |

PostgreSQL's own share is roughly the 214 MB floor; the rest is the extension.
It also sets the size of each pool, and with it the value and member caps above.

The default was halved from 16,384 when the key limit grew to 512 bytes, and
four rounds of shrinking the entries have not paid that back: the last two
freed 28 MB and 11 MB, where doubling the setting costs 74 MB. Raising it is
supported — it buys pool capacity as well as key capacity — but it is a
memory-budget decision, not something the entry layout funds.

### How the reservation got here

| | `shared_memory_size` | What changed |
|---|---|---|
| Composite key hashing | 587 → 413 MB | Hash, set, zset and list entries key on a 128-bit hash instead of carrying a 512-byte copy of the Redis key |
| Chunked value pool | 413 → 308 MB | Three fixed overflow tables became one 64-byte-chunk slab per table; the value cap went 512 B → 64 KiB |
| KV key hashing | 308 → 280 MB | The last table storing keys verbatim; `KvEntry` 592 → 232 bytes |
| Member hashing | 280 → 269 MB | The five fixed member arrays: `ZsetMeta` 304 → 72 bytes, `HashEntry` 216 → 160, `SetEntry` 144 → 88, `ZsetEntry` 152 → 96, against 4.25 MiB of new pools for the set and zset tables |
| The key directory | 269 → 280 MB | Two tables added — the directory at 11.03 MiB and a hash meta at 1.53 MiB — against about 2 MiB the exact `hash_estimate_size` reclaimed from the old 5/4 guess |
| Collection names | 280 → **286 MB** | 40 bytes of name in each of the four meta entries, so `KEYS` can see a key that is not a string. In the directory instead it would have been ~22 MB: that table is sized for 24,576 keys per database against the metas' 16,384, and ~8,192 of those are strings whose names the KV table already holds |

---

## 4. Pub/Sub

Pub/sub is independent of `storage_mode` and always allocated — it needs
`shared_preload_libraries`, not memory mode.

Each subscribing connection claims one of 256 slots. A slot holds a ring buffer
of 256 messages and the connection's subscriptions (16 channels and 16 patterns).
`PUBLISH` writes into the ring of every matching slot and bumps that slot's tail
counter.

Subscribers **wait on the tail counter**, not on the socket. A subscriber that
discovered messages by letting a 5 ms socket read time out would cost ~200
wakeups per second while idle and still delay delivery by up to 5 ms. The futex
lives in PostgreSQL's shared memory segment, so the shared (not `PRIVATE`)
operations are required — waiter and waker are different processes. The socket
poll that remains backs off from 5 ms to 250 ms while a connection stays silent;
it bounds only how long a *command* from a quiet subscriber sits unread, never
delivery latency.

Route lookups short-circuit on an atomic counter, so an installation with no
routes configured pays nothing.

### Size limits

Everything runs through fixed-size slots, so these are hard caps:

| | Limit | Over the limit |
|---|---|---|
| Channel / pattern name | 255 bytes | `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH` error |
| Message payload | 512 bytes | `PUBLISH` errors |
| Concurrent subscriber connections | 256 | `SUBSCRIBE` returns "max pub/sub subscribers reached" |
| Channels + patterns per connection | 16 each | Further subscriptions ignored |
| Queued undelivered messages per subscriber | 256 | Further messages dropped for that subscriber |
| Active routes | 64 | — |

Names and payloads are **rejected, not truncated**: a truncated subscription
would never match a publish to the same name, leaving the client waiting on a
channel it believed it was subscribed to.

The payload cap is deliberately *not* memory mode's value cap. Every slot in the
ring reserves its payload whether or not anyone publishes, so tracking a 64 KiB
cap would cost the ring 128× what it costs now. The slot table is ~66 MiB,
allocated at server start whether or not pub/sub is used.

### Table routing

Any `PUBLISH` can additionally be INSERTed into a PostgreSQL table — enough for
[Supabase Realtime broadcast from database](https://supabase.com/docs/guides/realtime/broadcast)
or any trigger-based integration. The extension is decoupled from Supabase; it
just INSERTs rows.

```sql
SELECT redis.create_pubsub_table('public', 'chat_messages');
SELECT redis.route_publish('chat', 'public', 'chat_messages');
-- redis-cli> PUBLISH chat "hello"
SELECT channel, payload, inserted_at FROM public.chat_messages;
```

| Function | Description |
|---|---|
| `redis.create_pubsub_table(schema, table)` | Create a routing target with the required columns |
| `redis.route_publish(channel, schema, table)` | Route `PUBLISH` on `channel` into `schema.table` |
| `redis.unroute_publish(channel)` | Remove the route |

Routes live in `redis.pubsub_routes`, are loaded into shared memory at startup,
and survive restart. The INSERT is dispatched fire-and-forget *after* in-memory
delivery completes and after the `PUBLISH` reply is sent.

Bring your own table if you prefer — it needs `channel BYTEA` and
`payload BYTEA`. `BYTEA` is required, not recommended: channels and payloads are
arbitrary bytes. Because the insert is fire-and-forget, a routed insert into a
wrongly-typed table fails *after* `PUBLISH` has already succeeded, and the error
goes to the PostgreSQL log rather than to the client. Check there if routed rows
stop appearing.

---

## 5. WATCH

`WATCH` snapshots a write-version counter per key and `EXEC` aborts if it moved.

The counters live in shared memory rather than per worker because of
`SO_REUSEPORT`: a client's `WATCH` is served by whichever worker accepted its
connection, while the conflicting write may be dispatched through any other
worker. Per-process counters reported "no conflict" for writes the process never
saw, and `EXEC` committed when it should have aborted.

Keys are hashed (FNV-1a over the database number followed by the key bytes) into
a fixed table of 65,536 `AtomicU64` counters — 512 KiB — rather than stored. That
keeps the structure allocation-free, lock-free and fixed-size: no eviction
policy, and no growth a client can drive by writing unique keys. The cost is that
two keys can share a counter, which makes an `EXEC` abort when it strictly did
not have to. **That direction is safe; the reverse would not be.**

---

## 6. The RESP layer

RESP2, with `HELLO 3` negotiating RESP3 push frames for pub/sub.

| Bound | Value | Why |
|---|---|---|
| `MAX_BULK_LEN` | 512 MiB | Matches Redis's `proto-max-bulk-len` |
| `MAX_ARRAY_LEN` | 65,536 elements | |
| `MAX_INLINE_LEN` | 8 KiB | Inline (non-array) command form |
| `BULK_CHUNK` | 64 KiB | Read granularity |
| `QUEUE_LIMIT` | 10,000 | Commands queued in one `MULTI` |

A declared bulk length does not commit memory: the parser grows the buffer as
bytes actually arrive, so a client announcing a 512 MiB bulk string and then
stalling holds only what it sent, not what it promised.

Error replies carry their own code when they have one. `write_error` prefixes
`ERR` only for messages that do not already start with a code from
`ERROR_CODES`, so an OOM goes out as `-OOM ...` — which is what clients match on.

---

## 7. Limits

Everything the memory-mode backend refuses, in one place. The durable half
(8–15) has none of them and is bounded only by disk; `auto` has none of the
shared-memory ones.

| | Limit | Where it comes from |
|---|---|---|
| Key | 512 bytes | Share of the chunk pool |
| Value | 64 KiB | An eighth of the pool, capped by the copy-under-lock ceiling |
| Hash field / set member | 512 bytes | Share of the chunk pool |
| Keys per type per database | `redis.mem_max_entries` | Fixed-size HTAB |
| Channel / pattern | 255 bytes | Fixed slot |
| Pub/sub payload | 512 bytes | Fixed ring slot |
| Bulk string on the wire | 512 MiB | RESP parser, matches Redis |

Redis caps a string at 512 MiB and places no limit at all on key or field
length. pg_redis honours the protocol limit at the parser, so **the durable half
matches Redis** — values there are bounded only by PostgreSQL's ~1 GB per-field
TOAST limit. The memory-mode limits are lower because shared memory is a
fixed-size region sized at postmaster start, so nothing in it can grow on
demand. That is a property of the backend, not of the protocol: switch the
database to `auto`, or use `SELECT durable`, and the same command is stored
exactly as Redis would store it.

---

## 8. Performance

Requests/second on Docker, Apple M-series, `redis-benchmark -n 50000 -c 200`
against the default database.

| Command | Redis 7 | memory | SPI/unlogged | SPI/logged |
|---------|--------:|-------:|-------------:|-----------:|
| PING    | 234,000 | 112,000 | 113,000 | 103,000 |
| GET     | 243,000 | 106,000 |  85,000 |  92,000 |
| SET     | 224,000 | 103,000 |  22,000 |  12,000 |
| INCR    | 253,000 | 116,000 |  32,000 |  14,000 |
| HSET    | 227,000 |  83,000 |  26,000 |  13,000 |
| ZADD    | 185,000 | 118,000 |  15,000 |   7,000 |
| SADD    | 194,000 |  82,000 |  71,000 |  66,000 |
| SPOP    | 183,000 | 116,000 |  51,000 |  64,000 |
| ZPOPMIN | 183,000 | 118,000 |  48,000 |  47,000 |

`redis-benchmark` writes 3-byte values, which never leave the 64-byte inline
slot — so the table above says nothing about the chunk pool.
`mise run bench-value-sizes` sweeps 3/63/64/65/200/512/4096/65536 bytes for
that. `mise run bench-high-write` (8 workers, `batch_size=256`) spreads
connections across more dispatchers.

### Pub/Sub

One publisher, varying subscriber counts. **recv/s** is total deliveries across
all subscribers.

| Subscribers | Redis 7 pub/s | Redis 7 recv/s | pg_redis pub/s | pg_redis recv/s |
|---|---|---|---|---|
| 1  | 2,509 | 2,506  | 9,451  | 9,172   |
| 4  | 2,359 | 9,433  | 12,297 | 38,086  |
| 16 | 3,865 | 61,762 | 49,810 | 348,682 |
| 32 | 2,896 | 92,516 | 8,993  | 237,891 |

recv/s scales linearly — one `PUBLISH` fans to N ring buffers in parallel.
Routing to a table roughly halves publisher throughput with no subscribers
(10,833 → 5,577) and costs progressively less as subscribers are added.

### Where the ceiling is

**Sharding the shared-memory LWLocks is not worth doing**, measured rather than
assumed:

| Measurement | Result |
|---|---|
| Throughput at 1 / 2 / 4 workers | 55k / 48k / 55k — no scaling at all |
| Worker wait events, 120 samples under `SET` load | 100% `RUNNING/cpu`, zero LWLock waits |
| `SET` (exclusive lock) vs `GET` (shared lock) | 52k vs 54k — indistinguishable |
| Pipelining `-P 1` → `-P 32` | 47.5k → 151k (3.2×) |

One worker matches four, and pipelining triples throughput without touching the
lock. The ceiling is the two mpsc thread hops per command. Memory-mode commands
need no SPI and no transaction, so a connection thread could in principle
execute them inline and skip both — the blocker is that PostgreSQL LWLocks
require a valid `MyProc`, which non-backend threads do not have. Closing that
needs a design, not a patch, and lock sharding only becomes interesting
afterwards.

### Benchmarking without fooling yourself

- **Point the benchmark at db 0 with `storage_mode=memory`.** The compose default
  is `redis.default_db=8`, the WAL-logged half — a benchmark that forgets this
  measures fsync and reports ~500 requests/second for everything.
  `bench-value-sizes` and `bench-report` set both; `bench` does not, by design.
- **`redis-benchmark` never clears `mylist`.** Its `lpush`, `rpush` and `lrange`
  tests build the list up over the whole run, and `-n 20000` is past what a list
  table holds at the default, so the run ends in the OOM the server is supposed
  to give and reports nothing at all. `docker/bench/report.sh` clears the key and
  uses 4,000 requests for those.
- **The noise band is ±6%, which is wider than most changes worth measuring.**
  Three rounds is not enough. One attempt at measuring the KV key change read as
  "SET and GET are 10% slower" while `PING`, which the change cannot touch, read
  as 8% *faster* — both noise. What works: alternate the two builds, nine rounds
  each at 50,000 requests, take medians, and include a command the change cannot
  reach as a calibration. At that sample size `PING` came back at +0.5% and
  everything else sat inside ±5% straddling zero.
- **`KEYS` needs its own measurement.** It is not a `redis-benchmark` test, and
  reading every key out of every entry is exactly what it does. Time it directly
  over a populated database.

CI runs `docker/bench/report.sh` on every pull request against memory mode and a
Redis 7 container on the same runner, and posts the result as one comment —
keyed on an HTML marker, so a push edits it in place and duplicates are deleted.
The same report goes to the job summary, which is all a fork's token can write.
It is a smoke signal, not a gate: what it catches is a collapse from tens of
thousands per second to hundreds. The ratio against the Redis beside it travels
better than the absolute numbers. `mise run bench-report` runs it locally.

---

## 9. Testing

Three layers covering different code. Running one is not running the others.

| Layer | Command | What it actually covers |
|---|---|---|
| Unit | `mise run test-unit` | Pure functions: parsing, key layout, limits, SipHash, the pool — no PostgreSQL |
| pgrx | `mise run test-pg` | **The SPI path only** — never the shared-memory backend |
| End-to-end | `mise run e2e` | Everything, in whichever storage mode the server runs |
| Parity | `mise run parity` | Every reply, diffed against a real Redis, on both halves |

**The gap in the middle is the important one.** `cargo pgrx test` starts its own
throwaway cluster *without* `shared_preload_libraries`, so memory mode — the
default storage mode, and all of `mem.rs` — is invisible to it. Four bugs
reached a branch that way. Run both storage modes for anything touching
`mem.rs`; CI does:

```bash
mise run e2e                              # storage_mode=auto
PG_REDIS_STORAGE_MODE=memory mise run e2e # the shared-memory backend
```

### Parity against a real Redis

The three layers above assert against expectations written by hand, so they can
only catch what someone already knew to expect. `docker/e2e/parity.ts` asks
Redis instead: the same commands to pg_redis and to `redis:7`, replies compared
byte for byte, on the shared-memory half and the SQL half in the same run.

```bash
mise run parity           # gate: fails on a difference not already recorded
mise run parity-update    # re-record after fixing or accepting one
```

66 cases, 1,394 replies a run, covering every command the parser accepts, the
corner cases the community trips on (empty collections vanishing, duplicate
arguments, source equal to destination, score and float formatting, what counts
as an integer, which operations keep a TTL), and the patterns people actually
deploy (rate limiter, session, `SET NX PX` lock, leaderboard, work queue, dedup,
cache-aside, tag intersection, hash-as-object, time index).

Three things it has to normalise, and nothing else:

- **Unordered replies.** `SMEMBERS`, `HGETALL`, `KEYS` and the set-algebra
  commands have no defined order in Redis, so comparing them verbatim measures
  two hash-table iteration orders. Sorted before comparison.
- **Draws and clocks.** `SPOP` without a count, `RANDOMKEY`, `TTL`, `INFO`,
  `SCAN` cursors. Only the reply *shape* is compared — the type byte, and an
  array's length.
- **A dropped connection.** A command that kills the server it was sent to never
  replies; the harness records `NO REPLY` and reconnects rather than waiting
  forever. That is how the `INCR` overflow below was found.

Differences are classified — `value`, `error-vs-value`, `error-code`,
`error-text` — and the known ones live in `docker/e2e/parity-baseline.json`.
**CI gates on new differences only.** A gate on zero differences would fail on
every run today, and one that failed always would be turned off.

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
redis.storage_mode = 'memory'      # or 'auto' — run both
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

Re-run step 2 after every code change — the running server holds the old `.so`
until it restarts.

### Against an assert-enabled PostgreSQL

`cargo pgrx init --pgN download` configures PostgreSQL with `--enable-cassert`,
which distribution packages do not. Code that misuses a PostgreSQL internal can
pass against system packages and fail only in CI, with an assertion crash that
also takes down the backend and cascades into unrelated tests. If a test fails
in CI and passes locally, suspect this first.

Not hypothetical: `UpdateActiveSnapshotCommandId` asserts the active snapshot
has `active_count == 1` and `regd_count == 0`, which the snapshot SPI has
already pushed satisfies neither. It passed locally and failed every CI leg.

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
intact, and give it its own `CARGO_TARGET_DIR` — the bindings differ, so sharing
one target directory means a full rebuild on every switch.

**Build it under the test user's home.** `$HOME/pgassert` as root plus tests as
the unprivileged user below means `pgrx install --test` cannot write to
`/root/pgassert`. That build's `pg_config --pkglibdir` also reports the *system*
lib dir, so its `.so` lands there rather than under the prefix — reinstall the
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

`cargo pgrx init --pgN download` fetches from `ftp.postgresql.org`, not always
reachable. Pointing pgrx at the PGDG packages
(`cargo pgrx init --pg15 /usr/lib/postgresql/15/bin/pg_config …`) gets the matrix
running; only the assert-enabled build has to be compiled.

### Traps

- **A `describe` that does not `SELECT cache` runs on db 8**, the durable half,
  whatever `storage_mode` says — so it says nothing about the shared-memory
  backend. The "Binary safety" describe sat there for its whole life and passed
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
  order. A run no longer needs a private server — but it does empty whatever
  database it is pointed at, on the reference too.
- **`SHOW redis.foo` names the result column `redis.foo`**, dot included.
  Destructuring by `foo` yields `undefined` with no error. Use
  `SELECT current_setting('redis.foo') AS mode`.

---

## 10. Continuous integration

`.github/workflows/ci.yml` is seven jobs. Two are trivial and gate the rest, so
a formatting mistake costs seconds rather than a full matrix:

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

The pgrx version comes from `Cargo.toml` — CI reads it from there rather than
keeping its own copy, because the auto-update workflow's token is not allowed to
rewrite a workflow file.

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

What is *left* between pg_redis and Redis — the 97 commands we do not implement,
the introspection that answers wrongly, and the two architectural blockers in
front of the rest — is measured in [Parity roadmap](PARITY.md). This section
records what is done and what is known-open in the surface we already have.

### The dispatch handoff

See [Where the ceiling is](#where-the-ceiling-is). Closing it needs a design,
not a patch.

### Redis parity

Done, and the thing everything else waited on. The audit has a machine behind
it — see [Parity against a real Redis](#parity-against-a-real-redis) — and what
it reports is recorded in `docker/e2e/parity-baseline.json`. Performance work
comes after this and is bounded by it: a change that buys throughput by
widening a parity or security gap is not a change worth making.

**2 differences over 1,394 replies**, down from 189. Every phase is done — 197
closed, and a sharper harness added 10 along the way. Both survivors are the
same accepted one, once per half: `INCRBYFLOAT` is `f64` where Redis uses
`long double` (see [phase 2](#phase-2-ranges-shapes-and-ties)).

The gate fails on a *new* difference, not on these two.

#### The plan

Six phases, ordered by what a client loses, not by effort. Every difference in
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

**Done.** Not parity gaps — things that could not work at all, every one on the
SQL half, and every one a worker kill before `in_subtransaction` learned to
catch. They went first because the harness reached them by accident and a client
would reach them on a Tuesday.

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
so `-1` and `100` looked alike and every expiry bug was invisible. It compares
the class now — gone, no expiry, or some expiry — which cost nothing and
immediately showed both, plus five more that belong to phase 4.

**Also here:** a caught SQL error is logged with its message and SQLSTATE
(`pg_redis: command failed: …`). Catching it stops PostgreSQL reporting it, so
without this the operator got nothing at all — which is how `HINCRBY` had been
failing unnoticed.

#### Phase 1: input Redis refuses

**Done.** All in the parser, which is what makes it one fix per behaviour rather
than two: both backends read the same `Command`.

- **Expiry validation.** `check_expire` refuses a non-positive expiry and one
  whose deadline could not be represented, so `SET k v EX 0`, `SETEX k 0 v` and
  `EXPIRE k 9999999999999999` are errors rather than a delete or a date in the
  far future. `check_expire_deadline` allows a past deadline, since `EXPIRE k -1`
  legitimately deletes the key.
- **Count validation.** `check_count` on `SPOP`, `LPOP`, `RPOP`, `ZPOPMIN` and
  `ZPOPMAX`. Not on `SRANDMEMBER` or `ZRANDMEMBER`, where a negative count means
  "with repeats".
- **Integer canonicality.** `mem::parse_stored_int` reads a counter the way
  Redis does — no `+1`, no `007`, no `-0` — and the SQL guard was widened to
  match, including a `numeric` range check so a stored value too large for
  `bigint` reads as "not an integer" rather than raising an overflow.
- **Float validation.** A score literal that overflows to infinity is out of
  range rather than infinite, so `ZADD z 1e400 m` is refused; only the spelled
  forms mean infinity. A `ZINCRBY` landing on NaN is refused without storing it,
  and so is an `INCRBYFLOAT` whose *result* is not finite — Redis draws that line
  at the result, not the argument, and takes an infinite increment happily.

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
- **Float precision — accepted, not fixed.** `INCRBYFLOAT` is `f64` where Redis
  uses `long double`, so `3 + 1.000000000000000005` is `4` here and
  `4.00000000000000001` there. 80-bit arithmetic is not available in stable Rust
  without a dependency, and `float8` is what the SQL half stores. The two
  differences stay in the baseline as a record.

#### Phase 3: error wording

**Done**, and the decision taken: Redis's strings verbatim. `wrong_arity`,
`NOT_AN_INTEGER`, `NOT_A_FLOAT` and `invalid_expire` name them once each, which
replaced about sixty hand-written variants — `GET missing argument at position 0`
became `wrong number of arguments for 'get' command`, and every `X requires
integer` became `value is not an integer or out of range`.

Not every message came from a table: `LSET` on a missing key says `no such key`
rather than `index out of range`, `HINCRBY` names the hash, `SELECT 99` is out
of range rather than unparseable, and `DECRBY key -9223372036854775808` gets
Redis's own `decrement would overflow`.

#### Phase 4: the key directory

**Done**, and the decision taken: one directory per database, not a probe of the
other tables per write. It closed two things that looked separate:

- **A key could hold every type at once (30).** `SET k v` then `LPUSH k a`,
  `SADD k m`, `HSET k f v` and `ZADD k 1 m` all succeeded; `TYPE` said `string`,
  and each accessor returned its own data. `SINTERSTORE` onto a key holding a
  string was the same bug from the other side: the set appeared and the string
  stayed.
- **Collections could not expire (4).** `EXPIRE` on a list, set, hash or sorted
  set returned 0 and set nothing, so every cache-with-a-TTL pattern over a
  collection kept its data forever.

Both were the same missing object: nothing knew what a key was. Each type had
its own table, keyed independently, and no one asked the others.

**The shared-memory half** gained a tenth HTAB — see [the key
directory](#the-key-directory) for its layout, its sizing and the lock order it
imposes. The hash table also gained the count meta the set table already had,
which is what tells the directory its last field has gone, and makes `HLEN` a
lookup rather than a scan of the whole database's hash table.

**The durable half** derives the type instead of storing it: the five tables are
already keyed by `key`, so one query answers "what holds this" for every key a
command names, in a single round trip before the command runs. Nothing to keep
in step, at the cost of that query. Only the expiry needed somewhere to live —
`redis.expiry_N`, one row per collection carrying a TTL — because a string's
`expires_at` column is the only one in the schema.

Three things fell out of building it that the parity harness had not asked for:

- `SET k v NX` over a list said OK on the durable half, because the `NX` test
  looked at `redis.kv_N` alone and a list is invisible there. So did `SETNX` and
  `MSETNX`. All three ask the whole directory now, and `XX` — whose condition a
  collection satisfies — replaces it rather than doing nothing.
- Redis discards a key's TTL whenever its value is replaced, `SUNIONSTORE` onto
  a live set included. Only the type change was clearing it.
- An expired collection has to be gone, not merely untyped: `TYPE`, `EXISTS`,
  `TTL` and `DBSIZE` on the durable half read `redis.expiry_N` and treat a
  passed deadline as absent, the way they already treat an expired string.

**What it costs the durable half.** The type query is a round trip the SQL
backend did not make before, so where it lands matters:

- A **write** is checked up front — it cannot be allowed to land and then be
  refused — so `LPUSH`, `SADD`, `HSET` and friends pay one lookup each.
- A **read** runs first and asks only when it came back empty: a key holding
  another type finds nothing, and so does a key that is not there. Every
  successful `GET`, `HGET` and `LRANGE` is unchanged.
- The commands that only **replace** a key — `SET`, `SETEX`, `PSETEX`, `MSET` —
  ask nothing at all. They clear the other tables from inside their own
  statement, as four extra index probes. Doing it as a lookup first halved
  `SET` throughput on the durable half in the first measurement, which is what
  prompted folding it in.

The benchmark numbers themselves are not worth quoting from this container: the
spread between identical rounds is two to three times, which rules out a
collapse and nothing finer.

Still open, and not part of the 40: `KEYS`, `SCAN` and `RANDOMKEY` see only the
KV table. The directory holds a key's hash, not its bytes, and no other table
stores a collection's name — so there is nothing to list them from. Fixing it
means either an inline key prefix in the directory entry (32 bytes becomes 80,
plus a sixth chunk pool) or a per-key table on each side. Neither is free, and
neither is what phase 4 was for.

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
| `SETRANGE`, `GETRANGE` | `SETRANGE` pads the gap with NUL bytes, which `chr(0)` cannot express in PostgreSQL text — the SQL half builds the padding as hex |
| `SETBIT`, `GETBIT`, `BITCOUNT` | Redis numbers bits from the most significant of byte 0; PostgreSQL's `get_bit` counts from the least, so both halves compute in Rust and agree by construction |
| `HINCRBYFLOAT`, `HRANDFIELD` | A field entered for an increment that turns out to be invalid is removed again, rather than left behind empty |
| `RPOPLPUSH` | Parses straight to `LMove { src_left: false, dst_left: true }` — one command, not two implementations |
| `RENAMENX`, `GETEX` | |
| `SINTERCARD` | `LIMIT 0` means no limit, which is Redis's reading and not the obvious one |
| `COPY` | See below |
| `OBJECT ENCODING` | See below |
| `FLUSHDB`, `FLUSHALL` | See below |

Three of them needed a decision rather than an implementation.

**`COPY` crosses the two halves.** `COPY k k DB 8` promotes a shared-memory
cache entry to a durable row, and `DB 0` brings one back. That is the one place
`COPY` does more here than in Redis, which has no such split. It reads through
`dump_key` into a form neither backend stores and writes through `restore_key`
into whichever the destination names — the only command that has to, which is
why the pair exists at all and why `COPY` is routed to the SPI path whatever
the storage mode.

**`OBJECT ENCODING` reports Redis's representation, not ours.** `int`,
`embstr`, `raw`, `listpack`, `quicklist`, `intset`, `hashtable`, `skiplist`,
derived from the value's shape against Redis's default thresholds. pg_redis has
an inline slot and a chunk pool, or a `bytea` column, and none of those names
describes it. Reporting our own would be truthful and would break every client
that branches on Redis's — the decision was to satisfy the client. A missing
key answers nil, which is Redis's own reply here rather than the error nearly
every other command gives.

**`FLUSHALL` empties all sixteen databases, the durable half included.** Redis's
own reach, taken deliberately over a safer default: databases 8–15 are
WAL-backed, and the Redis port bypasses PostgreSQL authentication, so any client
that can reach the port can erase them. `redis.listen_address` defaults to
loopback and `redis.password` exists; both matter more now than they did.
`FLUSHALL` is routed to the SPI path for the same reason `COPY` is — it is the
only other command that touches both halves.

Two things fell out that the harness had not asked for. `DBSIZE` on the
shared-memory half counted strings alone, reporting 1 for a database holding a
string, a list, a hash, a set and a sorted set; it reads the key directory now.
And a flush writes to keys it cannot name, so `WATCH` needs every counter
bumped rather than the ones `write_keys` returns — `watch::bump_all` aborts
watches on databases the flush did not touch, which is the direction that table
is allowed to err in.

### An expired collection outlives its own expiry

Key-level reads are exact the moment the deadline passes: `TTL`, `TYPE`,
`EXISTS` and `DBSIZE` go through the key directory on the shared-memory half and
`redis.expiry_N` on the durable one, and both read a passed expiry as absent.

The type-specific reads do not. `LLEN` reads the list's own meta, `SCARD` the
set's, `LRANGE` the entries — none consults the directory — so for up to a
second, until the sweep runs, `TYPE` answers `none` while `LLEN` still reports
the old count. Redis expires lazily on every access and has no such window.

The fix is not a lookup on each of those paths, which would put a directory
probe on the hot read path for a case that almost never happens. It is for the
type gate to *reap* rather than merely ignore: `mem_type_error` already looks
the key up before every typed command, so an expired one could be dropped there
and the command would then find nothing, which is what Redis does. That needs
`dir_lookup` to distinguish "absent" from "expired", which it currently does
not.

### `RANDOMKEY` favours strings

It returns the first live key the KV table's scan reaches, and only looks at the
collection metas when there is none. Redis draws from the whole keyspace. The
reply is a valid key either way, which is why the parity harness compares
`RANDOMKEY` by shape and not by value — but a caller sampling a database of
mostly collections will see the strings far more often than it should.

### `ZPOPMIN` is O(entries in the database)

Not the meta lookup, which is O(1) — the *refresh* after it. Removing the
minimum leaves the meta unable to name the new one without looking, so
`refresh_zset_meta` scans the whole zset table of that database once per element
popped. `ZREM` and every `ZREMRANGEBY*` pay it too. `redis-benchmark`'s
`zpopmin` runs against a nearly empty table and never shows it. A skip list or a
score-ordered index is the fix, and it is a data-structure change rather than a
patch.

### Settled, so not on this list

- **`redis.mem_max_entries` stays at 8192** — see
  [Sizing](#sizing-redismem_max_entries). Measured, not assumed.
- **Sharding the shared-memory LWLocks**, for the reason above.
- **The five fixed member arrays** — gone; members are length-prefixed like
  keys. See [How a collection entry stores its member](#how-a-collection-entry-stores-its-member).
- **Multi-key writes under OOM** — priced whole before they run. See
  [When a table fills](#when-a-table-fills).
- **A SQL error killing the worker** — `INCR` past `bigint`'s ceiling raised
  out of the dispatcher and ended the process; four of those ended the Redis
  endpoint. `in_subtransaction` catches now, and the reply matches Redis.
- **Where a collection's name is stored** — in the four meta tables, not the key
  directory. Priced both ways: 6 MB against 22 MB.
- **What `OBJECT ENCODING` reports** — Redis's representation names, not ours.
  See [phase 5](#phase-5-missing-commands).
- **How far `FLUSHALL` reaches** — all sixteen databases, as in Redis, the
  durable half included. Also [phase 5](#phase-5-missing-commands).
