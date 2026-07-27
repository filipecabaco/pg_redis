# Storage modes

## Overview

Data is stored across 16 databases (0–15), mirroring Redis's native database
model. They are split into two contiguous halves:

| DB numbers | Tables | Durability |
|------------|--------|------------|
| **0–7** — *ephemeral* | `redis.kv_0` … `redis.kv_7` | Configurable — see below |
| **8–15** — *durable* | `redis.kv_8` … `redis.kv_15` | WAL-backed, survives crashes |

`SELECT` accepts `cache` for 0 and `durable` for 8, so neither half needs to be
memorised as a number. `redis.default_db` (default `0`, as in Redis) chooses
where new connections start.

The ephemeral half has two storage backends controlled by `redis.storage_mode`.

## memory (default)

Databases 0–7 bypass PostgreSQL's SPI layer entirely. Commands are served from shared-memory hash tables (`ShmemInitHash`) with no transaction overhead, no WAL, and no buffer pool involvement. This is the default because it gives the closest performance profile to Redis.

`redis.storage_mode` is a `postmaster`-scope GUC — a PostgreSQL restart is required to change it.

## auto

Databases 0–7 use UNLOGGED PostgreSQL tables — the same SPI/transaction path as the durable half but without WAL writes. Use this when you need ephemeral data to be SQL-visible, or when values exceed the memory-mode size limits.

```ini
# postgresql.conf
redis.storage_mode = 'auto'
redis.default_db   = 0        # keep db 0 as the default
```

## Binary safety

Redis keys, values, hash fields and set members are arbitrary byte strings, and
both storage modes preserve them exactly — including NUL bytes and sequences
that are not valid UTF-8. The PostgreSQL tables use `BYTEA` columns rather than
`TEXT`, which cannot hold a NUL at all.

Two consequences when querying the tables directly from SQL:

```sql
-- Keys and values come back as bytea, so cast when you want to read them.
SELECT convert_from(key, 'UTF8'), convert_from(value, 'UTF8') FROM redis.kv_8;

-- ...and cast the other way when comparing against a literal.
SELECT value FROM redis.kv_8 WHERE key = convert_to('mykey', 'UTF8');
-- A plain literal also works, since it is coerced to bytea:
SELECT value FROM redis.kv_8 WHERE key = 'mykey';
```

Ordering is bytewise, which is what Redis means by lexicographic — so
`ZRANGEBYLEX` and `ORDER BY member` agree with Redis rather than with a
database collation.

## Comparison

| | `memory` (default) | `auto` |
|---|---|---|
| Storage for db 0–7 | Shared-memory HTAB per data type | UNLOGGED PostgreSQL tables |
| Transaction | None | `BEGIN` / `COMMIT` per batch |
| SPI overhead | None | Yes |
| Survives crash | No (shared memory, lost) | No (UNLOGGED, truncated) |
| SQL-visible | Returns nothing | `SELECT * FROM redis.kv_0` returns data |
| Max keys | `redis.mem_max_entries` per KV db (default 8,192) | Unlimited (disk) |
| Shared memory at the default | ~91 MiB, reserved at server start | None |
| Max value size | 64 KiB — see [values](#how-values-are-stored) | Unlimited (TOAST) |
| Max key size | 511 bytes | Unlimited |
| Max hash field / set member | 64 KiB — see [members](#how-members-are-stored) | Unlimited |

### Where this differs from Redis

Redis caps a string at 512 MiB (`proto-max-bulk-len`) and places no limit at all
on key or field length. pg_redis honours the 512 MiB protocol limit at the
parser, so **the durable half (8–15) matches Redis**: values there are bounded
only by PostgreSQL's ~1 GB per-field TOAST limit.

The memory-mode limits above are far lower because shared memory is a
fixed-size region allocated at postmaster start — the whole region has to be
sized up front, so nothing in it can grow to 512 MiB on demand. This is a
property of the backend, not of the protocol.

Anything over a limit is **rejected, never truncated**. A truncated key is worse
than an error: two keys sharing a 511-byte prefix would collapse onto one entry,
so a `GET` returns another key's value and a `DEL` removes it. The reply names
both the limit and the way past it:

```
ERR key exceeds redis.storage_mode='memory' limit of 511 bytes (use SELECT durable for unbounded values)
ERR value exceeds redis.storage_mode='memory' limit of 65536 bytes (use SELECT durable for unbounded values)
ERR hash field or set member exceeds redis.storage_mode='memory' limit of 65536 bytes (use SELECT durable for unbounded values)
```

The check runs before the command executes, so a refusal never leaves a partial
write behind — no half-stored value, no entry under a truncated key. Switch the
database to `auto`, or use `SELECT durable`, and the same command is stored
exactly as Redis would store it.

## When a table fills

`redis.mem_max_entries` (default 8192) bounds each data type in each ephemeral
database. What happens at the boundary is `redis.maxmemory_policy`, named after
Redis's setting of the same name:

| Policy | Behaviour |
|---|---|
| `noeviction` (default, as in Redis) | The write is refused with `OOM command not allowed when used memory > 'maxmemory'`. Nothing already stored is lost. |
| `allkeys-random` | Entries are evicted to make room. Hashes, sets, sorted sets and lists are evicted **whole** — a collection is never left half-present, and the key currently being written is never chosen, so a single key larger than its table still reports OOM. |
| `volatile-ttl` | Only keys that carry an expiry are evicted, soonest expiry first. Falls back to refusing the write if none do. |

Expired keys are reclaimed before any live key is considered, under every
policy. A background sweep also removes them once a second.

A full table has never been able to take the server down: `HASH_ENTER_NULL`
returns a null the command turns into the error above, rather than the
`out of shared memory` that dynahash raises for `HASH_ENTER`.

A table's **chunk pool** can run out before its entries do — see below. That is
reported as the same `OOM`, and eviction is asked for room the same way, so a
pool emptied by long values recovers under `allkeys-random` exactly as a full
table does.

The durable half has no such limit — it is bounded by disk.

## How keys are stored in memory mode

A hash, a set, a sorted set or a list holds one shared-memory entry per member,
and each entry has to identify the key it belongs to. Storing the key itself
cost 512 bytes in every one of them — 512 of a set member's 640 bytes, repeated
for every member of every set.

They instead store a 128-bit SipHash of the key, keyed with random material
drawn once per server start. Nothing needs the key bytes back: every access to
these tables compares against a key the caller already holds, and the commands
that hand a key to a client (`KEYS`, `SCAN`, `RANDOMKEY`) read the string table,
which still stores keys verbatim. The hash is keyed rather than plain so that a
client cannot construct two keys that collide and merge their contents.

At the default `redis.mem_max_entries` this takes the extension's shared-memory
reservation from **376 MiB to 202 MiB** — measured as a drop in
`shared_memory_size` from 587 MB to 413 MB.

## How members are stored

The member half of those composite keys was the same problem one level down. A
`SetEntry` was a 16-byte key hash and a fixed `[u8; 128]` member — which both
capped a hash field or a set member at 128 bytes, where Redis caps neither, and
reserved 128 bytes in every entry of the three largest tables whether the member
needed them or not.

Members are now hashed into the key the same way keys are, so a composite key is
**32 bytes rather than 144**, and the bytes themselves live in the entry: the
first 48 inline, the rest chained through the same chunk pool a value spills
into. A member's length stops being a property of the table's key layout and
becomes a property of the pool, exactly as a value's did.

More has to come back out than for a Redis key, which is why the bytes are kept
at all. `KEYS` and `SCAN` read key bytes from the string table, but `HGETALL`,
`HKEYS`, `SMEMBERS`, `SPOP`, `ZRANGE` and `ZPOPMIN` all hand a *member* back to
the client and there is no second table holding those. Storing them per entry
reuses the existing spill machinery and needs no reference counting; a shared,
deduplicated member table would halve the cost of a set stored twice, but it
would put a refcount on every eviction, `DEL`, `SPOP`, `ZREM` and
`ZREMRANGEBY*` path — and an entry that is dropped without its member being
released is invisible until the pool runs dry. Per-entry bytes were the cheaper
correctness bargain.

`ZsetMeta` pays for itself twice over here. It cached the extreme members as two
more 128-byte arrays, to keep `ZPOPMIN`/`ZPOPMAX` O(1); it now caches their
16-byte hashes instead. That is still O(1) and still one lookup — the hash *is*
half of the composite key — and the entry it lands on is where the member bytes
come from.

| At the default `redis.mem_max_entries` | Before | After |
|---|---|---|
| Composite key | 144 bytes | 32 bytes |
| `HashEntry` / `SetEntry` / `ZsetEntry` | 216 / 144 / 152 bytes | 160 / 88 / 96 bytes |
| `ZsetMeta` | 304 bytes | 72 bytes |
| Chunk pools | 3 per database, 12.8 MiB | 5 per database, 21.3 MiB |
| Extension's reservation | ~98 MiB | ~91 MiB |
| `shared_memory_size` | 308 MB | **301 MB** |

The pools grew because sets and sorted sets had no pool at all before — their
entries held nothing variable-length. Each table keeps its own so that every
pool operation runs under that table's LWLock and none needs a lock of its own.

The net 7 MB is those three numbers against each other. The three entry types
shed 56 bytes each and `ZsetMeta` shed 232, which is 6.6 MiB and 9.1 MiB across
the eight databases; the two new pools cost 8.5 MiB back. Shrinking the entries
does not save the full 128 bytes a member array held, because 16 of them come
back as the member's hash and 8 more as its length and chain head — the
remaining 48 are the inline slot, and each 8 bytes of that is 320 KiB.

### The member size limit

A member is bounded by the same expression a value is, against its own inline
slot:

```
max member = min(64 KiB, 48 + (redis.mem_max_entries / 8) * 64 bytes)
```

which is **65,536 bytes** at the default — the same number as a value, for the
same two reasons ([an eighth of the pool, and the ceiling on what one read may
copy under an LWLock](#the-value-size-limit)). There is one limit to remember
rather than two.

A hash's members and its values share one pool, so a hash whose fields are all
long has less room for long values, and the other way round. Both are refused
with `OOM` when the pool is empty, and both are recovered by `allkeys-random`
exactly as a full table is.

One thing the old layout got wrong went with it. A member was stored NUL-padded
inside the key and read back at the first NUL, so `a\0b` and `a` were stored as
two distinct entries that both read back as `a`. Members are length-prefixed
now, and arbitrary bytes round-trip.

## How values are stored

An entry carries the first **64 bytes** of its value inline. That covers most of
what a cache holds — `redis-benchmark` writes 3-byte values — and costs nothing
beyond the entry itself.

Anything longer spills into a **chunk pool**: one shared slab per table per
ephemeral database, holding `redis.mem_max_entries` chunks of 64 bytes each on a
free list, chained by index. A value of *n* bytes occupies `ceil((n - 64) / 64)`
chunks; the entry stores the index of the first, and each chunk the index of the
next. Freeing a value returns its chain to the free list in one pointer move.

The pool replaced a fixed overflow row per entry — 448 bytes reserved for every
key the table could ever hold, against the possibility that every value might
exceed the inline slot. Almost none do:

| At the default `redis.mem_max_entries` | Before | After |
|---|---|---|
| Overflow storage | 155 MiB of fixed rows | 12.8 MiB of pooled chunks |
| Extension's reservation | ~202 MiB | ~98 MiB |
| `shared_memory_size` | 413 MB | 308 MB |

(Members later moved into the pools too, taking the reservation to ~91 MiB and
`shared_memory_size` to 308 MB → **301 MB** — see
[members](#how-members-are-stored).)

### The value size limit

The 512-byte cap was the size of that fixed row. It is now a function of the
pool:

```
max value = min(64 KiB, 64 + (redis.mem_max_entries / 8) * 64 bytes)
```

which is **65,536 bytes** at the default, where the two bounds meet:

- **An eighth of the pool.** A pool is shared by every value in its table, so
  one value must not be able to take all of it. At the default that eighth is
  1,024 chunks — 64 KiB.
- **64 KiB outright.** Every read copies the value out of shared memory while
  the table's LWLock is held. That ceiling holds however large the pool is
  configured, so raising `redis.mem_max_entries` buys capacity, not longer
  values.

The limit is still a limit, and for the same reason as before: shared memory is
fixed at server start, so a value cannot grow into it on demand. What changed is
that a database's capacity for long values is now pooled rather than reserved
per key — 8,192 chunks per table per database at the default, which is 512 KiB
of spill, enough for every key to carry 128 bytes or for a handful to carry
64 KiB. A write that finds the pool empty is refused with `OOM`, exactly as a
full table is; nothing is truncated and nothing already stored is lost.

In the hash, set and zset tables that same pool also holds every member past its
48-byte inline slot, so the spill is shared between the two.

## Logged tables (databases 8–15)

The durable half always uses WAL-logged tables regardless of `storage_mode`.

| | Durable (8–15) | memory (0–7, default) |
|---|---|---|
| WAL writes | Yes | No |
| Survives crash | Yes | No |
| Replication | Yes | No |
| SQL-visible | Yes | No |
| Write speed | Standard | ~Redis-level |

Use the durable half (`SELECT durable`, or any db from 8 to 15) for data that must survive a PostgreSQL restart or needs to be queryable via SQL.

## When to use each mode

**Default (`memory`)** — ephemeral caches, session tokens, rate-limit counters. Closest to Redis behaviour.

**`auto`** — ephemeral data you want queryable via SQL joins, or keys/values exceeding the inline limits.

**Databases 8–15** — data that must survive crashes and participate in replication.

## Notes on memory mode

- Data is **lost on PostgreSQL restart**, crash, or `DROP EXTENSION`.
- `SELECT * FROM redis.kv_0` returns nothing — use `redis-cli` to inspect in-memory data.
- `pg_dump` does not capture in-memory databases.
- All in-memory operations are protected by per-database LWLocks. Multiple workers can safely operate on the same in-memory database concurrently.
