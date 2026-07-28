# Supported commands

## Connection

| Command | Behaviour |
|---------|-----------|
| `PING [msg]` | Returns `PONG` or echoes `msg` |
| `ECHO msg` | Returns `msg` |
| `SELECT db` | 0–15, or the names `cache` (0) and `durable` (8). Databases 0–7 are ephemeral, 8–15 WAL-logged — see [Storage modes](IMPLEMENTATION.md) |
| `AUTH [password]` | Validates against `redis.password` GUC; no-op when unset |
| `INFO` | Returns server info string |
| `COMMAND` | Returns empty array (client compatibility); `COMMAND COUNT` reports the number of accepted commands |
| `CLIENT ...` | No-op, returns OK |
| `TIME` | Unix seconds and leftover microseconds, from one clock reading |

## Key–value

| Command | Behaviour |
|---------|-----------|
| `GET key` | Returns value or nil if missing/expired |
| `SET key value [NX\|XX] [GET] [EX sec\|PX ms\|EXAT ts\|PXAT ts-ms\|KEEPTTL]` | Upsert with conditional flags and optional TTL |
| `SETEX key seconds value` | SET with seconds TTL |
| `PSETEX key ms value` | SET with milliseconds TTL |
| `MGET key [key ...]` | Bulk get, preserves nil for missing keys |
| `MSET key value [key value ...]` | Bulk upsert |
| `DEL key [key ...]` | Delete keys, returns count deleted |
| `EXISTS key [key ...]` | Returns count of existing keys |
| `TOUCH key [key ...]` | Counts as `EXISTS` does; there is no LRU clock to refresh |
| `SETRANGE key offset value` | Overwrite from `offset`, padding the gap with NUL bytes; returns the new length |
| `GETRANGE key start end` | Substring; negative indices count from the end, and both clamp |
| `SETBIT key offset 0\|1` | Set a bit, growing the value to reach it; returns the bit as it was |
| `GETBIT key offset` | The bit, or `0` past the end |
| `BITCOUNT key [start end [BYTE\|BIT]]` | Bits set, over the whole value or a range |
| `RENAME key newkey` | Rename, replacing `newkey`; errors if `key` is missing |
| `RENAMENX key newkey` | The same, but `0` rather than replacing an occupied `newkey` |
| `COPY src dst [DB n] [REPLACE]` | Duplicate a key of any type — see below |
| `DUMP key` / `RESTORE key ttl payload [REPLACE\|ABSTTL]` | Redis's serialisation format, interchangeable with a real 7.0 — see below |
| `OBJECT ENCODING key` | Redis's name for the representation — see below |
| `FLUSHDB [ASYNC\|SYNC]` | Empty the current database |
| `FLUSHALL [ASYNC\|SYNC]` | Empty **all sixteen**, the durable half included — see below |

Bits are numbered from the most significant of byte 0, as in Redis. `ASYNC` and
`SYNC` are accepted and ignored: nothing here defers the work, so both spellings
describe what already happens.

> **`FLUSHALL` destroys the durable half.** Databases 8–15 are WAL-backed and
> survive a restart, and the Redis port bypasses PostgreSQL authentication — so
> any client that can reach it can erase them, with no confirmation and nothing
> to roll back to but your own backups. This matches Redis, deliberately. If
> that reach is wider than you want, bind `redis.listen_address` to loopback
> (the default) and set `redis.password`.

### `COPY` across the two halves

`COPY src dst DB n` may name a database on the other storage half, which is how
a shared-memory cache entry becomes a durable row, or the reverse:

```
SELECT cache
SET session:42 '{"user":7}'
COPY session:42 session:42 DB 8    -- now WAL-backed
```

It copies every type, not just strings. Redis has no such split, so this is the
one place `COPY` does more here than there. The read and the write land in one
transaction, but they are two different storage engines: a crash between them
cannot leave the destination half-written, though the value must also pass every
limit of the half it is going to.

### `DUMP` and `RESTORE`

`DUMP` emits Redis's own payload format — an RDB-serialised value, a two-byte
version stamped 10, and a CRC-64 — so what it produces restores into a real
Redis 7.0 and vice versa. The bytes are not identical to what Redis itself
would emit for the same value, because valid encodings are not unique;
`RESTORE` accepts everything a 7.0 produces, listpacks and compressed strings
included. Two deliberate refusals, both loud: payloads stamped newer than
version 10 (what a 7.2 would hand you — Redis 7.0 refuses them too), and the
compact encodings no server since 6.x has written (ziplists, zipmaps).
`REPLACE` and `ABSTTL` behave as in Redis; `IDLETIME` and `FREQ` are accepted
and unused, there being no LRU or LFU clock to hand them to.

### `OBJECT ENCODING`

Reports the encoding **Redis** would use — `int`, `embstr`, `raw`, `listpack`,
`quicklist`, `intset`, `hashtable`, `skiplist` — derived from the value's shape
against Redis's default thresholds. pg_redis stores none of those
representations; it has an inline slot and a chunk pool, or a `bytea` column.
The name is reported for the benefit of clients that branch on it, and says
nothing about how this server stores anything. A missing key answers nil, which
is Redis's own reply here and not the error most commands give.

## Expiry

| Command | Behaviour |
|---------|-----------|
| `EXPIRE key seconds` | Set TTL in seconds |
| `PEXPIRE key ms` | Set TTL in milliseconds |
| `EXPIREAT key unix-ts` | Set absolute expiry (unix seconds) |
| `PEXPIREAT key unix-ts-ms` | Set absolute expiry (unix milliseconds) |
| `TTL key` | Remaining TTL in seconds; `-1` no expiry; `-2` missing |
| `PTTL key` | Remaining TTL in milliseconds |
| `PERSIST key` | Remove TTL |
| `EXPIRETIME key` | Absolute expiry as unix timestamp (seconds) |
| `PEXPIRETIME key` | Absolute expiry as unix timestamp (milliseconds) |
| `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ts\|PERSIST]` | Read the value and, optionally, change what happens to it next |

Every type carries an expiry, not just strings: a list, set, hash or sorted set
takes a TTL and is deleted whole when it passes. Replacing a key's value clears
its TTL, as in Redis — `SET` over a string, and `SUNIONSTORE` over a set alike.

## Hashes

| Command | Behaviour |
|---------|-----------|
| `HGET key field` | Returns field value or nil |
| `HSET key field value [field value ...]` | Upsert one or more fields, returns new field count |
| `HDEL key field [field ...]` | Delete fields, returns count deleted |
| `HGETALL key` | Returns interleaved field/value pairs, sorted by field |
| `HSTRLEN key field` | Byte length of one field's value, `0` when either is absent |
| `HINCRBYFLOAT key field increment` | Increment a field as a float; returns the new value |
| `HRANDFIELD key [count [WITHVALUES]]` | One field, `count` distinct ones, or `-count` with repeats |

> `EXPIRE` applies to the hash as a whole, as it does in Redis. Per-field TTLs
> (`HEXPIRE` and friends) are not implemented.

## Lists

| Command | Behaviour |
|---------|-----------|
| `LPUSH`/`RPUSH key value [value ...]` | Prepend/append, returns the new length |
| `LPOP`/`RPOP key [count]` | Pop from either end |
| `LRANGE key start stop` | Inclusive range, negative indices count from the tail |
| `LINSERT key BEFORE\|AFTER pivot value` | Returns the new length, `-1` if the pivot is absent, `0` if the key is |
| `LREM key count value` | `count > 0` from the head, `< 0` from the tail, `0` removes every match |
| `LPOS key element [RANK r] [COUNT n]` | Negative `RANK` searches from the tail; `COUNT 0` returns every match |
| `LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` | Pop from one end of `src` and push onto one end of `dst` |
| `RPOPLPUSH src dst` | `LMOVE src dst RIGHT LEFT`, under its older name |

`LINSERT` and `LREM` rewrite the list so its positions stay contiguous, which
makes them O(n). Removing in place would leave a gap, and every reader walks
positions in order — the list would silently end at the first gap.

## Sets

| Command | Behaviour |
|---------|-----------|
| `SADD`/`SREM key member [member ...]` | Add or remove, returns the count that changed |
| `SMEMBERS key` | Every member, in no particular order |
| `SCARD key` | Cardinality |
| `SISMEMBER`/`SMISMEMBER key member [member ...]` | Membership, as `1`/`0` |
| `SPOP`/`SRANDMEMBER key [count]` | `SPOP` removes what it returns; `SRANDMEMBER` does not |
| `SUNION`/`SINTER`/`SDIFF key [key ...]` | The combined set |
| `SUNIONSTORE`/`SINTERSTORE`/`SDIFFSTORE dst key [key ...]` | The same, stored, returning its cardinality |
| `SINTERCARD numkeys key [key ...] [LIMIT n]` | The intersection's size without building it; `LIMIT 0` means no limit |
| `SMOVE src dst member` | Move one member between sets |

## Errors and limits

Errors carry a Redis error code, so client-side handling that matches on one
works unchanged:

| Reply | Meaning |
|---|---|
| `-ERR ...` | The general case |
| `-WRONGTYPE Operation against a key holding the wrong kind of value` | The key holds another type. A key holds exactly one — see [the key directory](IMPLEMENTATION.md#the-key-directory) |
| `-OOM command not allowed when used memory > 'maxmemory'` | A shared-memory table is full and `redis.maxmemory_policy` is `noeviction`. See [When a table fills](IMPLEMENTATION.md#when-a-table-fills) |
| `-ERR key exceeds redis.storage_mode='memory' limit of 512 bytes ...` | The key, hash field, set member or value is larger than memory mode accepts |
| `-ERR key or member hash collides with a different one already stored ...` | Two keys, or two members of one collection, shared a 128-bit keyed hash. Refused rather than merged — see [how keys are stored](IMPLEMENTATION.md#lookups-verify-the-key) |

`WRONGTYPE` applies to both halves. The rest are specific to
`storage_mode = 'memory'`; the durable half is
bounded only by disk. Size limits are checked before the command runs, so a
refusal never leaves a partial write — no half-stored value and no entry under
a truncated key. A multi-key write is priced the same way: `MSET`, `SADD`,
`HSET`, `ZADD` and the pushes are refused whole rather than storing the keys
that fit.

## Transactions

| Command | Behaviour |
|---------|-----------|
| `MULTI` | Begin a transaction block; subsequent commands are queued |
| `EXEC` | Execute all queued commands atomically; returns nil if a `WATCH`ed key changed |
| `DISCARD` | Discard the queued commands and exit the transaction block |
| `WATCH key [key ...]` | Mark keys to watch; if any are modified before `EXEC`, the transaction aborts |
| `UNWATCH` | Clear all watched keys |

Commands queued inside `MULTI` receive `QUEUED` responses. Runtime errors inside `EXEC` (e.g. `INCR` on a non-integer) are returned as per-command errors without aborting the remaining commands: each command runs in its own subtransaction, so a failure rolls back that command only.

`WATCH` tracks writes through counters in shared memory, so it detects a conflicting write no matter which worker served it. Two consequences:

- It requires the extension in `shared_preload_libraries`; otherwise `WATCH` returns an error rather than silently failing to detect anything.
- Keys are hashed into a fixed table of counters, so unrelated keys can occasionally share one. That can abort an `EXEC` that did not strictly conflict; it can never let a conflicting one through. Retry on a nil `EXEC`, as with Redis.
