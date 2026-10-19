# Supported commands

## Connection

| Command | Behaviour |
|---------|-----------|
| `PING [msg]` | Returns `PONG` or echoes `msg` |
| `ECHO msg` | Returns `msg` |
| `SELECT db` | 0–15, or the names `cache` (0) and `durable` (8). By default databases 0–7 are `memory` and 8–15 `logged`, and any database's policy can be changed. See [Storage policies](STORAGE.md) |
| `AUTH [password]` | Validates against `redis.password` GUC; no-op when unset |
| `INFO [section ...]` | Every section, plus `default`, `all` and `everything`; `commandstats`, `errorstats` and `latencystats` are counted across workers |
| `COMMAND [subcommand]` | With no subcommand, Redis 7.2's command-table entry for every accepted command. `INFO`, `DOCS`, `LIST`, `GETKEYS` and `GETKEYSANDFLAGS` answer from the same table; `COUNT` reports the number of accepted commands |
| `CLIENT subcommand ...` | `ID`, `INFO`, `LIST`, `KILL`, `SETNAME`, `REPLY`, `PAUSE`, `UNBLOCK`, `TRACKING` and the rest act as in Redis; see [command coverage](command-coverage.md#connection) for the full list |
| `TIME` | Unix seconds and leftover microseconds, from one clock reading |

## Key–value

| Command | Behaviour |
|---------|-----------|
| `GET key` | Returns value or nil if missing/expired |
| <code>SET key value [NX&#124;XX] [GET] [EX sec&#124;PX ms&#124;EXAT ts&#124;PXAT ts-ms&#124;KEEPTTL]</code> | Upsert with conditional flags and optional TTL |
| `SETEX key seconds value` | SET with seconds TTL |
| `PSETEX key ms value` | SET with milliseconds TTL |
| `MGET key [key ...]` | Bulk get, preserves nil for missing keys |
| `MSET key value [key value ...]` | Bulk upsert |
| `DEL key [key ...]` | Delete keys, returns count deleted |
| `EXISTS key [key ...]` | Returns count of existing keys |
| `TOUCH key [key ...]` | Counts as `EXISTS` does; there is no LRU clock to refresh |
| `SETRANGE key offset value` | Overwrite from `offset`, padding the gap with NUL bytes; returns the new length |
| `GETRANGE key start end` | Substring; negative indices count from the end, and both clamp |
| <code>SETBIT key offset 0&#124;1</code> | Set a bit, growing the value to reach it; returns the bit as it was |
| `GETBIT key offset` | The bit, or `0` past the end |
| <code>BITCOUNT key [start end [BYTE&#124;BIT]]</code> | Bits set, over the whole value or a range |
| `RENAME key newkey` | Rename, replacing `newkey`; errors if `key` is missing |
| `RENAMENX key newkey` | The same, but returns `0` and leaves an occupied `newkey` in place |
| `COPY src dst [DB n] [REPLACE]` | Duplicate a key of any type; see below |
| `DUMP key` / <code>RESTORE key ttl payload [REPLACE&#124;ABSTTL]</code> | Redis's serialisation format, interchangeable with a real 7.2; see below |
| `OBJECT ENCODING key` | Redis's name for the representation; every other subcommand answers nil. See below |
| <code>FLUSHDB [ASYNC&#124;SYNC]</code> | Empty the current database |
| <code>FLUSHALL [ASYNC&#124;SYNC]</code> | Empty **all sixteen**, the durable half included; see below |

Bits are numbered from the most significant of byte 0, as in Redis. `ASYNC` and
`SYNC` are accepted and ignored. The flush always runs immediately, so neither
flag changes anything.

> **`FLUSHALL` destroys the durable half.** Databases with the `logged` policy
> (8–15 by default) are WAL-backed and survive a restart. The Redis port bypasses PostgreSQL authentication, so any
> client that can reach it can erase them with no confirmation. Only your own
> backups can bring them back. This matches Redis and is deliberate. If the
> port is reachable more widely than you want, bind `redis.listen_address` to
> loopback (the default) and set `redis.password`.

### `COPY` across the two halves

`COPY src dst DB n` may name a database on the other storage half. That turns a
shared-memory cache entry into a durable row, or the reverse:

```
SELECT cache
SET session:42 '{"user":7}'
COPY session:42 session:42 DB 8    -- now WAL-backed
```

It copies keys of every type. Redis has no such split, so copying across halves
is the one thing `COPY` does here that it cannot do in Redis. The read and the
write run in one transaction, across two different storage engines. A crash
between them cannot leave the destination half-written. The value must still
pass every limit of the destination half.

### `DUMP` and `RESTORE`

`DUMP` emits Redis's own payload format: an RDB-serialised value, a two-byte
version stamped 11, and a CRC-64. Its output restores into a real Redis 7.2
(the version this server reports), and a 7.2 payload restores here. This
includes streams, which are written in Redis's layout of node listpacks,
consumer groups, pending entries and consumer times. A Redis stream payload of
any format since 5.0 restores here. The bytes are not always identical to what
Redis itself would emit, because valid encodings are not unique (a set's
member order, for example). `RESTORE` accepts everything a 7.2 produces,
listpacks and compressed strings included. It deliberately refuses two kinds of
payload, with an error: payloads stamped newer than version 11, which is what a
7.4 produces and Redis 7.2 also refuses, and the compact encodings (ziplists,
zipmaps) that no server since 6.x has written.
`REPLACE`, `ABSTTL`, `IDLETIME` and `FREQ` behave as in Redis: `IDLETIME`
sets the key's access clock under an LRU-kept policy, `FREQ` its counter under
an LFU one, and the two exclude each other.

### `OBJECT ENCODING`

Reports the encoding **Redis** would use (`int`, `embstr`, `raw`, `listpack`,
`quicklist`, `intset`, `hashtable` or `skiplist`), derived from the value's
shape and Redis's default thresholds. pg_redis stores none of those
representations; it uses its own collections, or a `bytea` column.
The name is there for clients that branch on it, and says nothing about how
this server stores the value. A missing key answers nil. That is Redis's own
reply for this command, though most commands give an error instead.

`OBJECT IDLETIME` and `OBJECT FREQ` read the key's access field, kept as
Redis 7.2 keeps it ([The access clock](IMPLEMENTATION.md#the-access-clock)),
and give Redis's errors under the policy that does not keep what they ask
for. `OBJECT REFCOUNT` answers `2147483647` for a string holding an integer
from 0 to 9999, which Redis shares, unless an LRU or LFU policy is set with a
limit, and 1 for anything else. A missing key answers nil.

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
| <code>GETEX key [EX s&#124;PX ms&#124;EXAT ts&#124;PXAT ts&#124;PERSIST]</code> | Read the value and, optionally, change what happens to it next |

Every type carries an expiry. A list, set, hash or sorted set takes a TTL and
is deleted whole when it passes. Replacing a key's value clears its TTL, as in
Redis. That holds for `SET` over a string and for `SUNIONSTORE` over a set.

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
| <code>LINSERT key BEFORE&#124;AFTER pivot value</code> | Returns the new length, `-1` if the pivot is absent, `0` if the key is |
| `LREM key count value` | `count > 0` from the head, `< 0` from the tail, `0` removes every match |
| `LPOS key element [RANK r] [COUNT n] [MAXLEN len]` | Negative `RANK` searches from the tail; `COUNT 0` returns every match |
| <code>LMOVE src dst LEFT&#124;RIGHT LEFT&#124;RIGHT</code> | Pop from one end of `src` and push onto one end of `dst` |
| `RPOPLPUSH src dst` | `LMOVE src dst RIGHT LEFT`, under its older name |

`LINSERT` and `LREM` rewrite the list so its positions stay contiguous, which
makes them O(n). Removing in place would leave a gap. Every reader walks
positions in order, so the list would end at the first gap with no error.

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
| `-WRONGTYPE Operation against a key holding the wrong kind of value` | The key holds another type. A key holds exactly one type; see [one database, one dictionary](IMPLEMENTATION.md#one-database-one-dictionary) |
| `-OOM command not allowed when used memory > 'maxmemory'` | Memory mode is past `redis.maxmemory` and `redis.maxmemory_policy` could not evict enough. See [the limit and eviction](IMPLEMENTATION.md#the-limit-and-eviction) |

`WRONGTYPE` applies to every policy; `OOM` is specific to databases in memory,
and a `logged` database is bounded only by disk. The
limit is checked before the command runs, so a refused write changes nothing.

## Transactions

| Command | Behaviour |
|---------|-----------|
| `MULTI` | Begin a transaction block; subsequent commands are queued |
| `EXEC` | Run the queued commands with no other command from the same worker between them; returns nil if a `WATCH`ed key changed |
| `DISCARD` | Discard the queued commands and exit the transaction block |
| `WATCH key [key ...]` | Mark keys to watch; if any are modified before `EXEC`, the transaction aborts |
| `UNWATCH` | Clear all watched keys |

Atomicity is per worker. The server runs a pool of workers (`redis.workers`),
and each worker's dispatcher runs the queued commands as one batch, so no
other command that worker serves runs in between. Commands served by other
workers can interleave with them, where in Redis nothing can. `EVAL` and
`FCALL` scripts have the same guarantee. See
[known differences from Redis](IMPLEMENTATION.md#known-differences-from-redis).

Commands queued inside `MULTI` receive `QUEUED` responses. Runtime errors inside `EXEC` (e.g. `INCR` on a non-integer) are returned as per-command errors without aborting the remaining commands: on a table-backed database each command runs in its own subtransaction, so a failure rolls back that command only.

`WATCH` tracks writes through counters in shared memory, so it detects a conflicting write no matter which worker served it. Two consequences:

- It requires the extension in `shared_preload_libraries`; without it, `WATCH` returns an error, so it never silently fails to detect a conflict.
- Keys are hashed into a fixed table of counters, so unrelated keys can occasionally share one. That can abort an `EXEC` that did not strictly conflict; it can never let a conflicting one through. Retry on a nil `EXEC`, as with Redis.
