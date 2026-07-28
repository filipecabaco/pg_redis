# Command coverage

Every command pg_redis accepts, grouped by family. Anything absent is not
implemented and replies with an error.

This page is checked against the parser by a test, so a command cannot be
implemented without appearing here, or listed here without existing.

Behaviour is identical in both storage modes unless a note says otherwise. The
limits that apply only to `storage_mode = 'memory'` are described in
[Storage modes](IMPLEMENTATION.md#7-limits) rather than
repeated per command.


## Connection

| Command | Notes |
|---------|-------|
| `PING` |  |
| `ECHO` |  |
| `SELECT` | Also accepts the names `cache` (db 0) and `durable` (db 8) |
| `AUTH` | Validated against `redis.password`; a no-op when that is unset |
| `HELLO` | RESP3 handshake; push frames are used for pub/sub when negotiated |
| `RESET` |  |
| `QUIT` |  |
| `CLIENT` | Accepted and answered, but connection state is not tracked |
| `COMMAND` | Returns an empty array, enough for client handshakes; `COUNT` reports the number of rows on this page |
| `CONFIG` | `GET` returns nothing; `SET` is accepted and ignored |
| `INFO` |  |
| `TIME` | Seconds and microseconds, from one clock reading |

## Strings

| Command | Notes |
|---------|-------|
| `GET` |  |
| `SET` | NX, XX, GET, EX, PX, EXAT, PXAT and KEEPTTL |
| `SETEX` |  |
| `PSETEX` |  |
| `SETNX` |  |
| `MSETNX` |  |
| `MGET` |  |
| `MSET` |  |
| `APPEND` |  |
| `STRLEN` |  |
| `GETSET` |  |
| `GETDEL` |  |
| `INCR` |  |
| `DECR` |  |
| `INCRBY` |  |
| `DECRBY` |  |
| `INCRBYFLOAT` |  |
| `GETEX` | EX, PX, EXAT, PXAT and PERSIST |
| `SETRANGE` | Pads the gap with NUL bytes, as in Redis |
| `GETRANGE` |  |
| `SUBSTR` | Older name of `GETRANGE`; a separate command, as in Redis's own table |
| `SETBIT` |  |
| `GETBIT` |  |
| `BITCOUNT` | BYTE and BIT ranges; bits are numbered from the most significant |

## Keys

| Command | Notes |
|---------|-------|
| `DEL` |  |
| `UNLINK` |  |
| `EXISTS` |  |
| `TOUCH` | Counts as `EXISTS` does; there is no LRU clock to refresh |
| `TYPE` |  |
| `KEYS` | Full scan, as in Redis, over keys of every type. Glob matching is done in-process, not in SQL |
| `SCAN` | Cursor is always 0 — one full pass, no incremental guarantee |
| `RANDOMKEY` | Uniform over every live key, whatever table holds it |
| `DUMP` | Redis's serialisation format, version 10; restores into a real 7.0 |
| `RESTORE` | REPLACE and ABSTTL honoured, IDLETIME and FREQ accepted; refuses payloads newer than version 10 and pre-7 compact encodings |
| `RENAME` |  |
| `RENAMENX` |  |
| `COPY` | `DB` may name a database on the other storage half |
| `OBJECT` | `ENCODING` only; reports the encoding Redis would use |
| `DBSIZE` | Counts keys of every type |
| `FLUSHDB` | ASYNC and SYNC accepted and ignored |
| `FLUSHALL` | The same, over all sixteen databases — the durable half included |

## Expiry

| Command | Notes |
|---------|-------|
| `EXPIRE` | Applies to a key of any type; per-field TTLs (`HEXPIRE`) are not implemented |
| `PEXPIRE` |  |
| `EXPIREAT` |  |
| `PEXPIREAT` |  |
| `TTL` |  |
| `PTTL` |  |
| `PERSIST` |  |
| `EXPIRETIME` |  |
| `PEXPIRETIME` |  |

## Hashes

| Command | Notes |
|---------|-------|
| `HGET` |  |
| `HSET` |  |
| `HSETNX` |  |
| `HDEL` |  |
| `HGETALL` |  |
| `HMGET` |  |
| `HMSET` |  |
| `HKEYS` |  |
| `HVALS` |  |
| `HLEN` |  |
| `HEXISTS` |  |
| `HSTRLEN` |  |
| `HINCRBY` |  |
| `HINCRBYFLOAT` |  |
| `HRANDFIELD` | A negative count repeats; WITHVALUES interleaves |

## Lists

| Command | Notes |
|---------|-------|
| `LPUSH` |  |
| `RPUSH` |  |
| `LPUSHX` |  |
| `RPUSHX` |  |
| `LPOP` |  |
| `RPOP` |  |
| `LLEN` |  |
| `LRANGE` |  |
| `LINDEX` |  |
| `LSET` |  |
| `LINSERT` | Rewrites the list to keep positions contiguous, so it is O(n) |
| `LREM` | Same rewrite as LINSERT |
| `LTRIM` |  |
| `LPOS` | RANK and COUNT supported, including negative RANK |
| `LMOVE` |  |
| `RPOPLPUSH` | `LMOVE src dst RIGHT LEFT`, under its older name |

## Sets

| Command | Notes |
|---------|-------|
| `SADD` |  |
| `SREM` |  |
| `SMEMBERS` |  |
| `SCARD` |  |
| `SISMEMBER` |  |
| `SMISMEMBER` |  |
| `SPOP` |  |
| `SRANDMEMBER` |  |
| `SMOVE` |  |
| `SUNION` |  |
| `SINTER` |  |
| `SDIFF` |  |
| `SUNIONSTORE` |  |
| `SINTERSTORE` |  |
| `SDIFFSTORE` |  |
| `SINTERCARD` | `LIMIT 0` means no limit |

## Sorted sets

| Command | Notes |
|---------|-------|
| `ZADD` |  |
| `ZREM` |  |
| `ZSCORE` |  |
| `ZMSCORE` |  |
| `ZINCRBY` |  |
| `ZCARD` |  |
| `ZCOUNT` |  |
| `ZLEXCOUNT` |  |
| `ZRANK` |  |
| `ZREVRANK` |  |
| `ZRANGE` |  |
| `ZREVRANGE` |  |
| `ZRANGEBYSCORE` |  |
| `ZREVRANGEBYSCORE` |  |
| `ZRANGEBYLEX` |  |
| `ZREVRANGEBYLEX` |  |
| `ZPOPMIN` |  |
| `ZPOPMAX` |  |
| `ZRANDMEMBER` |  |
| `ZREMRANGEBYRANK` |  |
| `ZREMRANGEBYSCORE` |  |
| `ZREMRANGEBYLEX` |  |
| `ZUNION` |  |
| `ZINTER` |  |
| `ZINTERCARD` | `LIMIT 0` means no limit, as `SINTERCARD` |
| `ZDIFF` |  |
| `ZUNIONSTORE` |  |
| `ZINTERSTORE` |  |
| `ZDIFFSTORE` |  |

## Pub/Sub

| Command | Notes |
|---------|-------|
| `SUBSCRIBE` |  |
| `UNSUBSCRIBE` |  |
| `PSUBSCRIBE` |  |
| `PUNSUBSCRIBE` |  |
| `PUBLISH` | Optionally also inserts into a table — see Pub/Sub routing |
| `PUBSUB` | CHANNELS, NUMSUB, NUMPAT |

## Transactions

| Command | Notes |
|---------|-------|
| `MULTI` |  |
| `EXEC` | Each queued command runs in its own subtransaction |
| `DISCARD` |  |
| `WATCH` | Requires `shared_preload_libraries`; see Transactions in Commands |
| `UNWATCH` |  |

## Not implemented

Notable absences, so you do not have to infer them from silence: `BITOP`,
`BITPOS`, `BITFIELD`, `HSCAN`, `SSCAN`, `ZSCAN`, `SORT`, `MIGRATE`, `BLPOP`
and the other blocking list commands, `HEXPIRE` and the per-field TTLs, `EVAL`
and scripting, streams (`XADD` and friends), and cluster commands.

A test asserts that every command named above is genuinely rejected, so this
paragraph cannot quietly go stale — which it did once, naming as absent two
commands that had since been implemented.

