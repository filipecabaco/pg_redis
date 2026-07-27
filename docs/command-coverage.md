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
| `COMMAND` | Returns an empty array, enough for client handshakes |
| `CONFIG` | `GET` returns nothing; `SET` is accepted and ignored |
| `INFO` |  |

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

## Keys

| Command | Notes |
|---------|-------|
| `DEL` |  |
| `UNLINK` |  |
| `EXISTS` |  |
| `TYPE` |  |
| `KEYS` | Full scan, as in Redis. Glob matching is done in-process, not in SQL |
| `SCAN` | Cursor is always 0 — one full pass, no incremental guarantee |
| `RANDOMKEY` |  |
| `RENAME` |  |
| `DBSIZE` |  |

## Expiry

| Command | Notes |
|---------|-------|
| `EXPIRE` | Expiry applies to string keys only, as in Redis before `HEXPIRE` |
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
| `HINCRBY` |  |

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

Notable absences, so you do not have to infer them from silence: `GETRANGE`,
`SETRANGE`, `SETBIT`/`GETBIT` and the other bit operations, `HRANDFIELD`,
`HSCAN`/`SSCAN`/`ZSCAN`, `SINTERCARD`, `OBJECT`, `SORT`, `DUMP`/`RESTORE`,
`MIGRATE`, `BLPOP` and the other blocking list commands, `EVAL` and scripting,
streams (`XADD` and friends), and cluster commands.

