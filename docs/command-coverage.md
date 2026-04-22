# Command coverage

Status of Redis commands against pg_redis. Commands not listed are not implemented.

Legend: **Yes** = fully supported · **Partial** = supported with caveats · **No** = not implemented

## String / Key–value

| Command | Status | Notes |
|---------|--------|-------|
| `GET` | Yes | |
| `SET` | Yes | NX, XX, GET, EX, PX, EXAT, PXAT, KEEPTTL flags |
| `SETEX` | Yes | |
| `PSETEX` | Yes | |
| `MGET` | Yes | |
| `MSET` | Yes | |
| `DEL` | Yes | |
| `EXISTS` | Yes | Multi-key |
| `GETSET` | No | Use `SET … GET` instead |
| `GETEX` | No | |
| `GETDEL` | No | |
| `SETNX` | No | Use `SET … NX` instead |
| `MSETNX` | No | |
| `APPEND` | No | |
| `STRLEN` | No | |
| `INCR` | No | |
| `INCRBY` | No | |
| `INCRBYFLOAT` | No | |
| `DECR` | No | |
| `DECRBY` | No | |
| `SUBSTR` / `GETRANGE` | No | |
| `SETRANGE` | No | |

## Expiry

| Command | Status | Notes |
|---------|--------|-------|
| `EXPIRE` | Yes | Seconds |
| `PEXPIRE` | Yes | Milliseconds |
| `EXPIREAT` | Yes | Unix timestamp (seconds) |
| `PEXPIREAT` | Yes | Unix timestamp (milliseconds) |
| `TTL` | Yes | |
| `PTTL` | Yes | |
| `PERSIST` | Yes | |
| `EXPIRETIME` | Yes | |
| `PEXPIRETIME` | Yes | |

## Hashes

| Command | Status | Notes |
|---------|--------|-------|
| `HGET` | Yes | |
| `HSET` | Yes | Multi-field |
| `HDEL` | Yes | Multi-field |
| `HGETALL` | Yes | |
| `HMGET` | No | |
| `HMSET` | No | Use `HSET` with multiple fields |
| `HKEYS` | No | |
| `HVALS` | No | |
| `HLEN` | No | |
| `HEXISTS` | No | |
| `HINCRBY` | No | |
| `HINCRBYFLOAT` | No | |
| `HSCAN` | No | |
| `HRANDFIELD` | No | |
| `HEXPIRE` | No | Hash field TTL not supported |

## Lists

| Command | Status | Notes |
|---------|--------|-------|
| `LPUSH` | No | |
| `RPUSH` | No | |
| `LPOP` | No | |
| `RPOP` | No | |
| `LRANGE` | No | |
| `LLEN` | No | |
| `LINDEX` | No | |
| `LSET` | No | |
| `LREM` | No | |
| `LTRIM` | No | |
| `LINSERT` | No | |
| `LMOVE` | No | |
| `BLPOP` | No | |
| `BRPOP` | No | |

## Sets

| Command | Status | Notes |
|---------|--------|-------|
| `SADD` | No | |
| `SREM` | No | |
| `SMEMBERS` | No | |
| `SISMEMBER` | No | |
| `SCARD` | No | |
| `SUNION` | No | |
| `SINTER` | No | |
| `SDIFF` | No | |
| `SSCAN` | No | |
| `SRANDMEMBER` | No | |
| `SPOP` | No | |

## Sorted sets

| Command | Status | Notes |
|---------|--------|-------|
| `ZADD` | No | |
| `ZREM` | No | |
| `ZSCORE` | No | |
| `ZRANK` | No | |
| `ZRANGE` | No | |
| `ZRANGEBYSCORE` | No | |
| `ZCARD` | No | |
| `ZINCRBY` | No | |
| `ZCOUNT` | No | |
| `ZSCAN` | No | |

## Pub/Sub

| Command | Status | Notes |
|---------|--------|-------|
| `PUBLISH` | Yes | Routes to PostgreSQL tables via pattern matching — see [Pub/Sub routing](pubsub.md) |
| `SUBSCRIBE` | No | |
| `UNSUBSCRIBE` | No | |
| `PSUBSCRIBE` | No | |
| `PUNSUBSCRIBE` | No | |

## Transactions

| Command | Status | Notes |
|---------|--------|-------|
| `MULTI` | Yes | |
| `EXEC` | Yes | |
| `DISCARD` | Yes | |
| `WATCH` | Yes | |
| `UNWATCH` | Yes | |

## Server / Connection

| Command | Status | Notes |
|---------|--------|-------|
| `PING` | Yes | Optional message |
| `ECHO` | Yes | |
| `SELECT` | Yes | Databases 0–15 |
| `AUTH` | Yes | Validates against `redis.password` GUC |
| `INFO` | Yes | Returns server info string |
| `COMMAND` | Yes | Returns empty array (client compatibility) |
| `CLIENT` | Partial | No-op, returns OK |
| `QUIT` | No | |
| `RESET` | No | |
| `KEYS` | No | |
| `SCAN` | No | |
| `TYPE` | No | |
| `RENAME` | No | |
| `RENAMENX` | No | |
| `COPY` | No | |
| `MOVE` | No | |
| `RANDOMKEY` | No | |
| `OBJECT` | No | |
| `DEBUG` | No | |
| `FLUSHDB` | No | |
| `FLUSHALL` | No | |
| `DBSIZE` | No | |
| `SAVE` | No | |
| `BGSAVE` | No | |
| `LASTSAVE` | No | |
| `SHUTDOWN` | No | |
| `SLAVEOF` | No | |
| `REPLICAOF` | No | |
| `CONFIG` | No | Use PostgreSQL GUCs instead — see [Configuration](configuration.md) |

## Compatibility matrix

| Postgres version | pgrx version | Status |
|-----------------|--------------|--------|
| 17 | 0.18.0 | Tested |
| 16 | 0.18.0 | Tested |
| 15 | 0.18.0 | Tested |
| 18 | 0.18.0 | Tested |

Tested against `redis-cli` (Redis 7.x) and standard RESP2 clients.
