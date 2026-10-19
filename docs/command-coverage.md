# Command coverage

Every command pg_redis accepts, grouped by family. Anything absent is not
implemented and replies with an error.

This page is checked against the parser by a test, so a command cannot be
implemented without appearing here, or listed here without existing.

Behaviour is identical under every storage policy unless a note says otherwise.
The limits that apply only to databases in memory are described in
[Limits](IMPLEMENTATION.md#7-limits) rather than repeated per command.


## Connection

| Command | Notes |
|---------|-------|
| `PING` |  |
| `ECHO` |  |
| `SELECT` | Also accepts the names `cache` (db 0) and `durable` (db 8) |
| `AUTH` | Validated against `redis.password`; a no-op when that is unset |
| `HELLO` | `HELLO 3` switches the connection to RESP3 replies and pub/sub push frames |
| `RESET` |  |
| `QUIT` |  |
| `CLIENT` | `ID`, `INFO`, `LIST`, `KILL`, `GETNAME`/`SETNAME`, `SETINFO`, `REPLY`, `PAUSE`/`UNPAUSE`, `NO-EVICT`, `NO-TOUCH`, across every worker; `TRACKING` (standard, `BCAST`, `OPTIN`/`OPTOUT`, `NOLOOP`, `REDIRECT`), `CACHING`, `GETREDIR`, `TRACKINGINFO` |
| `COMMAND` | `INFO`, `DOCS`, `LIST`, `GETKEYS`, `GETKEYSANDFLAGS` from Redis 7.2's own command table, for the commands on this page; `COUNT` reports the number of rows on this page |
| `CONFIG` | `GET`/`SET` over Redis 7.2's parameter table, validated as Redis validates; most parameters describe Redis internals and change nothing here. `RESETSTAT`; `REWRITE` reports no config file |
| `INFO` | Every section; `commandstats`, `errorstats` and `latencystats` are counted across workers |
| `TIME` | Seconds and microseconds, from one clock reading |

## Server and administration

| Command | Notes |
|---------|-------|
| `SLOWLOG` | `GET`, `LEN`, `RESET`, honouring `slowlog-log-slower-than` and `slowlog-max-len` |
| `LATENCY` | `HISTOGRAM` from per-command latencies; no spike sampling, so `LATEST` and `HISTORY` are empty |
| `ACL` | A single user, `default`, with every permission; `WHOAMI`, `USERS`, `LIST`, `CAT`, `GETUSER`, `LOG`, `GENPASS` |
| `DEBUG` | `SLEEP`, `SET-ACTIVE-EXPIRE`, `PROTOCOL`; `RELOAD` and the tuning subcommands are accepted |
| `LASTSAVE` |  |
| `SAVE` | Nothing to save: the durable half is already in PostgreSQL |
| `BGSAVE` | As `SAVE` |
| `BGREWRITEAOF` | Accepted |
| `ROLE` | Always `master` |
| `REPLICAOF` | Only `NO ONE`; replicate with PostgreSQL instead |
| `SLAVEOF` | As `REPLICAOF` |
| `WAIT` | No replicas, so 0 at once |
| `WAITAOF` | Refused, as Redis does with `appendonly no` |
| `READONLY` | Cluster support is disabled |
| `READWRITE` | As `READONLY` |
| `CLUSTER` | As `READONLY`: every subcommand answers as a stand-alone Redis does |
| `ASKING` | As `READONLY` |
| `FAILOVER` | No replicas to fail over to |
| `SHUTDOWN` | Refused: stop PostgreSQL instead |
| `LOLWUT` |  |
| `MONITOR` | Every worker's commands, admin and scripting commands excluded and passwords redacted as Redis does; a line past 512 bytes is cut short |

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
| `LCS` | `LEN`, `IDX`, `MINMATCHLEN` and `WITHMATCHLEN`, ranges reported in Redis's order |
| `BITOP` | `AND`, `OR`, `XOR`, `NOT`; shorter inputs read as zero-padded |
| `BITPOS` | `BYTE` and `BIT` ranges |
| `BITFIELD` | `GET`, `SET`, `INCRBY`, `OVERFLOW WRAP\|SAT\|FAIL`, `#N` offsets; keeps the key's TTL |
| `BITFIELD_RO` | `GET` only |

## Keys

| Command | Notes |
|---------|-------|
| `DEL` |  |
| `UNLINK` |  |
| `EXISTS` |  |
| `TOUCH` | Counts as `EXISTS` does; there is no LRU clock to refresh |
| `TYPE` |  |
| `KEYS` | Full scan, as in Redis, over keys of every type. Glob matching is done in-process, not in SQL |
| `SCAN` | Cursor is always 0 — one full pass, no incremental guarantee. `MATCH`, `COUNT` and `TYPE` honoured |
| `RANDOMKEY` | Uniform over every live key, whatever table holds it |
| `DUMP` | Redis's serialisation format, version 11, streams included; restores into a real 7.2 |
| `RESTORE` | REPLACE and ABSTTL honoured, IDLETIME and FREQ accepted; Redis stream payloads of every format since 5.0; refuses payloads newer than version 11 and pre-7 compact encodings |
| `RENAME` |  |
| `RENAMENX` |  |
| `COPY` | `DB` may name a database on the other storage half |
| `OBJECT` | `ENCODING` only; reports the encoding Redis would use |
| `MEMORY` | `USAGE` only, an estimate from the key's serialised size; `SAMPLES` accepted and ignored |
| `DBSIZE` | Counts keys of every type |
| `FLUSHDB` | ASYNC and SYNC accepted and ignored |
| `FLUSHALL` | The same, over all sixteen databases — the durable half included |
| `MOVE` | Across the storage halves too; the TTL moves with the key |
| `SORT` | `BY` (with `->field`), `LIMIT`, `GET`, `ASC`/`DESC`, `ALPHA`, `STORE` |
| `SORT_RO` | `SORT` without `STORE` |
| `SWAPDB` | Any two databases, across the storage halves too; a key the target half cannot hold puts both back and fails |

## Expiry

| Command | Notes |
|---------|-------|
| `EXPIRE` | Applies to a key of any type; `NX`, `XX`, `GT` and `LT` on all four. Per-field TTLs (`HEXPIRE`) are not implemented |
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
| `HSCAN` | Cursor is always 0 — one full pass, `MATCH` honoured |

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
| `LPOS` | RANK, COUNT and MAXLEN supported, including negative RANK |
| `LMOVE` |  |
| `RPOPLPUSH` | `LMOVE src dst RIGHT LEFT`, under its older name |
| `LMPOP` | `LEFT`/`RIGHT` and `COUNT` |
| `BLPOP` | Blocks across workers; waiters on a key are served oldest first. Inside MULTI it runs once, as in Redis |
| `BRPOP` | As `BLPOP` |
| `BLMOVE` | As `BLPOP`; a destination of the wrong type is WRONGTYPE and the element stays put |
| `BRPOPLPUSH` | `BLMOVE src dst RIGHT LEFT` |
| `BLMPOP` | As `LMPOP`, blocking |

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
| `SSCAN` | Cursor is always 0 — one full pass, `MATCH` honoured |

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
| `ZSCAN` | Cursor is always 0 — one full pass, `MATCH` honoured |
| `ZRANGESTORE` | Every `ZRANGE` form: by rank, `BYSCORE`, `BYLEX`, `REV`, `LIMIT` |
| `ZMPOP` | `MIN`/`MAX` and `COUNT` |
| `BZPOPMIN` | As `BLPOP`, for sorted sets |
| `BZPOPMAX` | As `BZPOPMIN` |
| `BZMPOP` | As `ZMPOP`, blocking |

## HyperLogLog

Values are Redis's own `HYLL` strings, sparse and dense encodings included, so
`GET`, `DUMP` and a real Redis all agree on them.

| Command | Notes |
|---------|-------|
| `PFADD` |  |
| `PFCOUNT` | One key returns and stores the cached cardinality; several count their union |
| `PFMERGE` | The destination is one of the inputs, as in Redis |
| `PFDEBUG` | `GETREG`, `DECODE`, `ENCODING`, `TODENSE` |
| `PFSELFTEST` |  |

## GEO

A GEO set is a sorted set whose scores are Redis's 52-bit geohashes, so it
works on both halves and `ZRANGE` and friends see it as Redis would. Scores,
hashes, distances and the order results come back in match Redis 7.2.

| Command | Notes |
|---------|-------|
| `GEOADD` | `NX`, `XX`, `CH` |
| `GEODIST` | `M`, `KM`, `FT`, `MI` |
| `GEOHASH` |  |
| `GEOPOS` |  |
| `GEOSEARCH` | `FROMMEMBER`/`FROMLONLAT`, `BYRADIUS`/`BYBOX`, `ASC`/`DESC`, `COUNT [ANY]`, `WITHCOORD`/`WITHDIST`/`WITHHASH` |
| `GEOSEARCHSTORE` | `STOREDIST` |
| `GEORADIUS` | Deprecated in Redis, kept for clients that still send it; `STORE`/`STOREDIST` |
| `GEORADIUS_RO` |  |
| `GEORADIUSBYMEMBER` |  |
| `GEORADIUSBYMEMBER_RO` |  |

## Streams

A stream is held whole as one value per key and loaded, changed and stored
back by each command, so a command costs time proportional to the stream's
size; on the memory half a stream is also bound by the 64 KiB value cap.
Behaviour follows Redis 7.2 — IDs, ranges, consumer groups, pending lists,
claiming, lag — and keeps Redis's node layout, so approximate trimming and
`XINFO STREAM`'s `radix-tree-keys` match too. `DUMP`/`RESTORE` of a stream
round-trip between pg_redis servers but not with a real Redis.

| Command | Notes |
|---------|-------|
| `XADD` | `NOMKSTREAM`, `MAXLEN`/`MINID` with `=`/`~` and `LIMIT`, `ms-*` IDs |
| `XLEN` |  |
| `XRANGE` | Exclusive `(` bounds and `COUNT` |
| `XREVRANGE` |  |
| `XDEL` |  |
| `XTRIM` |  |
| `XREAD` | `BLOCK` across workers, `$` resolved when the read first runs |
| `XREADGROUP` | `NOACK`, history reads, `BLOCK` |
| `XGROUP` | `CREATE` (`MKSTREAM`, `ENTRIESREAD`), `SETID`, `DESTROY`, `CREATECONSUMER`, `DELCONSUMER` |
| `XACK` |  |
| `XPENDING` | Summary and extended forms, `IDLE` |
| `XCLAIM` | `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID`, `LASTID` |
| `XAUTOCLAIM` | Reports entries deleted while pending |
| `XINFO` | `STREAM [FULL [COUNT]]`, `GROUPS`, `CONSUMERS` |
| `XSETID` | `ENTRIESADDED`, `MAXDELETEDID` |

## Pub/Sub

| Command | Notes |
|---------|-------|
| `SUBSCRIBE` |  |
| `UNSUBSCRIBE` |  |
| `PSUBSCRIBE` |  |
| `PUNSUBSCRIBE` |  |
| `PUBLISH` | Optionally also inserts into a table — see Pub/Sub routing |
| `PUBSUB` | CHANNELS, NUMSUB, NUMPAT, SHARDCHANNELS, SHARDNUMSUB |
| `SPUBLISH` | Sharded pub/sub: its own namespace, as on a stand-alone Redis |
| `SSUBSCRIBE` |  |
| `SUNSUBSCRIBE` |  |

## Transactions

| Command | Notes |
|---------|-------|
| `MULTI` |  |
| `EXEC` | Each queued command runs in its own subtransaction |
| `DISCARD` |  |
| `WATCH` | Requires `shared_preload_libraries`; see Transactions in Commands |
| `UNWATCH` |  |

## Scripting and functions

Lua 5.1, run whole on the dispatcher, so a script is atomic against every
other command its worker serves. `redis.call` reaches both storage halves and
any database a script `SELECT`s. The libraries are Redis's own — `cjson`,
`cmsgpack`, `struct` and `bit` are its C sources — and globals are read-only,
as in Redis. The script cache and the function libraries are shared by every
worker through `redis.scripts` and `redis.functions`.

| Command | Notes |
|---------|-------|
| `EVAL` | Shebang flags (`#!lua flags=no-writes,...`) honoured |
| `EVALSHA` |  |
| `EVAL_RO` |  |
| `EVALSHA_RO` |  |
| `SCRIPT` | LOAD, EXISTS, FLUSH, KILL, HELP; DEBUG answers NO only |
| `FUNCTION` | LOAD, DELETE, FLUSH, LIST, DUMP, RESTORE, STATS, KILL, HELP |
| `FCALL` |  |
| `FCALL_RO` |  |

A script past `busy-reply-threshold` (default 5 s) makes every other client
get BUSY, on every worker, until it ends or `SCRIPT KILL` / `FUNCTION KILL`
stops it. Atomicity is per worker: on the memory half a script's sub-commands
take their own locks, so another worker's commands can interleave with them.

## Not implemented

Notable absences, so you do not have to infer them from silence: the
per-field hash TTLs of Redis 7.4 (`HEXPIRE`, `HPEXPIRE`, `HTTL`, `HPTTL`,
`HPERSIST`); `MIGRATE`; replication (`SYNC`, `PSYNC`, `REPLCONF`); modules
(`MODULE`); and `SENTINEL`.

A test asserts that every command named above is genuinely rejected, so this
paragraph cannot quietly go stale — which it did once, naming as absent two
commands that had since been implemented.

