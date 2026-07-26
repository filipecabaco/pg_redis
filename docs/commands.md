# Supported commands

## Connection

| Command | Behaviour |
|---------|-----------|
| `PING [msg]` | Returns `PONG` or echoes `msg` |
| `ECHO msg` | Returns `msg` |
| `SELECT db` | 0–15, or the names `cache` (0) and `durable` (8). Databases 0–7 are ephemeral, 8–15 WAL-logged — see [Storage modes](storage-modes.md) |
| `AUTH [password]` | Validates against `redis.password` GUC; no-op when unset |
| `INFO` | Returns server info string |
| `COMMAND` | Returns empty array (client compatibility) |
| `CLIENT ...` | No-op, returns OK |

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

## Hashes

| Command | Behaviour |
|---------|-----------|
| `HGET key field` | Returns field value or nil |
| `HSET key field value [field value ...]` | Upsert one or more fields, returns new field count |
| `HDEL key field [field ...]` | Delete fields, returns count deleted |
| `HGETALL key` | Returns interleaved field/value pairs, sorted by field |

> Expiry is not supported on hash keys (same behaviour as Redis hash TTLs without `HEXPIRE`).

## Lists

| Command | Behaviour |
|---------|-----------|
| `LPUSH`/`RPUSH key value [value ...]` | Prepend/append, returns the new length |
| `LPOP`/`RPOP key [count]` | Pop from either end |
| `LRANGE key start stop` | Inclusive range, negative indices count from the tail |
| `LINSERT key BEFORE\|AFTER pivot value` | Returns the new length, `-1` if the pivot is absent, `0` if the key is |
| `LREM key count value` | `count > 0` from the head, `< 0` from the tail, `0` removes every match |
| `LPOS key element [RANK r] [COUNT n]` | Negative `RANK` searches from the tail; `COUNT 0` returns every match |

`LINSERT` and `LREM` rewrite the list so its positions stay contiguous, which
makes them O(n). Removing in place would leave a gap, and every reader walks
positions in order — the list would silently end at the first gap.

## Errors and limits

Errors carry a Redis error code, so client-side handling that matches on one
works unchanged:

| Reply | Meaning |
|---|---|
| `-ERR ...` | The general case |
| `-OOM command not allowed when used memory > 'maxmemory'` | A shared-memory table is full and `redis.maxmemory_policy` is `noeviction`. See [When a table fills](storage-modes.md#when-a-table-fills) |
| `-ERR key exceeds redis.storage_mode='memory' limit of 511 bytes ...` | The key, hash field, set member or value is larger than a shared-memory slot |

Both of those are specific to `storage_mode = 'memory'`; the durable half is
bounded only by disk. Size limits are checked before the command runs, so a
refusal never leaves a partial write — no half-stored value and no entry under
a truncated key.

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
