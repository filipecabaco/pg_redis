# Supported commands

## Connection

| Command | Behaviour |
|---------|-----------|
| `PING [msg]` | Returns `PONG` or echoes `msg` |
| `ECHO msg` | Returns `msg` |
| `SELECT db` | 0–15; even = unlogged, odd = logged |
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

## Transactions

| Command | Behaviour |
|---------|-----------|
| `MULTI` | Begin a transaction block; subsequent commands are queued |
| `EXEC` | Execute all queued commands atomically; returns nil if a `WATCH`ed key changed |
| `DISCARD` | Discard the queued commands and exit the transaction block |
| `WATCH key [key ...]` | Mark keys to watch; if any are modified before `EXEC`, the transaction aborts |
| `UNWATCH` | Clear all watched keys |

Commands queued inside `MULTI` receive `QUEUED` responses. Runtime errors inside `EXEC` (e.g. `INCR` on a non-integer) are returned as per-command errors without aborting the remaining commands.
