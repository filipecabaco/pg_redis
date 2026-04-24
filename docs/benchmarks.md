# Benchmarks

All figures are requests/second on Docker, Apple M-series. Benchmarks use [`redis-benchmark`](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/) with `-n 50000 -c 200`, connecting to the default database (db 0 for memory/unlogged, db 1 for logged).

## Commands

| Command | Redis 7 | pg_redis (memory) | pg_redis (SPI/unlogged) | pg_redis (SPI/logged) |
|---------|---------|-------------------|-------------------------|-----------------------|
| PING    | 234,000 | 112,000           | 113,000                 | 103,000               |
| GET     | 243,000 | 106,000           | 85,000                  | 92,000                |
| SET     | 224,000 | 103,000           | 22,000                  | 12,000                |
| INCR    | 253,000 | 116,000           | 32,000                  | 14,000                |
| HSET    | 227,000 | 83,000            | 26,000                  | 13,000                |
| ZADD    | 185,000 | 118,000           | 15,000                  | 7,000                 |
| SADD    | 194,000 | 82,000            | 71,000                  | 66,000                |
| SPOP    | 183,000 | 116,000           | 51,000                  | 64,000                |
| ZPOPMIN | 183,000 | 118,000           | 48,000                  | 47,000                |

**Memory mode** (even-numbered databases with `redis.storage_mode=memory`) stores data in shared-memory hash tables with no transaction overhead. Write commands reach near-Redis throughput; reads are close behind.

**SPI/unlogged** (even-numbered databases with `redis.storage_mode=auto`) uses PostgreSQL UNLOGGED tables. Reads remain fast; writes pay transaction and SPI overhead but survive worker restarts (though not a crash).

**SPI/logged** (odd-numbered databases) uses WAL-logged PostgreSQL tables. All data survives crashes and is fully SQL-queryable. Write throughput is lower due to WAL fsync overhead.

Use `mise run bench-high-write` (8 workers, batch_size=256) to increase write throughput by spreading connections across more dispatchers.

## Pub/Sub

One publisher, varying subscriber counts. **pub/s** = publisher throughput; **recv/s** = total deliveries/s across all subscribers.

| Subscribers | Redis 7 pub/s | Redis 7 recv/s | pg_redis pub/s | pg_redis recv/s |
|-------------|--------------|----------------|----------------|-----------------|
| 1  | 2,509  | 2,506  | 9,451  | 9,172   |
| 4  | 2,359  | 9,433  | 12,297 | 38,086  |
| 16 | 3,865  | 61,762 | 49,810 | 348,682 |
| 32 | 2,896  | 92,516 | 8,993  | 237,891 |

recv/s scales linearly with subscriber count — one PUBLISH fans to N ring buffers in parallel.

## Table routing overhead

Routing a PUBLISH to a PostgreSQL table adds a fire-and-forget INSERT after the reply is sent.

| Scenario | pub/s (no routing) | pub/s (with routing) |
|----------|--------------------|----------------------|
| No subscribers | 10,833 | 5,577 |
| 1 subscriber   | 9,451  | 6,369 |
| 4 subscribers  | 12,297 | 11,071 |

## Running the benchmarks

```bash
mise run bench           # standard commands
mise run bench-pubsub    # pub/sub
```

Both profiles start pg_redis on `:6379` and Redis 7 on `:6380` with password `testpass`, run the suite, then tear down.
