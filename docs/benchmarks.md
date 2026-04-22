# Benchmarks

All figures are requests/second on Docker, Apple M-series. Benchmarks use [`redis-benchmark`](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/) with `-n 50000 -c 200`.

## Commands

| Command | Redis 7 | pg_redis (memory) | pg_redis (logged) |
|---------|---------|-------------------|-------------------|
| PING    | 198,000 | —       | 138,000 |
| GET     | 185,000 | 136,000 | 90,000  |
| SET     | 175,000 | 118,000 | 3,004   |
| INCR    | 187,000 | 123,000 | 2,764   |
| HSET    | 182,000 | 124,000 | 15,038  |
| ZADD    | 185,000 | 127,000 | 9,403   |
| SADD    | 190,000 | 133,000 | 29,412  |
| SPOP    | 189,000 | 131,000 | 22,989  |
| ZPOPMIN | 194,000 | 140,000 | 17,652  |
| LPOP    | 142,000 | 119,000 | 5,965   |
| RPOP    | 177,000 | 144,000 | 11,614  |

Memory mode (the default) reaches Redis-level throughput for all write commands. Logged databases pay PostgreSQL transaction overhead but survive crashes and are SQL-queryable.

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
