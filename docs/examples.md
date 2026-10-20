# Examples

Each recipe below was run against pg_redis, and the output shown is what it printed. Database
`0` uses the `memory` policy and database `8` the `logged` one, as they do by default (see
[Storage policies](./STORAGE.md)). On a logged database every key is a row you can read, join
and change from SQL, in the tables `redis.kv_8`, `redis.hash_8`, `redis.list_8`,
`redis.set_8`, `redis.zset_8` and `redis.stream_8`.

## Connect from any Redis client

pg_redis speaks RESP2 and RESP3 on port 6379, so a client needs nothing but its host,
password and database number.

=== "redis-cli"

    ```bash
    redis-cli -h db.internal -a "$REDIS_PASSWORD" -n 8
    ```

=== "Python (redis-py)"

    ```python
    import redis

    r = redis.Redis(host="db.internal", port=6379, password="...", db=8)
    r.set("greeting", "hello")
    print(r.get("greeting"))  # b'hello'
    ```

=== "Node (node-redis)"

    ```js
    import { createClient } from "redis";

    const client = createClient({ url: "redis://:password@db.internal:6379/8" });
    await client.connect();
    await client.set("greeting", "hello");
    console.log(await client.get("greeting")); // hello
    ```

=== "Elixir (Redix)"

    ```elixir
    {:ok, conn} = Redix.start_link("redis://:password@db.internal:6379/8")
    Redix.command!(conn, ["SET", "greeting", "hello"])
    Redix.command!(conn, ["GET", "greeting"]) # "hello"
    ```

## A cache that evicts

A memory database with an evicting policy works as a Redis cache does: keys carry a TTL, and
past `maxmemory` the least recently used go first.

```
127.0.0.1:6379> CONFIG SET maxmemory 64mb maxmemory-policy allkeys-lru
OK
127.0.0.1:6379> SET page:/pricing "<html>…</html>" EX 300
OK
127.0.0.1:6379> TTL page:/pricing
(integer) 300
```

Set `redis.maxmemory_policy` in `postgresql.conf` to keep the policy across restarts
([Configuration](./configuration.md)).

## Sessions you can join with your tables

On a logged database, a session the application writes is a set of rows PostgreSQL can join
with its own tables. Here `app_users` is the application's table of users, `(id int, email
text)`:

```
127.0.0.1:6379[8]> HSET session:9f2c user_id 42 ip 203.0.113.7 agent "Firefox/131"
(integer) 3
127.0.0.1:6379[8]> EXPIRE session:9f2c 86400
(integer) 1
```

```sql
SELECT convert_from(s.key, 'UTF8') AS session,
       u.email,
       e.expires_at > now() AS live
FROM redis.hash_8 s
JOIN app_users u ON u.id = convert_from(s.value, 'UTF8')::int
LEFT JOIN redis.expiry_8 e ON e.key = s.key
WHERE s.field = 'user_id';
```

```
   session    |      email      | live
--------------+-----------------+------
 session:9f2c | ada@example.com | t
```

Keys, fields and values are `bytea`, as Redis's are bytes: `convert_from(..., 'UTF8')` reads
them as text. A string's deadline is the `expires_at` column of `redis.kv_8`; a collection's
is in `redis.expiry_8`.

## Rate limiting

A script runs atomically, so the counter and its expiry are set together:

```
127.0.0.1:6379[8]> EVAL "local n = redis.call('INCR', KEYS[1]) if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return n" 1 rate:203.0.113.7 60
(integer) 1
127.0.0.1:6379[8]> EVAL "local n = redis.call('INCR', KEYS[1]) if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return n" 1 rate:203.0.113.7 60
(integer) 2
127.0.0.1:6379[8]> TTL rate:203.0.113.7
(integer) 60
```

Refuse the request when the reply passes the limit. With `FUNCTION LOAD` the script can be
stored once and called by name with `FCALL`.

## A reliable job queue

Workers move a job to a processing list as they take it, so a worker that dies leaves its job
there to be retried rather than losing it:

```
127.0.0.1:6379[8]> LPUSH jobs '{"id":1,"task":"send-welcome"}' '{"id":2,"task":"resize-avatar"}'
(integer) 2
127.0.0.1:6379[8]> BLMOVE jobs jobs:processing RIGHT LEFT 5
"{\"id\":1,\"task\":\"send-welcome\"}"
127.0.0.1:6379[8]> LREM jobs:processing 1 '{"id":1,"task":"send-welcome"}'
(integer) 1
127.0.0.1:6379[8]> LRANGE jobs 0 -1
1) "{\"id\":2,\"task\":\"resize-avatar\"}"
```

`BLMOVE` blocks until a job arrives, for up to the timeout given (here 5 seconds). On a logged
database the queue survives a restart, and the jobs are rows in `redis.list_8`.

## Events with consumer groups

A stream keeps every event in order; each group reads it once, and an event a consumer read
but never acknowledged can be claimed by another:

```
127.0.0.1:6379[8]> XADD orders 1-0 order_id 1001 total 42.50
"1-0"
127.0.0.1:6379[8]> XADD orders 2-0 order_id 1002 total 9.99
"2-0"
127.0.0.1:6379[8]> XGROUP CREATE orders billing 0
OK
127.0.0.1:6379[8]> XREADGROUP GROUP billing worker-1 COUNT 2 STREAMS orders >
1) 1) "orders"
   2) 1) 1) "1-0"
         2) 1) "order_id"
            2) "1001"
            3) "total"
            4) "42.50"
      2) 1) "2-0"
         2) 1) "order_id"
            2) "1002"
            3) "total"
            4) "9.99"
127.0.0.1:6379[8]> XACK orders billing 1-0
(integer) 1
127.0.0.1:6379[8]> XAUTOCLAIM orders billing worker-2 0 0-0
1) "0-0"
2) 1) 1) "2-0"
      2) 1) "order_id"
         2) "1002"
         3) "total"
         4) "9.99"
3) (empty array)
```

`worker-1` processed order 1001 and acknowledged it; `worker-2` takes over 1002, which was
left pending. In production pass `XAUTOCLAIM` a minimum idle time, such as `60000`, so only
events stuck for a minute move.

## A leaderboard, reported from SQL

```
127.0.0.1:6379[8]> ZADD scores 1200 ada 950 grace 1100 linus
(integer) 3
127.0.0.1:6379[8]> ZINCRBY scores 150 grace
"1100"
127.0.0.1:6379[8]> ZRANGE scores 0 2 REV WITHSCORES
1) "ada"
2) "1200"
3) "linus"
4) "1100"
5) "grace"
6) "1100"
```

The same scores, ranked with a window function for a report:

```sql
SELECT convert_from(member, 'UTF8') AS player, score,
       rank() OVER (ORDER BY score DESC) AS place
FROM redis.zset_8 WHERE key = 'scores';
```

```
 player | score | place
--------+-------+-------
 ada    |  1200 |     1
 linus  |  1100 |     2
 grace  |  1100 |     2
```

## Pub/sub messages straight into a table

A channel can be routed into a table, so what is published is also stored, ready to be
queried or replayed, whether or not anyone was subscribed:

```sql
SELECT redis.create_pubsub_table('public', 'order_events');
SELECT redis.route_publish('orders.created', 'public', 'order_events');
```

```
127.0.0.1:6379[8]> PUBLISH orders.created '{"order_id":1001,"total":42.5}'
(integer) 0
```

```sql
SELECT id, convert_from(channel, 'UTF8') AS channel,
       convert_from(payload, 'UTF8')::jsonb AS payload
FROM public.order_events;
```

```
 id |    channel     |              payload
----+----------------+-----------------------------------
  1 | orders.created | {"total": 42.5, "order_id": 1001}
```

`PUBLISH` replies 0 because no client was subscribed; subscribers that are get the message as
usual. `redis.unroute_publish('orders.created')` stops the copying.

## Write from SQL, read from Redis

A row written with SQL is a key the next command sees, so a migration, a trigger or a batch
job can set what applications read:

```sql
INSERT INTO redis.kv_8 (key, value) VALUES ('feature:checkout-v2', 'on')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL;
```

```
127.0.0.1:6379[8]> GET feature:checkout-v2
"on"
```

## Change where a database lives

A database moves between policies while clients keep using it:

```sql
SELECT * FROM redis.policies() WHERE db IN (0, 1, 8);
```

```
 db | policy | moving_from
----+--------+-------------
  0 | memory |
  1 | memory |
  8 | logged |
```

```sql
SELECT redis.set_policy(1, 'memory+snapshot');
```

`memory+snapshot` keeps the data in shared memory and snapshots it to a table, every
`redis.snapshot_interval` seconds and at shutdown, so it comes back after a restart. Moving
to `logged` turns every key into rows, moving each as it is next touched and the rest in the
background; `moving_from` shows a move in progress ([Storage policies](./STORAGE.md)).

## A user per application

Each application gets only the commands and keys it needs:

```
127.0.0.1:6379[8]> ACL SETUSER billing on >billing-secret ~orders* ~invoice:* +@read +@write +@stream +select -@dangerous
OK
```

Logged in as `billing`:

```
127.0.0.1:6379[8]> XLEN orders
(integer) 2
127.0.0.1:6379[8]> GET session:9f2c
(error) NOPERM No permissions to access a key
127.0.0.1:6379[8]> FLUSHALL
(error) NOPERM User billing has no permissions to run the 'flushall' command
```

With `redis.aclfile` set, `ACL SAVE` writes the users to it in Redis's format and the server
loads them at start ([Users](./configuration.md#users)).
