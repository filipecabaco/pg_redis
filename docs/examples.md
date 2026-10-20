---
template: examples.html
hide:
  - toc
---

# Examples

Everything on this page runs in the machine beside it: a real PostgreSQL with pg_redis,
booted in your tab. **Run** on a block types it into redis-cli, psql or a shell, and the
output shown under each block is what it printed.

Database `0` uses the `memory` policy and database `8` the `logged` one, as they do by
default ([Storage policies](./STORAGE.md)). On a logged database every key is a row you can
read, join and change from SQL. The views `redis.strings`, `redis.hashes`, `redis.lists`,
`redis.sets`, `redis.zsets` and `redis.streams` show them as text, `redis.keys` gives each key
with its whole value as JSON, and writes go to the tables underneath, `redis.kv_8`,
`redis.hash_8` and the rest ([Querying from SQL](./operations.md#querying-from-sql)).

## Production showcases

Four small applications built on the Redis libraries teams already run in production, used
unchanged: nothing in them knows they are not talking to Redis. Each runs a scenario and
checks what happened, failures and recovery included. They are in
[`examples/`](https://github.com/filipecabaco/pg_redis/tree/main/examples) and run in CI
on every change. The first run downloads Node into the machine, and the emulated CPU makes
them a few times slower than on a server.

### A job queue with BullMQ

[BullMQ](https://docs.bullmq.io) is the most used Redis job queue for Node. Signup emails go
to two workers; a third of them fail on their first try and are retried with exponential
backoff; a reminder waits out its delay; and a worker dies holding an invoice job, which
another worker takes over once its lock lapses. Its last check counts the queue's keys from
SQL.

Here it runs a lighter pass, six signups, on database 0, in memory; `redis.memory_keys(0)` is
what reads it. Run natively, as CI does, it sends twelve on database 8, where the queue
survives a restart and its keys are rows in `redis.keys` (`JOB_QUEUE_DB` picks the database).

```js
const queue = new Queue("emails", { connection });
await queue.addBulk(signups.map((data) => ({
  name: "welcome",
  data,
  opts: { attempts: 3, backoff: { type: "exponential", delay: 100 } },
})));
await queue.add("reminder", { to: "late@example.com" }, { delay: 1500 });
new Worker("emails", deliver, { connection, concurrency: 4 });
```

```console
$ node run.mjs job-queue
```

```text
▶ job-queue
  ✓ every job completed  ({"completed":7,"failed":0,"delayed":0,"waiting":0,"active":0,"paused":0})
  ✓ the transient failures were retried, with backoff  (2 retried)
  ✓ the delayed job waited for its delay  (1502 ms)
  ✓ a crashed worker's job was taken over and finished  (invoiced 1001)
  ✓ the queue's state is in database 0, in memory, read from SQL  (12 keys, 0 lists)
```

### Sessions and a cache for a web app

An Express app with [express-session](https://github.com/expressjs/session) and
[connect-redis](https://github.com/tj/connect-redis) for its sessions, on database 8, and a
cache-aside layer on database 0. A user signs in, the app is redeployed and they stay signed
in, the second request for the product list is a cache hit, and "who is signed in" is a SQL
query over the sessions, joined with the users table.

```js
app.use(session({
  store: new RedisStore({ client: new IORedis({ db: 8 }), prefix: "sess:" }),
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
}));
```

```console
$ node run.mjs web-sessions
```

```text
▶ web-sessions
  ✓ signing in sets a session cookie
  ✓ the session knows who is signed in  (ada@example.com)
  ✓ the second request is served from the cache  (MISS, then HIT)
  ✓ the session outlives the app
  ✓ SQL lists the signed-in users, joined with app_users  (ada@example.com)
  ✓ signing out deletes the session
```

The query behind that check. The showcase signs Ada out at the end, so run afterwards it finds
nobody signed in.

```sql
SELECT u.email, s.expires_at
FROM redis.strings s
JOIN app_users u ON u.id = (s.value::jsonb ->> 'userId')::int
WHERE s.db = 8 AND s.key LIKE 'sess:%';
```

### Rate limiting and live updates across instances

Two instances of one service, as a load balancer would run them. An API limit of five
requests a minute with [express-rate-limit](https://github.com/express-rate-limit/express-rate-limit)
and [rate-limit-redis](https://github.com/express-rate-limit/rate-limit-redis) holds across
both; [Socket.IO](https://socket.io) with its
[Redis adapter](https://socket.io/docs/v4/redis-adapter/) delivers an order update sent on one
instance to a client connected to the other.

```js
app.use("/api", rateLimit({
  windowMs: 60_000,
  limit: 5,
  store: new RedisStore({ sendCommand: (...args) => redis.call(...args) }),
}));
const io = new Server(http, { adapter: createAdapter(pub, sub) });
io.to("order:1001").emit("status", { order: 1001, status: "shipped" });
```

```console
$ node run.mjs rate-limit-realtime
```

```text
▶ rate-limit-realtime
  ✓ the limit is shared: the 6th and 7th requests are refused on either instance  (200,200,200,200,200,429,429)
  ✓ instance a sees the watchers connected to both  (2 sockets)
  ✓ an update sent on instance a reaches the client on instance b  ({"order":1001,"status":"shipped"})
```

### An order pipeline on streams

Checkout appends fifty orders to a stream; three fulfilment workers share them through a
consumer group. One dies with five orders read and not acknowledged; a janitor takes them over
with `XAUTOCLAIM`; processing is idempotent, so an order delivered twice is counted once; and
finance reads the revenue per region from SQL.

```js
const res = await redis.xreadgroup("GROUP", "fulfilment", consumer,
  "COUNT", 5, "BLOCK", 200, "STREAMS", "orders", ">");
// … fulfil each order, then
await redis.xack("orders", "fulfilment", id);

// orders a dead consumer left pending for over 500 ms
const [, claimed] = await redis.xautoclaim("orders", "fulfilment", "janitor", 500, "0-0");
```

```console
$ node run.mjs order-pipeline
```

```text
▶ order-pipeline
  ✓ worker-c's orders are left pending when it dies  (5 read, 5 pending)
  ✓ XAUTOCLAIM hands them to a live consumer  (5 claimed)
  ✓ every order fulfilled once, nothing pending  (50 fulfilled)
  ✓ SQL reports the revenue per region, adding up to the orders  (us 1041.25, eu 1038, apac 924.5)
```

To run all four against your own server:

```bash
docker compose --profile showcases up --build --exit-code-from showcases
```

## Recipes

Shorter patterns, one Redis feature at a time.

### Connect from any client

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

=== "Node (ioredis)"

    ```js
    import IORedis from "ioredis";

    const redis = new IORedis("redis://:password@db.internal:6379/8");
    await redis.set("greeting", "hello");
    console.log(await redis.get("greeting")); // hello
    ```

=== "Elixir (Redix)"

    ```elixir
    {:ok, conn} = Redix.start_link("redis://:password@db.internal:6379/8")
    Redix.command!(conn, ["SET", "greeting", "hello"])
    Redix.command!(conn, ["GET", "greeting"]) # "hello"
    ```

### A cache that evicts

A memory database with an evicting policy works as a Redis cache does: keys carry a TTL, and
past `maxmemory` the least recently used go first.

```redis
127.0.0.1:6379> CONFIG SET maxmemory 16mb maxmemory-policy allkeys-lru
OK
127.0.0.1:6379> SET page:/pricing "<html>…</html>" EX 300
OK
127.0.0.1:6379> TTL page:/pricing
(integer) 300
```

Set `redis.maxmemory_policy` in `postgresql.conf` to keep the policy across restarts
([Configuration](./configuration.md)).

### Sessions joined with your tables

On a logged database, a session the application writes is a set of rows PostgreSQL can join
with its own tables. Here `app_users` is the application's table of users:

```sql
CREATE TABLE IF NOT EXISTS app_users (id int PRIMARY KEY, email text);
INSERT INTO app_users VALUES (42, 'ada@example.com') ON CONFLICT DO NOTHING;
```

```redis
127.0.0.1:6379[8]> HSET session:9f2c user_id 42 ip 203.0.113.7 agent "Firefox/131"
(integer) 3
127.0.0.1:6379[8]> EXPIRE session:9f2c 86400
(integer) 1
```

```sql
SELECT s.key AS session, u.email
FROM redis.hashes s
JOIN app_users u ON u.id = s.value::int
WHERE s.db = 8 AND s.field = 'user_id';
```

```text
   session    |      email
--------------+-----------------
 session:9f2c | ada@example.com
```

The views list only keys that are live, so a session past its expiry is not there, as it is
not for Redis. The whole session, as one JSON value:

```sql
SELECT type, value, expires_at > now() + interval '23 hours' AS for_a_day
FROM redis.keys WHERE db = 8 AND key = 'session:9f2c';
```

```text
 type |                             value                              | for_a_day
------+----------------------------------------------------------------+-----------
 hash | {"ip": "203.0.113.7", "agent": "Firefox/131", "user_id": "42"} | t
```

### Rate limiting with a script

A script runs atomically, so the counter and its expiry are set together:

```redis
127.0.0.1:6379[8]> EVAL "local n = redis.call('INCR', KEYS[1]) if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return n" 1 rate:203.0.113.7 60
(integer) 1
127.0.0.1:6379[8]> EVAL "local n = redis.call('INCR', KEYS[1]) if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return n" 1 rate:203.0.113.7 60
(integer) 2
127.0.0.1:6379[8]> TTL rate:203.0.113.7
(integer) 60
```

Refuse the request when the reply passes the limit. With `FUNCTION LOAD` the script can be
stored once and called by name with `FCALL`.

### A reliable job queue with lists

Workers move a job to a processing list as they take it, so a worker that dies leaves its job
there to be retried rather than losing it:

```redis
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

### Events with consumer groups

A stream keeps every event in order; each group reads it once, and an event a consumer read
but never acknowledged can be claimed by another:

```redis
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

### A leaderboard, ranked from SQL

```redis
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
SELECT member AS player, score,
       rank() OVER (ORDER BY score DESC) AS place
FROM redis.zsets WHERE db = 8 AND key = 'scores';
```

```text
 player | score | place
--------+-------+-------
 ada    |  1200 |     1
 linus  |  1100 |     2
 grace  |  1100 |     2
```

### Pub/sub messages straight into a table

A channel can be routed into a table, so what is published is also stored, ready to be
queried or replayed, whether or not anyone was subscribed:

```sql
SELECT redis.create_pubsub_table('public', 'order_events');
SELECT redis.route_publish('orders.created', 'public', 'order_events');
```

```redis
127.0.0.1:6379[8]> PUBLISH orders.created '{"order_id":1001,"total":42.5}'
(integer) 0
```

```sql
SELECT id, channel_text AS channel, payload_text::jsonb AS payload
FROM public.order_events;
```

```text
 id |    channel     |              payload
----+----------------+-----------------------------------
  1 | orders.created | {"total": 42.5, "order_id": 1001}
```

`PUBLISH` replies 0 because no client was subscribed; subscribers that are get the message as
usual. `redis.unroute_publish('orders.created')` stops the copying.

### Write from SQL, read from Redis

A row written with SQL is a key the next command sees, so a migration, a trigger or a batch
job can set what applications read:

```sql
INSERT INTO redis.kv_8 (key, value) VALUES ('feature:checkout-v2', 'on')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL;
```

```redis
127.0.0.1:6379[8]> GET feature:checkout-v2
"on"
```

### Change where a database lives

A database moves between policies while clients keep using it:

```sql
SELECT * FROM redis.policies() WHERE db IN (0, 1, 8);
```

```text
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

### A user per application

Each application gets only the commands and keys it needs:

```redis
127.0.0.1:6379[8]> ACL SETUSER billing on >billing-secret ~orders* ~invoice:* +@read +@write +@stream +select -@dangerous
OK
```

Logged in as `billing`, it can read the order stream and nothing else:

```console
$ redis-cli --user billing --pass billing-secret --no-auth-warning -n 8 XLEN orders
$ redis-cli --user billing --pass billing-secret --no-auth-warning -n 8 GET session:9f2c
$ redis-cli --user billing --pass billing-secret --no-auth-warning -n 8 FLUSHALL
```

```text
(integer) 2
(error) NOPERM No permissions to access a key
(error) NOPERM User billing has no permissions to run the 'flushall' command
```

With `redis.aclfile` set, `ACL SAVE` writes the users to it in Redis's format and the server
loads them at start ([Users](./configuration.md#users)).
