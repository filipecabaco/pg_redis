# Showcases

Four production-style applications built on the Redis libraries teams already run, used
unchanged against pg_redis. Each runs a scenario and checks what happened, failures and
recovery included, and exits non-zero if a check fails.

| Showcase | Libraries | What it shows |
|----------|-----------|---------------|
| [`job-queue`](job-queue/app.mjs) | BullMQ | jobs over two workers, retries with exponential backoff, a delayed job, a crashed worker's job taken over; the queue read from SQL |
| [`web-sessions`](web-sessions/app.mjs) | Express, express-session, connect-redis | sessions that outlive a redeploy, a cache-aside layer with TTLs, signed-in users listed by a SQL join |
| [`rate-limit-realtime`](rate-limit-realtime/app.mjs) | express-rate-limit, rate-limit-redis, Socket.IO, @socket.io/redis-adapter | a rate limit shared by two instances, an update sent on one instance reaching a client on the other |
| [`order-pipeline`](order-pipeline/app.mjs) | ioredis, streams | a consumer group of workers, one dying mid-batch, `XAUTOCLAIM` recovery, idempotent processing, revenue reported from SQL |

## Run them

Against the repo's stack, as CI does:

```sh
docker compose --profile showcases up --build --exit-code-from showcases
```

Against any pg_redis, from this directory:

```sh
npm ci
REDIS_HOST=db.internal REDIS_PASSWORD=... \
DATABASE_URL=postgres://postgres@db.internal/postgres \
node run.mjs                 # all four, or: node run.mjs job-queue
```

They use databases `0` (memory) and `8` (logged) and the SQL tables in the `redis` schema.
`SHOWCASE_SLOW` stretches their waits and lock times for a slow machine; the in-browser demo
on the site sets it, since it runs them on an emulated CPU.

## In the browser

The site's [Examples](https://filipecabaco.github.io/pg_redis/examples/) page runs every one of
these, and every recipe on it, in a PostgreSQL booted in the visitor's tab: the demo image
(`docker/demo`) carries this directory, and `docker/demo/warm.exs` records the files they read
so the page can preload them.
