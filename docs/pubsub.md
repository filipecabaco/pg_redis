# Pub/Sub table routing

Any PUBLISH can be optionally routed to a PostgreSQL table, enabling [Supabase Realtime broadcast from database](https://supabase.com/docs/guides/realtime/broadcast) or any trigger-based integration. The extension is completely decoupled from Supabase — it just INSERTs rows.

## Setup

```sql
-- 1. Create a target table (id, channel, payload, inserted_at)
SELECT redis.create_pubsub_table('public', 'chat_messages');

-- 2. Route PUBLISH on 'chat' to that table
SELECT redis.route_publish('chat', 'public', 'chat_messages');

-- 3. Now any PUBLISH lands in the table too
-- redis-cli> PUBLISH chat "hello"

-- 4. Read it back
SELECT channel, payload, inserted_at FROM public.chat_messages;
```

## Functions

| Function | Description |
|----------|-------------|
| `redis.create_pubsub_table(schema, table)` | Create a routing target table with the required columns |
| `redis.route_publish(channel, schema, table)` | Route PUBLISH on `channel` to INSERT into `schema`.`table` |
| `redis.unroute_publish(channel)` | Remove the route for `channel` |

## Bring your own table

The target table must have `channel BYTEA` and `payload BYTEA` columns:

```sql
CREATE TABLE your_table (
    channel BYTEA NOT NULL,
    payload BYTEA NOT NULL
);
```

`BYTEA` is required, not merely recommended: channels and payloads are arbitrary
byte strings and `TEXT` cannot hold a NUL. A routed insert into a table whose
columns are the wrong type fails, and because the insert is fire-and-forget the
`PUBLISH` still succeeds — the failure is reported in the PostgreSQL log, not to
the publishing client. Check there if routed rows stop appearing.

## How it works

- Routes are stored in `redis.pubsub_routes` and loaded into shared memory on startup. Lookups are lock-free when no routes are configured (atomic counter short-circuit).
- The table INSERT is dispatched fire-and-forget via the BGW dispatcher after in-memory pub/sub delivery completes. The Redis PUBLISH reply is sent immediately.
- Routes survive server restart (persisted in `redis.pubsub_routes`).
- Up to 64 routes can be active simultaneously.

## Size limits

Pub/sub runs entirely through fixed-size shared-memory slots, so the following
are hard caps:

| | Limit | Over the limit |
|---|---|---|
| Channel / pattern name | 255 bytes | `SUBSCRIBE`, `PSUBSCRIBE` and `PUBLISH` return an error |
| Message payload | 512 bytes | `PUBLISH` returns an error |
| Concurrent subscriber connections | 256 | `SUBSCRIBE` returns "max pub/sub subscribers reached" |
| Channels + patterns per connection | 16 each | Further subscriptions are ignored |
| Undelivered messages queued per subscriber | 256 | Further messages are dropped for that subscriber |

Names and payloads over the limit are **rejected, not truncated**. A truncated
subscription would never match a publish to the same name, leaving the client
waiting on a channel it believed it was subscribed to.

Use the table routing above when payloads exceed 512 bytes.

The slot table costs roughly 66 MiB of shared memory and is allocated at server
start whether or not pub/sub is used.
