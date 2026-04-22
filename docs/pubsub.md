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

The target table must have `channel TEXT` and `payload TEXT` columns:

```sql
CREATE TABLE your_table (
    channel TEXT NOT NULL,
    payload TEXT NOT NULL
);
```

## How it works

- Routes are stored in `redis.pubsub_routes` and loaded into shared memory on startup. Lookups are lock-free when no routes are configured (atomic counter short-circuit).
- The table INSERT is dispatched fire-and-forget via the BGW dispatcher after in-memory pub/sub delivery completes. The Redis PUBLISH reply is sent immediately.
- Routes survive server restart (persisted in `redis.pubsub_routes`).
- Up to 64 routes can be active simultaneously.
