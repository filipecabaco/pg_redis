# Development

## Running tests

There are three layers and they cover different code. Running one is not
running the others:

| Layer | Command | What it covers |
|-------|---------|----------------|
| Unit | `mise run test-unit` | Parsing, key layout, size limits, the hash — no PostgreSQL |
| pgrx | `mise run test-pg` | The SPI path, against a PostgreSQL pgrx builds itself |
| End-to-end | `mise run e2e` | A live server over the Redis protocol, in one storage mode |

The gap between the second and third is the one to know about. `cargo pgrx
test` starts a throwaway cluster **without** `shared_preload_libraries`, so
memory mode — the default storage mode, and the whole of `src/mem.rs` — is
invisible to it. Only the end-to-end suite reaches that code, and only when the
server it runs against is configured for it:

```bash
mise run e2e                              # storage_mode=auto
PG_REDIS_STORAGE_MODE=memory mise run e2e # the shared-memory backend
```

CI runs both. Please do the same for anything touching `mem.rs`.

### Assertions

`cargo pgrx init --pgN download` builds PostgreSQL with `--enable-cassert`,
which distribution packages do not. Misuse of a PostgreSQL internal can pass
against a system package and fail only under pgrx, and an assertion failure
takes the backend down with it, so unrelated tests fail alongside the real one.
If a test fails in CI and passes locally, suspect this first —
[Handoff](handoff.md#ci-runs-against-an-assert-enabled-postgresql) has a recipe
for reproducing it.

### Byte-exact assertions

`Bun.RedisClient` encodes command arguments as UTF-8, so it cannot send a
`0xff` byte and reports 10 bytes for a 7-byte value. Anything asserting on
exact bytes or lengths has to go through the `RawRedis` helper at the top of
`docker/e2e/index.test.ts`.

## Continuous integration

`.github/workflows/ci.yml` is six jobs. Two are trivial and gate the rest, so a
formatting mistake costs seconds rather than a full matrix:

```
Workflows ─┐
Format    ─┴─> Clippy
               Test (pg15, pg16, pg17)
               Build image ──> E2E (auto), E2E (memory)
```

`Build image` exists because both end-to-end legs need the same container. When
they each built it they started together, so neither could reuse the other's
layer cache and both paid full price; now it is built once and the legs restore
the warm cache.

The pgrx version comes from `Cargo.toml` — CI reads it from there rather than
keeping its own copy, because the auto-update workflow's token is not allowed
to rewrite a workflow file.

## Local development

```bash
# Start a local postgres with the extension hot-reloaded
mise run run
# equivalent to:
cargo pgrx run pg17

# Inside the psql session:
CREATE EXTENSION pg_redis;

# In another terminal:
redis-cli -p 6379 ping
```

Format and lint:

```bash
mise run fmt    # cargo fmt
mise run lint   # cargo clippy -D warnings
```

## Schema

All tables live in the `redis` schema. Slots 0–7 use `UNLOGGED` tables; slots 8–15 use WAL-logged tables:

```sql
CREATE SCHEMA redis;

-- Key-value (pattern repeats for db 0–15)
CREATE UNLOGGED TABLE redis.kv_0 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE redis.kv_8 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);

-- Hash (same pattern)
CREATE UNLOGGED TABLE redis.hash_0 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE redis.hash_8 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, field));
```

All tables are queryable with standard SQL, joinable with your application data, and subject to normal PostgreSQL access control.
