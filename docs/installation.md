# Installation

Every release is built for PostgreSQL 15–18 on Linux, amd64 and arm64, glibc and musl. Pick
whichever fits how you run Postgres. All three use the same build.

## Docker

The official `postgres` image with pg_redis preloaded and created on first start:

```yaml
services:
  db:
    image: ghcr.io/filipecabaco/pg_redis:17          # or 17-alpine, 18, 16, 15
    environment:
      POSTGRES_PASSWORD: postgres
      PG_REDIS_PASSWORD: change-me   # without it, the Redis port only listens inside the container
    ports: ["5432:5432", "6379:6379"]
```

| Variable | Effect |
|----------|--------|
| `PG_REDIS_PASSWORD` | Listen on `0.0.0.0` and require `AUTH` with this password. Unset, the listener stays on loopback. Read on every start. |
| `PG_REDIS_PORT` | The listener's port, `6379` by default |

The image adds `pg_redis` to the sample config that `initdb` copies, so it applies to new
clusters. A volume initialised by another image keeps its own `postgresql.conf`: add
`shared_preload_libraries = 'pg_redis'` there and run `CREATE EXTENSION pg_redis` once.

## Your own image

The `pg_redis-ext` images hold only the extension's files, at the paths the official images
use. Copy them into an image built on the matching official one:

```dockerfile
FROM postgres:17-bookworm
COPY --from=ghcr.io/filipecabaco/pg_redis-ext:17-bookworm / /
CMD ["postgres", "-c", "shared_preload_libraries=pg_redis"]
```

Use the `-alpine` tag on an Alpine base.

## An existing server

`install.sh` finds `pg_config`, downloads the matching build, checks its SHA-256 and copies it
into place. `--configure` also appends pg_redis to `shared_preload_libraries` over `psql`;
without it, nothing in your configuration changes.

```bash
curl -fsSL https://filipecabaco.github.io/pg_redis/install.sh | sh -s -- --configure
sudo systemctl restart postgresql
psql -c 'CREATE EXTENSION pg_redis'
```

The glibc builds are made on Ubuntu 22.04, so they run on it, Debian 12 and anything newer, or on musl; each tarball records the oldest glibc it needs and the script checks it. Pass `--pg-config` to choose
between several installed versions, `--version v0.1.0` to pin a release, and `--dry-run` to see
what it would do. On anything else, build from source.

## Building from source

### Requirements

- PostgreSQL 15, 16, 17, or 18
- Rust toolchain (stable)
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx), at the exact
  version pinned in `Cargo.toml` — pgrx's generated bindings are tied to it

### Steps

```bash
# Install cargo-pgrx at the pinned version (skip if already installed).
# Read it from Cargo.toml rather than copying it, so this cannot drift when
# the dependency is bumped.
PGRX=$(sed -nE 's/^pgrx = "=([0-9.]+)".*/\1/p' Cargo.toml)
cargo install cargo-pgrx --version "=${PGRX}" --locked

# Point pgrx at your postgres installation
cargo pgrx init --pg17 $(which pg_config)

# Build and install into the active PostgreSQL
cargo pgrx install --release --features pg17
```

## Enable the extension

Add to `postgresql.conf`:

```
shared_preload_libraries = 'pg_redis'
```

Restart PostgreSQL, then create the extension:

```sql
CREATE EXTENSION pg_redis;
```

## Verify workers are running

```sql
SELECT pid, application_name
FROM pg_stat_activity
WHERE backend_type LIKE 'pg_redis worker%';

-- Or use the built-in helper:
SELECT redis.worker_count();
```
