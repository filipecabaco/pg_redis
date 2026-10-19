# Installation

!!! warning "No release is published yet"
    The Docker images and `install.sh` below are built by the release workflow and will
    work from the first tagged release. Until then, [build from source](#building-from-source)
    or run the repository's own Compose stack:

    ```bash
    git clone https://github.com/filipecabaco/pg_redis && cd pg_redis
    docker compose up
    ```

    This builds pg_redis from the checkout into a `postgres:17-alpine` image and starts one service,
    `postgres`, publishing PostgreSQL on port 5432 (user and password `postgres`) and the
    Redis listener on port 6379. The Redis password is `testpass` unless `PG_REDIS_PASSWORD`
    is set in your shell, and new connections start on database 8, a `logged` database:

    ```bash
    redis-cli -a testpass PING
    ```

## Requirements

- PostgreSQL 15, 16, 17 or 18.
- `pg_redis` in `shared_preload_libraries`, followed by a server restart. The background
  workers and the shared memory they use are set up when the server starts. On a server
  that has not preloaded it, `CREATE EXTENSION` and restoring a dump that contains the
  extension still work, with a warning that the Redis listener is not running.
- A superuser to run `CREATE EXTENSION pg_redis`. The extension is not marked trusted.
- Run `CREATE EXTENSION` in the database named by `redis.database` (`postgres` by default).
  The workers connect to that database, and the `redis.*` tables live in it.
- Room in `max_worker_processes`. pg_redis starts one background worker per `redis.workers`
  (4 by default) and no others. PostgreSQL's default `max_worker_processes` is 8, shared with
  parallel query, logical replication and other extensions. Workers past the limit are not
  started, and PostgreSQL logs `too many background workers`. Set it to at least
  `redis.workers`, plus what else on the server uses background workers, plus any you plan to
  add with `redis.add_workers()`. Changing it needs a restart.
- A server where you can add a third-party library to `shared_preload_libraries`. That covers
  PostgreSQL you run yourself, on a VM, in Docker or on Kubernetes. Hosted PostgreSQL
  services that do not allow preloading third-party extensions cannot run pg_redis.

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
      # without it, the Redis port only listens inside the container
      PG_REDIS_PASSWORD: change-me
    ports: ["5432:5432", "6379:6379"]
```

| Variable | Effect |
|----------|--------|
| `PG_REDIS_PASSWORD` | Listen on `0.0.0.0` and require `AUTH` with this password. If unset, the listener stays on loopback. Read on every start. |
| `PG_REDIS_PORT` | The listener's port, `6379` by default |

The image adds `pg_redis` to the sample config that `initdb` copies, so it applies to new
clusters. A volume initialised by another image keeps its own `postgresql.conf`. Add
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

The glibc builds are made on Ubuntu 22.04, so they run on it, Debian 12 and anything newer, or on musl. Each tarball records the oldest glibc it needs and the script checks it. Pass `--pg-config` to choose
between several installed versions, `--version v0.1.0` to pin a release, and `--dry-run` to see
what it would do. On anything else, build from source.

## Building from source

### Requirements

- PostgreSQL 15, 16, 17, or 18, with its server development headers and `pg_config`
- A C toolchain, clang and libclang (pgrx generates its bindings with bindgen, and
  `build.rs` compiles the vendored Lua from C), OpenSSL headers, pkg-config and git
- Rust stable from [rustup](https://rustup.rs). The crate uses the 2024 edition, which
  distribution packages of `rustc` are often too old for.
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx), at the exact
  version pinned in `Cargo.toml`, because pgrx's generated bindings are tied to it

On Debian or Ubuntu, with the [PostgreSQL apt repository](https://wiki.postgresql.org/wiki/Apt)
configured (replace `17` with your major version):

```bash
sudo apt-get install -y build-essential clang libclang-dev libssl-dev pkg-config git \
  postgresql-server-dev-17
```

On Alpine, building against the official `postgres:<major>-alpine` image's own `pg_config`:

```bash
apk add rust cargo rustfmt clang clang-dev llvm-dev musl-dev openssl-dev pkgconf make git
export CC=clang   # Alpine ships clang without a `cc`, which build.rs looks for
```

### Steps

```bash
git clone https://github.com/filipecabaco/pg_redis && cd pg_redis

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

Restart PostgreSQL, then create the extension as a superuser, in the database `redis.database`
names (`postgres` unless you changed it):

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
