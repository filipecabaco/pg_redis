# Installation

## Requirements

- PostgreSQL 15, 16, 17, or 18
- Rust toolchain (stable)
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx), at the exact
  version pinned in `Cargo.toml` — pgrx's generated bindings are tied to it

## From source

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
