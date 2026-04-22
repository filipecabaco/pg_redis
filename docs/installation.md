# Installation

## Requirements

- PostgreSQL 15, 16, 17, or 18
- Rust toolchain (stable)
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx) 0.18.0

## From source

```bash
# Install cargo-pgrx (skip if already installed)
cargo install cargo-pgrx --version "=0.18.0" --locked

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
