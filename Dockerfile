FROM postgres:17-alpine AS builder

# Capture pg_config from the base image BEFORE apk adds any competing version.
# postgres:17-alpine builds from source and puts pg_config in /usr/local/bin.
RUN PG_CONFIG=$(which pg_config) && \
    echo "$PG_CONFIG" > /pg_config_path && \
    echo "Base image pg_config: $PG_CONFIG ($($PG_CONFIG --version))"

RUN apk add --no-cache \
    rust \
    cargo \
    rustfmt \
    clang \
    clang-dev \
    llvm \
    llvm-dev \
    musl-dev \
    openssl-dev \
    postgresql17-dev \
    make \
    git

WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
COPY src/ src/
COPY sql/ sql/
COPY pg_redis.control pg_redis.control

# Install cargo-pgrx matching the project version
RUN cargo install cargo-pgrx --version "=0.16.1" --locked

RUN PG_CONFIG=$(cat /pg_config_path) && \
    echo "Building with pg_config: $PG_CONFIG ($($PG_CONFIG --version))" && \
    cargo pgrx init --pg17 "$PG_CONFIG"

RUN PG_CONFIG=$(cat /pg_config_path) && \
    cargo pgrx package --pg-config "$PG_CONFIG" --features pg17 && \
    grep -v '\\echo\|\\quit' /build/sql/pg_redis--0.0.0.sql \
      >> /build/target/release/pg_redis-pg17/usr/local/share/postgresql/extension/pg_redis--0.0.0.sql

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM postgres:17-alpine

COPY --from=builder /build/target/release/pg_redis-pg17/usr/ /usr/
