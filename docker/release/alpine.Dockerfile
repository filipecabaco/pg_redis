# Release build for musl: the same three targets as glibc.Dockerfile (dist, ext, runtime),
# built on and for the official postgres:<major>-alpine image.
#
# The official Alpine image compiles PostgreSQL from source into /usr/local and keeps the
# server headers, so its own pg_config is all pgrx needs; Alpine's postgresqlNN-dev packages
# would be a second, different PostgreSQL (and do not exist for every major).
#
#   docker build -f docker/release/alpine.Dockerfile --build-arg PG_MAJOR=17 --target runtime .
ARG PG_MAJOR=17

FROM postgres:${PG_MAJOR}-alpine AS build
ARG PG_MAJOR
ARG VERSION=0.0.0

RUN apk add --no-cache rust cargo rustfmt clang clang-dev llvm-dev musl-dev openssl-dev pkgconf make git

# The cc crate looks for `cc`; Alpine ships clang without it
ENV CC=clang

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
RUN PGRX=$(sed -nE 's/^pgrx = "=([0-9.]+)".*/\1/p' Cargo.toml) && \
    cargo install cargo-pgrx --version "=${PGRX}" --locked

COPY build.rs pg_redis.control ./
COPY vendor/ vendor/
COPY src/ src/
COPY sql/ sql/
COPY docs/command-coverage.md docs/command-coverage.md
COPY docker/release/package.sh /usr/local/bin/package-pg_redis

RUN package-pg_redis "${PG_MAJOR}" "${VERSION}" "$(command -v pg_config)"

FROM scratch AS dist
COPY --from=build /dist/ /

FROM scratch AS ext
COPY --from=build /out/ /

FROM postgres:${PG_MAJOR}-alpine AS runtime
COPY --from=ext / /
COPY docker/release/runtime.sh /tmp/runtime.sh
RUN sh /tmp/runtime.sh && rm /tmp/runtime.sh
COPY docker/release/entrypoint.sh /usr/local/bin/pg_redis-entrypoint.sh
COPY docker/release/initdb.sh /docker-entrypoint-initdb.d/10-pg_redis.sh
ENTRYPOINT ["pg_redis-entrypoint.sh"]
CMD ["postgres"]
EXPOSE 6379
