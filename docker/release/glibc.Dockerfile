# Release build for glibc: the extension packaged for one PostgreSQL major, as three targets.
#
#   dist     FROM scratch, holding one tarball (lib/ + extension/) for install.sh
#   ext      FROM scratch, holding the files at their absolute paths in the official
#            postgres:<major> Debian images:  COPY --from=<ext image> / /
#   runtime  postgres:<major>-<RUN_DIST> with the extension preloaded and created on first start
#
# The build runs on Ubuntu 22.04 against apt.postgresql.org (PGDG), the same packages the
# official Debian images are made from, so the .so has the same paths and ABI, while linking
# against glibc 2.35: older than Debian 12's 2.36, so one tarball covers Ubuntu 22.04+ and
# Debian 12+. (PGDG has retired bullseye, so the official bullseye images can no longer
# install server headers.)
#
# Driven by .github/workflows/release.yml; locally:
#   docker build -f docker/release/glibc.Dockerfile --build-arg PG_MAJOR=17 --target runtime .
ARG PG_MAJOR=17
ARG RUN_DIST=bookworm

FROM ubuntu:22.04 AS build
ARG PG_MAJOR
ARG VERSION=0.0.0
ENV DEBIAN_FRONTEND=noninteractive

# postgresql-common carries PGDG's own repository setup script. libclang is for pgrx's
# bindgen; Lua and Redis's Lua libraries are compiled by build.rs.
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-common ca-certificates curl gnupg && \
    /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y && \
    apt-get install -y --no-install-recommends \
      build-essential clang libclang-dev libssl-dev pkg-config git \
      "postgresql-server-dev-${PG_MAJOR}" && \
    rm -rf /var/lib/apt/lists/*
ENV PATH=/usr/lib/postgresql/${PG_MAJOR}/bin:$PATH

# Ubuntu's rustc is far older than edition 2024 needs
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
# cargo-pgrx at the exact version Cargo.toml pins: its generated bindings are tied to it
RUN PGRX=$(sed -nE 's/^pgrx = "=([0-9.]+)".*/\1/p' Cargo.toml) && \
    cargo install cargo-pgrx --version "=${PGRX}" --locked

COPY build.rs pg_redis.control ./
COPY vendor/ vendor/
COPY src/ src/
COPY sql/ sql/
# `COMMAND COUNT` is derived from the coverage page, so the build embeds it
COPY docs/command-coverage.md docs/command-coverage.md
COPY docker/release/package.sh /usr/local/bin/package-pg_redis

RUN package-pg_redis "${PG_MAJOR}" "${VERSION}" "$(command -v pg_config)"

FROM scratch AS dist
COPY --from=build /dist/ /

FROM scratch AS ext
COPY --from=build /out/ /

FROM postgres:${PG_MAJOR}-${RUN_DIST} AS runtime
COPY --from=ext / /
COPY docker/release/runtime.sh /tmp/runtime.sh
RUN sh /tmp/runtime.sh && rm /tmp/runtime.sh
COPY docker/release/entrypoint.sh /usr/local/bin/pg_redis-entrypoint.sh
COPY docker/release/initdb.sh /docker-entrypoint-initdb.d/10-pg_redis.sh
ENTRYPOINT ["pg_redis-entrypoint.sh"]
CMD ["postgres"]
EXPOSE 6379
