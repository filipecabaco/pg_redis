# Redis's own test suite, run against pg_redis as an external server.
#
# The suite is fetched at a pinned tag rather than vendored. 7.2 is the last
# BSD-licensed 7.x line, and its tests are what Redis itself gates releases on.
# Only the Tcl harness is needed: in external mode it spawns no redis-server.
FROM oven/bun:1-debian

RUN apt-get update \
 && apt-get install -y --no-install-recommends tcl procps ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

ARG REDIS_VERSION=7.2.5
RUN curl -fsSL "https://github.com/redis/redis/archive/refs/tags/${REDIS_VERSION}.tar.gz" \
      | tar xz -C /opt \
 && mv "/opt/redis-${REDIS_VERSION}" /opt/redis

WORKDIR /opt/redis

COPY run.ts skip.txt baseline.json ./acceptance/

CMD ["bun", "run", "acceptance/run.ts"]
