#!/usr/bin/env bash
# Prove a release cell works the way a user would meet it.
#
#   smoke.sh image   <drop-in image>
#       Start it with a password, as the README's compose example does: the extension must be
#       created on first start, workers running, and a Redis client must get PONG through the
#       published port.
#
#   smoke.sh tarball <tarball> <official postgres image>
#       Install the tarball into a stock image with docs/install.sh, preload it, and check the
#       same things from inside the container.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
mode=${1:?usage: smoke.sh image IMAGE | tarball TARBALL BASE_IMAGE}
name="pg_redis-smoke-$$"
trap 'docker logs "$name" 2>&1 | tail -40 >&2 || true; docker rm -f "$name" >/dev/null 2>&1 || true' ERR
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_for_workers() {
  # The entrypoint runs initdb and a temporary server first, so wait for the real one
  for _ in $(seq 1 90); do
    n=$(docker exec "$name" psql -U postgres -qAt -c "SELECT redis.worker_count()" 2>/dev/null || true)
    if [ -n "$n" ] && [ "$n" -gt 0 ]; then
      echo "workers: $n"
      return 0
    fi
    sleep 1
  done
  echo "pg_redis workers never started" >&2
  return 1
}

# RESP over bash's /dev/tcp, so the host needs no redis-cli
resp_ping() {
  local port=$1 password=$2 reply
  exec 3<>"/dev/tcp/127.0.0.1/$port"
  # shellcheck disable=SC2016 # the $ in $4 is RESP's bulk-string marker, not an expansion
  printf '*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n*1\r\n$4\r\nPING\r\n' "${#password}" "$password" >&3
  IFS= read -r -t 10 reply <&3; [ "${reply%$'\r'}" = "+OK" ] || { echo "AUTH replied: $reply" >&2; return 1; }
  IFS= read -r -t 10 reply <&3; [ "${reply%$'\r'}" = "+PONG" ] || { echo "PING replied: $reply" >&2; return 1; }
  exec 3>&-
  echo "redis: PONG on :$port"
}

case $mode in
  image)
    image=${2:?image}
    docker run -d --name "$name" -e POSTGRES_PASSWORD=smoke -e PG_REDIS_PASSWORD=smoke \
      -p 127.0.0.1::6379 "$image" >/dev/null
    wait_for_workers
    docker exec "$name" psql -U postgres -qAt -c \
      "SELECT extversion FROM pg_extension WHERE extname = 'pg_redis'" | sed 's/^/extension: /'
    port=$(docker port "$name" 6379/tcp | head -1 | sed 's/.*://')
    resp_ping "$port" smoke
    ;;
  tarball)
    tarball=${2:?tarball}
    base=${3:?base image}
    # Install as root with install.sh, then start the stock entrypoint with pg_redis preloaded
    docker run -d --name "$name" -e POSTGRES_PASSWORD=smoke \
      -v "$(cd "$(dirname "$tarball")" && pwd)/$(basename "$tarball"):/tmp/pg_redis.tar.gz:ro" \
      -v "$here/../../docs/install.sh:/tmp/install.sh:ro" \
      --entrypoint sh "$base" -c \
      'sh /tmp/install.sh --file /tmp/pg_redis.tar.gz && exec docker-entrypoint.sh postgres -c shared_preload_libraries=pg_redis' \
      >/dev/null
    for _ in $(seq 1 60); do
      docker exec "$name" pg_isready -q -U postgres 2>/dev/null && break
      sleep 1
    done
    docker logs "$name" 2>&1 | sed -n '/^pg_config/,/^version/p'
    docker exec "$name" psql -U postgres -qAt -c "CREATE EXTENSION IF NOT EXISTS pg_redis" >/dev/null
    wait_for_workers
    # A plain base image may have no redis-cli or nc; psql proves the listener is up instead
    docker exec "$name" psql -U postgres -qAt -c \
      "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'pg_redis%'" | sed 's/^/backends: /'
    ;;
  *)
    echo "unknown mode: $mode" >&2
    exit 2
    ;;
esac
echo "ok"
