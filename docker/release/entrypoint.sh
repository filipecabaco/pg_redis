#!/bin/sh
# Writes the Redis listener's settings from the environment on every start, then hands over to
# the official image's entrypoint.
#
#   PG_REDIS_PASSWORD  set: listen on 0.0.0.0 with this password, so a published port works
#                      unset: loopback only, the extension's own default
#   PG_REDIS_PORT      the listener's port (default 6379)
#
# The Redis port does not go through pg_hba.conf, so it is never exposed without a password.
set -eu
conf=/etc/pg_redis/pg_redis.conf

quote() { printf "'%s'" "$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e "s/'/''/g")"; }

if [ -w /etc/pg_redis ]; then
  {
    if [ -n "${PG_REDIS_PASSWORD:-}" ]; then
      echo "redis.listen_address = '0.0.0.0'"
      echo "redis.password = $(quote "$PG_REDIS_PASSWORD")"
    fi
    if [ -n "${PG_REDIS_PORT:-}" ]; then
      echo "redis.port = $(quote "$PG_REDIS_PORT")"
    fi
  } > "$conf"
  chmod 600 "$conf"
  [ "$(id -u)" = 0 ] && chown postgres:postgres "$conf"
elif [ -n "${PG_REDIS_PASSWORD:-}" ]; then
  echo "pg_redis: /etc/pg_redis is not writable by uid $(id -u); PG_REDIS_PASSWORD ignored" >&2
fi

exec docker-entrypoint.sh "$@"
