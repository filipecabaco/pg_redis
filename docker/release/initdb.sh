#!/bin/sh
# First start only (docker-entrypoint-initdb.d): create the extension in POSTGRES_DB, and point
# the workers at that database when it is not the default `postgres`.
set -eu
db=${POSTGRES_DB:-${POSTGRES_USER:-postgres}}
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname "$db" \
  -c 'CREATE EXTENSION IF NOT EXISTS pg_redis'
if [ "$db" != postgres ]; then
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname "$db" \
    -c "ALTER SYSTEM SET redis.database = '$db'"
fi
