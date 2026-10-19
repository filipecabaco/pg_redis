#!/bin/sh
# Turn an official postgres image with the extension copied in into the drop-in image.
set -eu

# initdb copies the sample config, so every new cluster preloads pg_redis. The include is
# rewritten by the entrypoint from PG_REDIS_* variables on every start. A cluster initialised
# before this image (an existing volume) keeps its own postgresql.conf and needs both lines.
find /usr -type f -name postgresql.conf.sample | while read -r sample; do
  printf '\n%s\n%s\n%s\n' \
    "# Added by the pg_redis image" \
    "shared_preload_libraries = 'pg_redis'" \
    "include_if_exists = '/etc/pg_redis/pg_redis.conf'" >> "$sample"
done

install -d -o postgres -g postgres -m 700 /etc/pg_redis
