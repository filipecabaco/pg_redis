# The in-browser demo: pg_redis inside Postgres, with redis-cli and psql stacked in tmux.
# Built into a static site by snowglobe (https://github.com/filipecabaco/snowglobe), which boots
# this image on an emulated arm64 machine in the visitor's tab. `mise run demo` builds it.
#
# The base is the repo's own Dockerfile built for arm64 (pg_redis:alpine-arm64), so the demo runs
# exactly the extension the e2e suite tests.
ARG BASE=pg_redis:alpine-arm64
FROM ${BASE}

# redis-cli comes with Alpine's redis package; the server it also installs is never started
RUN apk add --no-cache redis tmux

# A cluster made at build time and sized for a browser tab: one worker, small shared-memory
# tables, no durability work (the disk is the visitor's memory and goes away with the tab)
ENV PGDATA=/var/lib/postgresql/data
RUN install -d -o postgres -g postgres -m 700 "$PGDATA" && \
    su postgres -s /bin/sh -c "initdb -D $PGDATA -U postgres --auth=trust --encoding=UTF8 --no-locale" >/dev/null && \
    printf '%s\n' \
      "listen_addresses = ''" \
      "unix_socket_directories = '/run/postgresql'" \
      "max_connections = 12" \
      "shared_buffers = 16MB" \
      "dynamic_shared_memory_type = mmap" \
      "fsync = off" \
      "synchronous_commit = off" \
      "full_page_writes = off" \
      "wal_level = minimal" \
      "max_wal_senders = 0" \
      "autovacuum = off" \
      "jit = off" \
      "shared_preload_libraries = 'pg_redis'" \
      "redis.listen_address = '127.0.0.1'" \
      "redis.workers = 1" \
      "redis.maxmemory = 32MB" \
      >> "$PGDATA/postgresql.conf" && \
    install -d -o postgres -g postgres /run/postgresql && \
    su postgres -s /bin/sh -c "pg_ctl -D $PGDATA -w -s start && \
      psql -q -h /run/postgresql -U postgres -c 'CREATE EXTENSION pg_redis' && \
      pg_ctl -D $PGDATA -w -s stop"

COPY tmux.conf /root/.tmux.conf
COPY psqlrc /root/.psqlrc
# Inline argument hints overflow a narrow pane, and redis-cli's line editor then re-prints the
# line on every keystroke. (.redisclirc takes no comments, so this one lives here.)
COPY redisclirc /root/.redisclirc
COPY start.sh /usr/local/bin/start-demo

ENV LANG=C.UTF-8 HOME=/root TERM=xterm-256color
WORKDIR /root
CMD ["/usr/local/bin/start-demo"]

# Snapshot once both panes are up. No exercises: snowglobe waits for the ready text after each
# one, and tmux redraws the prompt rather than printing it, so they never finish. The warm
# pattern covers the same ground: the postgres database's files, which the SQL steps read.
LABEL snowglobe.title="pg_redis: Redis and SQL, one server" \
      snowglobe.ready="127.0.0.1:6379>" \
      snowglobe.memory="512" \
      snowglobe.warm="^/var/lib/postgresql/data/(base/5|global)/"
