#!/bin/sh
# Postgres (and with it pg_redis's listener) first, then one tmux window: redis-cli on top,
# psql below (stacked, since SQL lines are long and wrap badly in half a terminal). The snapshot is taken at the redis-cli prompt, so visitors restore a
# server that is already up. When tmux exits the console runs this again; Postgres is still up.
if ! pg_isready -q -h /run/postgresql; then
  install -d -o postgres -g postgres /run/postgresql
  su postgres -s /bin/sh -c "pg_ctl -D $PGDATA -l /run/postgresql/server.log -w -s start"
fi
until redis-cli -p 6379 PING >/dev/null 2>&1; do sleep 0.2; done

tmux kill-server 2>/dev/null
exec tmux new-session -s demo -n demo \
  "redis-cli -p 6379; sh" \; \
  select-pane -T " redis-cli :6379 " \; \
  split-window -v "psql -h /run/postgresql -U postgres; sh" \; \
  select-pane -T " psql :5432 " \; \
  select-pane -t 0
