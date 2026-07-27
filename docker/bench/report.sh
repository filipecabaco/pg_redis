#!/usr/bin/env bash
# Benchmark pg_redis against Redis and print the result as markdown.
#
# Written for CI to post on a pull request, and for `mise run bench-report`
# locally. The two differ only in how a command reaches the servers, so both
# paths go through $BENCH_RUN and $PSQL_RUN rather than through two scripts
# that would drift apart.
#
#   BENCH_RUN  how to run redis-benchmark          (default: in the redis container)
#   PSQL_RUN   how to run psql against pg_redis    (default: in the postgres container)
#   PG_HOST    hostname pg_redis answers on        (default: postgres)
#   REDIS_HOST hostname the reference Redis answers on (default: redis)
#   REDIS_PORT port the reference Redis answers on (default: 6379)
set -uo pipefail

BENCH_RUN=${BENCH_RUN-docker compose exec -T redis}
PSQL_RUN=${PSQL_RUN-docker compose exec -T postgres psql -U postgres}
PG_HOST=${PG_HOST:-postgres}
PG_PORT=${PG_PORT:-6379}
REDIS_HOST=${REDIS_HOST:-redis}
REDIS_PORT=${REDIS_PORT:-6379}
PASSWORD=${PASSWORD:-testpass}

# Requests per second for one redis-benchmark run, or "n/a" if it did not
# complete — a refused connection must not silently read as a zero.
rps() {
  local host=$1 port=$2 test=$3 size=$4 requests=$5 out
  out=$($BENCH_RUN redis-benchmark -h "$host" -p "$port" -a "$PASSWORD" \
          -n "$requests" -c 50 -d "$size" -q -t "$test" 2>/dev/null \
        | tr '\r' '\n' \
        | sed -n 's/.*: \([0-9.]*\) requests per second.*/\1/p' \
        | head -1)
  if [[ -z "$out" ]]; then echo "n/a"; else printf '%.0f' "$out"; fi
}

# redis-benchmark builds `mylist` up as it goes and never clears it — including
# for `-t lrange`, which pushes the list it is about to read. Twenty thousand
# elements is past what `redis.mem_max_entries` allows a list table to hold, so
# the run ends in the OOM the server is supposed to give, and reports nothing.
# Clearing the key and keeping the list runs inside the table measures the
# command instead of the limit.
LIST_REQUESTS=4000

clear_list() {
  $BENCH_RUN redis-cli -h "$1" -p "$2" -a "$PASSWORD" DEL mylist > /dev/null 2>&1
}

# Thousands separators, so 51020 and 510200 are not mistaken for each other at
# a glance in a table.
fmt() {
  if [[ "$1" == "n/a" ]]; then echo "n/a"; else printf "%'d" "$1"; fi
}

ratio() {
  if [[ "$1" == "n/a" || "$2" == "n/a" || "$2" == "0" ]]; then echo "—"; else
    awk -v a="$1" -v b="$2" 'BEGIN { printf "%.2f×", a / b }'
  fi
}

storage_mode=$($PSQL_RUN -tAc "SELECT current_setting('redis.storage_mode')" 2>/dev/null | tr -d '[:space:]')
shmem=$($PSQL_RUN -tAc 'SHOW shared_memory_size' 2>/dev/null | tr -d '[:space:]')
max_val=$($PSQL_RUN -tAc "SELECT current_setting('redis.mem_max_entries')" 2>/dev/null | tr -d '[:space:]')

echo "<!-- pg_redis-bench -->"
echo "## Benchmarks"
echo
echo "\`storage_mode=${storage_mode:-unknown}\` · \`mem_max_entries=${max_val:-unknown}\` · \`shared_memory_size=${shmem:-unknown}\` · 50 clients · Redis 7 on the same runner as the reference."
echo

# The value-size sweep is the interesting one: redis-benchmark defaults to
# 3-byte values, which never leave the 64-byte inline slot, so the standard
# suite says nothing about the chunk pool that holds everything longer.
echo "### Value sizes"
echo
echo "| bytes | SET pg_redis | SET Redis | vs Redis | GET pg_redis | GET Redis | vs Redis |"
echo "|---:|---:|---:|---:|---:|---:|---:|"
for d in 3 64 65 512 4096 65536; do
  # Fewer requests as the payload grows: 20k × 64 KiB is 1.3 GB over the
  # loopback and tells you nothing 5k does not.
  n=20000
  [[ $d -ge 4096 ]] && n=5000
  ps=$(rps "$PG_HOST" "$PG_PORT" set "$d" "$n")
  rs=$(rps "$REDIS_HOST" "$REDIS_PORT" set "$d" "$n")
  pg=$(rps "$PG_HOST" "$PG_PORT" get "$d" "$n")
  rg=$(rps "$REDIS_HOST" "$REDIS_PORT" get "$d" "$n")
  echo "| $(fmt "$d") | $(fmt "$ps") | $(fmt "$rs") | $(ratio "$ps" "$rs") | $(fmt "$pg") | $(fmt "$rg") | $(ratio "$pg" "$rg") |"
done
echo

echo "### Commands (3-byte values)"
echo
echo "| command | pg_redis | Redis | vs Redis |"
echo "|---|---:|---:|---:|"
for t in ping set get incr lpush rpush lpop rpop sadd hset spop zadd zpopmin lrange mset; do
  n=20000
  case $t in
    # The pushes and lrange start from an empty list; the pops drain what the
    # pushes just made, so they are left alone.
    lpush | rpush | lrange)
      n=$LIST_REQUESTS
      clear_list "$PG_HOST" "$PG_PORT"
      clear_list "$REDIS_HOST" "$REDIS_PORT"
      ;;
    lpop | rpop) n=$LIST_REQUESTS ;;
  esac
  p=$(rps "$PG_HOST" "$PG_PORT" "$t" 3 "$n")
  r=$(rps "$REDIS_HOST" "$REDIS_PORT" "$t" 3 "$n")
  echo "| $t | $(fmt "$p") | $(fmt "$r") | $(ratio "$p" "$r") |"
done
echo
echo "<sub>Requests per second, higher is better. 20,000 requests each, 4,000 for the list commands — see the note in \`docker/bench/report.sh\`. GitHub-hosted runners are shared and noisy: read this as a smoke test for a collapse in throughput, not as a regression gate. Both servers run on the same runner, so the ratio travels better than the absolute numbers.</sub>"
