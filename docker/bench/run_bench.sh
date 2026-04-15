#!/bin/sh
# Benchmarks pg_redis using redis-benchmark — the standard Redis benchmarking tool.
# Mirrors the methodology used to benchmark Redis itself:
#   https://redis.io/docs/management/optimization/benchmarks/
#
# Usage:
#   ./run_bench.sh [host] [port]
#
# Defaults: host=localhost, port=6379

HOST="${1:-localhost}"
PORT="${2:-6379}"
PASSWORD="${3:-}"

# Set QUICK=1 for a fast smoke-test run (10k requests), default is 100k
N="${QUICK:+10000}"
N="${N:-100000}"

AUTH_FLAG=""
if [ -n "$PASSWORD" ]; then
  AUTH_FLAG="-a $PASSWORD"
fi

# redis-benchmark flags mirroring the Redis team's own methodology:
#   -n  total number of requests per command
#   -c  number of parallel connections
#   -P  pipeline depth (requests batched per network round-trip)
#   -q  quiet: one summary line per command
COMMON="-h $HOST -p $PORT $AUTH_FLAG -n $N -c 50"

separator() { printf '\n%s\n' "────────────────────────────────────────────────────"; }

echo ""
echo "  pg_redis benchmark  —  $HOST:$PORT"
echo "  redis-benchmark $(redis-benchmark --version 2>/dev/null || echo 'n/a')"
echo "  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

# ── 1. Baseline: pipeline=1, single connection ────────────────────────────────
separator
echo "  [1/4] Baseline  (c=50, n=100k, no pipeline)"
separator
redis-benchmark $COMMON -q \
    -t ping,set,get

# ── 2. Pipelining ─────────────────────────────────────────────────────────────
separator
echo "  [2/4] Pipelining  (c=50, n=100k, P=16)"
separator
redis-benchmark $COMMON -P 16 -q \
    -t ping,set,get

# ── 3. Logged vs Unlogged throughput comparison ───────────────────────────────
# We switch mode with SELECT then run SET/GET in that mode.
# redis-benchmark does not support SELECT natively so we use inline commands.
separator
echo "  [3/4] Logged tables  (SELECT 1)"
separator
redis-benchmark $COMMON -q \
    --dbnum 1 \
    -t set,get

separator
echo "  [4/4] Unlogged tables  (SELECT 0)"
separator
redis-benchmark $COMMON -q \
    --dbnum 0 \
    -t set,get

# ── 5. Hash operations ────────────────────────────────────────────────────────
separator
echo "  [5/5] Hash operations  (HSET / HGET)"
separator
redis-benchmark $COMMON -q \
    -t hset,hget

echo ""
echo "  Done."
echo ""
