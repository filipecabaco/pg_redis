#!/bin/sh
set -e

HOST="${1:-localhost}"
PORT="${2:-6379}"
PASSWORD="${3:-testpass}"
export REDISCLI_AUTH="$PASSWORD"

PASS=0
FAIL=0

run() {
    local desc="$1"; shift
    local expected="$1"; shift
    local actual
    actual=$(redis-cli -h "$HOST" -p "$PORT" "$@" 2>&1)
    if [ "$actual" = "$expected" ]; then
        echo "  PASS  $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $desc"
        echo "        expected: $expected"
        echo "        got:      $actual"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo "=== pg_redis e2e test suite ==="
echo ""

apk add --no-cache postgresql-client >/dev/null 2>&1

psql_run() {
    local desc="$1"
    local expected="$2"
    local sql="$3"
    local actual
    actual=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -t -A -c "$sql" 2>&1)
    if [ "$actual" = "$expected" ]; then
        echo "  PASS  $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $desc"
        echo "        expected: $expected"
        echo "        got:      $actual"
        FAIL=$((FAIL + 1))
    fi
}

# ── Authentication ─────────────────────────────────────────────────────────────
echo "--- Authentication ---"

# PING is allowed before AUTH (standard Redis behaviour), but data commands must be rejected
noauth_ping=$(env -u REDISCLI_AUTH redis-cli -h "$HOST" -p "$PORT" PING 2>&1)
if [ "$noauth_ping" = "PONG" ]; then
    echo "  PASS  PING before AUTH returns PONG (allowed)"
    PASS=$((PASS + 1))
else
    echo "  FAIL  PING before AUTH should return PONG (got: $noauth_ping)"
    FAIL=$((FAIL + 1))
fi

noauth_get=$(env -u REDISCLI_AUTH redis-cli -h "$HOST" -p "$PORT" GET somekey 2>&1)
if echo "$noauth_get" | grep -q "NOAUTH"; then
    echo "  PASS  GET before AUTH returns NOAUTH"
    PASS=$((PASS + 1))
else
    echo "  FAIL  GET before AUTH should return NOAUTH (got: $noauth_get)"
    FAIL=$((FAIL + 1))
fi

# Wrong password must be rejected
wrongpass_out=$(env -u REDISCLI_AUTH redis-cli -h "$HOST" -p "$PORT" AUTH wrongpassword 2>&1)
if echo "$wrongpass_out" | grep -q "WRONGPASS"; then
    echo "  PASS  AUTH with wrong password returns WRONGPASS"
    PASS=$((PASS + 1))
else
    echo "  FAIL  AUTH with wrong password should return WRONGPASS (got: $wrongpass_out)"
    FAIL=$((FAIL + 1))
fi

# Correct password must succeed
run "AUTH with correct password" "OK" AUTH "$PASSWORD"

# ── BGW SPI sanity ─────────────────────────────────────────────────────────────
# These run first because a missing BackgroundWorker::transaction() wrapper
# causes a segfault on the FIRST SPI call, crashing the BGW and making every
# subsequent command return "Connection refused". PING alone does not trigger
# SPI and therefore does not catch this class of bug.
echo ""
echo "--- BGW SPI sanity (SET/GET roundtrip) ---"
run "SET reaches SPI without crash"  "OK"        SET bgw_sanity_key bgw_sanity_val
run "GET round-trips through SPI"    "bgw_sanity_val" GET bgw_sanity_key
run "DEL cleans up sanity key"       "1"         DEL bgw_sanity_key

# ── Connection ─────────────────────────────────────────────────────────────────
echo ""
echo "--- Connection ---"
run "PING"            "PONG"    PING
run "PING with arg"   "hello"   PING hello
run "ECHO"            "world"   ECHO world
run "CLIENT SETNAME"  "OK"      CLIENT SETNAME myapp

# ── Key-value ──────────────────────────────────────────────────────────────────
echo ""
echo "--- Key-Value ---"
run "SET"             "OK"      SET mykey myvalue
run "GET existing"    "myvalue" GET mykey
run "GET missing"     ""        GET nokey

run "SET with EX"     "OK"      SET ttlkey ttlval EX 100
run "TTL positive"    "100"     TTL ttlkey
pttl_val=$(redis-cli -h "$HOST" -p "$PORT" PTTL ttlkey 2>&1)
if [ "$pttl_val" -ge 99000 ] && [ "$pttl_val" -le 100000 ] 2>/dev/null; then
    echo "  PASS  PTTL positive"
    PASS=$((PASS + 1))
else
    echo "  FAIL  PTTL positive (expected ~100000, got $pttl_val)"
    FAIL=$((FAIL + 1))
fi

# overwrite
run "SET overwrites"  "OK"      SET mykey newvalue
run "GET after SET"   "newvalue" GET mykey

run "SETEX"           "OK"      SETEX exkey 50 exval
run "GET SETEX"       "exval"   GET exkey

run "PSETEX"          "OK"      PSETEX pexkey 50000 pexval
run "GET PSETEX"      "pexval"  GET pexkey

# ── MSET / MGET ────────────────────────────────────────────────────────────────
echo ""
echo "--- MSET / MGET ---"
run "MSET"            "OK"      MSET mk1 mv1 mk2 mv2 mk3 mv3
run "MGET all"        "mv1
mv2
mv3"                           MGET mk1 mk2 mk3
run "MGET with nil"   "mv1

mv3"                           MGET mk1 missing mk3

# ── DEL / EXISTS ──────────────────────────────────────────────────────────────
echo ""
echo "--- DEL / EXISTS ---"
run "EXISTS present"  "1"       EXISTS mykey
run "EXISTS missing"  "0"       EXISTS nokey
run "EXISTS multi"    "2"       EXISTS mk1 mk2 missing

run "DEL single"      "1"       DEL mk1
run "DEL missing"     "0"       DEL mk1
run "DEL multi"       "2"       DEL mk2 mk3

# ── Expiry ────────────────────────────────────────────────────────────────────
echo ""
echo "--- Expiry ---"
run "SET for expiry"  "OK"      SET expkey expval
run "EXPIRE"          "1"       EXPIRE expkey 9999
run "TTL after EXPIRE" "9999"   TTL expkey
run "PERSIST"         "1"       PERSIST expkey
run "TTL after PERSIST" "-1"    TTL expkey
run "TTL missing key" "-2"      TTL nonexistent

run "PEXPIRE"         "1"       PEXPIRE exkey 9999000
pttl_val2=$(redis-cli -h "$HOST" -p "$PORT" PTTL exkey 2>&1)
if [ "$pttl_val2" -ge 9990000 ] && [ "$pttl_val2" -le 9999000 ] 2>/dev/null; then
    echo "  PASS  PTTL"
    PASS=$((PASS + 1))
else
    echo "  FAIL  PTTL (expected ~9999000, got $pttl_val2)"
    FAIL=$((FAIL + 1))
fi
run "EXPIRETIME"      "-1"      EXPIRETIME mykey
run "PEXPIRETIME"     "-1"      PEXPIRETIME mykey

# ── TTL expiry deletion ───────────────────────────────────────────────────────
echo ""
echo "--- TTL expiry deletion ---"

# Lazy deletion: GET on an expired key must return nil AND remove the row
run "SET key with 1s TTL"          "OK"  SET expiring_key expiring_val EX 1
run "GET before expiry returns val" "expiring_val" GET expiring_key
sleep 2
run "GET after expiry returns nil"  ""   GET expiring_key

# Confirm the row is physically gone via psql (not just filtered)
psql_run "expired key is physically deleted after GET" \
    "0" \
    "SELECT count(*)::int FROM redis.kv_1 WHERE key = 'expiring_key'"

# Active expiry scan: keys expire and get cleaned up even without a GET
run "SET key for active scan"       "OK"  SET active_scan_key active_val EX 1
sleep 3
psql_run "active scan deletes without GET" \
    "0" \
    "SELECT count(*)::int FROM redis.kv_1 WHERE key = 'active_scan_key'"

# TTL returns -2 after key is gone
run "TTL on deleted key returns -2" "-2"  TTL expiring_key

# Non-expired key survives both lazy and active deletion
run "SET key with long TTL"         "OK"  SET long_ttl_key val EX 9999
sleep 2
run "GET long TTL key still present" "val" GET long_ttl_key
psql_run "long TTL key row still exists" \
    "1" \
    "SELECT count(*)::int FROM redis.kv_1 WHERE key = 'long_ttl_key'"
run "DEL long TTL key"              "1"   DEL long_ttl_key

# ── Hash ──────────────────────────────────────────────────────────────────────
echo ""
echo "--- Hash ---"
run "HSET single"     "1"       HSET myhash f1 v1
run "HGET existing"   "v1"      HGET myhash f1
run "HGET missing"    ""        HGET myhash nof

run "HSET multiple"   "2"       HSET myhash f2 v2 f3 v3
run "HGETALL"         "f1
v1
f2
v2
f3
v3"                             HGETALL myhash

run "HDEL single"     "1"       HDEL myhash f1
run "HDEL missing"    "0"       HDEL myhash f1
run "HGETALL after del" "f2
v2
f3
v3"                             HGETALL myhash

run "HDEL multi"      "2"       HDEL myhash f2 f3

# ── SELECT (mode switching) ───────────────────────────────────────────────────
# SELECT isolation must be tested in a single connection since SELECT state is
# per-connection and does not persist across separate redis-cli invocations.
echo ""
echo "--- SELECT (logged / unlogged) ---"

# Clean up keys from both dbs before the isolation test.
printf 'SELECT 0\r\nDEL ulkey\r\nSELECT 1\r\nDEL ulkey\r\n' | redis-cli -h "$HOST" -p "$PORT" >/dev/null 2>&1

select_out=$(printf 'SELECT 0\r\nSET ulkey ulval\r\nGET ulkey\r\nSELECT 1\r\nGET ulkey\r\n' \
    | redis-cli -h "$HOST" -p "$PORT" 2>&1)

check_line() {
    local desc="$1" expected="$2" line_n="$3"
    actual=$(echo "$select_out" | sed -n "${line_n}p")
    if [ "$actual" = "$expected" ]; then
        echo "  PASS  $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $desc"
        echo "        expected: $expected"
        echo "        got:      $actual"
        FAIL=$((FAIL + 1))
    fi
}

check_line "SELECT unlogged"                                   "OK"    1
check_line "SET in unlogged"                                   "OK"    2
check_line "GET from unlogged"                                 "ulval" 3
check_line "SELECT logged"                                     "OK"    4
check_line "GET from logged (nil, different table)"            ""      5

# ── SELECT isolation: GUC default must not bleed into per-connection state ────
echo ""
echo "--- SELECT isolation (table-level) ---"

printf 'SELECT 0\r\nDEL isolation_ul\r\nSELECT 1\r\nDEL isolation_ul\r\nDEL isolation_lg\r\n' | redis-cli -h "$HOST" -p "$PORT" >/dev/null 2>&1

iso_out=$(printf 'SELECT 0\r\nSET isolation_ul isolation_val\r\nGET isolation_ul\r\nSELECT 1\r\nGET isolation_ul\r\nSET isolation_lg isolation_val\r\nGET isolation_lg\r\nSELECT 0\r\nGET isolation_lg\r\nSELECT 1\r\nDEL isolation_lg\r\n' \
    | redis-cli -h "$HOST" -p "$PORT" 2>&1)

check_iso() {
    local desc="$1" expected="$2" line_n="$3"
    actual=$(echo "$iso_out" | sed -n "${line_n}p")
    if [ "$actual" = "$expected" ]; then
        echo "  PASS  $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $desc"
        echo "        expected: $expected"
        echo "        got:      $actual"
        FAIL=$((FAIL + 1))
    fi
}

check_iso "SELECT 0 returns OK"                   "OK"            1
check_iso "SET in unlogged after SELECT 0"        "OK"            2
check_iso "GET from unlogged"                     "isolation_val" 3
check_iso "SELECT 1 returns OK"                   "OK"            4
check_iso "GET unlogged key from logged (nil)"    ""              5
check_iso "SET in logged after SELECT 1"          "OK"            6
check_iso "GET from logged"                       "isolation_val" 7
check_iso "SELECT 0 again"                        "OK"            8
check_iso "GET logged key from unlogged (nil)"    ""              9
check_iso "SELECT 1 to clean logged"              "OK"            10
check_iso "DEL logged key"                        "1"             11

# ── Worker management ─────────────────────────────────────────────────────────
echo ""
echo "--- Worker management ---"

psql_run "worker_count returns positive" \
    "t" \
    "SELECT redis.worker_count() > 0"

BEFORE=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -t -A -c "SELECT redis.worker_count()" 2>&1)

psql_run "add_workers(2) returns 2" \
    "2" \
    "SELECT redis.add_workers(2)"

sleep 1

AFTER=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -t -A -c "SELECT redis.worker_count()" 2>&1)

if [ "$AFTER" -ge "$((BEFORE + 2))" ] 2>/dev/null; then
    echo "  PASS  worker_count increased by 2 after add_workers ($BEFORE -> $AFTER)"
    PASS=$((PASS + 1))
else
    echo "  FAIL  worker_count did not increase by 2 after add_workers ($BEFORE -> $AFTER)"
    FAIL=$((FAIL + 1))
fi

psql_run "remove_workers(2) returns 2" \
    "2" \
    "SELECT redis.remove_workers(2)"

sleep 1

FINAL=$(PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -t -A -c "SELECT redis.worker_count()" 2>&1)

if [ "$FINAL" -le "$AFTER" ] 2>/dev/null; then
    echo "  PASS  worker_count decreased after remove_workers ($AFTER -> $FINAL)"
    PASS=$((PASS + 1))
else
    echo "  FAIL  worker_count did not decrease after remove_workers ($AFTER -> $FINAL)"
    FAIL=$((FAIL + 1))
fi

# ── INFO / COMMAND ────────────────────────────────────────────────────────────
echo ""
echo "--- Server ---"
# Just check non-error response
INFO_OUT=$(redis-cli -h "$HOST" -p "$PORT" INFO 2>&1)
if echo "$INFO_OUT" | grep -q "redis_version"; then
    echo "  PASS  INFO returns server info"
    PASS=$((PASS + 1))
else
    echo "  FAIL  INFO missing redis_version"
    FAIL=$((FAIL + 1))
fi

COMMAND_OUT=$(redis-cli -h "$HOST" -p "$PORT" COMMAND 2>&1)
if [ "$COMMAND_OUT" = "(empty array)" ] || [ -z "$COMMAND_OUT" ]; then
    echo "  PASS  COMMAND returns empty array"
    PASS=$((PASS + 1))
else
    echo "  FAIL  COMMAND unexpected: $COMMAND_OUT"
    FAIL=$((FAIL + 1))
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "==============================="
echo "  PASS: $PASS   FAIL: $FAIL"
echo "==============================="
echo ""

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
