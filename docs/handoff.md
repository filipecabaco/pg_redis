# Handoff

Working notes for picking up the in-flight work: what is done, what is open,
how to verify a change, and which traps have already cost someone a day.

## State

| | |
|---|---|
| Rust tests | 126/126 on pg15, pg16, pg17, and against an assert-enabled pg17 |
| E2E | 213/213 under **both** `storage_mode=memory` and `storage_mode=auto` |
| Lint | clippy clean at `-D warnings`, `cargo fmt --check` clean |
| Shared memory | 301 MB `shared_memory_size` at the defaults, down from 587 MB |

## Verifying a change

There are three test layers and they cover different code. Running one is not
running the others.

| Layer | Command | What it actually covers |
|---|---|---|
| Unit | `cargo test --lib` | Pure functions: parsing, key layout, limits, SipHash, and that `docs/command-coverage.md` still matches the parser |
| pgrx | `cargo pgrx test pg17` | **The SPI path only** — never the shared-memory backend |
| E2E | `bun test` against a live server | Everything, in whichever storage mode the server runs |

The gap in the middle is the important one. `cargo pgrx test` starts its own
throwaway cluster without `shared_preload_libraries`, so memory mode — the
default storage mode, and all of `mem.rs` — is invisible to it. Four bugs
reached the branch that way. CI now runs the e2e suite in both storage modes
for exactly this reason.

### Running the e2e suite without Docker

`docker compose --profile e2e up` is the supported path, but it is not the only
one, and in a dev container without a working Docker daemon it is not an option.
`bun` runs the same suite directly against any live server:

```bash
# 1. A cluster with the extension preloaded. Run as an unprivileged user —
#    initdb refuses to run as root, and a long $HOME breaks the socket path.
initdb -D "$PGDATA" -U postgres -A trust
cat >> "$PGDATA/postgresql.conf" <<'CONF'
listen_addresses = 'localhost'
shared_preload_libraries = 'pg_redis'
redis.storage_mode = 'memory'      # or 'auto' — run both
redis.port = 6379
redis.database = 'postgres'
redis.default_db = 8
redis.password = 'testpass'
CONF

# 2. Install the extension into that PostgreSQL, then start it.
cargo pgrx install --release --pg-config /usr/lib/postgresql/17/bin/pg_config
pg_ctl -D "$PGDATA" -o '-k /tmp' start
psql -h 127.0.0.1 -U postgres -c 'CREATE EXTENSION pg_redis;'

# 3. The suite.
cd docker/e2e && bun install
DATABASE_URL="postgres://postgres@localhost:5432/postgres" \
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=testpass \
  bun test index.test.ts
```

Re-run step 2 after every code change — the running server holds the old
`.so` until it restarts.

### CI runs against an assert-enabled PostgreSQL

`cargo pgrx init --pgN download` configures PostgreSQL with `--enable-cassert`,
which the distribution packages do not. Code that misuses a PostgreSQL internal
can pass against system packages and fail only in CI, with an assertion crash
that also takes down the backend and cascades into unrelated tests.

That is not hypothetical: `UpdateActiveSnapshotCommandId` asserts the active
snapshot has `active_count == 1` and `regd_count == 0`, which the snapshot SPI
has already pushed satisfies neither. It passed locally and failed every CI leg.

To reproduce that locally, build one and point pgrx at it:

```bash
# ftp.postgresql.org may be unreachable; the Debian source package works.
echo "deb-src [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
  https://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
  > /etc/apt/sources.list.d/pgdg-src.list
apt-get update && apt-get install -y flex && apt-get source postgresql-17

cd postgresql-17-*/
./configure --prefix=$HOME/pgassert --enable-cassert --enable-debug \
  --without-icu --without-readline --without-zlib --with-openssl
make -j"$(nproc)" && make install

PGRX_HOME=$HOME/pgrxassert cargo pgrx init --pg17 "$HOME/pgassert/bin/pg_config"
PGRX_HOME=$HOME/pgrxassert cargo pgrx test pg17
```

Keep it under its own `PGRX_HOME` so the fast system-PostgreSQL config stays
intact for everyday runs, and give it its own `CARGO_TARGET_DIR` — the bindings
differ from the system build's, so sharing one target directory means a full
rebuild every time you switch.

### `cargo pgrx test` also has to run as a non-root user

It runs `initdb` itself, which refuses to run as root exactly as the e2e setup
above does — the failure is a wall of "Could not obtain test mutex" from every
test after the first, with the real message ("initdb: error: cannot be run as
root") only in the first one's output. From a root container:

```bash
useradd -m pgu
cp -a ~/.cargo ~/.pgrx /home/pgu/ && chown -R pgu /home/pgu
chown -R pgu "$PWD"                                   # target/ is written during the run
chown -R pgu /usr/lib/postgresql/*/lib /usr/share/postgresql/*/extension
sudo -u pgu env HOME=/home/pgu PATH=/home/pgu/.cargo/bin:$PATH \
  RUSTUP_HOME=$HOME/.rustup CARGO_HOME=/home/pgu/.cargo cargo pgrx test pg17
```

`cargo pgrx init --pgN download` fetches from `ftp.postgresql.org`, which is not
always reachable. Pointing pgrx at the PGDG packages
(`cargo pgrx init --pg15 /usr/lib/postgresql/15/bin/pg_config …`) gets the
matrix running; it is the assert-enabled build above that has to be compiled.

### Traps

- **`bun test -t "<filter>"` changes which database the tests run against.**
  The `SELECT isolation` describe is what leaves the pooled client on db 0; a
  filtered run skips it, so the connection stays on db 8 and the test silently
  exercises the durable half instead of shared memory. A list bug was
  misdiagnosed twice this way. Prefer a full run when a failure looks
  order-dependent.
- **`Bun.RedisClient` encodes arguments as UTF-8.** It cannot express a `0xff`
  byte, and reports 10 for a 7-byte value. Anything asserting on exact bytes or
  exact lengths must go through the `RawRedis` helper at the top of
  `index.test.ts`, or it measures the client's encoder rather than the server.
- **A rejected `client.send()` leaves the pooled client's replies out of step**
  for later tests. Where an error reply is the expected outcome, use `RawRedis`,
  which returns the frame as data.
- **`SHOW redis.foo` names the result column `redis.foo`**, dot included.
  Destructuring by `foo` yields `undefined` with no error. Use
  `SELECT current_setting('redis.foo') AS mode`.

## Open work

### 1. The dispatch handoff (throughput)

**Sharding the shared-memory LWLocks is not worth doing.** That was measured,
not assumed:

| Measurement | Result |
|---|---|
| Throughput at 1 / 2 / 4 workers | 55k / 48k / 55k — no scaling at all |
| Worker wait events, 120 samples under `SET` load | 100% `RUNNING/cpu`, zero LWLock waits |
| `SET` (exclusive lock) vs `GET` (shared lock) | 52k vs 54k — indistinguishable |
| Pipelining `-P 1` → `-P 32` | 47.5k → 151k (3.2×) |

One worker matches four, and pipelining triples throughput without touching the
lock. The ceiling is the **two mpsc thread hops per command** between a
connection thread and its worker's dispatcher (`DispatchMsg` in `worker.rs`).

Memory-mode commands need no SPI and no transaction, so a connection thread
could in principle execute them inline and skip both hops. The blocker is that
PostgreSQL LWLocks require a valid `MyProc`, which non-backend threads do not
have — which is why everything funnels through the bgworker main thread today.
Closing this needs a design, not a patch. Lock sharding only becomes
interesting afterwards.

### 2. Done — chunked value pool

The three overflow tables are gone. Values spill into a per-table slab of
64-byte chunks on a free list, chained by index; `shared_memory_size` went from
413 MB to 308 MB and the value cap from 512 bytes to 64 KiB. See
`docs/storage-modes.md` for the sizing and the two bounds on the cap.

What is left over from it:

- **A full pool refuses spilling writes, and only `reserve_chunks` asks for
  room.** It runs on the paths where bulk data arrives — `SET`, `MSET`,
  `GETSET`, `APPEND`, `HSET`, `HSETNX`, `LPUSH`, `RPUSH`. The others (`LSET`,
  `LINSERT`, `LMOVE`) reply `OOM` on an empty pool without trying to evict
  first. Eviction cannot simply be moved into `value_write`: it may remove any
  entry in the table, including the one being written, which is why it runs
  before the entry exists.
- **Chunk lifecycle is the thing to be careful with.** Every removal of an
  entry that owns pooled bytes goes through `remove_valued` (or `list_remove_at`
  / `zset_remove_at`, its bare-HTAB twins), which frees the chains *before* the
  entry — the chain heads live in the entry. A path that calls
  `hash_search(HASH_REMOVE)` on the KV, hash, set, zset or list table directly
  would leak silently until the pool ran dry.
  `values_round_trip_through_the_pool_at_every_boundary`,
  `dropping_a_hash_entry_returns_both_its_member_and_its_value` and the e2e
  "rewriting a large value many times" and "churning large members" tests are
  the guards.

### 3. Done — members hashed out of the composite key

The member half of a composite key is a 128-bit keyed SipHash now, so the key is
32 bytes instead of 144 and the member bytes live in the entry — 48 inline, the
rest in the same chunk pool a value spills into. `ZsetMeta` caches the extreme
members as hashes rather than as two 128-byte arrays, which keeps `ZPOPMIN` and
`ZPOPMAX` O(1) at 72 bytes instead of 304. The cap went from 128 bytes to 64 KiB
— the same bound a value has, and derived the same way — and
`shared_memory_size` from 308 MB to 301 MB. See `docs/storage-modes.md`.

Sets and sorted sets gained a chunk pool each (`POOLS_PER_DB` is 5), costing
8.5 MiB against the 15.7 MiB the entries shed. It also fixed a binary-safety bug
nobody had noticed: members were NUL-padded inside the key and read back at the
first NUL, so `a\0b` and `a` were two entries that both read back as `a`.

### 4. Smaller items

- **Multi-key writes are not atomic under OOM.** An `MSET` or multi-member
  `SADD` that fills a table part-way through stores the earlier keys and then
  errors, where Redis rejects the command whole. Whether there is room is only
  knowable at the insert, so this needs a capacity pre-check against the
  command's key count. Commented at the check site in `execute_mem`.
- **`redis.mem_max_entries` is still 8192**, and should stay there for now.
  Raising it back to the 16384 it was before `MAX_KEY_LEN` grew costs **+90 MB**
  (301 MB → 391 MB at `SHOW shared_memory_size`); the member work freed 7 MB, so
  it funds 8% of that. The 512-byte `MAX_KEY_LEN` is what actually pays for it —
  `KvEntry` is 592 bytes, of which 512 is the key, and the KV table alone is
  46 MiB of the extension's 91 MiB. Hashing the KV table's key the way the
  composite tables' is hashed is the change that would fund raising this, and it
  is harder: `KEYS`, `SCAN` and `RANDOMKEY` read those bytes back.

## Design notes

Things that are easy to get wrong without knowing why they are the way they are.

- **Databases split into contiguous halves**, not by parity: 0–7 ephemeral,
  8–15 durable. `SELECT` also accepts `cache` (0) and `durable` (8).
- **Composite tables key on two 128-bit keyed SipHashes** — one of the Redis
  key, one of the member — and on nothing else. Every access to those tables is
  a comparison against a key the caller already holds; only `KEYS`, `SCAN` and
  `RANDOMKEY` need real key bytes, and they read the string table. The hash is
  keyed from `pg_strong_random` per postmaster so a client cannot craft a
  collision and merge two keys' contents.
- **A composite entry must be given its member bytes when it is created.** The
  member is no longer in the HTAB key, so `hash_search(HASH_ENTER)` no longer
  writes it: `enter_member` (and `enter_member_raw`) is the only correct way to
  insert into the hash, set or zset table. An entry created any other way reads
  back with an empty member, or with the previous occupant's.
- **The invariant that makes recycled entries safe is `len == 0`.** dynahash
  hands a removed entry straight back out on the next insert, so `value_free`
  zeroes the length as well as releasing the chain, and a chain head is only
  ever read when its length says there is a tail. Every removal path has to go
  through it or the next occupant releases a chain that is already free.
- **Every insert uses `HASH_ENTER_NULL`, never `HASH_ENTER`.** On a
  `HASH_FIXED_SIZE` table dynahash answers a full table by raising
  `out of shared memory`, which unwinds by longjmp — past the Rust error
  handling that looks like it covers it — and kills the worker.
- **Lists renumber on removal.** Readers walk positions as
  `min_pos + i * LIST_POS_STEP`, so an in-place delete leaves a hole that
  truncates the list at the first gap. `LREM` and `LINSERT` go through a
  read-modify-write that renumbers from a fresh base.
- **Size limits are checked before execution**, in `Command::mem_too_long_error`,
  so a refusal never leaves a partial write. `debug_assert`s in `make_key`,
  `make_composite_key`, `make_list_key` and the three value writers catch
  anything that reaches the backend over-long.
- **Error replies carry their own code when they have one.** `write_error`
  prefixes `ERR` only for messages that do not already start with a code from
  `ERROR_CODES`, so an OOM reply goes out as `-OOM ...` — which is what clients
  match on.

## Benchmarking

`redis-benchmark` defaults to 3-byte values, so the standard suite never leaves
the 64-byte inline slot. `mise run bench-value-sizes` sweeps
3/63/64/65/200/512/4096/65536 bytes to make the inline-to-pool boundary
visible.

**Point the benchmark at db 0 with `storage_mode=memory`.** The compose default
is `redis.default_db=8`, the WAL-logged half — a benchmark that forgets this
measures fsync and reports ~500 requests/second for everything.
`bench-value-sizes` and `bench-report` set both; `bench` does not, by design.

**`redis-benchmark` never clears `mylist`.** Its `lpush`, `rpush` and `lrange`
tests build the list up over the whole run and `-n 20000` is past what a list
table holds at the default `redis.mem_max_entries`, so the run ends in an OOM
the tool reports as nothing at all. `docker/bench/report.sh` clears the key and
uses 4,000 requests for those.

CI runs `docker/bench/report.sh` on every pull request and posts it as a
comment, keyed on an HTML marker so each push edits the same one — and deletes
any duplicate, so the thread carries exactly one report however many times it
runs.
