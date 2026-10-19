# Parity roadmap

!!! note "History"
    As of 2026-10-19, this page is the working record of how parity with Redis
    was closed, kept for the reasoning behind each decision. For current
    behaviour see [Command coverage](command-coverage.md) and the
    [differences list on the home page](index.md#differences). Current
    numbers: pg_redis accepts 235 of Redis 7.2's 242 commands, and none of the
    2,250 replies the parity harness compares differ from x86-64 Redis 7.2.

What is left between pg_redis and Redis, why the headline number does not say
so, and what has to be decided before the next block of work starts.

[Section 12 of IMPLEMENTATION.md](IMPLEMENTATION.md#12-open-work) records the
parity work that is **done**: six phases, 189 differences down to 2. This page
covers what that number does not reach, and its figures come from measurement
against live servers.

First written for #12 (`28038db`, "Close the Redis parity gap").
Re-measure before trusting any table here; the recipe is below and takes two
minutes.

**Re-measured against Redis 7.2.5 after the parity block.** Redis 7.2 has 242
top-level commands; pg_redis now accepts **235**. The seven missing are the
replication and migration internals (`MIGRATE`, `PSYNC`, `REPLCONF`,
`RESTORE-ASKING`, `SYNC`), `MODULE` and `SENTINEL`. `COMMAND COUNT` answers 235
and is derived from the coverage page, not asserted. The block added
streams, scripting and functions, the blocking commands, the server and
introspection commands, HyperLogLog, GEO and RESP3, and closed the `ZUNION`
family's set-input gap. Redis's own acceptance suite is now the measure; see
"Redis's own acceptance suite" in `IMPLEMENTATION.md` for the current count.

The tables below predate that block. Their tier 2 ("commands we do not
implement") is now the list above; tier 1's rows are kept for their
reasoning, and several have since been closed, so re-check one against the
code before relying on it.

Previously, against Redis 7.0.15: 240 top-level commands, 150 accepted, 90
missing.

This page has been wrong three times, each time in the same direction: the
defect found was bigger than the entry predicted. Eviction's entry hid a
`FLUSHDB` segfault, the stale read listed as memory-only and one command
affected both halves and fifteen commands, and the `RANDOMKEY` entry described
as a skew was a wrong answer on the durable half. Measure before you plan.


## How to pick this up

Everything on this page came from probing a live pair of servers, without
reading the source. Redo the probe first, since a merged PR or two will have
moved the results.

```bash
# pg_redis on 6379 (password testpass), reference Redis 7 on 6380.
redis-cli -p 6380 COMMAND LIST | sort > /tmp/redis_cmds.txt

while read -r c; do
  out=$(redis-cli -p 6379 -a testpass --no-auth-warning "$c" 2>&1 | head -1)
  case "$out" in
    *"unknown command"*) echo "MISSING $c" ;;
    *) echo "HAVE $c" ;;
  esac
done < /tmp/redis_cmds.txt > /tmp/probe.out

grep MISSING /tmp/probe.out | sed 's/MISSING //' | grep -v '|'
```

Two things to know about that probe. `COMMAND LIST` returns container
subcommands as `acl|cat`, `object|encoding` and so on, which are not command
names. Filter them out with `grep -v '|'`, or every container we implement
reads as missing. The probe also classifies by the reply to a
**zero-argument** call, so a command that exists reports an arity error, which
is not `unknown command` and therefore counts as present. That is the right
answer for this purpose.

Reference used here: Redis 7.0.15, 240 top-level commands.


## Where parity actually stands

**No differences over 2,250 replies** against x86-64 Redis 7.2 (93 cases, run
on both storage halves), the build CI compares with. `INCRBYFLOAT` used to
differ there, because it was `f64` here where Redis uses an 80-bit
`long double`; it now computes in the 80-bit format too. Against arm64 Redis
two cases differ on both storage halves: `INCRBYFLOAT`, whose `long double`
there is 128-bit and prints back to `4`, and the last digits of a distance
stored by `GEOSEARCHSTORE … STOREDIST`. Arm64 Redis differs from x86-64 Redis
in the same way, so these four are kept in the baseline with a note and do not
fail a run on an arm64 machine. The reference image is the Debian `redis:7.2`,
on glibc: Alpine's musl `strtold` parses some out-of-range floats differently
from the glibc Redis most installations run.

When this section was first written the harness compared
1,394 replies.

That figure covers less than it seems to, in two ways. Both come from what a
differential harness can do, and neither is a flaw in this one:

- **It compares commands both servers implement.** Every command we do not
  implement is invisible to it by construction. 97 of Redis's 240 were in that
  set when this was written; 7 of Redis 7.2's 242 are now.
- **Two comparisons are deliberately loose.** `RANDOMKEY` is compared by shape,
  because any live key is a valid answer. `TTL` is compared by class (gone, no
  expiry, some expiry). That was itself a tightening: it used to be compared by
  reply shape, so `-1` and `100` looked alike and every expiry bug was
  invisible.

The gate is worth keeping and can be trusted for what it covers. It does not
measure coverage, and this page exists because nothing else did.


## Tier 1 — commands we have that behave differently

Differences a client can observe. The four rows are written up in
[§12](IMPLEMENTATION.md#12-open-work) and collected here in one place. A
fifth, found later, follows the table.

| Gap | Half | Status |
|---|---|---|
| `INCRBYFLOAT` precision — `f64` against Redis's `long double` | both | **Fixed.** `INCRBYFLOAT` and `HINCRBYFLOAT` compute in the x87 80-bit format and match x86-64 Redis digit for digit. Against arm64 Redis, whose `long double` is 128-bit, the last digit can differ. See [phase 2](IMPLEMENTATION.md#phase-2-ranges-shapes-and-ties) |
| GEO distance stored by `GEOSEARCHSTORE … STOREDIST` — last-digit `f64` rounding | both | **Accepted.** The baseline's two entries, one per half; differs against arm64 Redis only, and x86 Redis matches |
| An expired collection outlives its expiry: `TYPE` answers `none` while `LLEN` still reports the old count, until the sweep runs | both | **Fixed.** It affected both halves and more than one command; see [below](#the-stale-collection-read-was-bigger-than-this-table-said) |
| `SCAN`/`KEYS` return the whole keyspace in one reply, cursor always 0 | both | **Fixed** for `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN`: real cursors on both halves. See [Incremental cursors](#incremental-cursors) below. `KEYS` returns everything in one reply, as in Redis |
| `RANDOMKEY` returns the first live key the KV scan reaches, and consults the collection metas only when there is none | ~~memory~~ both | **Fixed**, and it was worse than this row said. The durable half read `kv_N` alone, so a database holding only collections answered *nil* while `DBSIZE` counted them, which is a wrong answer. Both halves now draw uniformly over every live key: the durable half with a five-table `UNION` under `ORDER BY random()`, the memory half with a single-pass reservoir over the KV scan and the collection names. An expired-but-unswept collection is excluded on both. The e2e `RANDOMKEY` describes pin all three properties per half |

A fifth was found while `ZINTERCARD` was being added, by probing the live pair:
**the `ZUNION`/`ZINTER`/`ZDIFF` family ignores a plain-set input where Redis
treats it as a sorted set whose scores are all 1.** It affects both halves,
including the `STORE` variants and `ZINTERCARD`. The type gate admits the set
(`ZSET_OR_SET`), but the aggregation reads only the zset table, so `ZDIFF z s`
answers members that Redis says are cancelled and `ZINTER z s` answers empty
where Redis intersects. Measured: with `z` a zset holding `m` and `s` a set
holding `m`, `ZDIFF 2 z s` is `["m"]` here and `[]` on Redis. `ZINTERCARD`
inherits the behaviour deliberately: its answer must equal `ZINTER`'s
cardinality, and fixing one command of the family while five others disagree
would be worse than the shared gap. It is open on both halves. The fix is one
piece of work across the six commands and is the next block; its shape is
sketched in [Suggested order](#suggested-order) so a fresh session can start
from the code instead of from this table.

**Status now: closed, on both halves.** The family reads a plain set as a
sorted set whose scores are all 1 (`src/composite.rs`), and the parity case
"Sorted-set arithmetic over plain sets" covers it.

`ZPOPMIN` being O(entries in the database) is a performance defect and not a
parity difference, so it is not counted here. On the memory half it is
history: since phase 6 a sorted set keeps its members in score
order (`src/memdb/zset.rs`), and `ZPOPMIN` takes them from the front.

### The stale-collection read was bigger than this table said

The row above used to say "memory" and describe one command. Measured against
a live pair before any change, it affected **both halves and fifteen read
commands**: `LLEN` `LRANGE` `LINDEX`, `SCARD` `SMEMBERS` `SISMEMBER`
`SRANDMEMBER`, `HLEN` `HGETALL` `HGET` `HKEYS`, and `ZCARD` `ZRANGE` `ZSCORE`
`ZCOUNT`. Strings were correct on both halves throughout because `kv_N`
carries its own `expires_at` and every statement reading one tests it. A
collection's rows carry no deadline (it lives in the key directory or in
`redis.expiry_N`), so a read of the rows cannot tell a live key from one the
sweep has not reached.

Both halves had the same shape of bug and needed different fixes, because they
disagree about when the type of a key is checked.

- **Memory.** `dir_lookup` folded "expiry has passed" onto "absent", so
  `mem_type_error` let the command through and the read went to the tables.
  `dir_lookup_raw` keeps the distinction and `mem_key_kind_reaping` acts on it,
  in the one place every memory command already passes through. It uses the
  same single directory lookup as before, so it costs nothing.
- **Durable.** Reads deliberately skip the type check: a read runs first and
  only asks what the key holds if it came back empty, which keeps a round trip
  off every successful `GET`. That optimisation is what made the stale read
  reachable, because a read that returns data is no evidence the key is live.
  A collection read now reaps first, with a keyed form of the sweep statement.

**That cost about 63% of durable-half collection-read throughput as first
shipped** (`redis-benchmark -n 20000 -c 50`): `HGET` 35.2k → 12.1k, `LLEN`
33.2k → 12.2k, `ZCARD` 30.5k → 11.4k, `SCARD` 27.0k → 11.4k. `GET` unchanged
at ~32k. The whole cost was the extra round trip. The first attempt routed
reads through `sql_type_error` (five `EXISTS` across five tables), and
replacing that with one indexed lookup on `redis.expiry_N` that usually
deletes nothing moved `HGET` only 11.3k → 12.1k. A second statement on the
read path costs roughly two thirds of throughput whatever it does.

**Decision taken: the hybrid.** The thirteen cached single-key collection
reads (`HGET` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HSTRLEN`,
`SMEMBERS` `SISMEMBER` `SCARD`, `ZCARD` `ZSCORE`, `LLEN`) carry the liveness
predicate inside their own statement and skip the up-front reap. The predicate
is `sql::live`, an InitPlan that probes `redis.expiry_N` once per query, not
once per row. The other ~56 inline statements (the `EXCEPT` chains and
`unnest` joins) keep reaping first. That is a single mechanism, and a
statement that forgot its predicate cannot miss it. The choice between the two
is made in `Command::execute` and nowhere else, and a command misclassified in
either direction is survivable: wrongly left off the list, it pays a reap it
did not need; wrongly put on the list, it fails its expired-key e2e test.
Guarding all 68 was rejected. It would have meant ~56 chances of a silent
stale read, policed by a source-scanning test, to speed up commands that are
not hot.

Re-measured after the change with the same recipe, on a container where `GET`
runs at 38–50k, the guarded reads are back at full speed: `HGET` 32.2–34.2k,
`LLEN` 32.6k, `ZCARD` 29.1k, `SCARD` 33.4k, `SISMEMBER` 30.1k, `HSTRLEN`
29.3k. The reads still paying the reap sit at a third of that (`LINDEX` 10.3k,
`LRANGE` 9.7k, measured in the same minute). The reap also still runs for a
guarded command whose read found nothing, before the type check. That keeps
`LLEN` on an expired hash answering 0 rather than `WRONGTYPE`, cleans the rows
without waiting for the sweep, and costs only the miss path.

## Tier 2 — commands we do not implement

**Status now: every command named in the table below is implemented except
`MIGRATE`**, blocking, GEO, streams, scripting and `SLOWLOG` included. The
seven Redis 7.2 commands still missing are listed at the top of this page, and
[Command coverage](command-coverage.md) lists what is accepted. The rest of
this section records the gap as it stood when it was written.

**150 of 240 accepted, 90 missing.** `MEMORY` (`USAGE` only) moved columns with
the acceptance-suite work. The table is grouped by what a client loses, the
same ordering [§12's phase plan](IMPLEMENTATION.md#the-plan) used, for the
same reason: a command that answers wrongly costs more than one that is
absent, and an absent command a client calls costs more than one it does not.

The tier-2 free wins (`TIME`, `TOUCH`, `HSTRLEN`, `ZINTERCARD`, and
`DUMP`/`RESTORE`) are done and no longer counted below. The first four were
as mechanical as predicted. `DUMP`/`RESTORE` took more than the old note here
implied when it called them mostly done already. `dump_key`/`restore_key`
supplied the traversal, but Redis's serialisation format needed its own module
(`src/rdb.rs`): RDB value encoding, listpack and intset readers, LZF
decompression, CRC-64. Payloads interchange with a real 7.0 in both
directions, verified live; version 11+ payloads and pre-7 compact encodings
are refused loudly, as `docs/commands.md` records.

| Group | # | Commands |
|---|---:|---|
| **Blocking** | 8 | `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BLMPOP` `BZPOPMIN` `BZPOPMAX` `BZMPOP` |
| **Incremental scan** | 3 | `HSCAN` `SSCAN` `ZSCAN` |
| **Plain data and keyspace** | 8 | `MOVE` `SWAPDB` `SORT` `SORT_RO` `LMPOP` `ZMPOP` `ZRANGESTORE` `LCS` |
| **Bitmaps** | 4 | `BITOP` `BITPOS` `BITFIELD` `BITFIELD_RO` |
| **HyperLogLog** | 5 | `PFADD` `PFCOUNT` `PFMERGE` `PFDEBUG` `PFSELFTEST` |
| **GEO** | 10 | `GEOADD` `GEODIST` `GEOHASH` `GEOPOS` `GEOSEARCH` `GEOSEARCHSTORE` `GEORADIUS`(+`_RO`) `GEORADIUSBYMEMBER`(+`_RO`) |
| **Sharded pub/sub** | 3 | `SPUBLISH` `SSUBSCRIBE` `SUNSUBSCRIBE` |
| **Streams** | 15 | `XADD` and friends |
| **Scripting and functions** | 8 | `EVAL` `EVAL_RO` `EVALSHA` `EVALSHA_RO` `FCALL` `FCALL_RO` `FUNCTION` `SCRIPT` |
| **Cluster, replication, admin** | 26 | `ACL` `CLUSTER` `REPLICAOF` `DEBUG` `MONITOR` `SLOWLOG` `WAIT` `MIGRATE` … |

Notes on individual entries worth knowing before planning:

- **Blocking is the single biggest gap.** It is the queue use case, and the
  only thing on this list that needs architectural work; the rest is
  mechanical.
- **`HSCAN`/`SSCAN`/`ZSCAN` are what client libraries use to iterate a
  collection.** Clients notice their absence through the library they
  use.
- **Bitmaps are half-built.** `SETBIT`, `GETBIT` and `BITCOUNT` landed in
  phase 5; `BITOP`/`BITPOS`/`BITFIELD` finish the family and can reuse
  `bit_at`/`set_bit_at`/`count_bits`/`byte_range` in `commands.rs`.
- **GEO is a sorted set with a geohash score.** It is self-contained, does not
  interact with anything already built, and is the only group here that is
  more arithmetic than plumbing.

**Keyspace notifications** (`__keyspace@N__` / `__keyevent@N__`) were absent
when this was written, and are not a command, so they appear nowhere in the
table above. They have since been built on the pub/sub machinery, along with
client tracking.


## Tier 3 — introspection that answers, and answers wrongly

This tier gets its own section because a wrong answer is worse than an absent
one: clients branch on these replies, and a plausible wrong answer misleads
where an error would not.

Measured side by side against the reference:

| | pg_redis | Redis |
|---|---|---|
| `OBJECT REFCOUNT k` | *(nil)* | `1` |
| `OBJECT IDLETIME k` | *(nil)* | `0` |
| `OBJECT FREQ k` | *(nil)* | error unless LFU is on |
| `CONFIG GET maxmemory` | ~~*(empty)*~~ the memory limit in bytes. **Closed.** | `maxmemory 0` |
| `COMMAND COUNT` | ~~`100`~~ ~~**150**~~ **235**, the rows of the coverage page. **Closed.** | `240` (7.0), `242` (7.2) |
| `COMMAND DOCS` / `INFO` / `LIST` | ~~*(empty)*~~ populated from Redis 7.2's command table. **Closed.** | populated |
| `CLIENT ID` | ~~`0`, always~~ unique per connection. **Closed.** | unique per connection |

The three `OBJECT` rows are still current: every `OBJECT` subcommand other
than `ENCODING` answers nil.

`COMMAND COUNT` was a hardcoded `Response::Integer(100)`. It now answers with
the number of rows in [docs/command-coverage.md](command-coverage.md), counted
at compile time. The page is asserted against the parser in both directions,
so the reply, the page and the parser cannot drift apart in pairs. The fix
raised an aliasing question: `SUBSTR` now has its own row instead of a note on
`GETRANGE`, because Redis's own command table counts it separately. The reply
is the number this server accepts, not Redis's 240, because a client asking is
asking about this server.

`CLIENT ID` always returning 0 is the visible symptom of connection state not
being tracked at all, which also blocks `CLIENT KILL`, `LIST`, `GETNAME` and
`UNPAUSE`. Fixing the family means giving a connection an identity; patching
the reply would not be enough.

**Status now: closed.** Every connection is registered with an id from a
counter in shared memory, and `CLIENT ID`, `INFO`, `LIST`, `KILL`,
`GETNAME`/`SETNAME` and `PAUSE`/`UNPAUSE` work across every worker.


## The two architectural blockers

Everything in tier 2 below these two is mechanical. These two are not, and
both were investigated far enough to know why.

### Blocking commands

The threading model decides this, so it comes first. Each worker has **one
dispatcher thread** plus one thread per connection. The dispatcher is the only
thread with a valid `MyProc`, and therefore the only one that may touch
PostgreSQL or take an LWLock. A connection thread parses, sends a
`DispatchMsg` down a `sync_channel`, and blocks on `resp_rx.recv()` until the
dispatcher answers.

That determines the shape of the answer:

- **A blocking command must never block the dispatcher.** It occupies the one
  thread every other client is queued behind. `BLPOP` has to execute as a
  non-blocking `LPOP` attempt that reports "nothing there", with the waiting
  done on the connection thread.
- **Waking has to be a poll, not a signal.** `redis.workers` defaults to **4**,
  and a worker is a *process*. A client blocked on worker 2 can be unblocked by
  an `RPUSH` that lands on worker 3, so any wake mechanism has to cross
  processes. In memory mode shared memory could carry one; on the durable half
  it would need `LISTEN`/`NOTIFY` or equivalent. Polling is the only mechanism
  that works in both modes with no new infrastructure, and the tree has a
  precedent: `subscribe_loop` already polls with a 5 ms floor backing off to
  250 ms while quiet.
- **Polling costs FIFO fairness.** Redis serves the client that has waited
  longest. A poll serves whoever wakes up first. That is an observable
  difference and the harness would not catch it, so accepting it has to be a
  deliberate decision made in advance.
- **`BLPOP` inside `MULTI` must not block.** Redis returns the nil reply
  immediately. `EXEC` arrives as `DispatchMsg::Batch`, a distinct variant from
  `DispatchMsg::Cmd`, so the two contexts can already be told apart at the
  point where the decision has to be made. Nothing new is needed for this, but
  it must not be forgotten.

**Status now: built, without giving up FIFO.** All eight commands exist
(`src/blocking.rs`). A blocked client waits on its connection thread, never on
the dispatcher, and each attempt is an ordinary non-blocking command. Between
attempts it watches the `WATCH` write counters of its keys, which every worker
bumps on every write, and retries when one moves. Waiters on the same key are
served oldest first through a ticket registry in shared memory, so Redis's
order holds across workers. Inside `MULTI` the command runs once and answers
nil when nothing is there.

### Incremental cursors

`SCAN` parses `cursor` and `COUNT` into fields named `_cursor` and `_count`
(underscore-prefixed because they are unused) and returns every matching key
with cursor 0. `HSCAN`/`SSCAN`/`ZSCAN` do not exist at all.

**Status now: built, on both halves, and everything below is history.** All
four commands return a page per call and a cursor, with Redis's guarantee that
a full iteration returns every element present from its start to its end. No
sorted index was needed. The dynahash tables the bullets below describe were
replaced in phase 6 of [Storage policies](STORAGE.md#growable-shared-memory-phase-6):
a keyspace, hash or set is now a `memdb::Dict`, whose entries sit in a dense
`Vec` found through a hashbrown table, and a removal swaps the last entry into
the hole or, in a listpack-encoded hash or set, moves the entries after it
down one (`src/memdb/dict.rs`). An entry therefore only moves to a lower
position, so the memory half's cursor is a position, iterated from the top
down, with no state and O(`COUNT`) per call. The durable half resumes after
the last key returned, through the primary key index, and keeps that key in a
cursor table in shared memory that every worker reads.
[SCAN cursors](IMPLEMENTATION.md#scan-cursors) has the details.

Adding the three commands with the same "cursor always 0" convention is
mechanical and would unblock client libraries. Giving any of the four real
cursors is not mechanical on the memory half:

- Redis's guarantee is that an element present for the whole iteration is
  returned at least once, delivered by a reverse-binary-increment walk over hash
  buckets. Our tables are dynahash, whose bucket layout is not public API, so
  that walk cannot be reproduced directly.
- A `hash_seq_search` scan cannot be parked between commands. It would have to
  hold the table across an unbounded client think-time.
- The cheap alternative is to make the member hash the cursor and, on each call,
  scan the table for hashes above it. That is correct but O(n) per call, so
  O(n²/COUNT) to iterate a collection. `ZPOPMIN` already has the same shape of
  defect, and adding another one deliberately would be a poor choice.

The durable half has no such problem: `ORDER BY` plus a cursor predicate is an
index scan.

So a split is likely: real cursors on the durable half, cursor-0 on the memory
half, and the difference documented. The other option is cursor-0 on both
until the sorted-index work that `ZPOPMIN` also wants gets done. Choosing
between them needs an explicit decision.


## Decisions needed before the next block

Nothing below can be settled from the code. Two of the four are now taken.

1. **Are streams, scripting, ACL and cluster non-goals?** *Open.* 50 of the 97
   missing commands. They look out of scope for a Postgres-backed Redis
   endpoint, but that has never been stated, so this page still counts them as
   parity debt. Declaring them non-goals would move the denominator from 240
   to ~190 and make the number meaningful.
2. **Is polled blocking acceptable, losing FIFO fairness?** **Taken: neither.
   Blocking is deferred.** Neither polling nor the cross-process wake
   infrastructure is being built. The eight blocking commands stay missing for
   now, on the same principle that orders this page: an absent command returns
   an error the client can see, while a `BLPOP` that quietly serves the wrong
   waiter is a difference no test here would catch. Revisit once the
   correctness work is done, and make the decision afresh then.
3. **Real `SCAN` cursors, or the cursor-0 convention extended to the
   collections?** **Taken: real, on both halves, via a sorted index.** The
   cheap split was rejected. The durable half gets `ORDER BY` plus a cursor
   predicate; the memory half gets the sorted index it needs to answer
   correctly. That is the same structure `ZPOPMIN`'s O(entries in the
   database) wants, so the two should be planned as one piece of work. It
   changes the shared-memory layout, so it needs sizing against
   [§3](IMPLEMENTATION.md#3-the-shared-memory-backend) before any code, and it
   is the largest item on this page that is not blocking.
4. **Does `CLIENT` get real connection state?** *Open.* It is a prerequisite for
   five commands; without it the family gives plausible wrong answers. Needed
   before the `CLIENT` half of tier 3; `COMMAND COUNT` does not wait on it.

**Status now.** The parity block overtook questions 1, 2 and 4. Streams,
scripting, `ACL` and `CLUSTER` are implemented, so they count toward parity;
the blocking commands are built with oldest-first order across workers; and
`CLIENT` has real connection state. Question 3 is settled without the sorted
index: all four scan commands have real cursors on both halves (see
[Incremental cursors](#incremental-cursors)).


## Tier 0 — correctness the harness never looks at

Everything above this line is about what we answer. This tier is about whether
the answer is right at all. None of it is parity debt in the sense the other
tiers are: the differential harness cannot see any of it, because it runs one
client, in sequence, against a freshly flushed database.

Three gaps were found by reading the code, with no test failing. **All three
now have tests, and the third had a crash behind it.**

- **The meta name chains had no leak test, and they are the one chain that has
  corrupted the pool.** *Closed.*
  `removing_a_meta_entry_releases_its_name_chain` drives all four meta tables
  through both removal paths (the per-type `remove_meta`/`remove_zset_meta`/
  `remove_count_meta` and the generic `remove_meta_of` that `DEL` and eviction
  use) against real dynahash tables, and asserts every chunk comes back.
  Deleting the `value_free` from any one of the four fails it, each with its own
  label. `a_meta_name_round_trips_through_the_pool_and_comes_back` covers the
  three slot accessors underneath.
- **Nothing tested concurrent clients for correctness.** *Closed.* The
  `Concurrent clients` describe runs eight connections on both halves against
  one counter, one list, one set, one hash and one sorted set, plus a reader
  racing a writer that keeps changing a value's size across the inline and chunk
  boundaries. It asserts totals, that a collection's meta count matches the
  members stored, and that no read is ever a splice of two writes.
  Giving `mem_incr` a read-modify-write window loses 11 of 200 increments and
  fails it.
- **Eviction under pressure was unproven end to end.** *Closed.* Writing past
  the chunk pool with the default `noeviction` policy **segfaulted the
  worker**, which takes the whole PostgreSQL instance down with it. Eviction
  was not at fault. The cause was `FLUSHDB`: `flush_table`
  removed entries without clearing the chains they named, so a later write into
  a recycled dynahash slot released those chunks a second time, spliced the free
  list into a cycle, and left `free_count` claiming nodes that were not there.
  `pool_alloc` trusted the count and walked off the list. It was written up in
  §3 of IMPLEMENTATION.md until phase 6 removed the chunk pool; the
  `Pressure and FLUSHDB` describe and
  `flushing_a_table_leaves_no_entry_naming_freed_chunks` are the regression.

The third item holds a lesson for the whole tier. It was the last item on the
list, reached by writing the test the doc asked for, and it found a different
defect from the one the entry predicted: a whole-instance crash reachable from
`FLUSHDB` and ordinary traffic, with no eviction involved. The entry was right
that the area was untested and wrong about what was in it.

Status now: the meta name chains, the chunk pool and the dynahash tables these
items describe went with the fixed tables in phase 6, and so did the three unit
tests named above. The `Concurrent clients` and `Pressure and FLUSHDB` e2e
describes still run.

The stale-collection read in [tier 1](#tier-1-commands-we-have-that-behave-differently)
belongs to this tier as much as to that one: `TYPE` answering `none` while
`LLEN` reports the old count is a client being served data that no longer
exists.


## Suggested order

Ordered by **data correctness first, parity second**. A goal of making a stock
client library work would give a different order. This order holds because a
missing command returns an error the client can see, and a wrong answer does
not. That puts all of tier 0 and tier 3 ahead of the 91 absences in tier 2,
even though tier 2 is where the visible feature gap is.

1. ~~**Tier 0.**~~ **Done.** All three items have tests on both halves, and the
   third turned up a worker segfault reachable from `FLUSHDB` plus ordinary
   traffic. See [tier 0](#tier-0-correctness-the-harness-never-looks-at).
2. ~~**The stale-collection read.**~~ **Done**, on both halves; this page had
   predicted one. See
   [above](#the-stale-collection-read-was-bigger-than-this-table-said); the
   durable half's fix costs throughput and the alternative is priced there.
3. ~~**Tier 3 introspection.**~~ `COMMAND COUNT` **done**: it is now derived
   from the coverage page instead of hardcoded; see [tier 3](#tier-3-introspection-that-answers-and-answers-wrongly).
   The `CLIENT` family still needs question 4 answered first, because the work
   is giving a connection an identity. The `CLIENT` family has since been
   built too.
4. ~~**`RANDOMKEY`'s skew.**~~ **Done**, both halves. On the durable half it
   was a wrong answer, which is worse than a skew. See [tier 1](#tier-1-commands-we-have-that-behave-differently).
5. ~~**The free wins in tier 2.**~~ **Done.** `TIME`, `HSTRLEN`, `TOUCH`,
   `ZINTERCARD`, and `DUMP`/`RESTORE`. The first four were mechanical as
   promised; `DUMP`/`RESTORE` needed `src/rdb.rs`, which implements the real
   serialisation format, with interchange verified against a live 7.0.15 in
   both directions. Adding `ZINTERCARD` surfaced the `ZUNION`-family set-input
   gap now recorded in [tier 1](#tier-1-commands-we-have-that-behave-differently).
6. ~~**Next: the `ZUNION` family's set-input gap.**~~ **Done**, on both halves:
   a plain set now reads as a sorted set whose scores are all 1
   (`src/composite.rs`). The plan below is kept as written, though the code it
   names on both halves (`zagg_body`, `src/mem.rs`) has since been replaced.
   It was the last known place this server gave a wrong answer instead of an
   error, so under this page's ordering it outranked everything below it. It covers six commands on both
   halves with one coherent fix, and needs no decision and no shared-memory
   change. Its shape is recorded here so the next session does not re-derive
   it. On the durable half,
   `zagg_body` (`src/commands.rs`) joins `redis.zset_{db}` only; the set
   tables need to enter the same body as score-1 rows (a `UNION ALL` of
   `SELECT key, member, 1.0 FROM redis.set_{db}` inside the join source
   covers Union/Inter; `Diff`'s two branches need the same treatment). On the
   memory half, `mem_zunionstore`/`mem_zinterstore`/`mem_zdiffstore`
   (`src/mem.rs`) read the zset table alone via `zset_collect`. Each needs a
   set-table fallback for a key with no zset entries, minding the lock order
   (zset lock is held; the set table has its own). `ZINTER`/`ZUNION`/`ZDIFF`
   without `STORE` go through the same two paths, and `ZINTERCARD` through
   `zagg_body`/`mem_zinterstore`, so the fix lands in exactly two places per
   half. The measured starting point is that `ZDIFF z s` answers `["m"]` where
   Redis answers `[]`, and `ZINTER z s` is empty where Redis answers `["m"]`.
   Those two probes, plus `STORE` and weight/aggregate variants over a set
   input, become the parity cases and e2e tests. Redis gives a set member
   score 1, and weights and aggregates then apply to it as to any other score.
7. **The sorted index, then `HSCAN`/`SSCAN`/`ZSCAN` and `SCAN` on top of it.**
   Question 3 is taken: real cursors on both halves. That makes this structural
   work, and it fixes `ZPOPMIN`'s O(entries in the database) at the same time,
   so plan the index once and use it for both. The unbounded cursor-0 reply it
   replaces is itself a correctness concern on a large keyspace, not only a
   parity one. **The first deliverable is a sizing memo.** It changes
   shared-memory layout, so size it with
   `pg_sys::hash_estimate_size` against [§3](IMPLEMENTATION.md#3-the-shared-memory-backend)
   and bring the number for sign-off before writing code. That instruction has
   paid for itself twice already. **Status now: done, without a sorted
   index.** Phase 6 replaced the fixed tables, so the `hash_estimate_size`
   sizing no longer applies, and the memory half's `ZPOPMIN` already reads
   from a sorted set kept in score order. The scan commands have real cursors
   on both halves; the only new shared memory is the durable half's cursor
   table, 4,096 entries of about 300 bytes
   ([SCAN cursors](IMPLEMENTATION.md#scan-cursors)).
8. ~~**Blocking.**~~ Deferred by question 2. The eight commands stay absent
   until the correctness work above is done. When it is taken up it changes how
   a connection is serviced, so it wants its own PR. **Done** since, with
   oldest-first order across workers; see
   [Blocking commands](#blocking-commands).
9. ~~**Finish the bitmap family**, then HyperLogLog, then GEO.~~ **Done.** Bitmaps reuse
   existing code, HyperLogLog is self-contained, and GEO is the largest.

Parked until their decisions are taken, and blocking nothing else: the
`CLIENT` family (question 4, real connection state) and the denominator
(question 1, whether streams, scripting, ACL and cluster are non-goals; they
are 50 of the 91 missing). Both have since been overtaken: `CLIENT` has real
connection state, and streams, scripting, `ACL` and `CLUSTER` are implemented.

One loose end deserves a slot in whichever block runs next: **the ~1-in-9 e2e
flake**. `AUTH with wrong password` accepts on a fresh connection and two
`TTL` tests desync behind it. It appeared once more during this block's
verification (one unidentified single-test failure in one of eight full runs,
with the other seven clean). It has still never been A/B'd against a pre-fix
build, so calling it pre-existing remains an assumption. Pinning it down means
capturing the failing test's name and reply (run the suite in a loop with
full output kept), then bisecting whether a fresh connection can race
`redis.password`'s GUC read.

Each block ends the way [§12](IMPLEMENTATION.md#12-open-work) requires: the
baseline shrinks by the stated count and by nothing else, and every behaviour it
changes gains an end-to-end test on **both** halves.
