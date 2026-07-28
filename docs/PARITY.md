# Parity roadmap

What is left between pg_redis and Redis, why the headline number does not say
so, and what has to be decided before the next block of work starts.

[Section 12 of IMPLEMENTATION.md](IMPLEMENTATION.md#12-open-work) records the
parity work that is **done** — six phases, 189 differences down to 2. This page
is the other half: what that number does not reach, measured rather than
recalled.

Written against `claude/pg-redis-member-array-b41dkl` at the point it went in.
Re-measure before trusting any table here; the recipe is below and takes two
minutes.


## How to pick this up

Everything on this page came from probing a live pair of servers, not from
reading the source. Redo it first — a merged PR or two will have moved it.

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
names — filter them out with `grep -v '|'` or every container we *do* implement
reads as missing. And it classifies by the reply to a **zero-argument** call, so
a command that exists reports an arity error, which is not `unknown command` and
therefore counts as present. That is the right answer for this purpose.

Reference used here: Redis 7.0.15, 240 top-level commands.


## Where parity actually stands

**2 differences over 1,394 replies.** Both are the same accepted cause, once per
storage half: `INCRBYFLOAT` is `f64` where Redis uses `long double`.

That figure is narrower than it sounds, in two specific ways, and neither is a
flaw in the harness so much as a limit of what a differential harness can be:

- **It compares commands both servers implement.** Every command we do not
  implement is invisible to it by construction. 97 of Redis's 240 are in that
  set.
- **Two comparisons are deliberately loose.** `RANDOMKEY` is compared by shape,
  because any live key is a valid answer. `TTL` is compared by *class* — gone,
  no expiry, some expiry — which was itself a tightening; it used to compare by
  reply shape, so `-1` and `100` looked alike and every expiry bug was
  invisible.

So: the gate is worth keeping and worth trusting for what it covers. It is not a
coverage measure, and this page exists because nothing else was one.


## Tier 1 — commands we have that behave differently

The four that a client can actually observe. All four are already written up in
[§12](IMPLEMENTATION.md#12-open-work); they are collected here so the picture is
in one place.

| Gap | Half | Status |
|---|---|---|
| `INCRBYFLOAT` precision — `f64` against Redis's `long double` | both | **Accepted.** The 2 baseline entries. See [phase 2](IMPLEMENTATION.md#phase-2-ranges-shapes-and-ties) |
| An expired collection outlives its expiry: `TYPE` answers `none` while `LLEN` still reports the old count, until the sweep runs | memory | Open, ~1s window. Fix is for `mem_type_error` to *reap* rather than ignore, which needs `dir_lookup` to tell "absent" from "expired" |
| `SCAN`/`KEYS` return the whole keyspace in one reply, cursor always 0 | both | Open. See [Incremental cursors](#incremental-cursors) below |
| `RANDOMKEY` returns the first live key the KV scan reaches, and consults the collection metas only when there is none | memory | Open. Valid key either way, but a database of mostly collections will see strings far more often than it should |

`ZPOPMIN` being O(entries in the database) is a performance defect rather than a
parity one, so it stays in §12 and is not repeated here.


## Tier 2 — commands we do not implement

**143 of 240 accepted, 97 missing.** Grouped by what a client loses, which is
the same ordering [§12's phase plan](IMPLEMENTATION.md#the-plan) used and for
the same reason: a command that answers wrongly costs more than one that is
absent, and an absent command a client actually calls costs more than one it
does not.

| Group | # | Commands |
|---|---:|---|
| **Blocking** | 8 | `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BLMPOP` `BZPOPMIN` `BZPOPMAX` `BZMPOP` |
| **Incremental scan** | 3 | `HSCAN` `SSCAN` `ZSCAN` |
| **Plain data and keyspace** | 12 | `HSTRLEN` `TOUCH` `MOVE` `SWAPDB` `TIME` `SORT` `SORT_RO` `LMPOP` `ZMPOP` `ZINTERCARD` `ZRANGESTORE` `LCS` |
| **Bitmaps** | 4 | `BITOP` `BITPOS` `BITFIELD` `BITFIELD_RO` |
| **HyperLogLog** | 5 | `PFADD` `PFCOUNT` `PFMERGE` `PFDEBUG` `PFSELFTEST` |
| **GEO** | 10 | `GEOADD` `GEODIST` `GEOHASH` `GEOPOS` `GEOSEARCH` `GEOSEARCHSTORE` `GEORADIUS`(+`_RO`) `GEORADIUSBYMEMBER`(+`_RO`) |
| **Serialisation** | 2 | `DUMP` `RESTORE` |
| **Sharded pub/sub** | 3 | `SPUBLISH` `SSUBSCRIBE` `SUNSUBSCRIBE` |
| **Streams** | 15 | `XADD` and friends |
| **Scripting and functions** | 8 | `EVAL` `EVAL_RO` `EVALSHA` `EVALSHA_RO` `FCALL` `FCALL_RO` `FUNCTION` `SCRIPT` |
| **Cluster, replication, admin** | 27 | `ACL` `CLUSTER` `REPLICAOF` `DEBUG` `MONITOR` `SLOWLOG` `WAIT` `MIGRATE` … |

Notes on individual entries worth knowing before planning:

- **Blocking is the single biggest gap.** It is the queue use case, and it is
  the one thing on this list that is architectural rather than mechanical.
- **`HSCAN`/`SSCAN`/`ZSCAN` are what client libraries use to iterate a
  collection.** Their absence is felt through the library, not through the
  command.
- **`ZINTERCARD` is a one-sided family.** `SINTERCARD` exists; its sorted-set
  twin does not.
- **Bitmaps are half-built.** `SETBIT`, `GETBIT` and `BITCOUNT` landed in
  phase 5; `BITOP`/`BITPOS`/`BITFIELD` finish the family and can reuse
  `bit_at`/`set_bit_at`/`count_bits`/`byte_range` in `commands.rs`.
- **`DUMP`/`RESTORE` are mostly done already.** `dump_key`/`restore_key` and the
  `KeyDump` enum exist for cross-half `COPY`; what is missing is Redis's
  serialisation format and its CRC, not the traversal.
- **GEO is a sorted set with a geohash score** — self-contained, no interaction
  with anything built, and the only group here that is more arithmetic than
  plumbing.

**Keyspace notifications** (`__keyspace@N__` / `__keyevent@N__`) are absent and
are not a command, so they appear nowhere in the table above. The pub/sub
machinery to carry them already exists.


## Tier 3 — introspection that answers, and answers wrongly

This tier deserves its own attention because a wrong answer is worse than an
absent one: a client branches on these, and a plausible lie misleads where an
error would not.

Measured side by side against the reference:

| | pg_redis | Redis |
|---|---|---|
| `OBJECT REFCOUNT k` | *(nil)* | `1` |
| `OBJECT IDLETIME k` | *(nil)* | `0` |
| `OBJECT FREQ k` | *(nil)* | error unless LFU is on |
| `CONFIG GET maxmemory` | *(empty)* | `maxmemory 0` |
| `COMMAND COUNT` | `100` | `240` |
| `COMMAND DOCS` / `INFO` / `LIST` | *(empty)* | populated |
| `CLIENT ID` | `0`, always | unique per connection |

`COMMAND COUNT` is a hardcoded `Response::Integer(100)` (`src/commands.rs`,
search `CmdCount`) and we accept 143. It is wrong by inspection and is a
one-line fix — the smallest genuine parity win on this page.

`CLIENT ID` always returning 0 is the visible edge of connection state not being
tracked at all, which is also what blocks `CLIENT KILL`, `LIST`, `GETNAME` and
`UNPAUSE`. Fixing the family means giving a connection an identity, not patching
the reply.


## The two architectural blockers

Everything in tier 2 below these two is mechanical. These are not, and both were
investigated far enough to know why.

### Blocking commands

The threading model decides this, so it goes first. Per worker there is **one
dispatcher thread** — the only thread with a valid `MyProc`, and therefore the
only one that may touch PostgreSQL or take an LWLock — plus one thread per
connection. A connection thread parses, sends a `DispatchMsg` down a
`sync_channel`, and blocks on `resp_rx.recv()` until the dispatcher answers.

That yields the shape of the answer directly:

- **A blocking command must never block the dispatcher.** It occupies the one
  thread every other client is queued behind. `BLPOP` has to execute as a
  non-blocking `LPOP` attempt that reports "nothing there", with the waiting
  done on the connection thread.
- **Waking has to be a poll, not a signal.** `redis.workers` defaults to **4**,
  and a worker is a *process*. A client blocked on worker 2 can be unblocked by
  an `RPUSH` that lands on worker 3, so any wake mechanism has to cross
  processes. In memory mode shared memory could carry one; on the durable half
  it would need `LISTEN`/`NOTIFY` or equivalent. Polling is the only mechanism
  that works in both modes with no new infrastructure — and there is precedent
  in the tree: `subscribe_loop` already polls with a 5 ms floor backing off to
  250 ms while quiet.
- **Polling costs FIFO fairness.** Redis serves the client that has waited
  longest. A poll serves whoever wakes up first. That is an observable
  difference and the harness would not catch it, so it needs to be a decision
  taken deliberately rather than a consequence discovered later.
- **`BLPOP` inside `MULTI` must not block.** Redis returns the nil reply
  immediately. `EXEC` arrives as `DispatchMsg::Batch`, which is a distinct
  variant from `DispatchMsg::Cmd` — so the two contexts are already
  distinguishable at exactly the point where the decision has to be made. Nothing
  new is needed for this; it just has to not be forgotten.

### Incremental cursors

`SCAN` parses `cursor` and `COUNT` into fields named `_cursor` and `_count` —
underscore-prefixed because they are unused — and returns every matching key
with cursor 0. `HSCAN`/`SSCAN`/`ZSCAN` do not exist at all.

Adding the three commands with the same "cursor always 0" convention is
mechanical and would unblock client libraries. Making any of the four *real* is
not, on the memory half:

- Redis's guarantee is that an element present for the whole iteration is
  returned at least once, delivered by a reverse-binary-increment walk over hash
  buckets. Our tables are dynahash, whose bucket layout is not public API, so
  that walk cannot be reproduced directly.
- A `hash_seq_search` scan cannot be parked between commands — it would have to
  hold the table across an unbounded client think-time.
- The cheap alternative is to make the member hash the cursor and, on each call,
  scan the table for hashes above it. Correct, and O(n) per call — so O(n²/COUNT)
  to iterate a collection, which is the same shape of defect as `ZPOPMIN`
  already has and would be a poor thing to add deliberately.

The durable half has no such problem: `ORDER BY` plus a cursor predicate is an
index scan.

So an honest split is likely: real cursors on the durable half, cursor-0 on the
memory half, and the difference documented — or cursor-0 on both until the
sorted-index work that `ZPOPMIN` also wants gets done. That is a decision, not a
detail.


## Decisions needed before the next block

Nothing below can be settled from the code.

1. **Are streams, scripting, ACL and cluster non-goals?** 50 of the 97 missing
   commands. They read as out of scope for a Postgres-backed Redis endpoint, but
   that has never been said out loud, so they are still counted as parity debt
   on this page. Saying so would move the honest denominator from 240 to ~190
   and make the number mean something.
2. **Is polled blocking acceptable, losing FIFO fairness?** The alternative is
   cross-process wake infrastructure, which is a much larger piece of work and
   has to be built twice, once per storage half.
3. **Real `SCAN` cursors, or the cursor-0 convention extended to the
   collections?** See above. Cheap and consistent, or correct and structural.
4. **Does `CLIENT` get real connection state?** It is a prerequisite for five
   commands and is otherwise a family of plausible lies.


## Tier 0 — correctness the harness never looks at

Everything above this line is about *what* we answer. This tier is about whether
the answer is right at all, and none of it is parity debt in the sense the other
tiers are — the differential harness cannot see any of it, because it runs one
client, in sequence, against a freshly flushed database.

Three gaps, found by reading rather than by any test failing:

- **The meta name chains have no leak test, and they are the one chain that has
  actually corrupted the pool.** `mem.rs`'s test module checks that every
  allocated chunk comes back for KV entries
  (`freeing_a_kv_entry_releases_both_its_chains`) and for collection entries
  (`freeing_a_collection_entry_releases_every_chain`). The four meta tables'
  name chains — added when `KEYS`/`SCAN` learned to see collections — appear in
  that module only in the size-budget assertion. That is precisely the chain
  `remove_meta_of` failed to free: the next occupant of the slot inherited a
  head pointing into the free list and the worker aborted. The bug was found by
  a crash, fixed, and never given a regression test. `pool_free_chunks` already
  gives the invariant; the test is a few lines and should exist before anything
  else is built on top.
- **Nothing tests concurrent clients for correctness.** `redis.workers` defaults
  to 4 *processes* sharing one set of HTABs and pools. The only concurrent code
  in the tree is `bench_pubsub.ts`, which measures throughput and asserts
  nothing about the data. Two clients incrementing the same counter, pushing to
  the same list, or racing an eviction against a read have never been checked to
  produce a defensible result.
- **Eviction under pressure is unproven end to end.** `key_evictor!` drops keys
  when a table fills. Its *sampling* is unit-tested
  (`the_eviction_sample_keeps_the_lowest_ranked_candidates`); what a client sees
  when its key is evicted mid-sequence is not.

The stale-collection read in [tier 1](#tier-1--commands-we-have-that-behave-differently)
belongs to this tier as much as to that one: `TYPE` answering `none` while
`LLEN` reports the old count is a client being served data that no longer
exists.


## Suggested order

Ordered by **data correctness first, parity second** — which is not the order a
"make a stock client library work" goal would give. The difference matters, so
it is worth saying why: a missing command errors, and an error is honest. A
wrong answer is not. That puts all of tier 0 and tier 3 ahead of the 97 absences
in tier 2, even though tier 2 is where the visible feature gap is.

1. **Tier 0.** The meta-name leak test first, since it is a few lines and guards
   a failure mode that has already happened once. Then a concurrent-client
   correctness test on both halves, then eviction under pressure. No decisions
   needed for any of it.
2. **The stale-collection read.** Make `dir_lookup` distinguish absent from
   expired and have `mem_type_error` reap rather than ignore. This is the only
   case where we knowingly serve data for a key we also report as gone.
3. **Tier 3 introspection.** `COMMAND COUNT` is wrong by inspection and is one
   line. The `CLIENT` family needs question 4 answered first, because giving a
   connection an identity is the actual work.
4. **`RANDOMKEY`'s skew**, which is a sampling-correctness bug wearing a
   parity hat.
5. **The free wins in tier 2.** `TIME`, `HSTRLEN`, `TOUCH`, `ZINTERCARD`, and
   `DUMP`/`RESTORE` on top of the existing `dump_key`. Mechanical, no decisions.
6. **`HSCAN`/`SSCAN`/`ZSCAN`**, under whatever comes out of question 3. Note
   that the unbounded cursor-0 reply is itself a correctness concern on a large
   keyspace, not only a parity one.
7. **Blocking**, under whatever comes out of question 2. Largest and most
   valuable; it changes how a connection is serviced, so it wants its own PR.
8. **Finish the bitmap family**, then HyperLogLog, then GEO — bitmaps reuse code
   that exists, HyperLogLog is self-contained, GEO is the largest.

Each block ends the way [§12](IMPLEMENTATION.md#12-open-work) requires: the
baseline shrinks by the stated count and by nothing else, and every behaviour it
changes gains an end-to-end test on **both** halves.
