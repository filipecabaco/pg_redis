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

**Re-measured against Redis 7.0.15 after the second correctness block.** The
probe now reads: 240 top-level commands in the reference, **149 accepted, 91
missing** — `TIME`, `TOUCH`, `HSTRLEN`, `ZINTERCARD`, `DUMP` and `RESTORE`
moved columns. `COMMAND COUNT` answers 149 and is derived, not asserted, so
the probe and the reply agree by construction. The tier 1 table gained a row
(`RANDOMKEY` closed, the `ZUNION` family's set-input gap opened) and the
throughput question tier 1 priced is decided and re-measured below.

A pattern worth carrying forward: this page has now been wrong three times,
each time in the same direction — the defect found was bigger than the entry
predicted. Eviction's entry hid a `FLUSHDB` segfault, the stale read's
"memory-only, one command" was both halves and sixteen, and `RANDOMKEY`'s
"skew" was a durable-half wrong answer. Measure before you plan.


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

What a client can actually observe. The four rows are written up in
[§12](IMPLEMENTATION.md#12-open-work) and collected here so the picture is in
one place; a fifth, found later, follows the table.

| Gap | Half | Status |
|---|---|---|
| `INCRBYFLOAT` precision — `f64` against Redis's `long double` | both | **Accepted.** The 2 baseline entries. See [phase 2](IMPLEMENTATION.md#phase-2-ranges-shapes-and-ties) |
| An expired collection outlives its expiry: `TYPE` answers `none` while `LLEN` still reports the old count, until the sweep runs | both | **Fixed.** It was never memory-only, and it was never one command — see [below](#the-stale-collection-read-was-bigger-than-this-table-said) |
| `SCAN`/`KEYS` return the whole keyspace in one reply, cursor always 0 | both | Open. See [Incremental cursors](#incremental-cursors) below |
| `RANDOMKEY` returns the first live key the KV scan reaches, and consults the collection metas only when there is none | ~~memory~~ both | **Fixed**, and it was worse than this row said: the durable half read `kv_N` alone, so a database holding only collections answered *nil* while `DBSIZE` counted them — a wrong answer, not a skew. Both halves now draw uniformly over every live key: the durable half with a five-table `UNION` under `ORDER BY random()`, the memory half with a single-pass reservoir over the KV scan and the collection names. An expired-but-unswept collection is excluded on both. The e2e `RANDOMKEY` describes pin all three properties per half |

A fifth entered the table while `ZINTERCARD` was being added, found by probing
the live pair rather than predicted: **the `ZUNION`/`ZINTER`/`ZDIFF` family
ignores a plain-set input where Redis treats it as a sorted set whose scores
are all 1.** Both halves, `STORE` variants and `ZINTERCARD` included — the
type gate admits the set (`ZSET_OR_SET`), but the aggregation reads only the
zset table, so `ZDIFF z s` answers members that Redis says are cancelled and
`ZINTER z s` answers empty where Redis intersects. Measured: with `z` a zset
holding `m` and `s` a set holding `m`, `ZDIFF 2 z s` is `["m"]` here and `[]`
on Redis. `ZINTERCARD` inherits the behaviour deliberately — its answer must
equal `ZINTER`'s cardinality, and shipping the fix in one command of the
family while five others disagree would be worse than the shared gap. Open,
both halves, and the fix is one piece of work across the six commands — it is
the next block, and its shape is sketched in
[Suggested order](#suggested-order) so a fresh session can start from the
code rather than from this table.

`ZPOPMIN` being O(entries in the database) is a performance defect rather than a
parity one, so it stays in §12 and is not repeated here.

### The stale-collection read was bigger than this table said

The row above used to read "memory", and to describe one command. Measured
against a live pair before touching it, it was **both halves and sixteen read
commands**: `LLEN` `LRANGE` `LINDEX`, `SCARD` `SMEMBERS` `SISMEMBER`
`SRANDMEMBER`, `HLEN` `HGETALL` `HGET` `HKEYS`, and `ZCARD` `ZRANGE` `ZSCORE`
`ZCOUNT`. Strings were correct on both halves throughout, which is not luck:
`kv_N` carries its own `expires_at` and every statement reading one tests it.
A collection's rows carry nothing — the deadline lives in the key directory or
in `redis.expiry_N` — so a read of the rows cannot tell a live key from one the
sweep has not reached.

Both halves had the same shape of bug and needed different fixes, because they
disagree about when the type of a key is checked.

- **Memory.** `dir_lookup` folded "expiry has passed" onto "absent", so
  `mem_type_error` let the command through and the read went to the tables.
  `dir_lookup_raw` keeps the distinction and `mem_key_kind_reaping` acts on it,
  in the one place every memory command already passes through. Same single
  directory lookup as before, so it costs nothing.
- **Durable.** Reads deliberately skip the type check: a read runs first and
  only asks what the key holds if it came back empty, which keeps a round trip
  off every successful `GET`. That optimisation is exactly what made the stale
  read reachable — coming back with data is not evidence the key is live. A
  collection read now reaps first, with a keyed form of the sweep statement.

**That cost about 63% of durable-half collection-read throughput as first
shipped** (`redis-benchmark -n 20000 -c 50`): `HGET` 35.2k → 12.1k, `LLEN`
33.2k → 12.2k, `ZCARD` 30.5k → 11.4k, `SCARD` 27.0k → 11.4k. `GET` unchanged
at ~32k. The price was the round trip and nothing else: the first attempt
routed reads through `sql_type_error` — five `EXISTS` across five tables — and
replacing that with one indexed lookup on `redis.expiry_N` that usually
deletes nothing moved `HGET` only 11.3k → 12.1k. A second statement on the
read path costs roughly two thirds of throughput whatever it does.

**Decision taken: the hybrid.** The thirteen cached single-key collection
reads — `HGET` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HSTRLEN`,
`SMEMBERS` `SISMEMBER` `SCARD`, `ZCARD` `ZSCORE`, `LLEN` — carry the liveness
predicate inside their own statement (`sql::live`, an InitPlan: one probe of
`redis.expiry_N` per query, not per row), and skip the up-front reap. The
other ~56 inline statements — the `EXCEPT` chains and `unnest` joins — keep
reaping first, one mechanism that cannot be missed by a statement that forgot
its predicate. Both are chosen in `Command::execute` and nowhere else, and a
command misclassified in either direction is survivable: off the list it pays
a reap it did not need, on the list wrongly it fails its expired-key e2e test.
Guarding all 68 was rejected as ~56 chances at a silent stale read, bought
with a source-scanning test, to speed up commands that are not hot.

Re-measured after the change, same recipe, on a container where `GET` runs
38–50k: the guarded reads are back at full speed — `HGET` 32.2–34.2k, `LLEN`
32.6k, `ZCARD` 29.1k, `SCARD` 33.4k, `SISMEMBER` 30.1k, `HSTRLEN` 29.3k —
while the reads still paying the reap sit at a third of that (`LINDEX` 10.3k,
`LRANGE` 9.7k, measured in the same minute). The reap also still runs for a
guarded command whose read found nothing, before the type check — that keeps
`LLEN` on an expired hash answering 0 rather than `WRONGTYPE`, cleans the rows
without waiting for the sweep, and costs only the miss path.

## Tier 2 — commands we do not implement

**149 of 240 accepted, 91 missing.** Grouped by what a client loses, which is
the same ordering [§12's phase plan](IMPLEMENTATION.md#the-plan) used and for
the same reason: a command that answers wrongly costs more than one that is
absent, and an absent command a client actually calls costs more than one it
does not.

The tier-2 "free wins" — `TIME`, `TOUCH`, `HSTRLEN`, `ZINTERCARD`, and
`DUMP`/`RESTORE` — are done and no longer counted below. The first four were
as mechanical as predicted. `DUMP`/`RESTORE` were not "mostly done already" in
the way the old note here implied: `dump_key`/`restore_key` supplied the
traversal, but Redis's serialisation format is its own module (`src/rdb.rs`) —
RDB value encoding, listpack and intset readers, LZF decompression, CRC-64.
Payloads interchange with a real 7.0 in both directions, verified live;
version 11+ payloads and pre-7 compact encodings are refused loudly, as
`docs/commands.md` records.

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
| **Cluster, replication, admin** | 27 | `ACL` `CLUSTER` `REPLICAOF` `DEBUG` `MONITOR` `SLOWLOG` `WAIT` `MIGRATE` … |

Notes on individual entries worth knowing before planning:

- **Blocking is the single biggest gap.** It is the queue use case, and it is
  the one thing on this list that is architectural rather than mechanical.
- **`HSCAN`/`SSCAN`/`ZSCAN` are what client libraries use to iterate a
  collection.** Their absence is felt through the library, not through the
  command.
- **Bitmaps are half-built.** `SETBIT`, `GETBIT` and `BITCOUNT` landed in
  phase 5; `BITOP`/`BITPOS`/`BITFIELD` finish the family and can reuse
  `bit_at`/`set_bit_at`/`count_bits`/`byte_range` in `commands.rs`.
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
| `COMMAND COUNT` | ~~`100`~~ **149** — fixed | `240` |
| `COMMAND DOCS` / `INFO` / `LIST` | *(empty)* | populated |
| `CLIENT ID` | `0`, always | unique per connection |

`COMMAND COUNT` was a hardcoded `Response::Integer(100)`. It now answers with
the number of rows in [docs/command-coverage.md](command-coverage.md), counted
at compile time — the page is asserted against the parser in both directions,
so the reply, the page and the parser cannot drift apart in pairs. The
aliasing question the fix forced: `SUBSTR` now has its own row rather than a
note on `GETRANGE`, because Redis's own command table counts it separately.
The honest answer is the number *we* accept, not Redis's 240 — a client asking
is asking about this server.

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

Nothing below can be settled from the code. Two of the four are now taken.

1. **Are streams, scripting, ACL and cluster non-goals?** *Open.* 50 of the 97
   missing commands. They read as out of scope for a Postgres-backed Redis
   endpoint, but that has never been said out loud, so they are still counted as
   parity debt on this page. Saying so would move the honest denominator from
   240 to ~190 and make the number mean something.
2. **Is polled blocking acceptable, losing FIFO fairness?** **Taken: neither —
   blocking is deferred.** Not polling, and not the cross-process wake
   infrastructure either. The eight blocking commands stay missing for now, on
   the same principle that orders this page: an absent command errors, and an
   error is honest, where a `BLPOP` that quietly serves the wrong waiter is a
   difference no test here would catch. Revisit once the correctness work is
   done, and take the decision then rather than inheriting it.
3. **Real `SCAN` cursors, or the cursor-0 convention extended to the
   collections?** **Taken: real, on both halves, via a sorted index.** Not the
   cheap split. The durable half gets `ORDER BY` plus a cursor predicate; the
   memory half gets the sorted index it needs to answer honestly — which is the
   same structure `ZPOPMIN`'s O(entries in the database) wants, so the two
   should be planned as one piece of work rather than two. It changes the
   shared-memory layout, so it wants sizing against
   [§3](IMPLEMENTATION.md#3-the-shared-memory-backend) before any code, and it
   is the largest item on this page that is not blocking.
4. **Does `CLIENT` get real connection state?** *Open.* It is a prerequisite for
   five commands and is otherwise a family of plausible lies. Needed before the
   `CLIENT` half of tier 3; `COMMAND COUNT` does not wait on it.


## Tier 0 — correctness the harness never looks at

Everything above this line is about *what* we answer. This tier is about whether
the answer is right at all, and none of it is parity debt in the sense the other
tiers are — the differential harness cannot see any of it, because it runs one
client, in sequence, against a freshly flushed database.

Three gaps, found by reading rather than by any test failing. **All three now
have tests, and the third had a crash behind it.**

- **The meta name chains had no leak test, and they are the one chain that has
  actually corrupted the pool.** *Closed.*
  `removing_a_meta_entry_releases_its_name_chain` drives all four meta tables
  through both removal paths — the per-type `remove_meta`/`remove_zset_meta`/
  `remove_count_meta` and the generic `remove_meta_of` that `DEL` and eviction
  use — against real dynahash tables, and asserts every chunk comes back.
  Deleting the `value_free` from any one of the four fails it, each with its own
  label. `a_meta_name_round_trips_through_the_pool_and_comes_back` covers the
  three slot accessors underneath.
- **Nothing tested concurrent clients for correctness.** *Closed.* The
  `Concurrent clients` describe runs eight connections on both halves against
  one counter, one list, one set, one hash and one sorted set, plus a reader
  racing a writer that keeps changing a value's size across the inline and chunk
  boundaries. It asserts totals, that a collection's meta count matches the
  members actually stored, and that no read is ever a splice of two writes.
  Giving `mem_incr` a read-modify-write window loses 11 of 200 increments and
  fails it.
- **Eviction under pressure was unproven end to end.** *Closed, and it was not
  eviction that was broken.* Writing past the chunk pool with the default
  `noeviction` policy **segfaulted the worker**, which takes the whole
  PostgreSQL instance down with it. The cause was `FLUSHDB`: `flush_table`
  removed entries without clearing the chains they named, so a later write into
  a recycled dynahash slot released those chunks a second time, spliced the free
  list into a cycle, and left `free_count` claiming nodes that were not there.
  `pool_alloc` trusted the count and walked off the list. Written up in
  [§3](IMPLEMENTATION.md#an-entry-must-not-outlive-the-chunks-it-names); the
  `Pressure and FLUSHDB` describe and
  `flushing_a_table_leaves_no_entry_naming_freed_chunks` are the regression.

What that third one says about the tier is worth keeping. It was the last item
on the list, reached by writing the test the doc asked for, and the defect it
found was not the one the entry predicted — it was a whole-instance crash
reachable from `FLUSHDB` and ordinary traffic, with no eviction involved. The
entry was right that the area was untested and wrong about what was in it.

The stale-collection read in [tier 1](#tier-1--commands-we-have-that-behave-differently)
belongs to this tier as much as to that one: `TYPE` answering `none` while
`LLEN` reports the old count is a client being served data that no longer
exists.


## Suggested order

Ordered by **data correctness first, parity second** — which is not the order a
"make a stock client library work" goal would give. The difference matters, so
it is worth saying why: a missing command errors, and an error is honest. A
wrong answer is not. That puts all of tier 0 and tier 3 ahead of the 91 absences
in tier 2, even though tier 2 is where the visible feature gap is.

1. ~~**Tier 0.**~~ **Done.** All three items have tests on both halves, and the
   third turned up a worker segfault reachable from `FLUSHDB` plus ordinary
   traffic. See [tier 0](#tier-0--correctness-the-harness-never-looks-at).
2. ~~**The stale-collection read.**~~ **Done**, on both halves rather than the
   one this page predicted. See
   [above](#the-stale-collection-read-was-bigger-than-this-table-said); the
   durable half's fix costs throughput and the alternative is priced there.
3. ~~**Tier 3 introspection.**~~ `COMMAND COUNT` **done** — derived from the
   coverage page rather than patched, see [tier 3](#tier-3--introspection-that-answers-and-answers-wrongly).
   The `CLIENT` family still needs question 4 answered first, because giving a
   connection an identity is the actual work.
4. ~~**`RANDOMKEY`'s skew.**~~ **Done**, both halves — and on the durable half
   it was a wrong answer, not a skew. See [tier 1](#tier-1--commands-we-have-that-behave-differently).
5. ~~**The free wins in tier 2.**~~ **Done.** `TIME`, `HSTRLEN`, `TOUCH`,
   `ZINTERCARD`, and `DUMP`/`RESTORE`. The first four were mechanical as
   promised; `DUMP`/`RESTORE` needed `src/rdb.rs` — the real serialisation
   format, interchange verified against a live 7.0.15 in both directions.
   Adding `ZINTERCARD` surfaced the `ZUNION`-family set-input gap now recorded
   in [tier 1](#tier-1--commands-we-have-that-behave-differently).
6. **Next: the `ZUNION` family's set-input gap** — the last known place this
   server gives a wrong answer rather than an error, which under this page's
   ordering outranks everything below it. Six commands, both halves, one
   coherent fix, no decision needed and no shared-memory change. The shape of
   it, so the next session does not re-derive it: on the durable half,
   `zagg_body` (`src/commands.rs`) joins `redis.zset_{db}` only — the set
   tables need to enter the same body as score-1 rows (a `UNION ALL` of
   `SELECT key, member, 1.0 FROM redis.set_{db}` inside the join source
   covers Union/Inter; `Diff`'s two branches need the same treatment). On the
   memory half, `mem_zunionstore`/`mem_zinterstore`/`mem_zdiffstore`
   (`src/mem.rs`) read the zset table alone via `zset_collect` — each needs a
   set-table fallback for a key with no zset entries, minding the lock order
   (zset lock is held; the set table has its own). `ZINTER`/`ZUNION`/`ZDIFF`
   without `STORE` go through the same two paths, and `ZINTERCARD` through
   `zagg_body`/`mem_zinterstore`, so the fix lands in exactly two places per
   half. Measured starting point: `ZDIFF z s` answers `["m"]` where Redis
   answers `[]`, `ZINTER z s` empty where Redis answers `["m"]` — those two
   probes, plus `STORE` and weight/aggregate variants over a set input,
   become the parity cases and e2e tests. Redis's semantics to match: a set
   member carries score 1, weights and aggregates then apply to it as to any
   other score.
7. **The sorted index, then `HSCAN`/`SSCAN`/`ZSCAN` and `SCAN` on top of it.**
   Question 3 is taken: real cursors on both halves. That makes this structural
   rather than mechanical, and it absorbs `ZPOPMIN`'s O(entries in the database)
   at the same time, so plan the index once and spend it twice. The unbounded
   cursor-0 reply it replaces is itself a correctness concern on a large
   keyspace, not only a parity one. **The first deliverable is a sizing memo,
   not a diff**: it changes shared-memory layout, so size it with
   `pg_sys::hash_estimate_size` against [§3](IMPLEMENTATION.md#3-the-shared-memory-backend)
   and bring the number for sign-off before writing code. That instruction has
   paid for itself twice already.
8. ~~**Blocking.**~~ Deferred by question 2 — the eight commands stay absent
   until the correctness work above is done. When it is taken up it changes how
   a connection is serviced, so it wants its own PR.
9. **Finish the bitmap family**, then HyperLogLog, then GEO — bitmaps reuse code
   that exists, HyperLogLog is self-contained, GEO is the largest.

Parked until their decisions are taken, and blocking nothing else: the
`CLIENT` family (question 4 — real connection state) and the honest
denominator (question 1 — are streams, scripting, ACL and cluster non-goals?
50 of the 91 missing).

A loose end worth a slot in whichever block runs next: **the ~1-in-9 e2e
flake**. `AUTH with wrong password` accepts on a fresh connection and two
`TTL` tests desync behind it. It bit once more during this block's
verification — one unidentified single-test failure in one of eight full
runs, seven clean — and it has still never been A/B'd against a pre-fix
build, so "pre-existing" remains an assumption. Pinning it down means
capturing the failing test's name and reply (run the suite in a loop with
full output kept), then bisecting whether a fresh connection can race
`redis.password`'s GUC read.

Each block ends the way [§12](IMPLEMENTATION.md#12-open-work) requires: the
baseline shrinks by the stated count and by nothing else, and every behaviour it
changes gains an end-to-end test on **both** halves.
