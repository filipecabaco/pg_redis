//! Eviction: Redis 7.2's `performEvictions`, over the memory engine's
//! databases.
//!
//! Before a command, when the arena is past the limit, keys are evicted
//! under `maxmemory-policy` until it is back under:
//!
//! - **`allkeys-lru`, `allkeys-lfu`, `volatile-lru`, `volatile-lfu`,
//!   `volatile-ttl`**: every database with keys the policy may take gives
//!   `maxmemory-samples` keys drawn at random, each scored by idle time,
//!   inverse frequency or nearness of its deadline, to a pool of the best 16
//!   candidates (`memdb::lru::Pool`). The best candidate still present is
//!   evicted. The pool outlives the round, which is what makes five samples
//!   approximate true LRU closely.
//! - **`allkeys-random`, `volatile-random`**: a random key, from each
//!   database in turn.
//!
//! The `volatile-*` policies draw only from keys with a deadline
//! (`memdb::keyspace`), as Redis draws from its `expires` dictionary.
//!
//! Each evicted key is reported as Redis reports one: INFO's `evicted_keys`,
//! the `evicted` keyspace event, a `CLIENT TRACKING` invalidation, and a
//! broken WATCH. A round stops after `maxmemory-eviction-tenacity`'s time
//! limit, checked every 16 keys, and the worker's next tick carries on, as
//! Redis's `evictionTimeProc` does. Runs on a worker's main thread only, never
//! inside `shm::with_db`.
#![forbid(unsafe_code)]

use std::cell::{Cell, RefCell};

use crate::access::Policy;
use crate::memdb::Score;
use crate::memdb::lru::Pool;
use crate::shm::{self, NUM_SLOTS};

/// `performEvictions`' result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evict {
    /// Under the limit, or back under it.
    Ok,
    /// Still over, with the time limit reached: carried on later. Commands
    /// run meanwhile.
    Running,
    /// Over the limit with nothing the policy may evict: a command that may
    /// grow memory is refused.
    Fail,
}

thread_local! {
    static POOL: RefCell<Pool<(usize, Vec<u8>)>> = const { RefCell::new(Pool::new()) };
    static NEXT_SLOT: Cell<usize> = const { Cell::new(0) };
    /// Inside [`before_run`]: whether its round left room.
    static RUN_ROOM: Cell<Option<bool>> = const { Cell::new(None) };
    /// Inside a script declared `allow-oom`: its commands may grow memory
    /// past the limit.
    static ALLOW_OOM: Cell<bool> = const { Cell::new(false) };
}

/// Run `f`, a script's body, with its commands let past `maxmemory` when
/// `allow` (`allow-oom`, or `no-writes`, which implies it).
pub fn allowing_oom<R>(allow: bool, f: impl FnOnce() -> R) -> R {
    /// Puts the outer value back, on unwinding too.
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            ALLOW_OOM.set(self.0);
        }
    }
    let _restore = Restore(ALLOW_OOM.replace(allow));
    f()
}

/// Before a command: evict what the policy allows. False when the command,
/// if it may grow memory, must be refused. Inside a transaction or script,
/// the answer of the round before it, with nothing evicted.
pub fn make_room() -> bool {
    ALLOW_OOM.get() || RUN_ROOM.get().unwrap_or_else(|| perform() != Evict::Fail)
}

/// Run `f`, a transaction, script or multi-step command, after one round of
/// eviction: Redis evicts before such a command and never inside it, where
/// a key vanishing between two steps would break its atomicity. Each step's
/// `make_room` answers by that round.
pub fn before_run<R>(f: impl FnOnce() -> R) -> R {
    let room = perform() != Evict::Fail;
    /// Puts the outer run's answer back, on unwinding too.
    struct Restore(Option<bool>);
    impl Drop for Restore {
        fn drop(&mut self) {
            RUN_ROOM.set(self.0);
        }
    }
    let _restore = Restore(RUN_ROOM.replace(Some(room)));
    f()
}

pub fn perform() -> Evict {
    if !shm::over_limit() {
        crate::server::eviction_exceeded(false);
        return Evict::Ok;
    }
    let s = crate::access::settings();
    let result = if s.policy == Policy::NoEviction {
        Evict::Fail
    } else {
        let cycle = std::time::Instant::now();
        let r = evict_until_under(s);
        crate::latency::record(
            crate::latency::EVICTION_CYCLE,
            cycle.elapsed().as_millis() as u64,
        );
        r
    };
    crate::server::eviction_exceeded(result != Evict::Ok);
    result
}

fn evict_until_under(s: crate::access::Settings) -> Evict {
    let started = std::time::Instant::now();
    let limit = s.time_limit_us();
    let volatile = s.policy.volatile();
    let used = || shm::usage().map_or(0, |u| u.used);
    // `performEvictions`: what to free is decided once, and each key counts
    // what its deletion gave back, measured before its invalidation and
    // events go out. Those queue messages of their own, which counted here
    // would make every eviction call for another.
    let to_free = shm::usage().map_or(0, |u| u.used.saturating_sub(u.limit));
    let (mut freed, mut keys) = (0usize, 0u64);
    while freed < to_free {
        let before = used();
        let pick = match s.policy {
            Policy::AllKeysRandom | Policy::VolatileRandom => pick_random(volatile),
            Policy::VolatileTtl => pick_pooled(volatile, s.samples, Score::Deadline),
            _ => pick_pooled(volatile, s.samples, Score::Access(s.tracking())),
        };
        let Some((slot, key)) = pick else {
            return Evict::Fail;
        };
        freed += before.saturating_sub(used());
        report(slot, &key);
        keys += 1;
        if keys.is_multiple_of(16) {
            // Under the limit already, as Redis checks every 16 keys.
            if !shm::over_limit() {
                break;
            }
            if started.elapsed().as_micros() as u64 > limit {
                return Evict::Running;
            }
        }
    }
    Evict::Ok
}

/// The pool policies' pick: sample every database into the pool, then take
/// the best candidate that is still there, evicting it.
fn pick_pooled(volatile: bool, samples: usize, score: Score) -> Option<(usize, Vec<u8>)> {
    POOL.with_borrow_mut(|pool| {
        loop {
            let mut total = 0;
            let mut drawn = Vec::new();
            for slot in 0..NUM_SLOTS {
                let mut out = Vec::new();
                let n = in_db(slot, || {
                    shm::with_db(slot, |db| {
                        db.sample(volatile, samples, score, &mut out);
                        db.evictable(volatile)
                    })
                    .unwrap_or(0)
                })
                .unwrap_or(0);
                total += n;
                drawn.extend(out.into_iter().map(|c| (slot, c)));
            }
            if total == 0 {
                return None;
            }
            for (slot, c) in drawn {
                pool.offer(c.score, (slot, c.key));
            }
            while let Some(c) = pool.pop_best() {
                let (slot, key) = c.key;
                if in_db(slot, || {
                    shm::with_db(slot, |db| db.evict_key(&key, volatile))
                }) == Some(Ok(true))
                {
                    return Some((slot, key));
                }
            }
        }
    })
}

/// Run `f` on memory slot `slot`'s database if no other client's transaction
/// or script holds it (`dblock::beside`): a database one holds is skipped,
/// as if it had nothing to evict.
fn in_db<R>(slot: usize, f: impl FnOnce() -> R) -> Option<R> {
    match crate::storage::db_of_slot(slot) {
        Some(db) => crate::dblock::beside(db, f),
        None => Some(f()),
    }
}

/// The random policies' pick: a random key, trying each database in turn.
fn pick_random(volatile: bool) -> Option<(usize, Vec<u8>)> {
    for _ in 0..NUM_SLOTS {
        let slot = NEXT_SLOT.with(|n| {
            let s = (n.get() + 1) % NUM_SLOTS;
            n.set(s);
            s
        });
        if let Some(Ok(Some(key))) =
            in_db(slot, || shm::with_db(slot, |db| db.evict_random(volatile)))
        {
            return Some((slot, key));
        }
    }
    None
}

/// What Redis does for each evicted key.
fn report(slot: usize, key: &[u8]) {
    crate::server::count_evicted(1);
    let Some(db) = crate::storage::db_of_slot(slot) else {
        return;
    };
    let keys = [key.to_vec()];
    if let Some(ctl) = crate::watch_state() {
        crate::watch::bump(ctl, db, key);
    }
    crate::tracking::invalidate(&keys, 0);
    crate::notify::emit(
        crate::notify::flags(),
        crate::notify::EVICTED,
        "evicted",
        key,
        db,
    );
}
