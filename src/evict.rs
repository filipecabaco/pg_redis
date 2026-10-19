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
}

/// Before a command: evict what the policy allows. False when the command,
/// if it may grow memory, must be refused.
pub fn make_room() -> bool {
    perform() != Evict::Fail
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
        evict_until_under(s)
    };
    crate::server::eviction_exceeded(result != Evict::Ok);
    result
}

fn evict_until_under(s: crate::access::Settings) -> Evict {
    let started = std::time::Instant::now();
    let limit = s.time_limit_us();
    let volatile = s.policy.volatile();
    let mut freed = 0u64;
    while shm::over_limit() {
        let pick = match s.policy {
            Policy::AllKeysRandom | Policy::VolatileRandom => pick_random(volatile),
            Policy::VolatileTtl => pick_pooled(volatile, s.samples, Score::Deadline),
            _ => pick_pooled(volatile, s.samples, Score::Access(s.tracking())),
        };
        let Some((slot, key)) = pick else {
            return Evict::Fail;
        };
        report(slot, &key);
        freed += 1;
        if freed.is_multiple_of(16) && started.elapsed().as_micros() as u64 > limit {
            return Evict::Running;
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
                let n = shm::with_db(slot, |db| {
                    db.sample(volatile, samples, score, &mut out);
                    db.evictable(volatile)
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
                if shm::with_db(slot, |db| db.evict_key(&key, volatile)) == Ok(true) {
                    return Some((slot, key));
                }
            }
        }
    })
}

/// The random policies' pick: a random key, trying each database in turn.
fn pick_random(volatile: bool) -> Option<(usize, Vec<u8>)> {
    for _ in 0..NUM_SLOTS {
        let slot = NEXT_SLOT.with(|n| {
            let s = (n.get() + 1) % NUM_SLOTS;
            n.set(s);
            s
        });
        if let Ok(Some(key)) = shm::with_db(slot, |db| db.evict_random(volatile)) {
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
