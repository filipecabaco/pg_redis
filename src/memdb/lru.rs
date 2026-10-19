//! Redis's access clock: the 24-bit `lru` field every object carries, which
//! holds either an LRU clock or an LFU counter with its decrement time, and
//! the eviction pool that the sampling policies fill.
//!
//! The arithmetic is Redis 7.2's (`evict.c`), so `OBJECT IDLETIME`,
//! `OBJECT FREQ` and the order keys are evicted in come out as Redis's do:
//!
//! - **LRU**: seconds since the epoch, truncated to 24 bits. The clock wraps
//!   about every 194 days, and an idle time is measured across one wrap.
//! - **LFU**: the high 16 bits are the minute of the last decrement (also
//!   wrapping), the low 8 a logarithmic counter. A new key starts at 5. An
//!   access first takes one off per `lfu-decay-time` minutes elapsed, then
//!   adds one with probability `1 / ((counter - 5) * lfu-log-factor + 1)`.
//!
//! The field is a `u32` here; only its low 24 bits are used, as in Redis.
#![forbid(unsafe_code)]

use std::cell::Cell;

/// `LRU_CLOCK_MAX`: the largest value the 24-bit field holds.
pub const LRU_CLOCK_MAX: u32 = (1 << 24) - 1;
/// `LRU_CLOCK_RESOLUTION`: one tick of the LRU clock, in milliseconds.
pub const LRU_CLOCK_RESOLUTION: u64 = 1000;
/// `LFU_INIT_VAL`: the counter a new key starts with, so it survives long
/// enough to be accessed again.
pub const LFU_INIT_VAL: u8 = 5;
/// `EVPOOL_SIZE`: the candidates the eviction pool keeps between rounds.
pub const EVPOOL_SIZE: usize = 16;

/// `getLRUClock`: the clock at `now_ms`.
pub fn lru_clock(now_ms: i64) -> u32 {
    ((now_ms.max(0) as u64 / LRU_CLOCK_RESOLUTION) as u32) & LRU_CLOCK_MAX
}

/// `estimateObjectIdleTime`: milliseconds since `lru` by the clock `clock`,
/// counting one wrap of the clock when `lru` is ahead of it.
pub fn idle_ms(lru: u32, clock: u32) -> u64 {
    let (lru, clock) = (u64::from(lru & LRU_CLOCK_MAX), u64::from(clock));
    if clock >= lru {
        (clock - lru) * LRU_CLOCK_RESOLUTION
    } else {
        (clock + (u64::from(LRU_CLOCK_MAX) - lru)) * LRU_CLOCK_RESOLUTION
    }
}

/// `LFUGetTimeInMinutes`: the low 16 bits of the minutes since the epoch.
pub fn lfu_minutes(now_ms: i64) -> u32 {
    ((now_ms.max(0) / 1000 / 60) as u32) & 0xffff
}

/// `LFUTimeElapsed`: minutes from `ldt` to `now`, counting one wrap.
pub fn lfu_elapsed(ldt: u32, now: u32) -> u32 {
    if now >= ldt {
        now - ldt
    } else {
        65535 - ldt + now
    }
}

/// `LFULogIncr`: add one with a probability that falls as the counter grows.
/// `r` is a uniform draw from [0, 1].
pub fn lfu_log_incr(counter: u8, log_factor: u32, r: f64) -> u8 {
    if counter == 255 {
        return 255;
    }
    let baseval = (f64::from(counter) - f64::from(LFU_INIT_VAL)).max(0.0);
    let p = 1.0 / (baseval * f64::from(log_factor) + 1.0);
    if r < p { counter + 1 } else { counter }
}

/// `LFUDecrAndReturn`: the counter less one per `decay_time` minutes since
/// its last decrement. The field itself is left as it is.
pub fn lfu_decr_and_return(lru: u32, decay_time: u32, now_min: u32) -> u8 {
    let ldt = (lru >> 8) & 0xffff;
    let counter = lru & 255;
    let periods = lfu_elapsed(ldt, now_min)
        .checked_div(decay_time)
        .unwrap_or(0);
    counter.saturating_sub(periods) as u8
}

/// How the field is kept: by the policy in force when it is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tracking {
    Lru,
    Lfu { log_factor: u32, decay_time: u32 },
}

/// What a command's lookups do to the field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Access {
    /// Whether a lookup updates the field: false for Redis's
    /// `LOOKUP_NOTOUCH` commands and for a client in `CLIENT NO-TOUCH` mode.
    pub touch: bool,
    pub tracking: Tracking,
}

impl Default for Access {
    fn default() -> Self {
        Access {
            touch: false,
            tracking: Tracking::Lru,
        }
    }
}

thread_local! {
    static ACCESS: Cell<Access> = Cell::new(Access::default());
}

/// Set what the commands about to run do to the field; the previous setting.
pub fn set_access(a: Access) -> Access {
    ACCESS.with(|c| c.replace(a))
}

pub fn access() -> Access {
    ACCESS.with(Cell::get)
}

/// A uniform draw from [0, 1], for `LFULogIncr`.
fn draw() -> f64 {
    (super::fast_random() >> 11) as f64 / (1u64 << 53) as f64
}

impl Tracking {
    /// The field of an object created at `now_ms`: `createObject`.
    pub fn fresh(self, now_ms: i64) -> u32 {
        match self {
            Tracking::Lru => lru_clock(now_ms),
            Tracking::Lfu { .. } => (lfu_minutes(now_ms) << 8) | u32::from(LFU_INIT_VAL),
        }
    }

    /// The field after an access at `now_ms`: `lookupKey`'s update.
    pub fn touched(self, lru: u32, now_ms: i64) -> u32 {
        self.touched_with(lru, now_ms, draw())
    }

    /// `touched`, with the draw `updateLFU` makes given.
    pub fn touched_with(self, lru: u32, now_ms: i64, r: f64) -> u32 {
        match self {
            Tracking::Lru => lru_clock(now_ms),
            Tracking::Lfu {
                log_factor,
                decay_time,
            } => {
                let now = lfu_minutes(now_ms);
                let counter = lfu_decr_and_return(lru, decay_time, now);
                let counter = lfu_log_incr(counter, log_factor, r);
                (now << 8) | u32::from(counter)
            }
        }
    }

    /// The eviction pool's score: higher is evicted first. Idle time under
    /// LRU, `255 - frequency` under LFU.
    pub fn score(self, lru: u32, now_ms: i64) -> u64 {
        match self {
            Tracking::Lru => idle_ms(lru, lru_clock(now_ms)),
            Tracking::Lfu { decay_time, .. } => {
                255 - u64::from(lfu_decr_and_return(lru, decay_time, lfu_minutes(now_ms)))
            }
        }
    }
}

/// `objectSetLRUOrLFU`, for `RESTORE ... IDLETIME | FREQ`: the field for a key
/// last used `idle_s` seconds ago, or with frequency `freq`; `None` when the
/// option given does not apply to the policy in force.
pub fn restored(
    tracking: Tracking,
    idle_s: Option<i64>,
    freq: Option<u8>,
    now_ms: i64,
) -> Option<u32> {
    match tracking {
        Tracking::Lfu { .. } => freq.map(|f| (lfu_minutes(now_ms) << 8) | u32::from(f)),
        Tracking::Lru => idle_s.map(|idle| {
            let clock = i64::from(lru_clock(now_ms));
            let ticks = (idle.saturating_mul(1000) / LRU_CLOCK_RESOLUTION as i64)
                .min(i64::from(LRU_CLOCK_MAX));
            let mut abs = clock - ticks;
            if abs < 0 {
                abs += i64::from(LRU_CLOCK_MAX);
            }
            abs as u32
        }),
    }
}

/// One candidate in the eviction pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate<K> {
    pub score: u64,
    pub key: K,
}

/// Redis's `EvictionPoolLRU`: up to 16 candidates in ascending order of
/// score, kept across evictions. Each round samples a few keys from every
/// database into it, and the highest-scoring candidate still present is
/// evicted. A candidate whose key has gone since is a ghost, dropped when it
/// comes up.
#[derive(Debug, Default)]
pub struct Pool<K> {
    entries: Vec<Candidate<K>>,
}

impl<K> Pool<K> {
    pub const fn new() -> Self {
        Pool {
            entries: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `evictionPoolPopulate`'s insertion: in place by score; into a full
    /// pool only when better than its worst, which is then dropped.
    pub fn offer(&mut self, score: u64, key: K) {
        let k = self.entries.partition_point(|c| c.score < score);
        let candidate = Candidate { score, key };
        if self.entries.len() < EVPOOL_SIZE {
            self.entries.insert(k, candidate);
        } else if k > 0 {
            self.entries.remove(0);
            self.entries.insert(k - 1, candidate);
        }
    }

    /// The best candidate, taken out of the pool.
    pub fn pop_best(&mut self) -> Option<Candidate<K>> {
        self.entries.pop()
    }

    #[cfg(test)]
    pub fn scores(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().map(|c| c.score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LFU: Tracking = Tracking::Lfu {
        log_factor: 10,
        decay_time: 1,
    };

    #[test]
    fn the_lru_clock_is_seconds_in_24_bits_and_wraps() {
        assert_eq!(lru_clock(0), 0);
        assert_eq!(lru_clock(1999), 1);
        assert_eq!(lru_clock((1 << 24) * 1000), 0, "wraps after 2^24 seconds");
        assert_eq!(lru_clock(((1 << 24) + 7) * 1000), 7);
        // Idle across a wrap: last used 10 ticks before the top, now 5 past
        // zero. Redis counts LRU_CLOCK_MAX - lru + clock.
        assert_eq!(idle_ms(LRU_CLOCK_MAX - 10, 5), 15 * 1000);
        assert_eq!(idle_ms(100, 160), 60 * 1000);
        assert_eq!(idle_ms(100, 100), 0);
    }

    #[test]
    fn a_new_key_starts_at_the_clock_or_at_five() {
        let now = 1_700_000_000_000;
        assert_eq!(Tracking::Lru.fresh(now), lru_clock(now));
        let f = LFU.fresh(now);
        assert_eq!(f & 255, 5);
        assert_eq!(f >> 8, lfu_minutes(now));
    }

    #[test]
    fn the_log_increment_gets_harder_as_the_counter_grows() {
        // At or below the initial value, any draw below 1 increments.
        assert_eq!(lfu_log_incr(0, 10, 0.999), 1);
        assert_eq!(lfu_log_incr(5, 10, 0.999), 6);
        // At 6 the chance is 1/(1*10+1).
        assert_eq!(lfu_log_incr(6, 10, 0.08), 7);
        assert_eq!(lfu_log_incr(6, 10, 0.1), 6);
        // A factor of 0 always increments; 255 saturates.
        assert_eq!(lfu_log_incr(200, 0, 0.999), 201);
        assert_eq!(lfu_log_incr(255, 0, 0.0), 255);
    }

    #[test]
    fn the_counter_matches_redis_after_many_accesses() {
        // Redis's own table (redis.conf, lfu-log-factor 10): 10 after 100
        // hits, 18 after 1,000, 142 after 100,000, 255 after a million. A
        // fixed sequence of draws stands in for rand().
        let mut lru = LFU.fresh(0);
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 11) as f64 / (1u64 << 53) as f64
        };
        let mut at = |n: u32, lru: &mut u32| {
            for _ in 0..n {
                *lru = LFU.touched_with(*lru, 0, next());
            }
            *lru & 255
        };
        let c100 = at(100, &mut lru);
        assert!((7..=13).contains(&c100), "100 hits gave {c100}");
        let c1k = at(900, &mut lru);
        assert!((14..=26).contains(&c1k), "1,000 hits gave {c1k}");
        let c100k = at(99_000, &mut lru);
        assert!((120..=160).contains(&c100k), "100,000 hits gave {c100k}");
        let c1m = at(900_000, &mut lru);
        assert!(c1m >= 240, "1M hits gave {c1m}");
    }

    #[test]
    fn the_counter_decays_one_per_period_and_never_below_zero() {
        let at_min = |m: u32| (m << 8) | 20;
        assert_eq!(lfu_decr_and_return(at_min(100), 1, 100), 20);
        assert_eq!(lfu_decr_and_return(at_min(100), 1, 107), 13);
        assert_eq!(lfu_decr_and_return(at_min(100), 2, 107), 17);
        assert_eq!(lfu_decr_and_return(at_min(100), 1, 1000), 0);
        // A decay time of 0 never decays.
        assert_eq!(lfu_decr_and_return(at_min(100), 0, 60000), 20);
        // Across the wrap of the 16-bit minutes.
        assert_eq!(lfu_elapsed(65530, 4), 9);
        assert_eq!(lfu_decr_and_return(at_min(65530), 1, 4), 11);
        // An access decays first, then increments, and restamps the minute.
        let t = LFU.touched_with(at_min(100), 107 * 60_000, 0.999);
        assert_eq!(t & 255, 13);
        assert_eq!(t >> 8, 107);
    }

    #[test]
    fn scores_put_the_idlest_and_the_rarest_first() {
        let now = 10_000_000;
        let clock = lru_clock(now);
        assert!(Tracking::Lru.score(clock - 50, now) > Tracking::Lru.score(clock - 5, now));
        let m = lfu_minutes(now);
        assert_eq!(LFU.score((m << 8) | 5, now), 250);
        assert!(LFU.score((m << 8) | 2, now) > LFU.score((m << 8) | 40, now));
    }

    #[test]
    fn restore_sets_the_field_for_the_policy_in_force() {
        let now = 50_000_000;
        let clock = lru_clock(now);
        assert_eq!(
            restored(Tracking::Lru, Some(1000), None, now),
            Some(clock - 1000)
        );
        assert_eq!(restored(Tracking::Lru, None, Some(9), now), None);
        assert_eq!(
            restored(LFU, None, Some(100), now).map(|f| f & 255),
            Some(100)
        );
        assert_eq!(restored(LFU, Some(3), None, now), None);
        // Further back than the clock: it wraps, as Redis's does.
        let early = 5_000;
        let back = restored(Tracking::Lru, Some(10), None, early).unwrap();
        assert_eq!(idle_ms(back, lru_clock(early)), 10_000);
    }

    #[test]
    fn the_pool_keeps_the_best_sixteen_in_order() {
        let mut p = Pool::new();
        for s in [5u64, 1, 9, 3] {
            p.offer(s, s);
        }
        assert_eq!(p.scores().collect::<Vec<_>>(), [1, 3, 5, 9]);
        for s in 10..30u64 {
            p.offer(s, s);
        }
        assert_eq!(p.len(), EVPOOL_SIZE);
        // Full: the worst went each time something better came.
        assert_eq!(p.scores().next(), Some(14));
        // Not better than the worst: refused.
        p.offer(2, 2);
        assert_eq!(p.scores().next(), Some(14));
        // Better than the worst: in, and the worst out.
        p.offer(20, 1000);
        assert_eq!(p.scores().next(), Some(15));
        assert!(p.scores().collect::<Vec<_>>().is_sorted());
        assert_eq!(p.pop_best().map(|c| c.score), Some(29));
        assert_eq!(p.len(), EVPOOL_SIZE - 1);
        // With room again, even the worst is taken.
        p.offer(0, 0);
        assert_eq!(p.scores().next(), Some(0));
    }

    #[test]
    fn sampling_into_the_pool_approximates_true_lru() {
        // Redis's argument for the pool: with 5 samples a round, the keys it
        // evicts are almost all from the idlest part of the keyspace. 10,000
        // keys idle 0..10,000; evict 1,000 and look at what went.
        let n = 10_000u64;
        let mut live: Vec<u64> = (0..n).collect();
        let mut pool = Pool::new();
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut rand = |m: usize| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % m as u64) as usize
        };
        let mut evicted = Vec::new();
        while evicted.len() < 1000 {
            for _ in 0..5 {
                let idle = live[rand(live.len())];
                pool.offer(idle, idle);
            }
            while let Some(c) = pool.pop_best() {
                if let Some(i) = live.iter().position(|&k| k == c.key) {
                    live.swap_remove(i);
                    evicted.push(c.key);
                    break;
                }
            }
        }
        // True LRU would take exactly the 1,000 idlest (9,000..10,000).
        let from_oldest_third = evicted.iter().filter(|&&k| k >= n * 2 / 3).count();
        assert!(from_oldest_third > 950, "{from_oldest_third} of 1000");
    }
}
