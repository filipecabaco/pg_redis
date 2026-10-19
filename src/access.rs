//! Access tracking: the eviction policy and its parameters as the command
//! path reads them, what each command's lookups do to a key's access field,
//! and where a logged database keeps that field.
//!
//! A memory database keeps the field in each key's entry (`memdb::lru`), and
//! its commands update it as they look keys up. A logged database's keys are
//! rows, and writing a row on every read would turn each `GET` into a table
//! write. Its fields live instead in a fixed table in shared memory, keyed by
//! a hash of the database and the key: 65,536 slots of 8 bytes. Two keys
//! that hash to one slot share it, and the later one wins, so a field can be
//! lost or start over; Redis's own clock is an approximation in the same
//! spirit. The table is not saved, so after a restart a logged key's field
//! starts again the first time it is looked at. Eviction never applies to a
//! logged database (it is bounded by disk), so only `OBJECT IDLETIME` and
//! `OBJECT FREQ` read these fields.
//!
//! Logged keys are touched from the connection, after the command has run,
//! from the keys its arguments name (`after_command`): the same key
//! specifications `COMMAND GETKEYS` reads, so no command needs its own list.
#![forbid(unsafe_code)]

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::memdb::lru::{self, Access, Tracking};
pub use crate::shm::Policy;

/// The parameters eviction and access tracking read, all `CONFIG SET`-able.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings {
    pub policy: Policy,
    /// `maxmemory-samples`.
    pub samples: usize,
    /// `lfu-log-factor`.
    pub log_factor: u32,
    /// `lfu-decay-time`, in minutes.
    pub decay_time: u32,
    /// `maxmemory-eviction-tenacity`.
    pub tenacity: u32,
    /// `maxmemory` is 0: Redis's "no limit", under which integers are shared
    /// whatever the policy.
    pub no_limit: bool,
}

impl Settings {
    pub fn tracking(self) -> Tracking {
        if self.policy.lfu() {
            Tracking::Lfu {
                log_factor: self.log_factor,
                decay_time: self.decay_time,
            }
        } else {
            Tracking::Lru
        }
    }

    /// `MAXMEMORY_FLAG_NO_SHARED_INTEGERS`: under an LRU or LFU policy with
    /// a limit set, Redis gives every key its own object, so that each has
    /// its own access field; otherwise 0–9999 are shared and `OBJECT
    /// REFCOUNT` reports `INT_MAX` for them.
    pub fn shares_integers(self) -> bool {
        self.no_limit || !(self.policy.lru() || self.policy.lfu())
    }

    /// `evictionTimeLimitUs`: how long one round of eviction may run.
    pub fn time_limit_us(self) -> u64 {
        match self.tenacity {
            t if t <= 10 => 50 * u64::from(t),
            t if t < 100 => (500.0 * 1.15f64.powf(f64::from(t) - 10.0)) as u64,
            _ => u64::MAX,
        }
    }

    fn read() -> Settings {
        let int = |name: &str, min: i64| crate::config::get_i64(name).max(min);
        Settings {
            policy: Policy::parse(&crate::config::get_str("maxmemory-policy"))
                .unwrap_or(Policy::NoEviction),
            samples: int("maxmemory-samples", 1) as usize,
            log_factor: int("lfu-log-factor", 0).min(u32::MAX as i64) as u32,
            decay_time: int("lfu-decay-time", 0).min(u32::MAX as i64) as u32,
            tenacity: int("maxmemory-eviction-tenacity", 0).min(100) as u32,
            no_limit: crate::config::get_str("maxmemory") == "0",
        }
    }
}

thread_local! {
    /// The settings and the generation they were read at.
    static CACHED: Cell<Option<((u32, u32), Settings)>> = const { Cell::new(None) };
}

/// The settings in force. Read once per change: every command asks, and a
/// `CONFIG SET` or a reload is what changes them.
pub fn settings() -> Settings {
    let generation = (
        crate::server::config_generation(),
        crate::config::guc_generation(),
    );
    if generation.0 & 1 == 0
        && let Some((g, s)) = CACHED.get()
        && g == generation
    {
        return s;
    }
    let s = Settings::read();
    CACHED.set(Some((generation, s)));
    s
}

/// Whether the client that sent the command is in `CLIENT NO-TOUCH` mode.
/// One atomic load unless some client is.
pub fn client_no_touch(id: u64) -> bool {
    crate::server::any_no_touch()
        && crate::server::slot_of(id)
            .is_some_and(|s| s.flags.load(Ordering::Relaxed) & crate::server::flag::NO_TOUCH != 0)
}

/// Redis's `LOOKUP_NOTOUCH` commands: they look a key up without updating
/// its access field. Every other command that looks a key up updates it.
fn never_touches(name: &str) -> bool {
    matches!(
        name,
        "EXISTS"
            | "TYPE"
            | "TTL"
            | "PTTL"
            | "EXPIRETIME"
            | "PEXPIRETIME"
            | "OBJECT"
            | "MEMORY"
            | "SCAN"
            | "KEYS"
            | "RANDOMKEY"
            | "DBSIZE"
    )
}

/// What a command's lookups do to the access field: whether they touch,
/// and how the field is kept under the policy in force. `TOUCH` touches
/// even for a client in no-touch mode, as in Redis.
pub fn mode(cmd: &crate::commands::Command, origin: u64) -> Access {
    use crate::commands::Command as C;
    let untouched = matches!(
        cmd,
        C::Exists { .. }
            | C::Type { .. }
            | C::Ttl { .. }
            | C::PTtl { .. }
            | C::ExpireTime { .. }
            | C::PExpireTime { .. }
            | C::ObjectEncoding { .. }
            | C::ObjectAccess { .. }
            | C::ObjectOther
            | C::MemoryUsage { .. }
            | C::Keys { .. }
            | C::Scan { .. }
            | C::RandomKey
            | C::DbSize
            | C::Keyspace
    );
    let touch = !untouched && (matches!(cmd, C::Touch { .. }) || !client_no_touch(origin));
    Access {
        touch,
        tracking: settings().tracking(),
    }
}

/// Run `f` with `access` in force for the memory engine's lookups.
pub fn with_mode<R>(access: Access, f: impl FnOnce() -> R) -> R {
    struct Restore(Access);
    impl Drop for Restore {
        fn drop(&mut self) {
            lru::set_access(self.0);
        }
    }
    let _restore = Restore(lru::set_access(access));
    f()
}

// ───────────────────────── logged databases' fields ─────────────────────────

pub const LOGGED_SLOTS: usize = 1 << 16;

/// Each slot: a 32-bit tag from the key's hash, 0 when free, above the
/// 24-bit access field.
#[repr(C)]
pub struct AccessTable {
    slots: [AtomicU64; LOGGED_SLOTS],
}

pub fn access_table_size() -> usize {
    std::mem::size_of::<AccessTable>()
}

/// Where `key` of `db` lives in the table, and the tag that marks it. A
/// flush changes the database's epoch, which moves every key elsewhere.
fn locate(db: u8, key: &[u8]) -> (usize, u64) {
    use std::hash::Hasher;
    #[allow(deprecated)] // fixed keys, the same in every process
    let mut h = std::hash::SipHasher::new_with_keys(0x7067_5f72_6564_6973, 0x6163_6365_7373);
    h.write_u8(db);
    h.write_u64(crate::server::flush_epoch(db));
    h.write(key);
    let h = h.finish();
    ((h as usize) & (LOGGED_SLOTS - 1), (h >> 32) | 1)
}

fn table() -> Option<&'static AccessTable> {
    crate::access_state()
}

fn pack(tag: u64, field: u32) -> u64 {
    (tag << 32) | u64::from(field & lru::LRU_CLOCK_MAX)
}

/// The field of a logged key, not touching it. A key the table holds
/// nothing for gets a fresh field, as if created now.
pub fn logged_field(db: u8, key: &[u8]) -> u32 {
    let now = crate::server::now_ms();
    let fresh = settings().tracking().fresh(now);
    let Some(t) = table() else { return fresh };
    let (i, tag) = locate(db, key);
    let v = t.slots[i].load(Ordering::Relaxed);
    if v >> 32 == tag {
        return v as u32;
    }
    t.slots[i].store(pack(tag, fresh), Ordering::Relaxed);
    fresh
}

/// Set a logged key's field.
pub fn logged_set(db: u8, key: &[u8], field: u32) {
    if let Some(t) = table() {
        let (i, tag) = locate(db, key);
        t.slots[i].store(pack(tag, field), Ordering::Relaxed);
    }
}

/// A lookup of a logged key: its field updated, or started.
pub fn logged_touch(db: u8, key: &[u8], tracking: Tracking) {
    let Some(t) = table() else { return };
    let now = crate::server::now_ms();
    let (i, tag) = locate(db, key);
    let v = t.slots[i].load(Ordering::Relaxed);
    let field = if v >> 32 == tag {
        tracking.touched(v as u32, now)
    } else {
        tracking.fresh(now)
    };
    t.slots[i].store(pack(tag, field), Ordering::Relaxed);
}

/// A logged key is gone: whatever is written there next starts fresh.
pub fn logged_forget(db: u8, key: &[u8]) {
    if let Some(t) = table() {
        let (i, tag) = locate(db, key);
        if t.slots[i].load(Ordering::Relaxed) >> 32 == tag {
            t.slots[i].store(0, Ordering::Relaxed);
        }
    }
}

/// After `RESTORE` or `COPY` wrote `key`: a new object's field, or the one
/// `IDLETIME` / `FREQ` asked for.
pub fn restored(db: u8, key: &[u8], idletime: Option<i64>, freq: Option<u8>) {
    let tracking = settings().tracking();
    let now = crate::server::now_ms();
    let asked = lru::restored(tracking, idletime, freq, now);
    match crate::storage::slot(db) {
        Some(slot) => {
            if let Some(field) = asked {
                let _ = crate::shm::with_db(slot, |m| m.set_access(key, field));
            }
        }
        None => logged_set(db, key, asked.unwrap_or_else(|| tracking.fresh(now))),
    }
}

/// After a command ran on a logged database, from the connection that sent
/// it: update the access fields of the keys it named, as its lookups would
/// have in Redis. Memory databases do this as the command runs.
pub fn after_command(db: u8, argv: &[Vec<u8>], no_touch: bool) {
    if crate::storage::slot(db).is_some() || argv.is_empty() {
        return;
    }
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_uppercase();
    // A script's keys are touched by the commands it runs, not by the call.
    let not_lookups = matches!(
        name.as_str(),
        "RESTORE"
            | "MULTI"
            | "EXEC"
            | "EVAL"
            | "EVALSHA"
            | "EVAL_RO"
            | "EVALSHA_RO"
            | "FCALL"
            | "FCALL_RO"
    );
    if never_touches(&name) || not_lookups {
        return;
    }
    let Ok(keys) = crate::admin::get_keys(argv) else {
        return;
    };
    let keys: Vec<Vec<u8>> = keys.into_iter().map(|(k, _)| k).collect();
    let tracking = settings().tracking();
    match name.as_str() {
        "DEL" | "UNLINK" | "GETDEL" => keys.iter().for_each(|k| logged_forget(db, k)),
        // The object moves, its field with it.
        "RENAME" => {
            if let [src, dst] = keys.as_slice() {
                if !no_touch {
                    logged_touch(db, src, tracking);
                }
                if let Some(field) = logged_peek(db, src) {
                    logged_forget(db, src);
                    logged_set(db, dst, field);
                }
            }
        }
        // The destination of a COPY is a new object, set by `restored`.
        "COPY" => {
            if let Some(src) = keys.first()
                && !no_touch
            {
                logged_touch(db, src, tracking);
            }
        }
        "TOUCH" => keys.iter().for_each(|k| logged_touch(db, k, tracking)),
        _ if no_touch => {}
        _ => keys.iter().for_each(|k| logged_touch(db, k, tracking)),
    }
}

/// The field a logged key has in the table, if any.
fn logged_peek(db: u8, key: &[u8]) -> Option<u32> {
    let t = table()?;
    let (i, tag) = locate(db, key);
    let v = t.slots[i].load(Ordering::Relaxed);
    (v >> 32 == tag).then_some(v as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_policy_parses_and_names_itself() {
        for p in Policy::ALL {
            assert_eq!(Policy::parse(p.name()), Some(p));
        }
        assert_eq!(Policy::parse("random"), None);
        assert!(Policy::VolatileTtl.volatile() && !Policy::AllKeysLru.volatile());
        assert!(Policy::VolatileLfu.lfu() && Policy::AllKeysLru.lru());
    }

    #[test]
    fn tenacity_becomes_redis_time_limits() {
        let s = |tenacity| Settings {
            policy: Policy::NoEviction,
            samples: 5,
            log_factor: 10,
            decay_time: 1,
            tenacity,
            no_limit: false,
        };
        assert_eq!(s(0).time_limit_us(), 0);
        assert_eq!(s(10).time_limit_us(), 500);
        assert_eq!(s(11).time_limit_us(), 575);
        assert_eq!(s(100).time_limit_us(), u64::MAX);
    }

    #[test]
    fn integers_are_shared_unless_an_lru_or_lfu_policy_has_a_limit() {
        let s = |policy, no_limit| Settings {
            policy,
            samples: 5,
            log_factor: 10,
            decay_time: 1,
            tenacity: 10,
            no_limit,
        };
        assert!(s(Policy::NoEviction, false).shares_integers());
        assert!(s(Policy::AllKeysRandom, false).shares_integers());
        assert!(!s(Policy::AllKeysLru, false).shares_integers());
        assert!(!s(Policy::VolatileLfu, false).shares_integers());
        assert!(s(Policy::AllKeysLru, true).shares_integers());
    }
}
