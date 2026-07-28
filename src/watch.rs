//! Write-version counters backing WATCH / EXEC.
//!
//! In shared memory rather than per worker because the listener uses
//! `SO_REUSEPORT`: a WATCH is served by whichever worker accepted the
//! connection, while the conflicting write may go through any other.
//!
//! Keys are hashed into a fixed table rather than stored, which keeps it
//! allocation-free, lock-free and unable to grow. Two keys sharing a counter
//! aborts an EXEC that did not have to abort; the reverse would not be safe.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counter table size. At 8 bytes each this is 512 KiB of shared memory, and
/// the spurious-abort rate is roughly `writes_during_the_watch / WATCH_SLOTS`.
const WATCH_SLOTS: usize = 65_536;

#[repr(C)]
pub struct WatchCtl {
    pub versions: [AtomicU64; WATCH_SLOTS],
}

unsafe impl Send for WatchCtl {}
unsafe impl Sync for WatchCtl {}

pub fn watch_ctl_size() -> usize {
    std::mem::size_of::<WatchCtl>()
}

/// FNV-1a over the database number followed by the key bytes.
fn slot_of(db: u8, key: &[u8]) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in std::iter::once(db).chain(key.iter().copied()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % WATCH_SLOTS as u64) as usize
}

/// Current write version for `key`, as snapshotted by WATCH.
///
/// # Safety
/// `ctl` must point at the initialised shared-memory block.
pub unsafe fn version(ctl: *mut WatchCtl, db: u8, key: &[u8]) -> u64 {
    unsafe { (*ctl).versions[slot_of(db, key)].load(Ordering::Acquire) }
}

/// Record a write to `key`, invalidating any WATCH taken before it.
///
/// # Safety
/// `ctl` must point at the initialised shared-memory block.
pub unsafe fn bump(ctl: *mut WatchCtl, db: u8, key: &[u8]) {
    unsafe {
        (*ctl).versions[slot_of(db, key)].fetch_add(1, Ordering::AcqRel);
    }
}

/// Record a write to every key there could be. `FLUSHDB` and `FLUSHALL` touch
/// keys they cannot name, and the table cannot be walked back to the keys that
/// hashed into it — so every counter moves. It aborts watches on databases the
/// flush did not touch, which is the direction this table is allowed to err in.
///
/// # Safety
/// `ctl` must point at the initialised shared-memory block.
pub unsafe fn bump_all(ctl: *mut WatchCtl) {
    for slot in 0..WATCH_SLOTS {
        unsafe { (*ctl).versions[slot].fetch_add(1, Ordering::AcqRel) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_in_range_and_stable() {
        for key in [&b""[..], b"k", b"a-much-longer-key-name", &[0xff; 300]] {
            for db in [0u8, 1, 15] {
                let slot = slot_of(db, key);
                assert!(slot < WATCH_SLOTS);
                assert_eq!(slot, slot_of(db, key), "hashing must be deterministic");
            }
        }
    }

    #[test]
    fn the_same_key_in_different_databases_is_tracked_separately() {
        // Not a correctness requirement — a collision here would only cost a
        // spurious abort — but sharing a counter for every db would make WATCH
        // on a busy db 0 abort transactions on db 1 constantly.
        assert_ne!(slot_of(0, b"key"), slot_of(1, b"key"));
    }

    #[test]
    fn distinct_keys_mostly_land_in_distinct_slots() {
        let keys: Vec<String> = (0..2000).map(|i| format!("user:{i}:session")).collect();
        let mut slots: Vec<usize> = keys.iter().map(|k| slot_of(0, k.as_bytes())).collect();
        slots.sort_unstable();
        slots.dedup();
        // 2000 keys over 65536 slots: ~30 collisions expected by birthday, so
        // anything below 1950 distinct means the hash is spreading badly.
        assert!(
            slots.len() > 1950,
            "only {} distinct slots for 2000 keys",
            slots.len()
        );
    }
}
