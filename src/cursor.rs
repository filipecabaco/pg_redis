//! The cursors of `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` on a database kept in
//! tables.
//!
//! The memory engine needs no state for a cursor: it is a position in the
//! dictionary (`memdb::dict::Dict::scan`). A table is iterated by keyset
//! pagination instead: each call resumes after the last key it returned, in
//! bytea order, through the primary key index. A key does not fit in the
//! 64-bit integer a client holds as its cursor, so that integer is a token
//! naming an entry here, where the next call finds the key to resume after.
//!
//! The table is in shared memory because the next call can arrive on any
//! worker: clients use connection pools, and the listener spreads connections
//! over workers. It has [`SLOTS`] entries, each guarded by one spinlock, and
//! the least recently used is taken when none is free.
//!
//! An entry keeps at most [`BOUND_LEN`] bytes of the key; a longer key is kept
//! as its prefix. Resuming after a prefix of the last key can only return
//! some keys again, never skip one. A token this table does not know, because
//! it was evicted, issued before a restart, made up, or issued for another
//! database or key, starts the iteration over, which also only repeats keys.
#![forbid(unsafe_code)]

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

/// Iterations in progress at once across the server. Past this, the least
/// recently used one restarts on its next call.
pub const SLOTS: usize = 4096;
/// Bytes of the resume key an entry keeps.
pub const BOUND_LEN: usize = 256;
/// The token's low bits name its slot; `SLOTS` is a power of two.
const SLOT_BITS: u32 = SLOTS.trailing_zeros();
/// Set in `meta` when the key equal to the bound is still to come.
const INCLUSIVE: u32 = 1 << 31;

#[repr(C)]
pub struct Slot {
    /// 0 when free.
    token: AtomicU64,
    /// What is iterated: the database, and the key for a collection.
    scope: AtomicU64,
    /// The table's clock at the slot's last use, for eviction.
    used: AtomicU64,
    /// The bound's length, and `INCLUSIVE`.
    meta: AtomicU32,
    bytes: [AtomicU8; BOUND_LEN],
}

/// Pointer-free and valid zeroed: every slot free, the lock open.
#[repr(C)]
pub struct CursorTable {
    lock: AtomicU32,
    clock: AtomicU64,
    slots: [Slot; SLOTS],
}

pub fn cursor_table_size() -> usize {
    std::mem::size_of::<CursorTable>()
}

/// One step's reply: the cursor for the next step, 0 when this one was the
/// last, and the rows found, each a key with what goes with it.
pub type Page<T> = (u64, Vec<(Vec<u8>, T)>);

/// Where an iteration resumes: the keys after `key`, or from it when
/// `inclusive`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bound {
    pub key: Vec<u8>,
    pub inclusive: bool,
}

/// What a cursor iterates: SCAN over a database, or one collection's
/// elements. A token resumes only the iteration it was issued for.
#[allow(deprecated)] // SipHasher is deprecated only in favour of unkeyed hashers.
pub fn scope(db: u8, kind: u8, key: &[u8]) -> u64 {
    // SipHash with fixed keys: the same in every worker, which is all a
    // scope needs. A collision only lets one iteration resume at another's
    // bound under a token that also had to match.
    let mut h = std::hash::SipHasher::new_with_keys(0x7063_7572, 0x736f_7273);
    (db, kind, key).hash(&mut h);
    h.finish()
}

/// Holds the table's spinlock. Nothing under it can raise or panic: it only
/// moves atomics around.
struct Locked<'a>(&'a CursorTable);

impl<'a> Locked<'a> {
    fn new(t: &'a CursorTable) -> Self {
        while t
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Locked(t)
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        self.0.lock.store(0, Ordering::Release);
    }
}

fn slot_of(token: u64) -> usize {
    (token & (SLOTS as u64 - 1)) as usize
}

impl CursorTable {
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The bound `token` resumes from, if it names a live iteration of
    /// `scope`.
    pub fn resume(&self, token: u64, scope: u64) -> Option<Bound> {
        if token == 0 {
            return None;
        }
        let s = &self.slots[slot_of(token)];
        let _held = Locked::new(self);
        if s.token.load(Ordering::Relaxed) != token || s.scope.load(Ordering::Relaxed) != scope {
            return None;
        }
        s.used.store(self.tick(), Ordering::Relaxed);
        let meta = s.meta.load(Ordering::Relaxed);
        let len = (meta & !INCLUSIVE) as usize;
        Some(Bound {
            key: s.bytes[..len.min(BOUND_LEN)]
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            inclusive: meta & INCLUSIVE != 0,
        })
    }

    /// Record that the iteration `prev` (0 for a new one) of `scope` resumes
    /// from `bound` next, and return the token for it. The slot `prev` had is
    /// reused while it still holds `prev`; otherwise a free slot, or the
    /// least recently used, is taken. `bound.key` must fit in `BOUND_LEN`.
    pub fn save(&self, prev: u64, scope: u64, bound: &Bound, random: u64) -> u64 {
        debug_assert!(bound.key.len() <= BOUND_LEN);
        let _held = Locked::new(self);
        let i = if prev != 0 && self.slots[slot_of(prev)].token.load(Ordering::Relaxed) == prev {
            slot_of(prev)
        } else {
            self.victim()
        };
        let s = &self.slots[i];
        // 51 random bits above the slot, at least one of them set, and the
        // top bit clear: non-zero, and positive for a client that parses the
        // cursor as a signed integer.
        let mut token = ((random >> 13) | 1) << SLOT_BITS | i as u64;
        if token == prev {
            token ^= 1 << SLOT_BITS << 1;
        }
        let len = bound.key.len().min(BOUND_LEN);
        for (b, &v) in s.bytes.iter().zip(&bound.key[..len]) {
            b.store(v, Ordering::Relaxed);
        }
        s.meta.store(
            len as u32 | if bound.inclusive { INCLUSIVE } else { 0 },
            Ordering::Relaxed,
        );
        s.scope.store(scope, Ordering::Relaxed);
        s.used.store(self.tick(), Ordering::Relaxed);
        s.token.store(token, Ordering::Relaxed);
        token
    }

    /// A free slot, or the one used least recently.
    fn victim(&self) -> usize {
        let mut best = (u64::MAX, 0);
        for (i, s) in self.slots.iter().enumerate() {
            if s.token.load(Ordering::Relaxed) == 0 {
                return i;
            }
            let used = s.used.load(Ordering::Relaxed);
            if used < best.0 {
                best = (used, i);
            }
        }
        best.1
    }

    /// The iteration `token` names is done: free its slot.
    pub fn finish(&self, token: u64) {
        if token == 0 {
            return;
        }
        let s = &self.slots[slot_of(token)];
        let _held = Locked::new(self);
        if s.token.load(Ordering::Relaxed) == token {
            s.token.store(0, Ordering::Relaxed);
        }
    }
}

/// The smallest key greater than every key that starts with `prefix`, or
/// `None` when there is none (the prefix is all `0xff`).
pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut s = prefix.to_vec();
    while let Some(last) = s.pop() {
        if last != 0xff {
            s.push(last + 1);
            return Some(s);
        }
    }
    None
}

/// Which rows a page asks for, in the order of their first column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Range {
    /// From the beginning.
    All,
    /// After the key.
    After(Vec<u8>),
    /// From the key.
    From(Vec<u8>),
    /// After the first, before the second.
    Between(Vec<u8>, Vec<u8>),
}

/// One step of a keyset iteration of `scope`: the rows of the page `cursor`
/// names and the cursor for the next, 0 when this page is the last.
///
/// `fetch(range, limit)` returns the rows in `range`, ordered by their key
/// and at most `limit` of them (all for `None`). The rows a step returns are
/// the first `count` after the resume point, or every one when that is fewer;
/// the caller filters them afterwards (expired keys, MATCH), which does not
/// move the cursor.
///
/// A key longer than [`BOUND_LEN`] is saved as its prefix, and the next step
/// resumes after that prefix. Should more than `count` keys share one prefix,
/// that step would return the same page again and never end, so it returns
/// the rest of the keys sharing the prefix too, and the next step resumes
/// from the prefix's successor. Without the table, which is only the case
/// where shared memory is not set up, one step returns every row.
pub fn step<T, E>(
    table: Option<&CursorTable>,
    cursor: u64,
    scope: u64,
    count: usize,
    random: u64,
    mut fetch: impl FnMut(&Range, Option<i64>) -> Result<Vec<(Vec<u8>, T)>, E>,
) -> Result<Page<T>, E> {
    let Some(table) = table else {
        return Ok((0, fetch(&Range::All, None)?));
    };
    let count = count.max(1);
    let bound = table.resume(cursor, scope);
    // Only a token issued for this iteration may be moved on or freed: one
    // for another database or key names someone else's entry.
    let cursor = if bound.is_some() { cursor } else { 0 };
    let range = match &bound {
        None => Range::All,
        Some(b) if b.inclusive => Range::From(b.key.clone()),
        Some(b) => Range::After(b.key.clone()),
    };
    let mut rows = fetch(&range, Some(i64::try_from(count).unwrap_or(i64::MAX)))?;
    if rows.len() < count {
        table.finish(cursor);
        return Ok((0, rows));
    }
    let last = rows.last().map(|(k, _)| k.clone()).unwrap_or_default();
    let next = if last.len() <= BOUND_LEN {
        Bound {
            key: last,
            inclusive: false,
        }
    } else {
        let prefix = last[..BOUND_LEN].to_vec();
        if range != Range::After(prefix.clone()) {
            Bound {
                key: prefix,
                inclusive: false,
            }
        } else {
            // Stuck on one prefix: the rest of its keys, in this reply.
            match successor(&prefix) {
                Some(up) => {
                    rows.extend(fetch(&Range::Between(last, up.clone()), None)?);
                    Bound {
                        key: up,
                        inclusive: true,
                    }
                }
                None => {
                    rows.extend(fetch(&Range::After(last), None)?);
                    table.finish(cursor);
                    return Ok((0, rows));
                }
            }
        }
    };
    Ok((table.save(cursor, scope, &next, random), rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The zeroed table, as shared memory starts it. Built on a thread with
    /// room for it: at over a megabyte it would overflow a test thread's
    /// stack on its way into the box.
    fn table() -> Box<CursorTable> {
        std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn(|| {
                let slots = std::array::from_fn(|_| Slot {
                    token: AtomicU64::new(0),
                    scope: AtomicU64::new(0),
                    used: AtomicU64::new(0),
                    meta: AtomicU32::new(0),
                    bytes: std::array::from_fn(|_| AtomicU8::new(0)),
                });
                Box::new(CursorTable {
                    lock: AtomicU32::new(0),
                    clock: AtomicU64::new(0),
                    slots,
                })
            })
            .and_then(|t| t.join().map_err(|_| std::io::Error::other("panicked")))
            .expect("build the table")
    }

    /// A sorted set of keys standing in for a table, read as `fetch` does.
    fn fetch_from(
        keys: &std::collections::BTreeSet<Vec<u8>>,
        range: &Range,
        limit: Option<i64>,
    ) -> Vec<(Vec<u8>, ())> {
        keys.iter()
            .filter(|k| match range {
                Range::All => true,
                Range::After(b) => *k > b,
                Range::From(b) => *k >= b,
                Range::Between(a, b) => *k > a && *k < b,
            })
            .take(limit.map_or(usize::MAX, |n| n as usize))
            .map(|k| (k.clone(), ()))
            .collect()
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// A whole iteration, with `between` run on the keys after every step.
    fn iterate(
        t: &CursorTable,
        keys: &mut std::collections::BTreeSet<Vec<u8>>,
        count: usize,
        mut between: impl FnMut(&mut std::collections::BTreeSet<Vec<u8>>),
    ) -> Vec<Vec<u8>> {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut seen = Vec::new();
        let mut cursor = 0;
        for _ in 0..100_000 {
            let (next, rows) = step(Some(t), cursor, 7, count, rng.next(), |r, l| {
                Ok::<_, ()>(fetch_from(keys, r, l))
            })
            .unwrap();
            seen.extend(rows.into_iter().map(|(k, _)| k));
            if next == 0 {
                return seen;
            }
            assert!(next < 1 << 63, "a cursor must read as a positive i64");
            cursor = next;
            between(keys);
        }
        panic!("the iteration did not end");
    }

    #[test]
    fn a_full_iteration_returns_every_key_and_frees_its_slot() {
        let t = table();
        let mut keys: std::collections::BTreeSet<Vec<u8>> =
            (0..1000).map(|i| format!("k{i}").into_bytes()).collect();
        let want = keys.clone();
        let seen = iterate(&t, &mut keys, 10, |_| {});
        assert_eq!(seen.len(), 1000, "nothing changed, so nothing repeats");
        assert_eq!(
            seen.into_iter().collect::<std::collections::BTreeSet<_>>(),
            want
        );
        assert!(t.slots.iter().all(|s| s.token.load(Ordering::Relaxed) == 0));
    }

    #[test]
    fn deletes_and_inserts_mid_iteration_skip_nothing_present_throughout() {
        let t = table();
        let mut keys: std::collections::BTreeSet<Vec<u8>> =
            (0..1000).map(|i| format!("k{i:04}").into_bytes()).collect();
        let mut removed = std::collections::BTreeSet::new();
        let mut rng = Rng(42);
        let seen = iterate(&t, &mut keys, 10, |keys| {
            for _ in 0..4 {
                let k = format!("k{:04}", rng.next() % 1000).into_bytes();
                if keys.remove(&k) {
                    removed.insert(k);
                }
                keys.insert(format!("new{}", rng.next() % 5000).into_bytes());
            }
        });
        let seen: std::collections::BTreeSet<_> = seen.into_iter().collect();
        for i in 0..1000 {
            let k = format!("k{i:04}").into_bytes();
            assert!(removed.contains(&k) || seen.contains(&k), "k{i:04} skipped");
        }
    }

    #[test]
    fn long_keys_sharing_a_prefix_still_end_and_skip_nothing() {
        let t = table();
        let long = |p: u8, i: u32| {
            let mut k = vec![p; BOUND_LEN + 10];
            k.extend(format!("{i:05}").into_bytes());
            k
        };
        // Fifty keys share one 256-byte prefix, more than a page, between
        // short keys; and a run of keys under an all-0xff prefix, which has
        // no successor.
        let mut keys: std::collections::BTreeSet<Vec<u8>> = (0..50)
            .map(|i| long(b'm', i))
            .chain((0..30).map(|i| format!("a{i:03}").into_bytes()))
            .chain((0..30).map(|i| format!("z{i:03}").into_bytes()))
            .chain((0..25).map(|i| long(0xff, i)))
            .collect();
        let want = keys.clone();
        let seen = iterate(&t, &mut keys, 10, |_| {});
        assert_eq!(
            seen.into_iter().collect::<std::collections::BTreeSet<_>>(),
            want
        );
    }

    #[test]
    fn an_unknown_or_foreign_token_restarts() {
        let t = table();
        let mut keys: std::collections::BTreeSet<Vec<u8>> =
            (0..30).map(|i| format!("k{i:02}").into_bytes()).collect();
        let first = |cursor: u64, scope: u64, keys: &std::collections::BTreeSet<Vec<u8>>| {
            step(Some(&*t), cursor, scope, 10, 99, |r, l| {
                Ok::<_, ()>(fetch_from(keys, r, l))
            })
            .unwrap()
        };
        let (token, page) = first(0, 1, &keys);
        assert_ne!(token, 0);
        assert_eq!(page[0].0, b"k00");
        // Sent for another scope, made up, or a memory engine's position, it
        // starts again, and leaves the iteration it belongs to alone...
        for (cursor, scope) in [(token, 2), (12345, 1), (u64::MAX, 1), (7, 1)] {
            assert_eq!(first(cursor, scope, &keys).1[0].0, b"k00");
        }
        // ...which its own scope resumes. Sent again after that, it is spent.
        let (next, page) = first(token, 1, &keys);
        assert_eq!(page[0].0, b"k10");
        assert_ne!(next, token);
        assert_eq!(first(token, 1, &keys).1[0].0, b"k00");
        keys.clear();
        assert_eq!(first(0, 1, &keys).0, 0);
    }

    #[test]
    fn eviction_takes_the_least_recently_used_slot() {
        let t = table();
        let b = Bound {
            key: b"x".to_vec(),
            inclusive: false,
        };
        let tokens: Vec<u64> = (0..SLOTS as u64)
            .map(|i| t.save(0, 1, &b, i.wrapping_mul(0x9e37_79b9) + 1))
            .collect();
        // Touch the first, so the second is the oldest.
        assert_eq!(t.resume(tokens[0], 1), Some(b.clone()));
        let newer = t.save(0, 1, &b, 777);
        assert_eq!(slot_of(newer), slot_of(tokens[1]));
        assert_eq!(t.resume(tokens[1], 1), None);
        assert_eq!(t.resume(tokens[0], 1), Some(b));
    }

    #[test]
    fn successor_bounds_every_key_with_the_prefix() {
        assert_eq!(successor(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(successor(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(successor(b"\xff\xff"), None);
        assert_eq!(successor(b""), None);
    }
}
