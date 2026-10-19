//! The memory engine, in safe Rust.
//!
//! A database is a [`Db`]: one dictionary from key to [`Entry`], each holding
//! a typed [`Value`] built from ordinary collections — [`dict::Dict`] for the
//! keyspace, hashes and sets, [`deque::Deque`] for lists, [`zset::ZSet`] for
//! sorted sets. Every collection is generic over its allocator: the server
//! gives them the shared-memory arena (`crate::shm`), the unit tests the
//! process heap, and nothing in this module knows which.
//!
//! No `unsafe` lives here. What makes it sound to share these structures
//! between worker processes — one address for the arena in every process, a
//! lock held for each access — is `crate::shm`'s to uphold.
#![forbid(unsafe_code)]

mod db;
pub mod deque;
pub mod dict;
pub mod encoding;
pub mod zset;

pub use db::{Db, take_reaped};

use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

/// Bytes owned by a collection, in its allocator.
pub type Bytes<A> = Vec<u8, A>;

pub fn bytes_in<A: Allocator>(s: &[u8], alloc: A) -> Bytes<A> {
    let mut v = Vec::with_capacity_in(s.len(), alloc);
    v.extend_from_slice(s);
    v
}

/// What a key holds. One key holds exactly one of these, which is what makes a
/// write of another type a `WRONGTYPE` rather than a second value beside the
/// first.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum KeyKind {
    String = 1,
    List = 2,
    Set = 3,
    Hash = 4,
    Zset = 5,
    Stream = 6,
}

impl KeyKind {
    /// The name `TYPE` answers with.
    pub fn name(self) -> &'static str {
        match self {
            KeyKind::String => "string",
            KeyKind::List => "list",
            KeyKind::Set => "set",
            KeyKind::Hash => "hash",
            KeyKind::Zset => "zset",
            KeyKind::Stream => "stream",
        }
    }
}

/// A stored counter, as Redis reads one: `i64::from_str` accepts `+1`, `007`
/// and `-0`, and Redis accepts only the canonical spelling.
pub fn parse_stored_int(bytes: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(bytes).ok()?;
    let digits = s.strip_prefix('-').unwrap_or(s);
    let canonical = match digits.as_bytes() {
        [] => false,
        [b'0'] => true,
        [b'1'..=b'9', rest @ ..] => rest.iter().all(u8::is_ascii_digit),
        _ => false,
    };
    // `-0` is not how Redis writes zero.
    if !canonical || s == "-0" {
        return None;
    }
    s.parse().ok()
}

/// The inclusive index range a range command selects, or `None` for none.
/// Negative indexes count back from the end.
pub fn range_bounds(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let s = if start < 0 { start + len } else { start }.max(0);
    let e = if stop < 0 { stop + len } else { stop }.min(len - 1);
    (len > 0 && e >= 0 && s <= e).then_some((s as usize, e as usize))
}

/// A xorshift generator: random picks for `SRANDMEMBER`, `RANDOMKEY` and
/// eviction need to be cheap, not unpredictable.
pub(crate) fn fast_random() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0);
    let mut s = SEED.load(Ordering::Relaxed);
    if s == 0 {
        s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
            | 1;
    }
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    SEED.store(s, Ordering::Relaxed);
    s
}

/// Indexes into a collection of `n` for a random-members command. A positive
/// count is that many distinct items (all of them when it is larger), in
/// random order; a negative count is `|count|` independent draws, repeats
/// allowed. Shared by `SRANDMEMBER`, `ZRANDMEMBER` and `HRANDFIELD`.
pub fn random_picks(n: usize, count: i64) -> std::vec::Vec<usize> {
    if n == 0 {
        return vec![];
    }
    if count < 0 {
        return (0..count.unsigned_abs())
            .map(|_| (fast_random() % n as u64) as usize)
            .collect();
    }
    // Partial Fisher-Yates: the first `take` slots end up a uniform sample.
    let take = (count as u64).min(n as u64) as usize;
    let mut idx: std::vec::Vec<usize> = (0..n).collect();
    for i in 0..take {
        let j = i + (fast_random() % (n - i) as u64) as usize;
        idx.swap(i, j);
    }
    idx.truncate(take);
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_ints_must_be_canonical() {
        assert_eq!(parse_stored_int(b"42"), Some(42));
        assert_eq!(parse_stored_int(b"-7"), Some(-7));
        for bad in [&b"+1"[..], b"007", b"-0", b"", b" 1", b"1.0"] {
            assert_eq!(parse_stored_int(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn range_bounds_clamp_like_redis() {
        assert_eq!(range_bounds(0, -1, 5), Some((0, 4)));
        assert_eq!(range_bounds(-100, 100, 5), Some((0, 4)));
        assert_eq!(range_bounds(3, 1, 5), None);
        assert_eq!(range_bounds(0, -1, 0), None);
        assert_eq!(range_bounds(5, 10, 5), None);
    }
}
