//! A sorted set: a dictionary from member to score, for lookups, beside the
//! members kept in `(score, member)` order, for ranges. The order is a list of
//! sorted blocks of bounded size — a B-tree one level deep — so an insert
//! moves at most one block's worth of entries and a rank costs a walk over
//! the blocks rather than the members.
#![forbid(unsafe_code)]

use std::cmp::Ordering;

use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

use super::dict::{Dict, Seed};
use super::{Bytes, bytes_in};
use crate::commands::{LexBound, ScoreBound};

/// A block splits past twice this many entries.
const BLOCK: usize = 128;

type Block<A> = Vec<(f64, Bytes<A>), A>;

pub struct ZSet<A: Allocator + Clone> {
    dict: Dict<f64, A>,
    blocks: Vec<Block<A>, A>,
}

/// Redis's order: by score, then by member bytes. No score is NaN, so the
/// comparison is total.
fn cmp(a: (f64, &[u8]), b: (f64, &[u8])) -> Ordering {
    a.0.partial_cmp(&b.0)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.1.cmp(b.1))
}

impl ScoreBound {
    /// Whether `s` is at or past this bound taken as a minimum.
    pub fn below_min(&self, s: f64) -> bool {
        if self.exclusive {
            s <= self.value
        } else {
            s < self.value
        }
    }
    /// Whether `s` is within this bound taken as a maximum.
    pub fn within_max(&self, s: f64) -> bool {
        if self.exclusive {
            s < self.value
        } else {
            s <= self.value
        }
    }
}

impl LexBound {
    /// Whether `m` falls short of this bound taken as a minimum.
    pub fn below_min(&self, m: &[u8]) -> bool {
        match self {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Inclusive(b) => m < b.as_slice(),
            LexBound::Exclusive(b) => m <= b.as_slice(),
        }
    }
    /// Whether `m` is within this bound taken as a maximum.
    pub fn within_max(&self, m: &[u8]) -> bool {
        match self {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Inclusive(b) => m <= b.as_slice(),
            LexBound::Exclusive(b) => m < b.as_slice(),
        }
    }
}

impl<A: Allocator + Clone> ZSet<A> {
    pub fn new_in(seed: Seed, alloc: A) -> Self {
        ZSet {
            dict: Dict::new_in(seed, alloc.clone()),
            blocks: Vec::new_in(alloc),
        }
    }

    fn alloc(&self) -> A {
        self.blocks.allocator().clone()
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.dict.get(member).copied()
    }

    /// The member at dictionary position `i` — an arbitrary but O(1) pick,
    /// for `ZRANDMEMBER`.
    pub fn nth_unordered(&self, i: usize) -> Option<(&[u8], f64)> {
        self.dict.at(i).map(|(m, s)| (m, *s))
    }

    /// The number of entries for which `pred` holds, given that it holds for
    /// a prefix of the order and for nothing after: the rank where it stops.
    fn partition_point(&self, pred: impl Fn(f64, &[u8]) -> bool) -> usize {
        let mut rank = 0;
        for b in self.blocks.iter() {
            match b.last() {
                Some((s, m)) if pred(*s, m) => rank += b.len(),
                _ => return rank + b.partition_point(|(s, m)| pred(*s, m)),
            }
        }
        rank
    }

    /// The block holding rank `r` and the offset within it; for `r == len`,
    /// the end of the last block.
    fn locate(&self, mut r: usize) -> (usize, usize) {
        for (bi, b) in self.blocks.iter().enumerate() {
            if r < b.len() {
                return (bi, r);
            }
            r -= b.len();
        }
        let last = self.blocks.len().saturating_sub(1);
        (last, self.blocks.get(last).map_or(0, |b| b.len()))
    }

    fn rank_of(&self, score: f64, member: &[u8]) -> usize {
        self.partition_point(|s, m| cmp((s, m), (score, member)) == Ordering::Less)
    }

    /// Set `member`'s score, adding it if new; the score it had.
    pub fn insert(&mut self, member: &[u8], score: f64) -> Option<f64> {
        let old = self.dict.get(member).copied();
        if old == Some(score) {
            return old;
        }
        // The dictionary first: a new member allocates there, and an
        // allocation failing before the order is touched leaves the two in
        // agreement.
        self.dict.insert(member, score);
        if let Some(s) = old {
            self.unlink(s, member);
        }
        self.link(score, member);
        old
    }

    fn link(&mut self, score: f64, member: &[u8]) {
        let entry = (score, bytes_in(member, self.alloc()));
        let rank = self.rank_of(score, member);
        if self.blocks.is_empty() {
            self.blocks.push(Vec::new_in(self.alloc()));
        }
        let (bi, off) = self.locate(rank);
        let block = &mut self.blocks[bi];
        block.insert(off, entry);
        if block.len() > 2 * BLOCK {
            let tail = block.split_off(BLOCK);
            self.blocks.insert(bi + 1, tail);
        }
    }

    fn unlink(&mut self, score: f64, member: &[u8]) {
        let (bi, off) = self.locate(self.rank_of(score, member));
        let block = &mut self.blocks[bi];
        block.remove(off);
        if block.is_empty() {
            self.blocks.remove(bi);
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let s = self.dict.remove(member)?;
        self.unlink(s, member);
        Some(s)
    }

    /// The member's rank from the low end.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let s = self.score(member)?;
        Some(self.rank_of(s, member))
    }

    /// Entries from rank `start` upward.
    pub fn iter_from(&self, start: usize) -> impl Iterator<Item = (&[u8], f64)> {
        let (bi, off) = self.locate(start);
        self.blocks
            .iter()
            .skip(bi)
            .enumerate()
            .flat_map(move |(i, b)| b.iter().skip(if i == 0 { off } else { 0 }))
            .map(|(s, m)| (m.as_slice(), *s))
    }

    /// Entries from rank `end - 1` downward, `end` entries in all.
    pub fn iter_rev_from(&self, end: usize) -> impl Iterator<Item = (&[u8], f64)> {
        let len = self.len();
        let skip = len - end.min(len);
        self.blocks
            .iter()
            .rev()
            .flat_map(|b| b.iter().rev())
            .skip(skip)
            .map(|(s, m)| (m.as_slice(), *s))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], f64)> {
        self.iter_from(0)
    }

    /// The first rank whose score satisfies `min`.
    pub fn score_start(&self, min: &ScoreBound) -> usize {
        self.partition_point(|s, _| min.below_min(s))
    }

    /// One past the last rank whose score satisfies `max`.
    pub fn score_end(&self, max: &ScoreBound) -> usize {
        self.partition_point(|s, _| max.within_max(s))
    }

    /// As `score_start`, by member: meaningful when every score is equal,
    /// which is what the lex commands require.
    pub fn lex_start(&self, min: &LexBound) -> usize {
        self.partition_point(|_, m| min.below_min(m))
    }

    pub fn lex_end(&self, max: &LexBound) -> usize {
        self.partition_point(|_, m| max.within_max(m))
    }

    /// Remove ranks `start..end`; how many went.
    pub fn remove_range(&mut self, start: usize, end: usize) -> usize {
        let doomed: std::vec::Vec<std::vec::Vec<u8>> = self
            .iter_from(start)
            .take(end.saturating_sub(start))
            .map(|(m, _)| m.to_vec())
            .collect();
        for m in &doomed {
            self.remove(m);
        }
        doomed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::Global;

    fn zset() -> ZSet<Global> {
        ZSet::new_in(Seed(3, 4), Global)
    }

    fn order(z: &ZSet<Global>) -> std::vec::Vec<(std::vec::Vec<u8>, f64)> {
        z.iter().map(|(m, s)| (m.to_vec(), s)).collect()
    }

    #[test]
    fn keeps_score_then_member_order_across_block_splits() {
        let mut z = zset();
        let mut model: std::collections::BTreeMap<std::vec::Vec<u8>, f64> = Default::default();
        let mut x: u32 = 11;
        for _ in 0..3000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            let m = format!("m{}", x % 700).into_bytes();
            if x.is_multiple_of(5) {
                assert_eq!(z.remove(&m), model.remove(&m));
            } else {
                let s = (x % 50) as f64;
                assert_eq!(z.insert(&m, s), model.insert(m, s));
            }
        }
        let mut want: std::vec::Vec<_> = model.into_iter().collect();
        want.sort_by(|a, b| cmp((a.1, &a.0), (b.1, &b.0)));
        assert_eq!(order(&z), want);
        for (r, (m, _)) in want.iter().enumerate() {
            assert_eq!(z.rank(m), Some(r));
        }
        let rev: std::vec::Vec<_> = z.iter_rev_from(z.len()).map(|(m, _)| m.to_vec()).collect();
        assert_eq!(
            rev,
            want.iter()
                .rev()
                .map(|(m, _)| m.clone())
                .collect::<std::vec::Vec<_>>()
        );
    }

    #[test]
    fn score_and_lex_bounds_give_ranks() {
        let mut z = zset();
        for (i, m) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            z.insert(m.as_bytes(), i as f64);
        }
        let b = |value, exclusive| ScoreBound { value, exclusive };
        assert_eq!(z.score_start(&b(1.0, false)), 1);
        assert_eq!(z.score_start(&b(1.0, true)), 2);
        assert_eq!(z.score_end(&b(3.0, false)), 4);
        assert_eq!(z.score_end(&b(3.0, true)), 3);
        let mut lex = zset();
        for m in ["a", "b", "c", "d"] {
            lex.insert(m.as_bytes(), 0.0);
        }
        assert_eq!(lex.lex_start(&LexBound::Exclusive(b"a".to_vec())), 1);
        assert_eq!(lex.lex_end(&LexBound::Inclusive(b"c".to_vec())), 3);
        assert_eq!(lex.lex_end(&LexBound::PosInf), 4);
        assert_eq!(lex.lex_start(&LexBound::NegInf), 0);
        assert_eq!(z.remove_range(1, 3), 2);
        assert_eq!(
            order(&z)
                .iter()
                .map(|(m, _)| m.as_slice())
                .collect::<std::vec::Vec<_>>(),
            [b"a", b"d", b"e"]
        );
    }
}
