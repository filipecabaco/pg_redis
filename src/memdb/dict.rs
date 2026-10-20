//! A byte-keyed map with dense storage: entries live in a `Vec`, and a
//! SwissTable of positions finds them. Dense storage is what Redis needs from
//! a dictionary beyond lookup — a random entry in O(1), for `RANDOMKEY`,
//! `SRANDMEMBER` and eviction sampling — and removing by swapping the last
//! entry into the hole keeps it dense.
#![forbid(unsafe_code)]

use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;
use hashbrown::HashTable;

use super::{Bytes, bytes_in};

/// The SipHash key every table in one server shares: drawn once per
/// postmaster, so a client cannot aim keys at one bucket, and the same in
/// every worker, so a table built by one is readable by all.
#[derive(Clone, Copy, Debug)]
pub struct Seed(pub u64, pub u64);

impl Seed {
    pub fn hash(self, bytes: &[u8]) -> u64 {
        use std::hash::Hasher;
        let mut h = siphasher::sip::SipHasher::new_with_keys(self.0, self.1);
        h.write(bytes);
        h.finish()
    }
}

struct Slot<V, A: Allocator> {
    hash: u64,
    key: Bytes<A>,
    value: V,
}

pub struct Dict<V, A: Allocator + Clone> {
    index: HashTable<u32, A>,
    slots: Vec<Slot<V, A>, A>,
    seed: Seed,
}

impl<V, A: Allocator + Clone> Dict<V, A> {
    pub fn new_in(seed: Seed, alloc: A) -> Self {
        Dict {
            index: HashTable::new_in(alloc.clone()),
            slots: Vec::new_in(alloc),
            seed,
        }
    }

    pub fn alloc(&self) -> A {
        self.slots.allocator().clone()
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Before adding an entry: when the entries are at capacity and doubling
    /// them would take more than `room` bytes, grow by what fits in the room
    /// instead, at least 16 entries and at most an eighth. Redis likewise
    /// declines to grow a dictionary past `maxmemory` (`dictExpandAllowed`):
    /// a doubling that eviction cannot take back would leave the arena over
    /// the limit with nothing left to evict.
    pub fn grow_within(&mut self, room: Option<usize>) {
        let cap = self.slots.capacity();
        if self.slots.len() < cap || cap == 0 {
            return;
        }
        let per = std::mem::size_of::<Slot<V, A>>() + std::mem::size_of::<u32>();
        if let Some(room) = room.filter(|&r| cap * per > r) {
            self.slots
                .reserve_exact((room / per).clamp(16, (cap / 8).max(16)));
        }
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn position(&self, key: &[u8]) -> Option<usize> {
        let hash = self.seed.hash(key);
        let slots = &self.slots;
        self.index
            .find(hash, |&i| slots[i as usize].key.as_slice() == key)
            .map(|&i| i as usize)
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.position(key).map(|i| &self.slots[i].value)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.position(key).map(|i| &mut self.slots[i].value)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.position(key).is_some()
    }

    /// The value at `key`, inserting `make()` first when there is none; and
    /// whether it was inserted.
    pub fn get_or_insert_with(&mut self, key: &[u8], make: impl FnOnce() -> V) -> (&mut V, bool) {
        if let Some(i) = self.position(key) {
            return (&mut self.slots[i].value, false);
        }
        let i = self.push(key, make());
        (&mut self.slots[i].value, true)
    }

    /// Set `key` to `value`; the value it replaced.
    pub fn insert(&mut self, key: &[u8], value: V) -> Option<V> {
        if let Some(i) = self.position(key) {
            return Some(std::mem::replace(&mut self.slots[i].value, value));
        }
        self.push(key, value);
        None
    }

    /// Append an entry known to be absent; its position.
    fn push(&mut self, key: &[u8], value: V) -> usize {
        let key = bytes_in(key, self.alloc());
        let hash = self.seed.hash(&key);
        let i = self.slots.len();
        self.slots.push(Slot { hash, key, value });
        let slots = &self.slots;
        self.index
            .insert_unique(hash, i as u32, |&j| slots[j as usize].hash);
        i
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        self.remove_entry(key).map(|(_, v)| v)
    }

    pub fn remove_entry(&mut self, key: &[u8]) -> Option<(Bytes<A>, V)> {
        let i = self.position(key)?;
        Some(self.remove_at(i))
    }

    /// Remove the entry at position `i`, moving the last one into its place.
    pub fn remove_at(&mut self, i: usize) -> (Bytes<A>, V) {
        let hash = self.slots[i].hash;
        if let Ok(e) = self.index.find_entry(hash, |&j| j as usize == i) {
            e.remove();
        }
        let last = self.slots.len() - 1;
        if i != last {
            let moved = self.slots[last].hash;
            if let Some(p) = self.index.find_mut(moved, |&j| j as usize == last) {
                *p = i as u32;
            }
        }
        let s = self.slots.swap_remove(i);
        self.shrink_if_sparse();
        (s.key, s.value)
    }

    /// Redis's `htNeedsResize`: a table less than a tenth full gives back
    /// what it does not use, as `tryResizeHashTables` shrinks it. Entries
    /// keep their positions, so a `SCAN` cursor holds across it.
    fn shrink_if_sparse(&mut self) {
        const MIN: usize = 64;
        let cap = self.slots.capacity();
        if cap > MIN && self.slots.len() * 10 < cap {
            let want = (self.slots.len() * 2).max(MIN);
            self.slots.shrink_to(want);
            let slots = &self.slots;
            self.index.shrink_to(want, |&j| slots[j as usize].hash);
        }
    }

    /// Remove `key` keeping the order of the rest, as a listpack deletes:
    /// every later entry moves down one. O(n), for the small collections
    /// that are listpacks. An entry only moves to a lower position, which is
    /// what [`Dict::scan`] relies on.
    pub fn remove_ordered(&mut self, key: &[u8]) -> Option<V> {
        let i = self.position(key)?;
        Some(self.remove_at_ordered(i).1)
    }

    /// [`Dict::remove_ordered`] by position.
    pub fn remove_at_ordered(&mut self, i: usize) -> (Bytes<A>, V) {
        let hash = self.slots[i].hash;
        if let Ok(e) = self.index.find_entry(hash, |&j| j as usize == i) {
            e.remove();
        }
        for p in self.index.iter_mut() {
            if *p as usize > i {
                *p -= 1;
            }
        }
        let s = self.slots.remove(i);
        self.shrink_if_sparse();
        (s.key, s.value)
    }

    /// Put the entries in the order `key` sorts them, as an intset is when
    /// it becomes a listpack. Entries can move up, so a `SCAN` cursor taken
    /// before can miss some; the collections this reorders are scanned in
    /// one call, which takes no cursor.
    pub fn sort_by_cached_key<K: Ord>(&mut self, mut key: impl FnMut(&[u8], &V) -> K) {
        self.slots.sort_by_cached_key(|s| key(&s.key, &s.value));
        self.index.clear();
        let slots = &self.slots;
        for (i, s) in slots.iter().enumerate() {
            self.index
                .insert_unique(s.hash, i as u32, |&j| slots[j as usize].hash);
        }
    }

    pub fn at(&self, i: usize) -> Option<(&[u8], &V)> {
        self.slots.get(i).map(|s| (s.key.as_slice(), &s.value))
    }

    pub fn value_at_mut(&mut self, i: usize) -> Option<&mut V> {
        self.slots.get_mut(i).map(|s| &mut s.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &V)> {
        self.slots.iter().map(|s| (s.key.as_slice(), &s.value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.slots.iter().map(|s| s.key.as_slice())
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.slots.iter().map(|s| &s.value)
    }

    /// Empty, giving the memory back, as Redis's `emptyDb` frees its tables.
    pub fn clear(&mut self) {
        let alloc = self.alloc();
        self.index = HashTable::new_in(alloc.clone());
        self.slots = Vec::new_in(alloc);
    }

    /// One step of a `SCAN`-style cursor: visit up to `count` entries and
    /// return the cursor to continue from, 0 when the iteration is done.
    ///
    /// The cursor is a position, and positions are visited from the top
    /// down. An insert appends at the end and a removal moves the last entry
    /// into the hole, so an entry only ever moves to a lower position. Every
    /// entry not yet visited therefore stays below the cursor however the
    /// dictionary changes between calls, which is Redis's guarantee: an entry
    /// present from the first call to the last is visited at least once. One
    /// that moves down from a position already visited is visited again.
    ///
    /// Cursor 0 starts at the top. A cursor past the end, from before
    /// removals or from another dictionary, is clamped to it. Each call costs
    /// O(`count`), and the cursor shrinks on every call, so the iteration
    /// ends.
    pub fn scan(&self, cursor: u64, count: usize, mut visit: impl FnMut(&[u8], &V)) -> u64 {
        let len = self.slots.len();
        let hi = match cursor {
            0 => len,
            c => usize::try_from(c).unwrap_or(usize::MAX).min(len),
        };
        let lo = hi.saturating_sub(count.max(1));
        for s in self.slots[lo..hi].iter().rev() {
            visit(&s.key, &s.value);
        }
        lo as u64
    }

    /// Keep only the entries `keep` accepts.
    #[cfg(test)]
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8], &mut V) -> bool) {
        let mut i = 0;
        while i < self.slots.len() {
            let s = &mut self.slots[i];
            if keep(&s.key, &mut s.value) {
                i += 1;
            } else {
                self.remove_at(i);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::Global;

    fn dict() -> Dict<u32, Global> {
        Dict::new_in(Seed(1, 2), Global)
    }

    #[test]
    fn insert_get_remove_keep_positions_consistent() {
        let mut d = dict();
        for i in 0..1000u32 {
            assert!(d.insert(format!("k{i}").as_bytes(), i).is_none());
        }
        assert_eq!(d.insert(b"k5", 55), Some(5));
        for i in (0..1000u32).step_by(3) {
            assert!(d.remove(format!("k{i}").as_bytes()).is_some());
        }
        for i in 0..1000u32 {
            let want = if i % 3 == 0 {
                None
            } else if i == 5 {
                Some(55)
            } else {
                Some(i)
            };
            assert_eq!(d.get(format!("k{i}").as_bytes()).copied(), want, "k{i}");
        }
        assert_eq!(d.len(), 1000 - 334);
    }

    #[test]
    fn growth_near_the_limit_is_an_eighth_not_a_doubling() {
        let mut d = dict();
        for i in 0..64u32 {
            d.insert(&i.to_be_bytes(), i);
        }
        while d.slots.len() < d.slots.capacity() {
            let n = d.len() as u32;
            d.insert(&n.to_be_bytes(), n);
        }
        let cap = d.slots.capacity();
        d.grow_within(Some(1));
        assert_eq!(d.slots.capacity(), cap + 16, "no room: the least step");
        // With room to spare, the Vec doubles as usual on the next push.
        let mut e = dict();
        for i in 0..64u32 {
            e.insert(&i.to_be_bytes(), i);
        }
        let cap = e.slots.capacity();
        e.grow_within(None);
        assert_eq!(e.slots.capacity(), cap);
    }

    #[test]
    fn retain_and_get_or_insert() {
        let mut d = dict();
        for i in 0..100u32 {
            d.insert(&i.to_be_bytes(), i);
        }
        d.retain(|_, v| *v % 2 == 0);
        assert_eq!(d.len(), 50);
        let (v, new) = d.get_or_insert_with(&3u32.to_be_bytes(), || 3);
        assert!(new);
        *v += 1;
        assert_eq!(d.get(&3u32.to_be_bytes()), Some(&4));
        assert!(!d.get_or_insert_with(&4u32.to_be_bytes(), || 0).1);
    }

    #[test]
    fn an_ordered_removal_keeps_the_rest_in_order_and_findable() {
        let mut d = dict();
        for i in 0..10u32 {
            d.insert(&key(i), i);
        }
        assert_eq!(d.remove_ordered(&key(3)), Some(3));
        assert_eq!(d.remove_ordered(&key(0)), Some(0));
        assert_eq!(d.remove_ordered(&key(42)), None);
        let order: std::vec::Vec<u32> = d.values().copied().collect();
        assert_eq!(order, [1, 2, 4, 5, 6, 7, 8, 9]);
        for i in [1, 2, 4, 9] {
            assert_eq!(d.get(&key(i)), Some(&i), "k{i}");
        }
        // Appending after an ordered removal lands at the end.
        d.insert(&key(3), 3);
        assert_eq!(d.at(d.len() - 1).map(|(_, v)| *v), Some(3));
    }

    #[test]
    fn sorting_reorders_entries_and_keeps_them_findable() {
        let mut d = dict();
        for i in [5u32, 1, 4, 2, 3] {
            d.insert(&key(i), i);
        }
        d.sort_by_cached_key(|_, v| *v);
        let order: std::vec::Vec<u32> = d.values().copied().collect();
        assert_eq!(order, [1, 2, 3, 4, 5]);
        for i in 1..=5 {
            assert_eq!(d.get(&key(i)), Some(&i));
        }
        assert_eq!(d.remove(&key(1)), Some(1));
        assert_eq!(d.get(&key(5)), Some(&5));
    }

    fn key(i: u32) -> std::vec::Vec<u8> {
        format!("k{i}").into_bytes()
    }

    /// Run a whole iteration, calling `between` after each step with the
    /// step number; every key returned, repeats included, and the number of
    /// calls.
    fn iterate(
        d: &mut Dict<u32, Global>,
        count: usize,
        mut between: impl FnMut(&mut Dict<u32, Global>, usize),
    ) -> (std::vec::Vec<std::vec::Vec<u8>>, usize) {
        let mut seen = std::vec::Vec::new();
        let mut cursor = 0;
        let mut calls = 0;
        loop {
            cursor = d.scan(cursor, count, |k, _| seen.push(k.to_vec()));
            calls += 1;
            assert!(calls < 100_000, "the iteration did not end");
            if cursor == 0 {
                return (seen, calls);
            }
            between(d, calls);
        }
    }

    #[test]
    fn a_full_scan_returns_every_entry_once_when_nothing_changes() {
        let mut d = dict();
        for i in 0..1000 {
            d.insert(&key(i), i);
        }
        let (mut seen, calls) = iterate(&mut d, 10, |_, _| {});
        assert_eq!(calls, 100, "1000 entries, 10 a call");
        seen.sort();
        let mut want: std::vec::Vec<_> = (0..1000).map(key).collect();
        want.sort();
        assert_eq!(seen, want);
    }

    #[test]
    fn entries_present_throughout_survive_removals_and_inserts_mid_scan() {
        // Many runs with different churn, so the swap-into-the-hole moves
        // land on both sides of the cursor.
        for seed in 1..40u32 {
            let mut d = dict();
            for i in 0..500 {
                d.insert(&key(i), i);
            }
            let mut removed = std::collections::HashSet::new();
            let mut x = seed.wrapping_mul(2654435761) | 1;
            let mut next_new = 10_000;
            let (seen, _) = iterate(&mut d, 7, |d, _| {
                for _ in 0..5 {
                    x ^= x << 13;
                    x ^= x >> 17;
                    x ^= x << 5;
                    let i = x % 500;
                    if d.remove(&key(i)).is_some() {
                        removed.insert(i);
                    }
                    d.insert(&key(next_new), next_new);
                    next_new += 1;
                }
            });
            let seen: std::collections::HashSet<_> = seen.into_iter().collect();
            for i in (0..500).filter(|i| !removed.contains(i)) {
                assert!(seen.contains(&key(i)), "seed {seed}: k{i} was skipped");
            }
        }
    }

    #[test]
    fn a_cursor_past_the_end_is_clamped_and_every_scan_ends() {
        let mut d = dict();
        for i in 0..20 {
            d.insert(&key(i), i);
        }
        // Past the end, and far past anything a usize holds on 32 bits.
        let mut seen = std::vec::Vec::new();
        let next = d.scan(u64::MAX, 5, |k, _| seen.push(k.to_vec()));
        assert_eq!(next, 15);
        assert_eq!(seen.len(), 5);
        // Shrinking under a cursor still ends the iteration.
        let mut cursor = d.scan(0, 3, |_, _| {});
        assert_eq!(cursor, 17);
        for i in 0..18 {
            d.remove(&key(i));
        }
        let mut seen = std::vec::Vec::new();
        cursor = d.scan(cursor, 3, |k, _| seen.push(k.to_vec()));
        assert_eq!(cursor, 0);
        seen.sort();
        assert_eq!(seen, [key(18), key(19)]);
        // An empty dictionary ends at once, and a COUNT of 0 still moves.
        assert_eq!(dict().scan(0, 10, |_, _| {}), 0);
        assert_eq!(d.scan(2, 0, |_, _| {}), 1);
    }
}
