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
    #[allow(deprecated)] // SipHasher is deprecated only in favour of unkeyed hashers.
    pub fn hash(self, bytes: &[u8]) -> u64 {
        use std::hash::Hasher;
        let mut h = std::hash::SipHasher::new_with_keys(self.0, self.1);
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

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    fn position(&self, key: &[u8]) -> Option<usize> {
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
        (s.key, s.value)
    }

    pub fn at(&self, i: usize) -> Option<(&[u8], &V)> {
        self.slots.get(i).map(|s| (s.key.as_slice(), &s.value))
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

    pub fn clear(&mut self) {
        self.index.clear();
        self.slots.clear();
    }

    /// Keep only the entries `keep` accepts.
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
}
