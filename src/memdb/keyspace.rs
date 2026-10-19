//! A database's keyspace: the dictionary of entries, and an index of the
//! keys that carry a deadline — Redis's `expires` dictionary, which the
//! `volatile-*` policies sample from and the expiry sweep walks.
//!
//! The index is a list of positions in the dictionary, and each entry knows
//! its place in the list, so adding, removing and moving are all O(1). A
//! dictionary removal moves its last entry into the hole (`Dict::remove_at`),
//! which keeps SCAN's guarantee; the index follows that move.
//!
//! An entry's deadline is private to this module, so nothing can set one
//! without the index hearing of it. Every change that allocates reserves the
//! index's room first: an allocation that fails panics (`crate::shm`), and
//! failing before either structure changes leaves the two consistent.
#![forbid(unsafe_code)]

use allocator_api2::alloc::Allocator;
use allocator_api2::vec::Vec;

use super::Bytes;
use super::db::Value;
use super::dict::{Dict, Seed};

const NONE: u32 = u32::MAX;

pub struct Entry<A: Allocator + Clone> {
    pub value: Value<A>,
    /// Microseconds since the Unix epoch; 0 for none.
    deadline: i64,
    /// Redis's 24-bit `lru` field: an LRU clock or LFU data (`lru.rs`).
    pub lru: u32,
    /// Where this entry is in the index of volatile keys, or `NONE`.
    vslot: u32,
}

impl<A: Allocator + Clone> Entry<A> {
    pub fn new(value: Value<A>, deadline: i64, lru: u32) -> Self {
        Entry {
            value,
            deadline,
            lru,
            vslot: NONE,
        }
    }

    pub fn deadline(&self) -> i64 {
        self.deadline
    }

    pub fn expired(&self, now: i64) -> bool {
        self.deadline != 0 && self.deadline <= now
    }
}

pub struct Keyspace<A: Allocator + Clone> {
    dict: Dict<Entry<A>, A>,
    volatile: Vec<u32, A>,
}

impl<A: Allocator + Clone> Keyspace<A> {
    pub fn new_in(seed: Seed, alloc: A) -> Self {
        Keyspace {
            dict: Dict::new_in(seed, alloc.clone()),
            volatile: Vec::new_in(alloc),
        }
    }

    pub fn alloc(&self) -> A {
        self.dict.alloc()
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    /// Keys with a deadline, live or past it.
    pub fn volatile_len(&self) -> usize {
        self.volatile.len()
    }

    /// The dictionary position of the `j`th volatile key.
    pub fn volatile_at(&self, j: usize) -> Option<usize> {
        self.volatile.get(j).map(|&p| p as usize)
    }

    pub fn position(&self, key: &[u8]) -> Option<usize> {
        self.dict.position(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry<A>> {
        self.dict.get(key)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry<A>> {
        self.dict.get_mut(key)
    }

    pub fn at(&self, i: usize) -> Option<(&[u8], &Entry<A>)> {
        self.dict.at(i)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &Entry<A>)> {
        self.dict.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.dict.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = &Entry<A>> {
        self.dict.values()
    }

    pub fn scan(&self, cursor: u64, count: usize, visit: impl FnMut(&[u8], &Entry<A>)) -> u64 {
        self.dict.scan(cursor, count, visit)
    }

    fn entry_at(&mut self, i: usize) -> &mut Entry<A> {
        self.dict
            .value_at_mut(i)
            .expect("an index position is in the dictionary")
    }

    /// Add the entry at position `i` to the index. Room must be reserved.
    fn index(&mut self, i: usize) {
        let j = self.volatile.len();
        self.volatile.push(i as u32);
        self.entry_at(i).vslot = j as u32;
    }

    /// Take the entry at position `i` out of the index.
    fn unindex(&mut self, i: usize) {
        let j = std::mem::replace(&mut self.entry_at(i).vslot, NONE);
        if j == NONE {
            return;
        }
        let j = j as usize;
        self.volatile.swap_remove(j);
        if let Some(&p) = self.volatile.get(j) {
            self.entry_at(p as usize).vslot = j as u32;
        }
    }

    /// Set the deadline of the entry at position `i`, 0 for none.
    pub fn set_deadline_at(&mut self, i: usize, deadline: i64) {
        let was = self.entry_at(i).deadline;
        if (was != 0) == (deadline != 0) {
            self.entry_at(i).deadline = deadline;
            return;
        }
        if deadline == 0 {
            self.unindex(i);
        } else {
            self.volatile.reserve(1);
            self.index(i);
        }
        self.entry_at(i).deadline = deadline;
    }

    pub fn set_deadline(&mut self, key: &[u8], deadline: i64) -> bool {
        match self.position(key) {
            Some(i) => {
                self.set_deadline_at(i, deadline);
                true
            }
            None => false,
        }
    }

    /// Set `key` to `entry`, whose own index slot is ignored; the entry it
    /// replaced.
    pub fn insert(&mut self, key: &[u8], mut entry: Entry<A>) -> Option<Entry<A>> {
        entry.vslot = NONE;
        let deadline = std::mem::replace(&mut entry.deadline, 0);
        self.volatile.reserve(1);
        let (i, old) = match self.position(key) {
            Some(i) => {
                let e = self.entry_at(i);
                entry.deadline = e.deadline;
                entry.vslot = e.vslot;
                (i, Some(std::mem::replace(e, entry)))
            }
            None => {
                self.dict.grow_within(crate::shm::room());
                self.dict.insert(key, entry);
                (self.dict.len() - 1, None)
            }
        };
        self.set_deadline_at(i, deadline);
        old.map(|mut o| {
            o.vslot = NONE;
            o
        })
    }

    /// The entry at `key`, inserting `make()` first when there is none (with
    /// no deadline); and whether it was inserted.
    pub fn get_or_insert_with(
        &mut self,
        key: &[u8],
        make: impl FnOnce() -> Entry<A>,
    ) -> (&mut Entry<A>, bool) {
        self.dict.grow_within(crate::shm::room());
        let (e, new) = self.dict.get_or_insert_with(key, || {
            let mut e = make();
            e.deadline = 0;
            e.vslot = NONE;
            e
        });
        (e, new)
    }

    /// Remove the entry at position `i`, moving the last one into its place.
    pub fn remove_at(&mut self, i: usize) -> (Bytes<A>, Entry<A>) {
        self.unindex(i);
        let last = self.dict.len() - 1;
        let (k, e) = self.dict.remove_at(i);
        if i != last {
            let j = self.entry_at(i).vslot;
            if j != NONE {
                self.volatile[j as usize] = i as u32;
            }
        }
        (k, e)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Entry<A>> {
        let i = self.position(key)?;
        Some(self.remove_at(i).1)
    }

    /// Keep only the entries `keep` accepts.
    #[cfg(test)]
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8], &mut Entry<A>) -> bool) {
        let mut i = 0;
        while i < self.dict.len() {
            let stays = match self.dict.at(i) {
                Some((k, _)) => {
                    let k = k.to_vec();
                    keep(&k, self.entry_at(i))
                }
                None => break,
            };
            if stays {
                i += 1;
            } else {
                self.remove_at(i);
            }
        }
    }

    pub fn clear(&mut self) {
        self.dict.clear();
        self.volatile.clear();
    }

    /// Every position the index holds points back at itself, and every
    /// entry with a deadline is in it.
    #[cfg(test)]
    pub fn check(&self) {
        for (j, &p) in self.volatile.iter().enumerate() {
            let (_, e) = self.dict.at(p as usize).expect("index points past the end");
            assert_eq!(e.vslot as usize, j);
            assert_ne!(e.deadline, 0);
        }
        let with = self.dict.values().filter(|e| e.deadline != 0).count();
        assert_eq!(with, self.volatile.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::Global;

    fn ks() -> Keyspace<Global> {
        Keyspace::new_in(Seed(3, 4), Global)
    }

    fn val() -> Value<Global> {
        Value::Str(Bytes::new_in(Global))
    }

    #[test]
    fn the_volatile_index_follows_every_change() {
        let mut k = ks();
        let mut x = 0x1234_5678u64;
        for step in 0..20_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let key = format!("k{}", x % 300).into_bytes();
            match x % 7 {
                0 | 1 => {
                    k.insert(&key, Entry::new(val(), (x % 3) as i64 * 5, 0));
                }
                2 => {
                    k.remove(&key);
                }
                3 => {
                    k.set_deadline(&key, (x % 2) as i64 * 9);
                }
                4 => {
                    k.get_or_insert_with(&key, || Entry::new(val(), 77, 0));
                }
                5 if step % 50 == 0 => k.retain(|_, e| e.deadline() == 0 || x.is_multiple_of(2)),
                _ => {
                    if k.len() > 0 {
                        k.remove_at((x as usize) % k.len());
                    }
                }
            }
            k.check();
        }
        assert!(k.volatile_len() > 0);
    }

    #[test]
    fn insert_keeps_or_drops_the_deadline_it_is_given() {
        let mut k = ks();
        k.insert(b"a", Entry::new(val(), 10, 1));
        k.insert(b"b", Entry::new(val(), 0, 2));
        assert_eq!(k.volatile_len(), 1);
        let old = k.insert(b"a", Entry::new(val(), 0, 3)).unwrap();
        assert_eq!(old.deadline(), 10);
        assert_eq!(k.volatile_len(), 0);
        assert_eq!(k.get(b"a").unwrap().lru, 3);
        k.check();
    }
}
