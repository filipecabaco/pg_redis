//! One database: the keyspace and every command's effect on it, as the
//! [`Store`] the shared command code runs against.
//!
//! Each method is atomic because its caller holds the database's lock for
//! the whole command (`crate::shm`); nothing here waits or locks.
//!
//! A key past its deadline is gone: [`Db::entry`] removes it on the way to
//! any read or write, and the worker's sweep removes the ones nobody asks
//! for. Reads of a key holding another type see nothing — the type check
//! before the command has already refused that, except where Redis answers
//! nil instead (`MGET`).
#![forbid(unsafe_code)]

use allocator_api2::alloc::Allocator;

use super::deque::Deque;
use super::dict::{Dict, Seed};
use super::encoding::{self, Enc, Limits, SetBuilder, SetView};
use super::keyspace::{Entry, Keyspace};
use super::lru::{self, Tracking};
use super::zset::ZSet;
use super::{Bytes, KeyKind, bytes_in, fast_random, parse_stored_int, random_picks, range_bounds};
use crate::commands::{ExpireCond, GetExExpiry, KeyDump, LexBound, Response, ScoreBound};
use crate::longdouble::{LongDouble, increment};
use crate::store::{Incr, LSet, Num, Out, SetArgs, SetDone, Store, ZaddOpts};

/// `proto-max-bulk-len`: no string grows past this.
const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

pub enum Value<A: Allocator + Clone> {
    Str(Bytes<A>),
    List(Deque<Bytes<A>, A>),
    /// A hash, a set and a sorted set carry the encoding Redis would have
    /// given them (`encoding.rs`), which decides their order.
    Hash(Dict<Bytes<A>, A>, Enc),
    Set(Dict<(), A>, Enc),
    Zset(ZSet<A>, Enc),
    /// A stream's serialised value — see `stream.rs`.
    Stream(Bytes<A>),
}

impl<A: Allocator + Clone> Value<A> {
    pub fn kind(&self) -> KeyKind {
        match self {
            Value::Str(_) => KeyKind::String,
            Value::List(_) => KeyKind::List,
            Value::Hash(..) => KeyKind::Hash,
            Value::Set(..) => KeyKind::Set,
            Value::Zset(..) => KeyKind::Zset,
            Value::Stream(_) => KeyKind::Stream,
        }
    }

    /// A collection with nothing left in it, which Redis never keeps.
    fn is_empty_collection(&self) -> bool {
        match self {
            Value::List(l) => l.is_empty(),
            Value::Hash(h, _) => h.is_empty(),
            Value::Set(s, _) => s.is_empty(),
            Value::Zset(z, _) => z.is_empty(),
            Value::Str(_) | Value::Stream(_) => false,
        }
    }
}

fn held_deadline<A: Allocator + Clone>(e: &Entry<A>) -> Option<i64> {
    (e.deadline() != 0).then_some(e.deadline())
}

fn now() -> i64 {
    crate::server::now_ms().saturating_mul(1000)
}

/// The access field a new key starts with under the policy in force.
fn fresh() -> u32 {
    lru::access().tracking.fresh(crate::server::now_ms())
}

/// A key the eviction sampler drew, with its score: higher goes first.
pub struct Sample {
    pub score: u64,
    pub key: Vec<u8>,
}

/// What the eviction sampler scores a key by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Score {
    /// Idle time or inverse frequency, from the access field.
    Access(Tracking),
    /// `volatile-ttl`: the sooner the deadline, the higher.
    Deadline,
}

thread_local! {
    /// Keys removed on access for having passed their deadline, since the
    /// last `take_reaped`. Redis reports such an expiry — the `expired`
    /// event, INFO's `expired_keys`, a tracking invalidation — and that is
    /// done outside the database's lock, by whoever ran the command.
    static REAPED: std::cell::RefCell<Vec<Vec<u8>>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The keys expired on access since the last call.
pub fn take_reaped() -> Vec<Vec<u8>> {
    REAPED.take()
}

fn too_large() -> Response {
    Response::Error("ERR string exceeds maximum allowed size (proto-max-bulk-len)".into())
}

fn vecs<'a>(items: impl Iterator<Item = &'a [u8]>) -> Vec<Vec<u8>> {
    items.map(<[u8]>::to_vec).collect()
}

pub struct Db<A: Allocator + Clone> {
    keys: Keyspace<A>,
    seed: Seed,
}

/// Typed access to a key's collection: `$get` for one that is there (or
/// `None`), `$make` creating it when it is not — `WRONGTYPE` if the key holds
/// something else.
macro_rules! typed {
    ($get:ident, $make:ident, $variant:ident, $ty:ty, $new:expr) => {
        fn $get(&mut self, key: &[u8]) -> Option<&mut $ty> {
            match &mut self.entry(key)?.value {
                Value::$variant(v) => Some(v),
                _ => None,
            }
        }

        fn $make(&mut self, key: &[u8]) -> Out<&mut $ty> {
            let (seed, alloc) = (self.seed, self.keys.alloc());
            self.entry(key);
            let (e, _) = self.keys.get_or_insert_with(key, || {
                Entry::new(Value::$variant(($new)(seed, alloc)), 0, fresh())
            });
            match &mut e.value {
                Value::$variant(v) => Ok(v),
                _ => Err(crate::commands::wrong_type()),
            }
        }
    };
}

/// `typed!` for the collections that carry an encoding: `$get` without it,
/// `$get_enc` with it, and `$make` creating the collection in the encoding
/// `enc` gives — and saying whether it did.
macro_rules! typed_enc {
    ($get:ident, $get_enc:ident, $make:ident, $variant:ident, $ty:ty, $new:expr) => {
        fn $get(&mut self, key: &[u8]) -> Option<&mut $ty> {
            self.$get_enc(key).map(|(v, _)| v)
        }

        fn $get_enc(&mut self, key: &[u8]) -> Option<(&mut $ty, &mut Enc)> {
            match &mut self.entry(key)?.value {
                Value::$variant(v, e) => Some((v, e)),
                _ => None,
            }
        }

        fn $make(
            &mut self,
            key: &[u8],
            enc: impl FnOnce() -> Enc,
        ) -> Out<(&mut $ty, &mut Enc, bool)> {
            let (seed, alloc) = (self.seed, self.keys.alloc());
            self.entry(key);
            let (e, created) = self.keys.get_or_insert_with(key, || {
                Entry::new(Value::$variant(($new)(seed, alloc), enc()), 0, fresh())
            });
            match &mut e.value {
                Value::$variant(v, e) => Ok((v, e, created)),
                _ => Err(crate::commands::wrong_type()),
            }
        }
    };
}

/// A set as the set operations read it: its members in its own order — an
/// intset's sorted — and its encoding.
struct MemSet<'a, A: Allocator + Clone> {
    d: &'a Dict<(), A>,
    enc: Enc,
}

impl<A: Allocator + Clone> SetView for MemSet<'_, A> {
    fn size(&self) -> usize {
        self.d.len()
    }

    fn has(&self, m: &[u8]) -> bool {
        self.d.contains(m)
    }

    fn members(&self) -> Vec<&[u8]> {
        set_order(self.d, self.enc)
    }
}

/// A set's members in the order its encoding keeps them.
fn set_order<A: Allocator + Clone>(d: &Dict<(), A>, enc: Enc) -> Vec<&[u8]> {
    let mut out: Vec<&[u8]> = d.keys().collect();
    if enc == Enc::Intset {
        encoding::sort_ints(&mut out, |m| m);
    }
    out
}

/// Remove `key` from a collection the way its encoding does: in place for a
/// listpack, so the rest keep their order.
fn remove_in<V, A: Allocator + Clone>(d: &mut Dict<V, A>, enc: Enc, key: &[u8]) -> Option<V> {
    if enc == Enc::Listpack {
        d.remove_ordered(key)
    } else {
        d.remove(key)
    }
}

/// The longest member of a set, as text: what an intset converting to a
/// listpack measures.
fn widest<A: Allocator + Clone>(d: &Dict<(), A>) -> usize {
    d.keys().map(<[u8]>::len).max().unwrap_or(0)
}

/// Add `m`, known to be absent, to a set, converting it first as Redis
/// would. An intset becoming anything else is put in its sorted order, which
/// a listpack then keeps.
fn set_insert<A: Allocator + Clone>(s: &mut Dict<(), A>, enc: &mut Enc, m: &[u8], l: &Limits) {
    let next = encoding::set_add(*enc, s.len(), m, || widest(s), l);
    if *enc == Enc::Intset && next != Enc::Intset {
        s.sort_by_cached_key(|k, _| super::parse_stored_int(k).unwrap_or(0));
    }
    *enc = next;
    s.insert(m, ());
}

impl<A: Allocator + Clone> Db<A> {
    pub fn new_in(seed: Seed, alloc: A) -> Self {
        Db {
            keys: Keyspace::new_in(seed, alloc),
            seed,
        }
    }

    fn bytes(&self, s: &[u8]) -> Bytes<A> {
        bytes_in(s, self.keys.alloc())
    }

    /// The live entry at `key`, as Redis's `lookupKey` finds it: one past its
    /// deadline is removed on the way, and a live one has its access field
    /// updated unless the command runs without touching (`lru::Access`).
    pub fn entry(&mut self, key: &[u8]) -> Option<&mut Entry<A>> {
        let access = lru::access();
        let e = self.lookup(key)?;
        if access.touch {
            e.lru = access.tracking.touched(e.lru, crate::server::now_ms());
        }
        Some(e)
    }

    /// `entry` with `LOOKUP_NOTOUCH`: expires, never touches.
    fn lookup(&mut self, key: &[u8]) -> Option<&mut Entry<A>> {
        if self.keys.get(key)?.expired(now()) {
            self.keys.remove(key);
            REAPED.with_borrow_mut(|r| r.push(key.to_vec()));
            return None;
        }
        self.keys.get_mut(key)
    }

    /// Look the keys up as a command reading them does, for the commands
    /// that then read them without `entry`.
    fn touch_all(&mut self, keys: &[Vec<u8>]) {
        for k in keys {
            self.entry(k);
        }
    }

    pub fn key_kind(&mut self, key: &[u8]) -> Option<KeyKind> {
        self.entry(key).map(|e| e.value.kind())
    }

    /// The access field of a live key without touching it, and the integer
    /// a string key holds: what `OBJECT` reads.
    pub fn access_of(&mut self, key: &[u8]) -> Option<(u32, Option<i64>)> {
        let e = self.lookup(key)?;
        let int = match &e.value {
            Value::Str(v) => parse_stored_int(v),
            _ => None,
        };
        Some((e.lru, int))
    }

    /// Set a live key's access field: `RESTORE ... IDLETIME | FREQ`.
    pub fn set_access(&mut self, key: &[u8], field: u32) {
        if let Some(e) = self.lookup(key) {
            e.lru = field;
        }
    }

    /// Write `value` at `key`. A key that was there keeps its access field,
    /// as Redis's `dbSetValue` keeps the old object's: the lookup before the
    /// write has already updated it. A new key starts fresh.
    fn put(&mut self, key: &[u8], value: Value<A>, deadline: i64) {
        let lru = self.keys.get(key).map_or_else(fresh, |e| e.lru);
        self.keys.insert(key, Entry::new(value, deadline, lru));
    }

    fn put_str(&mut self, key: &[u8], value: &[u8], deadline: i64) {
        let v = self.bytes(value);
        self.put(key, Value::Str(v), deadline);
    }

    /// Drop `key` if its collection has emptied.
    fn tidy(&mut self, key: &[u8]) {
        if self
            .keys
            .get(key)
            .is_some_and(|e| e.value.is_empty_collection())
        {
            self.keys.remove(key);
        }
    }

    typed!(string, string_mut, Str, Bytes<A>, |_, a| {
        Bytes::new_in(a)
    });
    typed!(list, list_mut, List, Deque<Bytes<A>, A>, |_, a| {
        Deque::new_in(a)
    });
    typed_enc!(
        hash,
        hash_enc,
        hash_mut,
        Hash,
        Dict<Bytes<A>, A>,
        Dict::new_in
    );
    typed_enc!(set, set_enc, set_mut, Set, Dict<(), A>, Dict::new_in);
    typed_enc!(zset, zset_enc, zset_mut, Zset, ZSet<A>, ZSet::new_in);

    /// The set at `key`, live at `now`, without touching it.
    fn peek_set(&self, key: &[u8], now: i64) -> Option<MemSet<'_, A>> {
        match self.keys.get(key) {
            Some(e) if !e.expired(now) => match &e.value {
                Value::Set(d, enc) => Some(MemSet { d, enc: *enc }),
                _ => None,
            },
            _ => None,
        }
    }

    /// The encoding of the hash, set or sorted set at `key`.
    pub fn encoding_of(&mut self, key: &[u8]) -> Option<Enc> {
        match &self.entry(key)?.value {
            Value::Hash(_, e) | Value::Set(_, e) | Value::Zset(_, e) => Some(*e),
            _ => None,
        }
    }

    /// Up to `limit` keys, live or past their deadline.
    pub fn keys_upto(&self, limit: usize) -> Vec<Vec<u8>> {
        vecs(self.keys.keys().take(limit))
    }

    /// Remove every key.
    pub fn clear_all(&mut self) {
        self.keys.clear();
    }

    /// Remove `key` whatever it holds and whether or not it is live.
    pub fn clear(&mut self, key: &[u8]) {
        self.keys.remove(key);
    }

    pub fn set_deadline(&mut self, key: &[u8], deadline_us: i64) {
        if self.lookup(key).is_some() {
            self.keys.set_deadline(key, deadline_us);
        }
    }

    /// The value and its deadline, 0 for none — `DUMP`, `MOVE`, migration and
    /// snapshots.
    pub fn dump(&mut self, key: &[u8]) -> Option<(KeyDump, i64)> {
        let e = self.entry(key)?;
        let dump = match &e.value {
            Value::Str(v) => KeyDump::String(v.to_vec()),
            Value::Stream(v) => KeyDump::Stream(v.to_vec()),
            Value::List(l) => KeyDump::List(l.iter().map(|v| v.to_vec()).collect()),
            Value::Set(s, enc) => KeyDump::Set(vecs(set_order(s, *enc).into_iter()), *enc),
            Value::Hash(h, enc) => KeyDump::Hash(
                h.iter().map(|(f, v)| (f.to_vec(), v.to_vec())).collect(),
                *enc,
            ),
            Value::Zset(z, enc) => {
                KeyDump::Zset(z.iter().map(|(m, s)| (m.to_vec(), s)).collect(), *enc)
            }
        };
        Some((dump, e.deadline()))
    }

    /// Write `dump` as `key` with a deadline (0 for none), replacing whatever
    /// was there.
    pub fn restore(&mut self, key: &[u8], dump: &KeyDump, deadline_us: i64) {
        let (seed, a) = (self.seed, self.keys.alloc());
        let value = match dump {
            KeyDump::String(v) => Value::Str(bytes_in(v, a)),
            KeyDump::Stream(v) => Value::Stream(bytes_in(v, a)),
            KeyDump::List(items) => {
                let mut l = Deque::new_in(a.clone());
                l.replace_with(items.iter().map(|v| bytes_in(v, a.clone())));
                Value::List(l)
            }
            KeyDump::Set(items, enc) => {
                let mut s = Dict::new_in(seed, a);
                for m in items {
                    s.insert(m, ());
                }
                Value::Set(s, *enc)
            }
            KeyDump::Hash(pairs, enc) => {
                let mut h = Dict::new_in(seed, a.clone());
                for (f, v) in pairs {
                    h.insert(f, bytes_in(v, a.clone()));
                }
                Value::Hash(h, *enc)
            }
            KeyDump::Zset(pairs, enc) => {
                let mut z = ZSet::new_in(seed, a);
                for (m, s) in pairs {
                    z.insert(m, *s);
                }
                Value::Zset(z, *enc)
            }
        };
        // A new object, as `rdbLoadObject` makes one: a fresh access field.
        if value.is_empty_collection() {
            self.keys.remove(key);
        } else {
            self.keys
                .insert(key, Entry::new(value, deadline_us, fresh()));
        }
    }

    /// Remove every key past its deadline: how many, and — when `names` —
    /// which, for keyspace notifications and tracking. Only the keys with a
    /// deadline are looked at.
    pub fn sweep_expired(&mut self, names: bool) -> (usize, Vec<Vec<u8>>) {
        let now = now();
        let dead: Vec<Vec<u8>> = (0..self.keys.volatile_len())
            .filter_map(|j| self.keys.volatile_at(j))
            .filter_map(|i| self.keys.at(i))
            .filter(|(_, e)| e.expired(now))
            .map(|(k, _)| k.to_vec())
            .collect();
        for k in &dead {
            self.keys.remove(k);
        }
        let count = dead.len();
        (count, if names { dead } else { Vec::new() })
    }

    /// Keys the policy may evict here: all of them, or under a `volatile-*`
    /// policy the ones with a deadline.
    pub fn evictable(&self, volatile: bool) -> usize {
        if volatile {
            self.keys.volatile_len()
        } else {
            self.keys.len()
        }
    }

    /// `evictionPoolPopulate`'s sample: up to `count` keys drawn at random,
    /// from every key or only the volatile ones, each with its score.
    pub fn sample(&self, volatile: bool, count: usize, score: Score, out: &mut Vec<Sample>) {
        let n = self.evictable(volatile);
        if n == 0 {
            return;
        }
        let now_ms = crate::server::now_ms();
        for _ in 0..count.min(n) {
            let j = (fast_random() % n as u64) as usize;
            let i = if volatile {
                match self.keys.volatile_at(j) {
                    Some(i) => i,
                    None => continue,
                }
            } else {
                j
            };
            if let Some((k, e)) = self.keys.at(i) {
                let score = match score {
                    Score::Access(t) => t.score(e.lru, now_ms),
                    Score::Deadline => u64::MAX - e.deadline().max(0) as u64,
                };
                out.push(Sample {
                    score,
                    key: k.to_vec(),
                });
            }
        }
    }

    /// Evict `key` if it is still there and, under a `volatile-*` policy,
    /// still has a deadline; whether it was.
    pub fn evict_key(&mut self, key: &[u8], volatile: bool) -> bool {
        match self.keys.get(key) {
            Some(e) if !volatile || e.deadline() != 0 => {
                self.keys.remove(key);
                true
            }
            _ => false,
        }
    }

    /// `allkeys-random` / `volatile-random`: evict one key at random; which.
    pub fn evict_random(&mut self, volatile: bool) -> Option<Vec<u8>> {
        let n = self.evictable(volatile);
        if n == 0 {
            return None;
        }
        let j = (fast_random() % n as u64) as usize;
        let i = if volatile {
            self.keys.volatile_at(j)?
        } else {
            j
        };
        let (k, _) = self.keys.remove_at(i);
        Some(k.to_vec())
    }

    fn members_of(&self, keys: &[Vec<u8>], now: i64) -> Vec<Option<MemSet<'_, A>>> {
        keys.iter().map(|k| self.peek_set(k, now)).collect()
    }

    /// `SUNION`'s result set, built as Redis builds it.
    fn union(&mut self, keys: &[Vec<u8>]) -> SetBuilder {
        self.touch_all(keys);
        encoding::union(&self.members_of(keys, now()), Limits::current())
    }

    /// `SINTER`: the members and the encoding of the set it walked.
    fn inter(&mut self, keys: &[Vec<u8>]) -> (Vec<Vec<u8>>, Enc) {
        self.touch_all(keys);
        let sets = self.members_of(keys, now());
        let walked = sets
            .iter()
            .map(|s| s.as_ref().map(|s| (s.size(), s.enc)))
            .collect::<Option<Vec<_>>>()
            .and_then(|s| {
                let order = encoding::inter_order(&s.iter().map(|x| x.0).collect::<Vec<_>>());
                order.first().map(|&i| s[i].1)
            })
            .unwrap_or(Enc::Listpack);
        (vecs(encoding::inter(&sets).into_iter()), walked)
    }

    fn diff(&mut self, keys: &[Vec<u8>]) -> SetBuilder {
        self.touch_all(keys);
        let same = keys.len() > 1 && keys[1..].contains(&keys[0]);
        encoding::diff(&self.members_of(keys, now()), same, Limits::current())
    }

    /// Replace `dst` with a set of `members`, in their order and `enc`; its
    /// size.
    fn store_set(&mut self, dst: &[u8], members: Vec<Vec<u8>>, enc: Enc) -> i64 {
        // Looked up for writing, as `setKey` does: a destination that was
        // there keeps its (updated) access field.
        if self.entry(dst).is_some() && members.is_empty() {
            self.keys.remove(dst);
        }
        if members.is_empty() {
            return 0;
        }
        let mut s = Dict::new_in(self.seed, self.keys.alloc());
        for m in &members {
            s.insert(m, ());
        }
        let n = s.len() as i64;
        self.put(dst, Value::Set(s, enc), 0);
        n
    }

    /// Entries of a sorted set over a rank window, high end first under `rev`,
    /// with `LIMIT offset count` applied.
    fn zwindow(
        z: &ZSet<A>,
        start: usize,
        end: usize,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<(Vec<u8>, f64)> {
        let (offset, count) = limit.unwrap_or((0, -1));
        if end <= start || offset < 0 {
            return Vec::new();
        }
        let take = if count < 0 {
            usize::MAX
        } else {
            count as usize
        };
        let window = end - start;
        let items: Box<dyn Iterator<Item = (&[u8], f64)>> = if rev {
            Box::new(z.iter_rev_from(end).take(window))
        } else {
            Box::new(z.iter_from(start).take(window))
        };
        items
            .skip(offset as usize)
            .take(take)
            .map(|(m, s)| (m.to_vec(), s))
            .collect()
    }

    fn zremove(&mut self, key: &[u8], range: impl FnOnce(&ZSet<A>) -> (usize, usize)) -> i64 {
        let Some(z) = self.zset(key) else { return 0 };
        let (start, end) = range(z);
        let n = if end > start {
            z.remove_range(start, end)
        } else {
            0
        };
        self.tidy(key);
        n as i64
    }

    fn list_index(len: usize, index: i64) -> Option<usize> {
        let i = if index < 0 { index + len as i64 } else { index };
        (0..len as i64).contains(&i).then_some(i as usize)
    }
}

impl<A: Allocator + Clone> Store for Db<A> {
    // ── keys ──

    fn del(&mut self, keys: &[Vec<u8>]) -> Out<i64> {
        let mut n = 0;
        for k in keys {
            if self.entry(k).is_some() {
                self.keys.remove(k);
                n += 1;
            }
        }
        Ok(n)
    }

    // EXISTS, TYPE and the TTL family look up with `LOOKUP_NOTOUCH`.
    fn exists(&mut self, keys: &[Vec<u8>]) -> Out<i64> {
        Ok(keys.iter().filter(|k| self.lookup(k).is_some()).count() as i64)
    }

    fn touch(&mut self, keys: &[Vec<u8>]) -> Out<i64> {
        Ok(keys.iter().filter(|k| self.entry(k).is_some()).count() as i64)
    }

    fn kind(&mut self, key: &[u8]) -> Out<Option<KeyKind>> {
        Ok(self.lookup(key).map(|e| e.value.kind()))
    }

    fn kinds(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Option<KeyKind>>> {
        Ok(keys
            .iter()
            .map(|k| self.lookup(k).map(|e| e.value.kind()))
            .collect())
    }

    fn deadline(&mut self, key: &[u8]) -> Out<Option<Option<i64>>> {
        Ok(self.lookup(key).map(|e| held_deadline(e)))
    }

    fn access(&mut self, key: &[u8]) -> Out<Option<(u32, Option<i64>)>> {
        Ok(self.access_of(key))
    }

    fn expire(&mut self, key: &[u8], deadline_us: i64, cond: ExpireCond) -> Out<bool> {
        let Some(e) = self.entry(key) else {
            return Ok(false);
        };
        if !cond.allows(held_deadline(e), deadline_us) {
            return Ok(false);
        }
        // 0 means no deadline, so the earliest one held is 1.
        self.keys.set_deadline(key, deadline_us.max(1));
        Ok(true)
    }

    fn persist(&mut self, key: &[u8]) -> Out<bool> {
        Ok(match self.entry(key) {
            Some(e) if e.deadline() != 0 => {
                self.keys.set_deadline(key, 0);
                true
            }
            _ => false,
        })
    }

    fn rename(&mut self, key: &[u8], newkey: &[u8]) -> Out<bool> {
        if self.entry(key).is_none() {
            return Ok(false);
        }
        if key != newkey
            && let Some(e) = self.keys.remove(key)
        {
            self.keys.insert(newkey, e);
        }
        Ok(true)
    }

    fn keys(&mut self, pattern: Option<&[u8]>) -> Out<Vec<Vec<u8>>> {
        let now = now();
        Ok(self
            .keys
            .iter()
            .filter(|(k, e)| {
                !e.expired(now) && pattern.is_none_or(|p| crate::pubsub::glob_match(p, k))
            })
            .map(|(k, _)| k.to_vec())
            .collect())
    }

    /// A key past its deadline is passed over, not removed: removing it would
    /// move another key, and the sweep or the next access removes it anyway.
    fn scan_keys(&mut self, cursor: u64, count: usize) -> Out<(u64, Vec<Vec<u8>>)> {
        let now = now();
        let mut out = Vec::new();
        let next = self.keys.scan(cursor, count, |k, e| {
            if !e.expired(now) {
                out.push(k.to_vec());
            }
        });
        Ok((next, out))
    }

    fn dbsize(&mut self) -> Out<i64> {
        let now = now();
        Ok(self.keys.values().filter(|e| !e.expired(now)).count() as i64)
    }

    fn keyspace(&mut self) -> Out<(i64, i64)> {
        let now = now();
        let live = self.keys.values().filter(|e| !e.expired(now));
        let (keys, volatile) =
            live.fold((0, 0), |(k, v), e| (k + 1, v + (e.deadline() != 0) as i64));
        Ok((keys, volatile))
    }

    fn random_key(&mut self) -> Out<Option<Vec<u8>>> {
        let now = now();
        // A few draws find a live key unless most have expired unswept; then
        // reap what the draws hit and fall back to looking.
        for _ in 0..16 {
            let n = self.keys.len();
            if n == 0 {
                return Ok(None);
            }
            let i = (fast_random() % n as u64) as usize;
            match self.keys.at(i) {
                Some((k, e)) if !e.expired(now) => return Ok(Some(k.to_vec())),
                _ => {
                    let (k, _) = self.keys.remove_at(i);
                    REAPED.with_borrow_mut(|r| r.push(k.to_vec()));
                }
            }
        }
        Ok(self
            .keys
            .iter()
            .find(|(_, e)| !e.expired(now))
            .map(|(k, _)| k.to_vec()))
    }

    fn flushdb(&mut self) -> Out<()> {
        self.keys.clear();
        Ok(())
    }

    // ── strings ──

    fn get(&mut self, key: &[u8]) -> Out<Option<Vec<u8>>> {
        Ok(self.string(key).map(|v| v.to_vec()))
    }

    fn set(&mut self, key: &[u8], value: &[u8], a: &SetArgs) -> Out<SetDone> {
        let now = now();
        let (exists, held_deadline, old) = match self.entry(key) {
            None => (false, 0, None),
            Some(e) => {
                let old = match (&e.value, a.get) {
                    (Value::Str(v), true) => Some(v.to_vec()),
                    _ => None,
                };
                (true, e.deadline(), old)
            }
        };
        if (a.nx && exists) || (a.xx && !exists) {
            return Ok(SetDone {
                written: false,
                old,
            });
        }
        let deadline = if a.keepttl {
            held_deadline
        } else {
            let base = if a.ex_abs { 0 } else { now };
            a.ex_ms
                .map_or(0, |ms| base.saturating_add(ms.saturating_mul(1000)).max(1))
        };
        self.put_str(key, value, deadline);
        Ok(SetDone { written: true, old })
    }

    fn setex(&mut self, key: &[u8], value: &[u8], ttl_ms: i64) -> Out<()> {
        self.entry(key);
        let deadline = now().saturating_add(ttl_ms.saturating_mul(1000)).max(1);
        self.put_str(key, value, deadline);
        Ok(())
    }

    fn setnx(&mut self, key: &[u8], value: &[u8]) -> Out<bool> {
        if self.entry(key).is_some() {
            return Ok(false);
        }
        self.put_str(key, value, 0);
        Ok(true)
    }

    fn getset(&mut self, key: &[u8], value: &[u8]) -> Out<Option<Vec<u8>>> {
        let old = self.string(key).map(|v| v.to_vec());
        self.put_str(key, value, 0);
        Ok(old)
    }

    fn getdel(&mut self, key: &[u8]) -> Out<Option<Vec<u8>>> {
        let old = self.string(key).map(|v| v.to_vec());
        if old.is_some() {
            self.keys.remove(key);
        }
        Ok(old)
    }

    fn getex(&mut self, key: &[u8], expiry: &GetExExpiry) -> Out<Option<Vec<u8>>> {
        let Some(v) = self.string(key).map(|v| v.to_vec()) else {
            return Ok(None);
        };
        match *expiry {
            GetExExpiry::Keep => {}
            GetExExpiry::Clear => self.set_deadline(key, 0),
            // A deadline already past deletes the key, as EXPIRE's does.
            GetExExpiry::At(us) if us <= now() => self.clear(key),
            GetExExpiry::At(us) => self.set_deadline(key, us.max(1)),
        }
        Ok(Some(v))
    }

    fn mget(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>> {
        Ok(keys
            .iter()
            .map(|k| self.string(k).map(|v| v.to_vec()))
            .collect())
    }

    fn mset(&mut self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<()> {
        for (k, v) in pairs {
            self.entry(k);
            self.put_str(k, v, 0);
        }
        Ok(())
    }

    fn msetnx(&mut self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<bool> {
        if pairs.iter().any(|(k, _)| self.entry(k).is_some()) {
            return Ok(false);
        }
        self.mset(pairs)?;
        Ok(true)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> Out<i64> {
        let v = self.string_mut(key)?;
        if v.len() + value.len() > MAX_STRING_LEN {
            return Err(too_large());
        }
        v.extend_from_slice(value);
        Ok(v.len() as i64)
    }

    fn strlen(&mut self, key: &[u8]) -> Out<i64> {
        Ok(self.string(key).map_or(0, |v| v.len() as i64))
    }

    fn setrange(&mut self, key: &[u8], offset: i64, value: &[u8]) -> Out<i64> {
        let off = offset as usize;
        let end = off + value.len();
        let v = self.string_mut(key)?;
        if v.len() < end {
            v.resize(end, 0);
        }
        v[off..end].copy_from_slice(value);
        Ok(v.len() as i64)
    }

    fn incrby(&mut self, key: &[u8], delta: i64) -> Out<Num<i64>> {
        let existed = self.string(key).is_some();
        let v = self.string_mut(key)?;
        let current = if existed {
            parse_stored_int(v)
        } else {
            Some(0)
        };
        let Some(current) = current else {
            return Ok(Num::NotNumber);
        };
        let Some(n) = current.checked_add(delta) else {
            return Ok(Num::Overflow);
        };
        v.clear();
        v.extend_from_slice(n.to_string().as_bytes());
        Ok(Num::Value(n))
    }

    fn incrbyfloat(&mut self, key: &[u8], delta: LongDouble) -> Out<Num<String>> {
        let existed = self.string(key).is_some();
        let v = self.string_mut(key)?;
        let text = match increment(existed.then_some(&v[..]), delta) {
            Num::Value(text) => text,
            refused => {
                if !existed {
                    self.keys.remove(key);
                }
                return Ok(refused);
            }
        };
        v.clear();
        v.extend_from_slice(text.as_bytes());
        Ok(Num::Value(text))
    }

    fn setbit(&mut self, key: &[u8], offset: i64, on: bool) -> Out<bool> {
        let v = self.string_mut(key)?;
        let byte = (offset / 8) as usize;
        if v.len() <= byte {
            v.resize(byte + 1, 0);
        }
        let mask = 0x80u8 >> (offset % 8);
        let was = v[byte] & mask != 0;
        if on {
            v[byte] |= mask;
        } else {
            v[byte] &= !mask;
        }
        Ok(was)
    }

    // ── hashes ──

    fn hget(&mut self, key: &[u8], field: &[u8]) -> Out<Option<Vec<u8>>> {
        Ok(self
            .hash(key)
            .and_then(|h| h.get(field))
            .map(|v| v.to_vec()))
    }

    fn hmget(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>> {
        let h = self.hash(key);
        Ok(fields
            .iter()
            .map(|f| h.as_ref().and_then(|h| h.get(f)).map(|v| v.to_vec()))
            .collect())
    }

    fn hgetall(&mut self, key: &[u8]) -> Out<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.hash(key).map_or_else(Vec::new, |h| {
            h.iter().map(|(f, v)| (f.to_vec(), v.to_vec())).collect()
        }))
    }

    /// A listpack comes back whole, cursor 0, as Redis scans one.
    fn hscan(
        &mut self,
        key: &[u8],
        cursor: u64,
        count: usize,
    ) -> Out<crate::cursor::Page<Vec<u8>>> {
        let mut out = Vec::new();
        let next = match self.hash_enc(key) {
            None => 0,
            Some((h, enc)) if enc.is_packed() => {
                out.extend(h.iter().map(|(f, v)| (f.to_vec(), v.to_vec())));
                0
            }
            Some((h, _)) => h.scan(cursor, count, |f, v| out.push((f.to_vec(), v.to_vec()))),
        };
        Ok((next, out))
    }

    fn hkeys(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>> {
        Ok(self.hash(key).map_or_else(Vec::new, |h| vecs(h.keys())))
    }

    fn hvals(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>> {
        Ok(self
            .hash(key)
            .map_or_else(Vec::new, |h| h.values().map(|v| v.to_vec()).collect()))
    }

    fn hlen(&mut self, key: &[u8]) -> Out<i64> {
        Ok(self.hash(key).map_or(0, |h| h.len() as i64))
    }

    fn hexists(&mut self, key: &[u8], field: &[u8]) -> Out<bool> {
        Ok(self.hash(key).is_some_and(|h| h.contains(field)))
    }

    fn hset(&mut self, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)], named: usize) -> Out<i64> {
        let l = Limits::current();
        let a = self.keys.alloc();
        let (h, enc, _) = self.hash_mut(key, || Enc::Listpack)?;
        let lens = pairs.iter().flat_map(|(f, v)| [f.len(), v.len()]);
        *enc = encoding::hash_presize(*enc, named, lens, &l);
        let added = pairs
            .iter()
            .filter(|(f, v)| h.insert(f, bytes_in(v, a.clone())).is_none())
            .count();
        *enc = encoding::hash_grown(*enc, h.len(), &l);
        Ok(added as i64)
    }

    fn hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Out<bool> {
        let l = Limits::current();
        let a = self.keys.alloc();
        let (h, enc, _) = self.hash_mut(key, || Enc::Listpack)?;
        if h.contains(field) {
            return Ok(false);
        }
        *enc = encoding::hash_presize(*enc, 1, [field.len(), value.len()], &l);
        h.insert(field, bytes_in(value, a));
        *enc = encoding::hash_grown(*enc, h.len(), &l);
        Ok(true)
    }

    fn hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<i64> {
        let Some((h, enc)) = self.hash_enc(key) else {
            return Ok(0);
        };
        let enc = *enc;
        let n = fields
            .iter()
            .filter(|f| remove_in(h, enc, f).is_some())
            .count();
        self.tidy(key);
        Ok(n as i64)
    }

    fn hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> Out<Num<i64>> {
        let l = Limits::current();
        let a = self.keys.alloc();
        let (h, enc, _) = self.hash_mut(key, || Enc::Listpack)?;
        let (v, _) = h.get_or_insert_with(field, || bytes_in(b"0", a));
        let Some(current) = parse_stored_int(v) else {
            return Ok(Num::NotNumber);
        };
        let Some(n) = current.checked_add(delta) else {
            return Ok(Num::Overflow);
        };
        let text = n.to_string();
        v.clear();
        v.extend_from_slice(text.as_bytes());
        // `hashTypeSet`: the field and the value it now holds are measured.
        *enc = encoding::hash_presize(*enc, 0, [field.len(), text.len()], &l);
        *enc = encoding::hash_grown(*enc, h.len(), &l);
        Ok(Num::Value(n))
    }

    fn hincrbyfloat(&mut self, key: &[u8], field: &[u8], delta: LongDouble) -> Out<Num<String>> {
        let l = Limits::current();
        let a = self.keys.alloc();
        let (h, enc, _) = self.hash_mut(key, || Enc::Listpack)?;
        let text = match increment(h.get(field).map(|v| &v[..]), delta) {
            Num::Value(text) => text,
            refused => {
                self.tidy(key);
                return Ok(refused);
            }
        };
        h.insert(field, bytes_in(text.as_bytes(), a));
        *enc = encoding::hash_presize(*enc, 0, [field.len(), text.len()], &l);
        *enc = encoding::hash_grown(*enc, h.len(), &l);
        Ok(Num::Value(text))
    }

    fn collection_encoding(&mut self, key: &[u8]) -> Out<Option<Enc>> {
        Ok(self.encoding_of(key))
    }

    // ── sets ──

    fn sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let Some(first) = members.first() else {
            return Ok(0);
        };
        let l = Limits::current();
        let (s, enc, created) =
            self.set_mut(key, || encoding::set_create(first, members.len(), &l))?;
        if !created {
            *enc = encoding::set_presize(*enc, members.len(), &l);
        }
        let mut added = 0;
        for m in members {
            if !s.contains(m) {
                set_insert(s, enc, m, &l);
                added += 1;
            }
        }
        Ok(added)
    }

    fn srem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let Some((s, enc)) = self.set_enc(key) else {
            return Ok(0);
        };
        let enc = *enc;
        let n = members
            .iter()
            .filter(|m| remove_in(s, enc, m).is_some())
            .count();
        self.tidy(key);
        Ok(n as i64)
    }

    fn sismember(&mut self, key: &[u8], member: &[u8]) -> Out<bool> {
        Ok(self.set(key).is_some_and(|s| s.contains(member)))
    }

    fn smismember(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<bool>> {
        let s = self.set(key);
        Ok(members
            .iter()
            .map(|m| s.as_ref().is_some_and(|s| s.contains(m)))
            .collect())
    }

    fn smembers(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>> {
        Ok(self
            .set_enc(key)
            .map_or_else(Vec::new, |(s, enc)| vecs(set_order(s, *enc).into_iter())))
    }

    /// An intset or a listpack comes back whole, cursor 0, as Redis scans one.
    fn sscan(&mut self, key: &[u8], cursor: u64, count: usize) -> Out<(u64, Vec<Vec<u8>>)> {
        let mut out = Vec::new();
        let next = match self.set_enc(key) {
            None => 0,
            Some((s, enc)) if enc.is_packed() => {
                out = vecs(set_order(s, *enc).into_iter());
                0
            }
            Some((s, _)) => s.scan(cursor, count, |m, _| out.push(m.to_vec())),
        };
        Ok((next, out))
    }

    fn scard(&mut self, key: &[u8]) -> Out<i64> {
        Ok(self.set(key).map_or(0, |s| s.len() as i64))
    }

    /// `spopWithCountCommand`'s cases. A count covering the set returns it
    /// as `SUNION` would and deletes the key. Otherwise the members popped
    /// are random: a listpack gives them in its order, and an intset gives a
    /// large share in its sorted order. Popping most of a hash table leaves a
    /// set Redis rebuilds from what remains, which can be a listpack again.
    fn spop(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>> {
        let l = Limits::current();
        let n = n.max(0) as usize;
        let Some((s, enc)) = self.set_enc(key) else {
            return Ok(Vec::new());
        };
        if n == 0 {
            return Ok(Vec::new());
        }
        let size = s.len();
        if n >= size {
            let whole = MemSet { d: s, enc: *enc };
            let (members, _) = encoding::union(&[Some(whole)], l).finish();
            self.keys.remove(key);
            return Ok(members);
        }
        // CASE 3: the members to keep are the few.
        let most = (size - n) * 5 <= n;
        let mut picks = random_picks(size, n as i64);
        if *enc == Enc::Listpack || (*enc == Enc::Intset && most) {
            picks.sort_unstable();
        }
        let mut out: Vec<Vec<u8>> = picks
            .iter()
            .filter_map(|&i| s.at(i).map(|(m, _)| m.to_vec()))
            .collect();
        if *enc == Enc::Intset && most {
            encoding::sort_ints(&mut out, |m| m);
        }
        if *enc == Enc::Listpack {
            // In place, from the back, so the positions still to go hold.
            for &i in picks.iter().rev() {
                s.remove_at_ordered(i);
            }
        } else {
            for m in &out {
                s.remove(m);
            }
        }
        if *enc == Enc::Hashtable && most {
            let mut rebuilt = SetBuilder::new(Enc::Listpack, l);
            for m in s.keys() {
                rebuilt.add(m);
            }
            *enc = rebuilt.enc();
        }
        Ok(out)
    }

    /// `srandmemberWithCountCommand`: a count covering the set returns all
    /// of it in its order; a listpack gives a random choice in its order;
    /// otherwise the choice comes in random order. A negative count draws
    /// with repeats.
    fn srandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>> {
        let Some((s, enc)) = self.set_enc(key) else {
            return Ok(Vec::new());
        };
        if n > 1 && n as usize >= s.len() {
            return Ok(vecs(set_order(s, *enc).into_iter()));
        }
        let mut picks = random_picks(s.len(), n);
        if n > 1 && *enc == Enc::Listpack {
            picks.sort_unstable();
        }
        Ok(picks
            .into_iter()
            .filter_map(|i| s.at(i).map(|(m, _)| m.to_vec()))
            .collect())
    }

    fn smove(&mut self, src: &[u8], dst: &[u8], member: &[u8]) -> Out<(bool, bool)> {
        let removed = self
            .set_enc(src)
            .is_some_and(|(s, enc)| remove_in(s, *enc, member).is_some());
        if !removed {
            return Ok((false, false));
        }
        self.tidy(src);
        let l = Limits::current();
        let (s, enc, _) = self.set_mut(dst, || encoding::set_create(member, 1, &l))?;
        if s.contains(member) {
            return Ok((true, false));
        }
        set_insert(s, enc, member, &l);
        Ok((true, true))
    }

    fn sunion(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        Ok(self.union(keys).finish().0)
    }

    fn sinter(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        Ok(self.inter(keys).0)
    }

    fn sdiff(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        Ok(self.diff(keys).finish().0)
    }

    fn sunionstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let (m, enc) = self.union(keys).finish();
        Ok(self.store_set(dst, m, enc))
    }

    fn sinterstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let (m, walked) = self.inter(keys);
        let built = encoding::inter_store(walked, m.iter().map(Vec::as_slice), Limits::current());
        let (m, enc) = built.finish();
        Ok(self.store_set(dst, m, enc))
    }

    fn sdiffstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let (m, enc) = self.diff(keys).finish();
        Ok(self.store_set(dst, m, enc))
    }

    fn sintercard(&mut self, keys: &[Vec<u8>], limit: Option<i64>) -> Out<i64> {
        let limit = limit.filter(|l| *l > 0).map_or(usize::MAX, |l| l as usize);
        Ok(self.inter(keys).0.len().min(limit) as i64)
    }

    // ── sorted sets ──

    fn zadd(&mut self, key: &[u8], pairs: &[(f64, Vec<u8>)], o: ZaddOpts) -> Out<i64> {
        if o.xx && self.zset(key).is_none() {
            return Ok(0);
        }
        let l = Limits::current();
        let first = pairs.first().map_or(0, |(_, m)| m.len());
        let (z, enc, created) =
            self.zset_mut(key, || encoding::zset_create(pairs.len(), first, &l))?;
        if !created {
            *enc = encoding::zset_presize(*enc, pairs.len(), &l);
        }
        let (mut added, mut changed) = (0, 0);
        for (score, member) in pairs {
            match z.score(member) {
                None if o.xx => {}
                None => {
                    *enc = encoding::zset_add(*enc, z.len(), member.len(), &l);
                    z.insert(member, *score);
                    added += 1;
                    changed += 1;
                }
                Some(_) if o.nx => {}
                Some(old) => {
                    let update = if o.gt {
                        *score > old
                    } else if o.lt {
                        *score < old
                    } else {
                        true
                    };
                    if update && old != *score {
                        z.insert(member, *score);
                        changed += 1;
                    }
                }
            }
        }
        self.tidy(key);
        Ok(if o.ch { changed } else { added })
    }

    fn zadd_incr(&mut self, key: &[u8], member: &[u8], delta: f64, o: ZaddOpts) -> Out<Incr> {
        let old = self.zset(key).and_then(|z| z.score(member));
        let new = match old {
            None if o.xx => return Ok(Incr::Skipped),
            None => delta,
            Some(_) if o.nx => return Ok(Incr::Skipped),
            Some(old) => {
                let new = old + delta;
                if new.is_nan() {
                    return Ok(Incr::NotANumber);
                }
                if (o.gt && new <= old) || (o.lt && new >= old) {
                    return Ok(Incr::Skipped);
                }
                new
            }
        };
        let l = Limits::current();
        let (z, enc, _) = self.zset_mut(key, || encoding::zset_create(1, member.len(), &l))?;
        if old.is_none() {
            *enc = encoding::zset_add(*enc, z.len(), member.len(), &l);
        }
        z.insert(member, new);
        Ok(Incr::Score(new))
    }
    fn zrem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let Some(z) = self.zset(key) else {
            return Ok(0);
        };
        let n = members.iter().filter(|m| z.remove(m).is_some()).count();
        self.tidy(key);
        Ok(n as i64)
    }

    fn zscore(&mut self, key: &[u8], member: &[u8]) -> Out<Option<f64>> {
        Ok(self.zset(key).and_then(|z| z.score(member)))
    }

    fn zmscore(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<Option<f64>>> {
        let z = self.zset(key);
        Ok(members
            .iter()
            .map(|m| z.as_ref().and_then(|z| z.score(m)))
            .collect())
    }

    fn zcard(&mut self, key: &[u8]) -> Out<i64> {
        Ok(self.zset(key).map_or(0, |z| z.len() as i64))
    }

    /// A listpack comes back whole, in score order, cursor 0, as Redis scans
    /// one.
    fn zscan(&mut self, key: &[u8], cursor: u64, count: usize) -> Out<crate::cursor::Page<f64>> {
        let mut out = Vec::new();
        let next = match self.zset_enc(key) {
            None => 0,
            Some((z, enc)) if enc.is_packed() => {
                out.extend(z.iter().map(|(m, s)| (m.to_vec(), s)));
                0
            }
            Some((z, _)) => z.scan(cursor, count, |m, s| out.push((m.to_vec(), s))),
        };
        Ok((next, out))
    }

    fn zcount(&mut self, key: &[u8], min: ScoreBound, max: ScoreBound) -> Out<i64> {
        Ok(self.zset(key).map_or(0, |z| {
            z.score_end(&max).saturating_sub(z.score_start(&min)) as i64
        }))
    }

    fn zlexcount(&mut self, key: &[u8], min: &LexBound, max: &LexBound) -> Out<i64> {
        Ok(self.zset(key).map_or(0, |z| {
            z.lex_end(max).saturating_sub(z.lex_start(min)) as i64
        }))
    }

    fn zrank(&mut self, key: &[u8], member: &[u8], rev: bool) -> Out<Option<(i64, f64)>> {
        let Some(z) = self.zset(key) else {
            return Ok(None);
        };
        let (Some(r), Some(s)) = (z.rank(member), z.score(member)) else {
            return Ok(None);
        };
        let r = if rev { z.len() - 1 - r } else { r };
        Ok(Some((r as i64, s)))
    }

    fn zrange_index(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Out<Vec<(Vec<u8>, f64)>> {
        let Some(z) = self.zset(key) else {
            return Ok(Vec::new());
        };
        let Some((s, e)) = range_bounds(start, stop, z.len()) else {
            return Ok(Vec::new());
        };
        // Under REV the indexes count from the high end.
        let (lo, hi) = if rev {
            (z.len() - 1 - e, z.len() - s)
        } else {
            (s, e + 1)
        };
        Ok(Self::zwindow(z, lo, hi, rev, None))
    }

    fn zrange_score(
        &mut self,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Out<Vec<(Vec<u8>, f64)>> {
        let Some(z) = self.zset(key) else {
            return Ok(Vec::new());
        };
        Ok(Self::zwindow(
            z,
            z.score_start(&min),
            z.score_end(&max),
            rev,
            limit,
        ))
    }

    fn zrange_lex(
        &mut self,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Out<Vec<Vec<u8>>> {
        let Some(z) = self.zset(key) else {
            return Ok(Vec::new());
        };
        let items = Self::zwindow(z, z.lex_start(min), z.lex_end(max), rev, limit);
        Ok(items.into_iter().map(|(m, _)| m).collect())
    }

    fn zpop(&mut self, key: &[u8], n: i64, min: bool) -> Out<Vec<(Vec<u8>, f64)>> {
        let Some(z) = self.zset(key) else {
            return Ok(Vec::new());
        };
        let n = (n.max(0) as usize).min(z.len());
        let picked = if min {
            Self::zwindow(z, 0, n, false, None)
        } else {
            Self::zwindow(z, z.len() - n, z.len(), true, None)
        };
        for (m, _) in &picked {
            z.remove(m);
        }
        self.tidy(key);
        Ok(picked)
    }

    /// `zrandmemberWithCountCommand`: a count covering the sorted set
    /// returns all of it from the highest score down, as Redis's iterator
    /// walks it, and a listpack gives a random choice in its order; otherwise
    /// the choice comes in random order.
    fn zrandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<(Vec<u8>, f64)>> {
        let Some((z, enc)) = self.zset_enc(key) else {
            return Ok(Vec::new());
        };
        if n > 1 && n as usize >= z.len() {
            let mut all: Vec<(Vec<u8>, f64)> = z.iter().map(|(m, s)| (m.to_vec(), s)).collect();
            all.reverse();
            return Ok(all);
        }
        if n > 1 && *enc == Enc::Listpack {
            let mut ranks = random_picks(z.len(), n);
            ranks.sort_unstable();
            let mut want = ranks.into_iter().peekable();
            let mut out = Vec::new();
            for (rank, (m, s)) in z.iter().enumerate() {
                if want.peek() == Some(&rank) {
                    want.next();
                    out.push((m.to_vec(), s));
                }
            }
            return Ok(out);
        }
        Ok(random_picks(z.len(), n)
            .into_iter()
            .filter_map(|i| z.nth_unordered(i).map(|(m, s)| (m.to_vec(), s)))
            .collect())
    }

    fn zremrange_rank(&mut self, key: &[u8], start: i64, stop: i64) -> Out<i64> {
        Ok(self.zremove(key, |z| {
            range_bounds(start, stop, z.len()).map_or((0, 0), |(s, e)| (s, e + 1))
        }))
    }

    fn zremrange_score(&mut self, key: &[u8], min: ScoreBound, max: ScoreBound) -> Out<i64> {
        Ok(self.zremove(key, |z| (z.score_start(&min), z.score_end(&max))))
    }

    fn zremrange_lex(&mut self, key: &[u8], min: &LexBound, max: &LexBound) -> Out<i64> {
        Ok(self.zremove(key, |z| (z.lex_start(min), z.lex_end(max))))
    }

    // ── lists ──

    fn lpush(
        &mut self,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
        only_existing: bool,
    ) -> Out<i64> {
        if only_existing && self.list(key).is_none() {
            return Ok(0);
        }
        let a = self.keys.alloc();
        let l = self.list_mut(key)?;
        for v in values {
            let b = bytes_in(v, a.clone());
            if left {
                l.push_front(b)
            } else {
                l.push_back(b)
            }
        }
        Ok(l.len() as i64)
    }

    fn lpop(&mut self, key: &[u8], n: i64, left: bool) -> Out<Vec<Vec<u8>>> {
        let Some(l) = self.list(key) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for _ in 0..n.max(0) {
            match if left { l.pop_front() } else { l.pop_back() } {
                Some(v) => out.push(v.to_vec()),
                None => break,
            }
        }
        self.tidy(key);
        Ok(out)
    }

    fn llen(&mut self, key: &[u8]) -> Out<i64> {
        Ok(self.list(key).map_or(0, |l| l.len() as i64))
    }

    fn lrange(&mut self, key: &[u8], start: i64, stop: i64) -> Out<Vec<Vec<u8>>> {
        let Some(l) = self.list(key) else {
            return Ok(Vec::new());
        };
        let Some((s, e)) = range_bounds(start, stop, l.len()) else {
            return Ok(Vec::new());
        };
        Ok(l.iter()
            .skip(s)
            .take(e - s + 1)
            .map(|v| v.to_vec())
            .collect())
    }

    fn lindex(&mut self, key: &[u8], index: i64) -> Out<Option<Vec<u8>>> {
        let Some(l) = self.list(key) else {
            return Ok(None);
        };
        Ok(Self::list_index(l.len(), index)
            .and_then(|i| l.get(i))
            .map(|v| v.to_vec()))
    }

    fn lset(&mut self, key: &[u8], index: i64, value: &[u8]) -> Out<LSet> {
        let a = self.keys.alloc();
        let Some(l) = self.list(key) else {
            return Ok(LSet::NoKey);
        };
        let Some(slot) = Self::list_index(l.len(), index).and_then(|i| l.get_mut(i)) else {
            return Ok(LSet::OutOfRange);
        };
        *slot = bytes_in(value, a);
        Ok(LSet::Done)
    }

    fn linsert(&mut self, key: &[u8], before: bool, pivot: &[u8], value: &[u8]) -> Out<i64> {
        let a = self.keys.alloc();
        let Some(l) = self.list(key) else {
            return Ok(0);
        };
        let Some(at) = l.iter().position(|v| v.as_slice() == pivot) else {
            return Ok(-1);
        };
        l.insert(if before { at } else { at + 1 }, bytes_in(value, a));
        Ok(l.len() as i64)
    }

    fn lrem(&mut self, key: &[u8], count: i64, value: &[u8]) -> Out<i64> {
        let Some(l) = self.list(key) else {
            return Ok(0);
        };
        let matches: Vec<usize> = l
            .iter()
            .enumerate()
            .filter(|(_, v)| v.as_slice() == value)
            .map(|(i, _)| i)
            .collect();
        let limit = if count == 0 {
            matches.len()
        } else {
            (count.unsigned_abs() as usize).min(matches.len())
        };
        let doomed: std::collections::HashSet<usize> = if count < 0 {
            matches.iter().rev().take(limit).copied().collect()
        } else {
            matches.iter().take(limit).copied().collect()
        };
        l.retain_indexed(|i, _| !doomed.contains(&i));
        self.tidy(key);
        Ok(doomed.len() as i64)
    }

    fn ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> Out<()> {
        let Some(l) = self.list(key) else {
            return Ok(());
        };
        match range_bounds(start, stop, l.len()) {
            Some((s, e)) => l.trim(s, e),
            None => {
                self.keys.remove(key);
            }
        }
        Ok(())
    }

    fn lmove(
        &mut self,
        src: &[u8],
        dst: &[u8],
        src_left: bool,
        dst_left: bool,
    ) -> Out<Option<Vec<u8>>> {
        let Some(l) = self.list(src) else {
            return Ok(None);
        };
        let Some(v) = (if src_left {
            l.pop_front()
        } else {
            l.pop_back()
        }) else {
            return Ok(None);
        };
        let out = v.to_vec();
        // Pushed before the source is tidied: onto itself, a one-element list
        // must come back round rather than be dropped and lose its TTL.
        let d = self.list_mut(dst)?;
        if dst_left {
            d.push_front(v)
        } else {
            d.push_back(v)
        }
        self.tidy(src);
        Ok(Some(out))
    }

    fn lpos(
        &mut self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<i64>,
        maxlen: i64,
    ) -> Out<Vec<i64>> {
        let Some(l) = self.list(key) else {
            return Ok(Vec::new());
        };
        let len = l.len();
        let want = match count {
            None => 1,
            Some(0) => usize::MAX,
            Some(n) => n as usize,
        };
        let scan = if maxlen == 0 {
            len
        } else {
            (maxlen as usize).min(len)
        };
        let order: Box<dyn Iterator<Item = usize>> = if rank > 0 {
            Box::new(0..scan)
        } else {
            Box::new((len - scan..len).rev())
        };
        let mut skip = rank.unsigned_abs().saturating_sub(1);
        let mut out = Vec::new();
        for i in order {
            if l.get(i).is_some_and(|v| v.as_slice() == element) {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                out.push(i as i64);
                if out.len() >= want {
                    break;
                }
            }
        }
        Ok(out)
    }
}

impl<A: Allocator + Clone> crate::stream::StreamStore for Db<A> {
    fn load(&mut self, key: &[u8]) -> Result<Option<crate::stream::Stream>, Response> {
        match self.entry(key).map(|e| &e.value) {
            None => Ok(None),
            Some(Value::Stream(v)) => crate::stream::Stream::decode(v)
                .map(Some)
                .ok_or_else(|| Response::Error("ERR corrupt stream value".into())),
            Some(_) => Err(crate::stream::not_stream()),
        }
    }

    /// Replace the stream's value, keeping the key's deadline.
    fn store(&mut self, key: &[u8], s: &crate::stream::Stream) -> Result<(), Response> {
        let deadline = self.entry(key).map_or(0, |e| e.deadline());
        let v = self.bytes(&s.encode());
        self.put(key, Value::Stream(v), deadline);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Command;
    use allocator_api2::alloc::Global;

    fn db() -> Db<Global> {
        Db::new_in(Seed(5, 6), Global)
    }

    /// Run a command line through the shared command code against `db`.
    fn run(db: &mut Db<Global>, line: &str) -> Response {
        let parts: Vec<Vec<u8>> = line.split(' ').map(|p| p.as_bytes().to_vec()).collect();
        let cmd = Command::parse(parts).unwrap_or_else(|e| panic!("{line}: {e}"));
        crate::store::run(&cmd, db).unwrap_or_else(|| panic!("{line}: not a store command"))
    }

    fn int(r: Response) -> i64 {
        match r {
            Response::Integer(n) => n,
            other => panic!("not an integer: {other:?}"),
        }
    }

    fn bulks(r: Response) -> Vec<String> {
        match r {
            Response::Array(v) => v
                .into_iter()
                .map(|b| String::from_utf8(b.unwrap_or_default()).unwrap())
                .collect(),
            other => panic!("not an array: {other:?}"),
        }
    }

    #[test]
    fn strings_count_and_expire() {
        let mut d = db();
        assert_eq!(int(run(&mut d, "INCRBY n 5")), 5);
        assert_eq!(int(run(&mut d, "INCR n")), 6);
        assert!(matches!(run(&mut d, "APPEND n x"), Response::Integer(2)));
        assert!(matches!(run(&mut d, "INCR n"), Response::Error(_)));
        assert!(matches!(run(&mut d, "SET k v EX 100"), Response::Ok));
        assert_eq!(int(run(&mut d, "TTL k")), 100);
        assert!(matches!(run(&mut d, "SET k v2 KEEPTTL"), Response::Ok));
        assert_eq!(int(run(&mut d, "TTL k")), 100);
        assert!(matches!(run(&mut d, "SET k v3 NX"), Response::Null));
        assert_eq!(int(run(&mut d, "PEXPIRE k -1")), 1);
        assert_eq!(int(run(&mut d, "EXISTS k")), 0);
        assert_eq!(int(run(&mut d, "SETRANGE r 3 ab")), 5);
        assert!(matches!(run(&mut d, "GET r"), Response::BulkString(v) if v == b"\0\0\0ab"));
        assert_eq!(int(run(&mut d, "SETBIT b 9 1")), 0);
        assert_eq!(int(run(&mut d, "GETBIT b 9")), 1);
    }

    #[test]
    fn hashes_sets_and_lists() {
        let mut d = db();
        assert_eq!(int(run(&mut d, "HSET h a 1 b 2 a 3")), 2);
        assert!(matches!(run(&mut d, "HGET h a"), Response::BulkString(v) if v == b"3"));
        assert_eq!(int(run(&mut d, "HINCRBY h a 4")), 7);
        assert_eq!(int(run(&mut d, "HDEL h a b")), 2);
        assert_eq!(int(run(&mut d, "EXISTS h")), 0, "an emptied hash is gone");

        assert_eq!(int(run(&mut d, "SADD s a b c")), 3);
        assert_eq!(int(run(&mut d, "SADD t b c d")), 3);
        let mut inter = bulks(run(&mut d, "SINTER s t"));
        inter.sort();
        assert_eq!(inter, ["b", "c"]);
        assert_eq!(int(run(&mut d, "SDIFFSTORE u s t")), 1);
        assert_eq!(bulks(run(&mut d, "SMEMBERS u")), ["a"]);
        assert_eq!(int(run(&mut d, "SMOVE u t a")), 1);
        assert_eq!(int(run(&mut d, "EXISTS u")), 0);

        assert_eq!(int(run(&mut d, "RPUSH l a b c")), 3);
        assert_eq!(int(run(&mut d, "LPUSH l z")), 4);
        assert_eq!(bulks(run(&mut d, "LRANGE l 0 -1")), ["z", "a", "b", "c"]);
        assert_eq!(int(run(&mut d, "LINSERT l BEFORE b x")), 5);
        assert_eq!(int(run(&mut d, "RPUSH l a")), 6);
        assert_eq!(int(run(&mut d, "LREM l -1 a")), 1);
        assert_eq!(
            bulks(run(&mut d, "LRANGE l 0 -1")),
            ["z", "a", "x", "b", "c"]
        );
        assert!(
            matches!(run(&mut d, "LMOVE l l LEFT RIGHT"), Response::BulkString(v) if v == b"z")
        );
        assert_eq!(bulks(run(&mut d, "LRANGE l -2 -1")), ["c", "z"]);
        assert!(matches!(run(&mut d, "LPOS l c"), Response::Integer(3)));
        assert!(matches!(run(&mut d, "LTRIM l 1 2"), Response::Ok));
        assert_eq!(bulks(run(&mut d, "LRANGE l 0 -1")), ["x", "b"]);
    }

    #[test]
    fn sorted_sets_rank_range_and_pop() {
        let mut d = db();
        assert_eq!(int(run(&mut d, "ZADD z 1 a 2 b 3 c 4 d")), 4);
        assert_eq!(int(run(&mut d, "ZADD z GT CH 0 a 5 b")), 1);
        assert_eq!(bulks(run(&mut d, "ZRANGE z 0 -1")), ["a", "c", "d", "b"]);
        assert_eq!(bulks(run(&mut d, "ZRANGE z 0 1 REV")), ["b", "d"]);
        assert_eq!(bulks(run(&mut d, "ZRANGEBYSCORE z (1 4 LIMIT 1 -1")), ["d"]);
        assert_eq!(int(run(&mut d, "ZCOUNT z -inf +inf")), 4);
        assert!(matches!(run(&mut d, "ZRANK z b"), Response::Integer(3)));
        assert!(matches!(run(&mut d, "ZREVRANK z b"), Response::Integer(0)));
        assert_eq!(bulks(run(&mut d, "ZPOPMIN z 2")), ["a", "1", "c", "3"]);
        assert_eq!(int(run(&mut d, "ZREMRANGEBYSCORE z 0 10")), 2);
        assert_eq!(int(run(&mut d, "EXISTS z")), 0);
    }

    fn encoding(d: &mut Db<Global>, key: &str) -> String {
        match run(d, &format!("OBJECT ENCODING {key}")) {
            Response::BulkString(v) => String::from_utf8(v).unwrap(),
            other => panic!("not a bulk string: {other:?}"),
        }
    }

    fn args(prefix: &str, items: impl Iterator<Item = String>) -> String {
        std::iter::once(prefix.to_string())
            .chain(items)
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// What Redis 7.2 answers, observed: a listpack keeps insertion order
    /// through deletes and updates, and once converted stays converted.
    #[test]
    fn hashes_keep_insertion_order_and_their_encoding() {
        let mut d = db();
        run(&mut d, "HSET h c 1 a 2 b 3");
        run(&mut d, "HDEL h a");
        run(&mut d, "HSET h d 4");
        run(&mut d, "HSET h c 9");
        assert_eq!(bulks(run(&mut d, "HKEYS h")), ["c", "b", "d"]);
        assert_eq!(bulks(run(&mut d, "HVALS h")), ["9", "3", "4"]);
        assert_eq!(encoding(&mut d, "h"), "listpack");
        // HSCAN returns a listpack whole, whatever COUNT asks for.
        match run(&mut d, "HSCAN h 0 COUNT 1") {
            Response::ScanResult { cursor, keys } => assert_eq!((cursor, keys.len()), (0, 6)),
            other => panic!("{other:?}"),
        }
        // A field named twice in one HSET stays where it was first named.
        run(&mut d, "HSET n x 1 y 2 x 3");
        assert_eq!(bulks(run(&mut d, "HGETALL n")), ["x", "3", "y", "2"]);

        run(
            &mut d,
            &args("HSET big", (0..513).map(|i| format!("f{i} v"))),
        );
        assert_eq!(encoding(&mut d, "big"), "hashtable");
        run(&mut d, &args("HDEL big", (3..513).map(|i| format!("f{i}"))));
        assert_eq!(encoding(&mut d, "big"), "hashtable");
        run(&mut d, "HSET long a 1");
        run(&mut d, &format!("HSET long b {}", "x".repeat(65)));
        run(&mut d, "HDEL long b");
        assert_eq!(encoding(&mut d, "long"), "hashtable");
        run(&mut d, &format!("HINCRBY inc {} 1", "f".repeat(65)));
        assert_eq!(encoding(&mut d, "inc"), "hashtable");
    }

    #[test]
    fn sets_follow_intset_listpack_and_hashtable_order() {
        let mut d = db();
        run(&mut d, "SADD s 5 -3 100 2");
        assert_eq!(bulks(run(&mut d, "SMEMBERS s")), ["-3", "2", "5", "100"]);
        assert_eq!(encoding(&mut d, "s"), "intset");
        // A string converts it: the integers keep their sorted order.
        run(&mut d, "SADD s x 1");
        assert_eq!(encoding(&mut d, "s"), "listpack");
        assert_eq!(
            bulks(run(&mut d, "SMEMBERS s")),
            ["-3", "2", "5", "100", "x", "1"]
        );
        run(&mut d, "SREM s x 100");
        assert_eq!(bulks(run(&mut d, "SMEMBERS s")), ["-3", "2", "5", "1"]);
        assert_eq!(encoding(&mut d, "s"), "listpack");
        // SUNION builds its own set: integers alone come back sorted.
        assert_eq!(bulks(run(&mut d, "SUNION s")), ["-3", "1", "2", "5"]);
        assert_eq!(int(run(&mut d, "SINTERSTORE t s s")), 4);
        assert_eq!(encoding(&mut d, "t"), "intset");

        run(&mut d, "SADD l c a b d");
        run(&mut d, "SREM l a");
        run(&mut d, "SADD l e");
        assert_eq!(bulks(run(&mut d, "SMEMBERS l")), ["c", "b", "d", "e"]);
        // A whole-set SRANDMEMBER and SPOP come back in order.
        assert_eq!(bulks(run(&mut d, "SRANDMEMBER l 10")), ["c", "b", "d", "e"]);
        let chosen = bulks(run(&mut d, "SRANDMEMBER l 2"));
        let order = ["c", "b", "d", "e"];
        let pos: Vec<usize> = chosen
            .iter()
            .map(|m| order.iter().position(|o| o == m).unwrap())
            .collect();
        assert!(pos[0] < pos[1], "{chosen:?} is not in the set's order");

        for (key, first) in [("i", "1"), ("w", "w")] {
            run(
                &mut d,
                &args(
                    &format!("SADD {key} {first}"),
                    (2..=600).map(|i| i.to_string()),
                ),
            );
            assert_eq!(encoding(&mut d, key), "hashtable");
            run(
                &mut d,
                &args(&format!("SREM {key}"), (4..=600).map(|i| i.to_string())),
            );
            assert_eq!(encoding(&mut d, key), "hashtable");
        }
        // SADD sizes a set by the members named, repeats included.
        run(&mut d, &args("SADD dup", (0..200).map(|_| "a".to_string())));
        assert_eq!(encoding(&mut d, "dup"), "hashtable");
        // Popping most of a hash table leaves a listpack.
        run(&mut d, &args("SADD pop", (0..300).map(|i| format!("m{i}"))));
        assert_eq!(bulks(run(&mut d, "SPOP pop 290")).len(), 290);
        assert_eq!(encoding(&mut d, "pop"), "listpack");
    }

    #[test]
    fn sorted_sets_stay_skiplists_and_scan_whole_as_listpacks() {
        let mut d = db();
        run(&mut d, "ZADD z 2 b 1 a 3 c");
        assert_eq!(encoding(&mut d, "z"), "listpack");
        match run(&mut d, "ZSCAN z 0 COUNT 1") {
            Response::ScanResult { keys, .. } => assert_eq!(
                keys.into_iter()
                    .map(|k| String::from_utf8(k.unwrap()).unwrap())
                    .collect::<Vec<_>>(),
                ["a", "1", "b", "2", "c", "3"]
            ),
            other => panic!("{other:?}"),
        }
        run(
            &mut d,
            &args("ZADD big", (0..129).map(|i| format!("{i} m{i}"))),
        );
        assert_eq!(encoding(&mut d, "big"), "skiplist");
        run(&mut d, "ZREMRANGEBYRANK big 0 100");
        assert_eq!(encoding(&mut d, "big"), "skiplist");
        run(&mut d, &format!("ZADD long 1 a 2 {}", "x".repeat(65)));
        run(&mut d, &format!("ZREM long {}", "x".repeat(65)));
        assert_eq!(encoding(&mut d, "long"), "skiplist");
        // Updating members already there converts nothing.
        run(
            &mut d,
            &args("ZADD up", (0..100).map(|i| format!("{i} m{i}"))),
        );
        run(
            &mut d,
            &args("ZADD up", (0..50).map(|i| format!("{} m{i}", i + 1000))),
        );
        assert_eq!(encoding(&mut d, "up"), "listpack");
    }

    #[test]
    fn a_dump_carries_the_order_and_the_encoding() {
        let mut d = db();
        run(&mut d, "SADD s a 3 1");
        run(&mut d, "SREM s a");
        let (dump, _) = d.dump(b"s").unwrap();
        d.restore(b"t", &dump, 0);
        assert_eq!(bulks(run(&mut d, "SMEMBERS t")), ["3", "1"]);
        assert_eq!(encoding(&mut d, "t"), "listpack");
        run(&mut d, &args("HSET h", (0..600).map(|i| format!("f{i} v"))));
        run(&mut d, &args("HDEL h", (2..600).map(|i| format!("f{i}"))));
        let (dump, _) = d.dump(b"h").unwrap();
        d.restore(b"g", &dump, 0);
        assert_eq!(encoding(&mut d, "g"), "hashtable");
        // RENAME moves the entry, encoding and all.
        run(&mut d, "RENAME g g2");
        assert_eq!(encoding(&mut d, "g2"), "hashtable");
    }

    #[test]
    fn keyspace_rename_and_dump_round_trip() {
        let mut d = db();
        run(&mut d, "RPUSH l a b");
        run(&mut d, "PEXPIRE l 100000");
        assert!(matches!(run(&mut d, "RENAME l m"), Response::Ok));
        assert_eq!(int(run(&mut d, "EXISTS l")), 0);
        assert!(int(run(&mut d, "PTTL m")) > 0);
        let (dump, deadline) = d.dump(b"m").unwrap();
        d.restore(b"n", &dump, deadline);
        assert_eq!(bulks(run(&mut d, "LRANGE n 0 -1")), ["a", "b"]);
        assert_eq!(int(run(&mut d, "DBSIZE")), 2);
        assert_eq!(d.keyspace().unwrap(), (2, 2));
        d.set_deadline(b"n", 1);
        assert_eq!(d.sweep_expired(true), (1, vec![b"n".to_vec()]));
        assert_eq!(d.evict_random(false), Some(b"m".to_vec()));
        assert_eq!(int(run(&mut d, "DBSIZE")), 0);
    }

    const LFU: Tracking = Tracking::Lfu {
        log_factor: 10,
        decay_time: 1,
    };

    fn under(touch: bool, tracking: Tracking) {
        lru::set_access(lru::Access { touch, tracking });
    }

    #[test]
    fn lookups_touch_and_notouch_lookups_do_not() {
        let mut d = db();
        under(true, LFU);
        run(&mut d, "SET k v");
        let field = |d: &mut Db<Global>| d.access_of(b"k").unwrap().0;
        assert_eq!(field(&mut d) & 255, 5, "a new key starts at LFU_INIT_VAL");
        // At 5 any access increments.
        run(&mut d, "GET k");
        assert_eq!(field(&mut d) & 255, 6);
        // EXISTS, TYPE and TTL look without touching.
        for c in ["EXISTS k", "TYPE k", "TTL k", "PTTL k"] {
            run(&mut d, c);
        }
        assert_eq!(field(&mut d) & 255, 6);
        // An overwrite keeps the field its lookup updated.
        let before = field(&mut d) & 255;
        run(&mut d, "SET k w");
        assert!(field(&mut d) & 255 >= before);
        // No touching at all: GET leaves it.
        under(false, LFU);
        let held = field(&mut d);
        run(&mut d, "GET k");
        assert_eq!(field(&mut d), held);
        // A rename carries the field.
        run(&mut d, "RENAME k k2");
        assert_eq!(d.access_of(b"k2").unwrap().0, held);
        under(false, Tracking::Lru);
    }

    #[test]
    fn volatile_sampling_sees_only_keys_with_a_deadline() {
        let mut d = db();
        under(false, Tracking::Lru);
        for i in 0..200 {
            run(&mut d, &format!("SET p{i} v"));
        }
        for i in 0..5 {
            run(&mut d, &format!("SET t{i} v EX 1000"));
        }
        assert_eq!(d.evictable(true), 5);
        let mut out = Vec::new();
        d.sample(true, 50, Score::Deadline, &mut out);
        assert_eq!(out.len(), 5);
        assert!(out.iter().all(|s| s.key.starts_with(b"t")));
        assert!(
            !d.evict_key(b"p1", true),
            "volatile-* never takes a persistent key"
        );
        assert!(d.evict_key(b"t1", true));
        while d.evict_random(true).is_some() {}
        assert_eq!(int(run(&mut d, "DBSIZE")), 200);
        // PERSIST takes a key out of the volatile set, EXPIRE puts it in.
        run(&mut d, "EXPIRE p3 100");
        run(&mut d, "EXPIRE p4 100");
        run(&mut d, "PERSIST p3");
        assert_eq!(d.evict_random(true), Some(b"p4".to_vec()));
        assert_eq!(d.evict_random(true), None);
    }
}
