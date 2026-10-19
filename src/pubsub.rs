//! Pub/sub across every worker: who is subscribed to what, and the messages
//! waiting for each subscriber.
//!
//! The registry lives in the pub/sub heap (`pubheap.rs`), so nothing in it
//! has a fixed size: channel and pattern names, payloads, the number of
//! subscribers and the number of subscriptions are bounded only by that
//! heap, as they are bounded only by memory in Redis. A subscriber that falls
//! behind is disconnected by Redis's own rule, `client-output-buffer-limit
//! pubsub` (32 MB at once, or 8 MB for 60 seconds, by default), counted in
//! the bytes its messages take on the wire.
//!
//! Each subscriber has a slot: its subscriptions and its queue. Publishers
//! copy a message into the queue of every subscriber it reaches, as Redis
//! writes it into each client's output buffer, and wake the owner, which
//! takes messages on its connection thread.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

use crate::pubheap::{PubHeap, catching};

type HVec<T> = allocator_api2::vec::Vec<T, PubHeap>;
type HBox<T> = allocator_api2::boxed::Box<T, PubHeap>;
type Index = hashbrown::HashMap<Key, HVec<u32>, std::hash::RandomState, PubHeap>;

/// A channel name as an index key: hashes and compares as its bytes, so the
/// index can be looked up by a plain `&[u8]`.
#[derive(PartialEq, Eq)]
struct Key(HVec<u8>);

impl std::hash::Hash for Key {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0[..].hash(state);
    }
}

impl std::borrow::Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

/// The longest channel a table route can name; routes are this extension's
/// own and keep fixed slots.
const CHAN_LEN: usize = 256;
pub const MAX_CHANNEL_LEN: usize = CHAN_LEN - 1;

/// Sharded pub/sub (`SSUBSCRIBE`, `SPUBLISH`) is its own namespace in Redis.
/// Its channels share the registry here under this prefix. A leading 0x01
/// marks a channel as internal: kept out of `PUBSUB CHANNELS` and out of
/// every pattern subscription.
pub const SHARD_PREFIX: &[u8] = b"\x01shard:";

/// The byte every internal channel starts with.
pub const INTERNAL_MARK: u8 = 0x01;

/// Whether `channel` is one of this extension's own: sharded, a client's
/// tracking channel, or MONITOR's.
pub fn is_internal(channel: &[u8]) -> bool {
    channel.first() == Some(&INTERNAL_MARK)
}

pub fn shard_name(channel: &[u8]) -> Vec<u8> {
    [SHARD_PREFIX, channel].concat()
}

/// The name a client used, for a channel as the registry stores it.
pub fn shown_name(channel: &[u8]) -> &[u8] {
    channel.strip_prefix(SHARD_PREFIX).unwrap_or(channel)
}

const MAX_ROUTES: usize = 64;
const ROUTE_SCHEMA_LEN: usize = 64;
const ROUTE_TABLE_LEN: usize = 64;

// ─────────────────────────────── the safety model ───────────────────────────
//
// The registry and the routes are each read and written only while holding
// their spin lock. `Locked` and `RoutesLocked` are what holding one looks
// like: the only way to reach the `UnsafeCell` behind it. The one other
// `unsafe` is a subscriber waiting on its own wake word after letting the
// lock go (`wait_for_message`), which is sound because only the subscriber
// itself ever frees its slot.

fn name(bytes: &[u8]) -> HVec<u8> {
    let mut v = HVec::with_capacity_in(bytes.len(), PubHeap);
    v.extend_from_slice(bytes);
    v
}

/// A class of `client-output-buffer-limit`: bytes queued at once that close
/// the connection, bytes that close it when held for `soft_secs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub hard: usize,
    pub soft: usize,
    pub soft_secs: i64,
}

impl Limits {
    /// What `CONFIG` holds now.
    pub fn current() -> Limits {
        parse_limits(&crate::config::get_str("client-output-buffer-limit"))
    }
}

/// Redis's default for the class.
const DEFAULT_LIMITS: Limits = Limits {
    hard: 32 << 20,
    soft: 8 << 20,
    soft_secs: 60,
};

/// The pubsub class out of `normal … replica … pubsub hard soft secs`.
fn parse_limits(setting: &str) -> Limits {
    class_limits(setting, "pubsub").unwrap_or(DEFAULT_LIMITS)
}

/// Bytes Redis's static reply buffer holds before a client's replies count
/// against its output limit (`PROTO_REPLY_CHUNK_BYTES`).
pub const REPLY_CHUNK: usize = 16 * 1024;

/// The normal class: the output limit of every client that is not a
/// subscriber, none by default. Parsed again only after a `CONFIG SET`.
pub fn normal_limits() -> Limits {
    thread_local! {
        static CACHE: std::cell::Cell<Option<(u32, Limits)>> = const { std::cell::Cell::new(None) };
    }
    let generation = crate::server::config_generation();
    CACHE.with(|c| match c.get() {
        Some((g, l)) if g == generation => l,
        _ => {
            let l = class_limits(
                &crate::config::get_str("client-output-buffer-limit"),
                "normal",
            )
            .unwrap_or(Limits {
                hard: 0,
                soft: 0,
                soft_secs: 0,
            });
            c.set(Some((generation, l)));
            l
        }
    })
}

/// How many elements of a reply with repeats (`HRANDFIELD`, `SRANDMEMBER`,
/// `ZRANDMEMBER` with a negative count) to build: `n`, or under a normal
/// output limit, just enough that the reply passes it. Redis stops such a
/// reply part way once the client is past its limit, and closes it.
pub fn cap_repeats(n: u64) -> u64 {
    match normal_limits().hard {
        0 => n,
        // The shortest element on the wire is `$0\r\n\r\n`.
        hard => n.min(((hard + REPLY_CHUNK) / 6 + 1) as u64),
    }
}

fn class_limits(setting: &str, class: &str) -> Option<Limits> {
    let words: Vec<&str> = setting.split_whitespace().collect();
    words
        .windows(4)
        .find(|w| w[0].eq_ignore_ascii_case(class))
        .and_then(|w| {
            Some(Limits {
                hard: memory(w[1])?,
                soft: memory(w[2])?,
                soft_secs: w[3].parse().ok()?,
            })
        })
}

/// A size as Redis writes one: bytes, or with k, kb, m, mb, g, gb (k is
/// 1000, kb 1024).
fn memory(word: &str) -> Option<usize> {
    let w = word.to_ascii_lowercase();
    let digits = w.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit: usize = match &w[digits.len()..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    digits.parse::<usize>().ok()?.checked_mul(unit)
}

/// Bytes a RESP bulk string of `n` bytes takes on the wire.
fn bulk(n: usize) -> usize {
    1 + n.to_string().len() + 2 + n + 2
}

/// Bytes a message frame takes on the wire: `message` or `pmessage`.
fn frame_bytes(channel: &[u8], pattern: &[u8], payload: &[u8]) -> usize {
    if pattern.is_empty() {
        4 + bulk(7) + bulk(channel.len()) + bulk(payload.len())
    } else {
        4 + bulk(8) + bulk(pattern.len()) + bulk(channel.len()) + bulk(payload.len())
    }
}

struct Queued {
    channel: HVec<u8>,
    pattern: HVec<u8>,
    payload: HVec<u8>,
    bytes: usize,
}

/// One subscriber: what it is subscribed to, and what waits for it.
struct Sub {
    channels: HVec<HVec<u8>>,
    patterns: HVec<HVec<u8>>,
    /// Messages from `head` on are undelivered.
    queue: HVec<Queued>,
    head: usize,
    /// Wire bytes of the undelivered messages.
    queued: usize,
    /// When `queued` went past the soft limit, 0 while under it.
    soft_since_ms: i64,
    /// Past an output limit: the connection closes, as Redis closes it.
    overflowed: bool,
    /// Moved on every message queued; the owner sleeps on it.
    seq: AtomicU32,
    /// The client the slot is for, for `CLIENT LIST`'s `omem`.
    owner: u64,
}

impl Sub {
    fn new() -> Sub {
        Sub {
            channels: HVec::new_in(PubHeap),
            patterns: HVec::new_in(PubHeap),
            queue: HVec::new_in(PubHeap),
            head: 0,
            queued: 0,
            soft_since_ms: 0,
            overflowed: false,
            seq: AtomicU32::new(0),
            owner: 0,
        }
    }

    fn wake(&self) {
        self.seq.fetch_add(1, Ordering::Release);
        crate::sys::futex_wake(&self.seq);
    }

    /// Queue one message, closing the subscriber past an output limit or
    /// when the heap cannot hold it.
    fn push(
        &mut self,
        channel: &[u8],
        pattern: &[u8],
        payload: &[u8],
        limits: &Limits,
        now_ms: i64,
    ) {
        if self.overflowed {
            return;
        }
        let bytes = frame_bytes(channel, pattern, payload);
        let queue = &mut self.queue;
        let queued = catching(|| {
            queue.push(Queued {
                channel: name(channel),
                pattern: name(pattern),
                payload: name(payload),
                bytes,
            })
        });
        if queued.is_none() {
            return self.overflow();
        }
        self.queued += bytes;
        if self.over_limit(limits, now_ms) {
            return self.overflow();
        }
        self.wake();
    }

    /// `checkClientOutputBufferLimits`: at or past the hard limit, or at or
    /// past the soft one for more than its seconds, counted from the first
    /// check that found it there.
    fn over_limit(&mut self, limits: &Limits, now_ms: i64) -> bool {
        let hard = limits.hard > 0 && self.queued >= limits.hard;
        let soft = limits.soft > 0 && self.queued >= limits.soft;
        if !soft {
            self.soft_since_ms = 0;
        } else if self.soft_since_ms == 0 {
            self.soft_since_ms = now_ms;
        } else if (now_ms - self.soft_since_ms) / 1000 > limits.soft_secs {
            return true;
        }
        hard
    }

    fn overflow(&mut self) {
        self.overflowed = true;
        self.queue = HVec::new_in(PubHeap);
        self.head = 0;
        self.queued = 0;
        self.wake();
    }

    fn pop(&mut self) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let q = self.queue.get(self.head)?;
        let out = (q.channel.to_vec(), q.pattern.to_vec(), q.payload.to_vec());
        self.queued -= q.bytes;
        self.head += 1;
        if self.head == self.queue.len() {
            self.queue.clear();
            self.head = 0;
        } else if self.head >= 64 && self.head * 2 >= self.queue.len() {
            self.queue.drain(..self.head);
            self.head = 0;
        }
        Some(out)
    }
}

struct Inner {
    /// Every slot, by number; `None` is free.
    subs: HVec<Option<HBox<Sub>>>,
    free: HVec<u32>,
    /// Channel to the slots subscribed to it.
    by_channel: Index,
    /// Every pattern subscription, with its slot; scanned on each publish,
    /// as Redis scans its pattern list.
    patterns: HVec<(HVec<u8>, u32)>,
}

/// The registry: built once in the pub/sub heap at startup.
pub struct Slots {
    lock: AtomicU8,
    inner: UnsafeCell<Inner>,
}

// SAFETY: `inner` is reached only through `Locked`, under `lock`; the rest
// is one atomic.
unsafe impl Sync for Slots {}

impl Slots {
    pub(crate) fn new() -> Slots {
        Slots {
            lock: AtomicU8::new(0),
            inner: UnsafeCell::new(Inner {
                subs: HVec::new_in(PubHeap),
                free: HVec::new_in(PubHeap),
                by_channel: Index::with_hasher_in(std::hash::RandomState::new(), PubHeap),
                patterns: HVec::new_in(PubHeap),
            }),
        }
    }
}

/// The registry, once the heap is built.
pub fn registry() -> Option<&'static Slots> {
    crate::pubheap::root().map(|r| &r.pubsub)
}

/// The registry with its lock held.
struct Locked<'a> {
    _guard: SpinGuard<'a>,
    inner: &'a mut Inner,
}

fn lock(slots: &Slots) -> Locked<'_> {
    let guard = SpinGuard::acquire(&slots.lock);
    // SAFETY: the lock is held for as long as the guard, which the returned
    // value owns, so this is the only reference to `inner`.
    let inner = unsafe { &mut *slots.inner.get() };
    Locked {
        _guard: guard,
        inner,
    }
}

/// What adding a subscription will insert, allocated before anything moves.
enum Prepared {
    Channel(Key, HVec<u32>),
    Pattern(HVec<u8>),
}

#[derive(Clone, Copy, PartialEq)]
enum List {
    Channels,
    Patterns,
}

impl Inner {
    fn sub(&mut self, idx: usize) -> Option<&mut Sub> {
        self.subs.get_mut(idx)?.as_deref_mut()
    }

    fn counts(&mut self, idx: usize) -> (u32, u32) {
        self.sub(idx).map_or((0, 0), |s| {
            (s.channels.len() as u32, s.patterns.len() as u32)
        })
    }

    fn new_slot(&mut self) -> usize {
        let sub = HBox::new_in(Sub::new(), PubHeap);
        self.subs.reserve(1);
        match self.free.pop() {
            Some(i) => {
                self.subs[i as usize] = Some(sub);
                i as usize
            }
            None => {
                self.subs.push(Some(sub));
                self.subs.len() - 1
            }
        }
    }

    /// Add one subscription. Everything that can allocate is done first, so
    /// a full heap leaves the registry as it was.
    fn add(&mut self, idx: usize, item: &[u8], which: List) {
        let Some(sub) = self.subs.get_mut(idx).and_then(|s| s.as_deref_mut()) else {
            return;
        };
        let list = match which {
            List::Channels => &mut sub.channels,
            List::Patterns => &mut sub.patterns,
        };
        if list.iter().any(|n| n[..] == *item) {
            return;
        }
        let by_channel = &mut self.by_channel;
        let patterns = &mut self.patterns;
        let prepared = catching(|| {
            list.reserve(1);
            let own = name(item);
            let entry = match which {
                List::Channels => {
                    by_channel.reserve(1);
                    if let Some(v) = by_channel.get_mut(item) {
                        v.reserve(1);
                    }
                    let mut first = HVec::with_capacity_in(1, PubHeap);
                    first.push(idx as u32);
                    Prepared::Channel(Key(name(item)), first)
                }
                List::Patterns => {
                    patterns.reserve(1);
                    Prepared::Pattern(name(item))
                }
            };
            (own, entry)
        });
        let Some((own, entry)) = prepared else { return };
        // Nothing below allocates: every vector has room.
        list.push(own);
        match entry {
            Prepared::Channel(key, first) => match by_channel.get_mut(item) {
                Some(v) => v.push(idx as u32),
                None => {
                    by_channel.insert(key, first);
                }
            },
            Prepared::Pattern(n) => patterns.push((n, idx as u32)),
        }
    }

    fn remove(&mut self, idx: usize, item: &[u8], which: List) {
        let Some(sub) = self.sub(idx) else { return };
        let list = match which {
            List::Channels => &mut sub.channels,
            List::Patterns => &mut sub.patterns,
        };
        let Some(at) = list.iter().position(|n| n[..] == *item) else {
            return;
        };
        list.remove(at);
        match which {
            List::Channels => self.unindex(idx, item),
            List::Patterns => {
                if let Some(at) = self
                    .patterns
                    .iter()
                    .position(|(p, i)| *i == idx as u32 && p[..] == *item)
                {
                    self.patterns.swap_remove(at);
                }
            }
        }
    }

    fn unindex(&mut self, idx: usize, channel: &[u8]) {
        if let Some(v) = self.by_channel.get_mut(channel) {
            v.retain(|i| *i != idx as u32);
            if v.is_empty() {
                self.by_channel.remove(channel);
            }
        }
    }

    /// Free a slot and every subscription it held.
    fn release(&mut self, idx: usize) {
        let Some(sub) = self.subs.get_mut(idx).and_then(Option::take) else {
            return;
        };
        for ch in sub.channels.iter() {
            self.unindex(idx, ch);
        }
        self.patterns.retain(|(_, i)| *i != idx as u32);
        drop(sub);
        // Pushing can only fail for a full heap; the slot is then simply not
        // reused.
        let _ = catching(|| self.free.push(idx as u32));
    }

    /// After a change: a slot left with no subscriptions is freed.
    fn settle(&mut self, idx: usize) -> (u32, u32) {
        let counts = self.counts(idx);
        if counts == (0, 0) {
            self.release(idx);
        }
        counts
    }
}

/// Take a new slot and subscribe it. `None` when the heap is full.
fn alloc_and(slots: &Slots, owner: u64, items: &[Vec<u8>], which: List) -> Option<(usize, u32)> {
    let l = lock(slots);
    let idx = catching(|| l.inner.new_slot())?;
    if let Some(sub) = l.inner.sub(idx) {
        sub.owner = owner;
    }
    for item in items {
        l.inner.add(idx, item, which);
    }
    let (c, p) = l.inner.counts(idx);
    if (c, p) == (0, 0) {
        l.inner.release(idx);
        return None;
    }
    Some((idx, if which == List::Channels { c } else { p }))
}

/// A slot for client `owner`, subscribed to `channels`: its number and how
/// many it holds.
pub fn slot_alloc_and_subscribe(
    slots: &Slots,
    owner: u64,
    channels: &[Vec<u8>],
) -> Option<(usize, u32)> {
    alloc_and(slots, owner, channels, List::Channels)
}

pub fn slot_alloc_and_psubscribe(
    slots: &Slots,
    owner: u64,
    patterns: &[Vec<u8>],
) -> Option<(usize, u32)> {
    alloc_and(slots, owner, patterns, List::Patterns)
}

/// Each client's undelivered pub/sub bytes, for `CLIENT LIST`'s `omem`.
pub fn output_bytes(slots: &Slots) -> std::collections::HashMap<u64, usize> {
    let l = lock(slots);
    let mut out = std::collections::HashMap::new();
    for sub in l.inner.subs.iter().flatten() {
        if sub.owner != 0 {
            *out.entry(sub.owner).or_insert(0) += sub.queued;
        }
    }
    out
}

/// `clientsCron`'s output limit check, for subscribers no message has
/// reached lately: one held past the soft limit for its time is closed.
pub fn enforce_limits(slots: &Slots) {
    let limits = Limits::current();
    if limits.soft == 0 {
        return;
    }
    let now = crate::server::now_ms();
    let l = lock(slots);
    for sub in l.inner.subs.iter_mut().flatten() {
        if !sub.overflowed && sub.queued > 0 && sub.over_limit(&limits, now) {
            sub.overflow();
        }
    }
}

pub fn slot_free(slots: &Slots, idx: usize) {
    lock(slots).inner.release(idx);
}

fn change(slots: &Slots, idx: usize, items: &[Vec<u8>], which: List, add: bool) -> (u32, u32) {
    let l = lock(slots);
    for item in items {
        if add {
            l.inner.add(idx, item, which);
        } else {
            l.inner.remove(idx, item, which);
        }
    }
    l.inner.settle(idx)
}

pub fn subscribe(slots: &Slots, idx: usize, channels: &[Vec<u8>]) -> (u32, u32) {
    change(slots, idx, channels, List::Channels, true)
}

pub fn unsubscribe(slots: &Slots, idx: usize, channels: &[Vec<u8>]) -> (u32, u32) {
    change(slots, idx, channels, List::Channels, false)
}

pub fn psubscribe(slots: &Slots, idx: usize, patterns: &[Vec<u8>]) -> (u32, u32) {
    change(slots, idx, patterns, List::Patterns, true)
}

pub fn punsubscribe(slots: &Slots, idx: usize, patterns: &[Vec<u8>]) -> (u32, u32) {
    change(slots, idx, patterns, List::Patterns, false)
}

pub fn channel_names(slots: &Slots, idx: usize) -> Vec<Vec<u8>> {
    let l = lock(slots);
    l.inner
        .sub(idx)
        .map(|s| s.channels.iter().map(|n| n.to_vec()).collect())
        .unwrap_or_default()
}

pub fn pattern_names(slots: &Slots, idx: usize) -> Vec<Vec<u8>> {
    let l = lock(slots);
    l.inner
        .sub(idx)
        .map(|s| s.patterns.iter().map(|n| n.to_vec()).collect())
        .unwrap_or_default()
}

/// Deliver `message` to every subscriber of `channel` and of a pattern that
/// matches it; the number reached, as Redis counts it.
pub fn publish(slots: &Slots, channel: &[u8], message: &[u8]) -> i64 {
    let limits = Limits::current();
    let now = crate::server::now_ms();
    let l = lock(slots);
    let Inner {
        subs,
        by_channel,
        patterns,
        ..
    } = &mut *l.inner;
    let mut count = 0;
    let direct: Vec<u32> = by_channel
        .get(channel)
        .map(|v| v.to_vec())
        .unwrap_or_default();
    for idx in direct {
        if let Some(sub) = subs.get_mut(idx as usize).and_then(|s| s.as_deref_mut()) {
            sub.push(channel, b"", message, &limits, now);
            count += 1;
        }
    }
    // Patterns never see an internal channel: `PSUBSCRIBE *` gets neither
    // sharded messages, tracking invalidations nor MONITOR's traffic.
    if !is_internal(channel) {
        for (pat, idx) in patterns.iter() {
            if glob_match(pat, channel)
                && let Some(sub) = subs.get_mut(*idx as usize).and_then(|s| s.as_deref_mut())
            {
                sub.push(channel, pat, message, &limits, now);
                count += 1;
            }
        }
    }
    count
}

/// Where slot `idx`'s queue is, for a subscriber to pass to
/// [`wait_for_message`].
pub fn current_tail(slots: &Slots, idx: usize) -> u32 {
    let l = lock(slots);
    l.inner
        .sub(idx)
        .map_or(0, |s| s.seq.load(Ordering::Acquire))
}

/// Block until a message is queued for slot `idx` after `seen_tail`, or
/// `timeout` elapses. A publish wakes the sleeper; the timeout only bounds
/// how long a client command sits unread. Only the slot's owner calls this.
pub fn wait_for_message(slots: &Slots, idx: usize, seen_tail: u32, timeout: std::time::Duration) {
    let word: *const AtomicU32 = {
        let l = lock(slots);
        match l.inner.sub(idx) {
            Some(s) => &s.seq,
            None => return,
        }
    };
    // SAFETY: the word is inside the slot's boxed `Sub`, which does not move
    // and is freed only by `slot_free`/`unsubscribe` from this same owner,
    // which is here, waiting, and not freeing it.
    crate::sys::futex_wait(unsafe { &*word }, seen_tail, timeout);
}

/// Whether slot `idx` went past an output limit and must be disconnected.
pub fn overflowed(slots: &Slots, idx: usize) -> bool {
    let l = lock(slots);
    l.inner.sub(idx).is_some_and(|s| s.overflowed)
}

/// Take the oldest message waiting for slot `idx`.
pub fn poll_message(slots: &Slots, idx: usize) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let l = lock(slots);
    l.inner.sub(idx)?.pop()
}

/// PUBSUB CHANNELS [pattern], or SHARDCHANNELS with `shard`.
pub fn pubsub_channels(slots: &Slots, pattern: Option<&[u8]>, shard: bool) -> Vec<Vec<u8>> {
    let mut names: Vec<Vec<u8>> = {
        let l = lock(slots);
        l.inner
            .by_channel
            .keys()
            .filter_map(|Key(name)| {
                // `shard` lists the sharded namespace by the names clients
                // used; otherwise internal channels are hidden.
                let listed = match (shard, name.strip_prefix(SHARD_PREFIX)) {
                    (true, Some(shown)) => Some(shown),
                    (false, None) if !is_internal(name) => Some(&name[..]),
                    _ => None,
                }?;
                pattern
                    .is_none_or(|p| glob_match(p, listed))
                    .then(|| listed.to_vec())
            })
            .collect()
    };
    names.sort_unstable();
    names
}

/// PUBSUB NUMSUB [channel ...]
pub fn pubsub_numsub(slots: &Slots, channels: &[Vec<u8>]) -> Vec<i64> {
    let l = lock(slots);
    channels
        .iter()
        .map(|c| l.inner.by_channel.get(&c[..]).map_or(0, |v| v.len() as i64))
        .collect()
}

/// `PUBSUB NUMPAT`: the number of distinct patterns subscribed to; two
/// clients on `ch*` count once, as in Redis.
pub fn pubsub_numpat(slots: &Slots) -> i64 {
    let mut patterns: Vec<Vec<u8>> = {
        let l = lock(slots);
        l.inner.patterns.iter().map(|(p, _)| p.to_vec()).collect()
    };
    patterns.sort_unstable();
    patterns.dedup();
    patterns.len() as i64
}

#[repr(C, align(8))]
pub struct RouteEntry {
    pub channel: [u8; CHAN_LEN],
    pub channel_len: u16,
    pub schema: [u8; ROUTE_SCHEMA_LEN],
    pub schema_len: u16,
    pub table: [u8; ROUTE_TABLE_LEN],
    pub table_len: u16,
    pub active: u8,
    pub _pad: u8,
}

#[repr(C, align(8))]
pub struct RouteCtl {
    lock: AtomicU8,
    pub initialised: AtomicU8,
    route_count: AtomicU8,
    _pad: [u8; 5],
    entries: UnsafeCell<[RouteEntry; MAX_ROUTES]>,
}

// SAFETY: zero is an empty, unlocked table, and `entries` is reached only
// through `RoutesLocked`, under `lock` (rule 1).
unsafe impl crate::shared::Shareable for RouteCtl {}
// SAFETY: as above; the cell is never touched without the lock.
unsafe impl Sync for RouteCtl {}

pub fn route_ctl_size() -> usize {
    std::mem::size_of::<RouteCtl>()
}

/// RAII guard for this module's cross-process `AtomicU8` spinlocks. Releasing
/// in `Drop` is not tidiness: these are shared-memory bytes with no owner
/// tracking, so unwinding while holding one wedges pub/sub in every process.
struct SpinGuard<'a>(&'a AtomicU8);

impl<'a> SpinGuard<'a> {
    fn acquire(lock: &'a AtomicU8) -> Self {
        while lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Self(lock)
    }
}

impl Drop for SpinGuard<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

/// The routes, with their lock held.
struct RoutesLocked<'a> {
    _guard: SpinGuard<'a>,
    entries: &'a mut [RouteEntry; MAX_ROUTES],
}

impl RouteCtl {
    fn locked(&self) -> RoutesLocked<'_> {
        let guard = SpinGuard::acquire(&self.lock);
        // SAFETY: rule 1 — `lock` is held for as long as the guard, which the
        // returned value owns, so this is the only reference to the entries.
        let entries = unsafe { &mut *self.entries.get() };
        RoutesLocked {
            _guard: guard,
            entries,
        }
    }
}

pub fn route_add(ctl: &RouteCtl, channel: &[u8], schema: &[u8], table: &[u8]) -> bool {
    let ch_len = channel.len().min(CHAN_LEN - 1);
    let sc_len = schema.len().min(ROUTE_SCHEMA_LEN - 1);
    let tb_len = table.len().min(ROUTE_TABLE_LEN - 1);

    let routes = ctl.locked();
    let mut free_idx = None;
    for (i, e) in routes.entries.iter_mut().enumerate() {
        if e.active == 0 {
            if free_idx.is_none() {
                free_idx = Some(i);
            }
            continue;
        }
        if e.channel_len as usize == ch_len && e.channel[..ch_len] == channel[..ch_len] {
            e.schema[..sc_len].copy_from_slice(&schema[..sc_len]);
            e.schema_len = sc_len as u16;
            e.table[..tb_len].copy_from_slice(&table[..tb_len]);
            e.table_len = tb_len as u16;
            return true;
        }
    }
    let Some(idx) = free_idx else { return false };
    let e = &mut routes.entries[idx];
    e.channel[..ch_len].copy_from_slice(&channel[..ch_len]);
    e.channel_len = ch_len as u16;
    e.schema[..sc_len].copy_from_slice(&schema[..sc_len]);
    e.schema_len = sc_len as u16;
    e.table[..tb_len].copy_from_slice(&table[..tb_len]);
    e.table_len = tb_len as u16;
    e.active = 1;
    ctl.route_count.fetch_add(1, Ordering::Relaxed);
    true
}

pub fn route_remove(ctl: &RouteCtl, channel: &[u8]) -> bool {
    let routes = ctl.locked();
    for e in routes.entries.iter_mut() {
        let ch_len = e.channel_len as usize;
        if e.active != 0 && ch_len == channel.len() && e.channel[..ch_len] == *channel {
            e.active = 0;
            ctl.route_count.fetch_sub(1, Ordering::Relaxed);
            return true;
        }
    }
    false
}

pub fn route_lookup(ctl: &RouteCtl, channel: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    if ctl.route_count.load(Ordering::Relaxed) == 0 {
        return None;
    }
    let routes = ctl.locked();
    routes.entries.iter().find_map(|e| {
        let ch_len = e.channel_len as usize;
        (e.active != 0 && ch_len == channel.len() && e.channel[..ch_len] == *channel).then(|| {
            (
                e.schema[..e.schema_len as usize].to_vec(),
                e.table[..e.table_len as usize].to_vec(),
            )
        })
    })
}

/// Redis-compatible glob match over `*`, `?` and `[...]`. Iterative and
/// index-based, so no input can drive a slice out of bounds: `publish` calls
/// this holding the pub/sub spinlock, where a panic would be fatal.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    // Redis's `stringmatchlen` never enters its loop for an empty name, so
    // only the empty pattern matches one: `*` included, checked on Redis 7.2.
    if string.is_empty() {
        return pattern.is_empty();
    }
    let (mut p, mut s) = (0usize, 0usize);
    // Position of the last `*` seen, and how much of `string` it currently absorbs.
    let (mut star_p, mut star_s) = (usize::MAX, 0usize);

    while s < string.len() {
        let consumed = match pattern.get(p) {
            Some(b'*') => {
                star_p = p;
                star_s = s;
                p += 1;
                continue;
            }
            Some(b'?') => {
                p += 1;
                s += 1;
                continue;
            }
            // `\x` matches `x` literally, as in Redis's `stringmatchlen`; a
            // backslash ending the pattern is an ordinary character.
            Some(b'\\') if p + 1 < pattern.len() => {
                p += 1;
                pattern[p] == string[s]
            }
            Some(b'[') => match match_class(&pattern[p + 1..], string[s]) {
                (true, len) => {
                    p += 1 + len;
                    s += 1;
                    continue;
                }
                (false, _) => false,
            },
            Some(&c) => c == string[s],
            None => false,
        };
        if consumed {
            p += 1;
            s += 1;
            continue;
        }
        // Mismatch: give the last `*` one more character, or fail outright.
        if star_p == usize::MAX {
            return false;
        }
        star_s += 1;
        s = star_s;
        p = star_p + 1;
    }

    // The string is exhausted; the rest of the pattern must be trailing `*`s.
    while pattern.get(p) == Some(&b'*') {
        p += 1;
    }
    p == pattern.len()
}

/// Match one `[...]` class body (everything after the `[`) against `ch`.
/// Returns whether it matched and how many bytes the class body occupies,
/// including its closing `]` when present.
fn match_class(pat: &[u8], ch: u8) -> (bool, usize) {
    let negate = pat.first() == Some(&b'^');
    let mut i = usize::from(negate);
    let mut matched = false;
    while let Some(&c) = pat.get(i) {
        if c == b'\\' && i + 1 < pat.len() {
            // `\x` in a class is `x`, `]` included.
            matched |= ch == pat[i + 1];
            i += 2;
            continue;
        }
        if c == b']' {
            return (matched != negate, i + 1);
        }
        if pat.len() - i > 2 && pat[i + 1] == b'-' {
            // Redis swaps a range given backwards: `[z-a]` is `[a-z]`.
            let (lo, hi) = if c <= pat[i + 2] {
                (c, pat[i + 2])
            } else {
                (pat[i + 2], c)
            };
            matched |= ch >= lo && ch <= hi;
            i += 3;
        } else {
            matched |= ch == c;
            i += 1;
        }
    }
    // Unterminated class — consume the remainder, as the original scan did.
    (matched != negate, i)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One table drives every wildcard form PSUBSCRIBE accepts. Cases are
    /// `(pattern, channel, should_match)`.
    const GLOB_CASES: &[(&[u8], &[u8], bool)] = &[
        // exact
        (b"hello", b"hello", true),
        (b"hello", b"world", false),
        (b"", b"", true),
        (b"", b"x", false),
        // `*`
        (b"*", b"anything", true),
        // Redis matches no wildcard against an empty name (checked on 7.2).
        (b"*", b"", false),
        (b"h*o", b"hello", true),
        (b"news.*", b"news.sports", true),
        (b"news.*", b"sports", false),
        (b"*a*b*", b"xaxb", true),
        (b"*a*b*", b"xbxa", false),
        // A `*` must not swallow a trailing literal that never matches.
        (b"*a", b"aab", false),
        (b"*a", b"aba", true),
        // `?`
        (b"h?llo", b"hello", true),
        (b"h?llo", b"hllo", false),
        // classes
        (b"[hH]ello", b"hello", true),
        (b"[hH]ello", b"Hello", true),
        (b"[hH]ello", b"xello", false),
        (b"[a-z]ello", b"hello", true),
        (b"[a-z]ello", b"Hello", false),
        (b"[^0-9]ello", b"hello", true),
        (b"[^0-9]ello", b"1ello", false),
        // Wildcards that run past the end of an empty channel. These used to
        // panic inside `publish`, wedging the pub/sub spinlock for every worker.
        (b"*?", b"", false),
        (b"*[a]", b"", false),
        (b"?", b"", false),
        (b"*a", b"", false),
        // Unterminated class must not run off the end either.
        (b"[abc", b"a", true),
        (b"[abc", b"z", false),
    ];

    #[test]
    fn glob_match_table() {
        for &(pattern, string, expected) in GLOB_CASES {
            assert_eq!(
                glob_match(pattern, string),
                expected,
                "pattern {:?} vs {:?}",
                String::from_utf8_lossy(pattern),
                String::from_utf8_lossy(string),
            );
        }
    }

    /// The registry over a test heap in ordinary memory, shared by every test
    /// in the process; each test uses channel names of its own.
    fn reg() -> &'static Slots {
        crate::pubheap::for_tests();
        registry().expect("the test registry")
    }

    fn v(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn a_message_reaches_its_channels_subscribers_in_order() {
        let r = reg();
        let (idx, n) = slot_alloc_and_subscribe(r, 1, &[v("t1:a"), v("t1:b")]).unwrap();
        assert_eq!(n, 2);
        assert_eq!(publish(r, b"t1:a", b"one"), 1);
        assert_eq!(publish(r, b"t1:b", b"two"), 1);
        assert_eq!(publish(r, b"t1:none", b"x"), 0);
        assert_eq!(poll_message(r, idx), Some((v("t1:a"), vec![], v("one"))));
        assert_eq!(poll_message(r, idx), Some((v("t1:b"), vec![], v("two"))));
        assert_eq!(poll_message(r, idx), None);
        slot_free(r, idx);
        assert_eq!(publish(r, b"t1:a", b"gone"), 0);
    }

    #[test]
    fn a_pattern_gets_a_pmessage_and_never_an_internal_channel() {
        let r = reg();
        let (idx, _) = slot_alloc_and_psubscribe(r, 1, &[v("t2:*")]).unwrap();
        assert_eq!(publish(r, b"t2:x", b"m"), 1);
        assert_eq!(poll_message(r, idx), Some((v("t2:x"), v("t2:*"), v("m"))));
        // A pattern that matches the internal mark still skips internal
        // channels. (`*` would also catch the other tests' channels.)
        psubscribe(r, idx, &[v("\x01*")]);
        publish(r, b"\x01shard:t2", b"internal");
        assert_eq!(poll_message(r, idx), None);
        slot_free(r, idx);
    }

    /// Redis has no limit on how many channels a client holds, how long a
    /// name is or how large a payload is; neither does this.
    #[test]
    fn subscriptions_names_and_payloads_are_unbounded() {
        let r = reg();
        let channels: Vec<Vec<u8>> = (0..200).map(|i| v(&format!("t3:{i}"))).collect();
        let (idx, n) = slot_alloc_and_subscribe(r, 1, &channels).unwrap();
        assert_eq!(n, 200);
        let long = vec![b'c'; 4000];
        subscribe(r, idx, std::slice::from_ref(&long));
        let payload = vec![7u8; 1 << 20];
        assert_eq!(publish(r, &long, &payload), 1);
        assert_eq!(poll_message(r, idx), Some((long.clone(), vec![], payload)));
        assert_eq!(
            pubsub_numsub(r, &[long, v("t3:199"), v("t3:nope")]),
            vec![1, 1, 0]
        );
        slot_free(r, idx);
        assert_eq!(pubsub_numsub(r, &[v("t3:0")]), vec![0]);
    }

    #[test]
    fn a_subscriber_past_the_hard_limit_is_disconnected() {
        let r = reg();
        let (idx, _) = slot_alloc_and_subscribe(r, 1, &[v("t4")]).unwrap();
        let limits = Limits {
            hard: 1 << 20,
            soft: 0,
            soft_secs: 0,
        };
        let l = lock(r);
        let sub = l.inner.sub(idx).unwrap();
        for _ in 0..3 {
            sub.push(b"t4", b"", &vec![0u8; 400 << 10], &limits, 1);
        }
        assert!(sub.overflowed);
        // Its queue is let go, and nothing more is queued for it.
        assert_eq!(sub.queued, 0);
        assert!(sub.pop().is_none());
        drop(l);
        assert!(overflowed(r, idx));
        slot_free(r, idx);
    }

    #[test]
    fn the_soft_limit_disconnects_only_once_held_for_its_time() {
        let r = reg();
        let (idx, _) = slot_alloc_and_subscribe(r, 1, &[v("t5")]).unwrap();
        let limits = Limits {
            hard: 0,
            soft: 100,
            soft_secs: 60,
        };
        let l = lock(r);
        let sub = l.inner.sub(idx).unwrap();
        sub.push(b"t5", b"", &[0u8; 200], &limits, 1_000);
        assert!(!sub.overflowed);
        sub.push(b"t5", b"", &[0u8; 10], &limits, 30_000);
        assert!(!sub.overflowed);
        // Held 60 seconds is not more than 60.
        sub.push(b"t5", b"", &[0u8; 10], &limits, 61_000);
        assert!(!sub.overflowed);
        sub.push(b"t5", b"", &[0u8; 10], &limits, 62_000);
        assert!(sub.overflowed);
        drop(l);
        slot_free(r, idx);
    }

    #[test]
    fn output_bytes_are_counted_per_owner() {
        let r = reg();
        let (idx, _) = slot_alloc_and_subscribe(r, 4242, &[v("t9")]).unwrap();
        publish(r, b"t9", b"hi");
        assert_eq!(
            output_bytes(r).get(&4242),
            Some(&frame_bytes(b"t9", b"", b"hi"))
        );
        poll_message(r, idx);
        assert_eq!(output_bytes(r).get(&4242), Some(&0));
        slot_free(r, idx);
        assert_eq!(output_bytes(r).get(&4242), None);
    }

    #[test]
    fn a_publish_wakes_a_waiting_subscriber() {
        let r = reg();
        let (idx, _) = slot_alloc_and_subscribe(r, 1, &[v("t6")]).unwrap();
        let seen = current_tail(r, idx);
        let waiter = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            wait_for_message(r, idx, seen, std::time::Duration::from_secs(10));
            started.elapsed()
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        publish(r, b"t6", b"wake");
        let waited = waiter.join().unwrap();
        assert!(waited < std::time::Duration::from_secs(5), "{waited:?}");
        assert_eq!(poll_message(r, idx).map(|m| m.2), Some(v("wake")));
        slot_free(r, idx);
    }

    #[test]
    fn unsubscribing_from_everything_frees_the_slot() {
        let r = reg();
        let (idx, _) = slot_alloc_and_subscribe(r, 1, &[v("t7:a"), v("t7:b")]).unwrap();
        assert_eq!(unsubscribe(r, idx, &[v("t7:a")]), (1, 0));
        assert_eq!(unsubscribe(r, idx, &[v("t7:b")]), (0, 0));
        assert_eq!(publish(r, b"t7:b", b"x"), 0);
        assert!(channel_names(r, idx).is_empty());
    }

    #[test]
    fn channels_and_patterns_are_listed_and_counted_as_redis_does() {
        let r = reg();
        let (a, _) = slot_alloc_and_subscribe(r, 1, &[v("t8:x"), v("t8:y")]).unwrap();
        let (b, _) = slot_alloc_and_psubscribe(r, 1, &[v("t8:*")]).unwrap();
        let (c, _) = slot_alloc_and_psubscribe(r, 1, &[v("t8:*")]).unwrap();
        subscribe(r, b, &[shard_name(b"t8:s")]);
        assert_eq!(
            pubsub_channels(r, Some(b"t8:*"), false),
            vec![v("t8:x"), v("t8:y")]
        );
        assert_eq!(pubsub_channels(r, Some(b"t8:*"), true), vec![v("t8:s")]);
        assert!(pubsub_numpat(r) >= 1);
        for i in [a, b, c] {
            slot_free(r, i);
        }
    }

    #[test]
    fn output_limits_parse_as_redis_writes_them() {
        assert_eq!(
            parse_limits("normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"),
            DEFAULT_LIMITS
        );
        assert_eq!(
            parse_limits("pubsub 64mb 16mb 30"),
            Limits {
                hard: 64 << 20,
                soft: 16 << 20,
                soft_secs: 30
            }
        );
        assert_eq!(parse_limits("pubsub 1k 0 0").hard, 1000);
        assert_eq!(parse_limits("nonsense"), DEFAULT_LIMITS);
    }

    #[test]
    fn a_frame_is_counted_in_its_wire_bytes() {
        // *3\r\n $7\r\nmessage\r\n $2\r\nch\r\n $2\r\nhi\r\n
        assert_eq!(frame_bytes(b"ch", b"", b"hi"), 4 + 13 + 8 + 8);
    }
}

pub mod shared {
    /// What `publish` did: the receivers reached, and the `(schema, table)`
    /// the channel is routed to, if any.
    pub type Published = (i64, Option<(Vec<u8>, Vec<u8>)>);

    /// `PUBLISH`: the receivers reached, and the table the channel is routed
    /// to, if any, so the caller can insert the message there.
    pub fn publish(channel: &[u8], message: &[u8]) -> Option<Published> {
        let slots = crate::pubsub_state()?;
        let n = super::publish(slots, channel, message);
        let route =
            crate::route_state().and_then(|route_ctl| super::route_lookup(route_ctl, channel));
        Some((n, route))
    }

    pub fn channels(pattern: Option<&[u8]>, shard: bool) -> Option<Vec<Vec<u8>>> {
        let slots = crate::pubsub_state()?;
        Some(super::pubsub_channels(slots, pattern, shard))
    }

    pub fn numsub(channels: &[Vec<u8>]) -> Option<Vec<i64>> {
        let slots = crate::pubsub_state()?;
        Some(super::pubsub_numsub(slots, channels))
    }

    pub fn numpat() -> Option<i64> {
        let slots = crate::pubsub_state()?;
        Some(super::pubsub_numpat(slots))
    }

    /// A pub/sub command run off the connection's thread — queued in a
    /// transaction, or called from a script. `None` for any other command.
    /// The reply comes with the insert a routed channel needs, which the
    /// caller runs: it is the one holding a database connection.
    pub fn run(
        cmd: crate::commands::Command,
    ) -> Option<(crate::commands::Response, Option<crate::commands::Command>)> {
        use crate::commands::{Command, Response, Value};
        Some(match cmd {
            Command::Publish { channel, message } => match publish(&channel, &message) {
                None => (Response::Integer(0), None),
                Some((n, route)) => (
                    Response::Integer(n),
                    route.map(|(schema, table)| Command::TablePublish {
                        schema,
                        table,
                        channel,
                        payload: message,
                    }),
                ),
            },
            Command::PubSubChannels { pattern, shard } => (
                Response::Array(
                    channels(pattern.as_deref(), shard)
                        .unwrap_or_default()
                        .into_iter()
                        .map(Some)
                        .collect(),
                ),
                None,
            ),
            Command::PubSubNumSub { channels, shard } => {
                let stored: Vec<Vec<u8>> = if shard {
                    channels.iter().map(|c| super::shard_name(c)).collect()
                } else {
                    channels.clone()
                };
                let counts = numsub(&stored).unwrap_or_else(|| vec![0; channels.len()]);
                (
                    Response::Value(Value::Array(
                        channels
                            .into_iter()
                            .zip(counts)
                            .flat_map(|(c, n)| [Value::Bulk(c), Value::Int(n)])
                            .collect(),
                    )),
                    None,
                )
            }
            Command::PubSubNumPat => (Response::Integer(numpat().unwrap_or(0)), None),
            _ => return None,
        })
    }
}
