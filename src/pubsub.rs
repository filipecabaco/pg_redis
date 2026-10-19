use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

const MAX_PUBSUB_SLOTS: usize = 256;
const PUBSUB_RING_CAP: usize = 256;
pub const MAX_SUBS_PER_SLOT: usize = 16;
/// The channel list has one entry more than a client may fill: the
/// connection's own client-tracking channel (`tracking::channel`) lives there,
/// so a client gets all `MAX_SUBS_PER_SLOT` of its own.
const CHANNEL_ENTRIES: usize = MAX_SUBS_PER_SLOT + 1;
/// Maximum PUBLISH payload: the size of one slot in the shared message ring.
pub const PUBSUB_MSG_LEN: usize = 512;
/// Maximum channel or pattern name. One byte is reserved for the NUL
/// terminator, so `MAX_CHANNEL_LEN` is what callers may actually use.
const CHAN_LEN: usize = 256;
pub const MAX_CHANNEL_LEN: usize = CHAN_LEN - 1;

/// Sharded pub/sub (`SSUBSCRIBE`, `SPUBLISH`) is its own namespace in Redis.
/// Its channels share the registry here under this prefix. A leading 0x01
/// marks a channel as internal — kept out of `PUBSUB CHANNELS` and out of
/// every pattern subscription. Not a NUL: slot entries are NUL-terminated, so
/// a name starting with one would read back empty.
pub const SHARD_PREFIX: &[u8] = b"\x01shard:";

/// The byte every internal channel starts with.
pub const INTERNAL_MARK: u8 = 0x01;

/// Whether `channel` is one of this extension's own: sharded, or MONITOR's.
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
// Two shared-memory blocks hold every process's pub/sub state, and two rules
// make sharing them sound. They are the only `unsafe` in this module.
//
// 1. Subscriptions, and the routes, are read and written only while holding
//    their block's spinlock. `Locked` and `RoutesLocked` are what holding it
//    looks like: the only way to reach the `UnsafeCell`s behind the lock.
// 2. A slot's message ring has one consumer, the connection that owns the
//    slot, and publishers who write only under the spinlock — so one producer
//    at a time. A publisher fills the message at `tail` and then publishes
//    `tail + 1`; the owner reads the message at `head` and then publishes
//    `head + 1`. Each side touches a message only when the other's index says
//    it may (`write_to_ring`, `poll_message`).

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

/// Header block: the global spinlock protecting every slot's subscriptions,
/// and the PUBLISH scans over them. A CAS on an `AtomicU8` works across the
/// worker processes sharing this page; no PostgreSQL call is needed.
#[repr(C)]
pub struct PubsubCtl {
    lock: AtomicU8,
    _pad: [u8; 7],
}

// SAFETY: one atomic; zero is unlocked.
unsafe impl crate::shared::Shareable for PubsubCtl {}

#[repr(C)]
pub struct PubsubMsg {
    pub channel: [u8; CHAN_LEN],
    pub channel_len: u16,
    pub pattern_len: u16,
    pub payload_len: u32,
    pub payload: [u8; PUBSUB_MSG_LEN],
    pub pattern: [u8; CHAN_LEN],
}

/// What a slot is subscribed to. Under `PubsubCtl::lock` (rule 1).
#[repr(C)]
pub struct Subs {
    pub channels: [[u8; CHAN_LEN]; CHANNEL_ENTRIES],
    pub patterns: [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
    pub channel_count: u32,
    pub pattern_count: u32,
    pub in_use: u8,
    _pad: [u8; 3],
}

impl Subs {
    fn reset(&mut self) {
        self.in_use = 0;
        self.channel_count = 0;
        self.pattern_count = 0;
    }
}

#[repr(C)]
pub struct PubsubSlot {
    subs: UnsafeCell<Subs>,
    head: AtomicU32,
    tail: AtomicU32,
    /// Set when a publish found the ring full. Redis disconnects a subscriber
    /// that falls this far behind (`client-output-buffer-limit pubsub`); its
    /// connection sees this and closes, so the client learns it lost messages
    /// instead of carrying on with a gap it cannot see.
    overflowed: AtomicU8,
    ring: [UnsafeCell<PubsubMsg>; PUBSUB_RING_CAP],
}

// SAFETY: zero is a free slot with an empty ring; `subs` is reached only under
// the global lock (rule 1) and `ring` only by the head/tail protocol (rule 2).
unsafe impl Sync for PubsubSlot {}

/// Every slot.
pub type Slots = [PubsubSlot; MAX_PUBSUB_SLOTS];

// SAFETY: an array of `PubsubSlot`s, each of which zero makes free and empty.
unsafe impl crate::shared::Shareable for Slots {}

pub fn pubsub_ctl_size() -> usize {
    std::mem::size_of::<PubsubCtl>()
}

/// Shared memory the slot table occupies — ~66 MiB at the current constants,
/// requested whether or not pub/sub is used. Raising `CHAN_LEN`,
/// `PUBSUB_MSG_LEN` or `PUBSUB_RING_CAP` is a real cost to every deployment.
pub fn pubsub_slots_size() -> usize {
    std::mem::size_of::<Slots>()
}

/// Every slot's subscriptions, with the global lock held.
struct Locked<'a> {
    _guard: SpinGuard<'a>,
    slots: &'a Slots,
}

impl<'a> Locked<'a> {
    fn new(ctl: &'a PubsubCtl, slots: &'a Slots) -> Self {
        Locked {
            _guard: SpinGuard::acquire(&ctl.lock),
            slots,
        }
    }

    /// Slot `idx`'s subscriptions. Borrowing `self` mutably keeps any two
    /// from being alive at once.
    fn subs(&mut self, idx: usize) -> &mut Subs {
        // SAFETY: rule 1 — the global lock is held for as long as `self`,
        // and the `&mut self` borrow makes this the only reference into any
        // slot's subscriptions until it ends.
        unsafe { &mut *self.slots[idx].subs.get() }
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

/// Free slot `idx`'s subscriptions and empty its ring. Under the lock: the
/// ring indices move here, while no publisher can be writing to it.
fn slot_reset(slots: &Slots, subs: &mut Subs, idx: usize) {
    subs.reset();
    slots[idx].head.store(0, Ordering::Relaxed);
    slots[idx].tail.store(0, Ordering::Relaxed);
    slots[idx].overflowed.store(0, Ordering::Relaxed);
}

/// Length of a NUL-terminated fixed-size slot entry.
fn entry_len(entry: &[u8; CHAN_LEN]) -> usize {
    entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN)
}

/// Compare a NUL-terminated slot entry against `value`, without locating the
/// terminator first: PUBLISH runs this against every subscription of every
/// slot, and almost all of them mismatch on the first byte.
fn entry_eq(entry: &[u8; CHAN_LEN], value: &[u8]) -> bool {
    value.len() < CHAN_LEN && entry[..value.len()] == *value && entry[value.len()] == 0
}

fn slot_add(arr: &mut [[u8; CHAN_LEN]], count: &mut u32, value: &[u8]) -> bool {
    let n = *count as usize;
    if arr[..n].iter().any(|entry| entry_eq(entry, value)) {
        return false;
    }
    let tracking = |name: &[u8]| name.starts_with(crate::tracking::TRACK_PREFIX);
    let clients = arr[..n]
        .iter()
        .filter(|entry| !tracking(&entry[..entry_len(entry)]))
        .count();
    if n >= arr.len() || (!tracking(value) && clients >= MAX_SUBS_PER_SLOT) {
        return false;
    }
    let copy_len = value.len().min(CHAN_LEN - 1);
    arr[n][..copy_len].copy_from_slice(&value[..copy_len]);
    arr[n][copy_len] = 0;
    *count += 1;
    true
}

fn slot_remove(arr: &mut [[u8; CHAN_LEN]], count: &mut u32, value: &[u8]) {
    let n = *count as usize;
    if let Some(i) = arr[..n].iter().position(|entry| entry_eq(entry, value)) {
        arr[i] = arr[n - 1];
        *count -= 1;
    }
}

fn slot_names(arr: &[[u8; CHAN_LEN]], count: u32) -> Vec<Vec<u8>> {
    arr[..count as usize]
        .iter()
        .map(|entry| entry[..entry_len(entry)].to_vec())
        .collect()
}

/// Which of a slot's two lists a subscription command works on.
#[derive(Clone, Copy)]
enum List {
    Channels,
    Patterns,
}

fn list(subs: &mut Subs, which: List) -> (&mut [[u8; CHAN_LEN]], &mut u32) {
    match which {
        List::Channels => (&mut subs.channels, &mut subs.channel_count),
        List::Patterns => (&mut subs.patterns, &mut subs.pattern_count),
    }
}

/// Mutate a slot under the lock and return its counts, freeing it when both
/// reach zero.
fn with_slot_locked(
    ctl: &PubsubCtl,
    slots: &Slots,
    idx: usize,
    mutate: impl FnOnce(&mut Subs),
) -> (u32, u32) {
    let mut locked = Locked::new(ctl, slots);
    let subs = locked.subs(idx);
    mutate(subs);
    let counts = (subs.channel_count, subs.pattern_count);
    if counts == (0, 0) {
        slot_reset(slots, subs, idx);
    }
    counts
}

/// Allocate a free slot and subscribe it in one lock acquisition. `None` if
/// every slot is taken.
fn slot_alloc_and_sub(
    ctl: &PubsubCtl,
    slots: &Slots,
    items: &[Vec<u8>],
    which: List,
) -> Option<(usize, u32)> {
    let mut locked = Locked::new(ctl, slots);
    (0..MAX_PUBSUB_SLOTS).find_map(|i| {
        let subs = locked.subs(i);
        (subs.in_use == 0).then(|| {
            slot_reset(slots, subs, i);
            subs.in_use = 1;
            let (arr, count) = list(subs, which);
            for item in items {
                slot_add(arr, count, item);
            }
            (i, *count)
        })
    })
}

pub fn slot_alloc_and_subscribe(
    ctl: &PubsubCtl,
    slots: &Slots,
    channels: &[Vec<u8>],
) -> Option<(usize, u32)> {
    slot_alloc_and_sub(ctl, slots, channels, List::Channels)
}

pub fn slot_alloc_and_psubscribe(
    ctl: &PubsubCtl,
    slots: &Slots,
    patterns: &[Vec<u8>],
) -> Option<(usize, u32)> {
    slot_alloc_and_sub(ctl, slots, patterns, List::Patterns)
}

pub fn slot_free(ctl: &PubsubCtl, slots: &Slots, idx: usize) {
    let mut locked = Locked::new(ctl, slots);
    let subs = locked.subs(idx);
    if subs.in_use != 0 {
        slot_reset(slots, subs, idx);
    }
}

fn change(
    ctl: &PubsubCtl,
    slots: &Slots,
    idx: usize,
    items: &[Vec<u8>],
    which: List,
    add: bool,
) -> (u32, u32) {
    with_slot_locked(ctl, slots, idx, |subs| {
        let (arr, count) = list(subs, which);
        for item in items {
            if add {
                slot_add(arr, count, item);
            } else {
                slot_remove(arr, count, item);
            }
        }
    })
}

pub fn subscribe(ctl: &PubsubCtl, slots: &Slots, idx: usize, channels: &[Vec<u8>]) -> (u32, u32) {
    change(ctl, slots, idx, channels, List::Channels, true)
}

pub fn unsubscribe(ctl: &PubsubCtl, slots: &Slots, idx: usize, channels: &[Vec<u8>]) -> (u32, u32) {
    change(ctl, slots, idx, channels, List::Channels, false)
}

pub fn psubscribe(ctl: &PubsubCtl, slots: &Slots, idx: usize, patterns: &[Vec<u8>]) -> (u32, u32) {
    change(ctl, slots, idx, patterns, List::Patterns, true)
}

pub fn punsubscribe(
    ctl: &PubsubCtl,
    slots: &Slots,
    idx: usize,
    patterns: &[Vec<u8>],
) -> (u32, u32) {
    change(ctl, slots, idx, patterns, List::Patterns, false)
}

pub fn channel_names(ctl: &PubsubCtl, slots: &Slots, idx: usize) -> Vec<Vec<u8>> {
    let mut locked = Locked::new(ctl, slots);
    let subs = locked.subs(idx);
    slot_names(&subs.channels, subs.channel_count)
}

pub fn pattern_names(ctl: &PubsubCtl, slots: &Slots, idx: usize) -> Vec<Vec<u8>> {
    let mut locked = Locked::new(ctl, slots);
    let subs = locked.subs(idx);
    slot_names(&subs.patterns, subs.pattern_count)
}

pub fn publish(ctl: &PubsubCtl, slots: &Slots, channel: &[u8], message: &[u8]) -> i64 {
    let mut locked = Locked::new(ctl, slots);
    let mut count: i64 = 0;
    for (i, slot) in slots.iter().enumerate() {
        let subs = locked.subs(i);
        if subs.in_use == 0 {
            continue;
        }
        let ch_count = subs.channel_count as usize;
        if subs.channels[..ch_count]
            .iter()
            .any(|entry| entry_eq(entry, channel))
            && write_to_ring(slot, channel, b"", message)
        {
            count += 1;
        }
        // Patterns never see an internal channel: `PSUBSCRIBE *` gets
        // neither sharded messages nor MONITOR's traffic.
        if is_internal(channel) {
            continue;
        }
        let pat_count = subs.pattern_count as usize;
        for entry in subs.patterns[..pat_count].iter() {
            let pat = &entry[..entry_len(entry)];
            if glob_match(pat, channel) && write_to_ring(slot, channel, pat, message) {
                count += 1;
            }
        }
    }
    count
}

/// Append a message to `slot`'s ring, if it has room. Callers hold the global
/// lock, which is what makes them the ring's one producer (rule 2).
fn write_to_ring(slot: &PubsubSlot, channel: &[u8], pattern: &[u8], payload: &[u8]) -> bool {
    let head = slot.head.load(Ordering::Acquire);
    let tail = slot.tail.load(Ordering::Relaxed);
    if tail.wrapping_sub(head) >= PUBSUB_RING_CAP as u32 {
        slot.overflowed.store(1, Ordering::Release);
        return false;
    }
    // SAFETY: rule 2 — the message at `tail` is outside what the consumer may
    // read (`head..tail`), and the room check above means the consumer has
    // finished with it: its Release store of `head` past it happened-before
    // our Acquire load. Only this producer, under the lock, writes it.
    let msg = unsafe { &mut *slot.ring[(tail as usize) % PUBSUB_RING_CAP].get() };

    let ch_len = channel.len().min(CHAN_LEN - 1);
    msg.channel[..ch_len].copy_from_slice(&channel[..ch_len]);
    msg.channel[ch_len] = 0;
    msg.channel_len = ch_len as u16;

    let pat_len = pattern.len().min(CHAN_LEN - 1);
    msg.pattern[..pat_len].copy_from_slice(&pattern[..pat_len]);
    msg.pattern[pat_len] = 0;
    msg.pattern_len = pat_len as u16;

    // Callers reject oversized payloads before reaching here; the clamp is a
    // belt-and-braces bound on the copy, not the enforcement point.
    let pay_len = payload.len().min(PUBSUB_MSG_LEN);
    msg.payload[..pay_len].copy_from_slice(&payload[..pay_len]);
    msg.payload_len = pay_len as u32;

    slot.tail.store(tail.wrapping_add(1), Ordering::Release);
    crate::sys::futex_wake(&slot.tail);
    true
}

/// Current ring position, for a subscriber to pass to [`wait_for_message`].
pub fn current_tail(slots: &Slots, idx: usize) -> u32 {
    slots[idx].tail.load(Ordering::Acquire)
}

/// Block until this slot's ring advances past `seen_tail`, or `timeout`
/// elapses. Waiting on the counter rather than polling the socket is what makes
/// delivery immediate; the timeout only bounds how long a client command sits
/// unread. A missed wakeup costs one timeout.
pub fn wait_for_message(slots: &Slots, idx: usize, seen_tail: u32, timeout: std::time::Duration) {
    wait_on(&slots[idx], seen_tail, timeout);
}

fn wait_on(slot: &PubsubSlot, seen_tail: u32, timeout: std::time::Duration) {
    crate::sys::futex_wait(&slot.tail, seen_tail, timeout);
}

/// Take the oldest message from slot `idx`'s ring. Only the connection that
/// owns the slot calls this: it is the ring's one consumer (rule 2).
/// Whether a message for slot `idx` was dropped because its ring was full.
pub fn overflowed(slots: &Slots, idx: usize) -> bool {
    slots[idx].overflowed.load(Ordering::Acquire) != 0
}

pub fn poll_message(slots: &Slots, idx: usize) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    poll_slot(&slots[idx])
}

fn poll_slot(slot: &PubsubSlot) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let head = slot.head.load(Ordering::Relaxed);
    let tail = slot.tail.load(Ordering::Acquire);
    if head == tail {
        return None;
    }
    // SAFETY: rule 2 — `head` is inside `head..tail`, published by the
    // producer's Release store of `tail` that our Acquire load saw, so the
    // message is fully written, and the producer will not touch it again until
    // our store of `head` below moves past it.
    let msg = unsafe { &*slot.ring[(head as usize) % PUBSUB_RING_CAP].get() };
    let channel = msg.channel[..msg.channel_len as usize].to_vec();
    let pattern = msg.pattern[..msg.pattern_len as usize].to_vec();
    let payload = msg.payload[..msg.payload_len as usize].to_vec();
    slot.head.store(head.wrapping_add(1), Ordering::Release);
    Some((channel, pattern, payload))
}

/// Every in-use slot's subscriptions, visited under the lock.
fn each_subs(ctl: &PubsubCtl, slots: &Slots, mut visit: impl FnMut(&Subs)) {
    let mut locked = Locked::new(ctl, slots);
    for i in 0..MAX_PUBSUB_SLOTS {
        let subs = locked.subs(i);
        if subs.in_use != 0 {
            visit(subs);
        }
    }
}

/// PUBSUB CHANNELS [pattern]
pub fn pubsub_channels(
    ctl: &PubsubCtl,
    slots: &Slots,
    pattern: Option<&[u8]>,
    shard: bool,
) -> Vec<Vec<u8>> {
    let mut names: Vec<Vec<u8>> = Vec::new();
    each_subs(ctl, slots, |subs| {
        for entry in subs.channels[..subs.channel_count as usize].iter() {
            let name = &entry[..entry_len(entry)];
            // `shard` lists the sharded namespace by the names clients used;
            // otherwise internal channels are hidden.
            let listed = match (shard, name.strip_prefix(SHARD_PREFIX)) {
                (true, Some(shown)) => Some(shown),
                (false, None) if !is_internal(name) => Some(name),
                _ => None,
            };
            if let Some(name) = listed
                && pattern.is_none_or(|p| glob_match(p, name))
            {
                names.push(name.to_vec());
            }
        }
    });
    // Dedup outside the lock so sort cost doesn't extend the critical section
    names.sort_unstable();
    names.dedup();
    names
}

/// PUBSUB NUMSUB [channel ...]
pub fn pubsub_numsub(ctl: &PubsubCtl, slots: &Slots, channels: &[Vec<u8>]) -> Vec<i64> {
    let mut counts = vec![0i64; channels.len()];
    each_subs(ctl, slots, |subs| {
        for entry in subs.channels[..subs.channel_count as usize].iter() {
            for (k, target) in channels.iter().enumerate() {
                if entry_eq(entry, target) {
                    counts[k] += 1;
                }
            }
        }
    });
    counts
}

/// `PUBSUB NUMPAT`: the number of distinct patterns subscribed to — two
/// clients on `ch*` count once, as in Redis.
pub fn pubsub_numpat(ctl: &PubsubCtl, slots: &Slots) -> i64 {
    let mut patterns: Vec<Vec<u8>> = Vec::new();
    each_subs(ctl, slots, |subs| {
        for entry in subs.patterns[..subs.pattern_count as usize].iter() {
            patterns.push(entry[..entry_len(entry)].to_vec());
        }
    });
    patterns.sort_unstable();
    patterns.dedup();
    patterns.len() as i64
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

    /// The slot table is requested from shared memory at postmaster start
    /// whether or not anyone uses pub/sub, so these constants are a cost every
    /// deployment pays. Changing one should be a deliberate act.
    #[test]
    fn slot_table_stays_within_its_shared_memory_budget() {
        let bytes = pubsub_slots_size();
        assert!(
            bytes <= 80 * 1024 * 1024,
            "pub/sub slots would request {} MiB of shared memory",
            bytes / 1024 / 1024
        );
        // Its own limit, not memory mode's value cap: every slot reserves its
        // payload whether or not anyone publishes, so a 64 KiB cap would cost
        // the ring 128× this.
        assert_eq!(PUBSUB_MSG_LEN, 512);
        // One byte of every channel slot is reserved for the terminator.
        assert_eq!(MAX_CHANNEL_LEN, CHAN_LEN - 1);
    }

    #[test]
    fn a_full_ring_marks_its_slot_overflowed() {
        let slot = slot();
        assert_eq!(slot.overflowed.load(Ordering::Acquire), 0);
        for _ in 0..PUBSUB_RING_CAP {
            assert!(write_to_ring(&slot, b"ch", b"", b"m"));
        }
        assert_eq!(slot.overflowed.load(Ordering::Acquire), 0);
        // One more than the ring holds is dropped, and the slot says so.
        assert!(!write_to_ring(&slot, b"ch", b"", b"m"));
        assert_eq!(slot.overflowed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn a_client_gets_every_channel_beside_its_tracking_channel() {
        let mut arr = [[0u8; CHAN_LEN]; CHANNEL_ENTRIES];
        let mut count = 0u32;
        let own = crate::tracking::channel(7);
        assert!(slot_add(&mut arr, &mut count, &own));
        for i in 0..MAX_SUBS_PER_SLOT {
            assert!(
                slot_add(&mut arr, &mut count, format!("c{i}").as_bytes()),
                "channel {i}"
            );
        }
        assert!(!slot_add(&mut arr, &mut count, b"one-too-many"));
        assert_eq!(count as usize, MAX_SUBS_PER_SLOT + 1);

        // The tracking channel fits after the client's own, too.
        let mut arr = [[0u8; CHAN_LEN]; CHANNEL_ENTRIES];
        let mut count = 0u32;
        for i in 0..MAX_SUBS_PER_SLOT {
            assert!(slot_add(&mut arr, &mut count, format!("c{i}").as_bytes()));
        }
        assert!(slot_add(&mut arr, &mut count, &own));
    }

    #[test]
    fn a_channel_name_at_the_limit_round_trips() {
        let mut arr = [[0u8; CHAN_LEN]; MAX_SUBS_PER_SLOT];
        let mut count = 0u32;
        let name = vec![b'c'; MAX_CHANNEL_LEN];

        assert!(slot_add(&mut arr, &mut count, &name));
        assert_eq!(count, 1);
        assert!(entry_eq(&arr[0], &name), "a stored name must match itself");
        assert_eq!(slot_names(&arr, count), vec![name.clone()]);

        // Subscribing twice to the same channel is a no-op, not a second entry.
        assert!(!slot_add(&mut arr, &mut count, &name));
        assert_eq!(count, 1);
    }

    /// One slot on the heap, as the postmaster would create it: zeroed.
    fn slot() -> std::sync::Arc<PubsubSlot> {
        // SAFETY: zero is a valid `PubsubSlot` — a free slot with an empty
        // ring (see its `Shareable` impl).
        std::sync::Arc::new(unsafe { std::mem::MaybeUninit::<PubsubSlot>::zeroed().assume_init() })
    }

    /// A publish must wake a waiting subscriber rather than leaving it to
    /// discover the message on the next timeout.
    #[test]
    fn a_publish_wakes_a_waiting_subscriber() {
        let slot = slot();
        let publisher = {
            let slot = std::sync::Arc::clone(&slot);
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(100));
                write_to_ring(&slot, b"chan", b"", b"payload");
            })
        };

        let start = std::time::Instant::now();
        // A ten-second timeout expected to return in ~100 ms: if the
        // wakeup were lost this test would take the full timeout.
        wait_on(&slot, 0, std::time::Duration::from_secs(10));
        let waited = start.elapsed();
        publisher.join().unwrap();

        assert!(
            waited < std::time::Duration::from_secs(5),
            "waited {waited:?}; the publish should have woken the subscriber"
        );
        assert_eq!(
            poll_slot(&slot),
            Some((b"chan".to_vec(), Vec::new(), b"payload".to_vec()))
        );
    }

    /// A publish that lands before the wait starts must not be slept through.
    #[test]
    fn a_publish_already_in_the_ring_does_not_block() {
        let slot = slot();
        write_to_ring(&slot, b"chan", b"", b"payload");

        let start = std::time::Instant::now();
        // seen_tail = 0, but the publish already moved it to 1.
        wait_on(&slot, 0, std::time::Duration::from_secs(10));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "a stale snapshot must return at once, not sleep"
        );
    }

    /// With no publisher at all the wait is bounded by its timeout, so a lost
    /// wakeup can only ever cost latency — never a hung subscriber.
    #[test]
    fn a_missed_wakeup_degrades_to_the_timeout() {
        let slot = slot();
        let start = std::time::Instant::now();
        wait_on(&slot, 0, std::time::Duration::from_millis(150));
        let waited = start.elapsed();
        assert!(
            waited >= std::time::Duration::from_millis(100),
            "returned after {waited:?}, expected to wait out the timeout"
        );
        assert!(
            waited < std::time::Duration::from_secs(2),
            "waited {waited:?}"
        );
    }

    /// The ring holds `PUBSUB_RING_CAP` messages and refuses the next, and
    /// hands them back in order.
    #[test]
    fn the_ring_is_bounded_and_ordered() {
        let slot = slot();
        for i in 0..PUBSUB_RING_CAP {
            assert!(write_to_ring(&slot, b"c", b"", i.to_string().as_bytes()));
        }
        assert!(!write_to_ring(&slot, b"c", b"", b"one too many"));
        for i in 0..PUBSUB_RING_CAP {
            assert_eq!(
                poll_slot(&slot).map(|(_, _, p)| p),
                Some(i.to_string().into_bytes())
            );
        }
        assert_eq!(poll_slot(&slot), None);
    }

    #[test]
    fn entry_eq_matches_nul_terminated_entry() {
        let mut entry = [0u8; CHAN_LEN];
        entry[..5].copy_from_slice(b"news.");
        assert!(entry_eq(&entry, b"news."));
        assert!(!entry_eq(&entry, b"news"));
        assert!(!entry_eq(&entry, b"news.x"));
        // A name too long to have been stored can never match.
        assert!(!entry_eq(&entry, &[b'a'; CHAN_LEN]));
    }
}

/// Safe entry points over the shared pub/sub state, for callers that run a
/// pub/sub command off the connection thread — a script's `redis.call`.
/// `None` when the extension was not preloaded and there is no state.
pub mod shared {
    /// What `publish` did: the receivers reached, and the `(schema, table)`
    /// the channel is routed to, if any.
    pub type Published = (i64, Option<(Vec<u8>, Vec<u8>)>);

    /// `PUBLISH`: the receivers reached, and the table the channel is routed
    /// to, if any, so the caller can insert the message there.
    pub fn publish(channel: &[u8], message: &[u8]) -> Option<Published> {
        let (ctl, slots) = crate::pubsub_state()?;
        let n = super::publish(ctl, slots, channel, message);
        let route =
            crate::route_state().and_then(|route_ctl| super::route_lookup(route_ctl, channel));
        Some((n, route))
    }

    pub fn channels(pattern: Option<&[u8]>, shard: bool) -> Option<Vec<Vec<u8>>> {
        let (ctl, slots) = crate::pubsub_state()?;
        Some(super::pubsub_channels(ctl, slots, pattern, shard))
    }

    pub fn numsub(channels: &[Vec<u8>]) -> Option<Vec<i64>> {
        let (ctl, slots) = crate::pubsub_state()?;
        Some(super::pubsub_numsub(ctl, slots, channels))
    }

    pub fn numpat() -> Option<i64> {
        let (ctl, slots) = crate::pubsub_state()?;
        Some(super::pubsub_numpat(ctl, slots))
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
