use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

const MAX_PUBSUB_SLOTS: usize = 256;
const PUBSUB_RING_CAP: usize = 256;
const MAX_SUBS_PER_SLOT: usize = 16;
/// Maximum PUBLISH payload. Matches `mem::MAX_TOTAL_VAL_LEN` so a value that
/// fits in memory mode also fits through pub/sub.
pub const PUBSUB_MSG_LEN: usize = 512;
/// Maximum channel or pattern name. One byte is reserved for the NUL
/// terminator, so `MAX_CHANNEL_LEN` is what callers may actually use.
const CHAN_LEN: usize = 256;
pub const MAX_CHANNEL_LEN: usize = CHAN_LEN - 1;

const MAX_ROUTES: usize = 64;
const ROUTE_SCHEMA_LEN: usize = 64;
const ROUTE_TABLE_LEN: usize = 64;

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
    pub lock: AtomicU8,
    pub initialised: AtomicU8,
    pub route_count: AtomicU8,
    pub _pad: [u8; 5],
    pub entries: [RouteEntry; MAX_ROUTES],
}

pub fn route_ctl_size() -> usize {
    std::mem::size_of::<RouteCtl>()
}

/// RAII guard for the cross-process `AtomicU8` spinlocks in this module.
///
/// Releasing in `Drop` rather than at each exit point matters for more than
/// tidiness: these locks are plain shared-memory bytes with no owner tracking,
/// so a thread that unwound while holding one would wedge every pub/sub
/// operation in every worker process, permanently, at 100% CPU.
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

/// Header block: global spinlock protecting subscription map changes and PUBLISH scans.
/// AtomicU8 CAS works correctly across Postgres BGW processes sharing this physical memory
/// page — no pg_sys calls required, safe from any thread.
#[repr(C)]
pub struct PubsubCtl {
    pub lock: AtomicU8,
    pub _pad: [u8; 7],
}

#[repr(C)]
pub struct PubsubMsg {
    pub channel: [u8; CHAN_LEN],
    pub channel_len: u16,
    pub pattern_len: u16,
    pub payload_len: u32,
    pub payload: [u8; PUBSUB_MSG_LEN],
    pub pattern: [u8; CHAN_LEN],
}

#[repr(C)]
pub struct PubsubSlot {
    pub channels: [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
    pub patterns: [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
    pub channel_count: u32,
    pub pattern_count: u32,
    pub in_use: u8,
    pub _pad: [u8; 3],
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub ring: [PubsubMsg; PUBSUB_RING_CAP],
}

unsafe impl Send for PubsubCtl {}
unsafe impl Sync for PubsubCtl {}
unsafe impl Send for PubsubSlot {}
unsafe impl Sync for PubsubSlot {}

pub fn pubsub_ctl_size() -> usize {
    std::mem::size_of::<PubsubCtl>()
}

/// Shared memory the slot table occupies — ~66 MiB at the current constants,
/// requested whether or not pub/sub is used. Raising `CHAN_LEN`,
/// `PUBSUB_MSG_LEN` or `PUBSUB_RING_CAP` is a real cost to every deployment.
pub fn pubsub_slots_size() -> usize {
    std::mem::size_of::<PubsubSlot>() * MAX_PUBSUB_SLOTS
}

pub unsafe fn route_add(ctl: *mut RouteCtl, channel: &[u8], schema: &[u8], table: &[u8]) -> bool {
    unsafe {
        let ch_len = channel.len().min(CHAN_LEN - 1);
        let sc_len = schema.len().min(ROUTE_SCHEMA_LEN - 1);
        let tb_len = table.len().min(ROUTE_TABLE_LEN - 1);

        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let mut free_idx = None;
        for i in 0..MAX_ROUTES {
            let e = &mut (*ctl).entries[i];
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
        let e = &mut (*ctl).entries[idx];
        e.channel[..ch_len].copy_from_slice(&channel[..ch_len]);
        e.channel_len = ch_len as u16;
        e.schema[..sc_len].copy_from_slice(&schema[..sc_len]);
        e.schema_len = sc_len as u16;
        e.table[..tb_len].copy_from_slice(&table[..tb_len]);
        e.table_len = tb_len as u16;
        e.active = 1;
        (*ctl).route_count.fetch_add(1, Ordering::Relaxed);
        true
    }
}

pub unsafe fn route_remove(ctl: *mut RouteCtl, channel: &[u8]) -> bool {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        for i in 0..MAX_ROUTES {
            let e = &mut (*ctl).entries[i];
            let ch_len = e.channel_len as usize;
            if e.active != 0 && ch_len == channel.len() && e.channel[..ch_len] == *channel {
                e.active = 0;
                (*ctl).route_count.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
        }
        false
    }
}

pub unsafe fn route_lookup(ctl: *mut RouteCtl, channel: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    unsafe {
        if (*ctl).route_count.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        (0..MAX_ROUTES).find_map(|i| {
            let e = &(*ctl).entries[i];
            let ch_len = e.channel_len as usize;
            (e.active != 0 && ch_len == channel.len() && e.channel[..ch_len] == *channel).then(
                || {
                    (
                        e.schema[..e.schema_len as usize].to_vec(),
                        e.table[..e.table_len as usize].to_vec(),
                    )
                },
            )
        })
    }
}

unsafe fn slot_reset(slot: *mut PubsubSlot) {
    unsafe {
        (*slot).in_use = 0;
        (*slot).channel_count = 0;
        (*slot).pattern_count = 0;
        (*slot).head.store(0, Ordering::Relaxed);
        (*slot).tail.store(0, Ordering::Relaxed);
    }
}

/// Length of a NUL-terminated fixed-size slot entry.
fn entry_len(entry: &[u8; CHAN_LEN]) -> usize {
    entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN)
}

/// Compare a NUL-terminated slot entry against `value`.
///
/// Deliberately avoids locating the terminator first: PUBLISH runs this against
/// every subscription of every slot, and the overwhelmingly common case
/// mismatches on the first byte.
fn entry_eq(entry: &[u8; CHAN_LEN], value: &[u8]) -> bool {
    value.len() < CHAN_LEN && entry[..value.len()] == *value && entry[value.len()] == 0
}

unsafe fn slot_add(
    arr: &mut [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
    count: &mut u32,
    value: &[u8],
) -> bool {
    let n = *count as usize;
    if arr[..n].iter().any(|entry| entry_eq(entry, value)) {
        return false;
    }
    if n >= MAX_SUBS_PER_SLOT {
        return false;
    }
    let copy_len = value.len().min(CHAN_LEN - 1);
    arr[n][..copy_len].copy_from_slice(&value[..copy_len]);
    arr[n][copy_len] = 0;
    *count += 1;
    true
}

unsafe fn slot_remove(
    arr: &mut [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
    count: &mut u32,
    value: &[u8],
) {
    let n = *count as usize;
    if let Some(i) = arr[..n].iter().position(|entry| entry_eq(entry, value)) {
        arr[i] = arr[n - 1];
        *count -= 1;
    }
}

unsafe fn slot_names(arr: &[[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT], count: u32) -> Vec<Vec<u8>> {
    arr[..count as usize]
        .iter()
        .map(|entry| entry[..entry_len(entry)].to_vec())
        .collect()
}

/// Acquire lock, mutate a slot, return counts. Frees the slot if both counts reach zero.
unsafe fn with_slot_locked<F>(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
    mutate: F,
) -> (u32, u32)
where
    F: FnOnce(&mut PubsubSlot),
{
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let slot = &mut *slots.add(idx);
        mutate(slot);
        let counts = (slot.channel_count, slot.pattern_count);
        if counts == (0, 0) {
            slot_reset(slot as *mut _);
        }
        counts
    }
}

/// Allocate a free slot and populate it in one lock acquisition.
/// `field_fn` selects which array/counter pair to populate.
/// Returns `None` if no slot is available.
unsafe fn slot_alloc_and_sub_inner<F>(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    items: &[Vec<u8>],
    field_fn: F,
) -> Option<(usize, u32)>
where
    F: Fn(
        *mut PubsubSlot,
    ) -> (
        &'static mut [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
        &'static mut u32,
    ),
{
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        (0..MAX_PUBSUB_SLOTS).find_map(|i| {
            let slot = slots.add(i);
            ((*slot).in_use == 0).then(|| {
                slot_reset(slot);
                (*slot).in_use = 1;
                let (arr, count) = field_fn(slot);
                for item in items {
                    slot_add(arr, count, item);
                }
                (i, *count)
            })
        })
    }
}

pub unsafe fn slot_alloc_and_subscribe(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    channels: &[Vec<u8>],
) -> Option<(usize, u32)> {
    unsafe {
        slot_alloc_and_sub_inner(ctl, slots, channels, |slot| {
            (&mut (*slot).channels, &mut (*slot).channel_count)
        })
    }
}

pub unsafe fn slot_alloc_and_psubscribe(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    patterns: &[Vec<u8>],
) -> Option<(usize, u32)> {
    unsafe {
        slot_alloc_and_sub_inner(ctl, slots, patterns, |slot| {
            (&mut (*slot).patterns, &mut (*slot).pattern_count)
        })
    }
}

pub unsafe fn slot_free(ctl: *mut PubsubCtl, slots: *mut PubsubSlot, idx: usize) {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let slot = slots.add(idx);
        if (*slot).in_use != 0 {
            slot_reset(slot);
        }
    }
}

pub unsafe fn subscribe(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
    channels: &[Vec<u8>],
) -> (u32, u32) {
    unsafe {
        with_slot_locked(ctl, slots, idx, |slot| {
            for ch in channels {
                slot_add(&mut slot.channels, &mut slot.channel_count, ch);
            }
        })
    }
}

pub unsafe fn unsubscribe(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
    channels: &[Vec<u8>],
) -> (u32, u32) {
    unsafe {
        with_slot_locked(ctl, slots, idx, |slot| {
            for ch in channels {
                slot_remove(&mut slot.channels, &mut slot.channel_count, ch);
            }
        })
    }
}

pub unsafe fn psubscribe(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
    patterns: &[Vec<u8>],
) -> (u32, u32) {
    unsafe {
        with_slot_locked(ctl, slots, idx, |slot| {
            for pat in patterns {
                slot_add(&mut slot.patterns, &mut slot.pattern_count, pat);
            }
        })
    }
}

pub unsafe fn punsubscribe(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
    patterns: &[Vec<u8>],
) -> (u32, u32) {
    unsafe {
        with_slot_locked(ctl, slots, idx, |slot| {
            for pat in patterns {
                slot_remove(&mut slot.patterns, &mut slot.pattern_count, pat);
            }
        })
    }
}

pub unsafe fn channel_names(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
) -> Vec<Vec<u8>> {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let slot = slots.add(idx);
        slot_names(&(*slot).channels, (*slot).channel_count)
    }
}

pub unsafe fn pattern_names(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
) -> Vec<Vec<u8>> {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let slot = slots.add(idx);
        slot_names(&(*slot).patterns, (*slot).pattern_count)
    }
}

pub unsafe fn publish(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    channel: &[u8],
    message: &[u8],
) -> i64 {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let mut count: i64 = 0;
        for i in 0..MAX_PUBSUB_SLOTS {
            let slot = slots.add(i);
            if (*slot).in_use == 0 {
                continue;
            }
            let ch_count = (*slot).channel_count as usize;
            if (&(*slot).channels)[..ch_count]
                .iter()
                .any(|entry| entry_eq(entry, channel))
                && write_to_ring(slot, channel, b"", message)
            {
                count += 1;
            }
            let pat_count = (*slot).pattern_count as usize;
            for entry in (&(*slot).patterns)[..pat_count].iter() {
                let pat = &entry[..entry_len(entry)];
                if glob_match(pat, channel) && write_to_ring(slot, channel, pat, message) {
                    count += 1;
                }
            }
        }
        count
    }
}

unsafe fn write_to_ring(
    slot: *mut PubsubSlot,
    channel: &[u8],
    pattern: &[u8],
    payload: &[u8],
) -> bool {
    unsafe {
        let head = (*slot).head.load(Ordering::Acquire);
        let tail = (*slot).tail.load(Ordering::Relaxed);
        if tail.wrapping_sub(head) >= PUBSUB_RING_CAP as u32 {
            return false;
        }
        let msg = &mut (*slot).ring[(tail as usize) % PUBSUB_RING_CAP];

        let ch_len = channel.len().min(CHAN_LEN - 1);
        msg.channel[..ch_len].copy_from_slice(&channel[..ch_len]);
        msg.channel[ch_len] = 0;
        msg.channel_len = ch_len as u16;

        let pat_len = pattern.len().min(CHAN_LEN - 1);
        msg.pattern[..pat_len].copy_from_slice(&pattern[..pat_len]);
        msg.pattern[pat_len] = 0;
        msg.pattern_len = pat_len as u16;

        // Callers reject oversized payloads before reaching here; the clamp is a
        // belt-and-braces bound on the memcpy, not the enforcement point.
        let pay_len = payload.len().min(PUBSUB_MSG_LEN);
        msg.payload[..pay_len].copy_from_slice(&payload[..pay_len]);
        msg.payload_len = pay_len as u32;

        (*slot).tail.store(tail + 1, Ordering::Release);
        wake_subscriber(slot);
        true
    }
}

/// Current ring position, for a subscriber to pass to [`wait_for_message`].
pub unsafe fn current_tail(slots: *mut PubsubSlot, idx: usize) -> u32 {
    unsafe { (*slots.add(idx)).tail.load(Ordering::Acquire) }
}

/// Block until this slot's ring advances past `seen_tail`, or `timeout` elapses.
///
/// Waiting on the tail counter rather than polling the socket is what makes
/// delivery immediate: a subscriber that discovered messages by letting a 5 ms
/// read time out would cost ~200 wakeups per second while idle and still delay
/// delivery by up to 5 ms. Here the timeout does nothing but bound how long a
/// client command sits unread.
///
/// The futex lives in PostgreSQL's shared memory segment, so the shared (not
/// `PRIVATE`) operations are required: the waiter and the waker are different
/// processes. A missed wakeup only costs one `timeout`, never a hang.
///
/// # Safety
/// `slots` must point at the initialised slot array and `idx` be in range.
#[cfg(target_os = "linux")]
pub unsafe fn wait_for_message(
    slots: *mut PubsubSlot,
    idx: usize,
    seen_tail: u32,
    timeout: std::time::Duration,
) {
    const FUTEX_WAIT: libc::c_int = 0;
    unsafe {
        let addr = std::ptr::addr_of!((*slots.add(idx)).tail) as *const u32;
        let deadline = libc::timespec {
            tv_sec: timeout.as_secs() as libc::time_t,
            tv_nsec: timeout.subsec_nanos() as libc::c_long,
        };
        // Returns EAGAIN immediately if tail already moved — exactly the race we
        // need handled, since a publish between the drain and this call must not
        // be slept through.
        libc::syscall(libc::SYS_futex, addr, FUTEX_WAIT, seen_tail, &deadline);
    }
}

#[cfg(target_os = "linux")]
unsafe fn wake_subscriber(slot: *mut PubsubSlot) {
    const FUTEX_WAKE: libc::c_int = 1;
    unsafe {
        let addr = std::ptr::addr_of!((*slot).tail) as *const u32;
        libc::syscall(libc::SYS_futex, addr, FUTEX_WAKE, 1i32);
    }
}

/// Fallback for platforms without futex: sleep briefly and let the caller
/// re-check, i.e. the polling behaviour this replaces on Linux.
#[cfg(not(target_os = "linux"))]
pub unsafe fn wait_for_message(
    _slots: *mut PubsubSlot,
    _idx: usize,
    _seen_tail: u32,
    timeout: std::time::Duration,
) {
    std::thread::sleep(timeout.min(std::time::Duration::from_millis(5)));
}

#[cfg(not(target_os = "linux"))]
unsafe fn wake_subscriber(_slot: *mut PubsubSlot) {}

/// Lock-free ring poll — safe from any thread, no spinlock needed.
pub unsafe fn poll_message(
    slots: *mut PubsubSlot,
    idx: usize,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    unsafe {
        let slot = slots.add(idx);
        let head = (*slot).head.load(Ordering::Relaxed);
        let tail = (*slot).tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let msg = &(*slot).ring[(head as usize) % PUBSUB_RING_CAP];
        let channel = msg.channel[..msg.channel_len as usize].to_vec();
        let pattern = msg.pattern[..msg.pattern_len as usize].to_vec();
        let payload = msg.payload[..msg.payload_len as usize].to_vec();
        (*slot).head.store(head + 1, Ordering::Release);
        Some((channel, pattern, payload))
    }
}

/// PUBSUB CHANNELS [pattern]
pub unsafe fn pubsub_channels(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    pattern: Option<&[u8]>,
) -> Vec<Vec<u8>> {
    unsafe {
        let mut names: Vec<Vec<u8>> = Vec::new();
        {
            let _guard = SpinGuard::acquire(&(*ctl).lock);
            for i in 0..MAX_PUBSUB_SLOTS {
                let slot = slots.add(i);
                if (*slot).in_use == 0 {
                    continue;
                }
                let ch_count = (*slot).channel_count as usize;
                for entry in (&(*slot).channels)[..ch_count].iter() {
                    let name = &entry[..entry_len(entry)];
                    if pattern.is_none_or(|p| glob_match(p, name)) {
                        names.push(name.to_vec());
                    }
                }
            }
        }
        // Dedup outside the lock so sort cost doesn't extend the critical section
        names.sort_unstable();
        names.dedup();
        names
    }
}

/// PUBSUB NUMSUB [channel ...]
pub unsafe fn pubsub_numsub(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    channels: &[Vec<u8>],
) -> Vec<i64> {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        let mut counts = vec![0i64; channels.len()];
        for i in 0..MAX_PUBSUB_SLOTS {
            let slot = slots.add(i);
            if (*slot).in_use == 0 {
                continue;
            }
            let ch_count = (*slot).channel_count as usize;
            for entry in (&(*slot).channels)[..ch_count].iter() {
                for (k, target) in channels.iter().enumerate() {
                    if entry_eq(entry, target) {
                        counts[k] += 1;
                    }
                }
            }
        }
        counts
    }
}

/// PUBSUB NUMPAT
pub unsafe fn pubsub_numpat(ctl: *mut PubsubCtl, slots: *mut PubsubSlot) -> i64 {
    unsafe {
        let _guard = SpinGuard::acquire(&(*ctl).lock);
        (0..MAX_PUBSUB_SLOTS)
            .map(|i| {
                let slot = slots.add(i);
                if (*slot).in_use != 0 {
                    (*slot).pattern_count as i64
                } else {
                    0
                }
            })
            .sum()
    }
}

/// Redis-compatible glob match over `*`, `?` and `[...]` classes.
///
/// Iterative O(n×m) backtracking — no recursion, and indices rather than
/// reslicing so no input can drive a slice out of bounds. A panic here would be
/// fatal: `publish` calls this while holding the pub/sub spinlock.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
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
        if c == b']' {
            return (matched != negate, i + 1);
        }
        if pat.len() - i > 2 && pat[i + 1] == b'-' {
            matched |= ch >= c && ch <= pat[i + 2];
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
        (b"*", b"", true),
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
        // The pub/sub payload limit is its own, deliberately not memory
        // mode's value cap: every slot in this ring reserves its payload
        // whether or not anyone publishes, so tracking a 64 KiB cap would cost
        // the ring 128× this. An over-long PUBLISH is refused in `worker.rs`,
        // naming this limit.
        assert_eq!(PUBSUB_MSG_LEN, 512);
        // One byte of every channel slot is reserved for the terminator.
        assert_eq!(MAX_CHANNEL_LEN, CHAN_LEN - 1);
    }

    #[test]
    fn a_channel_name_at_the_limit_round_trips() {
        let mut arr = [[0u8; CHAN_LEN]; MAX_SUBS_PER_SLOT];
        let mut count = 0u32;
        let name = vec![b'c'; MAX_CHANNEL_LEN];

        assert!(unsafe { slot_add(&mut arr, &mut count, &name) });
        assert_eq!(count, 1);
        assert!(entry_eq(&arr[0], &name), "a stored name must match itself");
        assert_eq!(unsafe { slot_names(&arr, count) }, vec![name.clone()]);

        // Subscribing twice to the same channel is a no-op, not a second entry.
        assert!(!unsafe { slot_add(&mut arr, &mut count, &name) });
        assert_eq!(count, 1);
    }

    /// Backing store for a slot, aligned for the atomics inside it.
    struct SlotBuf(#[allow(dead_code)] Vec<u64>, *mut PubsubSlot);
    // The pointer addresses a heap buffer owned by the test and is only ever
    // touched while that buffer is alive.
    unsafe impl Send for SlotBuf {}

    fn slot_buf() -> SlotBuf {
        let words = std::mem::size_of::<PubsubSlot>().div_ceil(8);
        let mut buf = vec![0u64; words];
        let ptr = buf.as_mut_ptr() as *mut PubsubSlot;
        unsafe { (*ptr).in_use = 1 };
        SlotBuf(buf, ptr)
    }

    /// A publish must wake a waiting subscriber rather than leaving it to
    /// discover the message on the next timeout.
    #[test]
    fn a_publish_wakes_a_waiting_subscriber() {
        let slot = slot_buf();
        let ptr = slot.1;

        let publisher = std::thread::spawn(move || {
            let slot = slot;
            std::thread::sleep(std::time::Duration::from_millis(100));
            unsafe { write_to_ring(slot.1, b"chan", b"", b"payload") };
            slot
        });

        let start = std::time::Instant::now();
        // A ten-second timeout expected to return in ~100 ms: if the
        // wakeup were lost this test would take the full timeout.
        unsafe { wait_for_message(ptr, 0, 0, std::time::Duration::from_secs(10)) };
        let waited = start.elapsed();
        let slot = publisher.join().unwrap();

        assert!(
            waited < std::time::Duration::from_secs(5),
            "waited {waited:?}; the publish should have woken the subscriber"
        );
        let message = unsafe { poll_message(slot.1, 0) };
        assert_eq!(
            message,
            Some((b"chan".to_vec(), Vec::new(), b"payload".to_vec()))
        );
    }

    /// A publish that lands before the wait starts must not be slept through.
    #[test]
    fn a_publish_already_in_the_ring_does_not_block() {
        let slot = slot_buf();
        unsafe { write_to_ring(slot.1, b"chan", b"", b"payload") };

        let start = std::time::Instant::now();
        // seen_tail = 0, but the publish already moved it to 1.
        unsafe { wait_for_message(slot.1, 0, 0, std::time::Duration::from_secs(10)) };
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "a stale snapshot must return at once, not sleep"
        );
    }

    /// With no publisher at all the wait is bounded by its timeout, so a lost
    /// wakeup can only ever cost latency — never a hung subscriber.
    #[test]
    fn a_missed_wakeup_degrades_to_the_timeout() {
        let slot = slot_buf();
        let start = std::time::Instant::now();
        unsafe { wait_for_message(slot.1, 0, 0, std::time::Duration::from_millis(150)) };
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
