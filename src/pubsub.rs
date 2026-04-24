use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

pub const MAX_PUBSUB_SLOTS: usize = 256;
pub const PUBSUB_RING_CAP: usize = 256;
pub const MAX_SUBS_PER_SLOT: usize = 16;
pub const PUBSUB_MSG_LEN: usize = 128;
pub const CHAN_LEN: usize = 64;

pub const MAX_ROUTES: usize = 64;
pub const ROUTE_SCHEMA_LEN: usize = 64;
pub const ROUTE_TABLE_LEN: usize = 64;

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

// Works on any *mut T where T has a `lock: AtomicU8` field.
macro_rules! acquire_lock {
    ($ctl:expr) => {
        while (*$ctl)
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
    };
}

macro_rules! release_lock {
    ($ctl:expr) => {
        (*$ctl).lock.store(0, Ordering::Release);
    };
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

pub fn pubsub_slots_size() -> usize {
    std::mem::size_of::<PubsubSlot>() * MAX_PUBSUB_SLOTS
}

unsafe fn spin_acquire(ctl: *mut PubsubCtl) {
    unsafe {
        acquire_lock!(ctl);
    }
}

unsafe fn spin_release(ctl: *mut PubsubCtl) {
    unsafe {
        release_lock!(ctl);
    }
}

pub unsafe fn route_add(ctl: *mut RouteCtl, channel: &[u8], schema: &[u8], table: &[u8]) -> bool {
    unsafe {
        let ch_len = channel.len().min(CHAN_LEN - 1);
        let sc_len = schema.len().min(ROUTE_SCHEMA_LEN - 1);
        let tb_len = table.len().min(ROUTE_TABLE_LEN - 1);

        acquire_lock!(ctl);
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
                release_lock!(ctl);
                return true;
            }
        }
        if let Some(idx) = free_idx {
            let e = &mut (*ctl).entries[idx];
            e.channel[..ch_len].copy_from_slice(&channel[..ch_len]);
            e.channel_len = ch_len as u16;
            e.schema[..sc_len].copy_from_slice(&schema[..sc_len]);
            e.schema_len = sc_len as u16;
            e.table[..tb_len].copy_from_slice(&table[..tb_len]);
            e.table_len = tb_len as u16;
            e.active = 1;
            (*ctl).route_count.fetch_add(1, Ordering::Relaxed);
            release_lock!(ctl);
            return true;
        }
        release_lock!(ctl);
        false
    }
}

pub unsafe fn route_remove(ctl: *mut RouteCtl, channel: &[u8]) -> bool {
    unsafe {
        acquire_lock!(ctl);
        for i in 0..MAX_ROUTES {
            let e = &mut (*ctl).entries[i];
            if e.active == 0 {
                continue;
            }
            let ch_len = e.channel_len as usize;
            if ch_len == channel.len() && e.channel[..ch_len] == *channel {
                e.active = 0;
                (*ctl).route_count.fetch_sub(1, Ordering::Relaxed);
                release_lock!(ctl);
                return true;
            }
        }
        release_lock!(ctl);
        false
    }
}

pub unsafe fn route_lookup(ctl: *mut RouteCtl, channel: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    unsafe {
        if (*ctl).route_count.load(Ordering::Relaxed) == 0 {
            return None;
        }
        acquire_lock!(ctl);
        let mut result = None;
        for i in 0..MAX_ROUTES {
            let e = &(*ctl).entries[i];
            if e.active == 0 {
                continue;
            }
            let ch_len = e.channel_len as usize;
            if ch_len == channel.len() && e.channel[..ch_len] == *channel {
                result = Some((
                    e.schema[..e.schema_len as usize].to_vec(),
                    e.table[..e.table_len as usize].to_vec(),
                ));
                break;
            }
        }
        release_lock!(ctl);
        result
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

unsafe fn slot_add(
    arr: &mut [[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT],
    count: &mut u32,
    value: &[u8],
) -> bool {
    let n = *count as usize;
    for entry in arr[..n].iter() {
        let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
        if len == value.len() && entry[..len] == *value {
            return false;
        }
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
    for (i, entry) in arr[..n].iter().enumerate() {
        let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
        if len == value.len() && entry[..len] == *value {
            if i < n - 1 {
                arr[i] = arr[n - 1];
            }
            *count -= 1;
            return;
        }
    }
}

unsafe fn slot_names(arr: &[[u8; CHAN_LEN]; MAX_SUBS_PER_SLOT], count: u32) -> Vec<Vec<u8>> {
    arr[..count as usize]
        .iter()
        .map(|entry| {
            let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
            entry[..len].to_vec()
        })
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
        spin_acquire(ctl);
        let slot = &mut *slots.add(idx);
        mutate(slot);
        let counts = (slot.channel_count, slot.pattern_count);
        if counts == (0, 0) {
            slot_reset(slot as *mut _);
        }
        spin_release(ctl);
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
        spin_acquire(ctl);
        let mut result = None;
        for i in 0..MAX_PUBSUB_SLOTS {
            let slot = slots.add(i);
            if (*slot).in_use == 0 {
                slot_reset(slot);
                (*slot).in_use = 1;
                let (arr, count) = field_fn(slot);
                for item in items {
                    slot_add(arr, count, item);
                }
                result = Some((i, *count));
                break;
            }
        }
        spin_release(ctl);
        result
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
        spin_acquire(ctl);
        let slot = slots.add(idx);
        if (*slot).in_use != 0 {
            slot_reset(slot);
        }
        spin_release(ctl);
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
        spin_acquire(ctl);
        let slot = slots.add(idx);
        let names = slot_names(&(*slot).channels, (*slot).channel_count);
        spin_release(ctl);
        names
    }
}

pub unsafe fn pattern_names(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    idx: usize,
) -> Vec<Vec<u8>> {
    unsafe {
        spin_acquire(ctl);
        let slot = slots.add(idx);
        let names = slot_names(&(*slot).patterns, (*slot).pattern_count);
        spin_release(ctl);
        names
    }
}

pub unsafe fn publish(
    ctl: *mut PubsubCtl,
    slots: *mut PubsubSlot,
    channel: &[u8],
    message: &[u8],
) -> i64 {
    unsafe {
        spin_acquire(ctl);
        let mut count: i64 = 0;
        for i in 0..MAX_PUBSUB_SLOTS {
            let slot = slots.add(i);
            if (*slot).in_use == 0 {
                continue;
            }
            let ch_count = (*slot).channel_count as usize;
            'ch: for entry in (&(*slot).channels)[..ch_count].iter() {
                let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
                if len == channel.len() && entry[..len] == *channel {
                    if write_to_ring(slot, channel, b"", message) {
                        count += 1;
                    }
                    break 'ch;
                }
            }
            let pat_count = (*slot).pattern_count as usize;
            for entry in (&(*slot).patterns)[..pat_count].iter() {
                let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
                if glob_match(&entry[..len], channel)
                    && write_to_ring(slot, channel, &entry[..len], message)
                {
                    count += 1;
                }
            }
        }
        spin_release(ctl);
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

        // Payloads longer than PUBSUB_MSG_LEN (128 bytes) are truncated by design;
        // callers should keep messages short or increase PUBSUB_MSG_LEN at compile time.
        let pay_len = payload.len().min(PUBSUB_MSG_LEN);
        msg.payload[..pay_len].copy_from_slice(&payload[..pay_len]);
        msg.payload_len = pay_len as u32;

        (*slot).tail.store(tail + 1, Ordering::Release);
        true
    }
}

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
        spin_acquire(ctl);
        let mut names: Vec<Vec<u8>> = Vec::new();
        for i in 0..MAX_PUBSUB_SLOTS {
            let slot = slots.add(i);
            if (*slot).in_use == 0 {
                continue;
            }
            let ch_count = (*slot).channel_count as usize;
            for entry in (&(*slot).channels)[..ch_count].iter() {
                let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
                let name = entry[..len].to_vec();
                if pattern.is_none_or(|p| glob_match(p, &name)) {
                    names.push(name);
                }
            }
        }
        spin_release(ctl);
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
        spin_acquire(ctl);
        let mut counts = vec![0i64; channels.len()];
        for i in 0..MAX_PUBSUB_SLOTS {
            let slot = slots.add(i);
            if (*slot).in_use == 0 {
                continue;
            }
            let ch_count = (*slot).channel_count as usize;
            for entry in (&(*slot).channels)[..ch_count].iter() {
                let len = entry.iter().position(|&b| b == 0).unwrap_or(CHAN_LEN);
                for (k, target) in channels.iter().enumerate() {
                    if len == target.len() && entry[..len] == **target {
                        counts[k] += 1;
                    }
                }
            }
        }
        spin_release(ctl);
        counts
    }
}

/// PUBSUB NUMPAT
pub unsafe fn pubsub_numpat(ctl: *mut PubsubCtl, slots: *mut PubsubSlot) -> i64 {
    unsafe {
        spin_acquire(ctl);
        let count: i64 = (0..MAX_PUBSUB_SLOTS)
            .map(|i| {
                let slot = slots.add(i);
                if (*slot).in_use != 0 {
                    (*slot).pattern_count as i64
                } else {
                    0
                }
            })
            .sum();
        spin_release(ctl);
        count
    }
}

/// Redis-compatible glob match. Iterative O(n×m) — no stack growth for multiple `*` wildcards.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pat = pattern;
    let mut s = string;
    let mut star_pat: &[u8] = b"";
    let mut star_s: &[u8] = b"";
    let mut has_star = false;

    loop {
        match pat.first() {
            Some(&b'*') => {
                has_star = true;
                star_pat = &pat[1..];
                star_s = s;
                pat = &pat[1..];
            }
            Some(&b'?') => {
                if s.is_empty() {
                    if has_star {
                        pat = star_pat;
                        s = &star_s[1..];
                        star_s = s;
                        continue;
                    }
                    return false;
                }
                pat = &pat[1..];
                s = &s[1..];
            }
            Some(&b'[') => {
                if s.is_empty() {
                    if has_star {
                        pat = star_pat;
                        s = &star_s[1..];
                        star_s = s;
                        continue;
                    }
                    return false;
                }
                let (matched, rest) = match_class(&pat[1..], s[0]);
                if matched {
                    pat = rest;
                    s = &s[1..];
                } else if has_star && !star_s.is_empty() {
                    star_s = &star_s[1..];
                    s = star_s;
                    pat = star_pat;
                } else {
                    return false;
                }
            }
            Some(&c) => {
                if s.first() == Some(&c) {
                    pat = &pat[1..];
                    s = &s[1..];
                } else if has_star && !star_s.is_empty() {
                    star_s = &star_s[1..];
                    s = star_s;
                    pat = star_pat;
                } else {
                    return false;
                }
            }
            None => return s.is_empty() || (has_star && pat.is_empty()),
        }
    }
}

fn match_class(pat: &[u8], ch: u8) -> (bool, &[u8]) {
    let negate = pat.first() == Some(&b'^');
    let mut p = if negate { &pat[1..] } else { pat };
    let mut matched = false;
    loop {
        match p.first() {
            None | Some(&b']') => {
                return (matched != negate, &p[p.first().map_or(0, |_| 1)..]);
            }
            Some(&c) if p.len() > 2 && p[1] == b'-' => {
                if ch >= c && ch <= p[2] {
                    matched = true;
                }
                p = &p[3..];
            }
            Some(&c) => {
                if ch == c {
                    matched = true;
                }
                p = &p[1..];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_exact() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
    }

    #[test]
    fn glob_star() {
        assert!(glob_match(b"h*o", b"hello"));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"news.*", b"news.sports"));
        assert!(!glob_match(b"news.*", b"sports"));
    }

    #[test]
    fn glob_question() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn glob_class() {
        assert!(glob_match(b"[hH]ello", b"hello"));
        assert!(glob_match(b"[hH]ello", b"Hello"));
        assert!(!glob_match(b"[hH]ello", b"xello"));
    }

    #[test]
    fn glob_range() {
        assert!(glob_match(b"[a-z]ello", b"hello"));
        assert!(!glob_match(b"[a-z]ello", b"Hello"));
    }

    #[test]
    fn glob_negated() {
        assert!(glob_match(b"[^0-9]ello", b"hello"));
        assert!(!glob_match(b"[^0-9]ello", b"1ello"));
    }

    #[test]
    fn glob_star_multi() {
        assert!(glob_match(b"*a*b*", b"xaxb"));
        assert!(!glob_match(b"*a*b*", b"xbxa"));
    }
}
