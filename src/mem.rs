use crate::htab::SharedTable;
use pgrx::pg_sys;
use std::ffi::c_void;
use std::ptr::addr_of;
use std::ptr::addr_of_mut;

// Maximum key length (null-terminated string fits in HTAB key). Keys are the
// thing applications namespace and make long, so this is generous.
pub const MAX_KEY_LEN: usize = 512;
// Maximum hash field / set member / sorted-set member length. Kept smaller than
// MAX_KEY_LEN on purpose: the hash, set and zset entries each store a key *and*
// a member, so this length is multiplied across the largest tables.
pub const MAX_MEMBER_LEN: usize = 128;
// HTAB key for the hash/set/zset tables: the redis key followed by the member.
/// Composite tables index on a 128-bit keyed hash of the Redis key rather than
/// the key itself.
///
/// A hash, a set, a sorted set and a list all store one entry per member, and
/// each entry carried a full `[u8; MAX_KEY_LEN]` copy of the key it belongs to
/// — 512 of a `SetEntry`'s 640 bytes, repeated for every member. Nothing ever
/// needed those bytes back: every composite-table access is a comparison
/// against a key the caller already holds, and the only paths that hand a key
/// to a client (`KEYS`, `SCAN`, `RANDOMKEY`) read the KV table, which still
/// stores keys verbatim.
///
/// Keyed with per-postmaster random material, so a client cannot craft two keys
/// that collide and merge their contents.
pub const KEY_HASH_LEN: usize = 16;
pub type KeyHash = [u8; KEY_HASH_LEN];

pub const COMPOSITE_KEY_LEN: usize = KEY_HASH_LEN + MAX_MEMBER_LEN;
// HTAB key for the list tables: the redis key followed by the 8-byte position.
pub const LIST_KEY_LEN: usize = KEY_HASH_LEN + 8;
// Inline value bytes stored directly in the main HTAB entry.
pub const INLINE_VAL_LEN: usize = 64;
// Maximum total value size accepted (inline + overflow).
pub const MAX_TOTAL_VAL_LEN: usize = 512;
// Overflow tail stored in the secondary overflow HTAB (bytes beyond INLINE_VAL_LEN).
pub const OVERFLOW_VAL_LEN: usize = MAX_TOTAL_VAL_LEN - INLINE_VAL_LEN;
// Number of even databases: 0,2,4,6,8,10,12,14 → indices 0..7
/// One shared-memory database per entry in the ephemeral half; `db` indexes
/// these directly.
pub const NUM_MEM_DBS: usize = crate::commands::DURABLE_FROM as usize;
// Step between list positions for LPUSH/RPUSH.
pub const LIST_POS_STEP: i64 = 1024;

fn htab_init_size() -> i64 {
    crate::MEM_MAX_ENTRIES.get() as i64
}

fn htab_init_size_small() -> i64 {
    (crate::MEM_MAX_ENTRIES.get() / 2).max(256) as i64
}

/// Fixed-size entry stored in the HTAB shared memory hash table.
/// The key field MUST be first — HTAB uses keysize bytes from the start.
#[repr(C)]
pub struct KvEntry {
    /// Null-terminated key (up to MAX_KEY_LEN - 1 chars). This is the HTAB lookup key.
    pub key: [u8; MAX_KEY_LEN],
    /// Inline value bytes (not null-terminated). Holds first INLINE_VAL_LEN bytes.
    pub value: [u8; INLINE_VAL_LEN],
    /// Total value length (may exceed INLINE_VAL_LEN if has_overflow == 1).
    pub value_len: u32,
    /// Expiry: microseconds since Unix epoch; 0 = no expiry.
    pub expires_at: i64,
    /// 1 if tail bytes are in kv_overflow_htab, 0 = fully inline.
    pub has_overflow: u8,
    _pad: [u8; 3],
}

/// Overflow tail for KvEntry. Key is the same string key as KvEntry.
#[repr(C)]
pub struct KvOverflow {
    pub key: [u8; MAX_KEY_LEN],
    pub value: [u8; OVERFLOW_VAL_LEN],
}

/// Fixed-size entry for the hash HTAB. Key is (redis_key[128], field[128]).
#[repr(C)]
pub struct HashEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub field: [u8; MAX_MEMBER_LEN],
    pub value: [u8; INLINE_VAL_LEN],
    pub value_len: u32,
    pub has_overflow: u8,
    _pad: [u8; 3],
}

/// Overflow tail for HashEntry. Composite key is key[128] + field[128].
#[repr(C)]
pub struct HashOverflow {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub field: [u8; MAX_MEMBER_LEN],
    pub value: [u8; OVERFLOW_VAL_LEN],
}

/// Fixed-size entry for the set HTAB. Key is (redis_key[128], member[128]).
#[repr(C)]
pub struct SetEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub member: [u8; MAX_MEMBER_LEN],
}

/// Fixed-size entry for the sorted set HTAB. Key is (redis_key[128], member[128]).
#[repr(C)]
pub struct ZsetEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub member: [u8; MAX_MEMBER_LEN],
    pub score: f64,
}

/// Fixed-size entry for the list HTAB. Key is (redis_key[128], pos_bytes[8]).
#[repr(C)]
pub struct ListEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub pos_bytes: [u8; 8],
    pub value: [u8; INLINE_VAL_LEN],
    pub value_len: u32,
    pub has_overflow: u8,
    _pad: [u8; 3],
}

/// Overflow tail for ListEntry. Key is key[128] + pos_bytes[8] = 136 bytes.
#[repr(C)]
pub struct ListOverflow {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub pos_bytes: [u8; 8],
    pub value: [u8; OVERFLOW_VAL_LEN],
}

/// Metadata entry for the list meta HTAB. Key is redis_key[128].
/// Tracks min/max position and count for O(1) LPUSH/RPUSH/LPOP/RPOP.
#[repr(C)]
pub struct ListMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub min_pos: i64,
    pub max_pos: i64,
    pub count: i64,
}

/// Metadata entry for the sorted set meta HTAB. Key is redis_key[128].
/// Tracks min/max score and members for O(1) ZPOPMIN/ZPOPMAX/ZCARD.
#[repr(C)]
pub struct ZsetMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub count: i64,
    pub min_score: f64,
    pub max_score: f64,
    pub min_member: [u8; MAX_MEMBER_LEN],
    pub max_member: [u8; MAX_MEMBER_LEN],
    pub min_member_len: u16,
    pub max_member_len: u16,
}

/// Metadata entry for the set meta HTAB. Key is redis_key[128].
/// Tracks count for O(1) SCARD/SPOP.
#[repr(C)]
pub struct SetMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub count: i64,
}

/// Control block in static shared memory.
#[repr(C)]
pub struct MemControlBlock {
    /// SipHash key for `key_hash`, drawn from `pg_strong_random` once per
    /// postmaster. Shared memory does not survive a restart, so neither does
    /// this, and it never has to match anything on disk.
    pub hash_key: [u64; 4],
    /// One LWLock per even-database — operations on db 0 and db 2 never block each other.
    pub lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub hash_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub set_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub zset_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub list_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    /// Handles to the 8 HTAB tables (one per ephemeral db).
    pub htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub hash_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub set_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub zset_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub list_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub list_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub zset_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub set_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    /// Overflow HTABs for tiered value storage (values > INLINE_VAL_LEN).
    pub kv_overflow_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub hash_overflow_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub list_overflow_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
}

// Safety: MemControlBlock lives in Postgres shared memory; all interior raw pointers
// (HTABs, LWLocks) are set once during shmem_startup_hook and every mutation of the
// tables they reference is serialized via the per-db LWLocks.
unsafe impl Send for MemControlBlock {}
unsafe impl Sync for MemControlBlock {}

// Thread-local per-bgworker references (set once in mem_init_worker).
thread_local! {
    static CTL_PTR: std::cell::Cell<*mut MemControlBlock> =
        const { std::cell::Cell::new(std::ptr::null_mut()) };
}

fn ctl() -> *mut MemControlBlock {
    CTL_PTR.with(|c| c.get())
}

/// Called once per bgworker after BackgroundWorkerInitializeConnection.
/// Attaches the thread-local CTL_PTR to the shared MemControlBlock.
/// The HTAB tables were already created in shmem_startup_hook; this just
/// caches the pointer for fast per-call access without going through SHMEM_CTL.
///
/// # Safety
/// Must be called from the bgworker main thread with a valid ctl pointer.
pub unsafe fn mem_init_worker(ctl_ptr: *mut MemControlBlock) {
    CTL_PTR.with(|c| c.set(ctl_ptr));
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

unsafe fn entry_is_expired(entry: *const KvEntry) -> bool {
    let exp = unsafe { (*entry).expires_at };
    exp != 0 && exp <= now_micros()
}

// ─────────────────────────── Composite key hashing ──────────────────────────

/// SipHash-2-4 over `msg`, keyed by `(k0, k1)`.
///
/// Composite keys are attacker-supplied, so the hash has to resist someone
/// deliberately constructing a collision to merge two keys' members. FNV — used
/// in `watch.rs`, where a collision only costs a spurious abort — would not.
fn siphash24(k0: u64, k1: u64, msg: &[u8]) -> u64 {
    let mut v0 = 0x736f6d6570736575 ^ k0;
    let mut v1 = 0x646f72616e646f6d ^ k1;
    let mut v2 = 0x6c7967656e657261 ^ k0;
    let mut v3 = 0x7465646279746573 ^ k1;

    macro_rules! round {
        () => {
            v0 = v0.wrapping_add(v1);
            v1 = v1.rotate_left(13);
            v1 ^= v0;
            v0 = v0.rotate_left(32);
            v2 = v2.wrapping_add(v3);
            v3 = v3.rotate_left(16);
            v3 ^= v2;
            v0 = v0.wrapping_add(v3);
            v3 = v3.rotate_left(21);
            v3 ^= v0;
            v2 = v2.wrapping_add(v1);
            v1 = v1.rotate_left(17);
            v1 ^= v2;
            v2 = v2.rotate_left(32);
        };
    }

    let mut chunks = msg.chunks_exact(8);
    for c in &mut chunks {
        let m = u64::from_le_bytes(c.try_into().unwrap());
        v3 ^= m;
        round!();
        round!();
        v0 ^= m;
    }
    // Final block: trailing bytes, with the length in the top byte.
    let mut b = (msg.len() as u64 & 0xff) << 56;
    for (i, &byte) in chunks.remainder().iter().enumerate() {
        b |= (byte as u64) << (8 * i);
    }
    v3 ^= b;
    round!();
    round!();
    v0 ^= b;

    v2 ^= 0xff;
    round!();
    round!();
    round!();
    round!();
    v0 ^ v1 ^ v2 ^ v3
}

/// Key material used only when shared memory is not attached — unit tests that
/// exercise the key helpers without a postmaster. No table exists in that case,
/// so nothing hashed under it is ever stored.
const UNSEEDED_HASH_KEY: [u64; 4] = [
    0x0706050403020100,
    0x0f0e0d0c0b0a0908,
    0x1716151413121110,
    0x1f1e1d1c1b1a1918,
];

fn hash_key_material() -> [u64; 4] {
    let c = ctl();
    if c.is_null() {
        return UNSEEDED_HASH_KEY;
    }
    unsafe { addr_of!((*c).hash_key).read() }
}

/// The 128-bit handle a composite table stores in place of the Redis key.
pub fn key_hash(key: &[u8]) -> KeyHash {
    let k = hash_key_material();
    let mut out = [0u8; KEY_HASH_LEN];
    out[..8].copy_from_slice(&siphash24(k[0], k[1], key).to_le_bytes());
    out[8..].copy_from_slice(&siphash24(k[2], k[3], key).to_le_bytes());
    out
}

// ───────────────────────── Full tables and eviction ─────────────────────────

/// What to do when a shared-memory table has no room for a new entry.
///
/// Named after Redis's `maxmemory-policy`, and `noeviction` is the default
/// there too — refusing the write is the behaviour a client is most likely to
/// already handle.
#[derive(Copy, Clone, PartialEq)]
pub enum EvictPolicy {
    NoEviction,
    AllKeysRandom,
    VolatileTtl,
}

pub fn evict_policy() -> EvictPolicy {
    match crate::MAXMEMORY_POLICY
        .get()
        .as_deref()
        .and_then(|s| s.to_str().ok())
    {
        Some("allkeys-random") => EvictPolicy::AllKeysRandom,
        Some("volatile-ttl") => EvictPolicy::VolatileTtl,
        _ => EvictPolicy::NoEviction,
    }
}

/// Entries freed per eviction pass.
///
/// Eviction costs one `hash_seq_search` over the table, so freeing a single
/// slot would make every write past the limit an O(n) scan. A batch amortises
/// that scan over the next `EVICT_BATCH` inserts.
const EVICT_BATCH: usize = 64;

thread_local! {
    /// Set when an insert was refused for want of room. Read and cleared by
    /// `Command::execute_mem`, which turns it into Redis's OOM error.
    ///
    /// The alternative was threading a "table full" result out through all 68
    /// `mem_*` entry points, whose return types are `bool`, `i64`,
    /// `Result<i64, String>`, `Vec<..>` and more. A worker dispatches one
    /// command at a time on one thread, so the flag cannot cross commands.
    static OOM: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn signal_oom() {
    OOM.set(true);
}

/// Clears the flag and reports whether the command just executed was refused
/// for want of shared memory.
pub fn take_oom() -> bool {
    OOM.replace(false)
}

/// Enter `key_ptr`, calling `make_room` and retrying once if the table is full.
///
/// A still-full table yields a null entry — the same shape the callers already
/// handle — and raises the OOM flag so their neutral return value is not
/// mistaken for success.
unsafe fn enter_or_evict<E>(
    table: &SharedTable<E>,
    key_ptr: *const std::ffi::c_void,
    make_room: impl FnOnce() -> bool,
) -> (*mut E, bool) {
    if let Some(hit) = unsafe { table.enter(key_ptr) } {
        return hit;
    }
    if make_room()
        && let Some(hit) = unsafe { table.enter(key_ptr) }
    {
        return hit;
    }
    signal_oom();
    (std::ptr::null_mut(), false)
}

/// Bare-pointer form of `enter_or_evict`, for the call sites that carry an
/// HTAB rather than a `SharedTable`.
unsafe fn enter_raw<E>(
    htab: *mut pg_sys::HTAB,
    key_ptr: *const std::ffi::c_void,
    make_room: impl FnOnce() -> bool,
) -> (*mut E, bool) {
    let Some(table) = (unsafe { SharedTable::<E>::from_raw(htab) }) else {
        return (std::ptr::null_mut(), false);
    };
    unsafe { enter_or_evict(&table, key_ptr, make_room) }
}

/// Free room in a KV table. Caller must hold the database's LWLock.
///
/// Expired entries go first under every policy — they are dead weight the
/// second-granularity background sweep has not reached yet, not data anyone
/// asked to keep. Only if there are none does the policy get a say.
unsafe fn evict_kv(htab: *mut pg_sys::HTAB, overflow_htab: *mut pg_sys::HTAB) -> bool {
    let policy = evict_policy();
    // Under noeviction nothing here is evictable except expired keys, and the
    // one-second background sweep already reclaims those. Returning early keeps
    // a full table from paying an O(n) scan on every rejected write.
    if policy == EvictPolicy::NoEviction {
        return false;
    }
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return false;
    };
    let table = &table;
    let now = now_micros();
    // (expiry used for ranking, key). Expired entries rank first by using 0.
    let mut victims: Vec<(i64, [u8; MAX_KEY_LEN])> = Vec::with_capacity(EVICT_BATCH);
    let mut saw_expired = false;

    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        let expired = exp != 0 && exp <= now;
        if expired {
            saw_expired = true;
        }
        let rank = match () {
            _ if expired => 0,
            // volatile-ttl only considers keys that already carry an expiry.
            _ if policy == EvictPolicy::VolatileTtl && exp == 0 => continue,
            // Soonest expiry first for volatile-ttl; arbitrary order otherwise.
            _ if policy == EvictPolicy::VolatileTtl => exp,
            _ => i64::MAX,
        };
        push_victim(&mut victims, rank, unsafe { addr_of!((*entry).key).read() });
    }

    // An expired entry outranks everything, so once one is seen the batch is
    // pure garbage collection and evicts no live data.
    if saw_expired {
        victims.retain(|(rank, _)| *rank == 0);
    }
    for (_, key_buf) in &victims {
        unsafe { table.remove(key_buf.as_ptr().cast()) };
        if let Some(ot) = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) } {
            unsafe { ot.remove(key_buf.as_ptr().cast()) };
        }
    }
    !victims.is_empty()
}

/// Keep the `EVICT_BATCH` lowest-ranked candidates seen so far, without sorting
/// the whole table.
fn push_victim<const N: usize>(victims: &mut Vec<(i64, [u8; N])>, rank: i64, key: [u8; N]) {
    if victims.len() < EVICT_BATCH {
        victims.push((rank, key));
        return;
    }
    let (worst_at, worst) = victims
        .iter()
        .enumerate()
        .max_by_key(|(_, (r, _))| *r)
        .map(|(i, (r, _))| (i, *r))
        .unwrap_or((0, i64::MAX));
    if rank < worst {
        victims[worst_at] = (rank, key);
    }
}

/// The key hash a composite entry belongs to. Eviction never sees the plaintext
/// key and does not need to — every table it touches is keyed on the hash.
unsafe fn composite_owner(entry_key: *const u8) -> KeyHash {
    let mut kh = [0u8; KEY_HASH_LEN];
    unsafe { std::ptr::copy_nonoverlapping(entry_key, kh.as_mut_ptr(), KEY_HASH_LEN) };
    kh
}

/// Pick a key to evict from a composite table — one that is not `keep`, which
/// is the key the caller is in the middle of writing.
///
/// Collections are evicted whole, as Redis does: dropping individual fields
/// would leave a hash that silently lost half its contents and still reports
/// itself as present.
unsafe fn victim_key<E>(table: &SharedTable<E>, keep: &KeyHash) -> Option<KeyHash> {
    if evict_policy() == EvictPolicy::NoEviction {
        return None;
    }
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let owner = unsafe { composite_owner(entry as *const u8) };
        if owner != *keep {
            return Some(owner);
        }
    }
    None
}

/// Composite keys of every entry belonging to `key`, collected before any
/// removal — `hash_search(HASH_REMOVE)` may not run while a sequential scan
/// over the same table is open.
unsafe fn entries_of<E, const N: usize>(table: &SharedTable<E>, kh: &KeyHash) -> Vec<[u8; N]> {
    let mut out = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if hash_matches_entry(entry as *const u8, kh) {
            let mut k = [0u8; N];
            unsafe { std::ptr::copy_nonoverlapping(entry as *const u8, k.as_mut_ptr(), N) };
            out.push(k);
        }
    }
    out
}

/// All three meta tables key on nothing but the hash, so one removal covers
/// every one of them.
unsafe fn remove_meta_of(meta_htab: *mut pg_sys::HTAB, kh: &KeyHash) {
    if let Some(table) = unsafe { SharedTable::<u8>::from_raw(meta_htab) } {
        unsafe { table.remove(kh.as_ptr().cast()) };
    }
}

unsafe fn delete_hash_overflow_of(overflow_htab: *mut pg_sys::HTAB, kh: &KeyHash, field: &[u8]) {
    if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) } {
        unsafe { table.remove(composite_key_of(kh, field).as_ptr().cast()) };
    }
}

unsafe fn delete_list_overflow_of(overflow_htab: *mut pg_sys::HTAB, kh: &KeyHash, pos: i64) {
    if let Some(table) = unsafe { SharedTable::<ListOverflow>::from_raw(overflow_htab) } {
        unsafe { table.remove(list_key_of(kh, pos).as_ptr().cast()) };
    }
}

/// The member/field part of a composite key, NUL-trimmed.
fn composite_tail(k: &[u8; COMPOSITE_KEY_LEN]) -> &[u8] {
    let tail = &k[KEY_HASH_LEN..];
    let end = tail.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
    &tail[..end]
}

unsafe fn evict_hash_key(
    htab: *mut pg_sys::HTAB,
    overflow_htab: *mut pg_sys::HTAB,
    keep: &KeyHash,
) -> bool {
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let table = &table;
    let Some(victim) = (unsafe { victim_key(table, keep) }) else {
        return false;
    };
    let keys: Vec<[u8; COMPOSITE_KEY_LEN]> = unsafe { entries_of(table, &victim) };
    for k in &keys {
        unsafe { table.remove(k.as_ptr().cast()) };
        unsafe { delete_hash_overflow_of(overflow_htab, &victim, composite_tail(k)) };
    }
    !keys.is_empty()
}

unsafe fn evict_set_key(
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    keep: &KeyHash,
) -> bool {
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return false;
    };
    let table = &table;
    let Some(victim) = (unsafe { victim_key(table, keep) }) else {
        return false;
    };
    let keys: Vec<[u8; COMPOSITE_KEY_LEN]> = unsafe { entries_of(table, &victim) };
    for k in &keys {
        unsafe { table.remove(k.as_ptr().cast()) };
    }
    unsafe { remove_meta_of(meta_htab, &victim) };
    !keys.is_empty()
}

unsafe fn evict_zset_key(
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    keep: &KeyHash,
) -> bool {
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return false;
    };
    let table = &table;
    let Some(victim) = (unsafe { victim_key(table, keep) }) else {
        return false;
    };
    let keys: Vec<[u8; COMPOSITE_KEY_LEN]> = unsafe { entries_of(table, &victim) };
    for k in &keys {
        unsafe { table.remove(k.as_ptr().cast()) };
    }
    unsafe { remove_meta_of(meta_htab, &victim) };
    !keys.is_empty()
}

unsafe fn evict_list_key(
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    overflow_htab: *mut pg_sys::HTAB,
    keep: &KeyHash,
) -> bool {
    let Some(table) = (unsafe { SharedTable::<ListEntry>::from_raw(htab) }) else {
        return false;
    };
    let table = &table;
    let Some(victim) = (unsafe { victim_key(table, keep) }) else {
        return false;
    };
    let keys: Vec<[u8; LIST_KEY_LEN]> = unsafe { entries_of(table, &victim) };
    for k in &keys {
        unsafe { table.remove(k.as_ptr().cast()) };
        let mut pos = [0u8; 8];
        pos.copy_from_slice(&k[KEY_HASH_LEN..]);
        unsafe { delete_list_overflow_of(overflow_htab, &victim, i64::from_le_bytes(pos)) };
    }
    unsafe { remove_meta_of(meta_htab, &victim) };
    !keys.is_empty()
}

fn kv_overflow_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).kv_overflow_htab[db_idx]).read() }
}

fn hash_overflow_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).hash_overflow_htab[db_idx]).read() }
}

fn list_overflow_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).list_overflow_htab[db_idx]).read() }
}

unsafe fn kv_read_full_value(
    entry: *const KvEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
) -> Vec<u8> {
    let (total_len, has_of) = unsafe { ((*entry).value_len as usize, (*entry).has_overflow != 0) };
    if !has_of || overflow_htab.is_null() {
        let inline_len = total_len.min(INLINE_VAL_LEN);
        let ptr = unsafe { addr_of!((*entry).value) as *const u8 };
        return unsafe { std::slice::from_raw_parts(ptr, inline_len).to_vec() };
    }
    let mut buf = Vec::with_capacity(total_len);
    unsafe {
        buf.extend_from_slice(std::slice::from_raw_parts(
            addr_of!((*entry).value) as *const u8,
            INLINE_VAL_LEN,
        ));
    }
    let key_buf = make_key(key);
    if let Some(table) = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) }
        && let Some(of) = unsafe { table.find(key_buf.as_ptr().cast()) }
    {
        let tail = total_len - INLINE_VAL_LEN;
        unsafe {
            buf.extend_from_slice(std::slice::from_raw_parts(
                addr_of!((*of).value) as *const u8,
                tail,
            ));
        }
    }
    buf
}

unsafe fn kv_read_inline_slice(entry: *const KvEntry) -> &'static [u8] {
    let total_len = unsafe { (*entry).value_len as usize };
    let inline_len = total_len.min(INLINE_VAL_LEN);
    let val_ptr = unsafe { addr_of!((*entry).value) as *const u8 };
    unsafe { std::slice::from_raw_parts(val_ptr, inline_len) }
}

unsafe fn kv_write_full_value(
    entry: *mut KvEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
    value: &[u8],
    expires_at: i64,
) -> bool {
    let total = value.len();
    if total > MAX_TOTAL_VAL_LEN {
        // Leave the entry empty rather than untouched. `hash_search(HASH_ENTER)`
        // hands back recycled shared memory with only the key initialised, so
        // returning here without writing would expose whichever key previously
        // occupied this element to the next reader.
        unsafe {
            addr_of_mut!((*entry).value_len).write(0);
            addr_of_mut!((*entry).has_overflow).write(0);
            addr_of_mut!((*entry).expires_at).write(expires_at);
        }
        return false;
    }

    let inline_len = total.min(INLINE_VAL_LEN);
    unsafe {
        let vptr = addr_of_mut!((*entry).value) as *mut u8;
        std::ptr::copy_nonoverlapping(value.as_ptr(), vptr, inline_len);
        addr_of_mut!((*entry).value_len).write(total as u32);
        addr_of_mut!((*entry).expires_at).write(expires_at);
    }

    if total > INLINE_VAL_LEN {
        unsafe { addr_of_mut!((*entry).has_overflow).write(1) };
        if let Some(table) = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) } {
            let key_buf = make_key(key);
            let Some((of, _found)) = (unsafe { table.enter(key_buf.as_ptr().cast()) }) else {
                // The tail has nowhere to live, and value_len already promises
                // it. Blank the entry rather than leave a length that reads
                // past what was stored.
                unsafe {
                    addr_of_mut!((*entry).value_len).write(0);
                    addr_of_mut!((*entry).has_overflow).write(0);
                }
                signal_oom();
                return false;
            };
            let tail = total - INLINE_VAL_LEN;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    value.as_ptr().add(INLINE_VAL_LEN),
                    addr_of_mut!((*of).value) as *mut u8,
                    tail,
                );
            }
        }
    } else {
        unsafe { addr_of_mut!((*entry).has_overflow).write(0) };
        if let Some(table) = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) } {
            let key_buf = make_key(key);
            unsafe { table.remove(key_buf.as_ptr().cast()) };
        }
    }
    true
}

unsafe fn kv_delete_overflow(overflow_htab: *mut pg_sys::HTAB, key: &[u8]) {
    if let Some(table) = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) } {
        let key_buf = make_key(key);
        unsafe { table.remove(key_buf.as_ptr().cast()) };
    }
}

unsafe fn hash_read_full_value(
    entry: *const HashEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
    field: &[u8],
) -> Vec<u8> {
    let (total_len, has_of) = unsafe { ((*entry).value_len as usize, (*entry).has_overflow != 0) };
    if !has_of || overflow_htab.is_null() {
        let inline_len = total_len.min(INLINE_VAL_LEN);
        let ptr = unsafe { addr_of!((*entry).value) as *const u8 };
        return unsafe { std::slice::from_raw_parts(ptr, inline_len).to_vec() };
    }
    let mut buf = Vec::with_capacity(total_len);
    unsafe {
        buf.extend_from_slice(std::slice::from_raw_parts(
            addr_of!((*entry).value) as *const u8,
            INLINE_VAL_LEN,
        ));
    }
    let k = make_composite_key(key, field);
    if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) }
        && let Some(of) = unsafe { table.find(k.as_ptr().cast()) }
    {
        let tail = total_len - INLINE_VAL_LEN;
        unsafe {
            buf.extend_from_slice(std::slice::from_raw_parts(
                addr_of!((*of).value) as *const u8,
                tail,
            ));
        }
    }
    buf
}

/// Returns false when the value's overflow tail had nowhere to go; the caller
/// must then drop the entry it just created.
unsafe fn hash_write_full_value(
    entry: *mut HashEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
    field: &[u8],
    value: &[u8],
) -> bool {
    debug_assert!(
        value.len() <= MAX_TOTAL_VAL_LEN,
        "over-long value reached memory backend"
    );
    let total = value.len().min(MAX_TOTAL_VAL_LEN);
    let inline_len = total.min(INLINE_VAL_LEN);
    unsafe {
        std::ptr::copy_nonoverlapping(
            value.as_ptr(),
            addr_of_mut!((*entry).value) as *mut u8,
            inline_len,
        );
        addr_of_mut!((*entry).value_len).write(total as u32);
    }

    if total > INLINE_VAL_LEN {
        unsafe { addr_of_mut!((*entry).has_overflow).write(1) };
        if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) } {
            let k = make_composite_key(key, field);
            let Some((of, _found)) = (unsafe { table.enter(k.as_ptr().cast()) }) else {
                unsafe {
                    addr_of_mut!((*entry).value_len).write(0);
                    addr_of_mut!((*entry).has_overflow).write(0);
                }
                signal_oom();
                return false;
            };
            let tail = total - INLINE_VAL_LEN;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    value.as_ptr().add(INLINE_VAL_LEN),
                    addr_of_mut!((*of).value) as *mut u8,
                    tail,
                );
            }
        }
    } else {
        unsafe { addr_of_mut!((*entry).has_overflow).write(0) };
        if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) } {
            let k = make_composite_key(key, field);
            unsafe { table.remove(k.as_ptr().cast()) };
        }
    }
    true
}

unsafe fn hash_delete_overflow(overflow_htab: *mut pg_sys::HTAB, key: &[u8], field: &[u8]) {
    if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) } {
        let k = make_composite_key(key, field);
        unsafe { table.remove(k.as_ptr().cast()) };
    }
}

unsafe fn list_read_full_value(
    entry: *const ListEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
    pos: i64,
) -> Vec<u8> {
    let (total_len, has_of) = unsafe { ((*entry).value_len as usize, (*entry).has_overflow != 0) };
    if !has_of || overflow_htab.is_null() {
        let inline_len = total_len.min(INLINE_VAL_LEN);
        let ptr = unsafe { addr_of!((*entry).value) as *const u8 };
        return unsafe { std::slice::from_raw_parts(ptr, inline_len).to_vec() };
    }
    let mut buf = Vec::with_capacity(total_len);
    unsafe {
        buf.extend_from_slice(std::slice::from_raw_parts(
            addr_of!((*entry).value) as *const u8,
            INLINE_VAL_LEN,
        ));
    }
    let k = make_list_key(key, pos);
    if let Some(table) = unsafe { SharedTable::<ListOverflow>::from_raw(overflow_htab) }
        && let Some(of) = unsafe { table.find(k.as_ptr().cast()) }
    {
        let tail = total_len - INLINE_VAL_LEN;
        unsafe {
            buf.extend_from_slice(std::slice::from_raw_parts(
                addr_of!((*of).value) as *const u8,
                tail,
            ));
        }
    }
    buf
}

/// Returns false when the value's overflow tail had nowhere to go; the caller
/// must then drop the entry it just created.
unsafe fn list_write_full_value(
    entry: *mut ListEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
    pos: i64,
    value: &[u8],
) -> bool {
    debug_assert!(
        value.len() <= MAX_TOTAL_VAL_LEN,
        "over-long value reached memory backend"
    );
    let total = value.len().min(MAX_TOTAL_VAL_LEN);
    let inline_len = total.min(INLINE_VAL_LEN);
    unsafe {
        std::ptr::copy_nonoverlapping(
            value.as_ptr(),
            addr_of_mut!((*entry).value) as *mut u8,
            inline_len,
        );
        addr_of_mut!((*entry).value_len).write(total as u32);
    }

    if total > INLINE_VAL_LEN {
        unsafe { addr_of_mut!((*entry).has_overflow).write(1) };
        if let Some(table) = unsafe { SharedTable::<ListOverflow>::from_raw(overflow_htab) } {
            let k = make_list_key(key, pos);
            let Some((of, _found)) = (unsafe { table.enter(k.as_ptr().cast()) }) else {
                unsafe {
                    addr_of_mut!((*entry).value_len).write(0);
                    addr_of_mut!((*entry).has_overflow).write(0);
                }
                signal_oom();
                return false;
            };
            let tail = total - INLINE_VAL_LEN;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    value.as_ptr().add(INLINE_VAL_LEN),
                    addr_of_mut!((*of).value) as *mut u8,
                    tail,
                );
            }
        }
    } else {
        unsafe { addr_of_mut!((*entry).has_overflow).write(0) };
        if let Some(table) = unsafe { SharedTable::<ListOverflow>::from_raw(overflow_htab) } {
            let k = make_list_key(key, pos);
            unsafe { table.remove(k.as_ptr().cast()) };
        }
    }
    true
}

unsafe fn list_delete_overflow(overflow_htab: *mut pg_sys::HTAB, key: &[u8], pos: i64) {
    if let Some(table) = unsafe { SharedTable::<ListOverflow>::from_raw(overflow_htab) } {
        let k = make_list_key(key, pos);
        unsafe { table.remove(k.as_ptr().cast()) };
    }
}

fn htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).htab[db_idx]).read() }
}

fn lwlock(db_idx: usize) -> *mut pg_sys::LWLock {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).lwlock[db_idx]).read() }
}

/// Callers are expected to have refused an over-long key already
/// (`Command::mem_too_long_error`). The assertions are the backstop for a
/// command that grows a new key argument without extending that check: two keys
/// sharing a prefix would otherwise silently collapse onto one entry.
fn make_key(s: &[u8]) -> [u8; MAX_KEY_LEN] {
    debug_assert!(
        s.len() < MAX_KEY_LEN,
        "over-long key reached memory backend"
    );
    let mut key = [0u8; MAX_KEY_LEN];
    let bytes = s;
    let len = bytes.len().min(MAX_KEY_LEN - 1);
    key[..len].copy_from_slice(&bytes[..len]);
    key
}

/// GET: returns value if key exists and not expired.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_get(db_idx: usize, key: &[u8]) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let (result, was_expired) = match unsafe { table.find(key_buf.as_ptr().cast()) } {
        Some(entry) if unsafe { entry_is_expired(entry) } => (None, true),
        Some(entry) => (
            Some(unsafe { kv_read_full_value(entry, overflow_htab, key) }),
            false,
        ),
        None => (None, false),
    };

    unsafe { pg_sys::LWLockRelease(lk) };

    if was_expired {
        unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        if let Some(entry2) = unsafe { table.find(key_buf.as_ptr().cast()) }
            && unsafe { entry_is_expired(entry2) }
        {
            unsafe { table.remove(key_buf.as_ptr().cast()) };
            unsafe { kv_delete_overflow(overflow_htab, key) };
        }
        unsafe { pg_sys::LWLockRelease(lk) };
    }

    result
}

/// SET: upsert key→value with optional expiry (microseconds since epoch, 0=no expiry).
///
/// Returns `false` when the value exceeds `MAX_TOTAL_VAL_LEN`, in which case
/// nothing is stored — callers must report the failure rather than reply +OK.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_set(db_idx: usize, key: &[u8], value: &[u8], expires_at_us: i64) -> bool {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return false;
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe {
        enter_or_evict(&table, key_buf.as_ptr().cast(), || {
            evict_kv(htab, overflow_htab)
        })
    };
    let ok = !entry.is_null()
        && unsafe { kv_write_full_value(entry, overflow_htab, key, value, expires_at_us) };
    if !ok && !found && !entry.is_null() {
        // A rejected SET must not leave a phantom empty key behind.
        unsafe { table.remove(key_buf.as_ptr().cast()) };
    }

    unsafe { pg_sys::LWLockRelease(lk) };
    ok
}

/// DEL: delete one or more keys, return count deleted.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_del(db_idx: usize, keys: &[&[u8]]) -> i64 {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return 0;
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let mut count = 0i64;

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    for key in keys {
        let key_buf = make_key(key);
        if let Some(entry) = unsafe { table.find(key_buf.as_ptr().cast()) } {
            let expired = unsafe { entry_is_expired(entry) };
            unsafe { table.remove(key_buf.as_ptr().cast()) };
            unsafe { kv_delete_overflow(overflow_htab, key) };
            if !expired {
                count += 1;
            }
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// EXISTS: count how many of the given keys exist (non-expired).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_exists(db_idx: usize, keys: &[&[u8]]) -> i64 {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = lwlock(db_idx);
    let mut count = 0i64;

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    for key in keys {
        let key_buf = make_key(key);
        if let Some(entry) = unsafe { table.find(key_buf.as_ptr().cast()) }
            && !unsafe { entry_is_expired(entry) }
        {
            count += 1;
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// INCR/DECR by delta. Returns new value or Err if not integer.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_incr(db_idx: usize, key: &[u8], delta: i64) -> Result<i64, String> {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return Err("ERR memory not initialized".to_string());
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let overflow_htab = kv_overflow_htab_for(db_idx);
    let (entry, found) = unsafe {
        enter_or_evict(&table, key_buf.as_ptr().cast(), || {
            evict_kv(htab, overflow_htab)
        })
    };

    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found || unsafe { entry_is_expired(entry) } {
        let new_val = delta;
        let s = new_val.to_string();
        unsafe { kv_write_full_value(entry, overflow_htab, key, s.as_bytes(), 0) };
        Ok(new_val)
    } else {
        let current_str = {
            let slice = unsafe { kv_read_inline_slice(entry) };
            std::str::from_utf8(slice)
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?
                .to_owned()
        };
        let current: i64 = current_str
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
        let new_val = current
            .checked_add(delta)
            .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;
        let ns = new_val.to_string();
        let exp = unsafe { (*entry).expires_at };
        unsafe { kv_write_full_value(entry, overflow_htab, key, ns.as_bytes(), exp) };
        Ok(new_val)
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// INCRBYFLOAT: increment float value, return new string value.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_incr_float(db_idx: usize, key: &[u8], delta: f64) -> Result<String, String> {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return Err("ERR memory not initialized".to_string());
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe {
        enter_or_evict(&table, key_buf.as_ptr().cast(), || {
            evict_kv(htab, overflow_htab)
        })
    };

    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found || unsafe { entry_is_expired(entry) } {
        let s = format_float(delta);
        unsafe { kv_write_full_value(entry, overflow_htab, key, s.as_bytes(), 0) };
        Ok(s)
    } else {
        let current_str = {
            let slice = unsafe { kv_read_inline_slice(entry) };
            std::str::from_utf8(slice)
                .map_err(|_| "ERR value is not a valid float".to_string())?
                .to_owned()
        };
        let current: f64 = current_str
            .parse()
            .map_err(|_| "ERR value is not a valid float".to_string())?;
        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            unsafe { pg_sys::LWLockRelease(lk) };
            return Err("ERR increment would produce NaN or Infinity".to_string());
        }
        let ns = format_float(new_val);
        let exp = unsafe { (*entry).expires_at };
        unsafe { kv_write_full_value(entry, overflow_htab, key, ns.as_bytes(), exp) };
        Ok(ns)
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

fn format_float(f: f64) -> String {
    format!("{}", f)
}

/// GET+SET: set new value, return old value.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_getset(db_idx: usize, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe {
        enter_or_evict(&table, key_buf.as_ptr().cast(), || {
            evict_kv(htab, overflow_htab)
        })
    };

    let old = if found && !entry.is_null() && !unsafe { entry_is_expired(entry) } {
        Some(unsafe { kv_read_full_value(entry, overflow_htab, key) })
    } else {
        None
    };

    if !entry.is_null() {
        unsafe { kv_write_full_value(entry, overflow_htab, key, value, 0) };
    }

    unsafe { pg_sys::LWLockRelease(lk) };
    old
}

/// GETDEL: get and delete atomically.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_getdel(db_idx: usize, key: &[u8]) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let result = if let Some(entry) = unsafe { table.find(key_buf.as_ptr().cast()) } {
        let val = if !unsafe { entry_is_expired(entry) } {
            Some(unsafe { kv_read_full_value(entry, overflow_htab, key) })
        } else {
            None
        };
        unsafe { table.remove(key_buf.as_ptr().cast()) };
        unsafe { kv_delete_overflow(overflow_htab, key) };
        val
    } else {
        None
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// APPEND: append to existing value, return new length.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_append(db_idx: usize, key: &[u8], suffix: &[u8]) -> i64 {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);
    let suffix_bytes = suffix;

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let overflow_htab = kv_overflow_htab_for(db_idx);
    let (entry, found) = unsafe {
        enter_or_evict(&table, key_buf.as_ptr().cast(), || {
            evict_kv(htab, overflow_htab)
        })
    };

    let new_len = if entry.is_null() {
        0i64
    } else if !found || unsafe { entry_is_expired(entry) } {
        let len = suffix_bytes.len().min(MAX_TOTAL_VAL_LEN);
        unsafe { kv_write_full_value(entry, overflow_htab, key, &suffix_bytes[..len], 0) };
        len as i64
    } else {
        let existing_len = unsafe { (*entry).value_len as usize };
        let append_len = suffix_bytes.len().min(MAX_TOTAL_VAL_LEN - existing_len);
        let new_val_len = existing_len + append_len;
        let mut new_val = unsafe { kv_read_full_value(entry, overflow_htab, key) };
        new_val.extend_from_slice(&suffix_bytes[..append_len]);
        let exp = unsafe { (*entry).expires_at };
        unsafe { kv_write_full_value(entry, overflow_htab, key, &new_val, exp) };
        new_val_len as i64
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    new_len
}

/// STRLEN: return value length or 0 if missing.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_strlen(db_idx: usize, key: &[u8]) -> i64 {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let result = match unsafe { table.find(key_buf.as_ptr().cast()) } {
        Some(entry) if !unsafe { entry_is_expired(entry) } => unsafe { (*entry).value_len as i64 },
        _ => 0,
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// TTL raw: return (exists: bool, expires_at_us: i64).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_ttl_raw(db_idx: usize, key: &[u8]) -> (bool, i64) {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return (false, 0);
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let result = match unsafe { table.find(key_buf.as_ptr().cast()) } {
        Some(entry) if !unsafe { entry_is_expired(entry) } => {
            (true, unsafe { (*entry).expires_at })
        }
        _ => (false, 0),
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// Set expiry (absolute microseconds since epoch). Return true if key exists.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_set_expiry(db_idx: usize, key: &[u8], expires_at_us: i64) -> bool {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return false;
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let result = if let Some(entry) = unsafe { table.find(key_buf.as_ptr().cast()) } {
        if !unsafe { entry_is_expired(entry) } {
            unsafe { addr_of_mut!((*entry).expires_at).write(expires_at_us) };
            true
        } else {
            false
        }
    } else {
        false
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// Remove expiry. Return true if key existed and had an expiry.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_persist(db_idx: usize, key: &[u8]) -> bool {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return false;
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let result = if let Some(entry) = unsafe { table.find(key_buf.as_ptr().cast()) } {
        if !unsafe { entry_is_expired(entry) } {
            let had_expiry = unsafe { (*entry).expires_at } != 0;
            if had_expiry {
                unsafe { addr_of_mut!((*entry).expires_at).write(0) };
            }
            had_expiry
        } else {
            false
        }
    } else {
        false
    };

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// MGET: return values for keys in order (None for missing/expired).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_mget(db_idx: usize, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return keys.iter().map(|_| None).collect();
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let result = keys
        .iter()
        .map(|key| {
            let key_buf = make_key(key);
            match unsafe { table.find(key_buf.as_ptr().cast()) } {
                Some(entry) if !unsafe { entry_is_expired(entry) } => {
                    Some(unsafe { kv_read_full_value(entry, overflow_htab, key) })
                }
                _ => None,
            }
        })
        .collect();

    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// MSET: set multiple keys.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_mset(db_idx: usize, pairs: &[(&[u8], &[u8])]) {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return;
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    for (key, value) in pairs {
        let key_buf = make_key(key);
        let (entry, _found) = unsafe {
            enter_or_evict(&table, key_buf.as_ptr().cast(), || {
                evict_kv(htab, overflow_htab)
            })
        };
        if !entry.is_null() {
            unsafe { kv_write_full_value(entry, overflow_htab, key, value, 0) };
        }
    }

    unsafe { pg_sys::LWLockRelease(lk) };
}

/// SCAN / KEYS: return keys matching glob pattern (always full scan, cursor always 0).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_scan(db_idx: usize, pattern: &[u8]) -> Vec<Vec<u8>> {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let lk = lwlock(db_idx);
    let now = now_micros();
    let mut results = Vec::new();

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        if exp != 0 && exp <= now {
            continue;
        }
        let key_ptr = unsafe { addr_of!((*entry).key) as *const u8 };
        let key_slice = unsafe { std::slice::from_raw_parts(key_ptr, MAX_KEY_LEN) };
        let key_end = key_slice
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(MAX_KEY_LEN);
        let key_str = &key_slice[..key_end];
        if glob_matches(pattern, key_str) {
            results.push(key_str.to_vec());
        }
    }

    unsafe { pg_sys::LWLockRelease(lk) };
    results
}

/// DBSIZE: count non-expired keys.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_dbsize(db_idx: usize) -> i64 {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = lwlock(db_idx);
    let now = now_micros();
    let mut count = 0i64;

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        if exp == 0 || exp > now {
            count += 1;
        }
    }

    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// Background expiry sweep: delete expired keys.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_sweep_expired(db_idx: usize) {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return;
    };
    let lk = lwlock(db_idx);
    let now = now_micros();

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let mut to_delete: Vec<[u8; MAX_KEY_LEN]> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        if exp != 0 && exp <= now {
            let key_ptr = unsafe { addr_of!((*entry).key) };
            to_delete.push(unsafe { key_ptr.read() });
        }
    }

    let overflow_htab = kv_overflow_htab_for(db_idx);
    let overflow_table = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) };
    for key_buf in &to_delete {
        unsafe { table.remove(key_buf.as_ptr().cast()) };
        if let Some(ref ot) = overflow_table {
            unsafe { ot.remove(key_buf.as_ptr().cast()) };
        }
    }

    unsafe { pg_sys::LWLockRelease(lk) };
}

/// TYPE: returns type string for a key, "none" for missing.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_type(db_idx: usize, key: &[u8]) -> &'static str {
    let htab = htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let is_string = if let Some(table) = unsafe { SharedTable::<KvEntry>::from_raw(htab) } {
        matches!(unsafe { table.find(key_buf.as_ptr().cast()) }, Some(e) if !unsafe { entry_is_expired(e) })
    } else {
        false
    };
    unsafe { pg_sys::LWLockRelease(lk) };

    if is_string {
        return "string";
    }

    let c = ctl();
    if !c.is_null() {
        {
            let htab2 = unsafe { addr_of!((*c).hash_htab[db_idx]).read() };
            let lk2 = unsafe { addr_of!((*c).hash_lwlock[db_idx]).read() };
            if !htab2.is_null() && !lk2.is_null() {
                unsafe { pg_sys::LWLockAcquire(lk2, pg_sys::LWLockMode::LW_SHARED) };
                let has_hash = unsafe { has_any_entry_for_key(htab2, key) };
                unsafe { pg_sys::LWLockRelease(lk2) };
                if has_hash {
                    return "hash";
                }
            }
        }
        {
            let meta_htab2 = unsafe { addr_of!((*c).set_meta_htab[db_idx]).read() };
            let lk2 = unsafe { addr_of!((*c).set_lwlock[db_idx]).read() };
            if !meta_htab2.is_null() && !lk2.is_null() {
                unsafe { pg_sys::LWLockAcquire(lk2, pg_sys::LWLockMode::LW_SHARED) };
                let meta = unsafe { find_set_meta(meta_htab2, key) };
                let has_set = !meta.is_null() && unsafe { (*meta).count > 0 };
                unsafe { pg_sys::LWLockRelease(lk2) };
                if has_set {
                    return "set";
                }
            }
        }
        {
            let meta_htab2 = unsafe { addr_of!((*c).zset_meta_htab[db_idx]).read() };
            let lk2 = unsafe { addr_of!((*c).zset_lwlock[db_idx]).read() };
            if !meta_htab2.is_null() && !lk2.is_null() {
                unsafe { pg_sys::LWLockAcquire(lk2, pg_sys::LWLockMode::LW_SHARED) };
                let meta = unsafe { find_zset_meta(meta_htab2, key) };
                let has_zset = !meta.is_null() && unsafe { (*meta).count > 0 };
                unsafe { pg_sys::LWLockRelease(lk2) };
                if has_zset {
                    return "zset";
                }
            }
        }
        {
            let meta_htab2 = unsafe { addr_of!((*c).list_meta_htab[db_idx]).read() };
            let lk2 = unsafe { addr_of!((*c).list_lwlock[db_idx]).read() };
            if !meta_htab2.is_null() && !lk2.is_null() {
                unsafe { pg_sys::LWLockAcquire(lk2, pg_sys::LWLockMode::LW_SHARED) };
                let has_list = unsafe { has_any_list_entry_for_key(meta_htab2, key) };
                unsafe { pg_sys::LWLockRelease(lk2) };
                if has_list {
                    return "list";
                }
            }
        }
    }

    "none"
}

unsafe fn has_any_entry_for_key(htab: *mut pg_sys::HTAB, key: &[u8]) -> bool {
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let kh = key_hash(key);
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if hash_matches_entry(entry as *const u8, &kh) {
            return true;
        }
    }
    false
}

unsafe fn has_any_list_entry_for_key(meta_htab: *mut pg_sys::HTAB, key: &[u8]) -> bool {
    let Some(table) = (unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) }) else {
        return false;
    };
    let key_buf = key_hash(key);
    match unsafe { table.find(key_buf.as_ptr().cast()) } {
        Some(meta) => unsafe { (*meta).count > 0 },
        None => false,
    }
}

/// Glob pattern matching supporting `*`, `?`, and `[...]` character classes.
/// Delegates to the iterative implementation in pubsub for consistency with PSUBSCRIBE.
pub fn glob_matches(pattern: &[u8], s: &[u8]) -> bool {
    crate::pubsub::glob_match(pattern, s)
}

/// Shared memory size for the MemControlBlock itself (holds pointers, not HTAB data).
pub fn mem_ctl_size() -> usize {
    std::mem::size_of::<MemControlBlock>()
}

/// Total shmem needed for all 8 HTAB tables.
/// PostgreSQL's HTAB with HASH_SHARED_MEM allocates entry storage + ~25% bucket overhead.
pub fn mem_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<KvEntry>();
    // 5/4 multiplier for bucket chains + HTAB internal bookkeeping per table
    let per_table = (htab_init_size() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_hash_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<HashEntry>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_set_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<SetEntry>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_zset_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<ZsetEntry>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_list_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<ListEntry>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_list_meta_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<ListMeta>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_zset_meta_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<ZsetMeta>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_set_meta_htab_total_size() -> usize {
    let entry_size = std::mem::size_of::<SetMeta>();
    let per_table = (htab_init_size_small() as usize) * entry_size * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_kv_overflow_total_size() -> usize {
    let per_table = (htab_init_size() as usize) * std::mem::size_of::<KvOverflow>() * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_hash_overflow_total_size() -> usize {
    let per_table =
        (htab_init_size_small() as usize) * std::mem::size_of::<HashOverflow>() * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

pub fn mem_list_overflow_total_size() -> usize {
    let per_table =
        (htab_init_size_small() as usize) * std::mem::size_of::<ListOverflow>() * 5 / 4 + 8192;
    per_table * NUM_MEM_DBS
}

/// Called from shmem_startup_hook (postmaster startup path) to create the 8 HTAB tables.
/// MUST NOT be called from bgworkers — ShmemInitHash for HASH_SHARED_MEM is only valid
/// during postmaster startup when ShmemAlloc is still open.
///
/// # Safety
/// - Must be called from the postmaster shmem_startup_hook, never from a bgworker.
/// - `ctl` must point to a valid, zeroed `MemControlBlock` in shared memory.
pub unsafe fn mem_init_tables(ctl: *mut MemControlBlock) {
    // Seeded before any table exists to be keyed with it. Drawn once per
    // postmaster: shared memory is empty at this point, so no stored hash can be
    // left over from a previous key.
    unsafe {
        let mut seed = [0u64; 4];
        if !pg_sys::pg_strong_random(seed.as_mut_ptr().cast(), std::mem::size_of_val(&seed)) {
            // Only fails where the platform offers no entropy at all. A
            // predictable key still hashes correctly; it forfeits only the
            // resistance to a client crafting a collision.
            pgrx::warning!("pg_redis: no strong randomness for the key hash; using a fixed seed");
            seed = UNSEEDED_HASH_KEY;
        }
        addr_of_mut!((*ctl).hash_key).write(seed);
    }

    let blob_flags = (pg_sys::HASH_ELEM
        | pg_sys::HASH_BLOBS
        | pg_sys::HASH_SHARED_MEM
        | pg_sys::HASH_FIXED_SIZE) as i32;
    let str_flags = (pg_sys::HASH_ELEM
        | pg_sys::HASH_STRINGS
        | pg_sys::HASH_SHARED_MEM
        | pg_sys::HASH_FIXED_SIZE) as i32;

    let sz = htab_init_size();
    let sz_small = htab_init_size_small();

    for i in 0..NUM_MEM_DBS {
        unsafe {
            let name = format!("pg_redis_kv_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: MAX_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<KvEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(name.as_ptr().cast(), sz, sz, &mut info, str_flags);
            std::ptr::addr_of_mut!((*ctl).htab[i]).write(htab);

            let name = format!("pg_redis_hash_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: COMPOSITE_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<HashEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).hash_htab[i]).write(htab);

            let name = format!("pg_redis_set_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: COMPOSITE_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<SetEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).set_htab[i]).write(htab);

            let name = format!("pg_redis_zset_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: COMPOSITE_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ZsetEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).zset_htab[i]).write(htab);

            let name = format!("pg_redis_list_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: LIST_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ListEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).list_htab[i]).write(htab);

            let name = format!("pg_redis_list_meta_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: KEY_HASH_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ListMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).list_meta_htab[i]).write(htab);

            let name = format!("pg_redis_zset_meta_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: KEY_HASH_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ZsetMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).zset_meta_htab[i]).write(htab);

            let name = format!("pg_redis_set_meta_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: KEY_HASH_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<SetMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).set_meta_htab[i]).write(htab);

            // KV overflow: HASH_STRINGS, key = MAX_KEY_LEN (same as KvEntry)
            let name = format!("pg_redis_kv_of_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: MAX_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<KvOverflow>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(name.as_ptr().cast(), sz, sz, &mut info, str_flags);
            std::ptr::addr_of_mut!((*ctl).kv_overflow_htab[i]).write(htab);

            // Hash overflow: HASH_BLOBS, composite key key[128] + field[128] = 256 bytes
            let name = format!("pg_redis_hash_of_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: COMPOSITE_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<HashOverflow>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).hash_overflow_htab[i]).write(htab);

            // List overflow: HASH_BLOBS, key[128] + pos_bytes[8] = 136 bytes
            let name = format!("pg_redis_list_of_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: LIST_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ListOverflow>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).list_overflow_htab[i]).write(htab);
        }
    }
}

// ─────────────────────────── Key helpers ────────────────────────────────────

fn make_composite_key(key: &[u8], field: &[u8]) -> [u8; COMPOSITE_KEY_LEN] {
    composite_key_of(&key_hash(key), field)
}

/// Same layout, for callers that already hold the hash — eviction works in hash
/// space, having never had the plaintext key.
fn composite_key_of(kh: &KeyHash, field: &[u8]) -> [u8; COMPOSITE_KEY_LEN] {
    debug_assert!(
        field.len() <= MAX_MEMBER_LEN,
        "over-long member reached memory backend"
    );
    let mut buf = [0u8; COMPOSITE_KEY_LEN];
    let fl = field.len().min(MAX_MEMBER_LEN);
    buf[..KEY_HASH_LEN].copy_from_slice(kh);
    buf[KEY_HASH_LEN..KEY_HASH_LEN + fl].copy_from_slice(&field[..fl]);
    buf
}

fn make_list_key(key: &[u8], pos: i64) -> [u8; LIST_KEY_LEN] {
    list_key_of(&key_hash(key), pos)
}

fn list_key_of(kh: &KeyHash, pos: i64) -> [u8; LIST_KEY_LEN] {
    let mut buf = [0u8; LIST_KEY_LEN];
    buf[..KEY_HASH_LEN].copy_from_slice(kh);
    buf[KEY_HASH_LEN..].copy_from_slice(&pos.to_le_bytes());
    buf
}

fn hash_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).hash_htab[db_idx]).read() }
}
fn hash_lwlock(db_idx: usize) -> *mut pg_sys::LWLock {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).hash_lwlock[db_idx]).read() }
}
fn set_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).set_htab[db_idx]).read() }
}
fn set_lwlock(db_idx: usize) -> *mut pg_sys::LWLock {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).set_lwlock[db_idx]).read() }
}
fn zset_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).zset_htab[db_idx]).read() }
}
fn zset_lwlock(db_idx: usize) -> *mut pg_sys::LWLock {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).zset_lwlock[db_idx]).read() }
}
fn list_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).list_htab[db_idx]).read() }
}
fn list_lwlock(db_idx: usize) -> *mut pg_sys::LWLock {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).list_lwlock[db_idx]).read() }
}
fn list_meta_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).list_meta_htab[db_idx]).read() }
}
fn zset_meta_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).zset_meta_htab[db_idx]).read() }
}
fn set_meta_htab_for(db_idx: usize) -> *mut pg_sys::HTAB {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).set_meta_htab[db_idx]).read() }
}

fn key_matches_entry(entry_key_ptr: *const u8, key: &[u8]) -> bool {
    hash_matches_entry(entry_key_ptr, &key_hash(key))
}

/// Whether a composite entry belongs to the key with this hash. Sixteen bytes
/// compared, rather than a scan for the NUL through five hundred and twelve.
fn hash_matches_entry(entry_key_ptr: *const u8, kh: &KeyHash) -> bool {
    unsafe { std::slice::from_raw_parts(entry_key_ptr, KEY_HASH_LEN) == kh }
}

// ─────────────────────────── Hash operations ────────────────────────────────

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hset(db_idx: usize, key: &[u8], field: &[u8], value: &[u8]) -> bool {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_hash_key(htab, overflow_htab, &key_hash(key))
        })
    };
    let mut is_new = !found;
    if !entry.is_null()
        && !unsafe { hash_write_full_value(entry, overflow_htab, key, field, value) }
    {
        unsafe { table.remove(k.as_ptr().cast()) };
        is_new = false;
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    is_new
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hget(db_idx: usize, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
    let htab = hash_htab_for(db_idx);
    let table = unsafe { SharedTable::<HashEntry>::from_raw(htab) }?;
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let result = unsafe { table.find(k.as_ptr().cast()) }
        .map(|entry| unsafe { hash_read_full_value(entry, overflow_htab, key, field) });
    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hdel(db_idx: usize, key: &[u8], fields: &[&[u8]]) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut count = 0i64;
    for f in fields {
        let k = make_composite_key(key, f);
        let existed = unsafe { table.find(k.as_ptr().cast()) }.is_some();
        unsafe { table.remove(k.as_ptr().cast()) };
        if existed {
            unsafe { hash_delete_overflow(overflow_htab, key, f) };
            count += 1;
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hexists(db_idx: usize, key: &[u8], field: &[u8]) -> bool {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let found = unsafe { table.find(k.as_ptr().cast()) }.is_some();
    unsafe { pg_sys::LWLockRelease(lk) };
    found
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hgetall(db_idx: usize, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        let fb = unsafe { addr_of!((*entry).field) as *const u8 };
        let fs = unsafe { std::slice::from_raw_parts(fb, MAX_MEMBER_LEN) };
        let fe = fs.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
        let field_str = fs[..fe].to_vec();
        let val_str = unsafe { hash_read_full_value(entry, overflow_htab, key, &field_str) };
        collected.push((field_str, val_str));
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    collected.sort_by(|a, b| a.0.cmp(&b.0));
    collected
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hkeys(db_idx: usize, key: &[u8]) -> Vec<Vec<u8>> {
    unsafe { mem_hgetall(db_idx, key) }
        .into_iter()
        .map(|(f, _)| f)
        .collect()
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hvals(db_idx: usize, key: &[u8]) -> Vec<Vec<u8>> {
    unsafe { mem_hgetall(db_idx, key) }
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hlen(db_idx: usize, key: &[u8]) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut count = 0i64;
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            count += 1;
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hmget(db_idx: usize, key: &[u8], fields: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return fields.iter().map(|_| None).collect();
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let results: Vec<Option<Vec<u8>>> = fields
        .iter()
        .map(|f| {
            let k = make_composite_key(key, f);
            unsafe { table.find(k.as_ptr().cast()) }
                .map(|entry| unsafe { hash_read_full_value(entry, overflow_htab, key, f) })
        })
        .collect();
    unsafe { pg_sys::LWLockRelease(lk) };
    results
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hincrby(
    db_idx: usize,
    key: &[u8],
    field: &[u8],
    delta: i64,
) -> Result<i64, String> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return Err("ERR memory not initialized".to_string());
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_hash_key(htab, overflow_htab, &key_hash(key))
        })
    };
    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found {
        let s = delta.to_string();
        // An integer always fits inline, so the overflow table is never touched.
        let _ = unsafe { hash_write_full_value(entry, overflow_htab, key, field, s.as_bytes()) };
        Ok(delta)
    } else {
        let cur_bytes = unsafe { hash_read_full_value(entry, overflow_htab, key, field) };
        let cur: i64 = std::str::from_utf8(&cur_bytes)
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
        let new_val = cur
            .checked_add(delta)
            .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;
        let ns = new_val.to_string();
        let _ = unsafe { hash_write_full_value(entry, overflow_htab, key, field, ns.as_bytes()) };
        Ok(new_val)
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hsetnx(db_idx: usize, key: &[u8], field: &[u8], value: &[u8]) -> bool {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_hash_key(htab, overflow_htab, &key_hash(key))
        })
    };
    let set = if !found && !entry.is_null() {
        let written = unsafe { hash_write_full_value(entry, overflow_htab, key, field, value) };
        if !written {
            unsafe { table.remove(k.as_ptr().cast()) };
        }
        written
    } else {
        false
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    set
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_hash_key(db_idx: usize, key: &[u8]) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut to_del: Vec<[u8; COMPOSITE_KEY_LEN]> = Vec::new();
    let mut to_del_fields: Vec<Vec<u8>> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            let fb = unsafe { addr_of!((*entry).field) as *const u8 };
            let fs = unsafe { std::slice::from_raw_parts(fb, MAX_MEMBER_LEN) };
            let fe = fs.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
            to_del_fields.push(fs[..fe].to_vec());
            let mut k = [0u8; COMPOSITE_KEY_LEN];
            unsafe {
                std::ptr::copy_nonoverlapping(entry as *const u8, k.as_mut_ptr(), COMPOSITE_KEY_LEN)
            };
            to_del.push(k);
        }
    }
    let count = to_del.len() as i64;
    for (k, field_str) in to_del.iter().zip(to_del_fields.iter()) {
        unsafe { table.remove(k.as_ptr().cast()) };
        unsafe { hash_delete_overflow(overflow_htab, key, field_str) };
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

// ─────────────────────────── Set operations ─────────────────────────────────

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sadd(db_idx: usize, key: &[u8], members: &[&[u8]]) -> i64 {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut count = 0i64;
    for m in members {
        let k = make_composite_key(key, m);
        let (entry, found) = unsafe {
            enter_or_evict(&table, k.as_ptr().cast(), || {
                evict_set_key(htab, meta_htab, &key_hash(key))
            })
        };
        if !found && !entry.is_null() {
            count += 1;
        }
    }
    if count > 0 {
        let meta = unsafe {
            get_or_create_set_meta(meta_htab, key, || {
                evict_set_key(htab, meta_htab, &key_hash(key))
            })
        };
        if !meta.is_null() {
            let old = unsafe { (*meta).count };
            unsafe { addr_of_mut!((*meta).count).write(old + count) };
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_srem(db_idx: usize, key: &[u8], members: &[&[u8]]) -> i64 {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut count = 0i64;
    for m in members {
        let k = make_composite_key(key, m);
        let existed = unsafe { table.find(k.as_ptr().cast()) }.is_some();
        unsafe { table.remove(k.as_ptr().cast()) };
        if existed {
            count += 1;
        }
    }
    if count > 0 {
        let meta = unsafe { find_set_meta(meta_htab, key) };
        if !meta.is_null() {
            let old = unsafe { (*meta).count };
            let new_count = old - count;
            if new_count <= 0 {
                unsafe { remove_set_meta(meta_htab, key) };
            } else {
                unsafe { addr_of_mut!((*meta).count).write(new_count) };
            }
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sismember(db_idx: usize, key: &[u8], member: &[u8]) -> bool {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return false;
    };
    let lk = set_lwlock(db_idx);
    let k = make_composite_key(key, member);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let found = unsafe { table.find(k.as_ptr().cast()) }.is_some();
    unsafe { pg_sys::LWLockRelease(lk) };
    found
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_smismember(db_idx: usize, key: &[u8], members: &[&[u8]]) -> Vec<bool> {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return members.iter().map(|_| false).collect();
    };
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let results: Vec<bool> = members
        .iter()
        .map(|m| {
            let k = make_composite_key(key, m);
            unsafe { table.find(k.as_ptr().cast()) }.is_some()
        })
        .collect();
    unsafe { pg_sys::LWLockRelease(lk) };
    results
}

unsafe fn set_collect_members(htab: *mut pg_sys::HTAB, key: &[u8]) -> Vec<Vec<u8>> {
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let mut members = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        let mb = unsafe { addr_of!((*entry).member) as *const u8 };
        let ms = unsafe { std::slice::from_raw_parts(mb, MAX_MEMBER_LEN) };
        let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
        members.push(ms[..me].to_vec());
    }
    members
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_smembers(db_idx: usize, key: &[u8]) -> Vec<Vec<u8>> {
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut members = unsafe { set_collect_members(htab, key) };
    unsafe { pg_sys::LWLockRelease(lk) };
    members.sort();
    members
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_scard(db_idx: usize, key: &[u8]) -> i64 {
    let meta_htab = set_meta_htab_for(db_idx);
    if meta_htab.is_null() {
        return 0;
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let meta = unsafe { find_set_meta(meta_htab, key) };
    let count = if !meta.is_null() {
        unsafe { (*meta).count }
    } else {
        0
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_spop(db_idx: usize, key: &[u8], count: i64) -> Vec<Vec<u8>> {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let meta = unsafe { find_set_meta(meta_htab, key) };
    if meta.is_null() || unsafe { (*meta).count } == 0 {
        unsafe { pg_sys::LWLockRelease(lk) };
        return vec![];
    }

    let total = unsafe { (*meta).count };
    let n = count.min(total);
    let mut results = Vec::new();
    let mut remaining = total;
    let key_bytes = key;

    for _ in 0..n {
        if remaining == 0 {
            break;
        }
        let target_offset = (fast_random() % remaining as u64) as i64;
        let mut current_offset = 0i64;
        let mut to_remove: Option<[u8; COMPOSITE_KEY_LEN]> = None;
        let mut to_remove_member: Vec<u8> = Vec::new();

        let mut scan = unsafe { table.scan() };
        while let Some(entry) = unsafe { scan.next() } {
            if !hash_matches_entry(entry as *const u8, &key_hash(key_bytes)) {
                continue;
            }
            if current_offset == target_offset {
                let mb = unsafe { addr_of!((*entry).member) as *const u8 };
                let ms = unsafe { std::slice::from_raw_parts(mb, MAX_MEMBER_LEN) };
                let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
                let member = ms[..me].to_vec();
                let mut composite = [0u8; COMPOSITE_KEY_LEN];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        entry as *const u8,
                        composite.as_mut_ptr(),
                        COMPOSITE_KEY_LEN,
                    )
                };
                to_remove = Some(composite);
                to_remove_member = member;
                break;
            }
            current_offset += 1;
        }
        // scan drops here, auto-terminating if not fully consumed

        if let Some(composite) = to_remove {
            unsafe { table.remove(composite.as_ptr().cast()) };
            results.push(to_remove_member);
            remaining -= 1;
        }
    }

    if remaining == 0 {
        unsafe { remove_set_meta(meta_htab, key) };
    } else {
        unsafe { addr_of_mut!((*meta).count).write(remaining) };
    }

    unsafe { pg_sys::LWLockRelease(lk) };
    results
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_srandmember(db_idx: usize, key: &[u8], count: i64) -> Vec<Vec<u8>> {
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut members = unsafe { set_collect_members(htab, key) };
    unsafe { pg_sys::LWLockRelease(lk) };
    if count >= 0 {
        let take = (count as usize).min(members.len());
        members.truncate(take);
        members
    } else {
        let need = (-count) as usize;
        if members.is_empty() {
            return vec![];
        }
        let mut result = Vec::with_capacity(need);
        for i in 0..need {
            result.push(members[i % members.len()].clone());
        }
        result
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_smove(db_idx: usize, src: &[u8], dst: &[u8], member: &[u8]) -> bool {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return false;
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let lk = set_lwlock(db_idx);
    let src_k = make_composite_key(src, member);
    let dst_k = make_composite_key(dst, member);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let found = unsafe { table.find(src_k.as_ptr().cast()) }.is_some();
    unsafe { table.remove(src_k.as_ptr().cast()) };
    if found {
        let (dst_entry, dst_existed) = unsafe {
            enter_or_evict(&table, dst_k.as_ptr().cast(), || {
                evict_set_key(htab, meta_htab, &key_hash(dst))
            })
        };
        let dst_is_new = !dst_existed && !dst_entry.is_null();
        if !meta_htab.is_null() {
            let src_meta = unsafe { find_set_meta(meta_htab, src) };
            if !src_meta.is_null() {
                let old = unsafe { (*src_meta).count };
                let new_count = old - 1;
                if new_count <= 0 {
                    unsafe { remove_set_meta(meta_htab, src) };
                } else {
                    unsafe { addr_of_mut!((*src_meta).count).write(new_count) };
                }
            }
            if dst_is_new {
                let dst_meta = unsafe {
                    get_or_create_set_meta(meta_htab, dst, || {
                        evict_set_key(htab, meta_htab, &key_hash(dst))
                    })
                };
                if !dst_meta.is_null() {
                    let old = unsafe { (*dst_meta).count };
                    unsafe { addr_of_mut!((*dst_meta).count).write(old + 1) };
                }
            }
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    found
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sunion(db_idx: usize, keys: &[&[u8]]) -> Vec<Vec<u8>> {
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut all: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for k in keys {
        let members = unsafe { set_collect_members(htab, k) };
        all.extend(members);
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    let mut result: Vec<Vec<u8>> = all.into_iter().collect();
    result.sort();
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sinter(db_idx: usize, keys: &[&[u8]]) -> Vec<Vec<u8>> {
    if keys.is_empty() {
        return vec![];
    }
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let first: std::collections::HashSet<Vec<u8>> = unsafe { set_collect_members(htab, keys[0]) }
        .into_iter()
        .collect();
    let mut result: std::collections::HashSet<Vec<u8>> = first;
    for k in &keys[1..] {
        let other: std::collections::HashSet<Vec<u8>> = unsafe { set_collect_members(htab, k) }
            .into_iter()
            .collect();
        result = result.intersection(&other).cloned().collect();
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    let mut out: Vec<Vec<u8>> = result.into_iter().collect();
    out.sort();
    out
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sdiff(db_idx: usize, keys: &[&[u8]]) -> Vec<Vec<u8>> {
    if keys.is_empty() {
        return vec![];
    }
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let first: std::collections::HashSet<Vec<u8>> = unsafe { set_collect_members(htab, keys[0]) }
        .into_iter()
        .collect();
    let mut result = first;
    for k in &keys[1..] {
        let other: std::collections::HashSet<Vec<u8>> = unsafe { set_collect_members(htab, k) }
            .into_iter()
            .collect();
        result = result.difference(&other).cloned().collect();
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    let mut out: Vec<Vec<u8>> = result.into_iter().collect();
    out.sort();
    out
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sunionstore(db_idx: usize, dst: &[u8], keys: &[&[u8]]) -> i64 {
    let members = unsafe { mem_sunion(db_idx, keys) };
    unsafe { mem_del_set_key(db_idx, dst) };
    let refs: Vec<&[u8]> = members.iter().map(|s| s.as_slice()).collect();
    unsafe { mem_sadd(db_idx, dst, &refs) }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sinterstore(db_idx: usize, dst: &[u8], keys: &[&[u8]]) -> i64 {
    let members = unsafe { mem_sinter(db_idx, keys) };
    unsafe { mem_del_set_key(db_idx, dst) };
    let refs: Vec<&[u8]> = members.iter().map(|s| s.as_slice()).collect();
    unsafe { mem_sadd(db_idx, dst, &refs) }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sdiffstore(db_idx: usize, dst: &[u8], keys: &[&[u8]]) -> i64 {
    let members = unsafe { mem_sdiff(db_idx, keys) };
    unsafe { mem_del_set_key(db_idx, dst) };
    let refs: Vec<&[u8]> = members.iter().map(|s| s.as_slice()).collect();
    unsafe { mem_sadd(db_idx, dst, &refs) }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_set_key(db_idx: usize, key: &[u8]) -> i64 {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut to_del: Vec<[u8; COMPOSITE_KEY_LEN]> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            let mut k = [0u8; COMPOSITE_KEY_LEN];
            unsafe {
                std::ptr::copy_nonoverlapping(entry as *const u8, k.as_mut_ptr(), COMPOSITE_KEY_LEN)
            };
            to_del.push(k);
        }
    }
    let count = to_del.len() as i64;
    for k in &to_del {
        unsafe { table.remove(k.as_ptr().cast()) };
    }
    if count > 0 {
        unsafe { remove_set_meta(meta_htab, key) };
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

// ─────────────────────────── Sorted set operations ──────────────────────────

unsafe fn zset_collect(htab: *mut pg_sys::HTAB, key: &[u8]) -> Vec<(Vec<u8>, f64)> {
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let mut entries = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        let mb = unsafe { addr_of!((*entry).member) as *const u8 };
        let ms = unsafe { std::slice::from_raw_parts(mb, MAX_MEMBER_LEN) };
        let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
        let member = ms[..me].to_vec();
        let score = unsafe { (*entry).score };
        entries.push((member, score));
    }
    entries
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
#[allow(clippy::too_many_arguments)]
pub unsafe fn mem_zadd(
    db_idx: usize,
    key: &[u8],
    members: &[(f64, &[u8])],
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
) -> i64 {
    let htab = zset_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = zset_meta_htab_for(db_idx);
    let lk = zset_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut added = 0i64;
    let mut changed = 0i64;
    let meta: *mut ZsetMeta = if !meta_htab.is_null() {
        unsafe {
            get_or_create_zset_meta(meta_htab, key, || {
                evict_zset_key(htab, meta_htab, &key_hash(key))
            })
        }
    } else {
        std::ptr::null_mut()
    };
    for (score, member) in members {
        let k = make_composite_key(key, member);
        let (entry, found) = unsafe {
            enter_or_evict(&table, k.as_ptr().cast(), || {
                evict_zset_key(htab, meta_htab, &key_hash(key))
            })
        };
        if entry.is_null() {
            continue;
        }
        if !found {
            if xx {
                unsafe { table.remove(k.as_ptr().cast()) };
                continue;
            }
            unsafe { addr_of_mut!((*entry).score).write(*score) };
            added += 1;
            changed += 1;
            if !meta.is_null() {
                let count = unsafe { (*meta).count };
                if count == 0 || *score < unsafe { (*meta).min_score } {
                    unsafe { addr_of_mut!((*meta).min_score).write(*score) };
                    let len = unsafe { &mut *addr_of_mut!((*meta).min_member_len) };
                    unsafe { write_meta_member(&mut (*meta).min_member, len, member) };
                }
                if count == 0 || *score > unsafe { (*meta).max_score } {
                    unsafe { addr_of_mut!((*meta).max_score).write(*score) };
                    let len = unsafe { &mut *addr_of_mut!((*meta).max_member_len) };
                    unsafe { write_meta_member(&mut (*meta).max_member, len, member) };
                }
                unsafe { addr_of_mut!((*meta).count).write(count + 1) };
            }
        } else {
            if nx {
                continue;
            }
            let old_score = unsafe { (*entry).score };
            let should_update = if gt {
                *score > old_score
            } else if lt {
                *score < old_score
            } else {
                true
            };
            if should_update {
                if (old_score - *score).abs() > f64::EPSILON {
                    changed += 1;
                }
                unsafe { addr_of_mut!((*entry).score).write(*score) };
                if !meta.is_null() {
                    let cur_min = unsafe { (*meta).min_score };
                    let cur_max = unsafe { (*meta).max_score };
                    let was_min =
                        unsafe { read_meta_member(&(*meta).min_member, (*meta).min_member_len) }
                            == *member;
                    let was_max =
                        unsafe { read_meta_member(&(*meta).max_member, (*meta).max_member_len) }
                            == *member;
                    if *score < cur_min {
                        unsafe { addr_of_mut!((*meta).min_score).write(*score) };
                        let len = unsafe { &mut *addr_of_mut!((*meta).min_member_len) };
                        unsafe { write_meta_member(&mut (*meta).min_member, len, member) };
                    } else if was_min && *score > cur_min {
                        unsafe { refresh_zset_meta(htab, meta, key) };
                    }
                    if *score > cur_max {
                        unsafe { addr_of_mut!((*meta).max_score).write(*score) };
                        let len = unsafe { &mut *addr_of_mut!((*meta).max_member_len) };
                        unsafe { write_meta_member(&mut (*meta).max_member, len, member) };
                    } else if was_max && *score < cur_max {
                        unsafe { refresh_zset_meta(htab, meta, key) };
                    }
                }
            }
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    if ch { changed } else { added }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
#[allow(clippy::too_many_arguments)]
pub unsafe fn mem_zadd_incr(
    db_idx: usize,
    key: &[u8],
    delta: f64,
    member: &[u8],
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
) -> Option<f64> {
    let htab = zset_htab_for(db_idx);
    let table = unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }?;
    let lk = zset_lwlock(db_idx);
    let k = make_composite_key(key, member);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let meta_htab = zset_meta_htab_for(db_idx);
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_zset_key(htab, meta_htab, &key_hash(key))
        })
    };
    let result = if entry.is_null() {
        None
    } else if !found {
        if xx {
            unsafe { table.remove(k.as_ptr().cast()) };
            None
        } else {
            unsafe { addr_of_mut!((*entry).score).write(delta) };
            Some(delta)
        }
    } else if nx {
        None
    } else {
        let old = unsafe { (*entry).score };
        let new_score = old + delta;
        let should_update = if gt {
            new_score > old
        } else if lt {
            new_score < old
        } else {
            true
        };
        if should_update {
            unsafe { addr_of_mut!((*entry).score).write(new_score) };
            Some(new_score)
        } else {
            Some(old)
        }
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrem(db_idx: usize, key: &[u8], members: &[&[u8]]) -> i64 {
    let htab = zset_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = zset_meta_htab_for(db_idx);
    let lk = zset_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut count = 0i64;
    for m in members {
        let k = make_composite_key(key, m);
        let existed = unsafe { table.find(k.as_ptr().cast()) }.is_some();
        unsafe { table.remove(k.as_ptr().cast()) };
        if existed {
            count += 1;
        }
    }
    if count > 0 && !meta_htab.is_null() {
        let meta = unsafe { find_zset_meta(meta_htab, key) };
        if !meta.is_null() {
            let old_count = unsafe { (*meta).count };
            let new_count = old_count - count;
            if new_count <= 0 {
                unsafe { remove_zset_meta(meta_htab, key) };
            } else {
                unsafe { addr_of_mut!((*meta).count).write(new_count) };
                unsafe { refresh_zset_meta(htab, meta, key) };
            }
        }
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zscore(db_idx: usize, key: &[u8], member: &[u8]) -> Option<f64> {
    let htab = zset_htab_for(db_idx);
    let table = unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }?;
    let lk = zset_lwlock(db_idx);
    let k = make_composite_key(key, member);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let result = unsafe { table.find(k.as_ptr().cast()) }.map(|entry| unsafe { (*entry).score });
    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zcard(db_idx: usize, key: &[u8]) -> i64 {
    let meta_htab = zset_meta_htab_for(db_idx);
    if meta_htab.is_null() {
        return 0;
    }
    let lk = zset_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let meta = unsafe { find_zset_meta(meta_htab, key) };
    let count = if !meta.is_null() {
        unsafe { (*meta).count }
    } else {
        0
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zincrby(db_idx: usize, key: &[u8], delta: f64, member: &[u8]) -> f64 {
    unsafe { mem_zadd_incr(db_idx, key, delta, member, false, false, false, false) }
        .unwrap_or(delta)
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrank(
    db_idx: usize,
    key: &[u8],
    member: &[u8],
    rev: bool,
) -> Option<(i64, Option<f64>)> {
    let htab = zset_htab_for(db_idx);
    if htab.is_null() {
        return None;
    }
    let lk = zset_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut all = unsafe { zset_collect(htab, key) };
    unsafe { pg_sys::LWLockRelease(lk) };
    all.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    if rev {
        all.reverse();
    }
    for (i, (m, s)) in all.iter().enumerate() {
        if m == member {
            return Some((i as i64, Some(*s)));
        }
    }
    None
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zcount(
    db_idx: usize,
    key: &[u8],
    min: f64,
    max: f64,
    ex_min: bool,
    ex_max: bool,
) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let all = zset_collect(htab, key);
        pg_sys::LWLockRelease(lk);
        all.iter()
            .filter(|(_, s)| {
                let lo = if ex_min { *s > min } else { *s >= min };
                let hi = if ex_max { *s < max } else { *s <= max };
                lo && hi
            })
            .count() as i64
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrange_by_index(
    db_idx: usize,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    withscores: bool,
) -> Vec<(Vec<u8>, Option<f64>)> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let mut all = zset_collect(htab, key);
        pg_sys::LWLockRelease(lk);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        if rev {
            all.reverse();
        }
        let len = all.len();
        if len == 0 {
            return vec![];
        }
        let s = if start < 0 {
            (start + len as i64).max(0) as usize
        } else {
            start as usize
        };
        let e = if stop < 0 {
            (stop + len as i64) as usize
        } else {
            stop as usize
        };
        if s >= len || s > e {
            return vec![];
        }
        let e = e.min(len - 1);
        all[s..=e]
            .iter()
            .map(|(m, sc)| (m.clone(), if withscores { Some(*sc) } else { None }))
            .collect()
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
#[allow(clippy::too_many_arguments)]
pub unsafe fn mem_zrange_by_score(
    db_idx: usize,
    key: &[u8],
    min: f64,
    max: f64,
    ex_min: bool,
    ex_max: bool,
    rev: bool,
    limit: Option<(i64, i64)>,
) -> Vec<(Vec<u8>, Option<f64>)> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let mut all = zset_collect(htab, key);
        pg_sys::LWLockRelease(lk);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        if rev {
            all.reverse();
        }
        let mut filtered: Vec<(Vec<u8>, Option<f64>)> = all
            .into_iter()
            .filter(|(_, s)| {
                let lo = if ex_min { *s > min } else { *s >= min };
                let hi = if ex_max { *s < max } else { *s <= max };
                lo && hi
            })
            .map(|(m, s)| (m, Some(s)))
            .collect();
        if let Some((offset, count)) = limit {
            let off = offset.max(0) as usize;
            if off >= filtered.len() {
                return vec![];
            }
            filtered = filtered
                .into_iter()
                .skip(off)
                .take(count.max(0) as usize)
                .collect();
        }
        filtered
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrangebylex(
    db_idx: usize,
    key: &[u8],
    min: &crate::commands::LexBound,
    max: &crate::commands::LexBound,
    rev: bool,
    limit: Option<(i64, i64)>,
) -> Vec<Vec<u8>> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let mut all = zset_collect(htab, key);
        pg_sys::LWLockRelease(lk);
        all.sort_by(|a, b| a.0.cmp(&b.0));
        if rev {
            all.reverse();
        }
        let mut filtered: Vec<Vec<u8>> = all
            .into_iter()
            .filter(|(m, _)| lex_in_range(m, min, max))
            .map(|(m, _)| m)
            .collect();
        if let Some((offset, count)) = limit {
            let off = offset.max(0) as usize;
            if off >= filtered.len() {
                return vec![];
            }
            filtered = filtered
                .into_iter()
                .skip(off)
                .take(count.max(0) as usize)
                .collect();
        }
        filtered
    }
}

fn lex_in_range(
    m: &[u8],
    min: &crate::commands::LexBound,
    max: &crate::commands::LexBound,
) -> bool {
    use crate::commands::LexBound;
    let lo = match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Inclusive(s) => m >= s.as_slice(),
        LexBound::Exclusive(s) => m > s.as_slice(),
    };
    let hi = match max {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Inclusive(s) => m <= s.as_slice(),
        LexBound::Exclusive(s) => m < s.as_slice(),
    };
    lo && hi
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zlexcount(
    db_idx: usize,
    key: &[u8],
    min: &crate::commands::LexBound,
    max: &crate::commands::LexBound,
) -> i64 {
    unsafe { mem_zrangebylex(db_idx, key, min, max, false, None).len() as i64 }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zpopmin(db_idx: usize, key: &[u8], count: i64) -> Vec<(Vec<u8>, f64)> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        if !meta_htab.is_null() {
            let mut results = Vec::new();
            for _ in 0..count.max(0) {
                let meta = find_zset_meta(meta_htab, key);
                if meta.is_null() || (*meta).count == 0 {
                    break;
                }
                let min_score = (*meta).min_score;
                let min_len = (*meta).min_member_len;
                let min_member = read_meta_member(&(*meta).min_member, min_len);
                let k = make_composite_key(key, &min_member);
                let mut found = false;
                pg_sys::hash_search(
                    htab,
                    k.as_ptr().cast::<c_void>(),
                    pg_sys::HASHACTION::HASH_REMOVE,
                    &mut found,
                );
                results.push((min_member, min_score));
                let old_count = (*meta).count;
                let new_count = old_count - 1;
                if new_count == 0 {
                    remove_zset_meta(meta_htab, key);
                } else {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, meta, key);
                }
            }
            pg_sys::LWLockRelease(lk);
            return results;
        }

        let mut all = zset_collect(htab, key);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        let take = count.max(0) as usize;
        let chosen: Vec<(Vec<u8>, f64)> = all.into_iter().take(take).collect();
        for (m, _) in &chosen {
            let k = make_composite_key(key, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        pg_sys::LWLockRelease(lk);
        chosen.into_iter().collect()
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zpopmax(db_idx: usize, key: &[u8], count: i64) -> Vec<(Vec<u8>, f64)> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        if !meta_htab.is_null() {
            let mut results = Vec::new();
            for _ in 0..count.max(0) {
                let meta = find_zset_meta(meta_htab, key);
                if meta.is_null() || (*meta).count == 0 {
                    break;
                }
                let max_score = (*meta).max_score;
                let max_len = (*meta).max_member_len;
                let max_member = read_meta_member(&(*meta).max_member, max_len);
                let k = make_composite_key(key, &max_member);
                let mut found = false;
                pg_sys::hash_search(
                    htab,
                    k.as_ptr().cast::<c_void>(),
                    pg_sys::HASHACTION::HASH_REMOVE,
                    &mut found,
                );
                results.push((max_member, max_score));
                let old_count = (*meta).count;
                let new_count = old_count - 1;
                if new_count == 0 {
                    remove_zset_meta(meta_htab, key);
                } else {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, meta, key);
                }
            }
            pg_sys::LWLockRelease(lk);
            return results;
        }

        let mut all = zset_collect(htab, key);
        all.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        let take = count.max(0) as usize;
        let chosen: Vec<(Vec<u8>, f64)> = all.into_iter().take(take).collect();
        for (m, _) in &chosen {
            let k = make_composite_key(key, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        pg_sys::LWLockRelease(lk);
        chosen.into_iter().collect()
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrandmember(
    db_idx: usize,
    key: &[u8],
    count: i64,
    withscores: bool,
) -> Vec<(Vec<u8>, Option<f64>)> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let mut all = zset_collect(htab, key);
        pg_sys::LWLockRelease(lk);
        if all.is_empty() {
            return vec![];
        }
        if count >= 0 {
            let take = (count as usize).min(all.len());
            all.truncate(take);
            all.into_iter()
                .map(|(m, s)| (m, if withscores { Some(s) } else { None }))
                .collect()
        } else {
            let need = (-count) as usize;
            let len = all.len();
            (0..need)
                .map(|i| {
                    let (m, s) = &all[i % len];
                    (m.clone(), if withscores { Some(*s) } else { None })
                })
                .collect()
        }
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zremrangebyrank(db_idx: usize, key: &[u8], start: i64, stop: i64) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let mut all = zset_collect(htab, key);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        let len = all.len();
        if len == 0 {
            pg_sys::LWLockRelease(lk);
            return 0;
        }
        let s = if start < 0 {
            (start + len as i64).max(0) as usize
        } else {
            start as usize
        };
        let e = if stop < 0 {
            (stop + len as i64) as usize
        } else {
            stop as usize
        };
        let e = e.min(len - 1);
        if s >= len || s > e {
            pg_sys::LWLockRelease(lk);
            return 0;
        }
        let to_del: Vec<Vec<u8>> = all[s..=e].iter().map(|(m, _)| m.clone()).collect();
        for m in &to_del {
            let k = make_composite_key(key, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        if !to_del.is_empty() && !meta_htab.is_null() {
            let new_count = (len - to_del.len()) as i64;
            if new_count == 0 {
                remove_zset_meta(meta_htab, key);
            } else {
                let meta = find_zset_meta(meta_htab, key);
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, meta, key);
                }
            }
        }
        pg_sys::LWLockRelease(lk);
        to_del.len() as i64
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zremrangebyscore(
    db_idx: usize,
    key: &[u8],
    min: f64,
    max: f64,
    ex_min: bool,
    ex_max: bool,
) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let all = zset_collect(htab, key);
        let total = all.len();
        let to_del: Vec<Vec<u8>> = all
            .into_iter()
            .filter(|(_, s)| {
                let lo = if ex_min { *s > min } else { *s >= min };
                let hi = if ex_max { *s < max } else { *s <= max };
                lo && hi
            })
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(key, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        if !to_del.is_empty() && !meta_htab.is_null() {
            let new_count = (total - to_del.len()) as i64;
            if new_count == 0 {
                remove_zset_meta(meta_htab, key);
            } else {
                let meta = find_zset_meta(meta_htab, key);
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, meta, key);
                }
            }
        }
        pg_sys::LWLockRelease(lk);
        to_del.len() as i64
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zremrangebylex(
    db_idx: usize,
    key: &[u8],
    min: &crate::commands::LexBound,
    max: &crate::commands::LexBound,
) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let all = zset_collect(htab, key);
        let total = all.len();
        let to_del: Vec<Vec<u8>> = all
            .into_iter()
            .filter(|(m, _)| lex_in_range(m, min, max))
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(key, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        if !to_del.is_empty() && !meta_htab.is_null() {
            let new_count = (total - to_del.len()) as i64;
            if new_count == 0 {
                remove_zset_meta(meta_htab, key);
            } else {
                let meta = find_zset_meta(meta_htab, key);
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, meta, key);
                }
            }
        }
        pg_sys::LWLockRelease(lk);
        to_del.len() as i64
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zmsmembers(db_idx: usize, key: &[u8], members: &[&[u8]]) -> Vec<Option<f64>> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return members.iter().map(|_| None).collect();
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let results: Vec<Option<f64>> = members
            .iter()
            .map(|m| {
                let k = make_composite_key(key, m);
                let mut found = false;
                let entry = pg_sys::hash_search(
                    htab,
                    k.as_ptr().cast::<c_void>(),
                    pg_sys::HASHACTION::HASH_FIND,
                    &mut found,
                ) as *mut ZsetEntry;
                if found && !entry.is_null() {
                    Some((*entry).score)
                } else {
                    None
                }
            })
            .collect();
        pg_sys::LWLockRelease(lk);
        results
    }
}

fn apply_aggregate(existing: f64, new: f64, agg: crate::commands::Aggregate) -> f64 {
    match agg {
        crate::commands::Aggregate::Sum => existing + new,
        crate::commands::Aggregate::Min => existing.min(new),
        crate::commands::Aggregate::Max => existing.max(new),
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zunionstore(
    db_idx: usize,
    dst: &[u8],
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: crate::commands::Aggregate,
) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let mut map: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
        for (ki, k) in keys.iter().enumerate() {
            let w = weights.get(ki).copied().unwrap_or(1.0);
            let entries = zset_collect(htab, k);
            for (m, s) in entries {
                let weighted = s * w;
                map.entry(m)
                    .and_modify(|e| *e = apply_aggregate(*e, weighted, aggregate))
                    .or_insert(weighted);
            }
        }
        let to_del: Vec<Vec<u8>> = {
            let old = zset_collect(htab, dst);
            old.into_iter().map(|(m, _)| m).collect()
        };
        for m in &to_del {
            let k = make_composite_key(dst, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        let count = map.len() as i64;
        for (m, s) in &map {
            let k = make_composite_key(dst, m);
            let (entry, _found): (*mut ZsetEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_zset_key(htab, meta_htab, &key_hash(dst))
                });
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(meta_htab, dst);
            } else {
                let meta = get_or_create_zset_meta(meta_htab, dst, || {
                    evict_zset_key(htab, meta_htab, &key_hash(dst))
                });
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(count);
                    refresh_zset_meta(htab, meta, dst);
                }
            }
        }
        pg_sys::LWLockRelease(lk);
        count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zinterstore(
    db_idx: usize,
    dst: &[u8],
    keys: &[&[u8]],
    weights: &[f64],
    aggregate: crate::commands::Aggregate,
) -> i64 {
    unsafe {
        if keys.is_empty() {
            return 0;
        }
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let w0 = weights.first().copied().unwrap_or(1.0);
        let first: std::collections::HashMap<Vec<u8>, f64> = zset_collect(htab, keys[0])
            .into_iter()
            .map(|(m, s)| (m, s * w0))
            .collect();
        let mut result = first;
        for (ki, k) in keys[1..].iter().enumerate() {
            let w = weights.get(ki + 1).copied().unwrap_or(1.0);
            let other: std::collections::HashMap<Vec<u8>, f64> = zset_collect(htab, k)
                .into_iter()
                .map(|(m, s)| (m, s * w))
                .collect();
            result = result
                .into_iter()
                .filter_map(|(m, s)| {
                    other
                        .get(&m)
                        .map(|&os| (m, apply_aggregate(s, os, aggregate)))
                })
                .collect();
        }
        let to_del: Vec<Vec<u8>> = zset_collect(htab, dst)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(dst, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        let count = result.len() as i64;
        for (m, s) in &result {
            let k = make_composite_key(dst, m);
            let (entry, _found): (*mut ZsetEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_zset_key(htab, meta_htab, &key_hash(dst))
                });
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(meta_htab, dst);
            } else {
                let meta = get_or_create_zset_meta(meta_htab, dst, || {
                    evict_zset_key(htab, meta_htab, &key_hash(dst))
                });
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(count);
                    refresh_zset_meta(htab, meta, dst);
                }
            }
        }
        pg_sys::LWLockRelease(lk);
        count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zdiffstore(db_idx: usize, dst: &[u8], keys: &[&[u8]]) -> i64 {
    unsafe {
        if keys.is_empty() {
            return 0;
        }
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let first: std::collections::HashMap<Vec<u8>, f64> =
            zset_collect(htab, keys[0]).into_iter().collect();
        let mut result = first;
        for k in &keys[1..] {
            let other: std::collections::HashSet<Vec<u8>> =
                zset_collect(htab, k).into_iter().map(|(m, _)| m).collect();
            result.retain(|m, _| !other.contains(m));
        }
        let to_del: Vec<Vec<u8>> = zset_collect(htab, dst)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(dst, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        let count = result.len() as i64;
        for (m, s) in &result {
            let k = make_composite_key(dst, m);
            let (entry, _found): (*mut ZsetEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_zset_key(htab, meta_htab, &key_hash(dst))
                });
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(meta_htab, dst);
            } else {
                let meta = get_or_create_zset_meta(meta_htab, dst, || {
                    evict_zset_key(htab, meta_htab, &key_hash(dst))
                });
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(count);
                    refresh_zset_meta(htab, meta, dst);
                }
            }
        }
        pg_sys::LWLockRelease(lk);
        count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_zset_key(db_idx: usize, key: &[u8]) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let to_del: Vec<Vec<u8>> = zset_collect(htab, key)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(key, m);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
        if !to_del.is_empty() {
            remove_zset_meta(meta_htab, key);
        }
        pg_sys::LWLockRelease(lk);
        to_del.len() as i64
    }
}

// ─────────────────────────── List operations ────────────────────────────────

unsafe fn get_or_create_meta(
    meta_htab: *mut pg_sys::HTAB,
    key: &[u8],
    make_room: impl FnOnce() -> bool,
) -> *mut ListMeta {
    let Some(table) = (unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) }) else {
        return std::ptr::null_mut();
    };
    let key_buf = key_hash(key);
    let (meta, found) = unsafe { enter_or_evict(&table, key_buf.as_ptr().cast(), make_room) };
    if !meta.is_null() && !found {
        unsafe {
            addr_of_mut!((*meta).min_pos).write(0);
            addr_of_mut!((*meta).max_pos).write(0);
            addr_of_mut!((*meta).count).write(0);
        }
    }
    meta
}

unsafe fn find_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) -> *mut ListMeta {
    let Some(table) = (unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) }) else {
        return std::ptr::null_mut();
    };
    let key_buf = key_hash(key);
    unsafe { table.find(key_buf.as_ptr().cast()) }.unwrap_or(std::ptr::null_mut())
}

unsafe fn remove_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) {
    if let Some(table) = unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) } {
        let key_buf = key_hash(key);
        unsafe { table.remove(key_buf.as_ptr().cast()) };
    }
}

// ─────────────────────────── Random number generator ────────────────────────

pub fn fast_random() -> u64 {
    static SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut s = SEED.load(std::sync::atomic::Ordering::Relaxed);
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
    SEED.store(s, std::sync::atomic::Ordering::Relaxed);
    s
}

// ─────────────────────────── ZsetMeta helpers ───────────────────────────────

unsafe fn write_meta_member(dest: &mut [u8; MAX_MEMBER_LEN], len: &mut u16, member: &[u8]) {
    let mb = member;
    let ml = mb.len().min(MAX_MEMBER_LEN);
    dest[..ml].copy_from_slice(&mb[..ml]);
    if ml < MAX_MEMBER_LEN {
        dest[ml] = 0;
    }
    *len = ml as u16;
}

unsafe fn read_meta_member(src: &[u8; MAX_MEMBER_LEN], len: u16) -> Vec<u8> {
    let l = (len as usize).min(MAX_MEMBER_LEN);
    src[..l].to_vec()
}

unsafe fn get_or_create_zset_meta(
    meta_htab: *mut pg_sys::HTAB,
    key: &[u8],
    make_room: impl FnOnce() -> bool,
) -> *mut ZsetMeta {
    unsafe {
        let key_buf = key_hash(key);
        let (meta, found): (*mut ZsetMeta, bool) =
            enter_raw(meta_htab, key_buf.as_ptr().cast::<c_void>(), make_room);
        if !meta.is_null() && !found {
            addr_of_mut!((*meta).count).write(0);
            addr_of_mut!((*meta).min_score).write(f64::INFINITY);
            addr_of_mut!((*meta).max_score).write(f64::NEG_INFINITY);
            addr_of_mut!((*meta).min_member_len).write(0);
            addr_of_mut!((*meta).max_member_len).write(0);
        }
        meta
    }
}

unsafe fn find_zset_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) -> *mut ZsetMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = key_hash(key);
        let mut found = false;
        let meta = pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut ZsetMeta;
        if found { meta } else { std::ptr::null_mut() }
    }
}

unsafe fn remove_zset_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) {
    unsafe {
        if meta_htab.is_null() {
            return;
        }
        let key_buf = key_hash(key);
        let mut found = false;
        pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        );
    }
}

unsafe fn refresh_zset_meta(zset_htab: *mut pg_sys::HTAB, meta: *mut ZsetMeta, key: &[u8]) {
    unsafe {
        let mut new_min = f64::INFINITY;
        let mut new_max = f64::NEG_INFINITY;
        let mut min_member: Vec<u8> = Vec::new();
        let mut max_member: Vec<u8> = Vec::new();

        let mut status: pg_sys::HASH_SEQ_STATUS = std::mem::zeroed();
        pg_sys::hash_seq_init(&mut status, zset_htab);
        loop {
            let entry = pg_sys::hash_seq_search(&mut status) as *mut ZsetEntry;
            if entry.is_null() {
                break;
            }
            if !key_matches_entry(addr_of!((*entry).key) as *const u8, key) {
                continue;
            }
            let score = (*entry).score;
            let mb = addr_of!((*entry).member) as *const u8;
            let ms = std::slice::from_raw_parts(mb, MAX_MEMBER_LEN);
            let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_MEMBER_LEN);
            let member = ms[..me].to_vec();
            if score < new_min || (score == new_min && member < min_member) {
                new_min = score;
                min_member = member.clone();
            }
            if score > new_max || (score == new_max && member > max_member) {
                new_max = score;
                max_member = member;
            }
        }

        addr_of_mut!((*meta).min_score).write(new_min);
        addr_of_mut!((*meta).max_score).write(new_max);
        let min_len = &mut *addr_of_mut!((*meta).min_member_len);
        let max_len = &mut *addr_of_mut!((*meta).max_member_len);
        write_meta_member(&mut (*meta).min_member, min_len, &min_member);
        write_meta_member(&mut (*meta).max_member, max_len, &max_member);
    }
}

// ─────────────────────────── SetMeta helpers ────────────────────────────────

unsafe fn get_or_create_set_meta(
    meta_htab: *mut pg_sys::HTAB,
    key: &[u8],
    make_room: impl FnOnce() -> bool,
) -> *mut SetMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = key_hash(key);
        let (meta, found): (*mut SetMeta, bool) =
            enter_raw(meta_htab, key_buf.as_ptr().cast::<c_void>(), make_room);
        if !meta.is_null() && !found {
            addr_of_mut!((*meta).count).write(0);
        }
        meta
    }
}

unsafe fn find_set_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) -> *mut SetMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = key_hash(key);
        let mut found = false;
        let meta = pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut SetMeta;
        if found { meta } else { std::ptr::null_mut() }
    }
}

unsafe fn remove_set_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) {
    unsafe {
        if meta_htab.is_null() {
            return;
        }
        let key_buf = key_hash(key);
        let mut found = false;
        pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        );
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lpush(db_idx: usize, key: &[u8], values: &[&[u8]]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let overflow_htab = list_overflow_htab_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = get_or_create_meta(meta_htab, key, || {
            evict_list_key(htab, meta_htab, overflow_htab, &key_hash(key))
        });
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let current_count = (*meta).count;
        let current_min = if current_count == 0 {
            0
        } else {
            (*meta).min_pos
        };
        let current_max = if current_count == 0 {
            0
        } else {
            (*meta).max_pos
        };

        for (i, v) in values.iter().enumerate() {
            let pos = current_min - LIST_POS_STEP * (i as i64 + 1);
            let k = make_list_key(key, pos);
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(htab, meta_htab, overflow_htab, &key_hash(key))
                });
            if !entry.is_null() && !list_write_full_value(entry, overflow_htab, key, pos, v) {
                // The value did not fit; drop the slot rather than leave an
                // element that reads back empty.
                if let Some(t) = SharedTable::<ListEntry>::from_raw(htab) {
                    t.remove(k.as_ptr().cast::<c_void>());
                }
            }
        }

        let new_min = current_min - LIST_POS_STEP * values.len() as i64;
        addr_of_mut!((*meta).min_pos).write(new_min);
        if current_count == 0 {
            addr_of_mut!((*meta).max_pos)
                .write(new_min + LIST_POS_STEP * (values.len() as i64 - 1));
        } else {
            addr_of_mut!((*meta).max_pos).write(current_max);
        }
        let new_count = current_count + values.len() as i64;
        addr_of_mut!((*meta).count).write(new_count);

        pg_sys::LWLockRelease(lk);
        new_count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_rpush(db_idx: usize, key: &[u8], values: &[&[u8]]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let overflow_htab = list_overflow_htab_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = get_or_create_meta(meta_htab, key, || {
            evict_list_key(htab, meta_htab, overflow_htab, &key_hash(key))
        });
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let current_count = (*meta).count;
        let current_min = if current_count == 0 {
            0
        } else {
            (*meta).min_pos
        };
        let current_max = if current_count == 0 {
            0
        } else {
            (*meta).max_pos
        };

        for (i, v) in values.iter().enumerate() {
            let pos = current_max + LIST_POS_STEP * (i as i64 + 1);
            let k = make_list_key(key, pos);
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(htab, meta_htab, overflow_htab, &key_hash(key))
                });
            if !entry.is_null() && !list_write_full_value(entry, overflow_htab, key, pos, v) {
                // The value did not fit; drop the slot rather than leave an
                // element that reads back empty.
                if let Some(t) = SharedTable::<ListEntry>::from_raw(htab) {
                    t.remove(k.as_ptr().cast::<c_void>());
                }
            }
        }

        let new_max = current_max + LIST_POS_STEP * values.len() as i64;
        if current_count == 0 {
            addr_of_mut!((*meta).min_pos)
                .write(new_max - LIST_POS_STEP * (values.len() as i64 - 1));
        } else {
            addr_of_mut!((*meta).min_pos).write(current_min);
        }
        addr_of_mut!((*meta).max_pos).write(new_max);
        let new_count = current_count + values.len() as i64;
        addr_of_mut!((*meta).count).write(new_count);

        pg_sys::LWLockRelease(lk);
        new_count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lpushx(db_idx: usize, key: &[u8], values: &[&[u8]]) -> i64 {
    unsafe {
        let meta_htab = list_meta_htab_for(db_idx);
        if meta_htab.is_null() {
            return 0;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let meta = find_meta(meta_htab, key);
        let exists = !meta.is_null() && (*meta).count > 0;
        pg_sys::LWLockRelease(lk);
        if !exists {
            return 0;
        }
        mem_lpush(db_idx, key, values)
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_rpushx(db_idx: usize, key: &[u8], values: &[&[u8]]) -> i64 {
    unsafe {
        let meta_htab = list_meta_htab_for(db_idx);
        if meta_htab.is_null() {
            return 0;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let meta = find_meta(meta_htab, key);
        let exists = !meta.is_null() && (*meta).count > 0;
        pg_sys::LWLockRelease(lk);
        if !exists {
            return 0;
        }
        mem_rpush(db_idx, key, values)
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lpop(db_idx: usize, key: &[u8], count: Option<i64>) -> Vec<Vec<u8>> {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return vec![];
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }

        let current_count = (*meta).count;
        if current_count == 0 {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }

        let overflow_htab = list_overflow_htab_for(db_idx);
        let take = count.map(|c| c.max(0)).unwrap_or(1).min(current_count) as usize;
        let mut results = Vec::with_capacity(take);
        let mut pos = (*meta).min_pos;

        for _ in 0..take {
            let k = make_list_key(key, pos);
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut ListEntry;
            if found && !entry.is_null() {
                results.push(list_read_full_value(entry, overflow_htab, key, pos));
            }
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
            list_delete_overflow(overflow_htab, key, pos);
            pos += LIST_POS_STEP;
        }

        let new_count = current_count - take as i64;
        if new_count == 0 {
            remove_meta(meta_htab, key);
        } else {
            addr_of_mut!((*meta).min_pos).write(pos);
            addr_of_mut!((*meta).count).write(new_count);
        }

        pg_sys::LWLockRelease(lk);
        results
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_rpop(db_idx: usize, key: &[u8], count: Option<i64>) -> Vec<Vec<u8>> {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return vec![];
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }

        let current_count = (*meta).count;
        if current_count == 0 {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }

        let overflow_htab = list_overflow_htab_for(db_idx);
        let take = count.map(|c| c.max(0)).unwrap_or(1).min(current_count) as usize;
        let mut results = Vec::with_capacity(take);
        let mut pos = (*meta).max_pos;

        for _ in 0..take {
            let k = make_list_key(key, pos);
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut ListEntry;
            if found && !entry.is_null() {
                results.push(list_read_full_value(entry, overflow_htab, key, pos));
            }
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
            list_delete_overflow(overflow_htab, key, pos);
            pos -= LIST_POS_STEP;
        }

        let new_count = current_count - take as i64;
        if new_count == 0 {
            remove_meta(meta_htab, key);
        } else {
            addr_of_mut!((*meta).max_pos).write(pos);
            addr_of_mut!((*meta).count).write(new_count);
        }

        pg_sys::LWLockRelease(lk);
        results
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_llen(db_idx: usize, key: &[u8]) -> i64 {
    unsafe {
        let meta_htab = list_meta_htab_for(db_idx);
        if meta_htab.is_null() {
            return 0;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let meta = find_meta(meta_htab, key);
        let count = if meta.is_null() { 0 } else { (*meta).count };
        pg_sys::LWLockRelease(lk);
        count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lrange(db_idx: usize, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>> {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return vec![];
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }
        let count = (*meta).count;
        if count == 0 {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }

        let min_pos = (*meta).min_pos;
        let len = count as usize;
        let s = if start < 0 {
            (start + count).max(0) as usize
        } else {
            start as usize
        };
        let e = if stop < 0 {
            (stop + count) as usize
        } else {
            stop as usize
        };
        if s >= len || s > e {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }
        let e = e.min(len - 1);

        let overflow_htab = list_overflow_htab_for(db_idx);
        let mut results = Vec::with_capacity(e - s + 1);
        for i in s..=e {
            let pos = min_pos + i as i64 * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut ListEntry;
            if found && !entry.is_null() {
                results.push(list_read_full_value(entry, overflow_htab, key, pos));
            }
        }

        pg_sys::LWLockRelease(lk);
        results
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lindex(db_idx: usize, key: &[u8], index: i64) -> Option<Vec<u8>> {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return None;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return None;
        }
        let count = (*meta).count;
        if count == 0 {
            pg_sys::LWLockRelease(lk);
            return None;
        }

        let idx = if index < 0 { index + count } else { index };
        if idx < 0 || idx >= count {
            pg_sys::LWLockRelease(lk);
            return None;
        }

        let min_pos = (*meta).min_pos;
        let pos = min_pos + idx * LIST_POS_STEP;
        let k = make_list_key(key, pos);
        let mut found = false;
        let entry = pg_sys::hash_search(
            htab,
            k.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut ListEntry;
        let overflow_htab = list_overflow_htab_for(db_idx);
        let result = if found && !entry.is_null() {
            Some(list_read_full_value(entry, overflow_htab, key, pos))
        } else {
            None
        };

        pg_sys::LWLockRelease(lk);
        result
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lset(db_idx: usize, key: &[u8], index: i64, value: &[u8]) -> bool {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return false;
        }
        let overflow_htab = list_overflow_htab_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return false;
        }
        let count = (*meta).count;
        if count == 0 {
            pg_sys::LWLockRelease(lk);
            return false;
        }

        let idx = if index < 0 { index + count } else { index };
        if idx < 0 || idx >= count {
            pg_sys::LWLockRelease(lk);
            return false;
        }

        let min_pos = (*meta).min_pos;
        let pos = min_pos + idx * LIST_POS_STEP;
        let k = make_list_key(key, pos);
        let mut found = false;
        let entry = pg_sys::hash_search(
            htab,
            k.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut ListEntry;
        if found && !entry.is_null() {
            // A failed write blanks the entry and raises the OOM flag, which
            // turns the reply into an error regardless of what is returned here.
            let _ = list_write_full_value(entry, overflow_htab, key, pos, value);
        }
        pg_sys::LWLockRelease(lk);
        found
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
///
/// Every element of a list, head to tail. Caller holds the list lock.
///
/// Reads by position but tolerates gaps, so it stays correct even if a list is
/// left non-contiguous by something else.
unsafe fn list_elements(
    htab: *mut pg_sys::HTAB,
    overflow_htab: *mut pg_sys::HTAB,
    meta: *const ListMeta,
    key: &[u8],
) -> Vec<Vec<u8>> {
    unsafe {
        let (min_pos, max_pos) = ((*meta).min_pos, (*meta).max_pos);
        let mut out = Vec::with_capacity((*meta).count as usize);
        let mut pos = min_pos;
        while pos <= max_pos {
            let k = make_list_key(key, pos);
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut ListEntry;
            if found && !entry.is_null() {
                out.push(list_read_full_value(entry, overflow_htab, key, pos));
            }
            pos += LIST_POS_STEP;
        }
        out
    }
}

/// Replace a list's contents, renumbering positions from a fresh base.
///
/// Removing elements in place leaves holes, and every reader walks positions as
/// `min_pos + i * LIST_POS_STEP` — so a hole silently truncates the list. LREM
/// used to leave exactly that: it removed the right elements, reported the
/// right count, and made the tail unreachable. Rewriting the whole list is O(n)
/// on commands that were already O(n).
unsafe fn list_replace(
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    overflow_htab: *mut pg_sys::HTAB,
    key: &[u8],
    meta: *mut ListMeta,
    elems: &[Vec<u8>],
) {
    unsafe {
        // Clear the old span first; the new one reuses those positions.
        let (min_pos, max_pos) = ((*meta).min_pos, (*meta).max_pos);
        let mut pos = min_pos;
        while pos <= max_pos {
            let k = make_list_key(key, pos);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
            list_delete_overflow(overflow_htab, key, pos);
            pos += LIST_POS_STEP;
        }

        if elems.is_empty() {
            remove_meta(meta_htab, key);
            return;
        }

        for (i, v) in elems.iter().enumerate() {
            let pos = min_pos + (i as i64) * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(htab, meta_htab, overflow_htab, &key_hash(key))
                });
            if !entry.is_null()
                && !list_write_full_value(entry, overflow_htab, key, pos, v)
                && let Some(t) = SharedTable::<ListEntry>::from_raw(htab)
            {
                t.remove(k.as_ptr().cast::<c_void>());
            }
        }
        addr_of_mut!((*meta).min_pos).write(min_pos);
        addr_of_mut!((*meta).max_pos).write(min_pos + (elems.len() as i64 - 1) * LIST_POS_STEP);
        addr_of_mut!((*meta).count).write(elems.len() as i64);
    }
}

/// LINSERT: returns the new length, -1 if the pivot is absent, 0 if the key is.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_linsert(
    db_idx: usize,
    key: &[u8],
    before: bool,
    pivot: &[u8],
    value: &[u8],
) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() || (*meta).count == 0 {
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let overflow_htab = list_overflow_htab_for(db_idx);
        let mut elems = list_elements(htab, overflow_htab, meta, key);
        let result = match elems.iter().position(|e| e == pivot) {
            None => -1,
            Some(at) => {
                elems.insert(if before { at } else { at + 1 }, value.to_vec());
                list_replace(htab, meta_htab, overflow_htab, key, meta, &elems);
                elems.len() as i64
            }
        };

        pg_sys::LWLockRelease(lk);
        result
    }
}

pub unsafe fn mem_lrem(db_idx: usize, key: &[u8], count: i64, value: &[u8]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() || (*meta).count == 0 {
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let overflow_htab = list_overflow_htab_for(db_idx);
        let elems = list_elements(htab, overflow_htab, meta, key);

        // count > 0 removes from the head, count < 0 from the tail, 0 removes
        // every match. Marking rather than filtering keeps the tail-first case
        // from having to reverse the list and reverse it back.
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let mut doomed = vec![false; elems.len()];
        let mut removed = 0usize;
        let order: Box<dyn Iterator<Item = usize>> = if count < 0 {
            Box::new((0..elems.len()).rev())
        } else {
            Box::new(0..elems.len())
        };
        for i in order {
            if removed >= limit {
                break;
            }
            if elems[i] == value {
                doomed[i] = true;
                removed += 1;
            }
        }

        if removed > 0 {
            let kept: Vec<Vec<u8>> = elems
                .into_iter()
                .zip(&doomed)
                .filter(|(_, d)| !**d)
                .map(|(e, _)| e)
                .collect();
            list_replace(htab, meta_htab, overflow_htab, key, meta, &kept);
        }

        pg_sys::LWLockRelease(lk);
        removed as i64
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_ltrim(db_idx: usize, key: &[u8], start: i64, stop: i64) {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return;
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            pg_sys::LWLockRelease(lk);
            return;
        }

        let min_pos = (*meta).min_pos;
        let len = current_count as usize;
        let s = if start < 0 {
            (start + current_count).max(0) as usize
        } else {
            start as usize
        };
        let e = if stop < 0 {
            (stop + current_count) as usize
        } else {
            stop as usize
        };
        let e = e.min(len.saturating_sub(1));

        let overflow_htab = list_overflow_htab_for(db_idx);
        for i in 0..len {
            if i < s || i > e {
                let pos = min_pos + i as i64 * LIST_POS_STEP;
                let k = make_list_key(key, pos);
                let mut found = false;
                pg_sys::hash_search(
                    htab,
                    k.as_ptr().cast::<c_void>(),
                    pg_sys::HASHACTION::HASH_REMOVE,
                    &mut found,
                );
                list_delete_overflow(overflow_htab, key, pos);
            }
        }

        if s >= len || s > e {
            remove_meta(meta_htab, key);
        } else {
            let new_count = (e - s + 1) as i64;
            let new_min = min_pos + s as i64 * LIST_POS_STEP;
            let new_max = min_pos + e as i64 * LIST_POS_STEP;
            addr_of_mut!((*meta).min_pos).write(new_min);
            addr_of_mut!((*meta).max_pos).write(new_max);
            addr_of_mut!((*meta).count).write(new_count);
        }

        pg_sys::LWLockRelease(lk);
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lmove(
    db_idx: usize,
    src: &[u8],
    dst: &[u8],
    src_left: bool,
    dst_left: bool,
) -> Option<Vec<u8>> {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return None;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let src_meta = find_meta(meta_htab, src);
        if src_meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return None;
        }
        let src_count = (*src_meta).count;
        if src_count == 0 {
            pg_sys::LWLockRelease(lk);
            return None;
        }

        let src_min = (*src_meta).min_pos;
        let src_max = (*src_meta).max_pos;
        let src_pos = if src_left { src_min } else { src_max };

        let overflow_htab = list_overflow_htab_for(db_idx);
        let sk = make_list_key(src, src_pos);
        let mut found = false;
        let src_entry = pg_sys::hash_search(
            htab,
            sk.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut ListEntry;
        if !found || src_entry.is_null() {
            pg_sys::LWLockRelease(lk);
            return None;
        }
        let value = list_read_full_value(src_entry, overflow_htab, src, src_pos);
        pg_sys::hash_search(
            htab,
            sk.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        );
        list_delete_overflow(overflow_htab, src, src_pos);

        let new_src_count = src_count - 1;
        if new_src_count == 0 {
            remove_meta(meta_htab, src);
        } else {
            if src_left {
                addr_of_mut!((*src_meta).min_pos).write(src_min + LIST_POS_STEP);
            } else {
                addr_of_mut!((*src_meta).max_pos).write(src_max - LIST_POS_STEP);
            }
            addr_of_mut!((*src_meta).count).write(new_src_count);
        }

        let dst_meta = get_or_create_meta(meta_htab, dst, || {
            evict_list_key(htab, meta_htab, overflow_htab, &key_hash(dst))
        });
        if dst_meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return Some(value);
        }
        let dst_count = (*dst_meta).count;
        let dst_min = if dst_count == 0 {
            0
        } else {
            (*dst_meta).min_pos
        };
        let dst_max = if dst_count == 0 {
            0
        } else {
            (*dst_meta).max_pos
        };

        let dst_pos = if dst_left {
            if dst_count == 0 {
                0
            } else {
                dst_min - LIST_POS_STEP
            }
        } else if dst_count == 0 {
            0
        } else {
            dst_max + LIST_POS_STEP
        };

        let dk = make_list_key(dst, dst_pos);
        let (entry, _f2): (*mut ListEntry, bool) =
            enter_raw(htab, dk.as_ptr().cast::<c_void>(), || {
                evict_list_key(htab, meta_htab, overflow_htab, &key_hash(dst))
            });
        if !entry.is_null()
            && !list_write_full_value(entry, overflow_htab, dst, dst_pos, &value)
            && let Some(t) = SharedTable::<ListEntry>::from_raw(htab)
        {
            t.remove(dk.as_ptr().cast::<c_void>());
        }

        let new_dst_count = dst_count + 1;
        if dst_count == 0 {
            addr_of_mut!((*dst_meta).min_pos).write(dst_pos);
            addr_of_mut!((*dst_meta).max_pos).write(dst_pos);
        } else if dst_left {
            addr_of_mut!((*dst_meta).min_pos).write(dst_pos);
        } else {
            addr_of_mut!((*dst_meta).max_pos).write(dst_pos);
        }
        addr_of_mut!((*dst_meta).count).write(new_dst_count);

        pg_sys::LWLockRelease(lk);
        Some(value)
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lpos(
    db_idx: usize,
    key: &[u8],
    value: &[u8],
    rank: i64,
    count: Option<i64>,
) -> Vec<i64> {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return vec![];
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            pg_sys::LWLockRelease(lk);
            return vec![];
        }

        let min_pos = (*meta).min_pos;
        let mut indices: Vec<i64> = (0..current_count).collect();
        if rank < 0 {
            indices.reverse();
        }

        let overflow_htab = list_overflow_htab_for(db_idx);
        let abs_rank = rank.unsigned_abs() as usize;
        // COUNT 0 asks for every match, as in Redis; treating it as a literal
        // zero made `LPOS key v COUNT 0` return just the first.
        let limit = match count {
            Some(0) => usize::MAX,
            Some(c) => c.max(0) as usize,
            None => 1,
        };
        let mut skip = if abs_rank > 0 { abs_rank - 1 } else { 0 };
        let mut results = Vec::new();

        for logical_i in indices {
            let pos = min_pos + logical_i * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut ListEntry;
            if found && !entry.is_null() {
                let v = list_read_full_value(entry, overflow_htab, key, pos);
                if v == value {
                    if skip > 0 {
                        skip -= 1;
                        continue;
                    }
                    // `logical_i` is already the index from the head. A
                    // negative RANK only reverses the order of the search, not
                    // what each index means, so flipping it here reported the
                    // mirror of the right answer.
                    let display_idx = logical_i;
                    if count.is_some() {
                        results.push(display_idx);
                        if results.len() >= limit {
                            break;
                        }
                    } else {
                        results.push(display_idx);
                        break;
                    }
                }
            }
        }

        pg_sys::LWLockRelease(lk);
        results
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_list_key(db_idx: usize, key: &[u8]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let overflow_htab = list_overflow_htab_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return 0;
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            remove_meta(meta_htab, key);
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let min_pos = (*meta).min_pos;
        for i in 0..current_count {
            let pos = min_pos + i * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
            list_delete_overflow(overflow_htab, key, pos);
        }
        remove_meta(meta_htab, key);

        pg_sys::LWLockRelease(lk);
        current_count
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zset_collect_all(db_idx: usize, key: &[u8]) -> Vec<(Vec<u8>, f64)> {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return vec![];
        }
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let mut all = zset_collect(htab, key);
        pg_sys::LWLockRelease(lk);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        all
    }
}

// ─────────────── Random key ─────────────────────────────────────────────────

/// Returns a single arbitrary non-expired key, or None if the database is empty.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_random_key(db_idx: usize) -> Option<Vec<u8>> {
    unsafe {
        let htab = htab_for(db_idx);
        if htab.is_null() {
            return None;
        }
        let lk = lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED);
        let now = now_micros();
        let mut result: Option<Vec<u8>> = None;
        let mut status: pg_sys::HASH_SEQ_STATUS = std::mem::zeroed();
        pg_sys::hash_seq_init(&mut status, htab);
        loop {
            let entry = pg_sys::hash_seq_search(&mut status) as *mut KvEntry;
            if entry.is_null() {
                break;
            }
            let exp = (*entry).expires_at;
            if exp != 0 && exp <= now {
                continue;
            }
            let key_end = (*entry)
                .key
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(MAX_KEY_LEN);
            result = Some((&(*entry).key)[..key_end].to_vec());
            pg_sys::hash_seq_term(&mut status);
            break;
        }
        pg_sys::LWLockRelease(lk);
        result
    }
}

// ─────────────── Extended DEL (wipes all type tables for a key) ─────────────

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_all_types(db_idx: usize, key: &[u8]) -> i64 {
    unsafe {
        let sum = mem_del_hash_key(db_idx, key)
            + mem_del_set_key(db_idx, key)
            + mem_del_zset_key(db_idx, key)
            + mem_del_list_key(db_idx, key);
        sum.min(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The victim picker keeps the lowest-ranked `EVICT_BATCH` candidates out of
    /// an arbitrarily long scan without sorting the table — expired entries
    /// (rank 0) must survive that filter no matter when they are seen.
    #[test]
    fn the_eviction_sample_keeps_the_lowest_ranked_candidates() {
        let mut victims: Vec<(i64, [u8; 4])> = Vec::new();
        // Fill past the batch with progressively better candidates, so every
        // push after the first EVICT_BATCH has to displace something.
        for i in 0..(EVICT_BATCH as i64 * 3) {
            push_victim(&mut victims, i64::MAX - i, [0; 4]);
        }
        assert_eq!(victims.len(), EVICT_BATCH);
        let worst = victims.iter().map(|(r, _)| *r).max().unwrap();
        let best = victims.iter().map(|(r, _)| *r).min().unwrap();
        assert_eq!(best, i64::MAX - (EVICT_BATCH as i64 * 3 - 1));
        assert!(worst <= i64::MAX - (EVICT_BATCH as i64 * 2));

        // An expired entry arriving last still displaces a live one.
        push_victim(&mut victims, 0, [7; 4]);
        assert_eq!(victims.len(), EVICT_BATCH);
        assert!(victims.iter().any(|(r, k)| *r == 0 && *k == [7; 4]));
    }

    /// `noeviction` is the default because it is Redis's, and it is the only
    /// policy that must never remove live data.
    #[test]
    fn the_default_policy_evicts_nothing() {
        assert!(evict_policy() == EvictPolicy::NoEviction);
    }

    /// `composite_tail` reads the member half of a composite key and
    /// `composite_owner` the hash half — together they are how eviction finds
    /// every row belonging to a victim without ever seeing the Redis key.
    #[test]
    fn composite_keys_split_back_into_hash_and_member() {
        let k = make_composite_key(b"mykey", b"myfield");
        assert_eq!(composite_tail(&k), b"myfield");
        assert_eq!(unsafe { composite_owner(k.as_ptr()) }, key_hash(b"mykey"));
        assert!(hash_matches_entry(k.as_ptr(), &key_hash(b"mykey")));
        assert!(!hash_matches_entry(k.as_ptr(), &key_hash(b"otherkey")));

        // A member filling the slot exactly has no NUL to stop at.
        let full = vec![b'm'; MAX_MEMBER_LEN];
        let k = make_composite_key(b"k", &full);
        assert_eq!(composite_tail(&k), &full[..]);
    }

    /// SipHash-2-4 against the reference vectors from the original paper, key
    /// `000102...0f` over the messages `00`, `0001`, ... Getting this wrong
    /// would still "work" — every lookup would simply be self-consistent
    /// nonsense — so it is pinned against something external.
    #[test]
    fn siphash_matches_the_reference_vectors() {
        let (k0, k1) = (0x0706050403020100u64, 0x0f0e0d0c0b0a0908u64);
        let expected: [u64; 8] = [
            0x726fdb47dd0e0e31,
            0x74f839c593dc67fd,
            0x0d6c8009d9a94f5a,
            0x85676696d7fb7e2d,
            0xcf2794e0277187b7,
            0x18765564cd99a68d,
            0xcbc9466e58fee3ce,
            0xab0200f58b01d137,
        ];
        for (n, want) in expected.iter().enumerate() {
            let msg: Vec<u8> = (0..n as u8).collect();
            assert_eq!(siphash24(k0, k1, &msg), *want, "message length {n}");
        }
    }

    /// Distinct keys must land in distinct composite namespaces, or two hashes
    /// would silently share their fields.
    #[test]
    fn distinct_keys_hash_apart() {
        let a = key_hash(b"user:1");
        assert_ne!(a, key_hash(b"user:2"));
        assert_ne!(a, key_hash(b"user:1x"));
        assert_ne!(a, key_hash(b""));
        // Stable within a run — every worker must agree on the same handle.
        assert_eq!(a, key_hash(b"user:1"));
    }

    // Wildcard semantics are covered exhaustively by `pubsub::tests::glob_match_table`;
    // this only pins the adapter KEYS/SCAN go through.
    #[test]
    fn glob_matches_delegates_to_the_byte_matcher() {
        assert!(glob_matches(b"h*o", b"hello"));
        assert!(!glob_matches(b"h*o", b"world"));
    }

    /// Every one of these is multiplied by `redis.mem_max_entries` across eight
    /// databases and requested from shared memory *before* the postmaster starts.
    /// Growing one carelessly turns "the server is slower" into "the server does
    /// not boot", so changes here should be deliberate.
    #[test]
    fn entry_sizes_stay_within_their_shared_memory_budget() {
        let sizes = [
            ("KvEntry", size_of::<KvEntry>(), 600),
            ("KvOverflow", size_of::<KvOverflow>(), 960),
            ("HashEntry", size_of::<HashEntry>(), 216),
            ("HashOverflow", size_of::<HashOverflow>(), 592),
            ("SetEntry", size_of::<SetEntry>(), 144),
            ("ZsetEntry", size_of::<ZsetEntry>(), 152),
            ("ListEntry", size_of::<ListEntry>(), 96),
            ("ListOverflow", size_of::<ListOverflow>(), 472),
            ("ListMeta", size_of::<ListMeta>(), 40),
            ("ZsetMeta", size_of::<ZsetMeta>(), 304),
            ("SetMeta", size_of::<SetMeta>(), 24),
        ];
        for (name, actual, expected) in sizes {
            assert_eq!(
                actual, expected,
                "{name} changed size; recompute the shared-memory request in docs \
                 before accepting this"
            );
        }
        // At the 8192 default this totals ~202 MiB of shared memory.
        let per_entry: usize = sizes.iter().map(|(_, s, _)| s).sum();
        assert!(
            per_entry <= 8192,
            "combined entry size {per_entry} exceeds the budget"
        );
    }

    /// The HTAB key layouts must match the `keysize` values passed to
    /// `ShmemInitHash`, or lookups read past the end of the key.
    #[test]
    fn composite_key_layout_matches_its_parts() {
        assert_eq!(COMPOSITE_KEY_LEN, KEY_HASH_LEN + MAX_MEMBER_LEN);
        assert_eq!(LIST_KEY_LEN, KEY_HASH_LEN + 8);
        assert!(size_of::<HashEntry>() >= COMPOSITE_KEY_LEN);

        let composite = make_composite_key(b"mykey", b"myfield");
        assert_eq!(&composite[..KEY_HASH_LEN], &key_hash(b"mykey"));
        assert_eq!(&composite[KEY_HASH_LEN..KEY_HASH_LEN + 7], b"myfield");
        assert_eq!(composite[KEY_HASH_LEN + 7], 0, "member half is NUL-padded");

        let list_key = make_list_key(b"mykey", -42);
        assert_eq!(&list_key[..KEY_HASH_LEN], &key_hash(b"mykey"));
        assert_eq!(&list_key[KEY_HASH_LEN..], &(-42i64).to_le_bytes());
    }
}
