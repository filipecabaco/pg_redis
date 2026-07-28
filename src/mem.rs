use crate::htab::{LockGuard, SharedTable};
use pgrx::pg_sys;
use std::ffi::c_void;
use std::ptr::addr_of;
use std::ptr::addr_of_mut;

/// Maximum key length. A pool bound, not an entry-layout one: keys are
/// length-prefixed, so this is how much of the slab one key may take.
pub const MAX_KEY_LEN: usize = 512;
/// Maximum hash field / set member / sorted-set member length. A share-of-the-
/// pool bound like `MAX_KEY_LEN`, not the size of an array: members are
/// length-prefixed.
pub const MAX_MEMBER_LEN: usize = 512;
// HTAB key for the hash/set/zset tables: the redis key followed by the member.
/// Every table indexes on a 128-bit keyed hash of the Redis key, not the key
/// itself: a collection stores one entry per member and a copy of the key in
/// each would dominate it. Keyed with per-postmaster random material, so a
/// client cannot craft a collision. The `KvEntry` keeps the bytes alongside,
/// because `KEYS`, `SCAN` and `RANDOMKEY` hand real keys back.
const KEY_HASH_LEN: usize = 16;
type KeyHash = [u8; KEY_HASH_LEN];

/// HTAB key for the hash, set and zset tables: the key's hash then the
/// member's. The member bytes live in the entry.
const COMPOSITE_KEY_LEN: usize = KEY_HASH_LEN + KEY_HASH_LEN;
// HTAB key for the list tables: the redis key followed by the 8-byte position.
const LIST_KEY_LEN: usize = KEY_HASH_LEN + 8;
// Inline value bytes stored directly in the main HTAB entry.
const INLINE_VAL_LEN: usize = 64;

/// Key bytes stored in a `KvEntry`; the rest spills into the pool the values
/// share. 128 rather than 64 because a key past its prefix takes a chunk those
/// values need, and ordinary keys reach 90 bytes but not 129. See
/// docs/IMPLEMENTATION.md for the arithmetic.
const INLINE_KEY_LEN: usize = 128;

/// Member bytes kept in the entry; the rest spills into the table's pool.
/// Shorter than `INLINE_KEY_LEN` because these pools hold one chunk per entry
/// slot, so a member up to `INLINE_MEMBER_LEN + CHUNK_LEN` always fits. See
/// docs/IMPLEMENTATION.md.
const INLINE_MEMBER_LEN: usize = 48;
/// Bytes of a collection's *name* kept in its meta entry, the rest chained into
/// that type's existing pool. This is what lets `KEYS` and `SCAN` see a list or
/// a set at all: the entry tables store `(key hash, member)` and the directory
/// stores a hash, so a collection's name is written down nowhere else.
///
/// 40 rather than the key's 128 because there are four of these tables: eight
/// bytes here costs four entries at every key, and the tail has a pool to go to
/// that is already sized and already locked alongside.
const INLINE_META_KEY_LEN: usize = 40;
// Number of even databases: 0,2,4,6,8,10,12,14 → indices 0..7
/// One shared-memory database per entry in the ephemeral half; `db` indexes
/// these directly.
pub const NUM_MEM_DBS: usize = crate::commands::DURABLE_FROM as usize;
// Step between list positions for LPUSH/RPUSH.
const LIST_POS_STEP: i64 = 1024;

fn htab_init_size() -> i64 {
    crate::MEM_MAX_ENTRIES.get() as i64
}

fn htab_init_size_small() -> i64 {
    (crate::MEM_MAX_ENTRIES.get() / 2).max(256) as i64
}

/// The key directory holds one entry per key rather than per member, but every
/// table can fill independently and a member table's worst case is one key per
/// entry. Sized for that, so the directory is never what runs out first: a
/// refusal the operator cannot act on is worse than the memory it saves.
fn dir_init_size() -> i64 {
    htab_init_size() + htab_init_size_small() * 4
}

/// Fixed-size entry in the KV table. The key field MUST be first — HTAB reads
/// keysize bytes from the start. The key bytes are kept alongside the hash,
/// which is what lets a lookup verify the key it found.
#[repr(C)]
struct KvEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`. The HTAB lookup key.
    pub key: KeyHash,
    /// First `INLINE_KEY_LEN` bytes of the key. The rest is in the chunk pool.
    pub key_inline: [u8; INLINE_KEY_LEN],
    /// Inline value bytes (not null-terminated). Holds first INLINE_VAL_LEN bytes.
    pub value: [u8; INLINE_VAL_LEN],
    /// Expiry: microseconds since Unix epoch; 0 = no expiry.
    pub expires_at: i64,
    /// Total key length; anything past INLINE_KEY_LEN lives in the chunk pool.
    pub key_len: u32,
    /// First chunk of the key's tail, `NIL_CHUNK` when the key fits inline.
    pub key_overflow: u32,
    /// Total value length; anything past INLINE_VAL_LEN lives in the chunk pool.
    pub value_len: u32,
    /// First chunk of the tail in the database's value pool, `NIL_CHUNK` when
    /// the value fits inline.
    pub overflow: u32,
}

/// Fixed-size entry for the hash HTAB. HTAB key is (key hash, field hash).
#[repr(C)]
struct HashEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    /// SipHash of the field. The second half of the HTAB key.
    pub field: KeyHash,
    /// First `INLINE_MEMBER_LEN` bytes of the field.
    pub field_inline: [u8; INLINE_MEMBER_LEN],
    pub value: [u8; INLINE_VAL_LEN],
    /// Total field length; anything past the inline prefix is in the pool.
    pub field_len: u32,
    /// First chunk of the field's tail, `NIL_CHUNK` when it fits inline.
    pub field_overflow: u32,
    pub value_len: u32,
    /// See `KvEntry::overflow`.
    pub overflow: u32,
}

/// Fixed-size entry for the set HTAB. HTAB key is (key hash, member hash).
#[repr(C)]
struct SetEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    /// SipHash of the member. The second half of the HTAB key.
    pub member: KeyHash,
    pub member_inline: [u8; INLINE_MEMBER_LEN],
    pub member_len: u32,
    pub member_overflow: u32,
}

/// Fixed-size entry for the sorted set HTAB. HTAB key is (key hash, member hash).
#[repr(C)]
struct ZsetEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    /// SipHash of the member. The second half of the HTAB key.
    pub member: KeyHash,
    pub member_inline: [u8; INLINE_MEMBER_LEN],
    pub member_len: u32,
    pub member_overflow: u32,
    pub score: f64,
}

/// Fixed-size entry for the list HTAB. Key is (redis_key[128], pos_bytes[8]).
#[repr(C)]
struct ListEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub pos_bytes: [u8; 8],
    pub value: [u8; INLINE_VAL_LEN],
    pub value_len: u32,
    /// See `KvEntry::overflow`.
    pub overflow: u32,
}

/// Metadata entry for the list meta HTAB. Key is redis_key[128].
/// Tracks min/max position and count for O(1) LPUSH/RPUSH/LPOP/RPOP.
#[repr(C)]
struct ListMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub min_pos: i64,
    pub max_pos: i64,
    pub count: i64,
    /// The key's own bytes — see `INLINE_META_KEY_LEN`.
    pub name_inline: [u8; INLINE_META_KEY_LEN],
    pub name_len: u32,
    pub name_overflow: u32,
}

/// Metadata entry for the sorted set meta HTAB. Key is the Redis key's hash.
/// Tracks min/max score and members for O(1) ZPOPMIN/ZPOPMAX/ZCARD.
///
/// Extremes are named by member hash: it is the second half of the entry
/// table's HTAB key, so `ZPOPMIN` reaches its member with one `hash_search`.
#[repr(C)]
struct ZsetMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub count: i64,
    pub min_score: f64,
    pub max_score: f64,
    /// SipHash of the member holding the minimum score; the composite key of
    /// its entry is `(key, min_member)`.
    pub min_member: KeyHash,
    pub max_member: KeyHash,
    /// The key's own bytes — see `INLINE_META_KEY_LEN`.
    pub name_inline: [u8; INLINE_META_KEY_LEN],
    pub name_len: u32,
    pub name_overflow: u32,
}

/// Metadata entry for a table whose only per-key fact is how many members it
/// holds. The set and hash tables each have one, which is what makes `SCARD`
/// and `HLEN` O(1) and what tells the key directory when the last member of a
/// key has gone.
#[repr(C)]
struct CountMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub count: i64,
    /// The key's own bytes — see `INLINE_META_KEY_LEN`.
    pub name_inline: [u8; INLINE_META_KEY_LEN],
    pub name_len: u32,
    pub name_overflow: u32,
}

/// What a key holds. One key holds exactly one of these, which is what makes a
/// write of another type a `WRONGTYPE` rather than a second value beside the
/// first.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum KeyKind {
    String = 1,
    List = 2,
    Set = 3,
    Hash = 4,
    Zset = 5,
}

impl KeyKind {
    /// The name `TYPE` answers with.
    pub fn name(self) -> &'static str {
        match self {
            KeyKind::String => "string",
            KeyKind::List => "list",
            KeyKind::Set => "set",
            KeyKind::Hash => "hash",
            KeyKind::Zset => "zset",
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => KeyKind::String,
            2 => KeyKind::List,
            3 => KeyKind::Set,
            4 => KeyKind::Hash,
            5 => KeyKind::Zset,
            _ => return None,
        })
    }
}

/// One entry per live key, whatever type holds it. The directory is what lets
/// any table answer a question about a key it does not store: which type owns
/// it, and — for a collection, whose own tables have nowhere to put one — when
/// it expires.
#[repr(C)]
struct DirEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`. The HTAB lookup key.
    pub key: KeyHash,
    /// Microseconds since the Unix epoch; 0 = no expiry. The KV table keeps its
    /// own copy of a string's expiry; for the four collection types this is the
    /// only one there is.
    pub expires_at: i64,
    /// A `KeyKind` discriminant.
    pub kind: u8,
}

// ────────────────────────── The chunked value pool ──────────────────────────

/// Payload bytes in one pool chunk. Matches `INLINE_VAL_LEN`, so a value costs
/// `ceil((len - 64) / 64)` chunks and the arithmetic has one boundary, not two.
const CHUNK_LEN: usize = 64;

/// End of a chain, and the head of a value with no tail at all.
const NIL_CHUNK: u32 = u32::MAX;

/// A single value may claim at most this fraction of its table's pool.
///
/// Without a bound one `SET` could take every chunk in the database and leave
/// every other key with nowhere to spill.
const POOL_SHARE_PER_VALUE: usize = 8;

/// Ceiling on a value however large the pool is: every read copies it out
/// under the table's LWLock, so an unbounded one holds a database still for a
/// memcpy.
const MAX_VAL_CEILING: usize = 64 * 1024;

/// Chunks each pool is built with, one per entry slot. Only for the postmaster
/// paths that size and create the pools: reading a GUC is sound on the thread
/// that owns PostgreSQL's FFI and nowhere else.
fn configured_pool_chunks() -> usize {
    crate::MEM_MAX_ENTRIES.get().max(1) as usize
}

/// Chunks each member pool is built with: one per entry slot of the set and
/// zset tables, which is half what the value pools carry.
fn configured_member_pool_chunks() -> usize {
    (htab_init_size_small() as usize).max(1)
}

/// Chunks in each pool as allocated, read from the control block rather than
/// the GUC: it keeps the value cap agreeing with the pool that exists. Falls
/// back to the boot value when shared memory is not attached.
fn pool_chunks() -> usize {
    let c = ctl();
    if c.is_null() {
        return crate::MEM_MAX_ENTRIES_DEFAULT as usize;
    }
    unsafe { addr_of!((*c).pool_chunks).read() as usize }
}

/// The same, for the member pools — see `pool_chunks`.
fn member_pool_chunks() -> usize {
    let c = ctl();
    if c.is_null() {
        return configured_member_pool_chunks();
    }
    unsafe { addr_of!((*c).member_pool_chunks).read() as usize }
}

/// Longest value the shared-memory backend accepts.
///
/// A function of the pool rather than a constant: the largest value that cannot
/// starve the table it lives in, bounded above by `MAX_VAL_CEILING`.
pub fn max_total_val_len() -> usize {
    let share = (pool_chunks() / POOL_SHARE_PER_VALUE) * CHUNK_LEN;
    (INLINE_VAL_LEN + share).min(MAX_VAL_CEILING)
}

/// Header of one pool. The chunk links and the chunk payloads follow it in the
/// same shared-memory allocation:
///
/// ```text
/// [ValPool][next: u32 × capacity][data: u8 × capacity × CHUNK_LEN]
/// ```
///
/// `next` doubles as the free list and as a stored value's chain, so a chunk is
/// on exactly one of the two and no bitmap has to stay in step.
#[repr(C)]
pub struct ValPool {
    capacity: u32,
    free_head: u32,
    free_count: u32,
    _pad: u32,
}

/// Bytes one pool of `chunks` chunks occupies, header and links included.
fn val_pool_size(chunks: usize) -> usize {
    std::mem::size_of::<ValPool>() + chunks * (std::mem::size_of::<u32>() + CHUNK_LEN)
}

/// Chunks a value of `len` bytes needs beyond its inline slot.
fn chunks_for(len: usize) -> usize {
    chunks_beyond(len, INLINE_VAL_LEN)
}

/// Chunks a byte string of `len` needs beyond an inline slot of `inline`.
/// Keys and values have different inline slots — see `INLINE_KEY_LEN`.
fn chunks_beyond(len: usize, inline: usize) -> usize {
    len.saturating_sub(inline).div_ceil(CHUNK_LEN)
}

unsafe fn pool_links(pool: *mut ValPool) -> *mut u32 {
    unsafe { pool.add(1).cast::<u32>() }
}

unsafe fn chunk_data(pool: *mut ValPool, idx: u32) -> *mut u8 {
    unsafe {
        let capacity = (*pool).capacity as usize;
        pool_links(pool)
            .add(capacity)
            .cast::<u8>()
            .add(idx as usize * CHUNK_LEN)
    }
}

/// Thread every chunk onto the free list. Called once per pool, from the
/// postmaster's `shmem_startup_hook`.
///
/// # Safety
/// `pool` must point at `val_pool_size(capacity)` writable bytes.
unsafe fn pool_init(pool: *mut ValPool, capacity: usize) {
    let capacity = capacity.min(NIL_CHUNK as usize) as u32;
    unsafe {
        addr_of_mut!((*pool).capacity).write(capacity);
        addr_of_mut!((*pool).free_head).write(if capacity == 0 { NIL_CHUNK } else { 0 });
        addr_of_mut!((*pool).free_count).write(capacity);
        addr_of_mut!((*pool)._pad).write(0);
        let links = pool_links(pool);
        for i in 0..capacity {
            let next = if i + 1 == capacity { NIL_CHUNK } else { i + 1 };
            links.add(i as usize).write(next);
        }
    }
}

/// Free chunks left in the pool.
unsafe fn pool_free_chunks(pool: *mut ValPool) -> usize {
    if pool.is_null() {
        return 0;
    }
    unsafe { (*pool).free_count as usize }
}

/// Detach `n` chunks from the free list and return the head of the chain.
///
/// All or nothing: a pool that cannot supply every chunk hands back `None` with
/// its free list untouched, so a refused write never strands chunks.
unsafe fn pool_alloc(pool: *mut ValPool, n: usize) -> Option<u32> {
    if n == 0 {
        return Some(NIL_CHUNK);
    }
    if pool.is_null() {
        return None;
    }
    unsafe {
        if ((*pool).free_count as usize) < n {
            return None;
        }
        let links = pool_links(pool);
        let head = (*pool).free_head;
        // The free list is already a chain; taking a prefix of it needs one
        // pointer moved, not `n`.
        let mut tail = head;
        for _ in 1..n {
            tail = links.add(tail as usize).read();
        }
        let next_free = links.add(tail as usize).read();
        links.add(tail as usize).write(NIL_CHUNK);
        addr_of_mut!((*pool).free_head).write(next_free);
        addr_of_mut!((*pool).free_count).write((*pool).free_count - n as u32);
        Some(head)
    }
}

/// Return a chain to the free list. A no-op for `NIL_CHUNK`, so every caller
/// can hand over a value's head unconditionally.
unsafe fn pool_release(pool: *mut ValPool, head: u32) {
    if pool.is_null() || head == NIL_CHUNK {
        return;
    }
    unsafe {
        let capacity = (*pool).capacity;
        debug_assert!(head < capacity, "chunk index past the end of the pool");
        if head >= capacity {
            return;
        }
        let links = pool_links(pool);
        let mut tail = head;
        let mut n = 1u32;
        // Bounded by the capacity: a corrupted link cannot spin here forever.
        while n <= capacity {
            let next = links.add(tail as usize).read();
            if next == NIL_CHUNK || next >= capacity {
                break;
            }
            tail = next;
            n += 1;
        }
        links.add(tail as usize).write((*pool).free_head);
        addr_of_mut!((*pool).free_head).write(head);
        addr_of_mut!((*pool).free_count).write((*pool).free_count + n);
    }
}

/// Copy `bytes` into a freshly allocated chain.
unsafe fn pool_store(pool: *mut ValPool, bytes: &[u8]) -> Option<u32> {
    let head = unsafe { pool_alloc(pool, bytes.len().div_ceil(CHUNK_LEN)) }?;
    unsafe {
        let links = pool_links(pool);
        let mut idx = head;
        for part in bytes.chunks(CHUNK_LEN) {
            debug_assert!(idx != NIL_CHUNK, "chain shorter than the value it holds");
            std::ptr::copy_nonoverlapping(part.as_ptr(), chunk_data(pool, idx), part.len());
            idx = links.add(idx as usize).read();
        }
    }
    Some(head)
}

/// Append `len` bytes of the chain starting at `head` to `out`.
unsafe fn pool_read_into(pool: *mut ValPool, head: u32, len: usize, out: &mut Vec<u8>) {
    if pool.is_null() {
        return;
    }
    unsafe {
        let links = pool_links(pool);
        let capacity = (*pool).capacity;
        let mut idx = head;
        let mut left = len;
        while left > 0 && idx != NIL_CHUNK && idx < capacity {
            let n = left.min(CHUNK_LEN);
            out.extend_from_slice(std::slice::from_raw_parts(chunk_data(pool, idx), n));
            left -= n;
            idx = links.add(idx as usize).read();
        }
    }
}

/// Whether the chain at `head` holds exactly `expect`. The comparing form of
/// `pool_read_into`: it runs on every read path, so it must not allocate.
unsafe fn pool_chain_eq(pool: *mut ValPool, head: u32, expect: &[u8]) -> bool {
    if pool.is_null() {
        return expect.is_empty();
    }
    unsafe {
        let links = pool_links(pool);
        let capacity = (*pool).capacity;
        let mut idx = head;
        let mut rest = expect;
        while !rest.is_empty() {
            if idx == NIL_CHUNK || idx >= capacity {
                return false;
            }
            let n = rest.len().min(CHUNK_LEN);
            if std::slice::from_raw_parts(chunk_data(pool, idx), n) != &rest[..n] {
                return false;
            }
            rest = &rest[n..];
            idx = links.add(idx as usize).read();
        }
        true
    }
}

/// The fields that make up a stored byte string, wherever it lives. Naming the
/// trio once keeps the chunk lifecycle to one implementation. `inline_cap` is a
/// field because keys, values and members have different prefixes.
#[derive(Copy, Clone)]
struct ValueSlot {
    inline: *mut u8,
    inline_cap: usize,
    len: *mut u32,
    head: *mut u32,
}

unsafe fn kv_slot(entry: *mut KvEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).value).cast(),
            inline_cap: INLINE_VAL_LEN,
            len: addr_of_mut!((*entry).value_len),
            head: addr_of_mut!((*entry).overflow),
        }
    }
}

/// The key of a KV entry, as a `ValueSlot`. Disjoint from `kv_slot`, so a
/// caller may hold both at once.
unsafe fn key_slot(entry: *mut KvEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).key_inline).cast(),
            inline_cap: INLINE_KEY_LEN,
            len: addr_of_mut!((*entry).key_len),
            head: addr_of_mut!((*entry).key_overflow),
        }
    }
}

/// The name of a collection, as a `ValueSlot`. One per meta type, because the
/// three structs differ in everything but these three fields.
macro_rules! meta_name_slot {
    ($name:ident, $entry:ty) => {
        unsafe fn $name(entry: *mut $entry) -> ValueSlot {
            unsafe {
                ValueSlot {
                    inline: addr_of_mut!((*entry).name_inline).cast(),
                    inline_cap: INLINE_META_KEY_LEN,
                    len: addr_of_mut!((*entry).name_len),
                    head: addr_of_mut!((*entry).name_overflow),
                }
            }
        }
    };
}

meta_name_slot!(count_meta_name_slot, CountMeta);
meta_name_slot!(list_meta_name_slot, ListMeta);
meta_name_slot!(zset_meta_name_slot, ZsetMeta);

unsafe fn hash_slot(entry: *mut HashEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).value).cast(),
            inline_cap: INLINE_VAL_LEN,
            len: addr_of_mut!((*entry).value_len),
            head: addr_of_mut!((*entry).overflow),
        }
    }
}

/// The field of a hash entry, as a `ValueSlot`. Disjoint from `hash_slot`: a
/// `HashEntry` owns two chains, as a `KvEntry` does.
unsafe fn hash_field_slot(entry: *mut HashEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).field_inline).cast(),
            inline_cap: INLINE_MEMBER_LEN,
            len: addr_of_mut!((*entry).field_len),
            head: addr_of_mut!((*entry).field_overflow),
        }
    }
}

unsafe fn set_member_slot(entry: *mut SetEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).member_inline).cast(),
            inline_cap: INLINE_MEMBER_LEN,
            len: addr_of_mut!((*entry).member_len),
            head: addr_of_mut!((*entry).member_overflow),
        }
    }
}

unsafe fn zset_member_slot(entry: *mut ZsetEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).member_inline).cast(),
            inline_cap: INLINE_MEMBER_LEN,
            len: addr_of_mut!((*entry).member_len),
            head: addr_of_mut!((*entry).member_overflow),
        }
    }
}

unsafe fn list_slot(entry: *mut ListEntry) -> ValueSlot {
    unsafe {
        ValueSlot {
            inline: addr_of_mut!((*entry).value).cast(),
            inline_cap: INLINE_VAL_LEN,
            len: addr_of_mut!((*entry).value_len),
            head: addr_of_mut!((*entry).overflow),
        }
    }
}

/// Read a value back, inline part and chunked tail together.
unsafe fn value_read(pool: *mut ValPool, slot: ValueSlot) -> Vec<u8> {
    unsafe {
        let total = slot.len.read() as usize;
        let inline_len = total.min(slot.inline_cap);
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(std::slice::from_raw_parts(slot.inline, inline_len));
        if total > slot.inline_cap {
            pool_read_into(pool, slot.head.read(), total - slot.inline_cap, &mut out);
        }
        out
    }
}

/// Whether the slot holds exactly `expect`, without copying it out. Length
/// first, then the inline prefix, and only then the pool.
unsafe fn value_eq(pool: *mut ValPool, slot: ValueSlot, expect: &[u8]) -> bool {
    unsafe {
        let total = slot.len.read() as usize;
        if total != expect.len() {
            return false;
        }
        let inline_len = total.min(slot.inline_cap);
        if std::slice::from_raw_parts(slot.inline, inline_len) != &expect[..inline_len] {
            return false;
        }
        total <= slot.inline_cap
            || pool_chain_eq(pool, slot.head.read(), &expect[slot.inline_cap..])
    }
}

/// Release whatever the slot holds and mark it empty. Every path that drops or
/// overwrites a value goes through here; a leaked chain is invisible until the
/// pool runs dry.
unsafe fn value_free(pool: *mut ValPool, slot: ValueSlot) {
    unsafe {
        if slot.len.read() as usize > slot.inline_cap {
            pool_release(pool, slot.head.read());
        }
        slot.len.write(0);
        slot.head.write(NIL_CHUNK);
    }
}

/// Store `value`, spilling past the inline capacity. False with the slot left
/// empty when the pool cannot hold the tail. The slot is always written: a
/// fresh entry is recycled shared memory holding the last occupant's bytes.
unsafe fn value_write(pool: *mut ValPool, slot: ValueSlot, value: &[u8]) -> bool {
    let limit = max_total_val_len();
    debug_assert!(
        value.len() <= limit,
        "over-long value reached memory backend"
    );
    // The old chain goes back first, so overwriting a value in place reuses its
    // own chunks instead of needing the pool to hold both copies at once.
    unsafe { value_free(pool, slot) };
    if value.len() > limit {
        return false;
    }
    let inline_len = value.len().min(slot.inline_cap);
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), slot.inline, inline_len);
        slot.len.write(value.len() as u32);
        if value.len() > slot.inline_cap {
            let Some(head) = pool_store(pool, &value[slot.inline_cap..]) else {
                // `value_len` already promises a tail that has nowhere to live.
                slot.len.write(0);
                signal_oom();
                return false;
            };
            slot.head.write(head);
        }
    }
    true
}

/// Control block in static shared memory.
#[repr(C)]
pub struct MemControlBlock {
    /// SipHash key for `key_hash`, drawn from `pg_strong_random` once per
    /// postmaster. Shared memory does not survive a restart, so neither does
    /// this, and it never has to match anything on disk.
    pub hash_key: [u64; 4],
    /// Chunks each value pool was built with — see `pool_chunks`.
    pub pool_chunks: u32,
    /// Chunks each member pool was built with — see `configured_member_pool_chunks`.
    pub member_pool_chunks: u32,
    /// One LWLock per even-database — operations on db 0 and db 2 never block each other.
    pub lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub hash_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub set_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub zset_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub list_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    /// Guards the key directory. Always the innermost lock: a path that already
    /// holds a table's lock may take this one, never the other way round.
    pub dir_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    /// Handles to the 8 HTAB tables (one per ephemeral db).
    pub htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub hash_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub set_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub zset_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub list_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub list_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub zset_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub set_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    pub hash_meta_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    /// One entry per live key of any type — see `DirEntry`.
    pub dir_htab: [*mut pg_sys::HTAB; NUM_MEM_DBS],
    /// Chunk pools holding the tail of every value past `INLINE_VAL_LEN`.
    /// One per table per database, so each is covered by that table's LWLock.
    pub kv_pool: [*mut ValPool; NUM_MEM_DBS],
    pub hash_pool: [*mut ValPool; NUM_MEM_DBS],
    pub list_pool: [*mut ValPool; NUM_MEM_DBS],
    /// Tail of every member past `INLINE_MEMBER_LEN`. One pool each, not one
    /// shared: these two tables are under different LWLocks.
    pub set_pool: [*mut ValPool; NUM_MEM_DBS],
    pub zset_pool: [*mut ValPool; NUM_MEM_DBS],
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

/// Cache the control block pointer in this worker. The tables were created in
/// `shmem_startup_hook`; this only avoids going through `SHMEM_CTL` per call.
///
/// # Safety
/// Must be called from the bgworker main thread with a valid ctl pointer.
pub unsafe fn mem_init_worker(ctl_ptr: *mut MemControlBlock) {
    CTL_PTR.with(|c| c.set(ctl_ptr));
}

/// A stored counter, as Redis reads one: `i64::from_str` accepts `+1`, `007`
/// and `-0`, and Redis accepts only the canonical spelling.
pub fn parse_stored_int(bytes: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(bytes).ok()?;
    let digits = s.strip_prefix('-').unwrap_or(s);
    let canonical = match digits.as_bytes() {
        [] => false,
        [b'0'] => true,
        [b'1'..=b'9', rest @ ..] => rest.iter().all(u8::is_ascii_digit),
        _ => false,
    };
    // `-0` is not how Redis writes zero.
    if !canonical || s == "-0" {
        return None;
    }
    s.parse().ok()
}

/// The inclusive index range a range command selects, or `None` for none.
/// Negative indexes count back from the end; `(stop + len) as usize` on a stop
/// past the start wraps to a huge number and selects everything.
fn range_bounds(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let s = if start < 0 { start + len } else { start }.max(0);
    let e = if stop < 0 { stop + len } else { stop }.min(len - 1);
    (len > 0 && e >= 0 && s <= e).then_some((s as usize, e as usize))
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

/// SipHash-2-4 over `msg`, keyed by `(k0, k1)`. Keys are attacker-supplied, so
/// the hash must resist a crafted collision; FNV, used in `watch.rs` where a
/// collision only costs a spurious abort, would not.
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
fn key_hash(key: &[u8]) -> KeyHash {
    let k = hash_key_material();
    let mut out = [0u8; KEY_HASH_LEN];
    out[..8].copy_from_slice(&siphash24(k[0], k[1], key).to_le_bytes());
    out[8..].copy_from_slice(&siphash24(k[2], k[3], key).to_le_bytes());
    out
}

// ───────────────────────── Full tables and eviction ─────────────────────────

/// What to do when a table has no room. Named after Redis's `maxmemory-policy`,
/// and `noeviction` is the default there too.
#[derive(Copy, Clone, PartialEq)]
enum EvictPolicy {
    NoEviction,
    AllKeysRandom,
    VolatileTtl,
}

/// Split from the GUC read so a unit test can call it: reading a GUC off the
/// dispatcher thread panics inside pgrx.
fn policy_from(setting: Option<&str>) -> EvictPolicy {
    match setting {
        Some("allkeys-random") => EvictPolicy::AllKeysRandom,
        Some("volatile-ttl") => EvictPolicy::VolatileTtl,
        _ => EvictPolicy::NoEviction,
    }
}

fn evict_policy() -> EvictPolicy {
    policy_from(
        crate::MAXMEMORY_POLICY
            .get()
            .as_deref()
            .and_then(|s| s.to_str().ok()),
    )
}

/// Entries freed per eviction pass. Eviction costs one `hash_seq_search`, so a
/// batch amortises that scan over the next `EVICT_BATCH` inserts.
const EVICT_BATCH: usize = 64;

/// Why a shared-memory write was refused, when it was.
///
/// One value rather than a flag per cause, so reading it costs two
/// thread-local accesses per command however many causes there are.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Refusal {
    None,
    /// The table or its chunk pool had no room.
    OutOfMemory,
    /// The key hashed onto an entry holding a different key.
    KeyCollision,
}

thread_local! {
    /// Set when a write was refused, read and cleared by `execute_mem`. A flag
    /// rather than a result threaded through 68 entry points with five return
    /// types; a worker dispatches one command at a time, so it cannot cross
    /// commands.
    static REFUSED: std::cell::Cell<Refusal> = const { std::cell::Cell::new(Refusal::None) };
}

fn signal_oom() {
    REFUSED.set(Refusal::OutOfMemory);
}

fn signal_key_collision() {
    REFUSED.set(Refusal::KeyCollision);
}

/// Clears the flag and reports why the command just executed was refused, if it
/// was. Last cause wins.
pub fn take_refusal() -> Refusal {
    REFUSED.replace(Refusal::None)
}

/// Enter `key_ptr`, calling `make_room` and retrying once if the table is full.
/// A still-full table yields a null entry and raises the OOM flag, so a neutral
/// return is not mistaken for success.
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

/// Evict ahead of a write needing more chunks than the pool has left.
///
/// Eviction is otherwise driven by the table filling, so an exhausted pool
/// under a half-empty table would refuse every spilling write forever. Runs
/// before the entry exists: `make_room` may remove any entry, and must not be
/// able to remove the one about to be written.
unsafe fn reserve_chunks(pool: *mut ValPool, value_len: usize, make_room: impl FnOnce() -> bool) {
    if value_len > INLINE_VAL_LEN && unsafe { pool_free_chunks(pool) } < chunks_for(value_len) {
        make_room();
    }
}

/// `reserve_chunks` for the KV table, where a write may need chunks for the
/// value *and* — when the key is new and longer than `INLINE_KEY_LEN` — for the
/// key. Same contract: runs before the entry exists, and costs one comparison
/// under `noeviction`.
unsafe fn kv_reserve(
    pool: *mut ValPool,
    key: &[u8],
    value_len: usize,
    make_room: impl FnOnce() -> bool,
) {
    let need = chunks_beyond(key.len(), INLINE_KEY_LEN) + chunks_for(value_len);
    if need > 0 && unsafe { pool_free_chunks(pool) } < need {
        make_room();
    }
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

/// Free room in a KV table; caller holds the database's LWLock. Expired
/// entries go first under every policy — the sweep has not reached them yet —
/// and only if there are none does the policy get a say.
unsafe fn evict_kv(db_idx: usize, htab: *mut pg_sys::HTAB, pool: *mut ValPool) -> bool {
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
    // (expiry used for ranking, key hash). Expired entries rank first by using
    // 0. Eviction removes by the hash it read out of the entry, so it never
    // needs the key bytes and never has to verify them.
    let mut victims: Vec<(i64, KeyHash)> = Vec::with_capacity(EVICT_BATCH);
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
        unsafe { kv_remove(db_idx, table, pool, key_buf.as_ptr().cast()) };
    }
    !victims.is_empty()
}

/// Remove an entry and return its chunks. The chain heads live in the entry, so
/// the release happens while it is still there; every removal of an entry that
/// owns a chain goes through here. `free_of` because a `KvEntry` owns two.
unsafe fn remove_valued<E>(
    table: &SharedTable<E>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
    free_of: unsafe fn(*mut ValPool, *mut E),
) -> bool {
    unsafe {
        let Some(entry) = table.find(key_ptr) else {
            return false;
        };
        free_of(pool, entry);
        table.remove(key_ptr);
        true
    }
}

/// A KV entry owns two chains: its value's and its key's. Both have to go back,
/// and both before the entry does.
unsafe fn kv_free(pool: *mut ValPool, entry: *mut KvEntry) {
    unsafe {
        value_free(pool, kv_slot(entry));
        value_free(pool, key_slot(entry));
    }
}

/// A hash entry owns two chains as well: its value's and its field's.
unsafe fn hash_free(pool: *mut ValPool, entry: *mut HashEntry) {
    unsafe {
        value_free(pool, hash_slot(entry));
        value_free(pool, hash_field_slot(entry));
    }
}

unsafe fn set_free(pool: *mut ValPool, entry: *mut SetEntry) {
    unsafe { value_free(pool, set_member_slot(entry)) }
}

unsafe fn zset_free(pool: *mut ValPool, entry: *mut ZsetEntry) {
    unsafe { value_free(pool, zset_member_slot(entry)) }
}

unsafe fn list_free(pool: *mut ValPool, entry: *mut ListEntry) {
    unsafe { value_free(pool, list_slot(entry)) }
}

unsafe fn kv_remove(
    db_idx: usize,
    table: &SharedTable<KvEntry>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
) -> bool {
    let removed = unsafe { remove_valued(table, pool, key_ptr, kv_free) };
    if removed {
        let kh: KeyHash = unsafe { std::slice::from_raw_parts(key_ptr.cast::<u8>(), KEY_HASH_LEN) }
            .try_into()
            .expect("a KV lookup key is exactly one key hash");
        unsafe { dir_forget(db_idx, &kh) };
    }
    removed
}

unsafe fn hash_remove(
    table: &SharedTable<HashEntry>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
) -> bool {
    unsafe { remove_valued(table, pool, key_ptr, hash_free) }
}

unsafe fn list_remove(
    table: &SharedTable<ListEntry>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
) -> bool {
    unsafe { remove_valued(table, pool, key_ptr, list_free) }
}

/// Every set-entry removal goes through here, so no member's chain is dropped
/// without going back to the pool.
unsafe fn set_remove(
    table: &SharedTable<SetEntry>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
) -> bool {
    unsafe { remove_valued(table, pool, key_ptr, set_free) }
}

unsafe fn zset_remove(
    table: &SharedTable<ZsetEntry>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
) -> bool {
    unsafe { remove_valued(table, pool, key_ptr, zset_free) }
}

/// `zset_remove` for the sorted-set paths that carry a bare HTAB rather than a
/// `SharedTable`.
unsafe fn zset_remove_at(
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    k: &[u8; COMPOSITE_KEY_LEN],
) -> bool {
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return false;
    };
    unsafe { zset_remove(&table, pool, k.as_ptr().cast()) }
}

/// `list_remove` for the list paths that carry a bare HTAB rather than a
/// `SharedTable`. Every element removal goes through one of the two, so no
/// list position can be dropped without its chunks going back to the pool.
unsafe fn list_remove_at(
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    k: &[u8; LIST_KEY_LEN],
) -> bool {
    let Some(table) = (unsafe { SharedTable::<ListEntry>::from_raw(htab) }) else {
        return false;
    };
    unsafe { list_remove(&table, pool, k.as_ptr().cast()) }
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

/// Pick a key to evict, never `keep` — the one the caller is writing.
/// Collections go whole, as Redis does: dropping fields would leave a hash that
/// lost half its contents and still reports itself as present.
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

/// Remove a meta entry by key hash, freeing the chain holding the key's name.
///
/// Typed on the meta struct rather than erased: the entry owns that chain, and
/// a slot recycled with a live head in it is the invariant `kv_enter` depends
/// on — the next occupant would inherit chunks that are back in the free list.
unsafe fn remove_meta_of<M>(
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    kh: &KeyHash,
    name_slot: unsafe fn(*mut M) -> ValueSlot,
) {
    if let Some(table) = unsafe { SharedTable::<M>::from_raw(meta_htab) } {
        if let Some(meta) = unsafe { table.find(kh.as_ptr().cast()) } {
            unsafe { value_free(pool, name_slot(meta)) };
        }
        unsafe { table.remove(kh.as_ptr().cast()) };
    }
}

/// Every collection name in one database, read out of the four meta tables.
///
/// `KEYS` and `SCAN` used to see the KV table alone, so a list or a set was
/// reachable by name and invisible to anything enumerating. Each meta table has
/// exactly one entry per key of its type, which is what makes this a scan of
/// four small tables rather than of every member.
///
/// # Safety
/// Must be called from a bgworker thread after `mem_init_worker`, holding no
/// table lock: this takes four in turn.
unsafe fn collection_names(db_idx: usize, mut keep: impl FnMut(&[u8]) -> bool) -> Vec<Vec<u8>> {
    let mut out = Vec::new();

    /// One meta table: take its lock, read every name, release.
    macro_rules! names_from {
        ($meta:ty, $slot:ident, $htab:expr, $pool:expr, $lock:expr) => {{
            let (htab, pool, lk) = ($htab, $pool, $lock);
            if let Some(table) = unsafe { SharedTable::<$meta>::from_raw(htab) }
                && !lk.is_null()
            {
                let _guard = unsafe { LockGuard::shared(lk) };
                let mut scan = unsafe { table.scan() };
                while let Some(meta) = unsafe { scan.next() } {
                    let name = unsafe { value_read(pool, $slot(meta)) };
                    // Empty means the pool had no room to record it — see
                    // `meta_name_write`. It is not a key called "".
                    if !name.is_empty() && keep(&name) {
                        out.push(name);
                    }
                }
            }
        }};
    }

    names_from!(
        CountMeta,
        count_meta_name_slot,
        set_meta_htab_for(db_idx),
        set_pool_for(db_idx),
        set_lwlock(db_idx)
    );
    names_from!(
        CountMeta,
        count_meta_name_slot,
        hash_meta_htab_for(db_idx),
        hash_pool_for(db_idx),
        hash_lwlock(db_idx)
    );
    names_from!(
        ListMeta,
        list_meta_name_slot,
        list_meta_htab_for(db_idx),
        list_pool_for(db_idx),
        list_lwlock(db_idx)
    );
    names_from!(
        ZsetMeta,
        zset_meta_name_slot,
        zset_meta_htab_for(db_idx),
        zset_pool_for(db_idx),
        zset_lwlock(db_idx)
    );
    out
}

/// Write a collection's name into its freshly created meta entry.
///
/// Best effort by design: the name is not the only copy of anything, so a pool
/// with no room costs the key its place in `KEYS` and nothing else. Refusing
/// the write instead would undo members already stored, and leaving the field
/// uninitialised would hand `KEYS` a recycled name. Loud, because it means the
/// pool is exhausted and the operator has a decision to make.
///
/// # Safety
/// Caller holds the lock covering `pool` and the meta's table.
unsafe fn meta_name_write(pool: *mut ValPool, slot: ValueSlot, key: &[u8]) {
    if !unsafe { value_write(pool, slot, key) } {
        pgrx::warning!(
            "pg_redis: no room to record the name of a {}-byte key; it will not \
             appear in KEYS or SCAN until it is rewritten",
            key.len()
        );
    }
}

/// Chunks a meta entry needs to record `key`, so a caller can reserve for the
/// name alongside the member it is about to store.
fn meta_name_chunks(key: &[u8]) -> usize {
    chunks_beyond(key.len(), INLINE_META_KEY_LEN)
}

/// Delete everything one key holds in a member-keyed table, along with its meta
/// entry and its place in the directory. The four tables differ only in how wide
/// their lookup key is and in which chains a removal has to hand back.
///
/// # Safety
/// Caller holds that table's LWLock.
unsafe fn drop_key_from<E, M, const N: usize>(
    db_idx: usize,
    table: &SharedTable<E>,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    kh: &KeyHash,
    remove: unsafe fn(&SharedTable<E>, *mut ValPool, *const c_void) -> bool,
    name_slot: unsafe fn(*mut M) -> ValueSlot,
) -> bool {
    // Collected before any removal — `hash_search(HASH_REMOVE)` may not run
    // while a sequential scan over the same table is open.
    let keys: Vec<[u8; N]> = unsafe { entries_of(table, kh) };
    for k in &keys {
        unsafe { remove(table, pool, k.as_ptr().cast()) };
    }
    unsafe { remove_meta_of(meta_htab, pool, kh, name_slot) };
    unsafe { dir_forget(db_idx, kh) };
    !keys.is_empty()
}

/// Evict one whole key from a member-keyed table to make room for `keep`.
macro_rules! key_evictor {
    ($name:ident, $entry:ty, $keylen:expr, $remove:ident, $meta:ty, $slot:ident) => {
        unsafe fn $name(
            db_idx: usize,
            htab: *mut pg_sys::HTAB,
            meta_htab: *mut pg_sys::HTAB,
            pool: *mut ValPool,
            keep: &KeyHash,
        ) -> bool {
            let Some(table) = (unsafe { SharedTable::<$entry>::from_raw(htab) }) else {
                return false;
            };
            let Some(victim) = (unsafe { victim_key(&table, keep) }) else {
                return false;
            };
            unsafe {
                drop_key_from::<_, $meta, $keylen>(
                    db_idx, &table, meta_htab, pool, &victim, $remove, $slot,
                )
            }
        }
    };
}

key_evictor!(
    evict_hash_key,
    HashEntry,
    COMPOSITE_KEY_LEN,
    hash_remove,
    CountMeta,
    count_meta_name_slot
);
key_evictor!(
    evict_set_key,
    SetEntry,
    COMPOSITE_KEY_LEN,
    set_remove,
    CountMeta,
    count_meta_name_slot
);
key_evictor!(
    evict_zset_key,
    ZsetEntry,
    COMPOSITE_KEY_LEN,
    zset_remove,
    ZsetMeta,
    zset_meta_name_slot
);
key_evictor!(
    evict_list_key,
    ListEntry,
    LIST_KEY_LEN,
    list_remove,
    ListMeta,
    list_meta_name_slot
);

/// The control block holds one array per table per kind of handle, and every
/// accessor over them was the same six lines: null-check the block, read the
/// slot. Sixteen of those is fifteen chances to read the wrong array.
macro_rules! ctl_accessor {
    ($name:ident, $field:ident, $ty:ty) => {
        fn $name(db_idx: usize) -> $ty {
            let c = ctl();
            if c.is_null() {
                return std::ptr::null_mut();
            }
            unsafe { addr_of!((*c).$field[db_idx]).read() }
        }
    };
}

ctl_accessor!(kv_pool_for, kv_pool, *mut ValPool);
ctl_accessor!(hash_pool_for, hash_pool, *mut ValPool);
ctl_accessor!(list_pool_for, list_pool, *mut ValPool);
ctl_accessor!(set_pool_for, set_pool, *mut ValPool);
ctl_accessor!(zset_pool_for, zset_pool, *mut ValPool);
ctl_accessor!(htab_for, htab, *mut pg_sys::HTAB);
ctl_accessor!(hash_htab_for, hash_htab, *mut pg_sys::HTAB);
ctl_accessor!(set_htab_for, set_htab, *mut pg_sys::HTAB);
ctl_accessor!(zset_htab_for, zset_htab, *mut pg_sys::HTAB);
ctl_accessor!(list_htab_for, list_htab, *mut pg_sys::HTAB);
ctl_accessor!(list_meta_htab_for, list_meta_htab, *mut pg_sys::HTAB);
ctl_accessor!(zset_meta_htab_for, zset_meta_htab, *mut pg_sys::HTAB);
ctl_accessor!(set_meta_htab_for, set_meta_htab, *mut pg_sys::HTAB);
ctl_accessor!(hash_meta_htab_for, hash_meta_htab, *mut pg_sys::HTAB);
ctl_accessor!(dir_htab_for, dir_htab, *mut pg_sys::HTAB);
ctl_accessor!(lwlock, lwlock, *mut pg_sys::LWLock);
ctl_accessor!(hash_lwlock, hash_lwlock, *mut pg_sys::LWLock);
ctl_accessor!(set_lwlock, set_lwlock, *mut pg_sys::LWLock);
ctl_accessor!(zset_lwlock, zset_lwlock, *mut pg_sys::LWLock);
ctl_accessor!(list_lwlock, list_lwlock, *mut pg_sys::LWLock);
ctl_accessor!(dir_lwlock, dir_lwlock, *mut pg_sys::LWLock);

// ──────────────────────────── The key directory ─────────────────────────────
//
// "What does this key hold?" has no home in the five tables that hold the data:
// each is keyed independently and none of them asks the others, which is how a
// key came to hold a string and a list and a set at once. The directory is that
// home, and it is also the only place a collection's expiry can live — of the
// five, only the KV entry has a field for one.
//
// Lock order: the directory lock is the innermost one. A path already holding a
// table's lock may take it; nothing may take a table's lock while holding it.
// Every `dir_*` below acquires and releases it, so callers hold nothing extra.

/// Record that `key` now holds `kind` and carries no expiry. Only ever called
/// where a key comes into existence, which is why clearing the expiry is right:
/// a stale entry left by an eviction must not expire the value replacing it.
/// False when the directory has no room, which refuses the write.
unsafe fn dir_touch(db_idx: usize, kh: &KeyHash, kind: KeyKind) -> bool {
    let htab = dir_htab_for(db_idx);
    if htab.is_null() {
        return true;
    }
    let lk = dir_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut found = false;
    let entry = unsafe {
        pg_sys::hash_search(
            htab,
            kh.as_ptr().cast(),
            pg_sys::HASHACTION::HASH_ENTER_NULL,
            &mut found,
        ) as *mut DirEntry
    };
    if !entry.is_null() {
        unsafe { addr_of_mut!((*entry).expires_at).write(0) };
        unsafe { addr_of_mut!((*entry).kind).write(kind as u8) };
    }
    !entry.is_null()
}

/// Record that nothing holds `key` any more.
unsafe fn dir_forget(db_idx: usize, kh: &KeyHash) {
    let htab = dir_htab_for(db_idx);
    if htab.is_null() {
        return;
    }
    let lk = dir_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut found = false;
    unsafe {
        pg_sys::hash_search(
            htab,
            kh.as_ptr().cast(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        )
    };
}

/// What `key` holds and when it expires, or `None` when nothing holds it — an
/// entry whose expiry has passed reads as absent, as an expired `KvEntry` does.
unsafe fn dir_lookup(db_idx: usize, kh: &KeyHash) -> Option<(KeyKind, i64)> {
    let htab = dir_htab_for(db_idx);
    if htab.is_null() {
        return None;
    }
    let lk = dir_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut found = false;
    let entry = unsafe {
        pg_sys::hash_search(
            htab,
            kh.as_ptr().cast(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut DirEntry
    };
    let out = if entry.is_null() {
        None
    } else {
        let kind = KeyKind::from_u8(unsafe { (*entry).kind });
        kind.map(|k| (k, unsafe { (*entry).expires_at }))
    };
    match out {
        Some((_, exp)) if exp != 0 && exp <= now_micros() => None,
        other => other,
    }
}

/// Set or clear a key's expiry. False when the directory does not hold the key.
unsafe fn dir_set_expiry(db_idx: usize, kh: &KeyHash, expires_at: i64) -> bool {
    let htab = dir_htab_for(db_idx);
    if htab.is_null() {
        return false;
    }
    let lk = dir_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut found = false;
    let entry = unsafe {
        pg_sys::hash_search(
            htab,
            kh.as_ptr().cast(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut DirEntry
    };
    let set = !entry.is_null();
    if set {
        unsafe { addr_of_mut!((*entry).expires_at).write(expires_at) };
    }
    set
}

/// Every key whose expiry has passed, with the type that holds its data.
/// Collected under the directory lock and returned rather than acted on: the
/// deletion needs each type's lock, which may not be taken while this is held.
unsafe fn dir_expired(db_idx: usize) -> Vec<(KeyHash, KeyKind)> {
    let htab = dir_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<DirEntry>::from_raw(htab) }) else {
        return Vec::new();
    };
    let lk = dir_lwlock(db_idx);
    let now = now_micros();
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut out = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        let Some(kind) = KeyKind::from_u8(unsafe { (*entry).kind }) else {
            continue;
        };
        if exp != 0 && exp <= now {
            out.push((unsafe { addr_of!((*entry).key).read() }, kind));
        }
    }
    out
}

/// The full value of a KV entry, inline part and pooled tail together.
unsafe fn kv_read_full_value(entry: *mut KvEntry, pool: *mut ValPool) -> Vec<u8> {
    unsafe { value_read(pool, kv_slot(entry)) }
}

/// The inline head of a KV value, without copying. Only sound for values the
/// caller knows are short — `mem_incr` and friends, whose values are integers.
unsafe fn kv_read_inline_slice(entry: *const KvEntry) -> &'static [u8] {
    let total_len = unsafe { (*entry).value_len as usize };
    let inline_len = total_len.min(INLINE_VAL_LEN);
    let val_ptr = unsafe { addr_of!((*entry).value) as *const u8 };
    unsafe { std::slice::from_raw_parts(val_ptr, inline_len) }
}

/// Write value and expiry. False when the pool could not hold the tail, in
/// which case the entry is left empty.
///
/// The expiry is written twice on purpose: `GET` reads the entry's copy under
/// the KV lock alone, and every key-level question — `TTL`, `TYPE`, `EXISTS` —
/// is answered from the directory without knowing which table holds the key.
unsafe fn kv_write_full_value(
    db_idx: usize,
    entry: *mut KvEntry,
    kh: &KeyHash,
    pool: *mut ValPool,
    value: &[u8],
    expires_at: i64,
) -> bool {
    unsafe {
        let ok = value_write(pool, kv_slot(entry), value);
        addr_of_mut!((*entry).expires_at).write(expires_at);
        dir_set_expiry(db_idx, kh, expires_at);
        ok
    }
}

unsafe fn hash_read_full_value(entry: *mut HashEntry, pool: *mut ValPool) -> Vec<u8> {
    unsafe { value_read(pool, hash_slot(entry)) }
}

/// Returns false when the value's tail had nowhere to go; the caller must then
/// drop the entry it just created.
unsafe fn hash_write_full_value(entry: *mut HashEntry, pool: *mut ValPool, value: &[u8]) -> bool {
    unsafe { value_write(pool, hash_slot(entry), value) }
}

unsafe fn list_read_full_value(entry: *mut ListEntry, pool: *mut ValPool) -> Vec<u8> {
    unsafe { value_read(pool, list_slot(entry)) }
}

/// Returns false when the value's tail had nowhere to go; the caller must then
/// drop the entry it just created.
unsafe fn list_write_full_value(entry: *mut ListEntry, pool: *mut ValPool, value: &[u8]) -> bool {
    unsafe { value_write(pool, list_slot(entry), value) }
}

/// The HTAB key of a KV entry: the 128-bit handle, not the key itself. The
/// assertion backstops a command that grows a key argument without extending
/// `mem_too_long_error` — an over-long key hashes fine and would be refused as
/// an OOM rather than a limit.
fn make_key(s: &[u8]) -> KeyHash {
    debug_assert!(
        s.len() <= MAX_KEY_LEN,
        "over-long key reached memory backend"
    );
    key_hash(s)
}

/// Whether a KV entry holds exactly `key`. The bytes are there for `KEYS` and
/// `SCAN` anyway, so the check costs a length test and a prefix compare against
/// cache-warm bytes.
unsafe fn kv_key_matches(entry: *mut KvEntry, pool: *mut ValPool, key: &[u8]) -> bool {
    unsafe { value_eq(pool, key_slot(entry), key) }
}

/// Run `f` on a KV entry's key without copying it when it fits inline. `KEYS`
/// and `SCAN` glob every key to keep a few, so only a key past the prefix is
/// assembled and only a match is copied.
unsafe fn with_kv_key<R>(entry: *mut KvEntry, pool: *mut ValPool, f: impl FnOnce(&[u8]) -> R) -> R {
    unsafe {
        let total = (*entry).key_len as usize;
        if total <= INLINE_KEY_LEN {
            let inline = addr_of!((*entry).key_inline).cast::<u8>();
            f(std::slice::from_raw_parts(inline, total))
        } else {
            f(&value_read(pool, key_slot(entry)))
        }
    }
}

/// Find the entry for `key`, or `None` if it is absent — or if what is there
/// belongs to a different key that hashed the same.
unsafe fn kv_find(
    table: &SharedTable<KvEntry>,
    pool: *mut ValPool,
    kh: &KeyHash,
    key: &[u8],
) -> Option<*mut KvEntry> {
    unsafe {
        let entry = table.find(kh.as_ptr().cast())?;
        kv_key_matches(entry, pool, key).then_some(entry)
    }
}

/// Enter the entry for `key`, writing the bytes into a slot that did not hold
/// them. Null with a `Refusal` when the table is full, the tail has nowhere to
/// live, or the slot belongs to a different key that hashed the same — where
/// the stored key keeps its value.
unsafe fn kv_enter(
    db_idx: usize,
    table: &SharedTable<KvEntry>,
    pool: *mut ValPool,
    kh: &KeyHash,
    key: &[u8],
    make_room: impl FnOnce() -> bool,
) -> (*mut KvEntry, bool) {
    unsafe {
        let (entry, found) = enter_or_evict(table, kh.as_ptr().cast(), make_room);
        if entry.is_null() {
            return (entry, false);
        }
        if found {
            if kv_key_matches(entry, pool, key) {
                return (entry, true);
            }
            // Its own cause, not OOM: raising `redis.mem_max_entries` is the
            // obvious response to an OOM and does nothing for a collision.
            // The key already stored keeps its value.
            signal_key_collision();
            return (std::ptr::null_mut(), false);
        }
        // A fresh slot is recycled shared memory. Its chains were released by
        // whoever removed the last occupant, so `value_write` finds them empty;
        // the expiry is the one field no chain lifecycle clears, so it is
        // cleared here rather than left to each caller to remember.
        addr_of_mut!((*entry).expires_at).write(0);
        if !value_write(pool, key_slot(entry), key) {
            table.remove(kh.as_ptr().cast());
            return (std::ptr::null_mut(), false);
        }
        dir_touch(db_idx, kh, KeyKind::String);
        (entry, false)
    }
}

/// The entry for a member, or `None` when absent — or when the slot holds a
/// different member that hashed the same. What `kv_find` does, for members.
unsafe fn member_find<E>(
    table: &SharedTable<E>,
    pool: *mut ValPool,
    k: &[u8; COMPOSITE_KEY_LEN],
    member: &[u8],
    slot_of: unsafe fn(*mut E) -> ValueSlot,
) -> Option<*mut E> {
    unsafe {
        let entry = table.find(k.as_ptr().cast())?;
        value_eq(pool, slot_of(entry), member).then_some(entry)
    }
}

/// Enter the entry for a member, writing the bytes into a slot that did not
/// already hold them. Null with a `Refusal` raised, as `kv_enter`.
unsafe fn member_enter<E>(
    table: &SharedTable<E>,
    pool: *mut ValPool,
    k: &[u8; COMPOSITE_KEY_LEN],
    member: &[u8],
    slot_of: unsafe fn(*mut E) -> ValueSlot,
    make_room: impl FnOnce() -> bool,
) -> (*mut E, bool) {
    unsafe {
        let (entry, found) = enter_or_evict(table, k.as_ptr().cast(), make_room);
        if entry.is_null() {
            return (entry, false);
        }
        if found {
            if value_eq(pool, slot_of(entry), member) {
                return (entry, true);
            }
            signal_key_collision();
            return (std::ptr::null_mut(), false);
        }
        if !value_write(pool, slot_of(entry), member) {
            table.remove(k.as_ptr().cast());
            return (std::ptr::null_mut(), false);
        }
        (entry, false)
    }
}

/// Remove a member's entry, if the slot holds that member and not one that
/// collided with it. The chain goes back before the entry does.
unsafe fn member_remove<E>(
    table: &SharedTable<E>,
    pool: *mut ValPool,
    k: &[u8; COMPOSITE_KEY_LEN],
    member: &[u8],
    slot_of: unsafe fn(*mut E) -> ValueSlot,
    free_of: unsafe fn(*mut ValPool, *mut E),
) -> bool {
    unsafe {
        let Some(entry) = member_find(table, pool, k, member, slot_of) else {
            return false;
        };
        free_of(pool, entry);
        table.remove(k.as_ptr().cast());
        true
    }
}

/// The chunks a member needs beyond its inline prefix.
fn chunks_for_member(len: usize) -> usize {
    chunks_beyond(len, INLINE_MEMBER_LEN)
}

/// `reserve_chunks` for a write carrying a member as well as a value.
unsafe fn reserve_member_chunks(
    pool: *mut ValPool,
    key: &[u8],
    member: &[u8],
    value_len: usize,
    make_room: impl FnOnce() -> bool,
) {
    // The key's own chunks too: a write that creates the collection also writes
    // its name into the meta entry, out of this same pool. Counted whether or
    // not the key is new, which over-reserves by a few chunks on a key that
    // already exists and costs nothing when the pool is not tight.
    let need = chunks_for_member(member.len()) + chunks_for(value_len) + meta_name_chunks(key);
    if need > 0 && unsafe { pool_free_chunks(pool) } < need {
        make_room();
    }
}

// ───────────────────── Room for a whole multi-key write ─────────────────────
//
// Redis refuses a command it cannot fit rather than storing a prefix of it.
// Only under `noeviction` — the other policies make room at the insert — and
// only the keys actually absent are counted.

/// Whether `entries` slots and `chunks` chunks are free. Caller holds the lock.
unsafe fn fits(
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    capacity: usize,
    entries: usize,
    chunks: usize,
) -> bool {
    let used = unsafe { pg_sys::hash_get_num_entries(htab) } as usize;
    used + entries <= capacity && chunks <= unsafe { pool_free_chunks(pool) }
}

/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_room_for_kv(db_idx: usize, pairs: &[(&[u8], usize)]) -> bool {
    if evict_policy() != EvictPolicy::NoEviction {
        return true;
    }
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return true;
    };
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut entries = 0usize;
    let mut chunks = 0usize;
    for (key, value_len) in pairs {
        if unsafe { kv_find(&table, pool, &key_hash(key), key) }.is_none() {
            entries += 1;
            chunks += chunks_beyond(key.len(), INLINE_KEY_LEN);
        }
        chunks += chunks_for(*value_len);
    }
    unsafe { fits(htab, pool, htab_init_size() as usize, entries, chunks) }
}

/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_room_for_hash(db_idx: usize, key: &[u8], pairs: &[(&[u8], usize)]) -> bool {
    if evict_policy() != EvictPolicy::NoEviction {
        return true;
    }
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return true;
    };
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut entries = 0usize;
    let mut chunks = 0usize;
    for (field, value_len) in pairs {
        let k = make_composite_key(key, field);
        if unsafe { member_find(&table, pool, &k, field, hash_field_slot) }.is_none() {
            entries += 1;
            chunks += chunks_for_member(field.len());
        }
        chunks += chunks_for(*value_len);
    }
    unsafe { fits(htab, pool, htab_init_size_small() as usize, entries, chunks) }
}

/// Room in the set or sorted-set table for every member of one command.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_room_for_members(
    db_idx: usize,
    sorted: bool,
    key: &[u8],
    members: &[&[u8]],
) -> bool {
    if evict_policy() != EvictPolicy::NoEviction {
        return true;
    }
    let (htab, pool, lk) = if sorted {
        (
            zset_htab_for(db_idx),
            zset_pool_for(db_idx),
            zset_lwlock(db_idx),
        )
    } else {
        (
            set_htab_for(db_idx),
            set_pool_for(db_idx),
            set_lwlock(db_idx),
        )
    };
    if htab.is_null() {
        return true;
    }
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut entries = 0usize;
    let mut chunks = 0usize;
    for m in members {
        let k = make_composite_key(key, m);
        let present = if sorted {
            unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }
                .and_then(|t| unsafe { member_find(&t, pool, &k, m, zset_member_slot) })
                .is_some()
        } else {
            unsafe { SharedTable::<SetEntry>::from_raw(htab) }
                .and_then(|t| unsafe { member_find(&t, pool, &k, m, set_member_slot) })
                .is_some()
        };
        if !present {
            entries += 1;
            chunks += chunks_for_member(m.len());
        }
    }
    unsafe { fits(htab, pool, htab_init_size_small() as usize, entries, chunks) }
}

/// Room in the list table for every element of one push. Every element is a new
/// entry, so nothing here can already be present.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_room_for_list(db_idx: usize, value_lens: &[usize]) -> bool {
    if evict_policy() != EvictPolicy::NoEviction {
        return true;
    }
    let htab = list_htab_for(db_idx);
    if htab.is_null() {
        return true;
    }
    let pool = list_pool_for(db_idx);
    let lk = list_lwlock(db_idx);
    let chunks: usize = value_lens.iter().map(|n| chunks_for(*n)).sum();
    let _guard = unsafe { LockGuard::shared(lk) };
    unsafe {
        fits(
            htab,
            pool,
            htab_init_size_small() as usize,
            value_lens.len(),
            chunks,
        )
    }
}

/// GET: returns value if key exists and not expired.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_get(db_idx: usize, key: &[u8]) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    // Read under the shared lock, and reap under the exclusive one only if the
    // read found an expired entry — two scopes rather than taking the heavier
    // lock for a case that almost never happens. The entry is looked up again
    // because the lock was not held in between.
    let (result, was_expired) = {
        let _guard = unsafe { LockGuard::shared(lk) };
        match unsafe { kv_find(&table, pool, &key_buf, key) } {
            Some(entry) if unsafe { entry_is_expired(entry) } => (None, true),
            Some(entry) => (Some(unsafe { kv_read_full_value(entry, pool) }), false),
            None => (None, false),
        }
    };

    if was_expired {
        let _guard = unsafe { LockGuard::exclusive(lk) };
        if let Some(entry2) = unsafe { kv_find(&table, pool, &key_buf, key) }
            && unsafe { entry_is_expired(entry2) }
        {
            unsafe { kv_remove(db_idx, &table, pool, key_buf.as_ptr().cast()) };
        }
    }

    result
}

/// SET: upsert key→value with an expiry in microseconds since the epoch, 0 for
/// none. False when nothing was stored, which the caller must report rather
/// than reply +OK.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_set(db_idx: usize, key: &[u8], value: &[u8], expires_at_us: i64) -> bool {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return false;
    };
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    let _guard = unsafe { LockGuard::exclusive(lk) };

    unsafe { kv_reserve(pool, key, value.len(), || evict_kv(db_idx, htab, pool)) };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };
    let ok = !entry.is_null()
        && unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, value, expires_at_us) };
    if !ok && !found && !entry.is_null() {
        // A rejected SET must not leave a phantom empty key behind — and the
        // entry owns the key chain `kv_enter` just gave it.
        unsafe { kv_remove(db_idx, &table, pool, key_buf.as_ptr().cast()) };
    }

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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let mut count = 0i64;

    let _guard = unsafe { LockGuard::exclusive(lk) };
    for key in keys {
        let key_buf = make_key(key);
        if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) } {
            let expired = unsafe { entry_is_expired(entry) };
            unsafe { kv_remove(db_idx, &table, pool, key_buf.as_ptr().cast()) };
            if !expired {
                count += 1;
            }
        }
    }
    count
}

/// EXISTS: count how many of the given keys exist (non-expired).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_exists(db_idx: usize, keys: &[&[u8]]) -> i64 {
    keys.iter()
        .filter(|key| unsafe { dir_lookup(db_idx, &key_hash(key)) }.is_some())
        .count() as i64
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

    let _guard = unsafe { LockGuard::exclusive(lk) };

    let pool = kv_pool_for(db_idx);
    unsafe { kv_reserve(pool, key, 0, || evict_kv(db_idx, htab, pool)) };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };

    if entry.is_null() {
        Err("ERR out of memory".to_string())
    } else if !found || unsafe { entry_is_expired(entry) } {
        let new_val = delta;
        let s = new_val.to_string();
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, s.as_bytes(), 0) };
        Ok(new_val)
    } else {
        let slice = unsafe { kv_read_inline_slice(entry) };
        let current = parse_stored_int(slice);
        match current {
            None => Err("ERR value is not an integer or out of range".to_string()),
            Some(current) => match current.checked_add(delta) {
                None => Err("ERR increment or decrement would overflow".to_string()),
                Some(new_val) => {
                    let ns = new_val.to_string();
                    let exp = unsafe { (*entry).expires_at };
                    unsafe {
                        kv_write_full_value(db_idx, entry, &key_buf, pool, ns.as_bytes(), exp)
                    };
                    Ok(new_val)
                }
            },
        }
    }
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    let _guard = unsafe { LockGuard::exclusive(lk) };

    unsafe { kv_reserve(pool, key, 0, || evict_kv(db_idx, htab, pool)) };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };

    if entry.is_null() {
        Err("ERR out of memory".to_string())
    } else if !found || unsafe { entry_is_expired(entry) } {
        if !delta.is_finite() {
            return Err("ERR increment would produce NaN or Infinity".to_string());
        }
        let s = format_float(delta);
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, s.as_bytes(), 0) };
        Ok(s)
    } else {
        // Falls through to the release below on every path — see `mem_incr`.
        let slice = unsafe { kv_read_inline_slice(entry) };
        let current = std::str::from_utf8(slice)
            .ok()
            .and_then(|s| s.parse::<f64>().ok());
        match current {
            None => Err("ERR value is not a valid float".to_string()),
            Some(current) => {
                let new_val = current + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    Err("ERR increment would produce NaN or Infinity".to_string())
                } else {
                    let ns = format_float(new_val);
                    let exp = unsafe { (*entry).expires_at };
                    unsafe {
                        kv_write_full_value(db_idx, entry, &key_buf, pool, ns.as_bytes(), exp)
                    };
                    Ok(ns)
                }
            }
        }
    }
}

pub fn format_float(f: f64) -> String {
    format!("{}", f)
}

/// SETRANGE: overwrite from `offset`, padding the gap with NUL bytes as Redis
/// does. `None` when the result would pass the value cap, which the caller
/// turns into Redis's "string exceeds maximum allowed size".
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_setrange(db_idx: usize, key: &[u8], offset: usize, value: &[u8]) -> Option<i64> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);
    let end = offset.checked_add(value.len())?;
    if end > max_total_val_len() {
        return None;
    }

    let _guard = unsafe { LockGuard::exclusive(lk) };
    let pool = kv_pool_for(db_idx);
    unsafe { kv_reserve(pool, key, end, || evict_kv(db_idx, htab, pool)) };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };

    if entry.is_null() {
        None
    } else {
        let (mut buf, exp) = if found && !unsafe { entry_is_expired(entry) } {
            (unsafe { kv_read_full_value(entry, pool) }, unsafe {
                (*entry).expires_at
            })
        } else {
            (Vec::new(), 0)
        };
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[offset..end].copy_from_slice(value);
        let len = buf.len() as i64;
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, &buf, exp) };
        Some(len)
    }
}

/// SETBIT: read, modify and write the value under one lock, growing it with
/// NUL bytes to reach the bit. `None` when that growth would pass the value
/// cap; `Some(was)` otherwise, with the bit as it stood.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_setbit(
    db_idx: usize,
    key: &[u8],
    offset: i64,
    edit: impl FnOnce(&mut Vec<u8>) -> bool,
) -> Option<bool> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);
    let need = (offset / 8) as usize + 1;
    if need > max_total_val_len() {
        return None;
    }

    let _guard = unsafe { LockGuard::exclusive(lk) };
    let pool = kv_pool_for(db_idx);
    unsafe { kv_reserve(pool, key, need, || evict_kv(db_idx, htab, pool)) };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };

    if entry.is_null() {
        None
    } else {
        let (mut buf, exp) = if found && !unsafe { entry_is_expired(entry) } {
            (unsafe { kv_read_full_value(entry, pool) }, unsafe {
                (*entry).expires_at
            })
        } else {
            (Vec::new(), 0)
        };
        let was = edit(&mut buf);
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, &buf, exp) };
        Some(was)
    }
}

/// HINCRBYFLOAT: `mem_hincrby` over `f64`. The stored text is the answer, so
/// the caller returns it rather than reformatting.
///
/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hincrbyfloat(
    db_idx: usize,
    key: &[u8],
    field: &[u8],
    delta: f64,
) -> Result<String, String> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return Err("ERR memory not initialized".to_string());
    };
    let meta_htab = hash_meta_htab_for(db_idx);
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    unsafe {
        reserve_member_chunks(pool, key, field, 0, || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        member_enter(&table, pool, &k, field, hash_field_slot, || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };

    let result = if entry.is_null() {
        return Err("ERR out of memory".to_string());
    } else {
        let base = if found {
            let cur = unsafe { hash_read_full_value(entry, pool) };
            match std::str::from_utf8(&cur)
                .ok()
                .and_then(|t| t.trim().parse::<f64>().ok())
            {
                Some(v) if v.is_finite() => Some(v),
                _ => None,
            }
        } else {
            Some(0.0)
        };
        match base {
            None => Err("ERR hash value is not a float".to_string()),
            Some(base) if !(base + delta).is_finite() => {
                Err("ERR increment would produce NaN or Infinity".to_string())
            }
            Some(base) => {
                let text = format_float(base + delta);
                let _ = unsafe { hash_write_full_value(entry, pool, text.as_bytes()) };
                if !found {
                    unsafe {
                        count_meta_grow(db_idx, KeyKind::Hash, meta_htab, pool, key, 1, || {
                            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
                        })
                    };
                }
                Ok(text)
            }
        }
    };
    // A field entered for an increment that turned out to be invalid must not
    // be left behind as an empty one.
    if result.is_err() && !found {
        unsafe { hash_remove(&table, pool, k.as_ptr().cast()) };
    }
    result
}

/// GET+SET: set new value, return old value.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_getset(db_idx: usize, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    let _guard = unsafe { LockGuard::exclusive(lk) };

    unsafe { kv_reserve(pool, key, value.len(), || evict_kv(db_idx, htab, pool)) };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };

    let old = if found && !entry.is_null() && !unsafe { entry_is_expired(entry) } {
        Some(unsafe { kv_read_full_value(entry, pool) })
    } else {
        None
    };

    if !entry.is_null() {
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, value, 0) };
    }

    old
}

/// GETDEL: get and delete atomically.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_getdel(db_idx: usize, key: &[u8]) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    let _guard = unsafe { LockGuard::exclusive(lk) };

    if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) } {
        let val = if !unsafe { entry_is_expired(entry) } {
            Some(unsafe { kv_read_full_value(entry, pool) })
        } else {
            None
        };
        unsafe { kv_remove(db_idx, &table, pool, key_buf.as_ptr().cast()) };
        val
    } else {
        None
    }
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

    let _guard = unsafe { LockGuard::exclusive(lk) };

    let pool = kv_pool_for(db_idx);
    unsafe {
        kv_reserve(pool, key, suffix_bytes.len(), || {
            evict_kv(db_idx, htab, pool)
        })
    };
    let (entry, found) = unsafe {
        kv_enter(db_idx, &table, pool, &key_buf, key, || {
            evict_kv(db_idx, htab, pool)
        })
    };

    let limit = max_total_val_len();
    if entry.is_null() {
        0i64
    } else if !found || unsafe { entry_is_expired(entry) } {
        let len = suffix_bytes.len().min(limit);
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, &suffix_bytes[..len], 0) };
        len as i64
    } else {
        let existing_len = unsafe { (*entry).value_len as usize };
        let append_len = suffix_bytes.len().min(limit.saturating_sub(existing_len));
        let new_val_len = existing_len + append_len;
        let mut new_val = unsafe { kv_read_full_value(entry, pool) };
        new_val.extend_from_slice(&suffix_bytes[..append_len]);
        let exp = unsafe { (*entry).expires_at };
        unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, &new_val, exp) };
        new_val_len as i64
    }
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    let _guard = unsafe { LockGuard::shared(lk) };

    match unsafe { kv_find(&table, pool, &key_buf, key) } {
        Some(entry) if !unsafe { entry_is_expired(entry) } => unsafe { (*entry).value_len as i64 },
        _ => 0,
    }
}

/// TTL raw: return (exists: bool, expires_at_us: i64).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_ttl_raw(db_idx: usize, key: &[u8]) -> (bool, i64) {
    match unsafe { dir_lookup(db_idx, &key_hash(key)) } {
        Some((_, expires_at)) => (true, expires_at),
        None => (false, 0),
    }
}

/// Set expiry (absolute microseconds since epoch). Return true if key exists.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_set_expiry(db_idx: usize, key: &[u8], expires_at_us: i64) -> bool {
    unsafe { mem_write_expiry(db_idx, key, |_| Some(expires_at_us)) }
}

/// Remove expiry. Return true if key existed and had an expiry.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_persist(db_idx: usize, key: &[u8]) -> bool {
    unsafe { mem_write_expiry(db_idx, key, |old| (old != 0).then_some(0)) }
}

/// Move a live key's expiry to whatever `next` returns, or leave it alone when
/// that is `None`. False when the key is not there, or when `next` declined.
///
/// A string keeps a second copy in its `KvEntry` for the `GET` path, so this is
/// the one place that has to know a string from a collection at all.
unsafe fn mem_write_expiry(
    db_idx: usize,
    key: &[u8],
    next: impl FnOnce(i64) -> Option<i64>,
) -> bool {
    let kh = key_hash(key);
    let Some((kind, old)) = (unsafe { dir_lookup(db_idx, &kh) }) else {
        return false;
    };
    let Some(expires_at) = next(old) else {
        return false;
    };
    if kind == KeyKind::String {
        let htab = htab_for(db_idx);
        let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
            return false;
        };
        let pool = kv_pool_for(db_idx);
        let lk = lwlock(db_idx);
        let _guard = unsafe { LockGuard::exclusive(lk) };
        if let Some(entry) = unsafe { kv_find(&table, pool, &kh, key) } {
            unsafe { addr_of_mut!((*entry).expires_at).write(expires_at) };
        }
    }
    unsafe { dir_set_expiry(db_idx, &kh, expires_at) }
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);

    let _guard = unsafe { LockGuard::shared(lk) };

    keys.iter()
        .map(|key| {
            let key_buf = make_key(key);
            match unsafe { kv_find(&table, pool, &key_buf, key) } {
                Some(entry) if !unsafe { entry_is_expired(entry) } => {
                    Some(unsafe { kv_read_full_value(entry, pool) })
                }
                _ => None,
            }
        })
        .collect()
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);

    let _guard = unsafe { LockGuard::exclusive(lk) };

    for (key, value) in pairs {
        let key_buf = make_key(key);
        unsafe { kv_reserve(pool, key, value.len(), || evict_kv(db_idx, htab, pool)) };
        let (entry, _found) = unsafe {
            kv_enter(db_idx, &table, pool, &key_buf, key, || {
                evict_kv(db_idx, htab, pool)
            })
        };
        if !entry.is_null() {
            unsafe { kv_write_full_value(db_idx, entry, &key_buf, pool, value, 0) };
        }
    }
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let now = now_micros();
    let mut results = Vec::new();

    let guard = unsafe { LockGuard::shared(lk) };

    // The inline prefix is in the entry the scan just touched, so only a key
    // past `INLINE_KEY_LEN` walks the pool.
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        if exp != 0 && exp <= now {
            continue;
        }
        let matched = unsafe {
            with_kv_key(entry, pool, |k| {
                glob_matches(pattern, k).then(|| k.to_vec())
            })
        };
        if let Some(k) = matched {
            results.push(k);
        }
    }
    // Dropped explicitly: `collection_names` takes four locks of its own, and
    // holding the KV one while it does would put two orders in play.
    drop(guard);

    results.extend(unsafe { collection_names(db_idx, |k| glob_matches(pattern, k)) });
    results
}

/// DBSIZE: count non-expired keys.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_dbsize(db_idx: usize) -> i64 {
    // The directory, not the KV table: it holds one entry per live key of every
    // type, where counting strings alone reported 1 for a database holding a
    // string, a list, a hash, a set and a sorted set.
    let htab = dir_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<DirEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = dir_lwlock(db_idx);
    let now = now_micros();
    let mut count = 0i64;

    let _guard = unsafe { LockGuard::shared(lk) };
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        if exp == 0 || exp > now {
            count += 1;
        }
    }
    count
}

/// Delete everything `kh` holds, whatever type holds it, taking that type's
/// LWLock for the duration.
///
/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the
///   thread-local CTL_PTR.
/// - No table lock may be held: this takes one, and a path holding a table lock
///   may only ever descend to the directory.
unsafe fn mem_drop_key(db_idx: usize, kind: KeyKind, kh: &KeyHash) {
    unsafe fn under<T>(lk: *mut pg_sys::LWLock, body: impl FnOnce() -> T) -> T {
        let _guard = unsafe { LockGuard::exclusive(lk) };
        body()
    }

    macro_rules! drop_collection {
        ($entry:ty, $keylen:expr, $remove:ident, $metaty:ty, $slot:ident,
         $htab:expr, $meta:expr, $pool:expr, $lock:expr) => {{
            let htab = $htab;
            let (meta_htab, pool, lk) = ($meta, $pool, $lock);
            if let Some(table) = unsafe { SharedTable::<$entry>::from_raw(htab) } {
                unsafe {
                    under(lk, || {
                        drop_key_from::<_, $metaty, $keylen>(
                            db_idx, &table, meta_htab, pool, kh, $remove, $slot,
                        )
                    })
                };
            }
        }};
    }

    match kind {
        KeyKind::String => {
            let htab = htab_for(db_idx);
            if let Some(table) = unsafe { SharedTable::<KvEntry>::from_raw(htab) } {
                let pool = kv_pool_for(db_idx);
                unsafe {
                    under(lwlock(db_idx), || {
                        kv_remove(db_idx, &table, pool, kh.as_ptr().cast())
                    })
                };
            }
            // A KV entry the eviction sampler already took leaves the directory
            // holding a key with no value, which nothing else would clear.
            unsafe { dir_forget(db_idx, kh) };
        }
        KeyKind::Hash => drop_collection!(
            HashEntry,
            COMPOSITE_KEY_LEN,
            hash_remove,
            CountMeta,
            count_meta_name_slot,
            hash_htab_for(db_idx),
            hash_meta_htab_for(db_idx),
            hash_pool_for(db_idx),
            hash_lwlock(db_idx)
        ),
        KeyKind::Set => drop_collection!(
            SetEntry,
            COMPOSITE_KEY_LEN,
            set_remove,
            CountMeta,
            count_meta_name_slot,
            set_htab_for(db_idx),
            set_meta_htab_for(db_idx),
            set_pool_for(db_idx),
            set_lwlock(db_idx)
        ),
        KeyKind::Zset => drop_collection!(
            ZsetEntry,
            COMPOSITE_KEY_LEN,
            zset_remove,
            ZsetMeta,
            zset_meta_name_slot,
            zset_htab_for(db_idx),
            zset_meta_htab_for(db_idx),
            zset_pool_for(db_idx),
            zset_lwlock(db_idx)
        ),
        KeyKind::List => drop_collection!(
            ListEntry,
            LIST_KEY_LEN,
            list_remove,
            ListMeta,
            list_meta_name_slot,
            list_htab_for(db_idx),
            list_meta_htab_for(db_idx),
            list_pool_for(db_idx),
            list_lwlock(db_idx)
        ),
    }
}

/// What `key` holds, or `None` when nothing does. One lookup, whatever the type
/// — which is the whole point of the directory.
///
/// # Safety
/// Must be called from a bgworker thread after `mem_init_worker`, holding no
/// table lock.
pub unsafe fn mem_key_kind(db_idx: usize, key: &[u8]) -> Option<KeyKind> {
    unsafe { dir_lookup(db_idx, &key_hash(key)) }.map(|(kind, _)| kind)
}

/// Delete `key` and everything the type holding it stored for it. What `SET`
/// does to a key that was a list: Redis replaces the value rather than refusing.
///
/// # Safety
/// As `mem_key_kind`.
pub unsafe fn mem_drop_key_of(db_idx: usize, kind: KeyKind, key: &[u8]) {
    unsafe { mem_drop_key(db_idx, kind, &key_hash(key)) };
}

/// Every lookup key in a table. Collected before any removal — `hash_search`
/// (HASH_REMOVE) may not run while a sequential scan over the same table is
/// open — and by key rather than by entry, since the key is what removal takes.
unsafe fn all_keys<E, const N: usize>(table: &SharedTable<E>) -> Vec<[u8; N]> {
    let mut out = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let mut k = [0u8; N];
        unsafe { std::ptr::copy_nonoverlapping(entry as *const u8, k.as_mut_ptr(), N) };
        out.push(k);
    }
    out
}

/// Empty one table and reset the pool its values spilled into. The pool is
/// re-formatted rather than walked chunk by chunk: every chain in it belonged
/// to an entry that is going, so there is nothing left to hand back to.
///
/// # Safety
/// Caller holds `lk`, and `pool` is used by no table but this one.
unsafe fn flush_table<E, const N: usize>(
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    chunks: usize,
    lk: *mut pg_sys::LWLock,
) {
    let _guard = unsafe { LockGuard::exclusive(lk) };
    if let Some(table) = unsafe { SharedTable::<E>::from_raw(htab) } {
        for k in unsafe { all_keys::<E, N>(&table) } {
            unsafe { table.remove(k.as_ptr().cast()) };
        }
    }
    if let Some(meta) = unsafe { SharedTable::<u8>::from_raw(meta_htab) } {
        for k in unsafe { all_keys::<u8, KEY_HASH_LEN>(&meta) } {
            unsafe { meta.remove(k.as_ptr().cast()) };
        }
    }
    if !pool.is_null() {
        unsafe { pool_init(pool, chunks) };
    }
}

/// `FLUSHDB`: drop everything one shared-memory database holds.
///
/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the
///   thread-local CTL_PTR.
/// - No table lock may be held.
pub unsafe fn mem_flush_db(db_idx: usize) {
    let chunks = pool_chunks();
    let member_chunks = member_pool_chunks();
    unsafe {
        flush_table::<KvEntry, KEY_HASH_LEN>(
            htab_for(db_idx),
            std::ptr::null_mut(),
            kv_pool_for(db_idx),
            chunks,
            lwlock(db_idx),
        );
        flush_table::<HashEntry, COMPOSITE_KEY_LEN>(
            hash_htab_for(db_idx),
            hash_meta_htab_for(db_idx),
            hash_pool_for(db_idx),
            chunks,
            hash_lwlock(db_idx),
        );
        flush_table::<SetEntry, COMPOSITE_KEY_LEN>(
            set_htab_for(db_idx),
            set_meta_htab_for(db_idx),
            set_pool_for(db_idx),
            member_chunks,
            set_lwlock(db_idx),
        );
        flush_table::<ZsetEntry, COMPOSITE_KEY_LEN>(
            zset_htab_for(db_idx),
            zset_meta_htab_for(db_idx),
            zset_pool_for(db_idx),
            member_chunks,
            zset_lwlock(db_idx),
        );
        flush_table::<ListEntry, LIST_KEY_LEN>(
            list_htab_for(db_idx),
            list_meta_htab_for(db_idx),
            list_pool_for(db_idx),
            chunks,
            list_lwlock(db_idx),
        );
        // Last, so nothing is left naming a key whose data has gone.
        flush_table::<DirEntry, KEY_HASH_LEN>(
            dir_htab_for(db_idx),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
            dir_lwlock(db_idx),
        );
    }
}

/// Background expiry sweep: delete every key whose expiry has passed, of
/// whatever type. It reads the directory rather than the KV table, which is why
/// a list or a sorted set can now carry an expiry at all.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_sweep_expired(db_idx: usize) {
    for (kh, kind) in unsafe { dir_expired(db_idx) } {
        unsafe { mem_drop_key(db_idx, kind, &kh) };
    }
}

/// TYPE: returns type string for a key, "none" for missing.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_type(db_idx: usize, key: &[u8]) -> &'static str {
    match unsafe { dir_lookup(db_idx, &key_hash(key)) } {
        Some((kind, _)) => kind.name(),
        None => "none",
    }
}

/// Glob pattern matching supporting `*`, `?`, and `[...]` character classes.
/// Delegates to the iterative implementation in pubsub for consistency with PSUBSCRIBE.
fn glob_matches(pattern: &[u8], s: &[u8]) -> bool {
    crate::pubsub::glob_match(pattern, s)
}

/// Shared memory size for the MemControlBlock itself (holds pointers, not HTAB data).
pub fn mem_ctl_size() -> usize {
    std::mem::size_of::<MemControlBlock>()
}

/// What one table of `entries` costs in shared memory, across all 8 databases.
///
/// dynahash's own estimate rather than a multiplier over the entry size: the
/// bucket array, the segment directory and the per-element header are all in
/// it, and a fudge factor that is 20% short is a postmaster that will not start.
fn htab_total_size(entries: i64, entry_size: usize) -> usize {
    let per_table =
        unsafe { pg_sys::hash_estimate_size(entries as std::os::raw::c_long, entry_size) };
    per_table * NUM_MEM_DBS
}

pub fn mem_htab_total_size() -> usize {
    htab_total_size(htab_init_size(), std::mem::size_of::<KvEntry>())
}

pub fn mem_hash_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<HashEntry>())
}

pub fn mem_set_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<SetEntry>())
}

pub fn mem_zset_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<ZsetEntry>())
}

pub fn mem_list_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<ListEntry>())
}

pub fn mem_list_meta_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<ListMeta>())
}

pub fn mem_zset_meta_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<ZsetMeta>())
}

pub fn mem_set_meta_htab_total_size() -> usize {
    htab_total_size(htab_init_size_small(), std::mem::size_of::<CountMeta>())
}

pub fn mem_hash_meta_htab_total_size() -> usize {
    mem_set_meta_htab_total_size()
}

pub fn mem_dir_htab_total_size() -> usize {
    htab_total_size(dir_init_size(), std::mem::size_of::<DirEntry>())
}

/// Value pools per database: one each for the KV, hash and list tables.
const VAL_POOLS_PER_DB: usize = 3;
/// Member pools per database: one each for the set and zset tables.
const MEMBER_POOLS_PER_DB: usize = 2;

/// Stride between pools inside the single allocation that holds them all.
/// Rounded up so every pool starts aligned, whatever `pool_chunks()` is.
fn val_pool_stride() -> usize {
    val_pool_size(configured_pool_chunks()).next_multiple_of(64)
}

/// The same, for the half-sized member pools.
fn member_pool_stride() -> usize {
    val_pool_size(configured_member_pool_chunks()).next_multiple_of(64)
}

/// Bytes one database's pools occupy: three value pools then two member pools.
fn pools_stride_per_db() -> usize {
    val_pool_stride() * VAL_POOLS_PER_DB + member_pool_stride() * MEMBER_POOLS_PER_DB
}

/// Total shmem for every chunk pool: one chunk per entry slot, handed out only
/// to the values that actually spill.
pub fn mem_val_pool_total_size() -> usize {
    pools_stride_per_db() * NUM_MEM_DBS + 8192
}

/// Create every table, from the postmaster's `shmem_startup_hook`. Never from a
/// bgworker: `ShmemInitHash` is only valid while `ShmemAlloc` is open.
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

    // Every table is `HASH_BLOBS`, and none may be `HASH_STRINGS`: dynahash
    // compares string keys with `strncmp`, which makes two keys agreeing up to
    // their first NUL byte the same key. Redis keys are arbitrary bytes.
    let blob_flags = (pg_sys::HASH_ELEM
        | pg_sys::HASH_BLOBS
        | pg_sys::HASH_SHARED_MEM
        | pg_sys::HASH_FIXED_SIZE) as i32;

    let sz = htab_init_size();
    let sz_small = htab_init_size_small();
    let sz_dir = dir_init_size();

    for i in 0..NUM_MEM_DBS {
        unsafe {
            let name = format!("pg_redis_kv_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: KEY_HASH_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<KvEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(name.as_ptr().cast(), sz, sz, &mut info, blob_flags);
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
                entrysize: std::mem::size_of::<CountMeta>() as pg_sys::Size,
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

            let name = format!("pg_redis_hash_meta_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: KEY_HASH_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<CountMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                blob_flags,
            );
            std::ptr::addr_of_mut!((*ctl).hash_meta_htab[i]).write(htab);

            let name = format!("pg_redis_dir_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: KEY_HASH_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<DirEntry>() as pg_sys::Size,
                ..Default::default()
            };
            let htab =
                pg_sys::ShmemInitHash(name.as_ptr().cast(), sz_dir, sz_dir, &mut info, blob_flags);
            std::ptr::addr_of_mut!((*ctl).dir_htab[i]).write(htab);
        }
    }

    // One allocation for all 40 pools, carved up by stride. A named
    // `ShmemInitStruct` apiece would work too, but each one costs an entry in
    // PostgreSQL's shared-memory index and none of them needs its own name.
    // Each database's block is three value pools then two half-sized member ones.
    let chunks = configured_pool_chunks();
    let member_chunks = configured_member_pool_chunks();
    let stride = val_pool_stride();
    let member_stride = member_pool_stride();
    let per_db = pools_stride_per_db();
    unsafe { addr_of_mut!((*ctl).pool_chunks).write(chunks as u32) };
    unsafe { addr_of_mut!((*ctl).member_pool_chunks).write(member_chunks as u32) };
    unsafe {
        let mut found = false;
        let base = pg_sys::ShmemInitStruct(
            c"pg_redis_val_pools".as_ptr(),
            per_db * NUM_MEM_DBS,
            &mut found,
        )
        .cast::<u8>();
        for i in 0..NUM_MEM_DBS {
            let db_base = base.add(i * per_db);
            let val_pools: [*mut ValPool; VAL_POOLS_PER_DB] =
                std::array::from_fn(|t| db_base.add(t * stride).cast::<ValPool>());
            for pool in val_pools {
                pool_init(pool, chunks);
            }
            let members_base = db_base.add(stride * VAL_POOLS_PER_DB);
            let member_pools: [*mut ValPool; MEMBER_POOLS_PER_DB] =
                std::array::from_fn(|t| members_base.add(t * member_stride).cast::<ValPool>());
            for pool in member_pools {
                pool_init(pool, member_chunks);
            }
            addr_of_mut!((*ctl).kv_pool[i]).write(val_pools[0]);
            addr_of_mut!((*ctl).hash_pool[i]).write(val_pools[1]);
            addr_of_mut!((*ctl).list_pool[i]).write(val_pools[2]);
            addr_of_mut!((*ctl).set_pool[i]).write(member_pools[0]);
            addr_of_mut!((*ctl).zset_pool[i]).write(member_pools[1]);
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
    composite_key_of_hashes(kh, &key_hash(field))
}

/// The composite key of a member already reduced to its hash — how `ZsetMeta`
/// names its extremes.
fn composite_key_of_hashes(kh: &KeyHash, mh: &KeyHash) -> [u8; COMPOSITE_KEY_LEN] {
    let mut buf = [0u8; COMPOSITE_KEY_LEN];
    buf[..KEY_HASH_LEN].copy_from_slice(kh);
    buf[KEY_HASH_LEN..].copy_from_slice(mh);
    buf
}

/// The member half of a composite key, read back out of an entry.
unsafe fn composite_member(entry_key: *const u8) -> KeyHash {
    let mut mh = [0u8; KEY_HASH_LEN];
    unsafe {
        std::ptr::copy_nonoverlapping(entry_key.add(KEY_HASH_LEN), mh.as_mut_ptr(), KEY_HASH_LEN)
    };
    mh
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
    let meta_htab = hash_meta_htab_for(db_idx);
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    unsafe {
        reserve_member_chunks(pool, key, field, value.len(), || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        member_enter(&table, pool, &k, field, hash_field_slot, || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let mut is_new = !found;
    if !entry.is_null() && !unsafe { hash_write_full_value(entry, pool, value) } {
        // The entry owns the field chain `member_enter` just gave it.
        unsafe { hash_remove(&table, pool, k.as_ptr().cast()) };
        is_new = false;
    }
    unsafe {
        count_meta_grow(
            db_idx,
            KeyKind::Hash,
            meta_htab,
            pool,
            key,
            is_new as i64,
            || evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key)),
        )
    };
    is_new
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hget(db_idx: usize, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
    let htab = hash_htab_for(db_idx);
    let table = unsafe { SharedTable::<HashEntry>::from_raw(htab) }?;
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    let _guard = unsafe { LockGuard::shared(lk) };
    unsafe { member_find(&table, pool, &k, field, hash_field_slot) }
        .map(|entry| unsafe { hash_read_full_value(entry, pool) })
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hdel(db_idx: usize, key: &[u8], fields: &[&[u8]]) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = hash_meta_htab_for(db_idx);
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut count = 0i64;
    for f in fields {
        let k = make_composite_key(key, f);
        if unsafe { member_remove(&table, pool, &k, f, hash_field_slot, hash_free) } {
            count += 1;
        }
    }
    unsafe { count_meta_shrink(db_idx, meta_htab, pool, key, count) };
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    let _guard = unsafe { LockGuard::shared(lk) };
    unsafe { member_find(&table, pool, &k, field, hash_field_slot) }.is_some()
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hgetall(db_idx: usize, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        let field_str = unsafe { value_read(pool, hash_field_slot(entry)) };
        let val_str = unsafe { hash_read_full_value(entry, pool) };
        collected.push((field_str, val_str));
    }
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
    let meta_htab = hash_meta_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let meta = unsafe { find_count_meta(meta_htab, key) };
    if meta.is_null() {
        0
    } else {
        unsafe { (*meta).count }
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hmget(db_idx: usize, key: &[u8], fields: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return fields.iter().map(|_| None).collect();
    };
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let results: Vec<Option<Vec<u8>>> = fields
        .iter()
        .map(|f| {
            let k = make_composite_key(key, f);
            unsafe { member_find(&table, pool, &k, f, hash_field_slot) }
                .map(|entry| unsafe { hash_read_full_value(entry, pool) })
        })
        .collect();
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
    let meta_htab = hash_meta_htab_for(db_idx);
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    unsafe {
        reserve_member_chunks(pool, key, field, 0, || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        member_enter(&table, pool, &k, field, hash_field_slot, || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    if entry.is_null() {
        Err("ERR out of memory".to_string())
    } else if !found {
        let s = delta.to_string();
        // An integer always fits inline, so the pool is never touched.
        let _ = unsafe { hash_write_full_value(entry, pool, s.as_bytes()) };
        unsafe {
            count_meta_grow(db_idx, KeyKind::Hash, meta_htab, pool, key, 1, || {
                evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            })
        };
        Ok(delta)
    } else {
        let cur_bytes = unsafe { hash_read_full_value(entry, pool) };
        let cur = parse_stored_int(&cur_bytes);
        match cur {
            None => Err("ERR hash value is not an integer".to_string()),
            Some(cur) => match cur.checked_add(delta) {
                None => Err("ERR increment or decrement would overflow".to_string()),
                Some(new_val) => {
                    let ns = new_val.to_string();
                    let _ = unsafe { hash_write_full_value(entry, pool, ns.as_bytes()) };
                    Ok(new_val)
                }
            },
        }
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hsetnx(db_idx: usize, key: &[u8], field: &[u8], value: &[u8]) -> bool {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let meta_htab = hash_meta_htab_for(db_idx);
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    unsafe {
        reserve_member_chunks(pool, key, field, value.len(), || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        member_enter(&table, pool, &k, field, hash_field_slot, || {
            evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let set = if !found && !entry.is_null() {
        let written = unsafe { hash_write_full_value(entry, pool, value) };
        if !written {
            unsafe { hash_remove(&table, pool, k.as_ptr().cast()) };
        }
        written
    } else {
        false
    };
    unsafe {
        count_meta_grow(
            db_idx,
            KeyKind::Hash,
            meta_htab,
            pool,
            key,
            set as i64,
            || evict_hash_key(db_idx, htab, meta_htab, pool, &key_hash(key)),
        )
    };
    set
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
unsafe fn mem_del_hash_key(db_idx: usize, key: &[u8]) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = hash_meta_htab_for(db_idx);
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    // Collected before any removal — `hash_search(HASH_REMOVE)` may not run
    // while a sequential scan over the same table is open.
    let to_del: Vec<[u8; COMPOSITE_KEY_LEN]> = unsafe { entries_of(&table, &key_hash(key)) };
    let count = to_del.len() as i64;
    for k in &to_del {
        unsafe { hash_remove(&table, pool, k.as_ptr().cast()) };
    }
    if count > 0 {
        unsafe { remove_count_meta(db_idx, meta_htab, pool, key) };
    }
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut count = 0i64;
    for m in members {
        let k = make_composite_key(key, m);
        unsafe {
            reserve_member_chunks(pool, key, m, 0, || {
                evict_set_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            })
        };
        let (entry, found) = unsafe {
            member_enter(&table, pool, &k, m, set_member_slot, || {
                evict_set_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            })
        };
        if !found && !entry.is_null() {
            count += 1;
        }
    }
    unsafe {
        count_meta_grow(db_idx, KeyKind::Set, meta_htab, pool, key, count, || {
            evict_set_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut count = 0i64;
    for m in members {
        let k = make_composite_key(key, m);
        if unsafe { member_remove(&table, pool, &k, m, set_member_slot, set_free) } {
            count += 1;
        }
    }
    unsafe { count_meta_shrink(db_idx, meta_htab, pool, key, count) };
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let k = make_composite_key(key, member);
    let _guard = unsafe { LockGuard::shared(lk) };
    unsafe { member_find(&table, pool, &k, member, set_member_slot) }.is_some()
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_smismember(db_idx: usize, key: &[u8], members: &[&[u8]]) -> Vec<bool> {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return members.iter().map(|_| false).collect();
    };
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let results: Vec<bool> = members
        .iter()
        .map(|m| {
            let k = make_composite_key(key, m);
            unsafe { member_find(&table, pool, &k, m, set_member_slot) }.is_some()
        })
        .collect();
    results
}

unsafe fn set_collect_members(
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
) -> Vec<Vec<u8>> {
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let mut members = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        members.push(unsafe { value_read(pool, set_member_slot(entry)) });
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut members = unsafe { set_collect_members(htab, pool, key) };
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
    let _guard = unsafe { LockGuard::shared(lk) };
    let meta = unsafe { find_count_meta(meta_htab, key) };
    if !meta.is_null() {
        unsafe { (*meta).count }
    } else {
        0
    }
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };

    let meta = unsafe { find_count_meta(meta_htab, key) };
    if meta.is_null() || unsafe { (*meta).count } == 0 {
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
                let member = unsafe { value_read(pool, set_member_slot(entry)) };
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
            unsafe { set_remove(&table, pool, composite.as_ptr().cast()) };
            results.push(to_remove_member);
            remaining -= 1;
        }
    }

    if remaining == 0 {
        unsafe { remove_count_meta(db_idx, meta_htab, pool, key) };
    } else {
        unsafe { addr_of_mut!((*meta).count).write(remaining) };
    }

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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut members = unsafe { set_collect_members(htab, pool, key) };
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let src_k = make_composite_key(src, member);
    let dst_k = make_composite_key(dst, member);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let found = unsafe { member_remove(&table, pool, &src_k, member, set_member_slot, set_free) };
    if found {
        let (dst_entry, dst_existed) = unsafe {
            member_enter(&table, pool, &dst_k, member, set_member_slot, || {
                evict_set_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
            })
        };
        let dst_is_new = !dst_existed && !dst_entry.is_null();
        if !meta_htab.is_null() {
            unsafe { count_meta_shrink(db_idx, meta_htab, pool, src, 1) };
            unsafe {
                count_meta_grow(
                    db_idx,
                    KeyKind::Set,
                    meta_htab,
                    pool,
                    dst,
                    dst_is_new as i64,
                    || evict_set_key(db_idx, htab, meta_htab, pool, &key_hash(dst)),
                )
            };
        }
    }
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut all: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for k in keys {
        let members = unsafe { set_collect_members(htab, pool, k) };
        all.extend(members);
    }
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let first: std::collections::HashSet<Vec<u8>> =
        unsafe { set_collect_members(htab, pool, keys[0]) }
            .into_iter()
            .collect();
    let mut result: std::collections::HashSet<Vec<u8>> = first;
    for k in &keys[1..] {
        let other: std::collections::HashSet<Vec<u8>> =
            unsafe { set_collect_members(htab, pool, k) }
                .into_iter()
                .collect();
        result = result.intersection(&other).cloned().collect();
    }
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
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let first: std::collections::HashSet<Vec<u8>> =
        unsafe { set_collect_members(htab, pool, keys[0]) }
            .into_iter()
            .collect();
    let mut result = first;
    for k in &keys[1..] {
        let other: std::collections::HashSet<Vec<u8>> =
            unsafe { set_collect_members(htab, pool, k) }
                .into_iter()
                .collect();
        result = result.difference(&other).cloned().collect();
    }
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
unsafe fn mem_del_set_key(db_idx: usize, key: &[u8]) -> i64 {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let pool = set_pool_for(db_idx);
    let lk = set_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
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
        unsafe { set_remove(&table, pool, k.as_ptr().cast()) };
    }
    if count > 0 {
        unsafe { remove_count_meta(db_idx, meta_htab, pool, key) };
    }
    count
}

// ─────────────────────────── Sorted set operations ──────────────────────────

unsafe fn zset_collect(
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
) -> Vec<(Vec<u8>, f64)> {
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let mut entries = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        let member = unsafe { value_read(pool, zset_member_slot(entry)) };
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
    let pool = zset_pool_for(db_idx);
    let lk = zset_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut added = 0i64;
    let mut changed = 0i64;
    let meta: *mut ZsetMeta = if !meta_htab.is_null() {
        unsafe {
            get_or_create_zset_meta(db_idx, meta_htab, pool, key, || {
                evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            })
        }
    } else {
        std::ptr::null_mut()
    };
    for (score, member) in members {
        let k = make_composite_key(key, member);
        unsafe {
            reserve_member_chunks(pool, key, member, 0, || {
                evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            })
        };
        let (entry, found) = unsafe {
            member_enter(&table, pool, &k, member, zset_member_slot, || {
                evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            })
        };
        if entry.is_null() {
            continue;
        }
        if !found {
            if xx {
                unsafe { zset_remove(&table, pool, k.as_ptr().cast()) };
                continue;
            }
            unsafe { addr_of_mut!((*entry).score).write(*score) };
            added += 1;
            changed += 1;
            unsafe { zset_meta_member_added(meta, htab, pool, key, *score, member) };
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
                unsafe { zset_meta_score_changed(htab, pool, meta, key, member, *score) };
            }
        }
    }
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
    let pool = zset_pool_for(db_idx);
    let lk = zset_lwlock(db_idx);
    let k = make_composite_key(key, member);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let meta_htab = zset_meta_htab_for(db_idx);
    unsafe {
        reserve_member_chunks(pool, key, member, 0, || {
            evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        member_enter(&table, pool, &k, member, zset_member_slot, || {
            evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        })
    };
    // A member that reaches the entry table and not the meta is invisible to
    // `ZCARD` and `ZPOPMIN` while `ZRANGE` still lists it. Created lazily so a
    // refused `XX` leaves no empty meta behind.
    if entry.is_null() {
        None
    } else if !found {
        if xx {
            unsafe { zset_remove(&table, pool, k.as_ptr().cast()) };
            None
        } else {
            unsafe { addr_of_mut!((*entry).score).write(delta) };
            let meta = unsafe {
                get_or_create_zset_meta(db_idx, meta_htab, pool, key, || {
                    evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(key))
                })
            };
            unsafe { zset_meta_member_added(meta, htab, pool, key, delta, member) };
            Some(delta)
        }
    } else if nx {
        None
    } else {
        let old = unsafe { (*entry).score };
        let new_score = old + delta;
        if new_score.is_nan() {
            // Infinities of opposite sign. The caller turns this into Redis's
            // error; the score it would have written is not stored.
            return Some(f64::NAN);
        }
        let should_update = if gt {
            new_score > old
        } else if lt {
            new_score < old
        } else {
            true
        };
        if should_update {
            unsafe { addr_of_mut!((*entry).score).write(new_score) };
            let meta = unsafe { find_zset_meta(meta_htab, key) };
            unsafe { zset_meta_score_changed(htab, pool, meta, key, member, new_score) };
            Some(new_score)
        } else {
            Some(old)
        }
    }
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
    let pool = zset_pool_for(db_idx);
    let lk = zset_lwlock(db_idx);
    let _guard = unsafe { LockGuard::exclusive(lk) };
    let mut count = 0i64;
    for m in members {
        let k = make_composite_key(key, m);
        if unsafe { member_remove(&table, pool, &k, m, zset_member_slot, zset_free) } {
            count += 1;
        }
    }
    if count > 0 && !meta_htab.is_null() {
        let meta = unsafe { find_zset_meta(meta_htab, key) };
        if !meta.is_null() {
            let old_count = unsafe { (*meta).count };
            let new_count = old_count - count;
            if new_count <= 0 {
                unsafe { remove_zset_meta(db_idx, meta_htab, pool, key) };
            } else {
                unsafe { addr_of_mut!((*meta).count).write(new_count) };
                unsafe { refresh_zset_meta(htab, pool, meta, key) };
            }
        }
    }
    count
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zscore(db_idx: usize, key: &[u8], member: &[u8]) -> Option<f64> {
    let htab = zset_htab_for(db_idx);
    let table = unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }?;
    let pool = zset_pool_for(db_idx);
    let lk = zset_lwlock(db_idx);
    let k = make_composite_key(key, member);
    let _guard = unsafe { LockGuard::shared(lk) };
    unsafe { member_find(&table, pool, &k, member, zset_member_slot) }
        .map(|entry| unsafe { (*entry).score })
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
    let _guard = unsafe { LockGuard::shared(lk) };
    let meta = unsafe { find_zset_meta(meta_htab, key) };
    if !meta.is_null() {
        unsafe { (*meta).count }
    } else {
        0
    }
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
    let pool = zset_pool_for(db_idx);
    let lk = zset_lwlock(db_idx);
    let _guard = unsafe { LockGuard::shared(lk) };
    let mut all = unsafe { zset_collect(htab, pool, key) };
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::shared(lk);
        let all = zset_collect(htab, pool, key);
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::shared(lk);
        let mut all = zset_collect(htab, pool, key);
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
        let Some((s, e)) = range_bounds(start, stop, len) else {
            return vec![];
        };
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::shared(lk);
        let mut all = zset_collect(htab, pool, key);
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::shared(lk);
        let mut all = zset_collect(htab, pool, key);
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);

        if !meta_htab.is_null() {
            let Some(table) = SharedTable::<ZsetEntry>::from_raw(htab) else {
                return vec![];
            };
            let mut results = Vec::new();
            for _ in 0..count.max(0) {
                let meta = find_zset_meta(meta_htab, key);
                if meta.is_null() || (*meta).count == 0 {
                    break;
                }
                let min_score = (*meta).min_score;
                // The meta names the member by hash, which is the second half
                // of its entry's composite key — so the bytes come from the
                // entry rather than from the meta.
                let k = composite_key_of_hashes(&key_hash(key), &(*meta).min_member);
                let Some(entry) = table.find(k.as_ptr().cast()) else {
                    // The meta named a member the table does not hold: recover
                    // rather than spin, and let the next pass see the truth.
                    refresh_zset_meta(htab, pool, meta, key);
                    break;
                };
                let min_member = value_read(pool, zset_member_slot(entry));
                zset_remove(&table, pool, k.as_ptr().cast());
                results.push((min_member, min_score));
                let old_count = (*meta).count;
                let new_count = old_count - 1;
                if new_count == 0 {
                    remove_zset_meta(db_idx, meta_htab, pool, key);
                } else {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, pool, meta, key);
                }
            }
            return results;
        }

        let mut all = zset_collect(htab, pool, key);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        let take = count.max(0) as usize;
        let chosen: Vec<(Vec<u8>, f64)> = all.into_iter().take(take).collect();
        for (m, _) in &chosen {
            let k = make_composite_key(key, m);
            zset_remove_at(htab, pool, &k);
        }
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);

        if !meta_htab.is_null() {
            let Some(table) = SharedTable::<ZsetEntry>::from_raw(htab) else {
                return vec![];
            };
            let mut results = Vec::new();
            for _ in 0..count.max(0) {
                let meta = find_zset_meta(meta_htab, key);
                if meta.is_null() || (*meta).count == 0 {
                    break;
                }
                let max_score = (*meta).max_score;
                // The meta names the member by hash, which is the second half
                // of its entry's composite key — so the bytes come from the
                // entry rather than from the meta.
                let k = composite_key_of_hashes(&key_hash(key), &(*meta).max_member);
                let Some(entry) = table.find(k.as_ptr().cast()) else {
                    // The meta named a member the table does not hold: recover
                    // rather than spin, and let the next pass see the truth.
                    refresh_zset_meta(htab, pool, meta, key);
                    break;
                };
                let max_member = value_read(pool, zset_member_slot(entry));
                zset_remove(&table, pool, k.as_ptr().cast());
                results.push((max_member, max_score));
                let old_count = (*meta).count;
                let new_count = old_count - 1;
                if new_count == 0 {
                    remove_zset_meta(db_idx, meta_htab, pool, key);
                } else {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, pool, meta, key);
                }
            }
            return results;
        }

        let mut all = zset_collect(htab, pool, key);
        all.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        let take = count.max(0) as usize;
        let chosen: Vec<(Vec<u8>, f64)> = all.into_iter().take(take).collect();
        for (m, _) in &chosen {
            let k = make_composite_key(key, m);
            zset_remove_at(htab, pool, &k);
        }
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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::shared(lk);
        let mut all = zset_collect(htab, pool, key);
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

/// Remove the members a range command selected and put the meta back in step.
/// The three `ZREMRANGEBY*` commands differ only in which members they choose.
///
/// # Safety
/// Caller holds the sorted set's LWLock.
unsafe fn zset_remove_selected(
    db_idx: usize,
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    meta_htab: *mut pg_sys::HTAB,
    key: &[u8],
    total: usize,
    to_del: &[Vec<u8>],
) -> i64 {
    unsafe {
        for m in to_del {
            let k = make_composite_key(key, m);
            zset_remove_at(htab, pool, &k);
        }
        if !to_del.is_empty() && !meta_htab.is_null() {
            let new_count = (total - to_del.len()) as i64;
            if new_count == 0 {
                remove_zset_meta(db_idx, meta_htab, pool, key);
            } else {
                let meta = find_zset_meta(meta_htab, key);
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(new_count);
                    refresh_zset_meta(htab, pool, meta, key);
                }
            }
        }
        to_del.len() as i64
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let mut all = zset_collect(htab, pool, key);
        all.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        let len = all.len();
        if len == 0 {
            return 0;
        }
        let Some((s, e)) = range_bounds(start, stop, len) else {
            return 0;
        };
        let to_del: Vec<Vec<u8>> = all[s..=e].iter().map(|(m, _)| m.clone()).collect();
        zset_remove_selected(db_idx, htab, pool, meta_htab, key, len, &to_del)
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let all = zset_collect(htab, pool, key);
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
        zset_remove_selected(db_idx, htab, pool, meta_htab, key, total, &to_del)
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let all = zset_collect(htab, pool, key);
        let total = all.len();
        let to_del: Vec<Vec<u8>> = all
            .into_iter()
            .filter(|(m, _)| lex_in_range(m, min, max))
            .map(|(m, _)| m)
            .collect();
        zset_remove_selected(db_idx, htab, pool, meta_htab, key, total, &to_del)
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
        let _guard = LockGuard::shared(lk);
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

/// Replace `dst` with an aggregate's result, and put its meta back in step.
///
/// The three `Z*STORE` commands differ only in how they build that result —
/// union, intersection, difference — and shared this verbatim, down to the
/// eviction closure.
///
/// # Safety
/// Caller holds the sorted set's LWLock.
unsafe fn zstore_result(
    db_idx: usize,
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    table: &SharedTable<ZsetEntry>,
    dst: &[u8],
    result: &std::collections::HashMap<Vec<u8>, f64>,
) -> i64 {
    let count = result.len() as i64;
    unsafe {
        for (m, s) in result {
            let k = make_composite_key(dst, m);
            reserve_member_chunks(pool, dst, m, 0, || {
                evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
            });
            let (entry, _found) = member_enter(table, pool, &k, m, zset_member_slot, || {
                evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
            });
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(db_idx, meta_htab, pool, dst);
            } else {
                let meta = get_or_create_zset_meta(db_idx, meta_htab, pool, dst, || {
                    evict_zset_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
                });
                if !meta.is_null() {
                    addr_of_mut!((*meta).count).write(count);
                    refresh_zset_meta(htab, pool, meta, dst);
                }
            }
        }
    }
    count
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let mut map: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
        for (ki, k) in keys.iter().enumerate() {
            let w = weights.get(ki).copied().unwrap_or(1.0);
            let entries = zset_collect(htab, pool, k);
            for (m, s) in entries {
                let weighted = s * w;
                map.entry(m)
                    .and_modify(|e| *e = apply_aggregate(*e, weighted, aggregate))
                    .or_insert(weighted);
            }
        }
        let to_del: Vec<Vec<u8>> = {
            let old = zset_collect(htab, pool, dst);
            old.into_iter().map(|(m, _)| m).collect()
        };
        for m in &to_del {
            let k = make_composite_key(dst, m);
            zset_remove_at(htab, pool, &k);
        }
        let Some(table) = SharedTable::<ZsetEntry>::from_raw(htab) else {
            return 0;
        };
        zstore_result(db_idx, htab, meta_htab, pool, &table, dst, &map)
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let w0 = weights.first().copied().unwrap_or(1.0);
        let first: std::collections::HashMap<Vec<u8>, f64> = zset_collect(htab, pool, keys[0])
            .into_iter()
            .map(|(m, s)| (m, s * w0))
            .collect();
        let mut result = first;
        for (ki, k) in keys[1..].iter().enumerate() {
            let w = weights.get(ki + 1).copied().unwrap_or(1.0);
            let other: std::collections::HashMap<Vec<u8>, f64> = zset_collect(htab, pool, k)
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
        let to_del: Vec<Vec<u8>> = zset_collect(htab, pool, dst)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(dst, m);
            zset_remove_at(htab, pool, &k);
        }
        let Some(table) = SharedTable::<ZsetEntry>::from_raw(htab) else {
            return 0;
        };
        zstore_result(db_idx, htab, meta_htab, pool, &table, dst, &result)
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let first: std::collections::HashMap<Vec<u8>, f64> =
            zset_collect(htab, pool, keys[0]).into_iter().collect();
        let mut result = first;
        for k in &keys[1..] {
            let other: std::collections::HashSet<Vec<u8>> = zset_collect(htab, pool, k)
                .into_iter()
                .map(|(m, _)| m)
                .collect();
            result.retain(|m, _| !other.contains(m));
        }
        let to_del: Vec<Vec<u8>> = zset_collect(htab, pool, dst)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(dst, m);
            zset_remove_at(htab, pool, &k);
        }
        let Some(table) = SharedTable::<ZsetEntry>::from_raw(htab) else {
            return 0;
        };
        zstore_result(db_idx, htab, meta_htab, pool, &table, dst, &result)
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
        let pool = zset_pool_for(db_idx);
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);
        let to_del: Vec<Vec<u8>> = zset_collect(htab, pool, key)
            .into_iter()
            .map(|(m, _)| m)
            .collect();
        for m in &to_del {
            let k = make_composite_key(key, m);
            zset_remove_at(htab, pool, &k);
        }
        if !to_del.is_empty() {
            remove_zset_meta(db_idx, meta_htab, pool, key);
        }
        to_del.len() as i64
    }
}

// ─────────────────────────── List operations ────────────────────────────────

unsafe fn get_or_create_meta(
    db_idx: usize,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
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
            meta_name_write(pool, list_meta_name_slot(meta), key);
            dir_touch(db_idx, &key_buf, KeyKind::List);
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

unsafe fn remove_meta(db_idx: usize, meta_htab: *mut pg_sys::HTAB, pool: *mut ValPool, key: &[u8]) {
    if let Some(table) = unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) } {
        let key_buf = key_hash(key);
        // The name's chain first: the entry owns it, and `remove` is the last
        // thing that can reach it.
        if let Some(meta) = unsafe { table.find(key_buf.as_ptr().cast()) } {
            unsafe { value_free(pool, list_meta_name_slot(meta)) };
        }
        unsafe { table.remove(key_buf.as_ptr().cast()) };
        unsafe { dir_forget(db_idx, &key_buf) };
    }
}

// ─────────────────────────── Random number generator ────────────────────────

fn fast_random() -> u64 {
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

/// The meta names its extremes by member hash. Writing one is a 16-byte copy,
/// and reading one back gives the entry's composite key rather than the bytes.
unsafe fn write_meta_member(dest: &mut KeyHash, member: &[u8]) {
    *dest = key_hash(member);
}

unsafe fn meta_member_is(stored: &KeyHash, member: &[u8]) -> bool {
    *stored == key_hash(member)
}

unsafe fn get_or_create_zset_meta(
    db_idx: usize,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
    make_room: impl FnOnce() -> bool,
) -> *mut ZsetMeta {
    unsafe {
        let key_buf = key_hash(key);
        let (meta, found): (*mut ZsetMeta, bool) =
            enter_raw(meta_htab, key_buf.as_ptr().cast::<c_void>(), make_room);
        if !meta.is_null() && !found {
            meta_name_write(pool, zset_meta_name_slot(meta), key);
            dir_touch(db_idx, &key_buf, KeyKind::Zset);
            addr_of_mut!((*meta).count).write(0);
            addr_of_mut!((*meta).min_score).write(f64::INFINITY);
            addr_of_mut!((*meta).max_score).write(f64::NEG_INFINITY);
            addr_of_mut!((*meta).min_member).write([0; KEY_HASH_LEN]);
            addr_of_mut!((*meta).max_member).write([0; KEY_HASH_LEN]);
        }
        meta
    }
}

/// Fold a newly created member into its sorted set's meta. Shared by every site
/// that creates one — `ZINCRBY` included — because a member the meta never
/// hears about is invisible to `ZCARD` and `ZPOPMIN` while `ZRANGE` lists it.
unsafe fn zset_meta_member_added(
    meta: *mut ZsetMeta,
    zset_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
    score: f64,
    member: &[u8],
) {
    if meta.is_null() {
        return;
    }
    unsafe {
        let count = (*meta).count;
        // Equal scores are ordered by member, as Redis orders them, so a tie
        // has to compare the two rather than keep whichever arrived first.
        if count == 0
            || score < (*meta).min_score
            || (score == (*meta).min_score
                && meta_member_precedes(zset_htab, pool, key, &(*meta).min_member, member))
        {
            addr_of_mut!((*meta).min_score).write(score);
            write_meta_member(&mut (*meta).min_member, member);
        }
        if count == 0
            || score > (*meta).max_score
            || (score == (*meta).max_score
                && !meta_member_precedes(zset_htab, pool, key, &(*meta).max_member, member))
        {
            addr_of_mut!((*meta).max_score).write(score);
            write_meta_member(&mut (*meta).max_member, member);
        }
        addr_of_mut!((*meta).count).write(count + 1);
    }
}

/// Whether `member` sorts before the member the meta names by hash. One
/// `hash_search`, asked only on an exact score tie. A meta naming a member the
/// table has lost answers yes, so the newcomer takes the extreme.
unsafe fn meta_member_precedes(
    zset_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
    held: &KeyHash,
    member: &[u8],
) -> bool {
    unsafe {
        let Some(table) = SharedTable::<ZsetEntry>::from_raw(zset_htab) else {
            return false;
        };
        let k = composite_key_of_hashes(&key_hash(key), held);
        let Some(entry) = table.find(k.as_ptr().cast()) else {
            return true;
        };
        member < value_read(pool, zset_member_slot(entry)).as_slice()
    }
}

/// Fold a score change into the meta. A new extreme names itself; a score
/// moving off one cannot name its replacement without `refresh_zset_meta`.
unsafe fn zset_meta_score_changed(
    zset_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    meta: *mut ZsetMeta,
    key: &[u8],
    member: &[u8],
    score: f64,
) {
    if meta.is_null() {
        return;
    }
    unsafe {
        let cur_min = (*meta).min_score;
        let cur_max = (*meta).max_score;
        let was_min = meta_member_is(&(*meta).min_member, member);
        let was_max = meta_member_is(&(*meta).max_member, member);
        if score < cur_min {
            addr_of_mut!((*meta).min_score).write(score);
            write_meta_member(&mut (*meta).min_member, member);
        } else if was_min && score > cur_min {
            refresh_zset_meta(zset_htab, pool, meta, key);
        }
        if score > cur_max {
            addr_of_mut!((*meta).max_score).write(score);
            write_meta_member(&mut (*meta).max_member, member);
        } else if was_max && score < cur_max {
            refresh_zset_meta(zset_htab, pool, meta, key);
        }
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

unsafe fn remove_zset_meta(
    db_idx: usize,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
) {
    unsafe {
        if meta_htab.is_null() {
            return;
        }
        let key_buf = key_hash(key);
        let meta = find_zset_meta(meta_htab, key);
        if !meta.is_null() {
            value_free(pool, zset_meta_name_slot(meta));
        }
        let mut found = false;
        pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        );
        dir_forget(db_idx, &key_buf);
    }
}

/// Recompute a sorted set's extremes by scanning its entries. Member bytes are
/// read only on a score tie, which is the only case that needs them.
unsafe fn refresh_zset_meta(
    zset_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    meta: *mut ZsetMeta,
    key: &[u8],
) {
    unsafe {
        let mut new_min = f64::INFINITY;
        let mut new_max = f64::NEG_INFINITY;
        let mut min_member: Vec<u8> = Vec::new();
        let mut max_member: Vec<u8> = Vec::new();
        let mut min_hash = [0u8; KEY_HASH_LEN];
        let mut max_hash = [0u8; KEY_HASH_LEN];
        let mut seen = false;

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
            if seen && score > new_min && score < new_max {
                continue;
            }
            let member = value_read(pool, zset_member_slot(entry));
            if !seen || score < new_min || (score == new_min && member < min_member) {
                new_min = score;
                min_hash = composite_member(entry as *const u8);
                min_member = member.clone();
            }
            if !seen || score > new_max || (score == new_max && member > max_member) {
                new_max = score;
                max_hash = composite_member(entry as *const u8);
                max_member = member;
            }
            seen = true;
        }

        addr_of_mut!((*meta).min_score).write(new_min);
        addr_of_mut!((*meta).max_score).write(new_max);
        addr_of_mut!((*meta).min_member).write(min_hash);
        addr_of_mut!((*meta).max_member).write(max_hash);
    }
}

// ─────────────────────────── CountMeta helpers ────────────────────────────────

unsafe fn get_or_create_count_meta(
    db_idx: usize,
    kind: KeyKind,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
    make_room: impl FnOnce() -> bool,
) -> *mut CountMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = key_hash(key);
        let (meta, found): (*mut CountMeta, bool) =
            enter_raw(meta_htab, key_buf.as_ptr().cast::<c_void>(), make_room);
        if !meta.is_null() && !found {
            addr_of_mut!((*meta).count).write(0);
            meta_name_write(pool, count_meta_name_slot(meta), key);
            dir_touch(db_idx, &key_buf, kind);
        }
        meta
    }
}

/// Add `added` members to a key's count, creating the meta — and with it the
/// key's directory entry — when the key is new. A no-op when nothing was added,
/// so a write that changed nothing does not bring a key into existence.
unsafe fn count_meta_grow(
    db_idx: usize,
    kind: KeyKind,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
    added: i64,
    make_room: impl FnOnce() -> bool,
) {
    if added <= 0 {
        return;
    }
    let meta = unsafe { get_or_create_count_meta(db_idx, kind, meta_htab, pool, key, make_room) };
    if !meta.is_null() {
        let old = unsafe { (*meta).count };
        unsafe { addr_of_mut!((*meta).count).write(old + added) };
    }
}

/// Take `removed` members off a key's count, dropping the meta and the
/// directory entry together when the last member goes.
unsafe fn count_meta_shrink(
    db_idx: usize,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
    removed: i64,
) {
    if removed <= 0 {
        return;
    }
    let meta = unsafe { find_count_meta(meta_htab, key) };
    if meta.is_null() {
        return;
    }
    let new_count = unsafe { (*meta).count } - removed;
    if new_count <= 0 {
        unsafe { remove_count_meta(db_idx, meta_htab, pool, key) };
    } else {
        unsafe { addr_of_mut!((*meta).count).write(new_count) };
    }
}

unsafe fn find_count_meta(meta_htab: *mut pg_sys::HTAB, key: &[u8]) -> *mut CountMeta {
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
        ) as *mut CountMeta;
        if found { meta } else { std::ptr::null_mut() }
    }
}

unsafe fn remove_count_meta(
    db_idx: usize,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
    key: &[u8],
) {
    unsafe {
        if meta_htab.is_null() {
            return;
        }
        let key_buf = key_hash(key);
        let meta = find_count_meta(meta_htab, key);
        if !meta.is_null() {
            value_free(pool, count_meta_name_slot(meta));
        }
        let mut found = false;
        pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        );
        dir_forget(db_idx, &key_buf);
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
        let pool = list_pool_for(db_idx);
        let lk = list_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);

        let meta = get_or_create_meta(db_idx, meta_htab, pool, key, || {
            evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        });
        if meta.is_null() {
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
            reserve_chunks(pool, v.len(), || {
                evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            });
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
                });
            if !entry.is_null() && !list_write_full_value(entry, pool, v) {
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
        let pool = list_pool_for(db_idx);
        let lk = list_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);

        let meta = get_or_create_meta(db_idx, meta_htab, pool, key, || {
            evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        });
        if meta.is_null() {
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
            reserve_chunks(pool, v.len(), || {
                evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
            });
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
                });
            if !entry.is_null() && !list_write_full_value(entry, pool, v) {
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
        // The lock goes before the push, which takes it again exclusively —
        // an LWLock is not re-entrant, and holding it here wedged the worker.
        let exists = {
            let _guard = LockGuard::shared(list_lwlock(db_idx));
            let meta = find_meta(meta_htab, key);
            !meta.is_null() && (*meta).count > 0
        };
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
        // The lock goes before the push, which takes it again exclusively —
        // an LWLock is not re-entrant, and holding it here wedged the worker.
        let exists = {
            let _guard = LockGuard::shared(list_lwlock(db_idx));
            let meta = find_meta(meta_htab, key);
            !meta.is_null() && (*meta).count > 0
        };
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
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return vec![];
        }

        let current_count = (*meta).count;
        if current_count == 0 {
            return vec![];
        }

        let pool = list_pool_for(db_idx);
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
                results.push(list_read_full_value(entry, pool));
            }
            list_remove_at(htab, pool, &k);
            pos += LIST_POS_STEP;
        }

        let new_count = current_count - take as i64;
        if new_count == 0 {
            remove_meta(db_idx, meta_htab, pool, key);
        } else {
            addr_of_mut!((*meta).min_pos).write(pos);
            addr_of_mut!((*meta).count).write(new_count);
        }

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
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return vec![];
        }

        let current_count = (*meta).count;
        if current_count == 0 {
            return vec![];
        }

        let pool = list_pool_for(db_idx);
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
                results.push(list_read_full_value(entry, pool));
            }
            list_remove_at(htab, pool, &k);
            pos -= LIST_POS_STEP;
        }

        let new_count = current_count - take as i64;
        if new_count == 0 {
            remove_meta(db_idx, meta_htab, pool, key);
        } else {
            addr_of_mut!((*meta).max_pos).write(pos);
            addr_of_mut!((*meta).count).write(new_count);
        }

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
        let _guard = LockGuard::shared(lk);
        let meta = find_meta(meta_htab, key);
        if meta.is_null() { 0 } else { (*meta).count }
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
        let _guard = LockGuard::shared(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return vec![];
        }
        let count = (*meta).count;
        if count == 0 {
            return vec![];
        }

        let min_pos = (*meta).min_pos;
        let Some((s, e)) = range_bounds(start, stop, count as usize) else {
            return vec![];
        };

        let pool = list_pool_for(db_idx);
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
                results.push(list_read_full_value(entry, pool));
            }
        }

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
        let _guard = LockGuard::shared(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return None;
        }
        let count = (*meta).count;
        if count == 0 {
            return None;
        }

        let idx = if index < 0 { index + count } else { index };
        if idx < 0 || idx >= count {
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
        let pool = list_pool_for(db_idx);
        if found && !entry.is_null() {
            Some(list_read_full_value(entry, pool))
        } else {
            None
        }
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
        let pool = list_pool_for(db_idx);
        let lk = list_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return false;
        }
        let count = (*meta).count;
        if count == 0 {
            return false;
        }

        let idx = if index < 0 { index + count } else { index };
        if idx < 0 || idx >= count {
            return false;
        }

        let min_pos = (*meta).min_pos;
        let pos = min_pos + idx * LIST_POS_STEP;
        let k = make_list_key(key, pos);
        // `reserve_chunks` normally has to run before the entry exists; here it
        // already does, and `evict_list_key` is told to keep this key, so the
        // element being rewritten cannot be the one eviction takes. Nothing
        // below reads `meta` again, so a victim's meta going away is harmless.
        reserve_chunks(pool, value.len(), || {
            evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
        });
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
            let _ = list_write_full_value(entry, pool, value);
        }
        found
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
///
/// Every element of a list, head to tail; caller holds the list lock. Tolerates
/// gaps, so a list left non-contiguous still reads correctly.
unsafe fn list_elements(
    htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
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
                out.push(list_read_full_value(entry, pool));
            }
            pos += LIST_POS_STEP;
        }
        out
    }
}

/// Replace a list's contents, renumbering from a fresh base. Readers walk
/// `min_pos + i * LIST_POS_STEP`, so a hole left by an in-place removal
/// truncates the list at the first gap with the count still reported full.
unsafe fn list_replace(
    db_idx: usize,
    htab: *mut pg_sys::HTAB,
    meta_htab: *mut pg_sys::HTAB,
    pool: *mut ValPool,
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
            list_remove_at(htab, pool, &k);
            pos += LIST_POS_STEP;
        }

        if elems.is_empty() {
            remove_meta(db_idx, meta_htab, pool, key);
            return;
        }

        for (i, v) in elems.iter().enumerate() {
            let pos = min_pos + (i as i64) * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
                });
            if !entry.is_null()
                && !list_write_full_value(entry, pool, v)
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
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() || (*meta).count == 0 {
            return 0;
        }

        let pool = list_pool_for(db_idx);
        let mut elems = list_elements(htab, pool, meta, key);
        match elems.iter().position(|e| e == pivot) {
            None => -1,
            Some(at) => {
                // `list_replace` returns the old elements' chunks before it
                // writes the new ones, so the only net demand is the inserted
                // value. `elems` is already an owned copy and `evict_list_key`
                // keeps this key, so evicting here cannot disturb either.
                reserve_chunks(pool, value.len(), || {
                    evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(key))
                });
                elems.insert(if before { at } else { at + 1 }, value.to_vec());
                list_replace(db_idx, htab, meta_htab, pool, key, meta, &elems);
                elems.len() as i64
            }
        }
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
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() || (*meta).count == 0 {
            return 0;
        }

        let pool = list_pool_for(db_idx);
        let elems = list_elements(htab, pool, meta, key);

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
            list_replace(db_idx, htab, meta_htab, pool, key, meta, &kept);
        }

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
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return;
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            return;
        }

        let min_pos = (*meta).min_pos;
        let len = current_count as usize;
        let kept = range_bounds(start, stop, len);
        let (s, e) = kept.unwrap_or((len, 0));

        let pool = list_pool_for(db_idx);
        for i in 0..len {
            if i < s || i > e {
                let pos = min_pos + i as i64 * LIST_POS_STEP;
                let k = make_list_key(key, pos);
                list_remove_at(htab, pool, &k);
            }
        }

        if kept.is_none() {
            remove_meta(db_idx, meta_htab, pool, key);
        } else {
            let new_count = (e - s + 1) as i64;
            let new_min = min_pos + s as i64 * LIST_POS_STEP;
            let new_max = min_pos + e as i64 * LIST_POS_STEP;
            addr_of_mut!((*meta).min_pos).write(new_min);
            addr_of_mut!((*meta).max_pos).write(new_max);
            addr_of_mut!((*meta).count).write(new_count);
        }
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
        let _guard = LockGuard::exclusive(lk);

        let src_meta = find_meta(meta_htab, src);
        if src_meta.is_null() {
            return None;
        }
        let src_count = (*src_meta).count;
        if src_count == 0 {
            return None;
        }

        let src_min = (*src_meta).min_pos;
        let src_max = (*src_meta).max_pos;
        let src_pos = if src_left { src_min } else { src_max };

        let pool = list_pool_for(db_idx);
        let sk = make_list_key(src, src_pos);
        let mut found = false;
        let src_entry = pg_sys::hash_search(
            htab,
            sk.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut ListEntry;
        if !found || src_entry.is_null() {
            return None;
        }
        let value = list_read_full_value(src_entry, pool);
        list_remove_at(htab, pool, &sk);

        let new_src_count = src_count - 1;
        if new_src_count == 0 {
            remove_meta(db_idx, meta_htab, pool, src);
        } else {
            if src_left {
                addr_of_mut!((*src_meta).min_pos).write(src_min + LIST_POS_STEP);
            } else {
                addr_of_mut!((*src_meta).max_pos).write(src_max - LIST_POS_STEP);
            }
            addr_of_mut!((*src_meta).count).write(new_src_count);
        }

        let dst_meta = get_or_create_meta(db_idx, meta_htab, pool, dst, || {
            evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
        });
        if dst_meta.is_null() {
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
        // The source element's chunks went back above, so this normally finds
        // room already. It does not when the source value was inline and the
        // destination has to spill — an APPEND-shaped LMOVE onto a full pool.
        // Runs before the destination entry exists, as `reserve_chunks` asks.
        reserve_chunks(pool, value.len(), || {
            evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
        });
        let (entry, _f2): (*mut ListEntry, bool) =
            enter_raw(htab, dk.as_ptr().cast::<c_void>(), || {
                evict_list_key(db_idx, htab, meta_htab, pool, &key_hash(dst))
            });
        if !entry.is_null()
            && !list_write_full_value(entry, pool, &value)
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
        let _guard = LockGuard::shared(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return vec![];
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            return vec![];
        }

        let min_pos = (*meta).min_pos;
        let mut indices: Vec<i64> = (0..current_count).collect();
        if rank < 0 {
            indices.reverse();
        }

        let pool = list_pool_for(db_idx);
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
                let v = list_read_full_value(entry, pool);
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

        results
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
unsafe fn mem_del_list_key(db_idx: usize, key: &[u8]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let pool = list_pool_for(db_idx);
        let lk = list_lwlock(db_idx);
        let _guard = LockGuard::exclusive(lk);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            return 0;
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            remove_meta(db_idx, meta_htab, pool, key);
            return 0;
        }

        let min_pos = (*meta).min_pos;
        for i in 0..current_count {
            let pos = min_pos + i * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            list_remove_at(htab, pool, &k);
        }
        remove_meta(db_idx, meta_htab, pool, key);

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
        let pool = zset_pool_for(db_idx);
        let lk = zset_lwlock(db_idx);
        let _guard = LockGuard::shared(lk);
        let mut all = zset_collect(htab, pool, key);
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
        let pool = kv_pool_for(db_idx);
        let lk = lwlock(db_idx);
        let guard = LockGuard::shared(lk);
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
            result = Some(with_kv_key(entry, pool, <[u8]>::to_vec));
            pg_sys::hash_seq_term(&mut status);
            break;
        }
        // As `mem_scan`: the KV lock goes before the four `collection_names`
        // takes, so only one lock order is ever in play.
        drop(guard);

        // A string if there is one, else whatever a collection is called. Redis
        // draws from every key; this favours strings, which is a distribution
        // difference rather than a wrong answer — `RANDOMKEY` is compared by
        // reply shape.
        result.or_else(|| collection_names(db_idx, |_| true).into_iter().next())
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

    /// Redis reads a counter only in its canonical spelling. Everything here
    /// parses fine as an `i64` and is still not an integer to Redis, which is
    /// the distinction `INCR` turns into an error rather than a silent rewrite.
    #[test]
    fn a_counter_is_only_read_in_its_canonical_spelling() {
        assert_eq!(parse_stored_int(b"0"), Some(0));
        assert_eq!(parse_stored_int(b"-1"), Some(-1));
        assert_eq!(parse_stored_int(b"9223372036854775807"), Some(i64::MAX));
        for rejected in [
            &b"+1"[..],
            b"007",
            b"-0",
            b" 1",
            b"1 ",
            b"1e2",
            b"0x10",
            b"",
            b"-",
            b"1.0",
            b"99999999999999999999999999",
        ] {
            assert_eq!(
                parse_stored_int(rejected),
                None,
                "{:?} is not a canonical integer",
                String::from_utf8_lossy(rejected)
            );
        }
    }

    /// The arm that used to be written out four times, three of them wrapping:
    /// `(stop + len) as usize` on a stop past the start is enormous, clamps to
    /// the last element, and selects everything.
    #[test]
    fn a_range_past_either_end_selects_nothing() {
        assert_eq!(range_bounds(0, -1, 3), Some((0, 2)));
        assert_eq!(range_bounds(1, 1, 3), Some((1, 1)));
        assert_eq!(range_bounds(-2, -1, 3), Some((1, 2)));
        assert_eq!(range_bounds(-100, 100, 3), Some((0, 2)));
        // The one that emptied a sorted set.
        assert_eq!(range_bounds(-100, -100, 3), None);
        assert_eq!(range_bounds(-1, -100, 3), None);
        assert_eq!(range_bounds(2, 1, 3), None);
        assert_eq!(range_bounds(5, 10, 3), None);
        assert_eq!(range_bounds(0, 0, 0), None);
    }

    /// `noeviction` is the default because it is Redis's, and it is the only
    /// policy that must never remove live data. Read through `policy_from`
    /// rather than `evict_policy`: the GUC behind it may only be read from the
    /// thread that owns PostgreSQL's FFI, which the test harness does not
    /// promise.
    #[test]
    fn the_default_policy_evicts_nothing() {
        assert!(policy_from(None) == EvictPolicy::NoEviction);
        assert!(policy_from(Some("nonsense")) == EvictPolicy::NoEviction);
        assert!(policy_from(Some("allkeys-random")) == EvictPolicy::AllKeysRandom);
        assert!(policy_from(Some("volatile-ttl")) == EvictPolicy::VolatileTtl);
    }

    /// Both halves of a composite key have to come back out: `composite_owner`
    /// is how eviction finds every row belonging to a victim, and
    /// `composite_member` is how `ZPOPMIN` names the member it removed.
    #[test]
    fn composite_keys_split_back_into_hash_and_member() {
        let k = make_composite_key(b"mykey", b"myfield");
        assert_eq!(unsafe { composite_owner(k.as_ptr()) }, key_hash(b"mykey"));
        assert_eq!(
            unsafe { composite_member(k.as_ptr()) },
            key_hash(b"myfield")
        );
        assert!(hash_matches_entry(k.as_ptr(), &key_hash(b"mykey")));
        assert!(!hash_matches_entry(k.as_ptr(), &key_hash(b"otherkey")));
        assert_eq!(
            k,
            composite_key_of_hashes(&key_hash(b"mykey"), &key_hash(b"myfield"))
        );

        // Members differing only after a NUL, or only in length, are distinct
        // keys — the truncation that made `SMEMBERS` return "a" twice.
        assert_ne!(
            make_composite_key(b"k", b"a\0one"),
            make_composite_key(b"k", b"a\0two")
        );
        assert_ne!(
            make_composite_key(b"k", b"a\0one"),
            make_composite_key(b"k", b"a")
        );
        // A member of any length still produces the same fixed-size key.
        let full = vec![b'm'; MAX_MEMBER_LEN];
        assert_eq!(make_composite_key(b"k", &full).len(), COMPOSITE_KEY_LEN);
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
            ("KvEntry", size_of::<KvEntry>(), 232),
            ("HashEntry", size_of::<HashEntry>(), 160),
            ("SetEntry", size_of::<SetEntry>(), 88),
            ("ZsetEntry", size_of::<ZsetEntry>(), 96),
            ("ListEntry", size_of::<ListEntry>(), 96),
            ("ListMeta", size_of::<ListMeta>(), 88),
            ("ZsetMeta", size_of::<ZsetMeta>(), 120),
            ("CountMeta", size_of::<CountMeta>(), 72),
            ("DirEntry", size_of::<DirEntry>(), 32),
        ];
        for (name, actual, expected) in sizes {
            assert_eq!(
                actual, expected,
                "{name} changed size; recompute the shared-memory request in docs \
                 before accepting this"
            );
        }
        // At the 8192 default this totals ~55 MiB of shared memory.
        let per_entry: usize = sizes.iter().map(|(_, s, _)| s).sum();
        assert!(
            per_entry <= 1024,
            "combined entry size {per_entry} exceeds the budget"
        );

        // The pool costs one link and one chunk per chunk, and nothing per
        // entry — which is the whole point of it. Five per database, the two
        // member pools at half the chunks of the three value ones.
        assert_eq!(val_pool_size(1), size_of::<ValPool>() + 4 + CHUNK_LEN);
        assert_eq!(val_pool_size(8192), 16 + 8192 * 68);
        // A member pool holds one chunk per entry slot, so half as many.
        assert_eq!(val_pool_size(4096), 16 + 4096 * 68);
    }

    /// A scratch pool on the heap. The pool code only ever touches the bytes
    /// it was handed, so it runs outside shared memory unchanged — which is
    /// what lets the chunk lifecycle be tested without a postmaster.
    struct TestPool {
        _buf: Vec<u64>,
        pool: *mut ValPool,
    }

    fn test_pool(chunks: usize) -> TestPool {
        let words = val_pool_size(chunks).div_ceil(8);
        let mut buf = vec![0u64; words];
        let pool = buf.as_mut_ptr().cast::<ValPool>();
        unsafe { pool_init(pool, chunks) };
        TestPool { _buf: buf, pool }
    }

    fn free_chunks(p: &TestPool) -> usize {
        unsafe { pool_free_chunks(p.pool) }
    }

    /// The tail of a value costs one chunk per `CHUNK_LEN` bytes past the
    /// inline slot — the boundary the value-size tests walk.
    #[test]
    fn a_value_costs_one_chunk_per_chunk_length_past_the_inline_slot() {
        assert_eq!(chunks_for(0), 0);
        assert_eq!(chunks_for(INLINE_VAL_LEN), 0);
        assert_eq!(chunks_for(INLINE_VAL_LEN + 1), 1);
        assert_eq!(chunks_for(INLINE_VAL_LEN + CHUNK_LEN), 1);
        assert_eq!(chunks_for(INLINE_VAL_LEN + CHUNK_LEN + 1), 2);
        assert_eq!(chunks_for(512), 7);
    }

    /// Whatever a chain is used for, every chunk in it has to come back — a
    /// pool that leaks is silent until it runs dry.
    #[test]
    fn every_allocated_chunk_comes_back_to_the_free_list() {
        let p = test_pool(64);
        assert_eq!(free_chunks(&p), 64);

        let mut chains = Vec::new();
        for n in [1usize, 7, 3, 13] {
            let head = unsafe { pool_alloc(p.pool, n) }.expect("pool has room");
            chains.push(head);
        }
        assert_eq!(free_chunks(&p), 64 - (1 + 7 + 3 + 13));

        for head in chains {
            unsafe { pool_release(p.pool, head) };
        }
        assert_eq!(free_chunks(&p), 64);

        // ...and the recycled chunks are usable again, in one whole run.
        let head = unsafe { pool_alloc(p.pool, 64) }.expect("pool is whole again");
        unsafe { pool_release(p.pool, head) };
        assert_eq!(free_chunks(&p), 64);
    }

    /// An allocation the pool cannot satisfy has to leave the free list exactly
    /// as it found it. A partial chain would strand every chunk it took.
    #[test]
    fn an_allocation_that_does_not_fit_strands_nothing() {
        let p = test_pool(8);
        assert!(unsafe { pool_alloc(p.pool, 9) }.is_none());
        assert_eq!(free_chunks(&p), 8);

        let head = unsafe { pool_alloc(p.pool, 6) }.expect("six of eight");
        assert!(unsafe { pool_alloc(p.pool, 3) }.is_none());
        assert_eq!(free_chunks(&p), 2);
        unsafe { pool_release(p.pool, head) };
        assert_eq!(free_chunks(&p), 8);
    }

    /// A value with no tail owns no chunks, so it must not be given one.
    #[test]
    fn a_value_that_fits_inline_takes_no_chunk() {
        let p = test_pool(4);
        assert_eq!(unsafe { pool_alloc(p.pool, 0) }, Some(NIL_CHUNK));
        assert_eq!(free_chunks(&p), 4);
        unsafe { pool_release(p.pool, NIL_CHUNK) };
        assert_eq!(free_chunks(&p), 4);
    }

    fn blank_entry() -> KvEntry {
        KvEntry {
            key: [0; KEY_HASH_LEN],
            key_inline: [0; INLINE_KEY_LEN],
            value: [0; INLINE_VAL_LEN],
            expires_at: 0,
            key_len: 0,
            key_overflow: NIL_CHUNK,
            value_len: 0,
            overflow: NIL_CHUNK,
        }
    }

    /// Position-dependent bytes: a chain spliced back in the wrong order, or a
    /// chunk boundary off by one, cannot pass by luck.
    fn payload(n: usize) -> Vec<u8> {
        (0..n).map(|i| (33 + (i % 90)) as u8).collect()
    }

    /// Every size either side of both boundaries — the inline slot and the
    /// chunk length — has to come back byte for byte.
    #[test]
    fn values_round_trip_through_the_pool_at_every_boundary() {
        let p = test_pool(256);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        for n in [0usize, 1, 63, 64, 65, 128, 129, 200, 511, 512, 1024] {
            let v = payload(n);
            assert!(unsafe { value_write(p.pool, kv_slot(e), &v) }, "{n} bytes");
            assert_eq!(unsafe { value_read(p.pool, kv_slot(e)) }, v, "{n} bytes");
        }

        unsafe { value_free(p.pool, kv_slot(e)) };
        assert_eq!(
            free_chunks(&p),
            256,
            "overwriting a value leaked its chunks"
        );
    }

    /// The largest value memory mode accepts is a chain of 1,023 chunks. A walk
    /// that reads one link too far, or a length rounded the wrong way at the
    /// last partial chunk, needs a chain that long to show itself — the
    /// boundary sizes above are all one or two chunks.
    #[test]
    fn a_value_at_the_cap_round_trips_through_its_whole_chain() {
        let limit = max_total_val_len();
        let needed = chunks_for(limit);
        assert!(
            needed > 1000,
            "the cap should be a long chain, not {needed}"
        );

        let p = test_pool(needed + 4);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        let v = payload(limit);
        assert!(unsafe { value_write(p.pool, kv_slot(e), &v) });
        assert_eq!(free_chunks(&p), 4, "the chain took more than it needed");
        assert_eq!(unsafe { value_read(p.pool, kv_slot(e)) }, v);

        // A thousand-link chain has to come back in one piece too. A byte over
        // the cap never reaches here — `value_write`'s debug assertion is the
        // backstop and the refusal has its own test.
        unsafe { value_free(p.pool, kv_slot(e)) };
        assert_eq!(free_chunks(&p), needed + 4, "the chain did not come back");
    }

    /// Shrinking a value past the inline boundary has to release the tail it no
    /// longer has: a stale chain is both a leak and, if the length ever grew
    /// back, someone else's bytes.
    #[test]
    fn shrinking_a_value_releases_its_tail() {
        let p = test_pool(64);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        assert!(unsafe { value_write(p.pool, kv_slot(e), &payload(500)) });
        assert!(free_chunks(&p) < 64);
        assert!(unsafe { value_write(p.pool, kv_slot(e), &payload(10)) });
        assert_eq!(free_chunks(&p), 64);
        assert_eq!(unsafe { (*e).overflow }, NIL_CHUNK);
        assert_eq!(unsafe { value_read(p.pool, kv_slot(e)) }, payload(10));
    }

    /// A value whose tail has nowhere to go leaves the slot empty rather than a
    /// length promising bytes that were never stored — and takes no chunks with
    /// it.
    #[test]
    fn a_value_the_pool_cannot_hold_leaves_the_slot_empty() {
        let p = test_pool(2);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        assert!(unsafe { value_write(p.pool, kv_slot(e), &payload(64 + 128)) });
        assert_eq!(free_chunks(&p), 0);

        // The overwrite returns the old chain first, so this fails on size
        // alone: three chunks' worth of tail into a two-chunk pool.
        assert!(!unsafe { value_write(p.pool, kv_slot(e), &payload(64 + 129)) });
        assert_eq!(unsafe { (*e).value_len }, 0);
        assert!(unsafe { value_read(p.pool, kv_slot(e)) }.is_empty());
        assert_eq!(free_chunks(&p), 2, "a refused write kept its chunks");
        assert_eq!(
            take_refusal(),
            Refusal::OutOfMemory,
            "a refused write must report OOM"
        );
    }

    /// The cap is a function of the pool now, not a constant — an eighth of it,
    /// and never above the ceiling a single read is allowed to copy.
    #[test]
    fn the_value_cap_follows_the_pool_within_its_bounds() {
        let limit = max_total_val_len();
        assert!(limit <= MAX_VAL_CEILING);
        // At the default 8192 chunks the eighth and the ceiling coincide.
        assert_eq!(limit, MAX_VAL_CEILING);
        assert!(chunks_for(limit) <= pool_chunks() / POOL_SHARE_PER_VALUE);
    }

    /// A key is stored the way a value is, so it has the same two boundaries
    /// and has to survive both — including the 512-byte cap, which is six
    /// chunks of tail.
    #[test]
    fn keys_round_trip_through_the_pool_at_every_boundary() {
        let p = test_pool(256);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        for n in [
            1usize,
            63,
            INLINE_KEY_LEN,
            65,
            128,
            129,
            200,
            511,
            MAX_KEY_LEN,
        ] {
            let k = payload(n);
            assert!(unsafe { value_write(p.pool, key_slot(e), &k) }, "{n} bytes");
            assert_eq!(unsafe { value_read(p.pool, key_slot(e)) }, k, "{n} bytes");
            assert!(unsafe { kv_key_matches(e, p.pool, &k) }, "{n} bytes");
        }

        unsafe { value_free(p.pool, key_slot(e)) };
        assert_eq!(free_chunks(&p), 256, "rewriting a key leaked its chunks");
    }

    /// The verification a `GET` does. It has to see a difference wherever it
    /// falls: in the length, in the inline prefix, or only in the pooled tail —
    /// the case an inline-only comparison would pass.
    #[test]
    fn a_stored_key_only_matches_itself() {
        let p = test_pool(64);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        let stored = payload(300);
        assert!(unsafe { value_write(p.pool, key_slot(e), &stored) });
        assert!(unsafe { kv_key_matches(e, p.pool, &stored) });

        // Same length, differing only in the last byte — well past the inline
        // prefix, so nothing but a walk of the chain can tell.
        let mut tail_differs = stored.clone();
        *tail_differs.last_mut().unwrap() ^= 1;
        assert!(!unsafe { kv_key_matches(e, p.pool, &tail_differs) });

        // Same length, differing in the inline prefix.
        let mut head_differs = stored.clone();
        head_differs[0] ^= 1;
        assert!(!unsafe { kv_key_matches(e, p.pool, &head_differs) });

        // A prefix of the stored key, and the stored key plus a byte.
        assert!(!unsafe { kv_key_matches(e, p.pool, &stored[..299]) });
        let mut longer = stored.clone();
        longer.push(b'x');
        assert!(!unsafe { kv_key_matches(e, p.pool, &longer) });

        unsafe { value_free(p.pool, key_slot(e)) };
    }

    /// A `HASH_STRINGS` table compares keys with `strncmp`, which makes two
    /// keys agreeing up to their first NUL one entry. Both halves of the
    /// defence have to hold: the hashes differ, and so does a stored key's
    /// comparison against the other.
    #[test]
    fn keys_differing_only_after_a_nul_are_distinct() {
        let a = b"a\0one";
        let b = b"a\0two";
        assert_ne!(key_hash(a), key_hash(b));
        assert_ne!(key_hash(b"a"), key_hash(a));

        let p = test_pool(4);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;
        assert!(unsafe { value_write(p.pool, key_slot(e), a) });
        assert!(unsafe { kv_key_matches(e, p.pool, a) });
        assert!(!unsafe { kv_key_matches(e, p.pool, b) });
        assert!(!unsafe { kv_key_matches(e, p.pool, b"a") });
        assert_eq!(unsafe { value_read(p.pool, key_slot(e)) }, a);
    }

    /// The refusal reasons must not be confusable: a collision reported as OOM
    /// points at `redis.mem_max_entries`, which cannot fix it.
    #[test]
    fn a_refusal_reports_the_cause_it_had() {
        assert_eq!(take_refusal(), Refusal::None);
        signal_oom();
        assert_eq!(take_refusal(), Refusal::OutOfMemory);
        assert_eq!(take_refusal(), Refusal::None, "reading clears the flag");
        signal_key_collision();
        assert_eq!(take_refusal(), Refusal::KeyCollision);
    }

    /// Both of a KV entry's chains have to come back, not just the value's.
    /// A key chain left behind is a leak nothing notices until the pool is dry.
    #[test]
    fn freeing_a_kv_entry_releases_both_its_chains() {
        let p = test_pool(64);
        let mut entry = blank_entry();
        let e = &mut entry as *mut KvEntry;

        assert!(unsafe { value_write(p.pool, key_slot(e), &payload(300)) });
        assert!(unsafe { value_write(p.pool, kv_slot(e), &payload(400)) });
        assert!(free_chunks(&p) < 64);

        unsafe { kv_free(p.pool, e) };
        assert_eq!(free_chunks(&p), 64);
        assert_eq!(unsafe { (*e).key_len }, 0);
        assert_eq!(unsafe { (*e).value_len }, 0);
        assert_eq!(unsafe { (*e).key_overflow }, NIL_CHUNK);
        assert_eq!(unsafe { (*e).overflow }, NIL_CHUNK);
    }

    fn blank_hash_entry() -> HashEntry {
        HashEntry {
            key: [0; KEY_HASH_LEN],
            field: [0; KEY_HASH_LEN],
            field_inline: [0; INLINE_MEMBER_LEN],
            value: [0; INLINE_VAL_LEN],
            field_len: 0,
            field_overflow: NIL_CHUNK,
            value_len: 0,
            overflow: NIL_CHUNK,
        }
    }

    fn blank_set_entry() -> SetEntry {
        SetEntry {
            key: [0; KEY_HASH_LEN],
            member: [0; KEY_HASH_LEN],
            member_inline: [0; INLINE_MEMBER_LEN],
            member_len: 0,
            member_overflow: NIL_CHUNK,
        }
    }

    /// A member is stored the way a key is, so it has the same two boundaries —
    /// and, unlike the NUL-terminated array it replaced, a member containing a
    /// NUL has to come back whole and compare unequal to its own prefix.
    #[test]
    fn members_round_trip_through_the_pool_at_every_boundary() {
        let p = test_pool(64);
        let mut entry = blank_set_entry();
        let e = &mut entry as *mut SetEntry;

        for n in [
            0usize,
            1,
            INLINE_MEMBER_LEN - 1,
            INLINE_MEMBER_LEN,
            INLINE_MEMBER_LEN + 1,
            INLINE_MEMBER_LEN + CHUNK_LEN,
            INLINE_MEMBER_LEN + CHUNK_LEN + 1,
            200,
            MAX_MEMBER_LEN,
        ] {
            let m = payload(n);
            let slot = unsafe { set_member_slot(e) };
            assert!(unsafe { value_write(p.pool, slot, &m) }, "{n} bytes");
            assert_eq!(unsafe { value_read(p.pool, slot) }, m, "{n} bytes");
            assert!(unsafe { value_eq(p.pool, slot, &m) }, "{n} bytes");
        }

        let slot = unsafe { set_member_slot(e) };
        assert!(unsafe { value_write(p.pool, slot, b"a\0one") });
        assert_eq!(unsafe { value_read(p.pool, slot) }, b"a\0one");
        assert!(!unsafe { value_eq(p.pool, slot, b"a\0two") });
        assert!(!unsafe { value_eq(p.pool, slot, b"a") });
        assert_ne!(key_hash(b"a\0one"), key_hash(b"a\0two"));

        unsafe { value_free(p.pool, slot) };
        assert_eq!(free_chunks(&p), 64, "a rewritten member leaked its chunks");
    }

    /// A hash entry owns two chains now, its field's as well as its value's, so
    /// it needs the same guarantee `freeing_a_kv_entry_releases_both_its_chains`
    /// makes — and a set or zset entry owns its member's.
    #[test]
    fn freeing_a_collection_entry_releases_every_chain() {
        let p = test_pool(64);

        let mut hentry = blank_hash_entry();
        let h = &mut hentry as *mut HashEntry;
        assert!(unsafe { value_write(p.pool, hash_field_slot(h), &payload(300)) });
        assert!(unsafe { value_write(p.pool, hash_slot(h), &payload(400)) });
        assert!(free_chunks(&p) < 64);
        unsafe { hash_free(p.pool, h) };
        assert_eq!(free_chunks(&p), 64);
        assert_eq!(unsafe { (*h).field_len }, 0);
        assert_eq!(unsafe { (*h).value_len }, 0);
        assert_eq!(unsafe { (*h).field_overflow }, NIL_CHUNK);
        assert_eq!(unsafe { (*h).overflow }, NIL_CHUNK);

        let mut sentry = blank_set_entry();
        let s = &mut sentry as *mut SetEntry;
        assert!(unsafe { value_write(p.pool, set_member_slot(s), &payload(MAX_MEMBER_LEN)) });
        assert!(free_chunks(&p) < 64);
        unsafe { set_free(p.pool, s) };
        assert_eq!(free_chunks(&p), 64);
        assert_eq!(unsafe { (*s).member_len }, 0);
        assert_eq!(unsafe { (*s).member_overflow }, NIL_CHUNK);
    }

    /// The HTAB key layouts must match the `keysize` values passed to
    /// `ShmemInitHash`, or lookups read past the end of the key.
    #[test]
    fn composite_key_layout_matches_its_parts() {
        assert_eq!(COMPOSITE_KEY_LEN, KEY_HASH_LEN * 2);
        assert_eq!(LIST_KEY_LEN, KEY_HASH_LEN + 8);
        // dynahash reads `keysize` bytes from the front of the entry, so the
        // key hash and the member hash must be the first two fields of each.
        for (name, off_key, off_member, size) in [
            (
                "HashEntry",
                std::mem::offset_of!(HashEntry, key),
                std::mem::offset_of!(HashEntry, field),
                size_of::<HashEntry>(),
            ),
            (
                "SetEntry",
                std::mem::offset_of!(SetEntry, key),
                std::mem::offset_of!(SetEntry, member),
                size_of::<SetEntry>(),
            ),
            (
                "ZsetEntry",
                std::mem::offset_of!(ZsetEntry, key),
                std::mem::offset_of!(ZsetEntry, member),
                size_of::<ZsetEntry>(),
            ),
        ] {
            assert_eq!(off_key, 0, "{name} must start with the key hash");
            assert_eq!(off_member, KEY_HASH_LEN, "{name} member hash misplaced");
            assert!(size >= COMPOSITE_KEY_LEN, "{name} shorter than its key");
        }

        // The KV table keys on the hash too now, so its entry must start with
        // one — dynahash reads `keysize` bytes from the front of the entry.
        assert_eq!(make_key(b"mykey"), key_hash(b"mykey"));
        assert_eq!(std::mem::offset_of!(KvEntry, key), 0);
        assert!(size_of::<KvEntry>() >= KEY_HASH_LEN);
        // A key at the cap is six chunks of tail out of the pool the values
        // share — the reason the cap is still a cap. A key of conventional
        // length costs none of it, which is what sets `INLINE_KEY_LEN`.
        assert_eq!(chunks_beyond(MAX_KEY_LEN, INLINE_KEY_LEN), 6);
        assert_eq!(chunks_beyond(INLINE_KEY_LEN, INLINE_KEY_LEN), 0);
        assert_eq!(chunks_beyond(INLINE_KEY_LEN + 1, INLINE_KEY_LEN), 1);
        // A full table of keys this long must still leave the whole pool to
        // the values — the cliff `INLINE_KEY_LEN` was chosen to move.
        assert_eq!(chunks_beyond(96, INLINE_KEY_LEN), 0);

        let composite = make_composite_key(b"mykey", b"myfield");
        assert_eq!(&composite[..KEY_HASH_LEN], &key_hash(b"mykey"));
        assert_eq!(&composite[KEY_HASH_LEN..], &key_hash(b"myfield"));

        // A member at the cap is eight chunks of a pool holding nothing else,
        // and one of conventional length costs no chunk at all.
        assert_eq!(chunks_for_member(MAX_MEMBER_LEN), 8);
        assert_eq!(chunks_for_member(INLINE_MEMBER_LEN), 0);
        assert_eq!(chunks_for_member(INLINE_MEMBER_LEN + 1), 1);
        assert_eq!(chunks_for_member(INLINE_MEMBER_LEN + CHUNK_LEN), 1);
        assert_eq!(chunks_for_member(36), 0);
        // Even the smallest pool memory mode builds — the 256-chunk floor of
        // `htab_init_size_small` — leaves a member at the cap under the eighth
        // share a value is allowed.
        assert!(chunks_for_member(MAX_MEMBER_LEN) <= 256 / POOL_SHARE_PER_VALUE);

        let list_key = make_list_key(b"mykey", -42);
        assert_eq!(&list_key[..KEY_HASH_LEN], &key_hash(b"mykey"));
        assert_eq!(&list_key[KEY_HASH_LEN..], &(-42i64).to_le_bytes());
    }
}
