use crate::htab::SharedTable;
use pgrx::pg_sys;
use std::ffi::c_void;
use std::ptr::addr_of;
use std::ptr::addr_of_mut;

/// Maximum key length. Keys are the thing applications namespace and make long,
/// so this is generous.
///
/// Not an entry-layout bound: keys are length-prefixed and only the first
/// `INLINE_KEY_LEN` bytes live in the entry. This is a pool bound — how much of
/// the slab the values spill into one key is allowed to take.
pub const MAX_KEY_LEN: usize = 512;
// Maximum hash field / set member / sorted-set member length. Kept smaller than
// MAX_KEY_LEN on purpose: the hash, set and zset entries each store a key *and*
// a member, so this length is multiplied across the largest tables.
pub const MAX_MEMBER_LEN: usize = 128;
// HTAB key for the hash/set/zset tables: the redis key followed by the member.
/// Every table indexes on a 128-bit keyed hash of the Redis key rather than on
/// the key itself.
///
/// A hash, a set, a sorted set and a list store one entry per member, and a
/// full copy of the key in each would dominate the entry — 512 bytes against a
/// `SetEntry`'s 144, repeated for every member. They need no more than the
/// hash: every composite-table access is a comparison against a key the caller
/// already holds.
///
/// The KV table is the exception. `KEYS`, `SCAN` and `RANDOMKEY` hand real keys
/// to clients, so it keys on the hash like the rest and keeps the bytes
/// alongside — see `KvEntry`.
///
/// Keyed with per-postmaster random material, so a client cannot craft two keys
/// that collide and merge their contents.
const KEY_HASH_LEN: usize = 16;
type KeyHash = [u8; KEY_HASH_LEN];

const COMPOSITE_KEY_LEN: usize = KEY_HASH_LEN + MAX_MEMBER_LEN;
// HTAB key for the list tables: the redis key followed by the 8-byte position.
const LIST_KEY_LEN: usize = KEY_HASH_LEN + 8;
// Inline value bytes stored directly in the main HTAB entry.
const INLINE_VAL_LEN: usize = 64;

/// Key bytes stored directly in a `KvEntry`; the rest spills into the pool.
///
/// Eight bytes here is 640 KiB of shared memory — 8192 entries × 8 databases,
/// plus dynahash's 5/4 — so the prefix costs 10 MiB. What stops it being
/// shorter is the pool rather than the entry: a key longer than this takes a
/// chunk out of the *same* slab the values spill into, and that slab holds one
/// chunk per entry.
///
/// Hence 128 and not 64. At 64 a key of 65–128 bytes costs a chunk, and
/// `myapp:prod:session:<uuid>` is 60 to 90 — so a full table of ordinary keys
/// would claim every chunk and leave nothing for any value past its own inline
/// slot. At 128 the same cliff sits at 129 bytes, which conventional keys do
/// not reach.
///
/// A longer key still works, at one chunk per 64 bytes over: a database of
/// 200-byte keys holds 4,096 rather than 8,192. See `docs/IMPLEMENTATION.md`.
const INLINE_KEY_LEN: usize = 128;
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

/// Fixed-size entry stored in the HTAB shared memory hash table.
/// The key field MUST be first — HTAB uses keysize bytes from the start.
///
/// Keyed on the same 128-bit hash the composite tables use, with the key bytes
/// kept alongside it the way a value is kept: `INLINE_KEY_LEN` inline and the
/// tail pooled. `KEYS`, `SCAN` and `RANDOMKEY` need those bytes back; keeping
/// them is also what lets a lookup verify the key it found is the key it asked
/// for, which no composite table can do.
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

/// Fixed-size entry for the hash HTAB. Key is (redis_key[128], field[128]).
#[repr(C)]
struct HashEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub field: [u8; MAX_MEMBER_LEN],
    pub value: [u8; INLINE_VAL_LEN],
    pub value_len: u32,
    /// See `KvEntry::overflow`.
    pub overflow: u32,
}

/// Fixed-size entry for the set HTAB. Key is (redis_key[128], member[128]).
#[repr(C)]
struct SetEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub member: [u8; MAX_MEMBER_LEN],
}

/// Fixed-size entry for the sorted set HTAB. Key is (redis_key[128], member[128]).
#[repr(C)]
struct ZsetEntry {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub member: [u8; MAX_MEMBER_LEN],
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
}

/// Metadata entry for the sorted set meta HTAB. Key is redis_key[128].
/// Tracks min/max score and members for O(1) ZPOPMIN/ZPOPMAX/ZCARD.
#[repr(C)]
struct ZsetMeta {
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
struct SetMeta {
    /// SipHash of the Redis key — see `KEY_HASH_LEN`.
    pub key: KeyHash,
    pub count: i64,
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

/// Ceiling on a memory-mode value however large the pool is.
///
/// Every read copies the value out of shared memory into a `Vec` while the
/// table's LWLock is held, so an unbounded value would hold a whole database
/// still for the length of one memcpy — and an order of magnitude more than
/// this belongs in the durable half anyway.
const MAX_VAL_CEILING: usize = 64 * 1024;

/// Chunks each pool is built with. One per entry slot: the inline slot already
/// covers the values that motivated it, so a chunk apiece doubles what an
/// entirely full table can hold before a value has nowhere to spill.
///
/// Only for the postmaster paths that size and create the pools — reading a
/// GUC is sound on the thread that owns PostgreSQL's FFI and nowhere else.
fn configured_pool_chunks() -> usize {
    crate::MEM_MAX_ENTRIES.get().max(1) as usize
}

/// Chunks in each pool, as actually allocated.
///
/// Read back out of the control block rather than from the GUC: it keeps the
/// value cap agreeing with the pool that exists, and keeps the GUC machinery
/// off the per-command path. Falls back to the boot value when shared memory
/// is not attached — unit tests, and the `auto` storage mode, where no pool
/// exists and nothing is stored under this limit anyway.
fn pool_chunks() -> usize {
    let c = ctl();
    if c.is_null() {
        return crate::MEM_MAX_ENTRIES_DEFAULT as usize;
    }
    unsafe { addr_of!((*c).pool_chunks).read() as usize }
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
/// `next` doubles as the free list and as the chain of a stored value, so a
/// chunk is on exactly one of the two at any moment and no separate bitmap has
/// to stay in step with it.
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

/// Whether the chain starting at `head` holds exactly `expect`.
///
/// The comparing form of `pool_read_into`. A lookup that verifies its key runs
/// on the read path of every command that takes one, so it must not allocate a
/// `Vec` to do the comparison.
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

/// The fields that make up a stored byte string, wherever it lives.
///
/// `KvEntry`, `HashEntry` and `ListEntry` carry the same trio at different
/// offsets, and a `KvEntry` carries it twice — once for its value and once for
/// its key. Naming it once is what keeps the chunk lifecycle to a single
/// implementation rather than four that have to be kept in step.
///
/// `inline_cap` is a field rather than a constant because the key prefix and
/// the value prefix are not the same size — see `INLINE_KEY_LEN`.
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

/// The key of a KV entry, as a `ValueSlot`.
///
/// A key is stored exactly the way a value is — inline prefix, pooled tail —
/// so it reads, writes and frees through the same three functions. `key_slot`
/// and `kv_slot` name disjoint fields of the same entry, so a caller may hold
/// both at once.
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

/// Whether the slot holds exactly `expect`, without copying it out.
///
/// The length is checked first, so the byte comparison only runs for a slot
/// that could still match, and the inline prefix — already in cache, the entry
/// having just been fetched — settles all but the keys longer than it.
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

/// Release whatever the slot holds and mark it empty.
///
/// Every path that drops or overwrites a value goes through here — a chain that
/// is not released is not noticed until the pool runs dry, which is why the
/// removal helpers below take the entry rather than its key.
unsafe fn value_free(pool: *mut ValPool, slot: ValueSlot) {
    unsafe {
        if slot.len.read() as usize > slot.inline_cap {
            pool_release(pool, slot.head.read());
        }
        slot.len.write(0);
        slot.head.write(NIL_CHUNK);
    }
}

/// Store `value` in the slot, spilling past the slot's inline capacity.
///
/// Returns false with the slot left empty when the pool cannot hold the tail.
/// The slot is always written, never left as the caller found it: an entry
/// fresh out of `hash_search(HASH_ENTER)` is recycled shared memory with only
/// its key initialised, so a return without a write would expose whatever the
/// last occupant left there.
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
    /// Chunks each pool was built with — see `pool_chunks`.
    pub pool_chunks: u32,
    _pad: u32,
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
    /// Chunk pools holding the tail of every value past `INLINE_VAL_LEN`.
    /// One per table per database, so each is covered by that table's LWLock.
    pub kv_pool: [*mut ValPool; NUM_MEM_DBS],
    pub hash_pool: [*mut ValPool; NUM_MEM_DBS],
    pub list_pool: [*mut ValPool; NUM_MEM_DBS],
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
fn key_hash(key: &[u8]) -> KeyHash {
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
enum EvictPolicy {
    NoEviction,
    AllKeysRandom,
    VolatileTtl,
}

fn evict_policy() -> EvictPolicy {
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
    /// Set when a write was refused. Read and cleared by
    /// `Command::execute_mem`, which turns it into the matching error.
    ///
    /// The alternative was threading a "table full" result out through all 68
    /// `mem_*` entry points, whose return types are `bool`, `i64`,
    /// `Result<i64, String>`, `Vec<..>` and more. A worker dispatches one
    /// command at a time on one thread, so the flag cannot cross commands.
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

/// Evict ahead of a write whose value needs more chunks than the pool has left.
///
/// Eviction is otherwise driven by the *table* filling up. A database whose
/// pool is exhausted but whose table still has slots would refuse every
/// spilling write for as long as it ran, `allkeys-random` or not, because
/// nothing would ever ask it to make room. This runs before the entry is
/// created: `make_room` may remove any entry in the table, and must not be
/// given the chance to remove the one about to be written.
///
/// Under `noeviction` — the default — `make_room` returns false without
/// scanning anything, so this costs one comparison.
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

/// Free room in a KV table. Caller must hold the database's LWLock.
///
/// Expired entries go first under every policy — they are dead weight the
/// second-granularity background sweep has not reached yet, not data anyone
/// asked to keep. Only if there are none does the policy get a say.
unsafe fn evict_kv(htab: *mut pg_sys::HTAB, pool: *mut ValPool) -> bool {
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
        unsafe { kv_remove(table, pool, key_buf.as_ptr().cast()) };
    }
    !victims.is_empty()
}

/// Remove an entry and return its chunks to the pool.
///
/// The chain heads live in the entry, so the release has to happen while the
/// entry is still there. Every removal of an entry that owns a chain goes
/// through here for that reason — a chunk leak is invisible until the pool runs
/// dry. `free_of` rather than a single slot because a `KvEntry` owns two.
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

unsafe fn hash_free(pool: *mut ValPool, entry: *mut HashEntry) {
    unsafe { value_free(pool, hash_slot(entry)) }
}

unsafe fn list_free(pool: *mut ValPool, entry: *mut ListEntry) {
    unsafe { value_free(pool, list_slot(entry)) }
}

unsafe fn kv_remove(
    table: &SharedTable<KvEntry>,
    pool: *mut ValPool,
    key_ptr: *const c_void,
) -> bool {
    unsafe { remove_valued(table, pool, key_ptr, kv_free) }
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

unsafe fn evict_hash_key(htab: *mut pg_sys::HTAB, pool: *mut ValPool, keep: &KeyHash) -> bool {
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let table = &table;
    let Some(victim) = (unsafe { victim_key(table, keep) }) else {
        return false;
    };
    let keys: Vec<[u8; COMPOSITE_KEY_LEN]> = unsafe { entries_of(table, &victim) };
    for k in &keys {
        unsafe { hash_remove(table, pool, k.as_ptr().cast()) };
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
    pool: *mut ValPool,
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
        unsafe { list_remove(table, pool, k.as_ptr().cast()) };
    }
    unsafe { remove_meta_of(meta_htab, &victim) };
    !keys.is_empty()
}

fn kv_pool_for(db_idx: usize) -> *mut ValPool {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).kv_pool[db_idx]).read() }
}

fn hash_pool_for(db_idx: usize) -> *mut ValPool {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).hash_pool[db_idx]).read() }
}

fn list_pool_for(db_idx: usize) -> *mut ValPool {
    let c = ctl();
    if c.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { addr_of!((*c).list_pool[db_idx]).read() }
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
unsafe fn kv_write_full_value(
    entry: *mut KvEntry,
    pool: *mut ValPool,
    value: &[u8],
    expires_at: i64,
) -> bool {
    unsafe {
        let ok = value_write(pool, kv_slot(entry), value);
        addr_of_mut!((*entry).expires_at).write(expires_at);
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

/// The HTAB key of a KV entry: the 128-bit handle, not the key itself.
///
/// Callers are expected to have refused an over-long key already
/// (`Command::mem_too_long_error`). The assertion backstops a command that
/// grows a new key argument without extending that check: an over-long key
/// hashes fine, so it would otherwise be refused at the write with an OOM
/// rather than a limit error.
fn make_key(s: &[u8]) -> KeyHash {
    debug_assert!(
        s.len() <= MAX_KEY_LEN,
        "over-long key reached memory backend"
    );
    key_hash(s)
}

/// Whether a KV entry holds exactly `key`.
///
/// A 128-bit keyed SipHash will not collide across `redis.mem_max_entries`
/// keys, but the entry keeps the key bytes for `KEYS` and `SCAN` anyway, so the
/// check is free to make here and impossible to make in a composite table. It
/// costs a length test plus, at most, a compare of the inline prefix against
/// bytes the lookup has already pulled into cache; only a key over
/// `INLINE_KEY_LEN` matching on both goes as far as walking the pool.
unsafe fn kv_key_matches(entry: *mut KvEntry, pool: *mut ValPool, key: &[u8]) -> bool {
    unsafe { value_eq(pool, key_slot(entry), key) }
}

/// Run `f` on a KV entry's key, without copying it when it fits inline.
///
/// `KEYS` and `SCAN` glob every key in the table and keep the few that match,
/// so reading each one into a `Vec` first would allocate once per entry to
/// return three. Only a key past the inline prefix has to be assembled at all,
/// and only a match is copied.
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

/// Enter the entry for `key`, writing the key bytes into a slot that did not
/// already hold them.
///
/// Null with a `Refusal` raised in three cases: the table is full, the key's
/// tail has nowhere to live, or the slot is held by a different key that hashed
/// the same. The last is the one a composite table can only answer by silently
/// merging the two keys' contents; here the write is refused and the key
/// already stored keeps its value.
unsafe fn kv_enter(
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
        (entry, false)
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

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let (result, was_expired) = match unsafe { kv_find(&table, pool, &key_buf, key) } {
        Some(entry) if unsafe { entry_is_expired(entry) } => (None, true),
        Some(entry) => (Some(unsafe { kv_read_full_value(entry, pool) }), false),
        None => (None, false),
    };

    unsafe { pg_sys::LWLockRelease(lk) };

    if was_expired {
        unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        if let Some(entry2) = unsafe { kv_find(&table, pool, &key_buf, key) }
            && unsafe { entry_is_expired(entry2) }
        {
            unsafe { kv_remove(&table, pool, key_buf.as_ptr().cast()) };
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    unsafe { kv_reserve(pool, key, value.len(), || evict_kv(htab, pool)) };
    let (entry, found) = unsafe { kv_enter(&table, pool, &key_buf, key, || evict_kv(htab, pool)) };
    let ok = !entry.is_null() && unsafe { kv_write_full_value(entry, pool, value, expires_at_us) };
    if !ok && !found && !entry.is_null() {
        // A rejected SET must not leave a phantom empty key behind — and the
        // entry owns the key chain `kv_enter` just gave it.
        unsafe { kv_remove(&table, pool, key_buf.as_ptr().cast()) };
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let mut count = 0i64;

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    for key in keys {
        let key_buf = make_key(key);
        if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) } {
            let expired = unsafe { entry_is_expired(entry) };
            unsafe { kv_remove(&table, pool, key_buf.as_ptr().cast()) };
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let mut count = 0i64;

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    for key in keys {
        let key_buf = make_key(key);
        if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) }
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

    let pool = kv_pool_for(db_idx);
    unsafe { kv_reserve(pool, key, 0, || evict_kv(htab, pool)) };
    let (entry, found) = unsafe { kv_enter(&table, pool, &key_buf, key, || evict_kv(htab, pool)) };

    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found || unsafe { entry_is_expired(entry) } {
        let new_val = delta;
        let s = new_val.to_string();
        unsafe { kv_write_full_value(entry, pool, s.as_bytes(), 0) };
        Ok(new_val)
    } else {
        // Every failure below has to fall through to the release at the end.
        // A `?` here returns with the database's LWLock still held, which wedges
        // the worker on its next command.
        let slice = unsafe { kv_read_inline_slice(entry) };
        let current = std::str::from_utf8(slice)
            .ok()
            .and_then(|s| s.parse::<i64>().ok());
        match current {
            None => Err("ERR value is not an integer or out of range".to_string()),
            Some(current) => match current.checked_add(delta) {
                None => Err("ERR increment or decrement would overflow".to_string()),
                Some(new_val) => {
                    let ns = new_val.to_string();
                    let exp = unsafe { (*entry).expires_at };
                    unsafe { kv_write_full_value(entry, pool, ns.as_bytes(), exp) };
                    Ok(new_val)
                }
            },
        }
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    unsafe { kv_reserve(pool, key, 0, || evict_kv(htab, pool)) };
    let (entry, found) = unsafe { kv_enter(&table, pool, &key_buf, key, || evict_kv(htab, pool)) };

    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found || unsafe { entry_is_expired(entry) } {
        let s = format_float(delta);
        unsafe { kv_write_full_value(entry, pool, s.as_bytes(), 0) };
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
                    unsafe { kv_write_full_value(entry, pool, ns.as_bytes(), exp) };
                    Ok(ns)
                }
            }
        }
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    unsafe { kv_reserve(pool, key, value.len(), || evict_kv(htab, pool)) };
    let (entry, found) = unsafe { kv_enter(&table, pool, &key_buf, key, || evict_kv(htab, pool)) };

    let old = if found && !entry.is_null() && !unsafe { entry_is_expired(entry) } {
        Some(unsafe { kv_read_full_value(entry, pool) })
    } else {
        None
    };

    if !entry.is_null() {
        unsafe { kv_write_full_value(entry, pool, value, 0) };
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let result = if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) } {
        let val = if !unsafe { entry_is_expired(entry) } {
            Some(unsafe { kv_read_full_value(entry, pool) })
        } else {
            None
        };
        unsafe { kv_remove(&table, pool, key_buf.as_ptr().cast()) };
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

    let pool = kv_pool_for(db_idx);
    unsafe { kv_reserve(pool, key, suffix_bytes.len(), || evict_kv(htab, pool)) };
    let (entry, found) = unsafe { kv_enter(&table, pool, &key_buf, key, || evict_kv(htab, pool)) };

    let limit = max_total_val_len();
    let new_len = if entry.is_null() {
        0i64
    } else if !found || unsafe { entry_is_expired(entry) } {
        let len = suffix_bytes.len().min(limit);
        unsafe { kv_write_full_value(entry, pool, &suffix_bytes[..len], 0) };
        len as i64
    } else {
        let existing_len = unsafe { (*entry).value_len as usize };
        let append_len = suffix_bytes.len().min(limit.saturating_sub(existing_len));
        let new_val_len = existing_len + append_len;
        let mut new_val = unsafe { kv_read_full_value(entry, pool) };
        new_val.extend_from_slice(&suffix_bytes[..append_len]);
        let exp = unsafe { (*entry).expires_at };
        unsafe { kv_write_full_value(entry, pool, &new_val, exp) };
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let result = match unsafe { kv_find(&table, pool, &key_buf, key) } {
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let result = match unsafe { kv_find(&table, pool, &key_buf, key) } {
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let result = if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) } {
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let result = if let Some(entry) = unsafe { kv_find(&table, pool, &key_buf, key) } {
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

    let result = keys
        .iter()
        .map(|key| {
            let key_buf = make_key(key);
            match unsafe { kv_find(&table, pool, &key_buf, key) } {
                Some(entry) if !unsafe { entry_is_expired(entry) } => {
                    Some(unsafe { kv_read_full_value(entry, pool) })
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    for (key, value) in pairs {
        let key_buf = make_key(key);
        unsafe { kv_reserve(pool, key, value.len(), || evict_kv(htab, pool)) };
        let (entry, _found) =
            unsafe { kv_enter(&table, pool, &key_buf, key, || evict_kv(htab, pool)) };
        if !entry.is_null() {
            unsafe { kv_write_full_value(entry, pool, value, 0) };
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
    let pool = kv_pool_for(db_idx);
    let lk = lwlock(db_idx);
    let now = now_micros();
    let mut results = Vec::new();

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };

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

    // The hash, not the key: the sweep removes what it found and never has to
    // name it. Collected before any removal — `hash_search(HASH_REMOVE)` may
    // not run while a sequential scan over the same table is open.
    let mut to_delete: Vec<KeyHash> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let exp = unsafe { (*entry).expires_at };
        if exp != 0 && exp <= now {
            let key_ptr = unsafe { addr_of!((*entry).key) };
            to_delete.push(unsafe { key_ptr.read() });
        }
    }

    // Through `kv_remove`, so the sweep returns each expired value's chunks
    // rather than stranding them until the postmaster restarts.
    let pool = kv_pool_for(db_idx);
    for key_buf in &to_delete {
        unsafe { kv_remove(&table, pool, key_buf.as_ptr().cast()) };
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
        let pool = kv_pool_for(db_idx);
        matches!(unsafe { kv_find(&table, pool, &key_buf, key) }, Some(e) if !unsafe { entry_is_expired(e) })
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
fn glob_matches(pattern: &[u8], s: &[u8]) -> bool {
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

/// Pools per database: one each for the KV, hash and list tables.
const POOLS_PER_DB: usize = 3;

/// Stride between pools inside the single allocation that holds them all.
/// Rounded up so every pool starts aligned, whatever `pool_chunks()` is.
fn val_pool_stride() -> usize {
    val_pool_size(configured_pool_chunks()).next_multiple_of(64)
}

/// Total shmem for every chunk pool.
///
/// This is what replaced the three fixed overflow tables. Those reserved a
/// whole 448-byte tail for every entry the table could ever hold, on the theory
/// that every value might exceed the inline slot; the pools reserve one chunk
/// per entry slot and hand them out only to the values that actually spill.
pub fn mem_val_pool_total_size() -> usize {
    val_pool_stride() * POOLS_PER_DB * NUM_MEM_DBS + 8192
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

    // Every table is `HASH_BLOBS`, and none may be `HASH_STRINGS`: dynahash
    // compares string keys with `strncmp`, which makes two keys agreeing up to
    // their first NUL byte the same key. Redis keys are arbitrary bytes.
    let blob_flags = (pg_sys::HASH_ELEM
        | pg_sys::HASH_BLOBS
        | pg_sys::HASH_SHARED_MEM
        | pg_sys::HASH_FIXED_SIZE) as i32;

    let sz = htab_init_size();
    let sz_small = htab_init_size_small();

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
        }
    }

    // One allocation for all 24 pools, carved up by stride. A named
    // `ShmemInitStruct` apiece would work too, but each one costs an entry in
    // PostgreSQL's shared-memory index and none of them needs its own name.
    let chunks = configured_pool_chunks();
    let stride = val_pool_stride();
    unsafe { addr_of_mut!((*ctl).pool_chunks).write(chunks as u32) };
    unsafe {
        let mut found = false;
        let base = pg_sys::ShmemInitStruct(
            c"pg_redis_val_pools".as_ptr(),
            stride * POOLS_PER_DB * NUM_MEM_DBS,
            &mut found,
        )
        .cast::<u8>();
        for i in 0..NUM_MEM_DBS {
            let pools: [*mut ValPool; POOLS_PER_DB] = std::array::from_fn(|t| {
                base.add((i * POOLS_PER_DB + t) * stride).cast::<ValPool>()
            });
            for pool in pools {
                pool_init(pool, chunks);
            }
            addr_of_mut!((*ctl).kv_pool[i]).write(pools[0]);
            addr_of_mut!((*ctl).hash_pool[i]).write(pools[1]);
            addr_of_mut!((*ctl).list_pool[i]).write(pools[2]);
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    unsafe {
        reserve_chunks(pool, value.len(), || {
            evict_hash_key(htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_hash_key(htab, pool, &key_hash(key))
        })
    };
    let mut is_new = !found;
    if !entry.is_null() && !unsafe { hash_write_full_value(entry, pool, value) } {
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let result = unsafe { table.find(k.as_ptr().cast()) }
        .map(|entry| unsafe { hash_read_full_value(entry, pool) });
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut count = 0i64;
    for f in fields {
        let k = make_composite_key(key, f);
        if unsafe { hash_remove(&table, pool, k.as_ptr().cast()) } {
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
    let pool = hash_pool_for(db_idx);
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
        let val_str = unsafe { hash_read_full_value(entry, pool) };
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let results: Vec<Option<Vec<u8>>> = fields
        .iter()
        .map(|f| {
            let k = make_composite_key(key, f);
            unsafe { table.find(k.as_ptr().cast()) }
                .map(|entry| unsafe { hash_read_full_value(entry, pool) })
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_hash_key(htab, pool, &key_hash(key))
        })
    };
    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found {
        let s = delta.to_string();
        // An integer always fits inline, so the pool is never touched.
        let _ = unsafe { hash_write_full_value(entry, pool, s.as_bytes()) };
        Ok(delta)
    } else {
        let cur_bytes = unsafe { hash_read_full_value(entry, pool) };
        let cur: i64 = std::str::from_utf8(&cur_bytes)
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?
            .parse()
            .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
        let new_val = cur
            .checked_add(delta)
            .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;
        let ns = new_val.to_string();
        let _ = unsafe { hash_write_full_value(entry, pool, ns.as_bytes()) };
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
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    unsafe {
        reserve_chunks(pool, value.len(), || {
            evict_hash_key(htab, pool, &key_hash(key))
        })
    };
    let (entry, found) = unsafe {
        enter_or_evict(&table, k.as_ptr().cast(), || {
            evict_hash_key(htab, pool, &key_hash(key))
        })
    };
    let set = if !found && !entry.is_null() {
        let written = unsafe { hash_write_full_value(entry, pool, value) };
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
unsafe fn mem_del_hash_key(db_idx: usize, key: &[u8]) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let pool = hash_pool_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    // Collected before any removal — `hash_search(HASH_REMOVE)` may not run
    // while a sequential scan over the same table is open.
    let to_del: Vec<[u8; COMPOSITE_KEY_LEN]> = unsafe { entries_of(&table, &key_hash(key)) };
    let count = to_del.len() as i64;
    for k in &to_del {
        unsafe { hash_remove(&table, pool, k.as_ptr().cast()) };
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
unsafe fn mem_del_set_key(db_idx: usize, key: &[u8]) -> i64 {
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
            unsafe { zset_meta_member_added(meta, *score, member) };
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
                unsafe { zset_meta_score_changed(htab, meta, key, member, *score) };
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
    // The meta is maintained here exactly as `mem_zadd` maintains it. A member
    // that reaches the entry table and not the meta is invisible to `ZCARD` and
    // `ZPOPMIN` while `ZRANGE` still lists it, and the count stays wrong for
    // the life of the key. `ZADD ... INCR` comes through here too.
    //
    // Created lazily rather than up front so a refused `XX` leaves no empty
    // meta behind.
    let result = if entry.is_null() {
        None
    } else if !found {
        if xx {
            unsafe { table.remove(k.as_ptr().cast()) };
            None
        } else {
            unsafe { addr_of_mut!((*entry).score).write(delta) };
            let meta = unsafe {
                get_or_create_zset_meta(meta_htab, key, || {
                    evict_zset_key(htab, meta_htab, &key_hash(key))
                })
            };
            unsafe { zset_meta_member_added(meta, delta, member) };
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
            let meta = unsafe { find_zset_meta(meta_htab, key) };
            unsafe { zset_meta_score_changed(htab, meta, key, member, new_score) };
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

/// Fold a member that has just been created into its sorted set's meta.
///
/// A member the meta never hears about is invisible to everything that reads
/// the meta — `ZCARD` counts it out and `ZPOPMIN` cannot see it — while
/// `ZRANGE`, which scans the entries themselves, still returns it. `ZINCRBY`
/// creating a member is exactly that shape, which is why this is shared rather
/// than written out at each site.
unsafe fn zset_meta_member_added(meta: *mut ZsetMeta, score: f64, member: &[u8]) {
    if meta.is_null() {
        return;
    }
    unsafe {
        let count = (*meta).count;
        if count == 0 || score < (*meta).min_score {
            addr_of_mut!((*meta).min_score).write(score);
            let len = &mut *addr_of_mut!((*meta).min_member_len);
            write_meta_member(&mut (*meta).min_member, len, member);
        }
        if count == 0 || score > (*meta).max_score {
            addr_of_mut!((*meta).max_score).write(score);
            let len = &mut *addr_of_mut!((*meta).max_member_len);
            write_meta_member(&mut (*meta).max_member, len, member);
        }
        addr_of_mut!((*meta).count).write(count + 1);
    }
}

/// Fold a score change on an existing member into the meta.
///
/// A new extreme names itself. A score that moves *off* an extreme cannot name
/// its replacement without looking, which is the one case that pays for
/// `refresh_zset_meta`'s scan.
unsafe fn zset_meta_score_changed(
    zset_htab: *mut pg_sys::HTAB,
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
        let was_min = read_meta_member(&(*meta).min_member, (*meta).min_member_len) == member;
        let was_max = read_meta_member(&(*meta).max_member, (*meta).max_member_len) == member;
        if score < cur_min {
            addr_of_mut!((*meta).min_score).write(score);
            let len = &mut *addr_of_mut!((*meta).min_member_len);
            write_meta_member(&mut (*meta).min_member, len, member);
        } else if was_min && score > cur_min {
            refresh_zset_meta(zset_htab, meta, key);
        }
        if score > cur_max {
            addr_of_mut!((*meta).max_score).write(score);
            let len = &mut *addr_of_mut!((*meta).max_member_len);
            write_meta_member(&mut (*meta).max_member, len, member);
        } else if was_max && score < cur_max {
            refresh_zset_meta(zset_htab, meta, key);
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
        let pool = list_pool_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = get_or_create_meta(meta_htab, key, || {
            evict_list_key(htab, meta_htab, pool, &key_hash(key))
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
            reserve_chunks(pool, v.len(), || {
                evict_list_key(htab, meta_htab, pool, &key_hash(key))
            });
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(htab, meta_htab, pool, &key_hash(key))
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
        let pool = list_pool_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = get_or_create_meta(meta_htab, key, || {
            evict_list_key(htab, meta_htab, pool, &key_hash(key))
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
            reserve_chunks(pool, v.len(), || {
                evict_list_key(htab, meta_htab, pool, &key_hash(key))
            });
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(htab, meta_htab, pool, &key_hash(key))
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
        let pool = list_pool_for(db_idx);
        let result = if found && !entry.is_null() {
            Some(list_read_full_value(entry, pool))
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
        let pool = list_pool_for(db_idx);
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
        // `reserve_chunks` normally has to run before the entry exists; here it
        // already does, and `evict_list_key` is told to keep this key, so the
        // element being rewritten cannot be the one eviction takes. Nothing
        // below reads `meta` again, so a victim's meta going away is harmless.
        reserve_chunks(pool, value.len(), || {
            evict_list_key(htab, meta_htab, pool, &key_hash(key))
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

/// Replace a list's contents, renumbering positions from a fresh base.
///
/// Removing elements in place leaves holes, and every reader walks positions as
/// `min_pos + i * LIST_POS_STEP` — so a hole silently truncates the list at the
/// first gap, with the right count still reported. Rewriting the whole list is
/// O(n) on commands that were already O(n).
unsafe fn list_replace(
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
            remove_meta(meta_htab, key);
            return;
        }

        for (i, v) in elems.iter().enumerate() {
            let pos = min_pos + (i as i64) * LIST_POS_STEP;
            let k = make_list_key(key, pos);
            let (entry, _found): (*mut ListEntry, bool) =
                enter_raw(htab, k.as_ptr().cast::<c_void>(), || {
                    evict_list_key(htab, meta_htab, pool, &key_hash(key))
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
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() || (*meta).count == 0 {
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let pool = list_pool_for(db_idx);
        let mut elems = list_elements(htab, pool, meta, key);
        let result = match elems.iter().position(|e| e == pivot) {
            None => -1,
            Some(at) => {
                // `list_replace` returns the old elements' chunks before it
                // writes the new ones, so the only net demand is the inserted
                // value. `elems` is already an owned copy and `evict_list_key`
                // keeps this key, so evicting here cannot disturb either.
                reserve_chunks(pool, value.len(), || {
                    evict_list_key(htab, meta_htab, pool, &key_hash(key))
                });
                elems.insert(if before { at } else { at + 1 }, value.to_vec());
                list_replace(htab, meta_htab, pool, key, meta, &elems);
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
            list_replace(htab, meta_htab, pool, key, meta, &kept);
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

        let pool = list_pool_for(db_idx);
        for i in 0..len {
            if i < s || i > e {
                let pos = min_pos + i as i64 * LIST_POS_STEP;
                let k = make_list_key(key, pos);
                list_remove_at(htab, pool, &k);
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
            pg_sys::LWLockRelease(lk);
            return None;
        }
        let value = list_read_full_value(src_entry, pool);
        list_remove_at(htab, pool, &sk);

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
            evict_list_key(htab, meta_htab, pool, &key_hash(dst))
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
        // The source element's chunks went back above, so this normally finds
        // room already. It does not when the source value was inline and the
        // destination has to spill — an APPEND-shaped LMOVE onto a full pool.
        // Runs before the destination entry exists, as `reserve_chunks` asks.
        reserve_chunks(pool, value.len(), || {
            evict_list_key(htab, meta_htab, pool, &key_hash(dst))
        });
        let (entry, _f2): (*mut ListEntry, bool) =
            enter_raw(htab, dk.as_ptr().cast::<c_void>(), || {
                evict_list_key(htab, meta_htab, pool, &key_hash(dst))
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

        pg_sys::LWLockRelease(lk);
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
            list_remove_at(htab, pool, &k);
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
        let pool = kv_pool_for(db_idx);
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
            result = Some(with_kv_key(entry, pool, <[u8]>::to_vec));
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

    /// `composite_owner` reads the hash half of a composite key back out — it
    /// is how eviction finds every row belonging to a victim without ever
    /// seeing the Redis key.
    #[test]
    fn composite_keys_split_back_into_hash_and_member() {
        let k = make_composite_key(b"mykey", b"myfield");
        assert_eq!(&k[KEY_HASH_LEN..KEY_HASH_LEN + 7], b"myfield");
        assert_eq!(unsafe { composite_owner(k.as_ptr()) }, key_hash(b"mykey"));
        assert!(hash_matches_entry(k.as_ptr(), &key_hash(b"mykey")));
        assert!(!hash_matches_entry(k.as_ptr(), &key_hash(b"otherkey")));

        // A member filling the slot exactly leaves no NUL in the key at all.
        let full = vec![b'm'; MAX_MEMBER_LEN];
        let k = make_composite_key(b"k", &full);
        assert_eq!(&k[KEY_HASH_LEN..], &full[..]);
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
            ("HashEntry", size_of::<HashEntry>(), 216),
            ("SetEntry", size_of::<SetEntry>(), 144),
            ("ZsetEntry", size_of::<ZsetEntry>(), 152),
            ("ListEntry", size_of::<ListEntry>(), 96),
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
        // At the 8192 default this totals ~70 MiB of shared memory.
        let per_entry: usize = sizes.iter().map(|(_, s, _)| s).sum();
        assert!(
            per_entry <= 2048,
            "combined entry size {per_entry} exceeds the budget"
        );

        // The pool costs one link and one chunk per chunk, and nothing per
        // entry — which is the whole point of it. Three per database.
        assert_eq!(val_pool_size(1), size_of::<ValPool>() + 4 + CHUNK_LEN);
        assert_eq!(val_pool_size(8192), 16 + 8192 * 68);
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

        // A thousand-link chain has to come back in one piece too. (One byte
        // over the cap is refused before it ever reaches here — the debug
        // assertion in `value_write` is the backstop, and
        // `oversized_keys_fields_and_values_are_refused_by_memory_mode` covers
        // the refusal itself.)
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

    /// The HTAB key layouts must match the `keysize` values passed to
    /// `ShmemInitHash`, or lookups read past the end of the key.
    #[test]
    fn composite_key_layout_matches_its_parts() {
        assert_eq!(COMPOSITE_KEY_LEN, KEY_HASH_LEN + MAX_MEMBER_LEN);
        assert_eq!(LIST_KEY_LEN, KEY_HASH_LEN + 8);
        assert!(size_of::<HashEntry>() >= COMPOSITE_KEY_LEN);

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
        assert_eq!(&composite[KEY_HASH_LEN..KEY_HASH_LEN + 7], b"myfield");
        assert_eq!(composite[KEY_HASH_LEN + 7], 0, "member half is NUL-padded");

        let list_key = make_list_key(b"mykey", -42);
        assert_eq!(&list_key[..KEY_HASH_LEN], &key_hash(b"mykey"));
        assert_eq!(&list_key[KEY_HASH_LEN..], &(-42i64).to_le_bytes());
    }
}
