use crate::htab::SharedTable;
use pgrx::pg_sys;
use std::ffi::c_void;
use std::ptr::addr_of;
use std::ptr::addr_of_mut;

// Maximum key length (null-terminated string fits in HTAB key)
pub const MAX_KEY_LEN: usize = 128;
// Inline value bytes stored directly in the main HTAB entry.
pub const INLINE_VAL_LEN: usize = 64;
// Maximum total value size accepted (inline + overflow).
pub const MAX_TOTAL_VAL_LEN: usize = 512;
// Overflow tail stored in the secondary overflow HTAB (bytes beyond INLINE_VAL_LEN).
pub const OVERFLOW_VAL_LEN: usize = MAX_TOTAL_VAL_LEN - INLINE_VAL_LEN;
// Number of even databases: 0,2,4,6,8,10,12,14 → indices 0..7
pub const NUM_MEM_DBS: usize = 8;
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
    pub key: [u8; MAX_KEY_LEN],
    pub field: [u8; MAX_KEY_LEN],
    pub value: [u8; INLINE_VAL_LEN],
    pub value_len: u32,
    pub has_overflow: u8,
    _pad: [u8; 3],
}

/// Overflow tail for HashEntry. Composite key is key[128] + field[128].
#[repr(C)]
pub struct HashOverflow {
    pub key: [u8; MAX_KEY_LEN],
    pub field: [u8; MAX_KEY_LEN],
    pub value: [u8; OVERFLOW_VAL_LEN],
}

/// Fixed-size entry for the set HTAB. Key is (redis_key[128], member[128]).
#[repr(C)]
pub struct SetEntry {
    pub key: [u8; MAX_KEY_LEN],
    pub member: [u8; MAX_KEY_LEN],
}

/// Fixed-size entry for the sorted set HTAB. Key is (redis_key[128], member[128]).
#[repr(C)]
pub struct ZsetEntry {
    pub key: [u8; MAX_KEY_LEN],
    pub member: [u8; MAX_KEY_LEN],
    pub score: f64,
}

/// Fixed-size entry for the list HTAB. Key is (redis_key[128], pos_bytes[8]).
#[repr(C)]
pub struct ListEntry {
    pub key: [u8; MAX_KEY_LEN],
    pub pos_bytes: [u8; 8],
    pub value: [u8; INLINE_VAL_LEN],
    pub value_len: u32,
    pub has_overflow: u8,
    _pad: [u8; 3],
}

/// Overflow tail for ListEntry. Key is key[128] + pos_bytes[8] = 136 bytes.
#[repr(C)]
pub struct ListOverflow {
    pub key: [u8; MAX_KEY_LEN],
    pub pos_bytes: [u8; 8],
    pub value: [u8; OVERFLOW_VAL_LEN],
}

/// Metadata entry for the list meta HTAB. Key is redis_key[128].
/// Tracks min/max position and count for O(1) LPUSH/RPUSH/LPOP/RPOP.
#[repr(C)]
pub struct ListMeta {
    pub key: [u8; MAX_KEY_LEN],
    pub min_pos: i64,
    pub max_pos: i64,
    pub count: i64,
}

/// Metadata entry for the sorted set meta HTAB. Key is redis_key[128].
/// Tracks min/max score and members for O(1) ZPOPMIN/ZPOPMAX/ZCARD.
#[repr(C)]
pub struct ZsetMeta {
    pub key: [u8; MAX_KEY_LEN],
    pub count: i64,
    pub min_score: f64,
    pub max_score: f64,
    pub min_member: [u8; MAX_KEY_LEN],
    pub max_member: [u8; MAX_KEY_LEN],
    pub min_member_len: u16,
    pub max_member_len: u16,
}

/// Metadata entry for the set meta HTAB. Key is redis_key[128].
/// Tracks count for O(1) SCARD/SPOP.
#[repr(C)]
pub struct SetMeta {
    pub key: [u8; MAX_KEY_LEN],
    pub count: i64,
}

/// Control block in static shared memory.
#[repr(C)]
pub struct MemControlBlock {
    /// One LWLock per even-database — operations on db 0 and db 2 never block each other.
    pub lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub hash_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub set_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub zset_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    pub list_lwlock: [*mut pg_sys::LWLock; NUM_MEM_DBS],
    /// Handles to the 8 HTAB tables (one per even db).
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
    key: &str,
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
    key: &str,
    value: &[u8],
    expires_at: i64,
) -> bool {
    let total = value.len();
    if total > MAX_TOTAL_VAL_LEN {
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
            let (of, _found) = unsafe { table.enter(key_buf.as_ptr().cast()) };
            if !of.is_null() {
                let tail = total - INLINE_VAL_LEN;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr().add(INLINE_VAL_LEN),
                        addr_of_mut!((*of).value) as *mut u8,
                        tail,
                    );
                }
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

unsafe fn kv_delete_overflow(overflow_htab: *mut pg_sys::HTAB, key: &str) {
    if let Some(table) = unsafe { SharedTable::<KvOverflow>::from_raw(overflow_htab) } {
        let key_buf = make_key(key);
        unsafe { table.remove(key_buf.as_ptr().cast()) };
    }
}

unsafe fn hash_read_full_value(
    entry: *const HashEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &str,
    field: &str,
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

unsafe fn hash_write_full_value(
    entry: *mut HashEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &str,
    field: &str,
    value: &[u8],
) {
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
            let (of, _found) = unsafe { table.enter(k.as_ptr().cast()) };
            if !of.is_null() {
                let tail = total - INLINE_VAL_LEN;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr().add(INLINE_VAL_LEN),
                        addr_of_mut!((*of).value) as *mut u8,
                        tail,
                    );
                }
            }
        }
    } else {
        unsafe { addr_of_mut!((*entry).has_overflow).write(0) };
        if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) } {
            let k = make_composite_key(key, field);
            unsafe { table.remove(k.as_ptr().cast()) };
        }
    }
}

unsafe fn hash_delete_overflow(overflow_htab: *mut pg_sys::HTAB, key: &str, field: &str) {
    if let Some(table) = unsafe { SharedTable::<HashOverflow>::from_raw(overflow_htab) } {
        let k = make_composite_key(key, field);
        unsafe { table.remove(k.as_ptr().cast()) };
    }
}

unsafe fn list_read_full_value(
    entry: *const ListEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &str,
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

unsafe fn list_write_full_value(
    entry: *mut ListEntry,
    overflow_htab: *mut pg_sys::HTAB,
    key: &str,
    pos: i64,
    value: &[u8],
) {
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
            let (of, _found) = unsafe { table.enter(k.as_ptr().cast()) };
            if !of.is_null() {
                let tail = total - INLINE_VAL_LEN;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr().add(INLINE_VAL_LEN),
                        addr_of_mut!((*of).value) as *mut u8,
                        tail,
                    );
                }
            }
        }
    } else {
        unsafe { addr_of_mut!((*entry).has_overflow).write(0) };
        if let Some(table) = unsafe { SharedTable::<ListOverflow>::from_raw(overflow_htab) } {
            let k = make_list_key(key, pos);
            unsafe { table.remove(k.as_ptr().cast()) };
        }
    }
}

unsafe fn list_delete_overflow(overflow_htab: *mut pg_sys::HTAB, key: &str, pos: i64) {
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

fn make_key(s: &str) -> [u8; MAX_KEY_LEN] {
    let mut key = [0u8; MAX_KEY_LEN];
    let bytes = s.as_bytes();
    let len = bytes.len().min(MAX_KEY_LEN - 1);
    key[..len].copy_from_slice(&bytes[..len]);
    key
}

/// GET: returns value if key exists and not expired.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_get(db_idx: usize, key: &str) -> Option<Vec<u8>> {
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
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_set(db_idx: usize, key: &str, value: &str, expires_at_us: i64) {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return;
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, _found) = unsafe { table.enter(key_buf.as_ptr().cast()) };
    if !entry.is_null() {
        unsafe { kv_write_full_value(entry, overflow_htab, key, value.as_bytes(), expires_at_us) };
    }

    unsafe { pg_sys::LWLockRelease(lk) };
}

/// DEL: delete one or more keys, return count deleted.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_del(db_idx: usize, keys: &[&str]) -> i64 {
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
pub unsafe fn mem_exists(db_idx: usize, keys: &[&str]) -> i64 {
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
pub unsafe fn mem_incr(db_idx: usize, key: &str, delta: i64) -> Result<i64, String> {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return Err("ERR memory not initialized".to_string());
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe { table.enter(key_buf.as_ptr().cast()) };
    let overflow_htab = kv_overflow_htab_for(db_idx);

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
pub unsafe fn mem_incr_float(db_idx: usize, key: &str, delta: f64) -> Result<String, String> {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return Err("ERR memory not initialized".to_string());
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe { table.enter(key_buf.as_ptr().cast()) };

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
pub unsafe fn mem_getset(db_idx: usize, key: &str, value: &str) -> Option<Vec<u8>> {
    let htab = htab_for(db_idx);
    let table = unsafe { SharedTable::<KvEntry>::from_raw(htab) }?;
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe { table.enter(key_buf.as_ptr().cast()) };

    let old = if found && !entry.is_null() && !unsafe { entry_is_expired(entry) } {
        Some(unsafe { kv_read_full_value(entry, overflow_htab, key) })
    } else {
        None
    };

    if !entry.is_null() {
        unsafe { kv_write_full_value(entry, overflow_htab, key, value.as_bytes(), 0) };
    }

    unsafe { pg_sys::LWLockRelease(lk) };
    old
}

/// GETDEL: get and delete atomically.
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_getdel(db_idx: usize, key: &str) -> Option<Vec<u8>> {
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
pub unsafe fn mem_append(db_idx: usize, key: &str, suffix: &str) -> i64 {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return 0;
    };
    let lk = lwlock(db_idx);
    let key_buf = make_key(key);
    let suffix_bytes = suffix.as_bytes();

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    let (entry, found) = unsafe { table.enter(key_buf.as_ptr().cast()) };
    let overflow_htab = kv_overflow_htab_for(db_idx);

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
pub unsafe fn mem_strlen(db_idx: usize, key: &str) -> i64 {
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
pub unsafe fn mem_ttl_raw(db_idx: usize, key: &str) -> (bool, i64) {
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
pub unsafe fn mem_set_expiry(db_idx: usize, key: &str, expires_at_us: i64) -> bool {
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
pub unsafe fn mem_persist(db_idx: usize, key: &str) -> bool {
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
pub unsafe fn mem_mget(db_idx: usize, keys: &[String]) -> Vec<Option<Vec<u8>>> {
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
pub unsafe fn mem_mset(db_idx: usize, pairs: &[(&str, &str)]) {
    let htab = htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<KvEntry>::from_raw(htab) }) else {
        return;
    };
    let overflow_htab = kv_overflow_htab_for(db_idx);
    let lk = lwlock(db_idx);

    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };

    for (key, value) in pairs {
        let key_buf = make_key(key);
        let (entry, _found) = unsafe { table.enter(key_buf.as_ptr().cast()) };
        if !entry.is_null() {
            unsafe { kv_write_full_value(entry, overflow_htab, key, value.as_bytes(), 0) };
        }
    }

    unsafe { pg_sys::LWLockRelease(lk) };
}

/// SCAN / KEYS: return keys matching glob pattern (always full scan, cursor always 0).
///
/// # Safety
/// Must be called from bgworker thread with mem_init_worker already called.
pub unsafe fn mem_scan(db_idx: usize, pattern: &str) -> Vec<Vec<u8>> {
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
        let key_str = std::str::from_utf8(&key_slice[..key_end]).unwrap_or("");
        if glob_matches(pattern, key_str) {
            results.push(key_str.as_bytes().to_vec());
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
pub unsafe fn mem_type(db_idx: usize, key: &str) -> &'static str {
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
            let htab2 = unsafe { addr_of!((*c).set_htab[db_idx]).read() };
            let lk2 = unsafe { addr_of!((*c).set_lwlock[db_idx]).read() };
            if !htab2.is_null() && !lk2.is_null() {
                unsafe { pg_sys::LWLockAcquire(lk2, pg_sys::LWLockMode::LW_SHARED) };
                let has_set = unsafe { has_any_set_entry_for_key(htab2, key) };
                unsafe { pg_sys::LWLockRelease(lk2) };
                if has_set {
                    return "set";
                }
            }
        }
        {
            let htab2 = unsafe { addr_of!((*c).zset_htab[db_idx]).read() };
            let lk2 = unsafe { addr_of!((*c).zset_lwlock[db_idx]).read() };
            if !htab2.is_null() && !lk2.is_null() {
                unsafe { pg_sys::LWLockAcquire(lk2, pg_sys::LWLockMode::LW_SHARED) };
                let has_zset = unsafe { has_any_zset_entry_for_key(htab2, key) };
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

unsafe fn has_any_entry_for_key(htab: *mut pg_sys::HTAB, key: &str) -> bool {
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let key_bytes = key.as_bytes();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let ek = unsafe { addr_of!((*entry).key) as *const u8 };
        let ek_slice = unsafe { std::slice::from_raw_parts(ek, MAX_KEY_LEN) };
        let ek_end = ek_slice.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
        if ek_slice[..ek_end] == key_bytes[..key_bytes.len().min(ek_end)]
            && key_bytes.len() == ek_end
        {
            return true;
        }
    }
    false
}

unsafe fn has_any_set_entry_for_key(htab: *mut pg_sys::HTAB, key: &str) -> bool {
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return false;
    };
    let key_bytes = key.as_bytes();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let ek = unsafe { addr_of!((*entry).key) as *const u8 };
        let ek_slice = unsafe { std::slice::from_raw_parts(ek, MAX_KEY_LEN) };
        let ek_end = ek_slice.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
        if ek_slice[..ek_end] == key_bytes[..key_bytes.len().min(ek_end)]
            && key_bytes.len() == ek_end
        {
            return true;
        }
    }
    false
}

unsafe fn has_any_zset_entry_for_key(htab: *mut pg_sys::HTAB, key: &str) -> bool {
    let Some(table) = (unsafe { SharedTable::<ZsetEntry>::from_raw(htab) }) else {
        return false;
    };
    let key_bytes = key.as_bytes();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        let ek = unsafe { addr_of!((*entry).key) as *const u8 };
        let ek_slice = unsafe { std::slice::from_raw_parts(ek, MAX_KEY_LEN) };
        let ek_end = ek_slice.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
        if ek_slice[..ek_end] == key_bytes[..key_bytes.len().min(ek_end)]
            && key_bytes.len() == ek_end
        {
            return true;
        }
    }
    false
}

unsafe fn has_any_list_entry_for_key(meta_htab: *mut pg_sys::HTAB, key: &str) -> bool {
    let Some(table) = (unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) }) else {
        return false;
    };
    let key_buf = make_key(key);
    match unsafe { table.find(key_buf.as_ptr().cast()) } {
        Some(meta) => unsafe { (*meta).count > 0 },
        None => false,
    }
}

/// Simple glob pattern matching supporting `*` and `?` wildcards.
pub fn glob_matches(pattern: &str, s: &str) -> bool {
    glob_match_impl(pattern.as_bytes(), s.as_bytes())
}

fn glob_match_impl(pat: &[u8], s: &[u8]) -> bool {
    match (pat.first(), s.first()) {
        (None, None) => true,
        (Some(&b'*'), _) => {
            glob_match_impl(&pat[1..], s) || (!s.is_empty() && glob_match_impl(pat, &s[1..]))
        }
        (Some(&b'?'), Some(_)) => glob_match_impl(&pat[1..], &s[1..]),
        (Some(p), Some(c)) if p == c => glob_match_impl(&pat[1..], &s[1..]),
        _ => false,
    }
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
                keysize: (MAX_KEY_LEN * 2) as pg_sys::Size,
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
                keysize: (MAX_KEY_LEN * 2) as pg_sys::Size,
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
                keysize: (MAX_KEY_LEN * 2) as pg_sys::Size,
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
                keysize: (MAX_KEY_LEN + 8) as pg_sys::Size,
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
                keysize: MAX_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ListMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                str_flags,
            );
            std::ptr::addr_of_mut!((*ctl).list_meta_htab[i]).write(htab);

            let name = format!("pg_redis_zset_meta_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: MAX_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<ZsetMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                str_flags,
            );
            std::ptr::addr_of_mut!((*ctl).zset_meta_htab[i]).write(htab);

            let name = format!("pg_redis_set_meta_{}\0", i * 2);
            let mut info = pg_sys::HASHCTL {
                keysize: MAX_KEY_LEN as pg_sys::Size,
                entrysize: std::mem::size_of::<SetMeta>() as pg_sys::Size,
                ..Default::default()
            };
            let htab = pg_sys::ShmemInitHash(
                name.as_ptr().cast(),
                sz_small,
                sz_small,
                &mut info,
                str_flags,
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
                keysize: (MAX_KEY_LEN * 2) as pg_sys::Size,
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
                keysize: (MAX_KEY_LEN + 8) as pg_sys::Size,
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

fn make_composite_key(key: &str, field: &str) -> [u8; 256] {
    let mut buf = [0u8; 256];
    let kb = key.as_bytes();
    let fb = field.as_bytes();
    let kl = kb.len().min(MAX_KEY_LEN);
    let fl = fb.len().min(MAX_KEY_LEN);
    buf[..kl].copy_from_slice(&kb[..kl]);
    buf[MAX_KEY_LEN..MAX_KEY_LEN + fl].copy_from_slice(&fb[..fl]);
    buf
}

fn make_list_key(key: &str, pos: i64) -> [u8; 136] {
    let mut buf = [0u8; 136];
    let kb = key.as_bytes();
    let kl = kb.len().min(MAX_KEY_LEN);
    buf[..kl].copy_from_slice(&kb[..kl]);
    buf[MAX_KEY_LEN..MAX_KEY_LEN + 8].copy_from_slice(&pos.to_le_bytes());
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

fn key_matches_entry(entry_key_ptr: *const u8, key: &str) -> bool {
    let key_bytes = key.as_bytes();
    let ek_slice = unsafe { std::slice::from_raw_parts(entry_key_ptr, MAX_KEY_LEN) };
    let ek_end = ek_slice.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
    ek_end == key_bytes.len() && &ek_slice[..ek_end] == key_bytes
}

// ─────────────────────────── Hash operations ────────────────────────────────

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hset(db_idx: usize, key: &str, field: &str, value: &str) -> bool {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let (entry, found) = unsafe { table.enter(k.as_ptr().cast()) };
    let is_new = !found;
    if !entry.is_null() {
        unsafe { hash_write_full_value(entry, overflow_htab, key, field, value.as_bytes()) };
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    is_new
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hget(db_idx: usize, key: &str, field: &str) -> Option<Vec<u8>> {
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
pub unsafe fn mem_hdel(db_idx: usize, key: &str, fields: &[&str]) -> i64 {
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
pub unsafe fn mem_hexists(db_idx: usize, key: &str, field: &str) -> bool {
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
pub unsafe fn mem_hgetall(db_idx: usize, key: &str) -> Vec<(String, Vec<u8>)> {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return vec![];
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut collected: Vec<(String, Vec<u8>)> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if !unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            continue;
        }
        let fb = unsafe { addr_of!((*entry).field) as *const u8 };
        let fs = unsafe { std::slice::from_raw_parts(fb, MAX_KEY_LEN) };
        let fe = fs.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
        let field_str = String::from_utf8_lossy(&fs[..fe]).into_owned();
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
pub unsafe fn mem_hkeys(db_idx: usize, key: &str) -> Vec<String> {
    unsafe { mem_hgetall(db_idx, key) }
        .into_iter()
        .map(|(f, _)| f)
        .collect()
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hvals(db_idx: usize, key: &str) -> Vec<Vec<u8>> {
    unsafe { mem_hgetall(db_idx, key) }
        .into_iter()
        .map(|(_, v)| v)
        .collect()
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hlen(db_idx: usize, key: &str) -> i64 {
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
pub unsafe fn mem_hmget(db_idx: usize, key: &str, fields: &[&str]) -> Vec<Option<Vec<u8>>> {
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
    key: &str,
    field: &str,
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
    let (entry, found) = unsafe { table.enter(k.as_ptr().cast()) };
    let result = if entry.is_null() {
        unsafe { pg_sys::LWLockRelease(lk) };
        return Err("ERR out of memory".to_string());
    } else if !found {
        let s = delta.to_string();
        unsafe { hash_write_full_value(entry, overflow_htab, key, field, s.as_bytes()) };
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
        unsafe { hash_write_full_value(entry, overflow_htab, key, field, ns.as_bytes()) };
        Ok(new_val)
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_hsetnx(db_idx: usize, key: &str, field: &str, value: &str) -> bool {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return false;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    let k = make_composite_key(key, field);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let (entry, found) = unsafe { table.enter(k.as_ptr().cast()) };
    let set = if !found && !entry.is_null() {
        unsafe { hash_write_full_value(entry, overflow_htab, key, field, value.as_bytes()) };
        true
    } else {
        false
    };
    unsafe { pg_sys::LWLockRelease(lk) };
    set
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_hash_key(db_idx: usize, key: &str) -> i64 {
    let htab = hash_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<HashEntry>::from_raw(htab) }) else {
        return 0;
    };
    let overflow_htab = hash_overflow_htab_for(db_idx);
    let lk = hash_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut to_del: Vec<[u8; 256]> = Vec::new();
    let mut to_del_fields: Vec<String> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            let fb = unsafe { addr_of!((*entry).field) as *const u8 };
            let fs = unsafe { std::slice::from_raw_parts(fb, MAX_KEY_LEN) };
            let fe = fs.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
            to_del_fields.push(String::from_utf8_lossy(&fs[..fe]).into_owned());
            let mut k = [0u8; 256];
            unsafe { std::ptr::copy_nonoverlapping(entry as *const u8, k.as_mut_ptr(), 256) };
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
pub unsafe fn mem_sadd(db_idx: usize, key: &str, members: &[&str]) -> i64 {
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
        let (entry, found) = unsafe { table.enter(k.as_ptr().cast()) };
        if !found && !entry.is_null() {
            count += 1;
        }
    }
    if count > 0 {
        let meta = unsafe { get_or_create_set_meta(meta_htab, key) };
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
pub unsafe fn mem_srem(db_idx: usize, key: &str, members: &[&str]) -> i64 {
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
pub unsafe fn mem_sismember(db_idx: usize, key: &str, member: &str) -> bool {
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
pub unsafe fn mem_smismember(db_idx: usize, key: &str, members: &[&str]) -> Vec<bool> {
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

unsafe fn set_collect_members(htab: *mut pg_sys::HTAB, key: &str) -> Vec<String> {
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
        let ms = unsafe { std::slice::from_raw_parts(mb, MAX_KEY_LEN) };
        let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
        members.push(String::from_utf8_lossy(&ms[..me]).into_owned());
    }
    members
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_smembers(db_idx: usize, key: &str) -> Vec<String> {
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
pub unsafe fn mem_scard(db_idx: usize, key: &str) -> i64 {
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
pub unsafe fn mem_spop(db_idx: usize, key: &str, count: i64) -> Vec<String> {
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
    let key_bytes = key.as_bytes();

    for _ in 0..n {
        if remaining == 0 {
            break;
        }
        let target_offset = (fast_random() % remaining as u64) as i64;
        let mut current_offset = 0i64;
        let mut to_remove: Option<[u8; 256]> = None;
        let mut to_remove_member = String::new();

        let mut scan = unsafe { table.scan() };
        while let Some(entry) = unsafe { scan.next() } {
            let ek = unsafe { addr_of!((*entry).key) as *const u8 };
            let ek_slice = unsafe { std::slice::from_raw_parts(ek, MAX_KEY_LEN) };
            let ek_end = ek_slice.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
            if ek_end != key_bytes.len() || &ek_slice[..ek_end] != key_bytes {
                continue;
            }
            if current_offset == target_offset {
                let mb = unsafe { addr_of!((*entry).member) as *const u8 };
                let ms = unsafe { std::slice::from_raw_parts(mb, MAX_KEY_LEN) };
                let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
                let member = String::from_utf8_lossy(&ms[..me]).into_owned();
                let mut composite = [0u8; 256];
                unsafe {
                    std::ptr::copy_nonoverlapping(entry as *const u8, composite.as_mut_ptr(), 256)
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
pub unsafe fn mem_srandmember(db_idx: usize, key: &str, count: i64) -> Vec<String> {
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
pub unsafe fn mem_smove(db_idx: usize, src: &str, dst: &str, member: &str) -> bool {
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
        let (dst_entry, dst_existed) = unsafe { table.enter(dst_k.as_ptr().cast()) };
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
                let dst_meta = unsafe { get_or_create_set_meta(meta_htab, dst) };
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
pub unsafe fn mem_sunion(db_idx: usize, keys: &[&str]) -> Vec<String> {
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let mut all: std::collections::HashSet<String> = std::collections::HashSet::new();
    for k in keys {
        let members = unsafe { set_collect_members(htab, k) };
        all.extend(members);
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    let mut result: Vec<String> = all.into_iter().collect();
    result.sort();
    result
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sinter(db_idx: usize, keys: &[&str]) -> Vec<String> {
    if keys.is_empty() {
        return vec![];
    }
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let first: std::collections::HashSet<String> = unsafe { set_collect_members(htab, keys[0]) }
        .into_iter()
        .collect();
    let mut result: std::collections::HashSet<String> = first;
    for k in &keys[1..] {
        let other: std::collections::HashSet<String> = unsafe { set_collect_members(htab, k) }
            .into_iter()
            .collect();
        result = result.intersection(&other).cloned().collect();
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    let mut out: Vec<String> = result.into_iter().collect();
    out.sort();
    out
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sdiff(db_idx: usize, keys: &[&str]) -> Vec<String> {
    if keys.is_empty() {
        return vec![];
    }
    let htab = set_htab_for(db_idx);
    if htab.is_null() {
        return vec![];
    }
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_SHARED) };
    let first: std::collections::HashSet<String> = unsafe { set_collect_members(htab, keys[0]) }
        .into_iter()
        .collect();
    let mut result = first;
    for k in &keys[1..] {
        let other: std::collections::HashSet<String> = unsafe { set_collect_members(htab, k) }
            .into_iter()
            .collect();
        result = result.difference(&other).cloned().collect();
    }
    unsafe { pg_sys::LWLockRelease(lk) };
    let mut out: Vec<String> = result.into_iter().collect();
    out.sort();
    out
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sunionstore(db_idx: usize, dst: &str, keys: &[&str]) -> i64 {
    let members = unsafe { mem_sunion(db_idx, keys) };
    unsafe { mem_del_set_key(db_idx, dst) };
    let refs: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
    unsafe { mem_sadd(db_idx, dst, &refs) }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sinterstore(db_idx: usize, dst: &str, keys: &[&str]) -> i64 {
    let members = unsafe { mem_sinter(db_idx, keys) };
    unsafe { mem_del_set_key(db_idx, dst) };
    let refs: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
    unsafe { mem_sadd(db_idx, dst, &refs) }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_sdiffstore(db_idx: usize, dst: &str, keys: &[&str]) -> i64 {
    let members = unsafe { mem_sdiff(db_idx, keys) };
    unsafe { mem_del_set_key(db_idx, dst) };
    let refs: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
    unsafe { mem_sadd(db_idx, dst, &refs) }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_del_set_key(db_idx: usize, key: &str) -> i64 {
    let htab = set_htab_for(db_idx);
    let Some(table) = (unsafe { SharedTable::<SetEntry>::from_raw(htab) }) else {
        return 0;
    };
    let meta_htab = set_meta_htab_for(db_idx);
    let lk = set_lwlock(db_idx);
    unsafe { pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let mut to_del: Vec<[u8; 256]> = Vec::new();
    let mut scan = unsafe { table.scan() };
    while let Some(entry) = unsafe { scan.next() } {
        if unsafe { key_matches_entry(addr_of!((*entry).key) as *const u8, key) } {
            let mut k = [0u8; 256];
            unsafe { std::ptr::copy_nonoverlapping(entry as *const u8, k.as_mut_ptr(), 256) };
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

unsafe fn zset_collect(htab: *mut pg_sys::HTAB, key: &str) -> Vec<(String, f64)> {
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
        let ms = unsafe { std::slice::from_raw_parts(mb, MAX_KEY_LEN) };
        let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
        let member = String::from_utf8_lossy(&ms[..me]).into_owned();
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
    key: &str,
    members: &[(f64, &str)],
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
        unsafe { get_or_create_zset_meta(meta_htab, key) }
    } else {
        std::ptr::null_mut()
    };
    for (score, member) in members {
        let k = make_composite_key(key, member);
        let (entry, found) = unsafe { table.enter(k.as_ptr().cast()) };
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
    key: &str,
    delta: f64,
    member: &str,
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
    let (entry, found) = unsafe { table.enter(k.as_ptr().cast()) };
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
pub unsafe fn mem_zrem(db_idx: usize, key: &str, members: &[&str]) -> i64 {
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
pub unsafe fn mem_zscore(db_idx: usize, key: &str, member: &str) -> Option<f64> {
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
pub unsafe fn mem_zcard(db_idx: usize, key: &str) -> i64 {
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
pub unsafe fn mem_zincrby(db_idx: usize, key: &str, delta: f64, member: &str) -> f64 {
    unsafe { mem_zadd_incr(db_idx, key, delta, member, false, false, false, false) }
        .unwrap_or(delta)
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrank(
    db_idx: usize,
    key: &str,
    member: &str,
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
    key: &str,
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
    key: &str,
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
            .map(|(m, sc)| {
                (
                    m.as_bytes().to_vec(),
                    if withscores { Some(*sc) } else { None },
                )
            })
            .collect()
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
#[allow(clippy::too_many_arguments)]
pub unsafe fn mem_zrange_by_score(
    db_idx: usize,
    key: &str,
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
            .map(|(m, s)| (m.into_bytes(), Some(s)))
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
    key: &str,
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
            .map(|(m, _)| m.into_bytes())
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

fn lex_in_range(m: &str, min: &crate::commands::LexBound, max: &crate::commands::LexBound) -> bool {
    use crate::commands::LexBound;
    let lo = match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Inclusive(s) => m >= s.as_str(),
        LexBound::Exclusive(s) => m > s.as_str(),
    };
    let hi = match max {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Inclusive(s) => m <= s.as_str(),
        LexBound::Exclusive(s) => m < s.as_str(),
    };
    lo && hi
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zlexcount(
    db_idx: usize,
    key: &str,
    min: &crate::commands::LexBound,
    max: &crate::commands::LexBound,
) -> i64 {
    unsafe { mem_zrangebylex(db_idx, key, min, max, false, None).len() as i64 }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zpopmin(db_idx: usize, key: &str, count: i64) -> Vec<(Vec<u8>, f64)> {
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
                results.push((min_member.into_bytes(), min_score));
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
        let chosen: Vec<(String, f64)> = all.into_iter().take(take).collect();
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
        chosen
            .into_iter()
            .map(|(m, s)| (m.into_bytes(), s))
            .collect()
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zpopmax(db_idx: usize, key: &str, count: i64) -> Vec<(Vec<u8>, f64)> {
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
                results.push((max_member.into_bytes(), max_score));
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
        let chosen: Vec<(String, f64)> = all.into_iter().take(take).collect();
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
        chosen
            .into_iter()
            .map(|(m, s)| (m.into_bytes(), s))
            .collect()
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zrandmember(
    db_idx: usize,
    key: &str,
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
                .map(|(m, s)| (m.into_bytes(), if withscores { Some(s) } else { None }))
                .collect()
        } else {
            let need = (-count) as usize;
            let len = all.len();
            (0..need)
                .map(|i| {
                    let (m, s) = &all[i % len];
                    (
                        m.as_bytes().to_vec(),
                        if withscores { Some(*s) } else { None },
                    )
                })
                .collect()
        }
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_zremrangebyrank(db_idx: usize, key: &str, start: i64, stop: i64) -> i64 {
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
        let to_del: Vec<String> = all[s..=e].iter().map(|(m, _)| m.clone()).collect();
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
    key: &str,
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
        let to_del: Vec<String> = all
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
    key: &str,
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
        let to_del: Vec<String> = all
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
pub unsafe fn mem_zmsmembers(db_idx: usize, key: &str, members: &[&str]) -> Vec<Option<f64>> {
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
    dst: &str,
    keys: &[&str],
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
        let mut map: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
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
        let to_del: Vec<String> = {
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
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_ENTER,
                &mut found,
            ) as *mut ZsetEntry;
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(meta_htab, dst);
            } else {
                let meta = get_or_create_zset_meta(meta_htab, dst);
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
    dst: &str,
    keys: &[&str],
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
        let first: std::collections::HashMap<String, f64> = zset_collect(htab, keys[0])
            .into_iter()
            .map(|(m, s)| (m, s * w0))
            .collect();
        let mut result = first;
        for (ki, k) in keys[1..].iter().enumerate() {
            let w = weights.get(ki + 1).copied().unwrap_or(1.0);
            let other: std::collections::HashMap<String, f64> = zset_collect(htab, k)
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
        let to_del: Vec<String> = zset_collect(htab, dst)
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
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_ENTER,
                &mut found,
            ) as *mut ZsetEntry;
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(meta_htab, dst);
            } else {
                let meta = get_or_create_zset_meta(meta_htab, dst);
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
pub unsafe fn mem_zdiffstore(db_idx: usize, dst: &str, keys: &[&str]) -> i64 {
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
        let first: std::collections::HashMap<String, f64> =
            zset_collect(htab, keys[0]).into_iter().collect();
        let mut result = first;
        for k in &keys[1..] {
            let other: std::collections::HashSet<String> =
                zset_collect(htab, k).into_iter().map(|(m, _)| m).collect();
            result.retain(|m, _| !other.contains(m));
        }
        let to_del: Vec<String> = zset_collect(htab, dst)
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
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_ENTER,
                &mut found,
            ) as *mut ZsetEntry;
            if !entry.is_null() {
                addr_of_mut!((*entry).score).write(*s);
            }
        }
        if !meta_htab.is_null() {
            if count == 0 {
                remove_zset_meta(meta_htab, dst);
            } else {
                let meta = get_or_create_zset_meta(meta_htab, dst);
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
pub unsafe fn mem_del_zset_key(db_idx: usize, key: &str) -> i64 {
    unsafe {
        let htab = zset_htab_for(db_idx);
        if htab.is_null() {
            return 0;
        }
        let meta_htab = zset_meta_htab_for(db_idx);
        let lk = zset_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let to_del: Vec<String> = zset_collect(htab, key)
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

unsafe fn get_or_create_meta(meta_htab: *mut pg_sys::HTAB, key: &str) -> *mut ListMeta {
    let Some(table) = (unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) }) else {
        return std::ptr::null_mut();
    };
    let key_buf = make_key(key);
    let (meta, found) = unsafe { table.enter(key_buf.as_ptr().cast()) };
    if !meta.is_null() && !found {
        unsafe {
            addr_of_mut!((*meta).min_pos).write(0);
            addr_of_mut!((*meta).max_pos).write(0);
            addr_of_mut!((*meta).count).write(0);
        }
    }
    meta
}

unsafe fn find_meta(meta_htab: *mut pg_sys::HTAB, key: &str) -> *mut ListMeta {
    let Some(table) = (unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) }) else {
        return std::ptr::null_mut();
    };
    let key_buf = make_key(key);
    unsafe { table.find(key_buf.as_ptr().cast()) }.unwrap_or(std::ptr::null_mut())
}

unsafe fn remove_meta(meta_htab: *mut pg_sys::HTAB, key: &str) {
    if let Some(table) = unsafe { SharedTable::<ListMeta>::from_raw(meta_htab) } {
        let key_buf = make_key(key);
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

unsafe fn write_meta_member(dest: &mut [u8; MAX_KEY_LEN], len: &mut u16, member: &str) {
    let mb = member.as_bytes();
    let ml = mb.len().min(MAX_KEY_LEN);
    dest[..ml].copy_from_slice(&mb[..ml]);
    if ml < MAX_KEY_LEN {
        dest[ml] = 0;
    }
    *len = ml as u16;
}

unsafe fn read_meta_member(src: &[u8; MAX_KEY_LEN], len: u16) -> String {
    let l = (len as usize).min(MAX_KEY_LEN);
    String::from_utf8_lossy(&src[..l]).into_owned()
}

unsafe fn get_or_create_zset_meta(meta_htab: *mut pg_sys::HTAB, key: &str) -> *mut ZsetMeta {
    unsafe {
        let key_buf = make_key(key);
        let mut found = false;
        let meta = pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_ENTER,
            &mut found,
        ) as *mut ZsetMeta;
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

unsafe fn find_zset_meta(meta_htab: *mut pg_sys::HTAB, key: &str) -> *mut ZsetMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = make_key(key);
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

unsafe fn remove_zset_meta(meta_htab: *mut pg_sys::HTAB, key: &str) {
    unsafe {
        if meta_htab.is_null() {
            return;
        }
        let key_buf = make_key(key);
        let mut found = false;
        pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_REMOVE,
            &mut found,
        );
    }
}

unsafe fn refresh_zset_meta(zset_htab: *mut pg_sys::HTAB, meta: *mut ZsetMeta, key: &str) {
    unsafe {
        let mut new_min = f64::INFINITY;
        let mut new_max = f64::NEG_INFINITY;
        let mut min_member = String::new();
        let mut max_member = String::new();

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
            let ms = std::slice::from_raw_parts(mb, MAX_KEY_LEN);
            let me = ms.iter().position(|&b| b == 0).unwrap_or(MAX_KEY_LEN);
            let member = String::from_utf8_lossy(&ms[..me]).into_owned();
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

unsafe fn get_or_create_set_meta(meta_htab: *mut pg_sys::HTAB, key: &str) -> *mut SetMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = make_key(key);
        let mut found = false;
        let meta = pg_sys::hash_search(
            meta_htab,
            key_buf.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_ENTER,
            &mut found,
        ) as *mut SetMeta;
        if !meta.is_null() && !found {
            addr_of_mut!((*meta).count).write(0);
        }
        meta
    }
}

unsafe fn find_set_meta(meta_htab: *mut pg_sys::HTAB, key: &str) -> *mut SetMeta {
    unsafe {
        if meta_htab.is_null() {
            return std::ptr::null_mut();
        }
        let key_buf = make_key(key);
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

unsafe fn remove_set_meta(meta_htab: *mut pg_sys::HTAB, key: &str) {
    unsafe {
        if meta_htab.is_null() {
            return;
        }
        let key_buf = make_key(key);
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
pub unsafe fn mem_lpush(db_idx: usize, key: &str, values: &[&str]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let overflow_htab = list_overflow_htab_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = get_or_create_meta(meta_htab, key);
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
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_ENTER,
                &mut found,
            ) as *mut ListEntry;
            if !entry.is_null() {
                list_write_full_value(entry, overflow_htab, key, pos, v.as_bytes());
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
pub unsafe fn mem_rpush(db_idx: usize, key: &str, values: &[&str]) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let overflow_htab = list_overflow_htab_for(db_idx);
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = get_or_create_meta(meta_htab, key);
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
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_ENTER,
                &mut found,
            ) as *mut ListEntry;
            if !entry.is_null() {
                list_write_full_value(entry, overflow_htab, key, pos, v.as_bytes());
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
pub unsafe fn mem_lpushx(db_idx: usize, key: &str, values: &[&str]) -> i64 {
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
pub unsafe fn mem_rpushx(db_idx: usize, key: &str, values: &[&str]) -> i64 {
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
pub unsafe fn mem_lpop(db_idx: usize, key: &str, count: Option<i64>) -> Vec<Vec<u8>> {
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
pub unsafe fn mem_rpop(db_idx: usize, key: &str, count: Option<i64>) -> Vec<Vec<u8>> {
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
pub unsafe fn mem_llen(db_idx: usize, key: &str) -> i64 {
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
pub unsafe fn mem_lrange(db_idx: usize, key: &str, start: i64, stop: i64) -> Vec<Vec<u8>> {
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
pub unsafe fn mem_lindex(db_idx: usize, key: &str, index: i64) -> Option<Vec<u8>> {
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
pub unsafe fn mem_lset(db_idx: usize, key: &str, index: i64, value: &str) -> bool {
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
            list_write_full_value(entry, overflow_htab, key, pos, value.as_bytes());
        }
        pg_sys::LWLockRelease(lk);
        found
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_lrem(db_idx: usize, key: &str, count: i64, value: &str) -> i64 {
    unsafe {
        let htab = list_htab_for(db_idx);
        let meta_htab = list_meta_htab_for(db_idx);
        if htab.is_null() || meta_htab.is_null() {
            return 0;
        }
        let lk = list_lwlock(db_idx);
        pg_sys::LWLockAcquire(lk, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let meta = find_meta(meta_htab, key);
        if meta.is_null() {
            pg_sys::LWLockRelease(lk);
            return 0;
        }
        let current_count = (*meta).count;
        if current_count == 0 {
            pg_sys::LWLockRelease(lk);
            return 0;
        }

        let overflow_htab = list_overflow_htab_for(db_idx);
        let min_pos = (*meta).min_pos;
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };

        let mut positions: Vec<i64> = (0..current_count)
            .map(|i| min_pos + i * LIST_POS_STEP)
            .collect();
        if count < 0 {
            positions.reverse();
        }

        let mut to_del = Vec::new();
        for pos in &positions {
            if to_del.len() >= limit {
                break;
            }
            let k = make_list_key(key, *pos);
            let mut found = false;
            let entry = pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut ListEntry;
            if found && !entry.is_null() {
                let v = list_read_full_value(entry, overflow_htab, key, *pos);
                if v == value.as_bytes() {
                    to_del.push(*pos);
                }
            }
        }

        for pos in &to_del {
            let k = make_list_key(key, *pos);
            let mut found = false;
            pg_sys::hash_search(
                htab,
                k.as_ptr().cast::<c_void>(),
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
            list_delete_overflow(overflow_htab, key, *pos);
        }

        let removed = to_del.len() as i64;
        let new_count = current_count - removed;
        if new_count == 0 {
            remove_meta(meta_htab, key);
        } else {
            addr_of_mut!((*meta).count).write(new_count);
            if !to_del.is_empty() {
                let new_min_pos = (0..current_count)
                    .map(|i| min_pos + i * LIST_POS_STEP)
                    .find(|p| {
                        let k = make_list_key(key, *p);
                        let mut f = false;
                        pg_sys::hash_search(
                            htab,
                            k.as_ptr().cast::<c_void>(),
                            pg_sys::HASHACTION::HASH_FIND,
                            &mut f,
                        );
                        f
                    })
                    .unwrap_or(min_pos);
                let new_max_pos = (0..current_count)
                    .rev()
                    .map(|i| min_pos + i * LIST_POS_STEP)
                    .find(|p| {
                        let k = make_list_key(key, *p);
                        let mut f = false;
                        pg_sys::hash_search(
                            htab,
                            k.as_ptr().cast::<c_void>(),
                            pg_sys::HASHACTION::HASH_FIND,
                            &mut f,
                        );
                        f
                    })
                    .unwrap_or(min_pos);
                addr_of_mut!((*meta).min_pos).write(new_min_pos);
                addr_of_mut!((*meta).max_pos).write(new_max_pos);
            }
        }

        pg_sys::LWLockRelease(lk);
        removed
    }
}

/// # Safety
/// - Must be called from a bgworker thread after `mem_init_worker` has set the thread-local CTL_PTR.
/// - The caller must ensure no concurrent writers bypass the per-db LWLock acquired internally.
pub unsafe fn mem_ltrim(db_idx: usize, key: &str, start: i64, stop: i64) {
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
    src: &str,
    dst: &str,
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

        let dst_meta = get_or_create_meta(meta_htab, dst);
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
        let mut f2 = false;
        let entry = pg_sys::hash_search(
            htab,
            dk.as_ptr().cast::<c_void>(),
            pg_sys::HASHACTION::HASH_ENTER,
            &mut f2,
        ) as *mut ListEntry;
        if !entry.is_null() {
            list_write_full_value(entry, overflow_htab, dst, dst_pos, &value);
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
    key: &str,
    value: &str,
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
        let limit = count.map(|c| c.max(0) as usize).unwrap_or(1);
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
                if v == value.as_bytes() {
                    if skip > 0 {
                        skip -= 1;
                        continue;
                    }
                    let display_idx = if rank < 0 {
                        current_count - 1 - logical_i
                    } else {
                        logical_i
                    };
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
pub unsafe fn mem_del_list_key(db_idx: usize, key: &str) -> i64 {
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
pub unsafe fn mem_zset_collect_all(db_idx: usize, key: &str) -> Vec<(String, f64)> {
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
pub unsafe fn mem_del_all_types(db_idx: usize, key: &str) -> i64 {
    unsafe {
        let sum = mem_del_hash_key(db_idx, key)
            + mem_del_set_key(db_idx, key)
            + mem_del_zset_key(db_idx, key)
            + mem_del_list_key(db_idx, key);
        sum.min(1)
    }
}
