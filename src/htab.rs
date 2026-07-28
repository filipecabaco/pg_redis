use pgrx::pg_sys;
use std::ffi::c_void;
use std::marker::PhantomData;

/// Holds an LWLock for a scope, releasing it on the way out.
///
/// Every early return used to need its own `LWLockRelease`, and the one that
/// forgot it wedged the worker on its next command — `HINCRBY` on a non-integer
/// field returned through `?` with the hash lock still held. Twenty-six
/// functions had between two and five release points each, one per way out.
///
/// A PostgreSQL error longjmps past `Drop` exactly as it longjmps past a manual
/// release, so nothing changes on that path: either way the lock is released
/// when the transaction aborts.
pub struct LockGuard(*mut pg_sys::LWLock);

impl LockGuard {
    /// # Safety
    /// `lock` must be a valid LWLock this backend does not already hold.
    pub unsafe fn shared(lock: *mut pg_sys::LWLock) -> Self {
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_SHARED) };
        LockGuard(lock)
    }

    /// # Safety
    /// As `shared`.
    pub unsafe fn exclusive(lock: *mut pg_sys::LWLock) -> Self {
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        LockGuard(lock)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        unsafe { pg_sys::LWLockRelease(self.0) };
    }
}

/// Typed wrapper around a PostgreSQL shared-memory HTAB.
///
/// Every method is `unsafe fn` — callers must:
/// - Hold the appropriate LWLock before calling find/enter/remove/scan
/// - Ensure E's key field is the first field and matches the HTAB keysize
pub struct SharedTable<E> {
    htab: *mut pg_sys::HTAB,
    _marker: PhantomData<*mut E>,
}

impl<E> SharedTable<E> {
    /// Wrap a raw HTAB pointer. Returns None if the pointer is null.
    pub unsafe fn from_raw(htab: *mut pg_sys::HTAB) -> Option<Self> {
        if htab.is_null() {
            None
        } else {
            Some(Self {
                htab,
                _marker: PhantomData,
            })
        }
    }

    /// HASH_FIND: returns a mutable pointer to the entry, or None if not found.
    pub unsafe fn find(&self, key_ptr: *const c_void) -> Option<*mut E> {
        unsafe {
            let mut found = false;
            let entry = pg_sys::hash_search(
                self.htab,
                key_ptr,
                pg_sys::HASHACTION::HASH_FIND,
                &mut found,
            ) as *mut E;
            if found && !entry.is_null() {
                Some(entry)
            } else {
                None
            }
        }
    }

    /// `HASH_ENTER_NULL`: `(entry, was_present)`, or `None` when the table is
    /// full.
    ///
    /// Not `HASH_ENTER`. These tables are `HASH_FIXED_SIZE`, and dynahash
    /// answers a full one by raising — which unwinds by longjmp with no
    /// subtransaction in scope on this path, so a full cache would be a dead
    /// worker.
    pub unsafe fn enter(&self, key_ptr: *const c_void) -> Option<(*mut E, bool)> {
        unsafe {
            let mut found = false;
            let entry = pg_sys::hash_search(
                self.htab,
                key_ptr,
                pg_sys::HASHACTION::HASH_ENTER_NULL,
                &mut found,
            ) as *mut E;
            (!entry.is_null()).then_some((entry, found))
        }
    }

    /// HASH_REMOVE: removes the entry for key_ptr; no-op if not present.
    pub unsafe fn remove(&self, key_ptr: *const c_void) {
        unsafe {
            let mut found = false;
            pg_sys::hash_search(
                self.htab,
                key_ptr,
                pg_sys::HASHACTION::HASH_REMOVE,
                &mut found,
            );
        }
    }

    /// Returns an iterator over all entries. The caller must NOT hold the scan
    /// across a lock release/reacquire boundary.
    pub unsafe fn scan(&self) -> TableScan<E> {
        unsafe {
            let mut status = std::mem::zeroed::<pg_sys::HASH_SEQ_STATUS>();
            pg_sys::hash_seq_init(&mut status, self.htab);
            TableScan {
                status,
                done: false,
                _marker: PhantomData,
            }
        }
    }
}

pub struct TableScan<E> {
    status: pg_sys::HASH_SEQ_STATUS,
    done: bool,
    _marker: PhantomData<*mut E>,
}

impl<E> TableScan<E> {
    pub unsafe fn next(&mut self) -> Option<*mut E> {
        unsafe {
            if self.done {
                return None;
            }
            let ptr = pg_sys::hash_seq_search(&mut self.status) as *mut E;
            if ptr.is_null() {
                self.done = true;
                None
            } else {
                Some(ptr)
            }
        }
    }
}

impl<E> Drop for TableScan<E> {
    fn drop(&mut self) {
        if !self.done {
            unsafe { pg_sys::hash_seq_term(&mut self.status) };
        }
    }
}
