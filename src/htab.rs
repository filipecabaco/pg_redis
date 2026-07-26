use pgrx::pg_sys;
use std::ffi::c_void;
use std::marker::PhantomData;

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

    /// HASH_ENTER_NULL: returns (entry_ptr, was_already_present), or `None` when
    /// the table is full.
    ///
    /// Deliberately not `HASH_ENTER`. These tables are created `HASH_FIXED_SIZE`,
    /// and dynahash answers a full fixed-size table by raising
    /// `out of shared memory` — an `ereport(ERROR)`, which unwinds by longjmp.
    /// The shared-memory command path exists precisely to avoid opening a
    /// transaction, so there is no subtransaction in scope to catch it. Getting
    /// a null back and reporting it is the difference between a full cache and
    /// a worker that dies.
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
