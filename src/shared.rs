//! The extension's fixed blocks of PostgreSQL shared memory: attached once,
//! in the postmaster's `shmem_startup_hook`, and handed out as references.
//!
//! A worker is forked from the postmaster after the hook has run, so it
//! inherits the attached address with the rest of its memory: the block is at
//! the same place in every process, and stays mapped for the life of the
//! server. That is what makes a `&'static` to it sound, and it is argued once,
//! here, instead of at every place that reads a counter.

use std::ffi::CStr;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use pgrx::pg_sys;

/// A shared-memory block holding one `T`.
pub struct Block<T> {
    ptr: AtomicPtr<T>,
}

/// Types whose all-zero bit pattern is a valid value and that are safe to
/// share between processes as they are: built from atomics, read and written
/// only through them. A block is zeroed when the postmaster first creates it,
/// so a block of anything else would hand out a value that was never built.
///
/// # Safety
/// The implementor must hold no field that zeroed memory does not make valid
/// (references, `NonZero`, enums without a zero variant, heap pointers) and
/// must mutate shared state only through atomics or `UnsafeCell` under a
/// cross-process lock.
pub unsafe trait Shareable: Sync {}

// SAFETY: nothing but atomic counters, for which zero is the start.
unsafe impl Shareable for crate::watch::WatchCtl {}
// SAFETY: atomics and arrays of structs of atomics; `server::init` sets the
// few fields zero is not the right start for, before any worker starts.
unsafe impl Shareable for crate::server::ServerCtl {}
// SAFETY: atomics only, read and written under its spinlock; zeroed, every
// slot is free and the lock open.
unsafe impl Shareable for crate::cursor::CursorTable {}

impl<T> Block<T> {
    pub const fn new() -> Self {
        Block {
            ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Attach the block named `name`, creating and zeroing it if this is the
    /// first process to ask. Returns whether it already existed.
    ///
    /// Call from the postmaster's `shmem_startup_hook` only, with `size` the
    /// amount the `shmem_request_hook` requested for it.
    pub fn attach(&self, name: &CStr, size: usize) -> bool {
        debug_assert!(size >= std::mem::size_of::<T>());
        let mut found = false;
        // SAFETY: `name` is a NUL-terminated string that outlives the call,
        // and `found` is a live `bool`. ShmemInitStruct returns `size` bytes of
        // the main shared segment, the same block on every call with `name`,
        // or raises an ERROR rather than returning null.
        let raw = unsafe { pg_sys::ShmemInitStruct(name.as_ptr(), size, &mut found) };
        if !found {
            // SAFETY: `raw` addresses `size` writable bytes no other process
            // can see yet: the postmaster attaches before forking anyone.
            unsafe { ptr::write_bytes(raw.cast::<u8>(), 0, size) };
        }
        self.ptr.store(raw.cast(), Ordering::Release);
        found
    }
}

impl<T: Shareable> Block<T> {
    /// The block, once attached.
    pub fn get(&self) -> Option<&'static T> {
        let p = self.ptr.load(Ordering::Acquire);
        // SAFETY: non-null only after `attach`, which pointed it at shared
        // memory mapped for the rest of the process's life and zeroed when
        // created. `T: Shareable` makes zeroed memory a valid `T` that may be
        // shared as `&T` between threads and processes.
        unsafe { p.as_ref() }
    }
}

// ──────────────────────────────── the hooks ─────────────────────────────────
//
// Shared memory is requested and attached from two postmaster hooks, which
// `_PG_init` installs here. Whatever another extension installed before is
// kept and called first.

static ON_REQUEST: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();
static ON_STARTUP: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();
static PREV_REQUEST: std::sync::OnceLock<pg_sys::shmem_request_hook_type> =
    std::sync::OnceLock::new();
static PREV_STARTUP: std::sync::OnceLock<pg_sys::shmem_startup_hook_type> =
    std::sync::OnceLock::new();

/// Run `on_request` in the postmaster's `shmem_request_hook` and `on_startup`
/// in its `shmem_startup_hook`. From `_PG_init`, once.
pub fn install_hooks(on_request: fn(), on_startup: fn()) {
    let _ = ON_REQUEST.set(on_request);
    let _ = ON_STARTUP.set(on_startup);
    // SAFETY: `_PG_init` runs in the postmaster, single-threaded, before
    // either hook can fire, so nothing reads the hook variables while they
    // change; the previous values are kept and chained.
    unsafe {
        let _ = PREV_REQUEST.set(pg_sys::shmem_request_hook);
        pg_sys::shmem_request_hook = Some(request_hook);
        let _ = PREV_STARTUP.set(pg_sys::shmem_startup_hook);
        pg_sys::shmem_startup_hook = Some(startup_hook);
    }
}

extern "C-unwind" fn request_hook() {
    if let Some(Some(prev)) = PREV_REQUEST.get() {
        // SAFETY: the hook another extension installed, called from the hook
        // PostgreSQL called, as it expects.
        unsafe { prev() };
    }
    if let Some(f) = ON_REQUEST.get() {
        f();
    }
}

extern "C-unwind" fn startup_hook() {
    if let Some(Some(prev)) = PREV_STARTUP.get() {
        // SAFETY: as in `request_hook`.
        unsafe { prev() };
    }
    if let Some(f) = ON_STARTUP.get() {
        f();
    }
}

/// Reserve `size` bytes of the main shared segment. From the request hook;
/// PostgreSQL raises an ERROR if called at any other time.
pub fn request(size: usize) {
    // SAFETY: no pointers are involved; PostgreSQL checks the phase itself.
    unsafe { pg_sys::RequestAddinShmemSpace(size) };
}
