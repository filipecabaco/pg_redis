//! The shared heap pub/sub, client tracking and the ACL users live in.
//!
//! Redis keeps a subscriber's subscriptions and its undelivered messages in
//! ordinary heap memory, so neither has a fixed size: a channel name, a
//! payload, the number of subscribers and the number of subscriptions are
//! bounded only by memory and by the client output buffer limits. Doing the
//! same across worker processes needs memory every process sees at the same
//! address and an allocator any thread can use: messages are delivered and
//! taken on connection threads, which may not call into PostgreSQL and so
//! cannot take an LWLock.
//!
//! So this is one region of PostgreSQL's main shared segment, sized by
//! `redis.pubsub_memory`, holding a header, a talc allocator behind a spin
//! lock, and one [`Root`] built in it at startup: the pub/sub registry, the
//! tracking table and the ACL users and log. [`PubHeap`] is a zero-sized handle to that allocator
//! for collections. The operating
//! system backs a page only once it is touched, so the region costs what is
//! queued and subscribed, up to the size.
//!
//! What makes this sound, and what each `unsafe` block below relies on:
//!
//! - **One address everywhere.** The region is created by the postmaster
//!   before it forks, so pointers into it are valid in every process.
//!   [`attach`] checks it rather than assuming it.
//! - **One user of talc at a time.** Every allocation and release holds the
//!   heap's spin lock, taken innermost: nothing that holds it takes another
//!   lock, and nothing under it can panic.
//! - **Failure unwinds.** An allocation the heap cannot satisfy panics with
//!   [`OutOfMemory`]; callers catch it at the top of each operation, and the
//!   collections, being panic-safe, stay consistent.

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

use allocator_api2::alloc::{AllocError, Allocator, Layout};
use pgrx::pg_sys;
use talc::DefaultBinning;
use talc::base::Talc;
use talc::source::Manual;

/// The panic payload of an allocation the heap could not satisfy.
pub struct OutOfMemory;

#[repr(C)]
struct Header {
    /// Where the region was built. Every process must find it there.
    home: usize,
    /// The heap's size in bytes.
    cap: usize,
    /// Bytes handed out by the allocator, and the most it has reached.
    used: AtomicUsize,
    peak: AtomicUsize,
    lock: AtomicU8,
    talc: UnsafeCell<Talc<Manual, DefaultBinning>>,
    /// The root built at startup, or null before then.
    root: AtomicPtr<Root>,
}

/// Set in the postmaster when the segment is built, and inherited by every
/// process it forks.
static HEADER: AtomicPtr<Header> = AtomicPtr::new(std::ptr::null_mut());

fn header() -> Option<&'static Header> {
    // SAFETY: non-null only once `startup` has written a whole `Header`
    // there, in memory that lives as long as the process.
    unsafe { HEADER.load(Ordering::Acquire).as_ref() }
}

fn heap_bytes() -> usize {
    crate::PUBSUB_MEMORY.get().max(1) as usize * 1024 * 1024
}

fn shmem_bytes() -> usize {
    std::mem::size_of::<Header>() + heap_bytes()
}

/// From `shmem_request_hook`.
pub fn request() {
    // SAFETY: PostgreSQL checks that it is the request phase.
    unsafe { pg_sys::RequestAddinShmemSpace(shmem_bytes()) };
}

/// Everything the heap holds.
pub struct Root {
    pub pubsub: crate::pubsub::Slots,
    pub tracking: Shared<crate::tracking::Table>,
    /// The ACL users' descriptions and the ACL LOG (`acl.rs`).
    pub acl: [Shared<HVec<u8>>; 2],
}

pub type HVec<T> = allocator_api2::vec::Vec<T, PubHeap>;

impl Root {
    fn new() -> Root {
        Root {
            pubsub: crate::pubsub::Slots::new(),
            tracking: Shared::new(crate::tracking::Table::new()),
            acl: [
                Shared::new(HVec::new_in(PubHeap)),
                Shared::new(HVec::new_in(PubHeap)),
            ],
        }
    }
}

/// From `shmem_startup_hook`, in the postmaster: build the heap, then the
/// root in it. Hash keys drawn there are stored in it, so every process
/// hashes alike.
pub fn startup() {
    // SAFETY: ShmemInitStruct returns the reserved region (or raises). The
    // first process to create it builds the header and the root in place
    // before storing its address, and no other process can see it until the
    // postmaster forks one.
    unsafe {
        let mut found = false;
        let at: *mut Header =
            pg_sys::ShmemInitStruct(c"pg_redis_pubsub_heap".as_ptr(), shmem_bytes(), &mut found)
                .cast();
        if !found {
            let mut talc = Talc::new(Manual);
            let heap = at.cast::<u8>().add(std::mem::size_of::<Header>());
            if talc.claim(heap, heap_bytes()).is_none() {
                pgrx::error!("pg_redis: shared memory too small for the pub/sub heap");
            }
            at.write(Header {
                home: at as usize,
                cap: heap_bytes(),
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                lock: AtomicU8::new(0),
                talc: UnsafeCell::new(talc),
                root: AtomicPtr::new(std::ptr::null_mut()),
            });
            HEADER.store(at, Ordering::Release);
            build_root(&*at);
        }
        HEADER.store(at, Ordering::Release);
    }
}

/// Check, in a worker, that the region is where its pointers say it is.
pub fn attach() {
    if let Some(h) = header()
        && h.home != h as *const Header as usize
    {
        pgrx::error!("pg_redis: shared memory is mapped at a different address in this process");
    }
}

/// Build the root through the heap whose header is `h`, once `HEADER`
/// points at it.
fn build_root(h: &Header) {
    let root = allocator_api2::boxed::Box::new_in(Root::new(), PubHeap);
    let (ptr, _) = allocator_api2::boxed::Box::into_raw_with_allocator(root);
    h.root.store(ptr, Ordering::Release);
}

/// The root `startup` built.
pub fn root() -> Option<&'static Root> {
    let p = header()?.root.load(Ordering::Acquire);
    // SAFETY: non-null only once `build_root` wrote a whole `Root` there, in
    // memory that lives as long as the process and is never freed.
    unsafe { p.as_ref() }
}

/// A value in the heap behind a spin lock of its own, for state any thread
/// of any worker reaches. Held briefly, and never while taking another
/// such lock.
pub struct Shared<T> {
    lock: AtomicU8,
    value: UnsafeCell<T>,
}

// SAFETY: `value` is reached only through a `Guard`, under `lock`.
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Shared<T> {
    pub fn new(value: T) -> Self {
        Shared {
            lock: AtomicU8::new(0),
            value: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> Guard<'_, T> {
        while self
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Guard(self)
    }
}

/// A [`Shared`] value with its lock held; released when dropped, unwinding
/// included, so a panic cannot leave it locked in every process.
pub struct Guard<'a, T>(&'a Shared<T>);

impl<T> std::ops::Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: the lock is held for as long as the guard.
        unsafe { &*self.0.value.get() }
    }
}

impl<T> std::ops::DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: the lock is held for as long as the guard, which this
        // borrows mutably, so this is the only reference.
        unsafe { &mut *self.0.value.get() }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.0.lock.store(0, Ordering::Release);
    }
}

/// Holds the heap's spin lock.
struct Held<'a>(&'a Header);

impl<'a> Held<'a> {
    fn new(h: &'a Header) -> Self {
        while h
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Held(h)
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.lock.store(0, Ordering::Release);
    }
}

/// The heap as an allocator, for collections that live in it.
#[derive(Clone, Copy, Default)]
pub struct PubHeap;

// SAFETY: allocations come from talc over memory reserved for as long as the
// process lives; every call into talc holds the spin lock, so no two run at
// once; a block is returned only to the allocator that gave it.
unsafe impl Allocator for PubHeap {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() == 0 {
            let dangling = NonNull::new(std::ptr::without_provenance_mut::<u8>(layout.align()))
                .ok_or(AllocError)?;
            return Ok(NonNull::slice_from_raw_parts(dangling, 0));
        }
        let h = header().ok_or(AllocError)?;
        let got = {
            let _held = Held::new(h);
            // SAFETY: the lock gives exclusive use of talc, and the layout's
            // size is not zero.
            unsafe { (*h.talc.get()).allocate(layout) }
        };
        match got {
            Some(p) => {
                let now = h.used.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
                h.peak.fetch_max(now, Ordering::Relaxed);
                Ok(NonNull::slice_from_raw_parts(p, layout.size()))
            }
            // Unwound to the operation's top rather than returned: the
            // collections' infallible methods would abort on an `Err`.
            None => std::panic::resume_unwind(Box::new(OutOfMemory)),
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        let Some(h) = header() else { return };
        {
            let _held = Held::new(h);
            // SAFETY: as in `allocate`; `ptr` came from `allocate` with this
            // layout, so it is talc's to take back.
            unsafe { (*h.talc.get()).deallocate(ptr.as_ptr(), layout) };
        }
        h.used.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

/// Run `f`, turning a full heap into `None` instead of a panic.
pub fn catching<R>(f: impl FnOnce() -> R) -> Option<R> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => Some(r),
        Err(p) if p.is::<OutOfMemory>() => None,
        Err(p) => std::panic::resume_unwind(p),
    }
}

/// A heap over ordinary process memory, for unit tests: builds the header
/// and the root in a leaked buffer and points the module at it, once per
/// test process.
#[cfg(test)]
pub fn for_tests() {
    let bytes = 256 << 20;
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let buf: &'static mut [u8] =
            Box::leak(vec![0u8; std::mem::size_of::<Header>() + bytes + 64].into_boxed_slice());
        let at = buf
            .as_mut_ptr()
            .wrapping_add(buf.as_ptr().align_offset(64))
            .cast::<Header>();
        // SAFETY: `at` is aligned, inside a leaked buffer large enough for
        // the header and the heap, used by nothing else.
        unsafe {
            let mut talc = Talc::new(Manual);
            let heap = at.cast::<u8>().add(std::mem::size_of::<Header>());
            talc.claim(heap, bytes).expect("claim the test heap");
            at.write(Header {
                home: at as usize,
                cap: bytes,
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                lock: AtomicU8::new(0),
                talc: UnsafeCell::new(talc),
                root: AtomicPtr::new(std::ptr::null_mut()),
            });
            HEADER.store(at, Ordering::Release);
            build_root(&*at);
        }
    });
}
