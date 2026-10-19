//! The shared-memory arena the memory engine lives in — and the only
//! `unsafe` the engine has.
//!
//! One region of PostgreSQL's main shared segment, sized by
//! `redis.maxmemory`, holds a header, an allocator (talc) and sixteen
//! [`Db`]s. The collections inside a `Db` allocate through [`Shm`], a
//! zero-sized handle to that allocator, so their buffers land in the arena
//! too and every worker process reads the same structures.
//!
//! The operating system backs a page of the region only once it is touched,
//! so the arena costs what the data does, up to the limit — Redis's
//! `maxmemory`, not a reservation paid up front.
//!
//! What makes this sound, and what each `unsafe` block below relies on:
//!
//! - **One address everywhere.** The main segment is mapped by the
//!   postmaster before it forks, so every process sees it at the same
//!   address and the plain pointers inside collections are valid in all of
//!   them. [`attach`] checks it rather than assuming it.
//! - **One holder at a time.** A database is reached only through
//!   [`with_db`], under its exclusive LWLock, so the `&mut Db` it hands out
//!   is the only reference in any process. The allocator has its own lock,
//!   taken innermost.
//! - **Failure unwinds, it does not corrupt.** An allocation the arena cannot
//!   satisfy panics; `with_db` catches it and the command answers `-OOM`.
//!   Rust's collections are panic-safe, so the structures stay consistent.

use std::cell::{Cell, UnsafeCell};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use allocator_api2::alloc::{AllocError, Allocator, Layout};
use pgrx::pg_sys;
use talc::DefaultBinning;
use talc::base::Talc;
use talc::source::Manual;

use crate::memdb::Db;
use crate::memdb::dict::Seed;

/// Every database can be held in memory.
pub const NUM_SLOTS: usize = 16;

/// The panic payload of an allocation the arena could not satisfy.
pub struct OutOfMemory;

/// Why [`with_db`] could not run its closure to the end.
#[derive(Debug, PartialEq, Eq)]
pub enum Fault {
    /// No arena: memory mode is off, or this process has not attached.
    NotReady,
    /// The arena is full.
    OutOfMemory,
    /// A database was asked for while this process already held one — a bug
    /// that would otherwise wait on its own lock forever.
    Reentered,
    /// The closure panicked.
    Failed,
}

#[repr(C)]
struct Arena {
    /// Where the arena was built. Every process must find it there.
    home: usize,
    /// `redis.maxmemory`, in bytes: what the arena was sized for.
    cap: usize,
    /// The limit in force: `cap`, or lower after `CONFIG SET maxmemory`.
    limit: AtomicUsize,
    /// Bytes handed out by the allocator, and the most it has reached.
    used: AtomicUsize,
    peak: AtomicUsize,
    seed: Seed,
    alloc_lock: *mut pg_sys::LWLock,
    db_locks: [*mut pg_sys::LWLock; NUM_SLOTS],
    talc: UnsafeCell<Talc<Manual, DefaultBinning>>,
    dbs: [UnsafeCell<Db<Shm>>; NUM_SLOTS],
}

/// Set in the postmaster when the segment is built, and inherited by every
/// process it forks.
static ARENA: AtomicPtr<Arena> = AtomicPtr::new(std::ptr::null_mut());

fn arena() -> Option<&'static Arena> {
    // SAFETY: non-null only once `startup` has written a whole `Arena` there,
    // in memory that lives as long as the process.
    unsafe { ARENA.load(Ordering::Acquire).as_ref() }
}

fn limit_bytes() -> usize {
    crate::MAXMEMORY.get() as usize * 1024 * 1024
}

/// Room past the limit: a command is admitted while use is under the limit
/// and may overshoot it, as in Redis; the headroom is what it overshoots
/// into before an allocation fails.
fn heap_bytes() -> usize {
    let limit = limit_bytes();
    limit + (limit / 4).max(32 << 20)
}

fn shmem_bytes() -> usize {
    std::mem::size_of::<Arena>() + heap_bytes()
}

/// Holds an LWLock for a scope. A PostgreSQL error unwinds through `Drop`
/// like any panic, so every way out releases it.
struct LockGuard(*mut pg_sys::LWLock);

impl LockGuard {
    /// # Safety
    /// `lock` must be a valid LWLock this process does not hold.
    unsafe fn exclusive(lock: *mut pg_sys::LWLock) -> Self {
        // SAFETY: the caller's contract — a valid LWLock this process does
        // not already hold, so acquiring it cannot self-deadlock.
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        LockGuard(lock)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // SAFETY: acquired in `exclusive` and not released since.
        unsafe { pg_sys::LWLockRelease(self.0) };
    }
}

/// From `shmem_request_hook`. PostgreSQL raises an ERROR if called at any
/// other time.
pub fn request() {
    // SAFETY: the tranche name is a NUL-terminated static; PostgreSQL checks
    // that it is the request phase.
    unsafe {
        pg_sys::RequestAddinShmemSpace(shmem_bytes());
        pg_sys::RequestNamedLWLockTranche(c"pg_redis_mem".as_ptr(), (NUM_SLOTS + 1) as i32);
    }
}

/// From `shmem_startup_hook`: build the arena, or find it built.
/// PostgreSQL raises an ERROR if `request` did not reserve the space.
pub fn startup() {
    // SAFETY: ShmemInitStruct returns the reserved region (or raises); the
    // first process to create it builds the arena in place before storing
    // its address, and no other process can see it until the postmaster
    // forks one.
    unsafe {
        let mut found = false;
        let at: *mut Arena =
            pg_sys::ShmemInitStruct(c"pg_redis_arena".as_ptr(), shmem_bytes(), &mut found).cast();
        if !found {
            let locks = pg_sys::GetNamedLWLockTranche(c"pg_redis_mem".as_ptr());
            let lock = |i: usize| std::ptr::addr_of_mut!((*locks.add(i)).lock);
            let mut key = [0u64; 2];
            if !pg_sys::pg_strong_random(key.as_mut_ptr().cast(), std::mem::size_of_val(&key)) {
                pgrx::error!("pg_redis: could not draw the hash key for shared memory");
            }
            let seed = Seed(key[0], key[1]);
            // The heap is the rest of the region; talc keeps its own
            // bookkeeping at the start of it and touches nothing else until
            // asked to allocate.
            let mut talc = Talc::new(Manual);
            let heap = at.cast::<u8>().add(std::mem::size_of::<Arena>());
            if talc.claim(heap, heap_bytes()).is_none() {
                pgrx::error!("pg_redis: shared memory too small for its allocator");
            }
            at.write(Arena {
                home: at as usize,
                cap: limit_bytes(),
                limit: AtomicUsize::new(limit_bytes()),
                used: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                seed,
                alloc_lock: lock(NUM_SLOTS),
                db_locks: std::array::from_fn(lock),
                talc: UnsafeCell::new(talc),
                dbs: std::array::from_fn(|_| UnsafeCell::new(Db::new_in(seed, Shm))),
            });
        }
        ARENA.store(at, Ordering::Release);
    }
}

/// Check, in a worker, that the arena is where its pointers say it is.
pub fn attach() {
    if let Some(a) = arena()
        && a.home != a as *const Arena as usize
    {
        pgrx::error!("pg_redis: shared memory is mapped at a different address in this process");
    }
}

/// What INFO memory reports.
#[derive(Default, Clone, Copy)]
pub struct Usage {
    pub used: usize,
    pub peak: usize,
    pub limit: usize,
}

pub fn usage() -> Option<Usage> {
    let a = arena()?;
    Some(Usage {
        used: a.used.load(Ordering::Relaxed),
        peak: a.peak.load(Ordering::Relaxed),
        limit: a.limit.load(Ordering::Relaxed),
    })
}

/// `CONFIG SET maxmemory`: a new limit, no higher than the arena was sized
/// for; 0 — Redis's "no limit" — is that size.
pub fn set_limit(bytes: u64) {
    if let Some(a) = arena() {
        let bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
        a.limit.store(
            if bytes == 0 { a.cap } else { bytes.min(a.cap) },
            Ordering::Relaxed,
        );
    }
}

thread_local! {
    static HOLDING: Cell<bool> = const { Cell::new(false) };
}

struct Holding;

impl Drop for Holding {
    fn drop(&mut self) {
        HOLDING.set(false);
    }
}

/// Run `f` on the database in `slot`, holding its lock throughout.
pub fn with_db<R>(slot: usize, f: impl FnOnce(&mut Db<Shm>) -> R) -> Result<R, Fault> {
    let a = arena().ok_or(Fault::NotReady)?;
    let cell = a.dbs.get(slot).ok_or(Fault::NotReady)?;
    if HOLDING.replace(true) {
        return Err(Fault::Reentered);
    }
    let _holding = Holding;
    // SAFETY: the lock is one of the tranche's, and `HOLDING` says this
    // process holds none of them.
    let _lock = unsafe { LockGuard::exclusive(a.db_locks[slot]) };
    // SAFETY: the exclusive lock makes this the only reference to the
    // database in any process until `_lock` drops, after `f` returns.
    let db = unsafe { &mut *cell.get() };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(db))).map_err(|p| {
        if p.is::<OutOfMemory>() {
            Fault::OutOfMemory
        } else {
            Fault::Failed
        }
    })
}

/// Bytes left under the limit, when there is an arena: what the keyspace
/// may grow by in one step (`Dict::grow_within`).
pub fn room() -> Option<usize> {
    let a = arena()?;
    Some(
        a.limit
            .load(Ordering::Relaxed)
            .saturating_sub(a.used.load(Ordering::Relaxed)),
    )
}

/// Whether the arena holds more than the limit: Redis's
/// `getMaxmemoryState` failing. What is done about it is `crate::evict`'s.
pub fn over_limit() -> bool {
    arena().is_some_and(|a| a.used.load(Ordering::Relaxed) > a.limit.load(Ordering::Relaxed))
}

/// The arena's allocator, as a handle that costs nothing to store: every
/// collection in the engine carries one.
#[derive(Clone, Copy, Debug, Default)]
pub struct Shm;

// SAFETY: blocks come from talc over the arena, which outlives every
// collection; each call holds the allocator's lock, so talc is never entered
// twice at once in any process.
unsafe impl Allocator for Shm {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() == 0 {
            let dangling = NonNull::new(std::ptr::without_provenance_mut::<u8>(layout.align()))
                .ok_or(AllocError)?;
            return Ok(NonNull::slice_from_raw_parts(dangling, 0));
        }
        let a = arena().ok_or(AllocError)?;
        let got = {
            // SAFETY: the allocator lock is taken innermost — no path holding
            // it takes another — and released before anything can panic.
            let _lock = unsafe { LockGuard::exclusive(a.alloc_lock) };
            // SAFETY: the lock above gives exclusive use of talc, and the
            // layout's size is not zero.
            unsafe { (*a.talc.get()).allocate(layout) }
        };
        match got {
            Some(p) => {
                let now = a.used.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
                a.peak.fetch_max(now, Ordering::Relaxed);
                Ok(NonNull::slice_from_raw_parts(p, layout.size()))
            }
            // Unwound to `with_db` rather than returned: the collections'
            // infallible methods would turn an `Err` into an abort. Without
            // the panic hook, so a full arena is a reply, not a log entry.
            None => std::panic::resume_unwind(Box::new(OutOfMemory)),
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        let Some(a) = arena() else { return };
        // SAFETY: as in `allocate`; `ptr` came from `allocate` with this
        // layout, so it is talc's to take back.
        unsafe {
            let _lock = LockGuard::exclusive(a.alloc_lock);
            (*a.talc.get()).deallocate(ptr.as_ptr(), layout);
        }
        a.used.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

/// `maxmemory-policy`, and the `redis.maxmemory_policy` GUC. Here rather
/// than in `access.rs` because the GUC derive implements an `unsafe` trait,
/// and this module is the one in the engine allowed `unsafe`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, pgrx::PostgresGucEnum)]
pub enum Policy {
    #[name = c"noeviction"]
    NoEviction,
    #[name = c"allkeys-lru"]
    AllKeysLru,
    #[name = c"allkeys-lfu"]
    AllKeysLfu,
    #[name = c"allkeys-random"]
    AllKeysRandom,
    #[name = c"volatile-lru"]
    VolatileLru,
    #[name = c"volatile-lfu"]
    VolatileLfu,
    #[name = c"volatile-random"]
    VolatileRandom,
    #[name = c"volatile-ttl"]
    VolatileTtl,
}

impl Policy {
    pub const ALL: [Policy; 8] = [
        Policy::NoEviction,
        Policy::AllKeysLru,
        Policy::AllKeysLfu,
        Policy::AllKeysRandom,
        Policy::VolatileLru,
        Policy::VolatileLfu,
        Policy::VolatileRandom,
        Policy::VolatileTtl,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Policy::NoEviction => "noeviction",
            Policy::AllKeysLru => "allkeys-lru",
            Policy::AllKeysLfu => "allkeys-lfu",
            Policy::AllKeysRandom => "allkeys-random",
            Policy::VolatileLru => "volatile-lru",
            Policy::VolatileLfu => "volatile-lfu",
            Policy::VolatileRandom => "volatile-random",
            Policy::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn parse(s: &str) -> Option<Policy> {
        Policy::ALL.into_iter().find(|p| p.name() == s)
    }

    /// `MAXMEMORY_FLAG_LFU`.
    pub fn lfu(self) -> bool {
        matches!(self, Policy::AllKeysLfu | Policy::VolatileLfu)
    }

    /// `MAXMEMORY_FLAG_LRU`.
    pub fn lru(self) -> bool {
        matches!(self, Policy::AllKeysLru | Policy::VolatileLru)
    }

    /// Whether only keys with a deadline may be evicted.
    pub fn volatile(self) -> bool {
        matches!(
            self,
            Policy::VolatileLru
                | Policy::VolatileLfu
                | Policy::VolatileRandom
                | Policy::VolatileTtl
        )
    }
}
