//! One reader/writer lock per database, server-wide, so that a command made of
//! several steps is atomic against every other client on that database, on
//! every worker, as it is in Redis.
//!
//! The locks live in shared memory and are taken only by the workers' main
//! threads, around what they run for clients:
//!
//! - an ordinary command, or a batch of them from several clients, holds each
//!   database it touches **shared**, so unrelated commands keep running in
//!   parallel across workers;
//! - `EXEC`, a script, and a command built from several steps hold their
//!   databases **exclusive** for their whole run, commit included;
//! - several databases are always taken in ascending order, which is what
//!   makes waiting for them deadlock-free.
//!
//! A waiting writer stops new readers from entering, so a stream of shared
//! holders cannot starve an exclusive request.
//!
//! Lock order: these locks first, then the memory engine's per-database
//! LWLocks and PostgreSQL's row and advisory locks, which a holder takes and
//! releases inside its run. Nothing waits for these locks while holding one of
//! those: every wait happens before the run's transaction starts. A script
//! that reaches a database it does not hold only ever tries the lock
//! ([`reach`]), and fails the call rather than wait inside its transaction.
#![forbid(unsafe_code)]

use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::commands::{Command, NUM_DBS};

/// A set of databases: bit `d` is database `d`.
pub type Mask = u16;

/// Every database.
pub const ALL: Mask = Mask::MAX;

pub const fn bit(db: u8) -> Mask {
    1 << (db as u32 % NUM_DBS as u32)
}

// The state word: the shared holders in the low 16 bits, the writers waiting
// in the next 15, and the writer holding it in the top bit.
const READERS: u32 = 0xFFFF;
const WAITER: u32 = 1 << 16;
const WAITERS: u32 = 0x7FFF << 16;
const WRITER: u32 = 1 << 31;

/// Spins before sleeping: holds are short, a sleep costs a system call each
/// way.
const SPINS: u32 = 64;
/// How long one sleep lasts at most. A wake is sent on every release that
/// could let a sleeper in; the bound is a safety net, not the mechanism.
const NAP: Duration = Duration::from_millis(50);

/// One database's lock: two words, zero when free.
#[repr(C)]
pub struct RwLock {
    state: AtomicU32,
    /// Processes asleep on `state`, so a release that finds none skips the
    /// wake-up call.
    sleepers: AtomicU32,
}

impl RwLock {
    #[cfg(test)]
    const fn new() -> RwLock {
        RwLock {
            state: AtomicU32::new(0),
            sleepers: AtomicU32::new(0),
        }
    }

    /// Wait for `state` to move from `seen`. A spin first, then a futex sleep.
    fn pause(&self, seen: u32, spins: &mut u32) {
        if *spins < SPINS {
            *spins += 1;
            std::hint::spin_loop();
            return;
        }
        // Counted before the futex checks the word: a release that changed
        // the word after we read it either sees the count and wakes us, or
        // its change is what the futex finds, and it returns at once.
        self.sleepers.fetch_add(1, Ordering::SeqCst);
        crate::sys::futex_wait(&self.state, seen, NAP);
        self.sleepers.fetch_sub(1, Ordering::SeqCst);
    }

    fn wake(&self) {
        if self.sleepers.load(Ordering::SeqCst) > 0 {
            crate::sys::futex_wake_all(&self.state);
        }
    }

    /// Take it shared if no writer holds it or waits for it.
    pub fn try_shared(&self) -> bool {
        let s = self.state.load(Ordering::Relaxed);
        s & (WRITER | WAITERS) == 0
            && s & READERS < READERS
            && self
                .state
                .compare_exchange(s, s + 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
    }

    pub fn lock_shared(&self) {
        let mut spins = 0;
        loop {
            let s = self.state.load(Ordering::Relaxed);
            if s & (WRITER | WAITERS) == 0 && s & READERS < READERS {
                if self
                    .state
                    .compare_exchange_weak(s, s + 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }
                continue;
            }
            self.pause(s, &mut spins);
        }
    }

    pub fn unlock_shared(&self) {
        let before = self.state.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(
            before & READERS > 0,
            "shared unlock of a lock not held shared"
        );
        // The last reader out lets a waiting writer in.
        if before & READERS == 1 && before & WAITERS != 0 {
            self.wake();
        }
    }

    /// Take it exclusive if nobody holds it or waits for it.
    pub fn try_exclusive(&self) -> bool {
        self.state
            .compare_exchange(0, WRITER, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    pub fn lock_exclusive(&self) {
        if self.try_exclusive() {
            return;
        }
        // Counted as waiting from here on: no new reader gets in.
        self.state.fetch_add(WAITER, Ordering::SeqCst);
        let mut spins = 0;
        loop {
            let s = self.state.load(Ordering::Relaxed);
            if s & (WRITER | READERS) == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        s,
                        (s - WAITER) | WRITER,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return;
                }
                continue;
            }
            self.pause(s, &mut spins);
        }
    }

    pub fn unlock_exclusive(&self) {
        let before = self.state.fetch_and(!WRITER, Ordering::SeqCst);
        debug_assert!(before & WRITER != 0, "exclusive unlock of a lock not held");
        self.wake();
    }

    #[cfg(test)]
    fn raw(&self) -> u32 {
        self.state.load(Ordering::SeqCst)
    }
}

/// The sixteen locks, in shared memory.
#[repr(C)]
pub struct DbLocks {
    locks: [RwLock; NUM_DBS],
}

pub fn db_locks_size() -> usize {
    std::mem::size_of::<DbLocks>()
}

impl DbLocks {
    #[cfg(test)]
    pub fn new() -> DbLocks {
        DbLocks {
            locks: std::array::from_fn(|_| RwLock::new()),
        }
    }

    pub fn lock(&self, db: u8) -> &RwLock {
        &self.locks[db as usize % NUM_DBS]
    }
}

/// What a run holds: databases shared, and databases exclusive. A database in
/// both is held exclusive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub shared: Mask,
    pub exclusive: Mask,
}

impl Plan {
    pub const NONE: Plan = Plan {
        shared: 0,
        exclusive: 0,
    };

    pub fn shared(dbs: Mask) -> Plan {
        Plan {
            shared: dbs,
            exclusive: 0,
        }
    }

    pub fn exclusive(dbs: Mask) -> Plan {
        Plan {
            shared: 0,
            exclusive: dbs,
        }
    }

    pub fn union(self, other: Plan) -> Plan {
        let exclusive = self.exclusive | other.exclusive;
        Plan {
            shared: (self.shared | other.shared) & !exclusive,
            exclusive,
        }
    }

    /// The same databases, every one exclusive: a transaction's.
    pub fn all_exclusive(self) -> Plan {
        Plan::exclusive(self.shared | self.exclusive)
    }
}

thread_local! {
    /// What this thread holds now, `None` outside a held run. Read again by
    /// [`release_held`] when the process exits without unwinding.
    static HELD: Cell<Option<Plan>> = const { Cell::new(None) };
}

/// A run's locks. Dropping it releases them; so does [`release_held`], when
/// the process exits with a FATAL error that never unwinds to the drop.
pub struct Held {
    locks: Option<&'static DbLocks>,
}

/// Take `plan`'s locks, waiting for each, in ascending database order. With no
/// shared memory (the extension not preloaded) nothing is taken.
pub fn hold(plan: Plan) -> Held {
    hold_on(crate::db_locks(), plan)
}

pub fn hold_on(locks: Option<&'static DbLocks>, plan: Plan) -> Held {
    debug_assert!(
        HELD.get().is_none(),
        "a run's locks taken while another's are held"
    );
    HELD.set(Some(Plan::NONE));
    if let Some(locks) = locks {
        let exclusive = plan.exclusive;
        let shared = plan.shared & !exclusive;
        for db in 0..NUM_DBS as u8 {
            let b = bit(db);
            if exclusive & b != 0 {
                locks.lock(db).lock_exclusive();
            } else if shared & b != 0 {
                locks.lock(db).lock_shared();
            } else {
                continue;
            }
            HELD.set(HELD.get().map(|h| {
                h.union(Plan {
                    shared: shared & b,
                    exclusive: exclusive & b,
                })
            }));
        }
    }
    Held { locks }
}

/// Hold database `db` shared if that needs no wait: for the dispatcher's
/// background work (the expiry sweep, snapshots, a migration's sweep), which
/// skips a database a script or transaction holds and comes back to it next
/// round. Waiting instead would stop the dispatcher serving every other
/// database for as long as the script runs.
pub fn try_hold_shared(db: u8) -> Option<Held> {
    try_hold_shared_on(crate::db_locks(), db)
}

pub fn try_hold_shared_on(locks: Option<&'static DbLocks>, db: u8) -> Option<Held> {
    debug_assert!(HELD.get().is_none(), "background work inside a held run");
    if let Some(locks) = locks {
        if !locks.lock(db).try_shared() {
            return None;
        }
        HELD.set(Some(Plan::shared(bit(db))));
    } else {
        HELD.set(Some(Plan::NONE));
    }
    Some(Held { locks })
}

impl Drop for Held {
    fn drop(&mut self) {
        release(self.locks);
    }
}

fn release(locks: Option<&'static DbLocks>) {
    let Some(held) = HELD.take() else { return };
    let Some(locks) = locks else { return };
    for db in (0..NUM_DBS as u8).rev() {
        let b = bit(db);
        if held.exclusive & b != 0 {
            locks.lock(db).unlock_exclusive();
        } else if held.shared & b != 0 {
            locks.lock(db).unlock_shared();
        }
    }
}

/// Release whatever this thread still holds. For the worker's exit hook: a
/// FATAL error ends the process without unwinding, and a background worker
/// that exits with status 1 is restarted without the crash recovery that
/// would zero shared memory, so a lock it held would stay held.
pub fn release_held() {
    release(crate::db_locks());
}

/// Run `f`, a change to database `db` made on the side of whatever this
/// thread runs, as eviction is: at once if the run holds `db`, otherwise
/// holding it shared for just `f` if that needs no wait. `None`, without
/// running `f`, when another client holds it exclusive or waits to: a
/// transaction or script on it sees nothing change under it, as in Redis,
/// where eviction happens only between commands.
pub fn beside<R>(db: u8, f: impl FnOnce() -> R) -> Option<R> {
    beside_on(crate::db_locks(), db, f)
}

pub fn beside_on<R>(locks: Option<&'static DbLocks>, db: u8, f: impl FnOnce() -> R) -> Option<R> {
    let held = HELD.get();
    let Some(locks) = locks else {
        return Some(f());
    };
    if held.is_some_and(|h| (h.shared | h.exclusive) & bit(db) != 0) {
        return Some(f());
    }
    if !locks.lock(db).try_shared() {
        return None;
    }
    // Recorded with the run's, so an exit that never unwinds releases it.
    HELD.set(Some(
        held.unwrap_or(Plan::NONE).union(Plan::shared(bit(db))),
    ));
    /// Gives the side hold back, on unwinding too.
    struct Side(&'static RwLock, Option<Plan>);
    impl Drop for Side {
        fn drop(&mut self) {
            HELD.set(self.1);
            self.0.unlock_shared();
        }
    }
    let _side = Side(locks.lock(db), held);
    Some(f())
}

/// Extend the run in progress to `dbs`, each exclusive, without waiting: for
/// a script that reaches a database it was not started with (a command name
/// built at run time). `Err` names the first one another client holds. A
/// no-op outside a held run, as in a backend that runs a script itself.
pub fn reach(dbs: Mask) -> Result<(), u8> {
    reach_on(crate::db_locks(), dbs)
}

pub fn reach_on(locks: Option<&'static DbLocks>, dbs: Mask) -> Result<(), u8> {
    let (Some(held), Some(locks)) = (HELD.get(), locks) else {
        return Ok(());
    };
    let missing = dbs & !held.exclusive;
    if missing == 0 {
        return Ok(());
    }
    for db in 0..NUM_DBS as u8 {
        let b = bit(db);
        if missing & b == 0 {
            continue;
        }
        // Held shared, it cannot be raised: another reader may be inside.
        if held.shared & b != 0 || !locks.lock(db).try_exclusive() {
            return Err(db);
        }
        HELD.set(HELD.get().map(|h| h.union(Plan::exclusive(b))));
    }
    Ok(())
}

/// What this thread holds now: for tests and assertions.
#[cfg(any(test, feature = "pg_test"))]
pub fn held() -> Option<Plan> {
    HELD.get()
}

// ───────────────────────────────── policy ───────────────────────────────────

/// The databases a command touches when run against `db`.
pub fn dbs_of(cmd: &Command, db: u8) -> Mask {
    use crate::composite::Op;
    match cmd {
        Command::FlushAll { .. } => ALL,
        Command::Copy {
            db: Some(target), ..
        } => bit(db) | bit(*target),
        Command::Composite(Op::Move { db: to, .. }) => bit(db) | bit(*to),
        Command::Composite(Op::SwapDb { a, b }) => bit(db) | bit(*a) | bit(*b),
        Command::Script(sc) if crate::scripting::runs_code(sc) => {
            if crate::scripting::reaches_other_dbs(sc) {
                ALL
            } else {
                bit(db)
            }
        }
        // Script management and the routed-table insert hold no key.
        Command::Script(_) | Command::TablePublish { .. } => 0,
        // `SAVE` reads every database it snapshots.
        Command::Admin(a) if a.name == "SAVE" => ALL,
        _ => bit(db),
    }
}

/// Whether a command runs several steps that must not interleave with another
/// client's, and so holds its databases exclusive: a script, and anything that
/// reads and then writes through more than one storage call.
pub fn needs_exclusive(cmd: &Command, db: u8) -> bool {
    match cmd {
        Command::Script(sc) => crate::scripting::runs_code(sc),
        Command::Composite(op) => !op.single_step(),
        // On the tables a stream is read whole, changed, and written back in
        // a second statement. In memory it is one call under the slot's lock.
        Command::Stream(sc) | Command::StreamRead { cmd: sc, .. } => {
            sc.writes() && crate::storage::slot(db).is_none()
        }
        // A type check, then a pop or a move.
        Command::Block(_) | Command::BlockTry { .. } => true,
        // The destination checked, the source dumped, the destination
        // replaced: three calls.
        Command::Copy { .. } | Command::Restore { .. } => true,
        // Sixteen databases, one after another.
        Command::FlushAll { .. } => true,
        // A TRUNCATE of the database's tables (`sql::truncate_dbs`), which no
        // other worker may hold a lock on.
        Command::FlushDb { .. } => crate::storage::slot(db).is_none(),
        _ => false,
    }
}

/// The locks one command run on its own holds.
pub fn plan_one(cmd: &Command, db: u8) -> Plan {
    let dbs = dbs_of(cmd, db);
    if needs_exclusive(cmd, db) {
        Plan::exclusive(dbs)
    } else {
        Plan::shared(dbs)
    }
}

/// The locks a batch holds: what each command needs, together.
pub fn plan_batch(cmds: &[(Command, u8)]) -> Plan {
    cmds.iter()
        .fold(Plan::NONE, |p, (cmd, db)| p.union(plan_one(cmd, *db)))
}

/// The locks `EXEC` holds: every database its queue touches, exclusive, for
/// the whole transaction.
pub fn plan_exec(cmds: &[(Command, u8)]) -> Plan {
    plan_batch(cmds).all_exclusive()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread;

    fn leaked() -> &'static DbLocks {
        Box::leak(Box::new(DbLocks::new()))
    }

    #[test]
    fn shared_holders_coexist_and_exclude_a_writer() {
        let l = RwLock::new();
        assert!(l.try_shared());
        assert!(l.try_shared());
        assert!(!l.try_exclusive());
        l.unlock_shared();
        assert!(!l.try_exclusive());
        l.unlock_shared();
        assert!(l.try_exclusive());
        assert!(!l.try_shared());
        assert!(!l.try_exclusive());
        l.unlock_exclusive();
        assert_eq!(l.raw(), 0);
    }

    #[test]
    fn exclusive_excludes_everyone() {
        let locks = leaked();
        let inside = Arc::new(AtomicUsize::new(0));
        let overlap = Arc::new(AtomicBool::new(false));
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let (inside, overlap) = (inside.clone(), overlap.clone());
                thread::spawn(move || {
                    for _ in 0..500 {
                        let exclusive = i % 2 == 0;
                        let l = locks.lock(3);
                        if exclusive {
                            l.lock_exclusive();
                            if inside.fetch_add(1000, Ordering::SeqCst) != 0 {
                                overlap.store(true, Ordering::SeqCst);
                            }
                            std::hint::spin_loop();
                            inside.fetch_sub(1000, Ordering::SeqCst);
                            l.unlock_exclusive();
                        } else {
                            l.lock_shared();
                            if inside.fetch_add(1, Ordering::SeqCst) >= 1000 {
                                overlap.store(true, Ordering::SeqCst);
                            }
                            inside.fetch_sub(1, Ordering::SeqCst);
                            l.unlock_shared();
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert!(!overlap.load(Ordering::SeqCst));
        assert_eq!(locks.lock(3).raw(), 0);
    }

    #[test]
    fn a_waiting_writer_keeps_new_readers_out() {
        let locks = leaked();
        let l = locks.lock(0);
        l.lock_shared();
        let writer = thread::spawn(move || {
            locks.lock(0).lock_exclusive();
            locks.lock(0).unlock_exclusive();
        });
        // Wait until the writer has registered.
        while l.raw() & WAITERS == 0 {
            thread::yield_now();
        }
        // A reader arriving now waits behind it.
        assert!(!l.try_shared());
        let got_in = Arc::new(AtomicBool::new(false));
        let reader = {
            let got_in = got_in.clone();
            thread::spawn(move || {
                locks.lock(0).lock_shared();
                got_in.store(true, Ordering::SeqCst);
                locks.lock(0).unlock_shared();
            })
        };
        thread::sleep(Duration::from_millis(30));
        assert!(
            !got_in.load(Ordering::SeqCst),
            "a reader overtook a waiting writer"
        );
        l.unlock_shared();
        writer.join().unwrap();
        reader.join().unwrap();
        assert!(got_in.load(Ordering::SeqCst));
        assert_eq!(l.raw(), 0);
    }

    #[test]
    fn a_held_run_is_released_on_drop_and_on_panic() {
        let locks = leaked();
        {
            let _h = hold_on(Some(locks), Plan::exclusive(bit(1) | bit(4)));
            assert_eq!(held(), Some(Plan::exclusive(bit(1) | bit(4))));
            assert!(!locks.lock(1).try_shared());
            assert!(!locks.lock(4).try_shared());
            assert!(locks.lock(2).try_shared());
            locks.lock(2).unlock_shared();
        }
        assert_eq!(held(), None);
        assert_eq!(locks.lock(1).raw(), 0);
        assert_eq!(locks.lock(4).raw(), 0);

        let r = std::panic::catch_unwind(|| {
            let _h = hold_on(
                Some(locks),
                Plan::shared(bit(5)).union(Plan::exclusive(bit(6))),
            );
            panic!("a command failed");
        });
        assert!(r.is_err());
        assert_eq!(held(), None);
        assert_eq!(locks.lock(5).raw(), 0);
        assert_eq!(locks.lock(6).raw(), 0);
    }

    #[test]
    fn release_held_frees_what_a_lost_guard_held() {
        let locks = leaked();
        let h = hold_on(Some(locks), Plan::exclusive(bit(7)));
        // What a FATAL exit leaves: the guard never dropped.
        std::mem::forget(h);
        assert!(!locks.lock(7).try_shared());
        release(Some(locks));
        assert_eq!(locks.lock(7).raw(), 0);
        assert_eq!(held(), None);
    }

    #[test]
    fn many_databases_in_ascending_order_do_not_deadlock() {
        let locks = leaked();
        let count = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8u8)
            .map(|i| {
                let count = count.clone();
                thread::spawn(move || {
                    for n in 0..300u32 {
                        // Overlapping sets, built in different orders: the
                        // order taken is the database order regardless.
                        let a = ((u32::from(i) + n) % 16) as u8;
                        let b = (i * 3 + 5) % 16;
                        let plan = if n % 3 == 0 {
                            Plan::exclusive(bit(a) | bit(b))
                        } else {
                            Plan::shared(bit(b)).union(Plan::exclusive(bit(a)))
                        };
                        let _h = hold_on(Some(locks), plan);
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(count.load(Ordering::Relaxed), 8 * 300);
        for db in 0..16 {
            assert_eq!(locks.lock(db).raw(), 0);
        }
    }

    #[test]
    fn reach_only_tries() {
        let locks = leaked();
        // Outside a held run it does nothing.
        assert_eq!(reach_on(Some(locks), bit(2)), Ok(()));
        assert_eq!(locks.lock(2).raw(), 0);

        let _h = hold_on(Some(locks), Plan::exclusive(bit(5)));
        assert_eq!(reach_on(Some(locks), bit(5)), Ok(()));
        assert_eq!(reach_on(Some(locks), bit(2)), Ok(()));
        assert_eq!(held(), Some(Plan::exclusive(bit(2) | bit(5))));
        // Another holder of database 9: reaching it fails instead of waiting.
        let other = thread::spawn(move || locks.lock(9).try_shared())
            .join()
            .unwrap();
        assert!(other);
        assert_eq!(reach_on(Some(locks), bit(9)), Err(9));
        locks.lock(9).unlock_shared();
        assert_eq!(reach_on(Some(locks), bit(9)), Ok(()));
        drop(_h);
        for db in [2, 5, 9] {
            assert_eq!(locks.lock(db).raw(), 0);
        }
    }

    #[test]
    fn background_work_skips_a_held_database() {
        let locks = leaked();
        assert!(locks.lock(3).try_exclusive());
        assert!(try_hold_shared_on(Some(locks), 3).is_none());
        assert_eq!(held(), None);
        locks.lock(3).unlock_exclusive();
        {
            let _h = try_hold_shared_on(Some(locks), 3).expect("free now");
            assert_eq!(held(), Some(Plan::shared(bit(3))));
            assert!(!locks.lock(3).try_exclusive());
        }
        assert_eq!(locks.lock(3).raw(), 0);
    }

    #[test]
    fn plans_combine() {
        let p = Plan::shared(bit(0) | bit(1)).union(Plan::exclusive(bit(1)));
        assert_eq!(
            p,
            Plan {
                shared: bit(0),
                exclusive: bit(1)
            }
        );
        assert_eq!(p.all_exclusive(), Plan::exclusive(bit(0) | bit(1)));
    }

    fn cmd(argv: &[&str]) -> Command {
        Command::parse(
            argv.iter()
                .map(|a| a.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn what_each_command_holds() {
        let one = |argv: &[&str], db| plan_one(&cmd(argv), db);
        // Single commands: their own database, shared.
        assert_eq!(one(&["GET", "k"], 2), Plan::shared(bit(2)));
        assert_eq!(one(&["INCR", "k"], 0), Plan::shared(bit(0)));
        assert_eq!(
            one(&["LMOVE", "a", "b", "LEFT", "RIGHT"], 0),
            Plan::shared(bit(0))
        );
        assert_eq!(one(&["BITPOS", "k", "1"], 0), Plan::shared(bit(0)));
        assert_eq!(
            one(&["BITFIELD_RO", "k", "GET", "u8", "0"], 0),
            Plan::shared(bit(0))
        );
        // Several steps: exclusive.
        assert_eq!(one(&["LCS", "a", "b"], 1), Plan::exclusive(bit(1)));
        assert_eq!(
            one(&["BITFIELD", "k", "INCRBY", "u8", "0", "1"], 1),
            Plan::exclusive(bit(1))
        );
        assert_eq!(
            one(&["ZUNIONSTORE", "d", "1", "a"], 1),
            Plan::exclusive(bit(1))
        );
        assert_eq!(one(&["PFADD", "h", "a"], 1), Plan::exclusive(bit(1)));
        assert_eq!(one(&["RESTORE", "k", "0", "x"], 1), Plan::exclusive(bit(1)));
        assert_eq!(
            one(&["EVAL", "return redis.call('get', KEYS[1])", "1", "k"], 3),
            Plan::exclusive(bit(3))
        );
        assert_eq!(
            one(
                &["EVAL_RO", "return redis.call('get', KEYS[1])", "1", "k"],
                3
            ),
            Plan::exclusive(bit(3))
        );
        // Several databases: every one involved, exclusive.
        assert_eq!(
            one(&["MOVE", "k", "5"], 2),
            Plan::exclusive(bit(2) | bit(5))
        );
        assert_eq!(
            one(&["COPY", "a", "b", "DB", "4"], 0),
            Plan::exclusive(bit(0) | bit(4))
        );
        assert_eq!(
            one(&["SWAPDB", "1", "3"], 0),
            Plan::exclusive(bit(0) | bit(1) | bit(3))
        );
        assert_eq!(one(&["FLUSHALL"], 0), Plan::exclusive(ALL));
        assert_eq!(
            one(&["EVAL", "redis.call('SELECT', 1) return 1", "0"], 0),
            Plan::exclusive(ALL)
        );
        // An EVALSHA this thread never compiled: every database.
        assert_eq!(
            one(&["EVALSHA", &"a".repeat(40), "0"], 0),
            Plan::exclusive(ALL)
        );
        // Script management touches no key.
        assert_eq!(one(&["SCRIPT", "LOAD", "return 1"], 0), Plan::NONE);
    }

    #[test]
    fn a_transaction_holds_every_database_in_its_queue_exclusive() {
        let queue = vec![
            (cmd(&["SET", "k", "1"]), 0),
            (cmd(&["SELECT", "3"]), 0),
            (cmd(&["GET", "k"]), 3),
        ];
        assert_eq!(plan_exec(&queue), Plan::exclusive(bit(0) | bit(3)));
        // The same commands from different clients, batched: shared.
        assert_eq!(plan_batch(&queue), Plan::shared(bit(0) | bit(3)));
    }
}
