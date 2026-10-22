//! The PostgreSQL internals the extension calls directly — subtransactions,
//! snapshots, transaction control, a few backend globals — each behind a safe
//! function. pgrx wraps most of the server's API safely; these it does not.
//!
//! Two things make the functions here sound to call from safe code:
//!
//! - **The thread.** Every call through `pg_sys` passes pgrx's FFI guard,
//!   which panics off the backend's main thread rather than letting a second
//!   thread into PostgreSQL. The connection threads never get this far: they
//!   hand commands to the worker's main thread.
//! - **The transaction.** Each function that needs one asserts it, so misuse
//!   is a panic, which pgrx turns into an ERROR, not undefined behaviour.

use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;

/// Whether a transaction is open.
pub fn in_transaction() -> bool {
    // SAFETY: reads backend-local transaction state; no preconditions.
    unsafe { pg_sys::IsTransactionState() }
}

/// Whether the current role is a superuser.
pub fn is_superuser() -> bool {
    // SAFETY: reads the current role's catalog entry from the syscache, which
    // any backend may do; no preconditions.
    unsafe { pg_sys::superuser() }
}

/// Read the configuration files again after a SIGHUP, as a backend does
/// between queries, so `SHOW` and every GUC read see the new values.
pub fn reload_config() {
    assert!(!in_transaction(), "a reload is taken between transactions");
    // SAFETY: called on the main thread (the FFI guard enforces it) and
    // outside a transaction (asserted), where PostgreSQL's own backends call
    // it. An invalid setting in the file is logged and skipped, not raised.
    unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) }
}

/// Whether the library is being loaded from `shared_preload_libraries`, at
/// postmaster start: the one time shared memory can be requested, static
/// background workers registered and restart-only settings defined.
pub fn preloading() -> bool {
    // SAFETY: a plain read of a backend-global flag PostgreSQL sets while it
    // processes shared_preload_libraries; no preconditions.
    unsafe { pg_sys::process_shared_preload_libraries_in_progress }
}

/// The database this backend is connected to.
pub fn database_oid() -> pg_sys::Oid {
    // SAFETY: a plain read of a backend-local global, set once at connection
    // and never written again.
    unsafe { pg_sys::MyDatabaseId }
}

/// Allocate in the backend's top memory context from here on: what a batch
/// served entirely from shared memory does, outside any transaction.
pub fn use_top_memory_context() {
    // SAFETY: points the current context at TopMemoryContext, which lives for
    // the whole backend; a plain write of a backend-local global.
    unsafe { pg_sys::CurrentMemoryContext = pg_sys::TopMemoryContext };
}

/// Run `body` in its own subtransaction, keeping its writes only if `keep`
/// says so: a batch holds commands from unrelated connections, so one
/// client's failure must not roll back another's write. The primitives
/// `SAVEPOINT` reaches through SQL, called directly — three statements parsed,
/// planned and executed per command would cost more than most commands.
/// Mirrors PL/pgSQL's EXCEPTION blocks, catch included: an ERROR raised in
/// `body` becomes what `on_raise` makes of it, with a line to log, rather than
/// a dead worker.
pub fn subtransaction<T>(
    body: impl FnOnce() -> T,
    keep: impl FnOnce(&T) -> bool,
    on_raise: impl Fn(&CaughtError) -> (T, String),
) -> (T, Option<String>) {
    assert!(
        in_transaction(),
        "a subtransaction needs an open transaction"
    );
    // It only reads the caught error and builds a value from it.
    let on_raise = std::panic::AssertUnwindSafe(on_raise);
    // SAFETY: inside a transaction (asserted) on the main thread (pgrx's FFI
    // guard): the state BeginInternalSubTransaction and the snapshot calls
    // require. Every exit below releases or rolls back the subtransaction it
    // began and restores the memory context and resource owner it found.
    unsafe {
        let outer_context = pg_sys::CurrentMemoryContext;
        let outer_owner = pg_sys::CurrentResourceOwner;

        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        // BeginInternalSubTransaction switches to the subtransaction's context;
        // run the command in the caller's so its allocations outlive the release.
        pg_sys::MemoryContextSwitchTo(outer_context);

        // Its own snapshot, the way a new statement gets one: SPI reads reuse
        // whatever is active, so a GET queued after a SET in the same MULTI
        // would see the pre-SET world.
        //
        // Push/pop rather than `UpdateActiveSnapshotCommandId`, which asserts
        // `active_count == 1` and `regd_count == 0` — neither true of the
        // snapshot SPI pushed. Only an assert-enabled build shows it.
        let pushed = pg_sys::ActiveSnapshotSet();
        if pushed {
            pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
        }

        // The log line rides in the return value: the handler must be
        // `UnwindSafe`, and a `&mut String` is not.
        let (value, raised) =
            pg_sys::PgTryBuilder::new(std::panic::AssertUnwindSafe(|| (body(), None)))
                .catch_others(|caught| {
                    // Borrowed whole, so the closure captures the wrapper
                    // rather than the bare function inside it.
                    let wrapped = &on_raise;
                    let (value, log) = (wrapped.0)(&caught);
                    (value, Some(log))
                })
                .execute();

        if raised.is_some() {
            // `execute` flushed the error state already — flush then abort, as
            // PL/pgSQL does. The abort pops the subtransaction's snapshot, so
            // `PopActiveSnapshot` here would pop somebody else's.
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
        } else {
            if pushed {
                pg_sys::PopActiveSnapshot();
            }
            if keep(&value) {
                pg_sys::ReleaseCurrentSubTransaction();
            } else {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
            }
        }
        pg_sys::MemoryContextSwitchTo(outer_context);
        pg_sys::CurrentResourceOwner = outer_owner;

        // Advance the command counter so the *next* command's snapshot sees
        // what this one wrote.
        pg_sys::CommandCounterIncrement();

        (value, raised)
    }
}

/// What `subtransaction` does around a command, without the subtransaction:
/// the command gets a snapshot of its own, so it sees what the commands before
/// it wrote, and the next one sees what it wrote. For a batch that a failure
/// rolls back whole instead (`transaction_or_abort`).
pub fn statement<T>(body: impl FnOnce() -> T) -> T {
    assert!(in_transaction(), "a statement needs an open transaction");
    // SAFETY: inside a transaction (asserted), on the main thread. The push
    // and pop pair up; if `body` raises, the transaction is aborted whole,
    // which releases the snapshot with everything else.
    unsafe {
        let pushed = pg_sys::ActiveSnapshotSet();
        if pushed {
            pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
        }
        let value = body();
        if pushed {
            pg_sys::PopActiveSnapshot();
        }
        pg_sys::CommandCounterIncrement();
        value
    }
}

/// Run `body` in a transaction of its own and commit it if it returns
/// `Some`; roll it back if it returns `None` or raises an ERROR. What
/// `BackgroundWorker::transaction` does, except that it always commits.
pub fn transaction_or_abort<T>(body: impl FnOnce() -> Option<T>) -> Option<T> {
    assert!(!in_transaction(), "a transaction is already open");
    // SAFETY: no transaction is open (asserted), so starting one is allowed;
    // the snapshot pushed here is popped before the commit, or released by
    // the abort.
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }
    let value = pg_sys::PgTryBuilder::new(std::panic::AssertUnwindSafe(body))
        .catch_others(|_| None)
        .execute();
    // SAFETY: the transaction started above is still the current one: `body`
    // ran inside it and any ERROR was caught and flushed.
    unsafe {
        match value {
            Some(_) => {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            }
            // The abort releases the snapshot, the SPI connection and the
            // locks the transaction took.
            None => pg_sys::AbortCurrentTransaction(),
        }
    }
    value
}

static ON_EXIT: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

/// Run `f` as this process exits through `proc_exit`, a FATAL error's exit
/// included: what releases the database locks (`src/dblock.rs`) when nothing
/// unwound to their guards. One callback per process; a second call is
/// ignored.
pub fn before_exit(f: fn()) {
    if ON_EXIT.set(f).is_err() {
        return;
    }
    extern "C-unwind" fn run(_code: std::ffi::c_int, _arg: pg_sys::Datum) {
        if let Some(f) = ON_EXIT.get() {
            f();
        }
    }
    // SAFETY: registers a callback with no argument to free; `run` touches
    // nothing but a Rust static and the atomics `f` reads. PostgreSQL calls it
    // once, on the main thread, while shared memory is still attached.
    unsafe { pg_sys::before_shmem_exit(Some(run), pg_sys::Datum::from(0usize)) };
}

/// Whether this process's commits wait for their WAL to reach disk:
/// `appendfsync always` keeps the server's own `synchronous_commit`, which may
/// also wait for a synchronous standby; otherwise `off`. Set for the session,
/// so every transaction after it has it.
pub fn set_synchronous_commit(wait: bool) {
    assert!(!in_transaction(), "set between transactions");
    let name = c"synchronous_commit";
    // SAFETY: no transaction is open (asserted). The name is a NUL-terminated
    // static, and the reset string PostgreSQL returns stays valid until the
    // next change of the setting, which is the call that reads it.
    unsafe {
        let value = if wait {
            pg_sys::GetConfigOptionResetString(name.as_ptr())
        } else {
            c"off".as_ptr()
        };
        pg_sys::SetConfigOption(
            name.as_ptr(),
            value,
            pg_sys::GucContext::PGC_SUSET,
            pg_sys::GucSource::PGC_S_SESSION,
        );
    }
}

/// Send the table counts this process's commits have gathered — the rows
/// each wrote, updated and deleted — to the statistics PostgreSQL keeps, at
/// most once a second; the rest wait for the next call. A backend does this
/// as it goes idle. A background worker has to, or `pg_stat_user_tables`
/// never hears of its writes and autovacuum, which goes by them, never
/// vacuums or analyzes its tables.
pub fn report_table_stats() {
    assert!(
        !in_transaction(),
        "table stats are reported between transactions"
    );
    // SAFETY: no transaction is open (asserted), as PostgresMain calls it.
    unsafe {
        pg_sys::pgstat_report_stat(false);
    }
}
