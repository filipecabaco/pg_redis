#![forbid(unsafe_code)]
use crate::commands::{Command, Response};
use crate::resp::*;
use pgrx::bgworkers::*;
use pgrx::pg_sys;
use pgrx::prelude::*;
use socket2::{Domain, Socket, Type};
use std::collections::HashMap;
use std::io::{self, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

type PendingBatch = Option<(Vec<(Command, u8)>, u64, mpsc::SyncSender<Vec<Response>>)>;

const PUBSUB_NOT_LOADED: &str = "pub/sub requires shared_preload_libraries = 'pg_redis'";
const WATCH_NOT_LOADED: &str = "WATCH requires shared_preload_libraries = 'pg_redis'";

/// Channel names and payloads live in fixed-size shared-memory slots. Silently
/// truncating either is worse than refusing: a truncated subscription never
/// matches a publish to the same name, so the client waits forever on a channel
/// it believes it is subscribed to.
fn pubsub_len_error(items: &[Vec<u8>], limit: usize, what: &str) -> Option<String> {
    items
        .iter()
        .find(|i| i.len() > limit)
        .map(|_| format!("{what} exceeds the pub/sub limit of {limit} bytes"))
}

/// Each message names the client it came from (0 for the server itself), so
/// a write's invalidations can skip a NOLOOP client's own.
enum DispatchMsg {
    Cmd(Command, u8, u64, mpsc::SyncSender<Response>),
    Batch(Vec<(Command, u8)>, u64, mpsc::SyncSender<Vec<Response>>),
    FireAndForget(Command, u8),
    /// Ahead of a command on a migrating database: its keys to move, each
    /// with its database, and the databases to drain (`storage::plan`).
    Move(Vec<(u8, Vec<u8>)>, Vec<u8>, mpsc::SyncSender<()>),
}

/// Carry out a `Move`: drains first, then the keys left to move.
fn run_move(keys: &[(u8, Vec<u8>)], drains: &[u8]) {
    for db in drains {
        crate::storage::drain(*db);
    }
    crate::storage::move_keys(keys);
}

thread_local! {
    /// The client a connection thread serves, which `dispatch_one` names in
    /// every command it sends.
    static CONN_CLIENT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

const QUEUE_LIMIT: usize = 10_000;

/// How long a subscribed connection blocks on the socket each time round the
/// loop before going back to waiting for published messages.
const SUBSCRIBE_POLL: Duration = Duration::from_millis(5);
/// How long a subscriber sleeps between socket polls, backing off from the
/// first to the second while it stays silent. Not delivery latency — a publish
/// wakes the sleeper — only how long a command from a quiet subscriber waits.
const SUBSCRIBE_WAIT_MIN: Duration = Duration::from_millis(5);
const SUBSCRIBE_WAIT_MAX: Duration = Duration::from_millis(250);

pub fn worker_main(db_oid_datum: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let db_oid = pgrx::pg_sys::Oid::from_u32(db_oid_datum.value() as u32);
    if db_oid == pgrx::pg_sys::InvalidOid {
        let db_name = crate::DATABASE
            .get()
            .as_deref()
            .and_then(|s| s.to_str().ok())
            .unwrap_or("postgres")
            .to_string();
        BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);
    } else {
        BackgroundWorker::connect_worker_to_spi_by_oid(Some(db_oid), None);
    }

    // The policies set with `redis.set_policy`, before anyone is served.
    BackgroundWorker::transaction(|| Spi::connect_mut(crate::storage::load));

    crate::shm::attach();
    // memory+snapshot databases filled back in before anyone is served.
    crate::storage::restore_snapshots();

    // Load persisted routes into shared memory. Only the first BGW to win the CAS does the load;
    // others skip (routes are already in shared memory).
    if let Some(route_ctl) = crate::route_state() {
        use std::sync::atomic::Ordering;
        let init = &route_ctl.initialised;
        if init
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            BackgroundWorker::transaction(|| {
                Spi::connect(|client| {
                    // Guard against the extension not yet being installed (e.g. during pgrx tests
                    // workers start before CREATE EXTENSION runs and the table doesn't exist yet).
                    let table_exists: bool = client
                        .select(
                            "SELECT 1 FROM pg_catalog.pg_class c \
                             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                             WHERE n.nspname = 'redis' AND c.relname = 'pubsub_routes'",
                            Some(1),
                            &[],
                        )
                        .ok()
                        .map(|t| !t.is_empty())
                        .unwrap_or(false);

                    if !table_exists {
                        return;
                    }

                    match client.select(
                        "SELECT channel, schema, tbl FROM redis.pubsub_routes",
                        None,
                        &[],
                    ) {
                        Ok(rows) => {
                            for row in rows {
                                let ch: Option<String> = row.get(1).unwrap_or(None);
                                let sc: Option<String> = row.get(2).unwrap_or(None);
                                let tb: Option<String> = row.get(3).unwrap_or(None);
                                if let (Some(ch), Some(sc), Some(tb)) = (ch, sc, tb) {
                                    // Fetched here rather than captured: the closure
                                    // must be unwind-safe, and the table is not.
                                    if let Some(routes) = crate::route_state() {
                                        crate::pubsub::route_add(
                                            routes,
                                            ch.as_bytes(),
                                            sc.as_bytes(),
                                            tb.as_bytes(),
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => pgrx::warning!("pg_redis: failed to load routes: {e}"),
                    }
                })
            });
            init.store(2, Ordering::Release);
        }
    }

    // Connection threads report these through CONFIG GET and INFO, and may
    // not read a GUC themselves.
    crate::config::snapshot_gucs();
    let port = crate::PORT.get() as u16;
    let default_db: u8 = crate::DEFAULT_DB.get().clamp(0, 15) as u8;
    let listen_addr = crate::LISTEN_ADDRESS
        .get()
        .as_deref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or("0.0.0.0")
        .to_string();
    let max_conn = crate::MAX_CONNECTIONS.get().max(1) as usize;
    let password = configured_password();
    let batch_size = crate::BATCH_SIZE.get().max(1) as usize;

    let (cmd_tx, cmd_rx) = mpsc::sync_channel::<DispatchMsg>(256);

    std::thread::spawn(move || {
        accept_loop(port, cmd_tx, default_db, listen_addr, max_conn, password)
    });

    let mut last_expiry_scan = Instant::now();

    loop {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        match cmd_rx.recv_timeout(Duration::from_millis(250)) {
            Ok(first) => match first {
                DispatchMsg::Move(keys, drains, tx) => {
                    run_move(&keys, &drains);
                    tx.send(()).ok();
                }
                DispatchMsg::FireAndForget(cmd, db) => {
                    run_dispatch_batch(&[(cmd, db)], &[0], true);
                }
                DispatchMsg::Batch(cmds, origin, resp_tx) => {
                    let origins = vec![origin; cmds.len()];
                    let responses = run_dispatch_batch(&cmds, &origins, false);
                    resp_tx.send(responses).ok();
                }
                DispatchMsg::Cmd(first_cmd, first_db, first_origin, first_resp_tx) => {
                    let mut cmds: Vec<(Command, u8)> = Vec::with_capacity(batch_size);
                    let mut origins: Vec<u64> = Vec::with_capacity(batch_size);
                    let mut txs: Vec<mpsc::SyncSender<Response>> = Vec::with_capacity(batch_size);
                    let mut faf_cmds: Vec<(Command, u8)> = Vec::new();
                    let mut pending_batch: PendingBatch = None;

                    cmds.push((first_cmd, first_db));
                    origins.push(first_origin);
                    txs.push(first_resp_tx);

                    while cmds.len() < batch_size {
                        match cmd_rx.try_recv() {
                            Ok(DispatchMsg::Cmd(c, d, o, tx)) => {
                                cmds.push((c, d));
                                origins.push(o);
                                txs.push(tx);
                            }
                            Ok(DispatchMsg::Batch(batch_cmds, batch_origin, batch_tx)) => {
                                pending_batch = Some((batch_cmds, batch_origin, batch_tx));
                                break;
                            }
                            Ok(DispatchMsg::FireAndForget(c, d)) => {
                                faf_cmds.push((c, d));
                            }
                            // Another connection's; independent of these.
                            Ok(DispatchMsg::Move(keys, drains, tx)) => {
                                run_move(&keys, &drains);
                                tx.send(()).ok();
                            }
                            Err(_) => break,
                        }
                    }

                    let responses = run_dispatch_batch(&cmds, &origins, true);
                    for (tx, resp) in txs.into_iter().zip(responses) {
                        tx.send(resp).ok();
                    }

                    if !faf_cmds.is_empty() {
                        let zeros = vec![0; faf_cmds.len()];
                        run_dispatch_batch(&faf_cmds, &zeros, true);
                    }

                    if let Some((batch_cmds, batch_origin, batch_tx)) = pending_batch {
                        let o = vec![batch_origin; batch_cmds.len()];
                        let responses = run_dispatch_batch(&batch_cmds, &o, false);
                        batch_tx.send(responses).ok();
                    }
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }

        // Redis keeps the tracking table to `tracking-table-max-keys` from
        // its cron, invalidating keys at random to get back under it.
        crate::tracking::limit_used_slots(100);

        // memory+snapshot databases whose interval has passed.
        crate::storage::snapshots_round();

        // Migrating databases move the keys nobody touched, a time-boxed
        // slice per round so commands keep their turn (docs/STORAGE.md).
        if crate::storage::any_migrating() {
            let until = Instant::now() + Duration::from_millis(50);
            for db in 0..crate::commands::NUM_DBS as u8 {
                while crate::storage::migrating(db)
                    && crate::storage::sweep(db, 256)
                    && Instant::now() < until
                {}
            }
        }
        // Redis expires nothing while clients are paused: the keys wait for
        // the pause to end.
        if last_expiry_scan.elapsed() >= Duration::from_secs(1) && !crate::server::paused() {
            last_expiry_scan = Instant::now();
            // redis.kv_* only exist where CREATE EXTENSION has run, and the
            // worker connects to redis.database, which commonly has not. A
            // missing relation raises, which killed the worker on a loop.
            let sql_tables_exist = kv_tables_exist();
            let notify = crate::notify::flags();
            let names = notify & crate::notify::EXPIRED != 0 || crate::server::tracking_active();
            for db in 0u8..16 {
                let (count, keys) = if let Some(slot) = crate::storage::slot(db) {
                    crate::shm::with_db(slot, |m| m.sweep_expired(names)).unwrap_or_default()
                } else if sql_tables_exist {
                    // A client's write can deadlock with the sweep, which
                    // deletes across a database's tables in one statement.
                    // Losing that is no reason to stop the worker: roll back,
                    // and the next tick sweeps the same keys.
                    let keys: Vec<Vec<u8>> = crate::pg::transaction_or_abort(|| {
                        Spi::connect_mut(|client| {
                            client
                                .update(
                                    &crate::commands::sql::sweep_expired(db as usize),
                                    None,
                                    &[],
                                )
                                .ok()
                                .map(|t| {
                                    t.filter_map(|r| r.get::<Vec<u8>>(1).ok().flatten())
                                        .collect()
                                })
                        })
                    })
                    .unwrap_or_default();
                    (keys.len(), keys)
                } else {
                    (0, Vec::new())
                };
                crate::server::count_expired(count as u64);
                crate::tracking::invalidate(&keys, 0);
                for key in keys {
                    crate::notify::emit(notify, crate::notify::EXPIRED, "expired", &key, db);
                }
            }
        }
    }
}

/// Whether this database has the `redis.kv_*` tables the SQL expiry sweep needs.
///
/// Re-checked each tick rather than cached, so a worker that started before
/// `CREATE EXTENSION` begins sweeping once the tables appear, without a restart.
fn kv_tables_exist() -> bool {
    BackgroundWorker::transaction(kv_tables_present)
}

/// The catalog probe behind [`kv_tables_exist`], without the surrounding
/// transaction — callable from an ordinary backend, which is what lets a test
/// exercise it.
pub(crate) fn kv_tables_present() -> bool {
    Spi::get_one::<bool>("SELECT to_regclass('redis.kv_1') IS NOT NULL")
        .ok()
        .flatten()
        .unwrap_or(false)
}

/// The reply for a command PostgreSQL chose as a deadlock victim. Two workers'
/// batches can take the same rows in opposite orders — concurrent `SADD`s of
/// shared members do — and the victim did nothing wrong. `run_dispatch_batch`
/// re-runs it after its batch commits; a client only sees this if every retry
/// lost too.
const LOCK_CONFLICT: &str = "ERR deadlock detected between concurrent writes, retry the command";
const LOCK_CONFLICT_RETRIES: usize = 3;

/// The reply for a PostgreSQL error raised while running a command, and the
/// line to log for it. Overflow gets Redis's wording; anything else is a bug in
/// a statement, so the client is told only that the command failed. Catching
/// the error stops PostgreSQL reporting it, so nothing else writes it down.
fn sql_error_response(caught: &pg_sys::panic::CaughtError) -> (Response, String) {
    let (code, message) = match caught {
        pg_sys::panic::CaughtError::PostgresError(e)
        | pg_sys::panic::CaughtError::ErrorReport(e)
        | pg_sys::panic::CaughtError::RustPanic { ereport: e, .. } => {
            (e.sql_error_code(), e.message().to_string())
        }
    };
    let log = format!("command failed: {message} (SQLSTATE {code:?})");
    match code {
        pg_sys::errcodes::PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE => (
            Response::Error("ERR increment or decrement would overflow".to_string()),
            log,
        ),
        pg_sys::errcodes::PgSqlErrorCode::ERRCODE_T_R_DEADLOCK_DETECTED
        | pg_sys::errcodes::PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE => {
            (Response::Error(LOCK_CONFLICT.to_string()), log)
        }
        _ => (
            Response::Error("ERR internal error executing the command".to_string()),
            log,
        ),
    }
}

/// Run a command in its own subtransaction (`pg::subtransaction`), keeping
/// its writes only if it succeeded, and answering an ERROR it raised as a
/// reply rather than a dead worker.
pub(crate) fn in_subtransaction<F: FnOnce() -> Response>(body: F) -> Response {
    let (response, raised) = crate::pg::subtransaction(
        body,
        |r| !matches!(r, Response::Error(_)),
        sql_error_response,
    );
    if let Some(log) = raised {
        pgrx::warning!("pg_redis: {log}");
    }
    response
}

/// A MULTI's queue: each command as parsed, or the error it parsed to, with
/// the line it came from.
type Queue = Vec<Result<(Command, Vec<Vec<u8>>), String>>;

/// Key-value pairs to write, grouped by database.
type PairsByDb = Vec<(u8, Vec<(Vec<u8>, Vec<u8>)>)>;

/// A batch whose every command is a plain `SET`: one statement per database
/// writes them all, the last value of a key named twice winning, as running
/// them in order would leave it.
fn plain_sets(cmds: &[(Command, u8)]) -> Option<PairsByDb> {
    let mut by_db: PairsByDb = Vec::new();
    for (cmd, db) in cmds {
        let Command::Set {
            key,
            value,
            ex_ms: None,
            nx: false,
            xx: false,
            get: false,
            keepttl: false,
            ..
        } = cmd
        else {
            return None;
        };
        match by_db.iter_mut().find(|(d, _)| d == db) {
            Some((_, pairs)) => pairs.push((key.clone(), value.clone())),
            None => by_db.push((*db, vec![(key.clone(), value.clone())])),
        }
    }
    Some(by_db)
}

/// Run a batch in one transaction with no subtransaction per command — the
/// begin, release and subxid each one costs — and commit it only if every
/// command succeeded. Anything else — an error reply, a raised error, a lost
/// lock — rolls the whole batch back and returns `None`, and the caller runs
/// it again the careful way, each command in its own subtransaction, which
/// answers the failing command and keeps the others' writes.
///
/// A batch of nothing but plain `SET`s is written with one statement per
/// database (`Store::mset`): what a burst of clients setting keys turns into.
fn run_flat<F>(
    cmds: &[(Command, u8)],
    origins: &[u64],
    mut run: F,
) -> Option<Vec<(Response, Vec<Vec<u8>>)>>
where
    F: FnMut(&mut pgrx::spi::SpiClient<'_>, &Command, u8) -> (Response, Vec<Vec<u8>>),
{
    use crate::store::Store;
    crate::pg::transaction_or_abort(|| {
        Spi::connect_mut(|client| {
            if let Some(by_db) = plain_sets(cmds) {
                for (db, pairs) in &by_db {
                    let mut store = crate::store::SqlStore {
                        client: &mut *client,
                        db: *db,
                    };
                    if store.mset(pairs).is_err() {
                        return None;
                    }
                }
                return Some(cmds.iter().map(|_| (Response::Ok, Vec::new())).collect());
            }
            let mut out = Vec::with_capacity(cmds.len());
            for ((cmd, db), origin) in cmds.iter().zip(origins) {
                crate::tracking::set_origin(*origin);
                let r = run(client, cmd, *db);
                if matches!(r.0, Response::Error(_)) {
                    return None;
                }
                out.push(r);
            }
            Some(out)
        })
    })
}

/// `retry_conflicts` re-runs a deadlock victim on its own once the batch has
/// committed. Right for a batch of independent commands from different
/// connections; wrong for an EXEC, whose commands must run in the order queued.
fn run_dispatch_batch(
    cmds: &[(Command, u8)],
    origins: &[u64],
    retry_conflicts: bool,
) -> Vec<Response> {
    // Whether one command is served from shared memory. Decided per command:
    // a batch may mix halves — commands from different connections, or a
    // MULTI that SELECTs across them — and a memory-half command must never
    // fall through to SQL, where it would land in `kv_N`, a table the memory
    // half never reads.
    let on_mem = |cmd: &Command, db: u8| crate::storage::slot(db).is_some() && !cmd.needs_sql();
    // Whether a key is there, on whichever backend serves its database — what
    // keyspace notifications look up around a command.
    let exists = |client: Option<&mut pgrx::spi::SpiClient<'_>>, d: u8, k: &[u8]| {
        let probe = Command::Exists {
            keys: vec![k.to_vec()],
        };
        let r = if on_mem(&probe, d) {
            probe.execute_mem(d)
        } else if let Some(client) = client {
            probe.execute(client, d)
        } else {
            return false;
        };
        matches!(r, Response::Integer(n) if n > 0)
    };
    // With each reply, the keys the command named as written and left as
    // they were: no WATCH is broken on them and no tracking client told.
    // `guarded`: in a subtransaction of its own (`in_subtransaction`), or
    // as a plain statement of a batch that `run_flat` can roll back whole.
    let run_as = |client: &mut pgrx::spi::SpiClient<'_>, cmd: &Command, db: u8, guarded: bool| {
        let before = crate::notify::before(cmd, db, &mut |d, k| exists(Some(&mut *client), d, k));
        let r = if on_mem(cmd, db) {
            cmd.execute_mem(db)
        } else if guarded {
            in_subtransaction(|| cmd.execute(client, db))
        } else {
            crate::pg::statement(|| cmd.execute(client, db))
        };
        crate::notify::after(before, cmd, db, &r, &mut |d, k| {
            exists(Some(&mut *client), d, k)
        });
        (r, crate::notify::take_untouched())
    };
    let run = |client: &mut pgrx::spi::SpiClient<'_>, cmd: &Command, db: u8| {
        run_as(client, cmd, db, true)
    };
    // The flat path's conditions: every command on tables, so a rollback
    // undoes all of it; none with an effect outside the database during the
    // run (`needs_sql` names those: PUBLISH, scripts, blocking commands, the
    // composites); and no keyspace events, which go out as a command runs.
    let flat_ok = cmds.len() > 1
        && crate::notify::flags() == 0
        && cmds
            .iter()
            .all(|(cmd, db)| !on_mem(cmd, *db) && !cmd.needs_sql());
    let flat = if flat_ok {
        run_flat(cmds, origins, |client, cmd, db| {
            run_as(client, cmd, db, false)
        })
    } else {
        None
    };
    let ran: Vec<(Response, Vec<Vec<u8>>)> = if let Some(ran) = flat {
        ran
    } else if cmds.iter().all(|(cmd, db)| on_mem(cmd, *db)) {
        crate::pg::use_top_memory_context();
        cmds.iter()
            .zip(origins)
            .map(|((cmd, db), origin)| {
                crate::tracking::set_origin(*origin);
                let before = crate::notify::before(cmd, *db, &mut |d, k| exists(None, d, k));
                let r = cmd.execute_mem(*db);
                crate::notify::after(before, cmd, *db, &r, &mut |d, k| exists(None, d, k));
                (r, crate::notify::take_untouched())
            })
            .collect()
    } else if cmds.len() == 1 {
        // Still a subtransaction with no sibling to protect: an SPI error
        // unwinds by longjmp past the `Err` arms that look like they handle it
        // and kills the worker. The routed-table INSERT reaches user tables
        // this extension never validated, so it is a reachable path.
        let (cmd, db) = &cmds[0];
        crate::tracking::set_origin(origins[0]);
        vec![BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| run(client, cmd, *db))
        })]
    } else {
        BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                cmds.iter()
                    .zip(origins)
                    .map(|((cmd, db), origin)| {
                        crate::tracking::set_origin(*origin);
                        run(client, cmd, *db)
                    })
                    .collect()
            })
        })
    };
    let (mut responses, mut untouched): (Vec<Response>, Vec<Vec<Vec<u8>>>) =
        ran.into_iter().unzip();
    // Retried after the commit, not inside the batch: the locks the batch's
    // earlier commands took are held until then, so an immediate retry would
    // wait on the same peer and lose the same way.
    if retry_conflicts {
        for (i, (cmd, db)) in cmds.iter().enumerate() {
            let mut attempts = 0;
            while attempts < LOCK_CONFLICT_RETRIES
                && matches!(&responses[i], Response::Error(e) if e == LOCK_CONFLICT)
            {
                attempts += 1;
                crate::tracking::set_origin(origins[i]);
                (responses[i], untouched[i]) = BackgroundWorker::transaction(|| {
                    Spi::connect_mut(|client| run(client, cmd, *db))
                });
            }
        }
    }
    // What each client wrote, for the tracking invalidations sent once the
    // batch is done: grouped by client, so a transaction's BCAST messages
    // carry all its keys, one message per prefix, as Redis sends them.
    let tracking = crate::server::tracking_active();
    let mut written: Vec<(u64, Vec<Vec<u8>>)> = Vec::new();
    let mut flushed = false;
    let mut note = |origin: u64, key: &[u8]| match written.last_mut() {
        Some((o, keys)) if *o == origin => keys.push(key.to_vec()),
        _ => written.push((origin, vec![key.to_vec()])),
    };
    // Publish the writes so a WATCH on any worker sees them. One `write_keys()`
    // pass, not two — it allocates a Vec per command.
    if let Some(watch_ctl) = crate::watch_state() {
        for (i, (((cmd, db), resp), origin)) in cmds.iter().zip(&responses).zip(origins).enumerate()
        {
            if wrote_nothing(cmd, resp) {
                continue;
            }
            match cmd {
                Command::FlushDb => crate::server::bump_flush_epoch(*db),
                Command::FlushAll => {
                    (0..crate::commands::NUM_DBS as u8).for_each(crate::server::bump_flush_epoch)
                }
                _ => {}
            }
            if matches!(cmd, Command::FlushDb | Command::FlushAll)
                && !matches!(resp, Response::Error(_))
            {
                flushed = true;
            }
            match watch_targets(cmd, *db) {
                None => crate::watch::bump_all(watch_ctl),
                Some(keys) => {
                    for (d, key) in keys {
                        if d == *db && untouched[i].contains(&key) {
                            continue;
                        }
                        crate::watch::bump(watch_ctl, d, &key);
                        if tracking && !matches!(resp, Response::Error(_)) {
                            note(*origin, &key);
                        }
                    }
                }
            }
        }
        // What scripts' sub-commands wrote, recorded as they ran.
        for (origin, target) in crate::scripting::take_writes() {
            match target {
                None => crate::watch::bump_all(watch_ctl),
                Some((d, key)) => {
                    crate::watch::bump(watch_ctl, d, &key);
                    if tracking {
                        note(origin, &key);
                    }
                }
            }
        }
    }
    if tracking {
        if flushed {
            crate::tracking::invalidate_all();
        }
        for (origin, keys) in written {
            crate::tracking::invalidate(&keys, origin);
        }
    }
    responses
}

/// A blocking try that found nothing wrote nothing; bumping its keys would
/// abort a WATCH on a list nobody touched.
pub(crate) fn wrote_nothing(cmd: &Command, resp: &Response) -> bool {
    matches!(cmd, Command::Block(_) | Command::BlockTry { .. })
        && matches!(
            resp,
            Response::NullArray | Response::Null | Response::Error(_)
        )
}

/// The keys a command may have written, by database — `None` when it writes
/// keys it cannot name and every WATCH must be invalidated.
pub(crate) fn watch_targets(cmd: &Command, db: u8) -> Option<Vec<(u8, Vec<u8>)>> {
    let owned = |v: Vec<(u8, &[u8])>| v.into_iter().map(|(d, k)| (d, k.to_vec())).collect();
    match cmd {
        Command::Block(op) | Command::BlockTry { op, .. } => Some(owned(op.write_targets(db))),
        // A flush breaks the WATCH on every key that existed, through the
        // flush epoch, not the per-key counters.
        Command::FlushDb | Command::FlushAll => Some(Vec::new()),
        // A composite names the database of each key it may write.
        Command::Composite(op) if op.touches_everything() => None,
        Command::Composite(op) => Some(owned(op.write_targets(db))),
        // A script's writes are recorded by its sub-commands as they run.
        Command::Script(_) => Some(Vec::new()),
        _ => {
            // COPY ... DB n writes its destination in the other database.
            let write_db = match cmd {
                Command::Copy {
                    db: Some(target), ..
                } => *target,
                _ => db,
            };
            Some(
                cmd.write_keys()
                    .into_iter()
                    .map(|k| (write_db, k.to_vec()))
                    .collect(),
            )
        }
    }
}

fn accept_loop(
    port: u16,
    cmd_tx: mpsc::SyncSender<DispatchMsg>,
    default_db: u8,
    listen_addr: String,
    max_conn: usize,
    password: Option<Vec<u8>>,
) {
    let addr: SocketAddr = match format!("{}:{}", listen_addr, port).parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!(
                "pg_redis: invalid listen address '{}:{}': {}",
                listen_addr, port, e
            );
            return;
        }
    };
    // The Redis port bypasses PostgreSQL authentication entirely: whoever reaches
    // it gets read/write access to every redis.* table through the worker's own
    // privileges. Binding it off-host without a password is almost never intended.
    if password.is_none() && !addr.ip().is_loopback() {
        eprintln!(
            "pg_redis: WARNING listening on {} with no redis.password set — \
             any host that can reach this port has full access to redis.* data",
            addr
        );
    }
    let socket = match Socket::new(Domain::IPV4, Type::STREAM, None) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("pg_redis: failed to create socket: {}", e);
            return;
        }
    };
    if let Err(e) = socket.set_reuse_address(true) {
        eprintln!("pg_redis: SO_REUSEADDR failed: {}", e);
    }
    if let Err(e) = socket.set_reuse_port(true) {
        eprintln!("pg_redis: SO_REUSEPORT failed: {}", e);
    }
    if let Err(e) = socket.bind(&addr.into()) {
        eprintln!("pg_redis: failed to bind port {}: {}", port, e);
        return;
    }
    if let Err(e) = socket.listen(128) {
        eprintln!("pg_redis: listen failed: {}", e);
        return;
    }
    let listener: TcpListener = socket.into();
    let conn_count = Arc::new(AtomicUsize::new(0));

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if conn_count.load(Ordering::Relaxed) >= max_conn {
                    if let Ok(mut w) = s.try_clone() {
                        write_error(&mut w, "max clients reached").ok();
                    }
                    continue;
                }
                if let Err(e) = s.set_read_timeout(Some(Duration::from_secs(30))) {
                    eprintln!("pg_redis: set_read_timeout failed: {}", e);
                }
                if let Err(e) = s.set_nodelay(true) {
                    eprintln!("pg_redis: set_nodelay failed: {}", e);
                }
                let tx = cmd_tx.clone();
                let counter = Arc::clone(&conn_count);
                let pw = password.clone();
                counter.fetch_add(1, Ordering::Relaxed);
                std::thread::spawn(move || {
                    conn_loop(s, tx, default_db, pw);
                    counter.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) => {
                eprintln!("pg_redis: accept error: {}", e);
            }
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    // Compare across max(len), reading past-the-end bytes as 0, so a length
    // difference does not short-circuit the loop and leak via timing. Done
    // without padding buffers: `a` is attacker-supplied and may be megabytes,
    // and AUTH must not hand an unauthenticated client an allocation lever.
    let mut equal = ((a.len() ^ b.len()) as u64).ct_eq(&0);
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        equal &= x.ct_eq(&y);
    }
    bool::from(equal)
}

fn configured_password() -> Option<Vec<u8>> {
    crate::PASSWORD.get().as_deref().and_then(|s| {
        let b = s.to_bytes();
        if b.is_empty() { None } else { Some(b.to_vec()) }
    })
}

fn conn_loop(
    stream: TcpStream,
    cmd_tx: mpsc::SyncSender<DispatchMsg>,
    default_db: u8,
    required_password: Option<Vec<u8>>,
) {
    let write_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let laddr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let fd = {
        use std::os::fd::AsRawFd;
        stream.as_raw_fd()
    };
    let client = crate::server::ClientHandle::register(&addr, &laddr, fd);
    // Waits happen between commands, in the poll below; a read only starts
    // once data is there, so no timeout is needed mid-command.
    stream.set_read_timeout(None).ok();
    let mut writer = ConnWriter::new(write_stream);
    let mut parser = RespParser::new(stream);
    let mut reply_mode = crate::admin::ReplyMode::On;
    let mut in_flight: Option<InFlight> = None;
    let mut db = default_db;
    let mut proto: u8 = 2;
    let mut authenticated = required_password.is_none();
    let mut multi = false;
    // Queued commands with the lines they came from, which MONITOR shows
    // when EXEC runs them.
    let mut queue: Queue = Vec::new();
    let mut watched: HashMap<(u8, Vec<u8>), Watched> = HashMap::new();
    // RESP3: a subscribed connection stays in this loop, running any command,
    // with its messages delivered between commands as push frames. RESP2 has
    // a subscribe mode of its own instead (`subscribe_loop`).
    let mut sub_slot: Option<usize> = None;
    // Whether `sub_slot` listens on this client's tracking channel.
    let mut track_sub = false;
    // Whether another client's invalidations are redirected here.
    let mut redirect_target = false;
    CONN_CLIENT.with(|c| c.set(client.id));
    client.set_proto(2);

    'conn: loop {
        if let Some(job) = in_flight.take() {
            // Normally done as the reply was flushed; here for one that
            // never was.
            writer.remember_read();
            let client_cmd = job
                .args
                .first()
                .is_some_and(|a| a.eq_ignore_ascii_case(b"CLIENT"));
            crate::tracking::command_done(&client, client_cmd, multi);
            finish(job, &client);
            client.unblock_answered();
        }
        if sub_slot.is_none() {
            track_sub = false;
        }
        // A RESP3 client hears invalidations on its own channel: its own
        // when it tracks, and those redirected to it.
        adopt_handoff(
            &mut sub_slot,
            &mut track_sub,
            &mut redirect_target,
            &mut writer,
            &client,
            proto,
        );
        if proto == 3 && !track_sub && (redirect_target || tracking_on(&client)) {
            track_sub = listen_for_invalidations(&mut sub_slot, &client);
        }
        if let Some(s) = client.slot() {
            s.track_listening
                .store(u32::from(track_sub), std::sync::atomic::Ordering::Release);
        }
        client.set_qbuf(0);
        // A client that hung up may have left a pipeline in the read buffer.
        // Redis drops it with the client; running it would land writes after
        // the client — and whoever runs next — believes the connection gone.
        if writer.broken || client.killed() {
            break;
        }
        // Between commands, wait in short slices so a CLIENT KILL from any
        // worker, or the `timeout` setting, is noticed while the client idles.
        if let Some(idx) = sub_slot {
            deliver_messages(&mut writer, idx, proto);
        }
        if !parser.has_buffered() {
            let idle_since = std::time::Instant::now();
            loop {
                adopt_handoff(
                    &mut sub_slot,
                    &mut track_sub,
                    &mut redirect_target,
                    &mut writer,
                    &client,
                    proto,
                );
                let ready = match (sub_slot, crate::pubsub_state()) {
                    // Sleep until a message is published or a slice passes,
                    // then look at the socket: whichever comes first is served.
                    (Some(idx), Some((_, slots))) => {
                        let tail = crate::pubsub::current_tail(slots, idx);
                        parser.wait_readable(Duration::ZERO) || {
                            crate::pubsub::wait_for_message(slots, idx, tail, SUBSCRIBE_POLL);
                            deliver_messages(&mut writer, idx, proto);
                            parser.wait_readable(Duration::ZERO)
                        }
                    }
                    _ => parser.wait_readable(Duration::from_millis(100)),
                };
                if ready {
                    break;
                }
                if client.killed() || writer.broken {
                    break 'conn;
                }
                // Redis never times out a subscriber.
                let timeout = crate::config::get_i64("timeout");
                if sub_slot.is_none()
                    && timeout > 0
                    && idle_since.elapsed().as_secs() as i64 >= timeout
                {
                    break 'conn;
                }
            }
        }
        let parts = match parser.read_command() {
            Ok(p) if p.is_empty() => continue,
            Ok(p) => p,
            Err(e) => {
                // A malformed request is answered before the connection
                // closes, so the client learns why.
                if let Some(reason) = crate::resp::protocol_error_reason(&e) {
                    reply(&mut writer, |w| write_error(w, &reason));
                }
                break;
            }
        };

        let cmd_name = parts
            .first()
            .map(|p| String::from_utf8_lossy(p).to_uppercase())
            .unwrap_or_default();

        // What `CLIENT LIST` reports as this client's query buffer while the
        // command runs: the command as it came over the wire.
        client.set_qbuf(
            parts
                .iter()
                .map(|p| p.len() + 5 + p.len().to_string().len())
                .sum::<usize>()
                + 3
                + parts.len().to_string().len(),
        );
        let counted = stat_name(&parts);
        // Queued commands are never paused: only the EXEC that runs them is.
        let queueing = multi && !matches!(cmd_name.as_str(), "EXEC" | "DISCARD" | "RESET" | "QUIT");
        if !queueing {
            wait_while_paused(&client, || {
                pause_holds(&parts, &counted, multi.then_some(&queue), &cmd_tx)
            });
            if crate::storage::any_migrating() {
                migrate_first(
                    &cmd_tx,
                    db,
                    &parts,
                    (multi && cmd_name == "EXEC").then_some(&queue),
                );
            }
        }
        client.touch(&counted, db);
        crate::resp::REPLIED_ERROR.with(|e| e.set(false));
        // CLIENT REPLY: SKIP drops this one reply, OFF every reply until ON —
        // and ON itself answers, which the admin arm unmutes for.
        writer.muted = !matches!(reply_mode, crate::admin::ReplyMode::On);
        if reply_mode == crate::admin::ReplyMode::Skip {
            reply_mode = crate::admin::ReplyMode::On;
        }
        let known = stat_index(&counted);
        in_flight = Some(InFlight {
            started: std::time::Instant::now(),
            stat: known,
            rejected: false,
            args: parts.clone(),
        });
        writer.pending_read = Some(PendingRead {
            slot: client.slot(),
            id: client.id,
            args: parts.clone(),
            multi,
        });

        // A script that has run past `busy-reply-threshold` holds a
        // dispatcher; until it ends, only the commands meant for that moment
        // — SCRIPT KILL, FUNCTION STATS, the transaction verbs — are let
        // through, as Redis lets them.
        if let Some(eval) =
            crate::server::busy_script(|| crate::config::get_i64("busy-reply-threshold"))
            && !crate::scripting::allowed_while_busy(&parts)
        {
            if let Some(job) = in_flight.as_mut() {
                job.rejected = true;
            }
            let msg = if eval {
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."
            } else {
                "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE."
            };
            if multi && cmd_name == "EXEC" {
                // `execCommandAbort`: the transaction is over.
                multi = false;
                queue.clear();
                watched.clear();
                let abort = format!("EXECABORT Transaction discarded because of: {msg}");
                reply(&mut writer, |w| write_error(w, &abort));
                continue;
            }
            if multi {
                // `rejectCommand` flags the transaction, so EXEC aborts.
                queue.push(Err(msg.to_string()));
            }
            reply(&mut writer, |w| write_error(w, msg));
            continue;
        }

        // RESET and QUIT act immediately even inside MULTI, as in Redis.
        if multi && !matches!(cmd_name.as_str(), "RESET" | "QUIT") {
            match cmd_name.as_str() {
                "EXEC" => {
                    multi = false;
                    let has_errors = queue.iter().any(|r| r.is_err());
                    if has_errors {
                        feed_monitors(&parts, db, &client);
                        queue.clear();
                        watched.clear();
                        write_error(
                            &mut writer,
                            "EXECABORT Transaction discarded because of previous errors.",
                        )
                        .ok();
                        flush(&mut writer);
                        continue;
                    }
                    // Re-read the shared counters: any worker may have written
                    // one of these keys since WATCH snapshotted it.
                    let watch_dirty = match crate::watch_state() {
                        Some(ctl) => watched.iter().any(|((w_db, key), w)| {
                            let now = crate::watch::version(ctl, *w_db, key);
                            w.broken(now, crate::server::flush_epoch(*w_db))
                        }),
                        // Without shared memory there is nothing to compare against.
                        // WATCH already refused, so nothing can be watched here.
                        None => false,
                    };
                    // A queued SELECT moves every command after it, and the
                    // connection itself once the transaction has run.
                    let mut exec_db = db;
                    let mut lines: Vec<(Vec<Vec<u8>>, u8)> = Vec::new();
                    let cmds: Vec<(Command, u8)> = queue
                        .drain(..)
                        .filter_map(|r| r.ok())
                        .map(|(cmd, line)| {
                            let at = exec_db;
                            if let Command::Select { db: selected } = &cmd {
                                exec_db = *selected;
                            }
                            lines.push((line, at));
                            (cmd, at)
                        })
                        .collect();
                    watched.clear();
                    if !watch_dirty {
                        for (line, at) in &lines {
                            feed_monitors(line, *at, &client);
                        }
                    }
                    feed_monitors(&parts, db, &client);
                    if watch_dirty {
                        if proto == 3 {
                            write!(&mut writer, "_\r\n").ok();
                        } else {
                            write_null_array(&mut writer).ok();
                        }
                        flush(&mut writer);
                        continue;
                    }
                    let unblocks: Option<Vec<(u8, Vec<u8>)>> = crate::server::waiters_active()
                        .then(|| {
                            cmds.iter()
                                .filter_map(|(c, d)| watch_targets(c, *d))
                                .flatten()
                                .collect()
                        });
                    // A RESP3 transaction may change the connection's own
                    // subscriptions. Those commands run here, in order: the
                    // commands around them go to the dispatcher in segments,
                    // so each sees the subscriptions the ones before it left.
                    if proto == 3 && cmds.iter().any(|(c, _)| subscription_command(c)) {
                        write_array_header(&mut writer, cmds.len()).ok();
                        let mut segment: Vec<(Command, u8)> = Vec::new();
                        let mut segment_lines: Vec<Vec<Vec<u8>>> = Vec::new();
                        let mut ok = true;
                        let run_segment =
                            |segment: &mut Vec<(Command, u8)>,
                             lines: &mut Vec<Vec<Vec<u8>>>,
                             writer: &mut ConnWriter| {
                                if segment.is_empty() {
                                    return true;
                                }
                                let (tx, rx) = mpsc::sync_channel(1);
                                if cmd_tx
                                    .send(DispatchMsg::Batch(
                                        std::mem::take(segment),
                                        client.id,
                                        tx,
                                    ))
                                    .is_err()
                                {
                                    return false;
                                }
                                let Ok(responses) = rx.recv() else {
                                    return false;
                                };
                                for (resp, line) in responses.into_iter().zip(lines.drain(..)) {
                                    write_reply(writer, resp, &line, proto).ok();
                                }
                                true
                            };
                        for ((cmd, d), (line, _)) in cmds.into_iter().zip(lines.iter()) {
                            if !subscription_command(&cmd) {
                                segment.push((cmd, d));
                                segment_lines.push(line.clone());
                                continue;
                            }
                            ok &= run_segment(&mut segment, &mut segment_lines, &mut writer);
                            match &cmd {
                                Command::Subscribe { channels } => subscribe_in_place(
                                    &mut writer,
                                    &mut sub_slot,
                                    channels,
                                    SubKind::Channel,
                                    proto,
                                    &client,
                                ),
                                Command::PSubscribe { patterns } => subscribe_in_place(
                                    &mut writer,
                                    &mut sub_slot,
                                    patterns,
                                    SubKind::Pattern,
                                    proto,
                                    &client,
                                ),
                                Command::SSubscribe { channels } => {
                                    let stored: Vec<Vec<u8>> = channels
                                        .iter()
                                        .map(|c| crate::pubsub::shard_name(c))
                                        .collect();
                                    subscribe_in_place(
                                        &mut writer,
                                        &mut sub_slot,
                                        &stored,
                                        SubKind::Shard,
                                        proto,
                                        &client,
                                    )
                                }
                                _ => unsubscribe_here(
                                    &mut writer,
                                    &cmd,
                                    &mut sub_slot,
                                    proto,
                                    &client,
                                ),
                            }
                        }
                        ok &= run_segment(&mut segment, &mut segment_lines, &mut writer);
                        if !ok {
                            break;
                        }
                        settle(unblocks, true);
                        db = exec_db;
                        flush(&mut writer);
                        continue;
                    }
                    let (resp_tx, resp_rx) = mpsc::sync_channel(1);
                    if cmd_tx
                        .send(DispatchMsg::Batch(cmds, client.id, resp_tx))
                        .is_err()
                    {
                        break;
                    }
                    match resp_rx.recv() {
                        Ok(responses) => {
                            settle(unblocks, true);
                            db = exec_db;
                            write_array_header(&mut writer, responses.len()).ok();
                            for (resp, (line, _)) in responses.into_iter().zip(&lines) {
                                write_reply(&mut writer, resp, line, proto).ok();
                            }
                            flush(&mut writer);
                        }
                        Err(_) => break,
                    }
                }
                "DISCARD" => {
                    multi = false;
                    queue.clear();
                    watched.clear();
                    reply(&mut writer, |w| write_simple_string(w, "OK"));
                }
                "MULTI" => {
                    write_error(&mut writer, "MULTI calls can not be nested").ok();
                    flush(&mut writer);
                }
                "WATCH" => {
                    // Refused, but Redis does not flag the transaction for it:
                    // the EXEC that follows still runs.
                    write_error(&mut writer, "ERR WATCH inside MULTI is not allowed").ok();
                    flush(&mut writer);
                }
                _ => {
                    if queue.len() >= QUEUE_LIMIT {
                        multi = false;
                        queue.clear();
                        watched.clear();
                        write_error(&mut writer, "transaction queue limit exceeded").ok();
                        flush(&mut writer);
                    } else if crate::scripting::flagged(&parts, "no_multi") {
                        // `rejectCommand`: refused, and the transaction flagged.
                        let e = "ERR Command not allowed inside a transaction".to_string();
                        write_error(&mut writer, &e).ok();
                        queue.push(Err(e));
                        flush(&mut writer);
                    } else {
                        match Command::parse(&parts) {
                            Ok(cmd) => {
                                queue.push(Ok((cmd, parts.clone())));
                                reply(&mut writer, |w| write_simple_string(w, "QUEUED"));
                            }
                            Err(e) => {
                                queue.push(Err(e.clone()));
                                write_error(&mut writer, &e).ok();
                                flush(&mut writer);
                            }
                        }
                    }
                }
            }
            continue;
        }

        let cmd = match Command::parse(&parts) {
            Ok(c) => c,
            Err(e) => {
                write_error(&mut writer, &e).ok();
                flush(&mut writer);
                continue;
            }
        };
        feed_monitors(&parts, db, &client);

        match &cmd {
            Command::Hello {
                proto: requested_proto,
                auth: inline_auth,
                ..
            } => {
                if let Some(password) = inline_auth {
                    let ok = match &required_password {
                        None => true,
                        Some(expected) => constant_time_eq(password.as_bytes(), expected),
                    };
                    if ok {
                        authenticated = true;
                    } else {
                        reply(&mut writer, |w| {
                            write_error(
                                w,
                                "WRONGPASS invalid username-password pair or user is disabled.",
                            )
                        });
                        continue;
                    }
                }
                if let Some(v) = requested_proto {
                    proto = *v;
                    client.set_proto(proto);
                    // Invalidations redirected here before the switch now
                    // arrive as pushes.
                    redirect_target = proto == 3 && crate::server::redirected_to(client.id);
                }
                let negotiated = proto;
                if negotiated == 3 {
                    reply(&mut writer, |w| {
                        write_map_header(w, 7)?;
                        write_bulk_string(w, b"server")?;
                        write_bulk_string(w, b"pg_redis")?;
                        write_bulk_string(w, b"version")?;
                        write_bulk_string(w, crate::commands::REDIS_VERSION.as_bytes())?;
                        write_bulk_string(w, b"proto")?;
                        write_integer(w, 3)?;
                        write_bulk_string(w, b"id")?;
                        write_integer(w, client.id as i64)?;
                        write_bulk_string(w, b"mode")?;
                        write_bulk_string(w, b"standalone")?;
                        write_bulk_string(w, b"role")?;
                        write_bulk_string(w, b"master")?;
                        write_bulk_string(w, b"modules")?;
                        write_array_header(w, 0)
                    });
                } else {
                    reply(&mut writer, |w| {
                        write_array_header(w, 14)?;
                        write_bulk_string(w, b"server")?;
                        write_bulk_string(w, b"pg_redis")?;
                        write_bulk_string(w, b"version")?;
                        write_bulk_string(w, crate::commands::REDIS_VERSION.as_bytes())?;
                        write_bulk_string(w, b"proto")?;
                        write_integer(w, 2)?;
                        write_bulk_string(w, b"id")?;
                        write_integer(w, client.id as i64)?;
                        write_bulk_string(w, b"mode")?;
                        write_bulk_string(w, b"standalone")?;
                        write_bulk_string(w, b"role")?;
                        write_bulk_string(w, b"master")?;
                        write_bulk_string(w, b"modules")?;
                        write_array_header(w, 0)
                    });
                }
                continue;
            }
            Command::Reset => {
                if let (Some(idx), Some((ctl, slots))) = (sub_slot.take(), crate::pubsub_state()) {
                    crate::pubsub::slot_free(ctl, slots, idx);
                    client.set_flag(crate::server::flag::PUBSUB, false);
                }
                crate::tracking::disable(&client);
                proto = 2;
                client.set_proto(2);
                authenticated = required_password.is_none();
                db = default_db;
                multi = false;
                queue.clear();
                watched.clear();
                reply(&mut writer, |w| write_simple_string(w, "RESET"));
                continue;
            }
            Command::Quit => {
                writer.quit_reply = true;
                break;
            }
            Command::Ping { msg } => {
                match msg {
                    Some(m) => write_bulk_string(&mut writer, m).ok(),
                    None => write_simple_string(&mut writer, "PONG").ok(),
                };
                flush(&mut writer);
                continue;
            }
            Command::Auth { password } => {
                match &required_password {
                    None => reply(&mut writer, |w| write_simple_string(w, "OK")),
                    Some(expected) => {
                        if constant_time_eq(password.as_bytes(), expected) {
                            authenticated = true;
                            reply(&mut writer, |w| write_simple_string(w, "OK"));
                        } else {
                            reply(&mut writer, |w| {
                                write_error(
                                    w,
                                    "WRONGPASS invalid username-password pair or user is disabled.",
                                )
                            });
                        }
                    }
                }
                continue;
            }
            _ => {}
        }

        if !authenticated {
            reply(&mut writer, |w| {
                write_error(w, "NOAUTH Authentication required.")
            });
            continue;
        }

        match &cmd {
            Command::Echo { msg } => reply(&mut writer, |w| write_bulk_string(w, msg)),
            Command::Select { db: selected_db } => {
                db = *selected_db;
                reply(&mut writer, |w| write_simple_string(w, "OK"));
            }
            Command::Multi => {
                multi = true;
                reply(&mut writer, |w| write_simple_string(w, "OK"));
            }
            Command::Exec => {
                write_error(&mut writer, "EXEC without MULTI").ok();
                flush(&mut writer);
            }
            Command::Discard => {
                write_error(&mut writer, "DISCARD without MULTI").ok();
                flush(&mut writer);
            }
            Command::Watch { keys } => match crate::watch_state() {
                // Refuse rather than accept a WATCH that cannot detect anything:
                // silently returning OK here would let EXEC commit over a
                // conflicting write.
                None => reply(&mut writer, |w| write_error(w, WATCH_NOT_LOADED)),
                Some(ctl) => {
                    // The counters first, then what the keys hold: a write in
                    // between moves a counter, which breaks the WATCH anyway.
                    let marks: Vec<(u64, u64)> = keys
                        .iter()
                        .map(|k| {
                            (
                                crate::watch::version(ctl, db, k),
                                crate::server::flush_epoch(db),
                            )
                        })
                        .collect();
                    // One round trip for every key: whether it exists, and
                    // when it expires.
                    let (resp_tx, resp_rx) = mpsc::sync_channel(1);
                    let probes = keys
                        .iter()
                        .map(|k| (Command::PTtl { key: k.clone() }, db))
                        .collect();
                    let ttls = match cmd_tx.send(DispatchMsg::Batch(probes, client.id, resp_tx)) {
                        Ok(()) => resp_rx.recv().unwrap_or_default(),
                        Err(_) => Vec::new(),
                    };
                    let now = crate::server::now_ms();
                    for (i, key) in keys.iter().enumerate() {
                        let (version, flush_epoch) = marks[i];
                        let (existed, deadline_ms) = match ttls.get(i) {
                            Some(Response::Integer(-2)) => (false, None),
                            Some(Response::Integer(ms)) if *ms >= 0 => (true, Some(now + ms)),
                            _ => (true, None),
                        };
                        watched.insert(
                            (db, key.clone()),
                            Watched {
                                version,
                                flush_epoch,
                                existed,
                                deadline_ms,
                            },
                        );
                    }
                    reply(&mut writer, |w| write_simple_string(w, "OK"));
                }
            },
            Command::Unwatch => {
                watched.clear();
                reply(&mut writer, |w| write_simple_string(w, "OK"));
            }
            Command::Time => {
                write_reply(&mut writer, crate::commands::time_reply(), &parts, proto).ok();
                flush(&mut writer);
            }
            Command::Stream(sc)
                if matches!(sc.name.as_str(), "XREAD" | "XREADGROUP")
                    && sc.args.iter().any(|a| a.eq_ignore_ascii_case(b"BLOCK")) =>
            {
                let sc = sc.clone();
                let read = |resolved: Option<Vec<Vec<u8>>>| {
                    dispatch_one(
                        &cmd_tx,
                        Command::StreamRead {
                            cmd: sc.clone(),
                            resolved,
                        },
                        db,
                    )
                };
                // The deadline counts from now, not from the first wait.
                let deadline = sc
                    .args
                    .windows(2)
                    .find(|w| w[0].eq_ignore_ascii_case(b"BLOCK"))
                    .and_then(|w| crate::blocking::parse_timeout_ms(&w[1]).ok())
                    .unwrap_or(0);
                let resolved = match read(None) {
                    Response::StreamWait(ids) => ids,
                    r => {
                        write_reply(&mut writer, r, &parts, proto).ok();
                        flush(&mut writer);
                        continue;
                    }
                };
                let keys: Vec<Vec<u8>> = sc.keys().into_iter().map(|k| k.to_vec()).collect();
                let group = sc.name == "XREADGROUP";
                let outcome = wait_blocked(
                    &parser,
                    &client,
                    db,
                    &keys,
                    Wait {
                        deadline_ms: deadline,
                        timeout_reply: Response::NullArray,
                        nokey: group,
                    },
                    Vec::new(),
                    || match read(Some(resolved.clone())) {
                        Response::StreamWait(_) => None,
                        // A blocked XREAD is served by a stream and nothing
                        // else: a key that became another type, or went, is
                        // not ready yet. XREADGROUP reruns and answers
                        // whatever that rerun answers, NOGROUP included.
                        Response::Error(e) if !group && e.starts_with("WRONGTYPE") => None,
                        r => Some(r),
                    },
                );
                match outcome {
                    Some(r) => {
                        write_reply(&mut writer, r, &parts, proto).ok();
                        flush(&mut writer);
                    }
                    None => break 'conn,
                }
            }
            Command::Block(op) => {
                let op = op.clone();
                let attempt = |first: bool| {
                    dispatch_one(
                        &cmd_tx,
                        Command::BlockTry {
                            op: op.clone(),
                            first,
                        },
                        db,
                    )
                };
                let written: Vec<(u8, Vec<u8>)> = op
                    .write_targets(db)
                    .into_iter()
                    .map(|(d, k)| (d, k.to_vec()))
                    .collect();
                let first = attempt(true);
                if !matches!(first, Response::NullArray) {
                    // An immediate move is a write like any other.
                    if !matches!(first, Response::Null | Response::Error(_)) {
                        settle(Some(written), true);
                    }
                    write_reply(&mut writer, first, &parts, proto).ok();
                    flush(&mut writer);
                    continue;
                }
                let outcome = wait_blocked(
                    &parser,
                    &client,
                    db,
                    &op.keys,
                    Wait {
                        deadline_ms: op.deadline_ms,
                        timeout_reply: op.timeout_reply(),
                        nokey: false,
                    },
                    written,
                    || {
                        let r = attempt(false);
                        (!matches!(r, Response::NullArray)).then_some(r)
                    },
                );
                match outcome {
                    Some(r) => {
                        write_reply(&mut writer, r, &parts, proto).ok();
                        flush(&mut writer);
                    }
                    None => break 'conn,
                }
            }
            Command::Monitor => {
                if !monitor_loop(&mut writer, &mut parser, &client) {
                    break 'conn;
                }
            }
            // SAVE snapshots the memory+snapshot databases, which needs SPI:
            // the dispatcher runs it.
            Command::Admin(a) if a.name == "SAVE" => {
                let response = dispatch_one(&cmd_tx, Command::Admin(a.clone()), db);
                write_reply(&mut writer, response, &parts, proto).ok();
                flush(&mut writer);
            }
            Command::Admin(a) => {
                let mut conn = crate::admin::Conn {
                    client: &client,
                    db,
                    reply: reply_mode,
                };
                let response = crate::admin::execute(a, &mut conn, &mut |cmd, d| {
                    dispatch_one(&cmd_tx, cmd, d)
                });
                reply_mode = conn.reply;
                // CLIENT REPLY ON answers; OFF and SKIP do not — not even to
                // the command that set them.
                writer.muted = !matches!(reply_mode, crate::admin::ReplyMode::On);
                write_reply(&mut writer, response, &parts, proto).ok();
                flush(&mut writer);
            }
            Command::Publish { channel, message }
                if channel.len() > crate::pubsub::MAX_CHANNEL_LEN
                    || message.len() > crate::pubsub::PUBSUB_MSG_LEN =>
            {
                let err = if channel.len() > crate::pubsub::MAX_CHANNEL_LEN {
                    format!(
                        "channel name exceeds the pub/sub limit of {} bytes",
                        crate::pubsub::MAX_CHANNEL_LEN
                    )
                } else {
                    format!(
                        "message exceeds the pub/sub limit of {} bytes",
                        crate::pubsub::PUBSUB_MSG_LEN
                    )
                };
                reply(&mut writer, |w| write_error(w, &err));
            }
            Command::Publish { channel, message } => match crate::pubsub_state() {
                None => reply(&mut writer, |w| write_error(w, PUBSUB_NOT_LOADED)),
                Some((ctl, slots)) => {
                    let n = crate::pubsub::publish(ctl, slots, channel, message);
                    if let Some(route_ctl) = crate::route_state()
                        && let Some((schema, table)) =
                            crate::pubsub::route_lookup(route_ctl, channel)
                    {
                        let cmd = Command::TablePublish {
                            schema,
                            table,
                            channel: channel.to_vec(),
                            payload: message.to_vec(),
                        };
                        if cmd_tx.try_send(DispatchMsg::FireAndForget(cmd, 0)).is_err() {
                            eprintln!("pg_redis: route publish dropped — dispatcher queue full");
                        }
                    }
                    reply(&mut writer, |w| write_integer(w, n));
                }
            },
            Command::PubSubChannels { pattern, shard } => match crate::pubsub_state() {
                None => reply(&mut writer, |w| write_error(w, PUBSUB_NOT_LOADED)),
                Some((ctl, slots)) => {
                    let channels =
                        crate::pubsub::pubsub_channels(ctl, slots, pattern.as_deref(), *shard);
                    reply(&mut writer, |w| {
                        write_array_header(w, channels.len())?;
                        for ch in &channels {
                            write_bulk_string(w, ch)?;
                        }
                        Ok(())
                    });
                }
            },
            Command::PubSubNumSub { channels, shard } => match crate::pubsub_state() {
                None => reply(&mut writer, |w| write_error(w, PUBSUB_NOT_LOADED)),
                Some((ctl, slots)) => {
                    let stored: Vec<Vec<u8>> = if *shard {
                        channels
                            .iter()
                            .map(|c| crate::pubsub::shard_name(c))
                            .collect()
                    } else {
                        channels.clone()
                    };
                    let counts = crate::pubsub::pubsub_numsub(ctl, slots, &stored);
                    reply(&mut writer, |w| {
                        write_array_header(w, channels.len() * 2)?;
                        for (ch, n) in channels.iter().zip(&counts) {
                            write_bulk_string(w, ch)?;
                            write_integer(w, *n)?;
                        }
                        Ok(())
                    });
                }
            },
            Command::PubSubNumPat => match crate::pubsub_state() {
                None => reply(&mut writer, |w| write_error(w, PUBSUB_NOT_LOADED)),
                Some((ctl, slots)) => {
                    let n = crate::pubsub::pubsub_numpat(ctl, slots);
                    reply(&mut writer, |w| write_integer(w, n));
                }
            },
            Command::PubSubHelp => {
                reply(&mut writer, |w| {
                    write_array_header(w, 4)?;
                    write_bulk_string(
                        w,
                        b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
                    )?;
                    write_bulk_string(w, b"CHANNELS [<pattern>] -- Return the currently active channels matching a pattern (default: all).")?;
                    write_bulk_string(w, b"NUMSUB [<channel> ...] -- Return the number of subscribers for the specified channels.")?;
                    write_bulk_string(
                        w,
                        b"NUMPAT -- Return the number of subscriptions to patterns.",
                    )
                });
            }
            Command::Subscribe { channels } => {
                if let Some(err) =
                    pubsub_len_error(channels, crate::pubsub::MAX_CHANNEL_LEN, "channel name")
                {
                    reply(&mut writer, |w| write_error(w, &err));
                    continue;
                }
                if proto == 3 {
                    subscribe_in_place(
                        &mut writer,
                        &mut sub_slot,
                        channels,
                        SubKind::Channel,
                        proto,
                        &client,
                    );
                    continue;
                }
                client.set_flag(crate::server::flag::PUBSUB, true);
                let cont = enter_subscribe_mode(
                    &mut writer,
                    &mut parser,
                    proto,
                    channels,
                    SubKind::Channel,
                    crate::pubsub::slot_alloc_and_subscribe,
                );
                client.set_flag(crate::server::flag::PUBSUB, false);
                if !cont {
                    break;
                }
            }
            Command::SSubscribe { channels } => {
                let stored: Vec<Vec<u8>> = channels
                    .iter()
                    .map(|c| crate::pubsub::shard_name(c))
                    .collect();
                if let Some(err) =
                    pubsub_len_error(&stored, crate::pubsub::MAX_CHANNEL_LEN, "channel name")
                {
                    reply(&mut writer, |w| write_error(w, &err));
                    continue;
                }
                if proto == 3 {
                    subscribe_in_place(
                        &mut writer,
                        &mut sub_slot,
                        &stored,
                        SubKind::Shard,
                        proto,
                        &client,
                    );
                    continue;
                }
                client.set_flag(crate::server::flag::PUBSUB, true);
                let cont = enter_subscribe_mode(
                    &mut writer,
                    &mut parser,
                    proto,
                    &stored,
                    SubKind::Shard,
                    crate::pubsub::slot_alloc_and_subscribe,
                );
                client.set_flag(crate::server::flag::PUBSUB, false);
                if !cont {
                    break;
                }
            }
            Command::PSubscribe { patterns } => {
                if let Some(err) =
                    pubsub_len_error(patterns, crate::pubsub::MAX_CHANNEL_LEN, "pattern")
                {
                    reply(&mut writer, |w| write_error(w, &err));
                    continue;
                }
                if proto == 3 {
                    subscribe_in_place(
                        &mut writer,
                        &mut sub_slot,
                        patterns,
                        SubKind::Pattern,
                        proto,
                        &client,
                    );
                    continue;
                }
                client.set_flag(crate::server::flag::PUBSUB, true);
                let cont = enter_subscribe_mode(
                    &mut writer,
                    &mut parser,
                    proto,
                    patterns,
                    SubKind::Pattern,
                    crate::pubsub::slot_alloc_and_psubscribe,
                );
                client.set_flag(crate::server::flag::PUBSUB, false);
                if !cont {
                    break;
                }
            }
            Command::Unsubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::SUnsubscribe { .. } => {
                unsubscribe_here(&mut writer, &cmd, &mut sub_slot, proto, &client);
                flush(&mut writer);
            }
            _ => {
                // Only worked out while someone is blocked: then the clients
                // this write unblocks are served before it replies.
                let unblocks = crate::server::waiters_active()
                    .then(|| watch_targets(&cmd, db))
                    .flatten();
                let (resp_tx, resp_rx) = mpsc::sync_channel(1);
                if cmd_tx
                    .send(DispatchMsg::Cmd(cmd, db, client.id, resp_tx))
                    .is_err()
                {
                    break;
                }
                match resp_rx.recv() {
                    Ok(response) => {
                        if !matches!(response, Response::Error(_)) {
                            settle(unblocks, true);
                        }
                        write_reply(&mut writer, response, &parts, proto).ok();
                        flush(&mut writer);
                    }
                    Err(_) => break,
                }
            }
        }
    }
    // A RESP3 subscriber's slot outlives no connection, and neither does one
    // handed to it and not yet taken.
    if let Some((ctl, slots)) = crate::pubsub_state() {
        if let Some(idx) = sub_slot {
            crate::pubsub::slot_free(ctl, slots, idx);
        }
        let handed = client.slot().map_or(crate::server::HANDOFF_NONE, |s| {
            s.track_handoff.swap(
                crate::server::HANDOFF_CLOSED,
                std::sync::atomic::Ordering::AcqRel,
            )
        });
        if handed >= 0 {
            crate::pubsub::slot_free(ctl, slots, handed as usize);
        }
    }
    crate::tracking::disable(&client);
    // QUIT answers only once the client is gone, as Redis frees it in the
    // same step that writes the reply: whoever sees the OK can rely on the
    // client no longer existing — a tracking redirect to it is broken, a
    // CLIENT LIST omits it.
    drop(client);
    if writer.quit_reply {
        write_simple_string(&mut writer, "OK").ok();
        writer.flush().ok();
    }
}

fn tracking_on(client: &crate::server::ClientHandle) -> bool {
    client.slot().is_some_and(|s| {
        s.tracking.load(std::sync::atomic::Ordering::Relaxed) & crate::tracking::ON != 0
    })
}

/// After a command replied: if this client tracks, remember the keys a
/// read-only command named. Inside MULTI the queued command has not run yet.
fn track_read(
    slot: Option<&'static crate::server::ClientSlot>,
    id: u64,
    argv: &[Vec<u8>],
    multi: bool,
) {
    let tracking = slot.is_some_and(|s| {
        s.tracking.load(std::sync::atomic::Ordering::Relaxed) & crate::tracking::ON != 0
    });
    if multi || argv.is_empty() || !tracking {
        return;
    }
    let name = stat_name(argv);
    // A read-only script is not itself a read: only the commands it runs are
    // (`call()` in server.c), and those are remembered as they run.
    if matches!(name.as_str(), "eval_ro" | "evalsha_ro" | "fcall_ro") {
        return;
    }
    let readonly = crate::commands::COMMAND_SPECS
        .get(name.as_str())
        .is_some_and(|s| s.flags.contains(&"readonly") && !s.flags.contains(&"pubsub"));
    if !readonly {
        return;
    }
    if let Ok(keys) = crate::admin::get_keys(argv) {
        crate::tracking::after_read(id, keys.into_iter().map(|(k, _)| k).collect());
    }
}

/// Before a command runs on a migrating database, have the dispatcher move
/// the keys it names there — or drain the database, for a command that has
/// to see all of it — and wait for that. An EXEC does it for every queued
/// command, following any SELECT among them.
fn migrate_first(
    cmd_tx: &mpsc::SyncSender<DispatchMsg>,
    db: u8,
    parts: &[Vec<u8>],
    queue: Option<&Queue>,
) {
    let mut keys = Vec::new();
    let mut drains = Vec::new();
    let mut add = |db: u8, line: &[Vec<u8>]| {
        let (k, d) = crate::storage::plan(db, line);
        keys.extend(k);
        drains.extend(d);
    };
    match queue {
        Some(queue) => {
            let mut d = db;
            for (cmd, line) in queue.iter().flatten() {
                if let Command::Select { db: to } = cmd {
                    d = *to;
                }
                add(d, line);
            }
        }
        None => add(db, parts),
    }
    if keys.is_empty() && drains.is_empty() {
        return;
    }
    drains.sort_unstable();
    drains.dedup();
    let (tx, rx) = mpsc::sync_channel(1);
    if cmd_tx.send(DispatchMsg::Move(keys, drains, tx)).is_ok() {
        rx.recv().ok();
    }
}

/// Take over a pub/sub slot another connection subscribed to this one's
/// tracking channel (`tracking::offer_listener`): as the connection's slot
/// when it has none; otherwise the connection's own slot joins the channel
/// first and the handed one is drained and freed, so an invalidation landing
/// in between may arrive twice but never not at all.
fn adopt_handoff(
    sub_slot: &mut Option<usize>,
    track_sub: &mut bool,
    redirect_target: &mut bool,
    writer: &mut ConnWriter,
    client: &crate::server::ClientHandle,
    proto: u8,
) {
    use std::sync::atomic::Ordering;
    let Some(s) = client.slot() else { return };
    if s.track_handoff.load(Ordering::Acquire) < 0 {
        return;
    }
    let idx = s
        .track_handoff
        .swap(crate::server::HANDOFF_NONE, Ordering::AcqRel);
    if idx < 0 {
        return;
    }
    let idx = idx as usize;
    *redirect_target = true;
    match *sub_slot {
        None => {
            *sub_slot = Some(idx);
            *track_sub = true;
        }
        Some(_) => {
            if !*track_sub {
                *track_sub = listen_for_invalidations(sub_slot, client);
            }
            deliver_messages(writer, idx, proto);
            if let Some((ctl, slots)) = crate::pubsub_state() {
                crate::pubsub::slot_free(ctl, slots, idx);
            }
        }
    }
    s.track_listening
        .store(u32::from(*track_sub), Ordering::Release);
}

/// Subscribe the connection's slot — taking one if it has none — to its own
/// tracking channel. Not a subscription the client sees: it is not counted,
/// and does not put the connection in pub/sub mode.
fn listen_for_invalidations(
    sub_slot: &mut Option<usize>,
    client: &crate::server::ClientHandle,
) -> bool {
    let Some((ctl, slots)) = crate::pubsub_state() else {
        return false;
    };
    let channel = vec![crate::tracking::channel(client.id)];
    match *sub_slot {
        Some(idx) => {
            crate::pubsub::subscribe(ctl, slots, idx, &channel);
            true
        }
        None => match crate::pubsub::slot_alloc_and_subscribe(ctl, slots, &channel) {
            Some((idx, _)) => {
                *sub_slot = Some(idx);
                true
            }
            None => false,
        },
    }
}

/// `UNSUBSCRIBE` and its kin, on the connection that runs them: drop the
/// names from the connection's slot — freeing it once nothing is left — or,
/// outside pub/sub mode, one frame per name counting zero (a single nil frame
/// when no names were given).
fn unsubscribe_here(
    writer: &mut ConnWriter,
    cmd: &Command,
    sub_slot: &mut Option<usize>,
    proto: u8,
    client: &crate::server::ClientHandle,
) {
    let (kind, names) = match cmd {
        Command::PUnsubscribe { names } => (SubKind::Pattern, names),
        Command::SUnsubscribe { names } => (SubKind::Shard, names),
        Command::Unsubscribe { names } => (SubKind::Channel, names),
        _ => return,
    };
    // A pub/sub frame goes out even under CLIENT REPLY OFF.
    let muted = std::mem::replace(&mut writer.muted, false);
    if let (Some(idx), Some((ctl, slots))) = (*sub_slot, crate::pubsub_state()) {
        let stored: Vec<Vec<u8>> = if kind == SubKind::Shard {
            names.iter().map(|c| crate::pubsub::shard_name(c)).collect()
        } else {
            names.clone()
        };
        let mut freed = false;
        let done = handle_unsub(
            writer,
            SubSlot { ctl, slots, idx },
            proto,
            &stored,
            kind,
            |ctl, slots, idx, items| {
                if kind == SubKind::Pattern {
                    crate::pubsub::punsubscribe(ctl, slots, idx, items)
                } else {
                    crate::pubsub::unsubscribe(ctl, slots, idx, items)
                }
            },
            &mut freed,
        );
        if done {
            if !freed {
                crate::pubsub::slot_free(ctl, slots, idx);
            }
            *sub_slot = None;
            client.set_flag(crate::server::flag::PUBSUB, false);
        }
        writer.muted = muted;
        return;
    }
    let verb = kind.unverb();
    let frames: Vec<Option<&[u8]>> = if names.is_empty() {
        vec![None]
    } else {
        names.iter().map(|n| Some(n.as_slice())).collect()
    };
    for name in frames {
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, verb).ok();
        match name {
            Some(n) => write_bulk_string(writer, n).ok(),
            None => write_null_bulk(writer).ok(),
        };
        write_integer(writer, 0).ok();
    }
    writer.muted = muted;
}

/// A pub/sub command that changes the connection's own subscriptions, which
/// only the connection can run.
fn subscription_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Subscribe { .. }
            | Command::PSubscribe { .. }
            | Command::SSubscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::SUnsubscribe { .. }
    )
}

/// Deliver what is waiting in a RESP3 subscriber's ring. A pub/sub frame is a
/// push: it goes out even under CLIENT REPLY OFF, as in Redis.
fn deliver_messages(writer: &mut ConnWriter, idx: usize, proto: u8) {
    let Some((_, slots)) = crate::pubsub_state() else {
        return;
    };
    let muted = std::mem::replace(&mut writer.muted, false);
    let mut any = false;
    while let Some((channel, pattern, payload)) = crate::pubsub::poll_message(slots, idx) {
        write_message_frame(writer, &channel, &pattern, &payload, proto);
        any = true;
    }
    if any {
        writer.flush().ok();
    }
    writer.muted = muted;
}

/// One published message, as the subscriber receives it.
fn write_message_frame(
    writer: &mut ConnWriter,
    channel: &[u8],
    pattern: &[u8],
    payload: &[u8],
    proto: u8,
) {
    if channel.starts_with(crate::tracking::TRACK_PREFIX) {
        match crate::tracking::frame(payload, proto) {
            Some((kind, value)) if proto == 3 => {
                write!(writer, ">2\r\n").ok();
                write_bulk_string(writer, kind.as_bytes()).ok();
                write_value3(writer, &value).ok();
            }
            Some((_, value)) => {
                write_array_header(writer, 3).ok();
                write_bulk_string(writer, b"message").ok();
                write_bulk_string(writer, b"__redis__:invalidate").ok();
                write_value(writer, &value).ok();
            }
            None => {}
        }
    } else if let Some(shard) = channel.strip_prefix(crate::pubsub::SHARD_PREFIX) {
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, b"smessage").ok();
        write_bulk_string(writer, shard).ok();
        write_bulk_string(writer, payload).ok();
    } else if pattern.is_empty() {
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, b"message").ok();
        write_bulk_string(writer, channel).ok();
        write_bulk_string(writer, payload).ok();
    } else {
        write_pubsub_header(writer, 4, proto).ok();
        write_bulk_string(writer, b"pmessage").ok();
        write_bulk_string(writer, pattern).ok();
        write_bulk_string(writer, channel).ok();
        write_bulk_string(writer, payload).ok();
    }
}

/// `SUBSCRIBE` and its kin on a RESP3 connection: the subscriptions join the
/// connection's slot, taken with the first one, and the loop goes on.
fn subscribe_in_place(
    writer: &mut ConnWriter,
    sub_slot: &mut Option<usize>,
    items: &[Vec<u8>],
    kind: SubKind,
    proto: u8,
    client: &crate::server::ClientHandle,
) {
    let Some((ctl, slots)) = crate::pubsub_state() else {
        reply(writer, |w| write_error(w, PUBSUB_NOT_LOADED));
        return;
    };
    let muted = std::mem::replace(&mut writer.muted, false);
    match *sub_slot {
        Some(idx) => confirm_subscriptions(writer, ctl, slots, idx, proto, items, kind),
        None => {
            let alloc = if kind == SubKind::Pattern {
                crate::pubsub::slot_alloc_and_psubscribe(ctl, slots, &items[..1])
            } else {
                crate::pubsub::slot_alloc_and_subscribe(ctl, slots, &items[..1])
            };
            match alloc {
                None => reply(writer, |w| {
                    write_error(w, "max pub/sub subscribers reached")
                }),
                Some((idx, _)) => {
                    *sub_slot = Some(idx);
                    client.set_flag(crate::server::flag::PUBSUB, true);
                    // A client in pub/sub mode is where a REDIRECT can point.
                    let own = vec![crate::tracking::channel(client.id)];
                    crate::pubsub::subscribe(ctl, slots, idx, &own);
                    write_pubsub_header(writer, 3, proto).ok();
                    write_bulk_string(writer, kind.verb()).ok();
                    write_bulk_string(writer, crate::pubsub::shown_name(&items[0])).ok();
                    write_integer(writer, sub_count(ctl, slots, idx, kind) as i64).ok();
                    confirm_subscriptions(writer, ctl, slots, idx, proto, &items[1..], kind);
                }
            }
        }
    }
    writer.muted = muted;
}

fn enter_subscribe_mode(
    writer: &mut ConnWriter,
    parser: &mut crate::resp::RespParser,
    proto: u8,
    items: &[Vec<u8>],
    kind: SubKind,
    alloc_fn: impl Fn(
        &'static crate::pubsub::PubsubCtl,
        &'static crate::pubsub::Slots,
        &[Vec<u8>],
    ) -> Option<(usize, u32)>,
) -> bool {
    let Some((ctl, slots)) = crate::pubsub_state() else {
        reply(writer, |w| write_error(w, PUBSUB_NOT_LOADED));
        return true;
    };
    // Pub/sub frames are pushes: they go out even under CLIENT REPLY OFF.
    writer.muted = false;
    let own_channel = vec![crate::tracking::channel(CONN_CLIENT.with(|c| c.get()))];
    // The slot is taken with the first name; the rest go through the same
    // path as a SUBSCRIBE already in subscribe mode, so duplicates and names
    // past the per-connection limit are answered the way Redis would.
    match alloc_fn(ctl, slots, &items[..1]) {
        None => {
            reply(writer, |w| {
                write_error(w, "max pub/sub subscribers reached")
            });
            true
        }
        Some((idx, _)) => {
            // A client in pub/sub mode is where a REDIRECT can point.
            crate::pubsub::subscribe(ctl, slots, idx, &own_channel);
            write_pubsub_header(writer, 3, proto).ok();
            write_bulk_string(writer, kind.verb()).ok();
            write_bulk_string(writer, crate::pubsub::shown_name(&items[0])).ok();
            write_integer(writer, 1).ok();
            confirm_subscriptions(writer, ctl, slots, idx, proto, &items[1..], kind);
            subscribe_loop(writer, parser, idx, proto)
        }
    }
}

fn subscribe_loop(
    writer: &mut ConnWriter,
    parser: &mut crate::resp::RespParser,
    slot_idx: usize,
    proto: u8,
) -> bool {
    // Capture state once — these pointers are stable for the lifetime of the process.
    let Some((ctl, slots)) = crate::pubsub_state() else {
        return true;
    };
    // Short enough that a command sent by a subscribed client is noticed
    // promptly, long enough that an idle subscriber is not spinning on it.
    // Message delivery does not depend on this: the wait below is woken by the
    // publisher.
    parser.set_read_timeout(Some(SUBSCRIBE_POLL)).ok();
    let mut keep_conn = true;
    let mut slot_freed = false;
    let mut idle_wait = SUBSCRIBE_WAIT_MIN;

    'outer: loop {
        // Snapshot before draining, so a publish that lands between the drain
        // and the wait makes the wait return immediately instead of sleeping.
        let seen_tail = crate::pubsub::current_tail(slots, slot_idx);

        // Drain ring buffer — lock-free, safe from any thread (AtomicU32, no pg_sys)
        loop {
            match crate::pubsub::poll_message(slots, slot_idx) {
                None => break,
                Some((channel, pattern, payload)) => {
                    write_message_frame(writer, &channel, &pattern, &payload, proto);
                    writer.flush().ok();
                }
            }
        }

        let parts = match parser.read_command() {
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                // No client command pending. Sleep until the publisher wakes us
                // rather than immediately polling the socket again.
                crate::pubsub::wait_for_message(slots, slot_idx, seen_tail, idle_wait);
                idle_wait = (idle_wait * 2).min(SUBSCRIBE_WAIT_MAX);
                continue;
            }
            Err(e) => {
                if let Some(reason) = crate::resp::protocol_error_reason(&e) {
                    write_error(writer, &reason).ok();
                    writer.flush().ok();
                }
                keep_conn = false;
                break;
            }
            Ok(p) if p.is_empty() => continue,
            Ok(p) => p,
        };
        // The client is talking; go back to acknowledging quickly.
        idle_wait = SUBSCRIBE_WAIT_MIN;

        let cmd_name = parts
            .first()
            .map(|p| String::from_utf8_lossy(p).to_uppercase())
            .unwrap_or_default();
        let args = if parts.len() > 1 {
            &parts[1..]
        } else {
            &[][..]
        };

        match cmd_name.as_str() {
            "SUBSCRIBE" if !args.is_empty() => {
                confirm_subscriptions(writer, ctl, slots, slot_idx, proto, args, SubKind::Channel);
            }
            "PSUBSCRIBE" if !args.is_empty() => {
                confirm_subscriptions(writer, ctl, slots, slot_idx, proto, args, SubKind::Pattern);
            }
            "SSUBSCRIBE" if !args.is_empty() => {
                let stored: Vec<Vec<u8>> =
                    args.iter().map(|c| crate::pubsub::shard_name(c)).collect();
                confirm_subscriptions(writer, ctl, slots, slot_idx, proto, &stored, SubKind::Shard);
            }
            "SUNSUBSCRIBE" => {
                let stored: Vec<Vec<u8>> =
                    args.iter().map(|c| crate::pubsub::shard_name(c)).collect();
                if handle_unsub(
                    writer,
                    SubSlot {
                        ctl,
                        slots,
                        idx: slot_idx,
                    },
                    proto,
                    &stored,
                    SubKind::Shard,
                    crate::pubsub::unsubscribe,
                    &mut slot_freed,
                ) {
                    break 'outer;
                }
            }
            "UNSUBSCRIBE" => {
                if handle_unsub(
                    writer,
                    SubSlot {
                        ctl,
                        slots,
                        idx: slot_idx,
                    },
                    proto,
                    args,
                    SubKind::Channel,
                    crate::pubsub::unsubscribe,
                    &mut slot_freed,
                ) {
                    break 'outer;
                }
            }
            "PUNSUBSCRIBE" => {
                if handle_unsub(
                    writer,
                    SubSlot {
                        ctl,
                        slots,
                        idx: slot_idx,
                    },
                    proto,
                    args,
                    SubKind::Pattern,
                    crate::pubsub::punsubscribe,
                    &mut slot_freed,
                ) {
                    break 'outer;
                }
            }
            "PING" => {
                let msg = args.first().map(|v| v.as_slice()).unwrap_or(b"");
                write_pubsub_header(writer, 2, proto).ok();
                write_bulk_string(writer, b"pong").ok();
                write_bulk_string(writer, msg).ok();
                writer.flush().ok();
            }
            "RESET" => {
                crate::pubsub::slot_free(ctl, slots, slot_idx);
                slot_freed = true;
                write_simple_string(writer, "RESET").ok();
                writer.flush().ok();
                break 'outer;
            }
            "QUIT" => {
                crate::pubsub::slot_free(ctl, slots, slot_idx);
                slot_freed = true;
                writer.quit_reply = true;
                keep_conn = false;
                break 'outer;
            }
            _ => {
                write_error(writer, "Command not allowed in subscribe mode").ok();
                writer.flush().ok();
            }
        }
    }

    if !slot_freed {
        crate::pubsub::slot_free(ctl, slots, slot_idx);
    }
    parser.set_read_timeout(Some(Duration::from_secs(30))).ok();
    keep_conn
}

/// The three kinds of subscription a connection holds.
#[derive(Clone, Copy, PartialEq)]
enum SubKind {
    Channel,
    Pattern,
    /// `SSUBSCRIBE`: a channel in the sharded namespace.
    Shard,
}

impl SubKind {
    fn verb(self) -> &'static [u8] {
        match self {
            SubKind::Channel => b"subscribe",
            SubKind::Pattern => b"psubscribe",
            SubKind::Shard => b"ssubscribe",
        }
    }

    fn unverb(self) -> &'static [u8] {
        match self {
            SubKind::Channel => b"unsubscribe",
            SubKind::Pattern => b"punsubscribe",
            SubKind::Shard => b"sunsubscribe",
        }
    }
}

/// What a slot holds of one kind, as the registry stores the names.
fn names_of(
    ctl: &'static crate::pubsub::PubsubCtl,
    slots: &'static crate::pubsub::Slots,
    slot_idx: usize,
    kind: SubKind,
) -> Vec<Vec<u8>> {
    match kind {
        SubKind::Pattern => crate::pubsub::pattern_names(ctl, slots, slot_idx),
        SubKind::Channel => crate::pubsub::channel_names(ctl, slots, slot_idx)
            .into_iter()
            .filter(|n| !crate::pubsub::is_internal(n))
            .collect(),
        SubKind::Shard => crate::pubsub::channel_names(ctl, slots, slot_idx)
            .into_iter()
            .filter(|n| n.starts_with(crate::pubsub::SHARD_PREFIX))
            .collect(),
    }
}

/// The count a confirmation reports: Redis counts sharded subscriptions apart
/// from channels and patterns, which it counts together.
fn sub_count(
    ctl: &'static crate::pubsub::PubsubCtl,
    slots: &'static crate::pubsub::Slots,
    slot_idx: usize,
    kind: SubKind,
) -> usize {
    match kind {
        SubKind::Shard => names_of(ctl, slots, slot_idx, SubKind::Shard).len(),
        _ => {
            names_of(ctl, slots, slot_idx, SubKind::Channel).len()
                + names_of(ctl, slots, slot_idx, SubKind::Pattern).len()
        }
    }
}

/// Subscribe one name at a time so every confirmation carries the real count.
/// A name the slot has no room for is refused with an error rather than
/// confirmed: a confirmed subscription that receives nothing is worse than none.
fn confirm_subscriptions(
    writer: &mut ConnWriter,
    ctl: &'static crate::pubsub::PubsubCtl,
    slots: &'static crate::pubsub::Slots,
    slot_idx: usize,
    proto: u8,
    names: &[Vec<u8>],
    kind: SubKind,
) {
    for name in names {
        let one = std::slice::from_ref(name);
        let before = names_of(ctl, slots, slot_idx, kind);
        if kind == SubKind::Pattern {
            crate::pubsub::psubscribe(ctl, slots, slot_idx, one);
        } else {
            crate::pubsub::subscribe(ctl, slots, slot_idx, one);
        }
        let now = names_of(ctl, slots, slot_idx, kind);
        if now.len() == before.len() && !before.contains(name) {
            write_error(
                writer,
                &format!(
                    "pg_redis allows at most {} {} per connection",
                    crate::pubsub::MAX_SUBS_PER_SLOT,
                    if kind == SubKind::Pattern {
                        "patterns"
                    } else {
                        "channels"
                    }
                ),
            )
            .ok();
            continue;
        }
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, kind.verb()).ok();
        write_bulk_string(writer, crate::pubsub::shown_name(name)).ok();
        write_integer(writer, sub_count(ctl, slots, slot_idx, kind) as i64).ok();
    }
    writer.flush().ok();
}

/// Returns `true` when the caller should exit subscribe mode — when the
/// connection holds no subscription of any kind.
/// A connection's pub/sub slot: the shared state and its index in it.
#[derive(Clone, Copy)]
struct SubSlot {
    ctl: &'static crate::pubsub::PubsubCtl,
    slots: &'static crate::pubsub::Slots,
    idx: usize,
}

fn handle_unsub(
    writer: &mut ConnWriter,
    slot: SubSlot,
    proto: u8,
    args: &[Vec<u8>],
    kind: SubKind,
    unsub_fn: impl Fn(
        &'static crate::pubsub::PubsubCtl,
        &'static crate::pubsub::Slots,
        usize,
        &[Vec<u8>],
    ) -> (u32, u32),
    slot_freed: &mut bool,
) -> bool {
    let SubSlot {
        ctl,
        slots,
        idx: slot_idx,
    } = slot;
    let total = || {
        sub_count(ctl, slots, slot_idx, SubKind::Channel)
            + sub_count(ctl, slots, slot_idx, SubKind::Shard)
    };
    // A bare unsubscribe drops every name of its own kind, and only those.
    let to_unsub: Vec<Vec<u8>> = if args.is_empty() {
        names_of(ctl, slots, slot_idx, kind)
    } else {
        args.to_vec()
    };
    if to_unsub.is_empty() {
        // Nothing of this kind to drop; the other kinds keep the connection
        // in subscribe mode, and the reply counts what remains of this one.
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, kind.unverb()).ok();
        write_null_bulk(writer).ok();
        write_integer(writer, sub_count(ctl, slots, slot_idx, kind) as i64).ok();
        writer.flush().ok();
        // Do not mark slot_freed — let subscribe_loop's cleanup call slot_free.
        return total() == 0;
    }
    for item in &to_unsub {
        unsub_fn(ctl, slots, slot_idx, std::slice::from_ref(item));
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, kind.unverb()).ok();
        write_bulk_string(writer, crate::pubsub::shown_name(item)).ok();
        write_integer(writer, sub_count(ctl, slots, slot_idx, kind) as i64).ok();
    }
    writer.flush().ok();
    // No subscription left — but the slot still holds the connection's
    // internal tracking channel, so it was not reset on its own: the caller
    // frees it.
    let _ = slot_freed;
    total() == 0
}

fn write_value(w: &mut impl std::io::Write, v: &crate::commands::Value) -> std::io::Result<()> {
    use crate::commands::Value;
    match v {
        Value::Nil => write_null_bulk(w),
        Value::NilArray => write_null_array(w),
        Value::Int(n) => write_integer(w, *n),
        Value::Bulk(b) => write_bulk_string(w, b),
        Value::Simple(s) => write_simple_string(w, s),
        Value::Error(e) => write_error_verbatim(w, e),
        Value::Array(items) => {
            write_array_header(w, items.len())?;
            items.iter().try_for_each(|i| write_value(w, i))
        }
        Value::Map(pairs) => {
            write_array_header(w, pairs.len() * 2)?;
            pairs.iter().try_for_each(|(k, v)| {
                write_value(w, k)?;
                write_value(w, v)
            })
        }
        Value::Double(d) => write_bulk_string(w, crate::commands::format_score(*d).as_bytes()),
        Value::Set(items) => {
            write_array_header(w, items.len())?;
            items.iter().try_for_each(|i| write_value(w, i))
        }
        Value::Bool(b) => write_integer(w, i64::from(*b)),
        Value::Verbatim(text) => write_bulk_string(w, text.as_bytes()),
    }
}

/// A value framed as RESP3: maps, sets, doubles, booleans, verbatim strings
/// and the one null for every kind of nil.
fn write_value3(w: &mut impl std::io::Write, v: &crate::commands::Value) -> std::io::Result<()> {
    use crate::commands::Value;
    match v {
        Value::Nil | Value::NilArray => write!(w, "_\r\n"),
        Value::Int(n) => write_integer(w, *n),
        Value::Bulk(b) => write_bulk_string(w, b),
        Value::Simple(s) => write_simple_string(w, s),
        Value::Error(e) => write_error_verbatim(w, e),
        Value::Array(items) => {
            write_array_header(w, items.len())?;
            items.iter().try_for_each(|i| write_value3(w, i))
        }
        Value::Set(items) => {
            write!(w, "~{}\r\n", items.len())?;
            items.iter().try_for_each(|i| write_value3(w, i))
        }
        Value::Map(pairs) => {
            write_map_header(w, pairs.len())?;
            pairs.iter().try_for_each(|(k, v)| {
                write_value3(w, k)?;
                write_value3(w, v)
            })
        }
        Value::Double(d) => write!(w, ",{}\r\n", crate::commands::format_score(*d)),
        Value::Bool(b) => write!(w, "#{}\r\n", if *b { 't' } else { 'f' }),
        Value::Verbatim(text) => {
            write!(w, "={}\r\ntxt:", text.len() + 4)?;
            w.write_all(text.as_bytes())?;
            write!(w, "\r\n")
        }
    }
}

/// A command's reply in the protocol the connection negotiated. RESP2 keeps
/// the writer that has always served it; RESP3 reframes the reply for the
/// command that produced it.
fn write_reply(
    w: &mut impl std::io::Write,
    response: Response,
    argv: &[Vec<u8>],
    proto: u8,
) -> std::io::Result<()> {
    if proto != 3 {
        return write_response(w, response);
    }
    // An error keeps the RESP2 writer's code vetting and errorstats counting.
    if let Response::Error(e) = &response {
        return write_error(w, e);
    }
    write_value3(w, &crate::resp3::shape(argv, response.into_value()))
}

fn write_response(w: &mut impl std::io::Write, response: Response) -> std::io::Result<()> {
    match response {
        Response::Pong(None) => write_simple_string(w, "PONG"),
        Response::Pong(Some(msg)) => write_bulk_string(w, &msg),
        Response::Null => write_null_bulk(w),
        Response::NullArray => write_null_array(w),
        Response::Ok => write_simple_string(w, "OK"),
        Response::Integer(n) => write_integer(w, n),
        Response::BulkString(data) => write_bulk_string(w, &data),
        Response::SimpleString(s) => write_simple_string(w, &s),
        Response::Array(items) => {
            write_array_header(w, items.len())?;
            for item in items {
                match item {
                    Some(data) => write_bulk_string(w, &data)?,
                    None => write_null_bulk(w)?,
                }
            }
            Ok(())
        }
        Response::IntegerArray(items) => {
            write_array_header(w, items.len())?;
            for n in items {
                write_integer(w, n)?;
            }
            Ok(())
        }
        Response::RankWithScore(rank, score) => {
            write_array_header(w, 2)?;
            write_integer(w, rank)?;
            write_bulk_string(w, &score)
        }
        Response::Value(v) => write_value(w, &v),
        // Consumed by the connection loop; never meant for a client.
        Response::StreamWait(_) => write_null_array(w),
        Response::ScanResult { keys } => {
            write_array_header(w, 2)?;
            write_bulk_string(w, b"0")?;
            write_array_header(w, keys.len())?;
            for item in keys {
                match item {
                    Some(data) => write_bulk_string(w, &data)?,
                    None => write_null_bulk(w)?,
                }
            }
            Ok(())
        }
        Response::Error(msg) => write_error(w, &msg),
    }
}

/// The write half of a connection. Remembers the first failed write, which is
/// how the read loop learns the client is gone.
struct ConnWriter {
    inner: BufWriter<TcpStream>,
    broken: bool,
    /// `CLIENT REPLY OFF|SKIP`: replies are produced and dropped.
    muted: bool,
    /// The command being answered, for a tracking client's read to be
    /// remembered before its reply leaves: once the client has the reply, a
    /// write from anyone else must invalidate what it read. Redis remembers
    /// the keys in `call()`, ahead of the reply.
    pending_read: Option<PendingRead>,
    /// QUIT was asked for: its OK goes out after the connection is torn
    /// down.
    quit_reply: bool,
}

struct PendingRead {
    slot: Option<&'static crate::server::ClientSlot>,
    id: u64,
    args: Vec<Vec<u8>>,
    multi: bool,
}

impl ConnWriter {
    fn new(stream: TcpStream) -> Self {
        Self {
            inner: BufWriter::new(stream),
            broken: false,
            muted: false,
            pending_read: None,
            quit_reply: false,
        }
    }

    /// Remember the pending command's keys, unless it answered with an error.
    fn remember_read(&mut self) {
        if let Some(p) = self.pending_read.take()
            && !crate::resp::REPLIED_ERROR.with(|e| e.get())
        {
            track_read(p.slot, p.id, &p.args, p.multi);
        }
    }
}

impl Write for ConnWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.muted {
            return Ok(buf.len());
        }
        let r = self.inner.write(buf);
        self.broken |= r.is_err();
        r
    }

    fn flush(&mut self) -> io::Result<()> {
        self.remember_read();
        let r = self.inner.flush();
        self.broken |= r.is_err();
        r
    }
}

/// Wait, as a blocked client, for a write to `keys` — or the deadline, a
/// CLIENT UNBLOCK, a CLIENT KILL, or the client hanging up — calling
/// `attempt` whenever a key's write counter moves and no older waiter shares
/// it. `attempt` answers `Some` once it has something. `None` means the
/// connection should close.
/// What a WATCH saw of a key. EXEC aborts when the key's write counter moved,
/// when the database was flushed while the key existed, or when the key
/// expired since — Redis counts an expiry as a touch.
struct Watched {
    version: u64,
    flush_epoch: u64,
    existed: bool,
    deadline_ms: Option<i64>,
}

impl Watched {
    fn broken(&self, version: u64, flush_epoch: u64) -> bool {
        version != self.version
            || (self.existed && flush_epoch != self.flush_epoch)
            || self
                .deadline_ms
                .is_some_and(|d| crate::server::now_ms() >= d)
    }
}

/// How long a write waits for the clients it unblocks to be served.
const SETTLE_LIMIT: Duration = Duration::from_millis(500);

/// The write counters of `(db, key)` pairs, paired with the ids the waiter
/// registry knows the keys by.
fn settle_targets(keys: &[(u8, Vec<u8>)]) -> Vec<(u64, u64)> {
    let Some(ctl) = crate::watch_state() else {
        return Vec::new();
    };
    keys.iter()
        .map(|(d, k)| {
            (
                crate::blocking::key_id(*d, k),
                crate::watch::version(ctl, *d, k),
            )
        })
        .collect()
}

/// After a write: wait until the clients blocked on what it wrote have been
/// served, as Redis serves them before the writer's reply.
fn settle(keys: Option<Vec<(u8, Vec<u8>)>>, wait_for_settling: bool) {
    if let Some(keys) = keys
        && crate::server::waiters_active()
    {
        crate::server::settle_waiters(&settle_targets(&keys), wait_for_settling, SETTLE_LIMIT);
    }
}

/// How a blocked command waits: until when, what it answers when the time
/// runs out, and `nokey` — whether a key that goes away serves it.
struct Wait {
    deadline_ms: i64,
    timeout_reply: Response,
    nokey: bool,
}

fn wait_blocked(
    parser: &RespParser,
    client: &crate::server::ClientHandle,
    db: u8,
    keys: &[Vec<u8>],
    wait: Wait,
    written: Vec<(u8, Vec<u8>)>,
    mut attempt: impl FnMut() -> Option<Response>,
) -> Option<Response> {
    let Wait {
        deadline_ms,
        timeout_reply,
        nokey,
    } = wait;
    let ids: Vec<u64> = keys
        .iter()
        .map(|k| crate::blocking::key_id(db, k))
        .collect();
    let versions = || -> Vec<u64> {
        match crate::watch_state() {
            Some(ctl) => keys
                .iter()
                .map(|k| crate::watch::version(ctl, db, k))
                .collect(),
            None => vec![],
        }
    };
    let mut seen = versions();
    let waiter = crate::server::WaiterHandle::register(&ids, &seen, nokey);
    client.set_flag(crate::server::flag::BLOCKED, true);
    let mut pending = false;
    let mut last_try = std::time::Instant::now();
    let outcome = loop {
        if client.killed() || parser.peer_closed() {
            break None;
        }
        if let Some(as_error) = client.take_unblock() {
            break Some(if as_error {
                Response::Error("UNBLOCKED client unblocked via CLIENT UNBLOCK".to_string())
            } else {
                timeout_reply
            });
        }
        if deadline_ms != 0 && crate::server::now_ms() >= deadline_ms {
            break Some(timeout_reply);
        }
        let now_v = versions();
        if now_v != seen {
            seen = now_v;
            pending = true;
        }
        // A write that bypassed the counters is caught by trying now and then.
        if last_try.elapsed() >= Duration::from_millis(50) {
            pending = true;
        }
        if pending && waiter.my_turn(&seen) {
            pending = false;
            last_try = std::time::Instant::now();
            let tried_at = versions();
            if let Some(r) = attempt() {
                // Served: the clients this one's own write wakes are served
                // before it replies, as Redis chains them.
                waiter.set_settling();
                settle(Some(written), false);
                break Some(r);
            }
            waiter.mark_seen(&tried_at);
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(waiter);
    client.set_flag(crate::server::flag::BLOCKED, false);
    outcome
}

/// The pub/sub channel MONITOR lines travel on. Its leading 0x01 marks it
/// internal: out of `PUBSUB CHANNELS` and out of every pattern subscription.
const MONITOR_CHANNEL: &[u8] = b"\x01pg_redis:monitor";

/// Redis's `sdscatrepr`: an argument quoted, with C escapes for anything not
/// printable.
fn repr(arg: &[u8], out: &mut String) {
    out.push('"');
    for &b in arg {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out.push('"');
}

/// Which arguments MONITOR shows as `(redacted)`: passwords, in the places
/// Redis redacts them.
fn redacted(parts: &[Vec<u8>]) -> Vec<bool> {
    let mut r = vec![false; parts.len()];
    let name = String::from_utf8_lossy(&parts[0]).to_ascii_uppercase();
    match name.as_str() {
        "AUTH" => r.iter_mut().skip(1).for_each(|x| *x = true),
        "HELLO" | "MIGRATE" => {
            for i in 1..parts.len() {
                if parts[i].eq_ignore_ascii_case(b"AUTH") {
                    let n = if name == "HELLO" { 2 } else { 1 };
                    (i + 1..(i + 1 + n).min(parts.len())).for_each(|j| r[j] = true);
                } else if parts[i].eq_ignore_ascii_case(b"AUTH2") {
                    (i + 1..(i + 3).min(parts.len())).for_each(|j| r[j] = true);
                }
            }
        }
        _ => {}
    }
    r
}

/// Send a command line to every MONITOR client, as Redis's
/// `replicationFeedMonitors` does: admin commands and the ones flagged
/// `skip_monitor` are never shown. Costs one atomic load when nobody watches.
fn feed_monitors(parts: &[Vec<u8>], db: u8, client: &crate::server::ClientHandle) {
    feed_monitors_from(parts, db, || client.addr());
}

/// What a script's `redis.call` runs, as MONITOR shows it: `[db lua]`.
pub(crate) fn feed_monitors_from_script(parts: &[Vec<u8>], db: u8) {
    feed_monitors_from(parts, db, || "lua".to_string());
}

fn feed_monitors_from(parts: &[Vec<u8>], db: u8, origin: impl FnOnce() -> String) {
    let Some(ctl) = crate::server::ctl() else {
        return;
    };
    if ctl.monitors.load(std::sync::atomic::Ordering::Relaxed) == 0 || parts.is_empty() {
        return;
    }
    let name = String::from_utf8_lossy(&parts[0]).to_ascii_lowercase();
    // The script commands are flagged skip-monitor, yet Redis feeds them to
    // MONITOR itself, ahead of the commands they run.
    let script = matches!(
        name.as_str(),
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro"
    );
    if !script
        && let Some(spec) = crate::commands::COMMAND_SPECS.get(name.as_str())
        && spec
            .flags
            .iter()
            .any(|f| *f == "admin" || *f == "skip_monitor")
    {
        return;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mut line = format!(
        "+{}.{:06} [{} {}] ",
        now.as_secs(),
        now.subsec_micros(),
        db,
        origin()
    );
    let hide = redacted(parts);
    for (i, a) in parts.iter().enumerate() {
        if i > 0 {
            line.push(' ');
        }
        if hide[i] {
            line.push_str("\"(redacted)\"");
        } else {
            repr(a, &mut line);
        }
    }
    line.push_str("\r\n");
    if let Some((pctl, slots)) = crate::pubsub_state() {
        let msg = &line.as_bytes()[..line.len().min(crate::pubsub::PUBSUB_MSG_LEN)];
        crate::pubsub::publish(pctl, slots, MONITOR_CHANNEL, msg);
    }
}

/// MONITOR mode: every command any client runs, as it runs, until QUIT or
/// RESET. Returns whether the connection stays open.
fn monitor_loop(
    writer: &mut ConnWriter,
    parser: &mut RespParser,
    client: &crate::server::ClientHandle,
) -> bool {
    let (Some(ctl), Some((pctl, slots))) = (crate::server::ctl(), crate::pubsub_state()) else {
        reply(writer, |w| write_error(w, PUBSUB_NOT_LOADED));
        return true;
    };
    let Some((idx, _)) =
        crate::pubsub::slot_alloc_and_subscribe(pctl, slots, &[MONITOR_CHANNEL.to_vec()])
    else {
        reply(writer, |w| {
            write_error(w, "max pub/sub subscribers reached")
        });
        return true;
    };
    ctl.monitors
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    client.set_flag(crate::server::flag::MONITOR, true);
    reply(writer, |w| write_simple_string(w, "OK"));
    let keep = loop {
        while let Some((_, _, payload)) = crate::pubsub::poll_message(slots, idx) {
            writer.write_all(&payload).ok();
        }
        writer.flush().ok();
        if writer.broken || client.killed() {
            break false;
        }
        if parser.has_buffered() || parser.wait_readable(Duration::from_millis(2)) {
            match parser.read_command() {
                Err(_) => break false,
                Ok(p) if p.is_empty() => {}
                Ok(p) => match String::from_utf8_lossy(&p[0]).to_ascii_uppercase().as_str() {
                    "QUIT" => {
                        writer.quit_reply = true;
                        break false;
                    }
                    "RESET" => {
                        reply(writer, |w| write_simple_string(w, "RESET"));
                        break true;
                    }
                    _ => {}
                },
            }
        }
    };
    ctl.monitors
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    client.set_flag(crate::server::flag::MONITOR, false);
    crate::pubsub::slot_free(pctl, slots, idx);
    keep
}

/// Run one data command on the dispatcher and wait for its reply.
fn dispatch_one(cmd_tx: &mpsc::SyncSender<DispatchMsg>, cmd: Command, db: u8) -> Response {
    let (resp_tx, resp_rx) = mpsc::sync_channel(1);
    let origin = CONN_CLIENT.with(|c| c.get());
    if cmd_tx
        .send(DispatchMsg::Cmd(cmd, db, origin, resp_tx))
        .is_err()
    {
        return Response::Error("ERR server is shutting down".to_string());
    }
    resp_rx
        .recv()
        .unwrap_or_else(|_| Response::Error("ERR server is shutting down".to_string()))
}

/// What a command was, for the accounting done once it has replied.
struct InFlight {
    started: std::time::Instant,
    stat: Option<usize>,
    rejected: bool,
    args: Vec<Vec<u8>>,
}

/// Index of a command in the generated table, which is also its
/// `INFO commandstats` slot.
fn stat_index(name: &str) -> Option<usize> {
    static INDEX: std::sync::LazyLock<HashMap<&'static str, usize>> =
        std::sync::LazyLock::new(|| {
            crate::redis_tables::COMMANDS
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name, i))
                .collect()
        });
    INDEX.get(name).copied()
}

/// The name a command line is counted under: `config|get` for a container's
/// subcommand, the command itself otherwise.
fn stat_name(parts: &[Vec<u8>]) -> String {
    let top = String::from_utf8_lossy(&parts[0]).to_ascii_lowercase();
    if let Some(sub) = parts.get(1) {
        let full = format!(
            "{top}|{}",
            String::from_utf8_lossy(sub).to_ascii_lowercase()
        );
        if stat_index(&full).is_some() {
            return full;
        }
    }
    top
}

/// Close out a command: commandstats, latency, and the slow log.
fn finish(job: InFlight, client: &crate::server::ClientHandle) {
    let usec = job.started.elapsed().as_micros() as u64;
    let failed = crate::resp::REPLIED_ERROR.with(|e| e.replace(false));
    if let Some(i) = job.stat {
        crate::server::count_command(i, usec, failed && !job.rejected, job.rejected);
    }
    let threshold = crate::config::get_i64("slowlog-log-slower-than");
    if threshold >= 0 && usec as i64 >= threshold && !job.rejected {
        let max_len = crate::config::get_i64("slowlog-max-len").max(0) as usize;
        crate::server::slowlog_push(
            &job.args,
            usec as i64,
            &client.addr(),
            &client.name(),
            max_len,
        );
    }
}

/// Hold a command while `CLIENT PAUSE` applies to it: every command under
/// PAUSE ALL, the ones that may write under PAUSE WRITE.
/// `CLIENT PAUSE`: hold the command until the pause ends. Under `WRITE`, only
/// what `is_write` says is a write waits; it is only asked once a pause is on.
/// A held client counts as blocked, as Redis counts it.
fn wait_while_paused(client: &crate::server::ClientHandle, is_write: impl FnOnce() -> bool) {
    let Some(ctl) = crate::server::ctl() else {
        return;
    };
    use std::sync::atomic::Ordering::Relaxed;
    let paused = || {
        let until = ctl.pause_until_ms.load(Relaxed);
        until != 0 && crate::server::now_ms() < until
    };
    if !paused() {
        return;
    }
    if ctl.pause_writes_only.load(Relaxed) == 1 && !is_write() {
        return;
    }
    client.set_flag(crate::server::flag::BLOCKED, true);
    while paused() {
        std::thread::sleep(Duration::from_millis(5));
    }
    client.set_flag(crate::server::flag::BLOCKED, false);
}

/// Whether `CLIENT PAUSE WRITE` holds this command: Redis's write and
/// may-replicate flags, a script by what it declares, and an EXEC by what it
/// queued.
fn pause_holds(
    parts: &[Vec<u8>],
    spec_name: &str,
    queue: Option<&Queue>,
    cmd_tx: &mpsc::SyncSender<DispatchMsg>,
) -> bool {
    let name = spec_name.to_ascii_uppercase();
    let flags_of = |sha: Option<String>, function: Option<Vec<u8>>| match dispatch_one(
        cmd_tx,
        Command::Script(crate::scripting::ScriptCmd::Flags { sha, function }),
        0,
    ) {
        Response::Integer(f) => f,
        _ => -1,
    };
    match name.as_str() {
        "EVAL" => parts
            .get(1)
            .is_none_or(|b| crate::scripting::flags_write(crate::scripting::body_flags(b))),
        "EVALSHA" => crate::scripting::flags_write(flags_of(
            parts
                .get(1)
                .map(|s| String::from_utf8_lossy(s).to_ascii_lowercase()),
            None,
        )),
        "FCALL" => crate::scripting::flags_write(flags_of(None, parts.get(1).cloned())),
        "EXEC" => queue.is_some_and(|q| {
            q.iter().flatten().any(|(_, line)| {
                let n = stat_name(line);
                pause_holds(line, &n, None, cmd_tx)
            })
        }),
        _ => crate::commands::COMMAND_SPECS
            .get(spec_name)
            .is_some_and(|s| {
                s.flags
                    .iter()
                    .any(|f| *f == "write" || *f == "may_replicate")
            }),
    }
}

fn reply<W: std::io::Write>(w: &mut W, f: impl FnOnce(&mut W) -> std::io::Result<()>) {
    f(w).ok();
    w.flush().ok();
}

fn flush(w: &mut impl std::io::Write) {
    w.flush().ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    /// Stands in for the SPI dispatcher: answers every command with +OK and
    /// reports which db each single command was routed to.
    fn fake_dispatcher(cmd_rx: mpsc::Receiver<DispatchMsg>) -> mpsc::Receiver<u8> {
        let (db_tx, db_rx) = mpsc::sync_channel(16);
        std::thread::spawn(move || {
            for msg in cmd_rx {
                match msg {
                    DispatchMsg::Cmd(_, db, _, resp_tx) => {
                        db_tx.send(db).ok();
                        resp_tx.send(Response::Ok).ok();
                    }
                    DispatchMsg::Batch(cmds, _, resp_tx) => {
                        resp_tx
                            .send(cmds.iter().map(|_| Response::Ok).collect())
                            .ok();
                    }
                    DispatchMsg::FireAndForget(..) => {}
                    DispatchMsg::Move(_, _, tx) => {
                        tx.send(()).ok();
                    }
                }
            }
        });
        db_rx
    }

    struct Conn {
        client: TcpStream,
        dispatched: mpsc::Receiver<u8>,
    }

    impl Conn {
        fn open(default_db: u8) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let (cmd_tx, cmd_rx) = mpsc::sync_channel::<DispatchMsg>(256);
            let dispatched = fake_dispatcher(cmd_rx);
            std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                conn_loop(stream, cmd_tx, default_db, None);
            });
            let client = TcpStream::connect(addr).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            Conn { client, dispatched }
        }

        fn send(&mut self, parts: &[&str]) -> &mut Self {
            let mut buf = format!("*{}\r\n", parts.len());
            for p in parts {
                buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
            }
            self.client.write_all(buf.as_bytes()).unwrap();
            self
        }

        /// Read one RESP line, stripping the CRLF.
        fn line(&mut self) -> String {
            let mut out = String::new();
            let mut byte = [0u8; 1];
            loop {
                self.client.read_exact(&mut byte).unwrap();
                match byte[0] {
                    b'\n' => return out,
                    b'\r' => {}
                    c => out.push(c as char),
                }
            }
        }

        fn reply_to(&mut self, parts: &[&str]) -> String {
            self.send(parts).line()
        }

        /// The db a dispatched command was routed to, or None if nothing was dispatched.
        fn routed_db(&mut self) -> Option<u8> {
            self.dispatched.recv_timeout(Duration::from_secs(2)).ok()
        }

        fn dispatched_nothing(&mut self) -> bool {
            self.dispatched
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        }
    }

    // ─────────────────────────── db selection ────────────────────────────────

    #[test]
    fn commands_route_to_the_connection_default_db() {
        for default_db in [0u8, 1] {
            let mut c = Conn::open(default_db);
            c.send(&["SET", "k", "v"]);
            assert_eq!(c.routed_db(), Some(default_db));
        }
    }

    #[test]
    fn select_switches_the_db_for_subsequent_commands() {
        let mut c = Conn::open(1);
        assert_eq!(c.reply_to(&["SELECT", "0"]), "+OK");
        c.send(&["SET", "k", "v"]);
        assert_eq!(c.routed_db(), Some(0));

        assert_eq!(c.reply_to(&["SELECT", "1"]), "+OK");
        c.send(&["SET", "k", "v"]);
        assert_eq!(c.routed_db(), Some(1));
    }

    #[test]
    fn selected_db_is_per_connection() {
        let (mut a, mut b) = (Conn::open(1), Conn::open(0));
        a.reply_to(&["SELECT", "2"]);
        b.reply_to(&["SELECT", "3"]);
        a.send(&["SET", "k", "v"]);
        b.send(&["SET", "k", "v"]);
        assert_eq!(a.routed_db(), Some(2), "connection A kept its own db");
        assert_eq!(b.routed_db(), Some(3), "connection B kept its own db");
    }

    // ───────────────────────────── dispatch ──────────────────────────────────

    #[test]
    fn data_commands_reach_the_dispatcher() {
        for cmd in [&["SET", "k", "v"][..], &["GET", "k"], &["DEL", "k"]] {
            let mut c = Conn::open(1);
            c.send(cmd);
            assert!(c.routed_db().is_some(), "{cmd:?} must reach the dispatcher");
        }
    }

    #[test]
    fn connection_local_commands_never_reach_the_dispatcher() {
        for cmd in [&["PING"][..], &["SELECT", "0"], &["MULTI"], &["UNWATCH"]] {
            let mut c = Conn::open(1);
            c.reply_to(cmd);
            assert!(c.dispatched_nothing(), "{cmd:?} must be answered locally");
        }
    }

    #[test]
    fn dispatcher_reply_is_forwarded_to_the_client() {
        let mut c = Conn::open(1);
        assert_eq!(c.reply_to(&["SET", "k", "v"]), "+OK");
    }

    // ─────────────────────────── MULTI / WATCH ───────────────────────────────

    #[test]
    fn simple_commands_get_their_documented_replies() {
        let cases: &[(&[&str], &str)] = &[
            (&["PING"], "+PONG"),
            (&["MULTI"], "+OK"),
            (&["UNWATCH"], "+OK"),
            // Both are allowed before AUTH when no password is configured.
            (&["AUTH", "anything"], "+OK"),
        ];
        for (cmd, expected) in cases {
            let mut c = Conn::open(1);
            assert_eq!(&c.reply_to(cmd), expected, "for {cmd:?}");
        }
    }

    /// WATCH needs the shared-memory counters, which exist only under
    /// `shared_preload_libraries`. Refusing is the safe answer: a +OK would let
    /// EXEC commit over a conflict it could not observe.
    #[test]
    fn watch_refuses_when_shared_memory_is_unavailable() {
        let mut c = Conn::open(1);
        let resp = c.reply_to(&["WATCH", "mykey"]);
        assert!(
            resp.starts_with("-ERR") && resp.contains("shared_preload_libraries"),
            "WATCH should refuse without shared memory, got {resp}"
        );
    }

    #[test]
    fn transaction_commands_out_of_context_return_errors() {
        // (setup, command that should error)
        let cases: &[(&[&[&str]], &[&str])] = &[
            (&[], &["EXEC"]),
            (&[], &["DISCARD"]),
            (&[&["MULTI"]], &["MULTI"]),
            (&[&["MULTI"]], &["WATCH", "k"]),
        ];
        for (setup, cmd) in cases {
            let mut c = Conn::open(1);
            for s in *setup {
                c.reply_to(s);
            }
            let resp = c.reply_to(cmd);
            assert!(resp.starts_with("-ERR"), "{cmd:?} should error, got {resp}");
        }
    }

    #[test]
    fn queued_commands_execute_on_exec() {
        let mut c = Conn::open(1);
        assert_eq!(c.reply_to(&["MULTI"]), "+OK");
        assert_eq!(c.reply_to(&["SET", "k", "v"]), "+QUEUED");
        c.send(&["EXEC"]);
        assert_eq!(c.line(), "*1", "EXEC returns one reply per queued command");
        assert_eq!(c.line(), "+OK");
    }

    #[test]
    fn discard_drops_the_queue_without_executing_it() {
        let mut c = Conn::open(1);
        c.reply_to(&["MULTI"]);
        c.reply_to(&["SET", "k", "v"]);
        assert_eq!(c.reply_to(&["DISCARD"]), "+OK");
        assert!(
            c.dispatched_nothing(),
            "discarded commands must not be executed"
        );
    }

    // ───────────────────────────────  AUTH  ──────────────────────────────────

    #[test]
    fn constant_time_eq_compares_by_value() {
        let cases: &[(&[u8], &[u8], bool)] = &[
            (b"secret", b"secret", true),
            (b"secret", b"wroong", false),
            (b"short", b"longer", false),
            (b"", b"", true),
            (b"", b"x", false),
            // Differs only in the final byte — must not be reported equal.
            (b"secreta", b"secretb", false),
        ];
        for &(a, b, expected) in cases {
            assert_eq!(
                constant_time_eq(a, b),
                expected,
                "{:?} vs {:?}",
                String::from_utf8_lossy(a),
                String::from_utf8_lossy(b)
            );
        }
    }
}
