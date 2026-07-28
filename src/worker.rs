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

type PendingBatch = Option<(Vec<(Command, u8)>, mpsc::SyncSender<Vec<Response>>)>;

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

enum DispatchMsg {
    Cmd(Command, u8, mpsc::SyncSender<Response>),
    Batch(Vec<(Command, u8)>, mpsc::SyncSender<Vec<Response>>),
    FireAndForget(Command, u8),
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

    let mem_mode = crate::storage_mode() == crate::StorageMode::Memory;
    if mem_mode {
        let ctl = crate::shmem_ctl();
        if !ctl.is_null() {
            unsafe {
                pg_sys::CurrentMemoryContext = pg_sys::TopMemoryContext;
                crate::mem::mem_init_worker(ctl);
            }
        }
    }

    // Load persisted routes into shared memory. Only the first BGW to win the CAS does the load;
    // others skip (routes are already in shared memory).
    if let Some(route_ctl) = crate::route_state() {
        use std::sync::atomic::Ordering;
        let init = unsafe { &(*route_ctl).initialised };
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
                                    unsafe {
                                        crate::pubsub::route_add(
                                            route_ctl,
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
                DispatchMsg::FireAndForget(cmd, db) => {
                    run_dispatch_batch(&[(cmd, db)], mem_mode);
                }
                DispatchMsg::Batch(cmds, resp_tx) => {
                    let responses = run_dispatch_batch(&cmds, mem_mode);
                    resp_tx.send(responses).ok();
                }
                DispatchMsg::Cmd(first_cmd, first_db, first_resp_tx) => {
                    let mut cmds: Vec<(Command, u8)> = Vec::with_capacity(batch_size);
                    let mut txs: Vec<mpsc::SyncSender<Response>> = Vec::with_capacity(batch_size);
                    let mut faf_cmds: Vec<(Command, u8)> = Vec::new();
                    let mut pending_batch: PendingBatch = None;

                    cmds.push((first_cmd, first_db));
                    txs.push(first_resp_tx);

                    while cmds.len() < batch_size {
                        match cmd_rx.try_recv() {
                            Ok(DispatchMsg::Cmd(c, d, tx)) => {
                                cmds.push((c, d));
                                txs.push(tx);
                            }
                            Ok(DispatchMsg::Batch(batch_cmds, batch_tx)) => {
                                pending_batch = Some((batch_cmds, batch_tx));
                                break;
                            }
                            Ok(DispatchMsg::FireAndForget(c, d)) => {
                                faf_cmds.push((c, d));
                            }
                            Err(_) => break,
                        }
                    }

                    let responses = run_dispatch_batch(&cmds, mem_mode);
                    for (tx, resp) in txs.into_iter().zip(responses) {
                        tx.send(resp).ok();
                    }

                    if !faf_cmds.is_empty() {
                        run_dispatch_batch(&faf_cmds, mem_mode);
                    }

                    if let Some((batch_cmds, batch_tx)) = pending_batch {
                        let responses = run_dispatch_batch(&batch_cmds, mem_mode);
                        batch_tx.send(responses).ok();
                    }
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
        }

        if last_expiry_scan.elapsed() >= Duration::from_secs(1) {
            last_expiry_scan = Instant::now();
            // redis.kv_* only exist where CREATE EXTENSION has run, and the
            // worker connects to redis.database, which commonly has not. A
            // missing relation raises, which killed the worker on a loop.
            let sql_tables_exist = kv_tables_exist();
            for db in 0u8..16 {
                if mem_mode && crate::commands::is_ephemeral(db) {
                    unsafe {
                        pg_sys::CurrentMemoryContext = pg_sys::TopMemoryContext;
                        crate::mem::mem_sweep_expired(db as usize);
                    }
                } else if sql_tables_exist {
                    BackgroundWorker::transaction(|| {
                        Spi::connect_mut(|client| {
                            client
                                .update(
                                    &crate::commands::sql::sweep_expired(db as usize),
                                    None,
                                    &[],
                                )
                                .ok();
                        })
                    });
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
        _ => (
            Response::Error("ERR internal error executing the command".to_string()),
            log,
        ),
    }
}

/// Run `body` in its own subtransaction, keeping its writes only if it
/// succeeded: a batch holds commands from unrelated connections, so one
/// client's failure must not roll back another's write. The primitives
/// `SAVEPOINT` reaches through SQL, called directly. Mirrors PL/pgSQL's
/// EXCEPTION dance, catch included — a raise inside `body` becomes a reply
/// rather than a dead worker.
///
/// # Safety
/// Must run on the background worker's main thread, inside a transaction.
pub(crate) unsafe fn in_subtransaction<F: FnOnce() -> Response>(body: F) -> Response {
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
        // `UnwindSafe`, and a `&mut String` is not. It is emitted after the
        // abort below, once the error state has been flushed.
        let (response, raised) =
            pg_sys::PgTryBuilder::new(std::panic::AssertUnwindSafe(|| (body(), None)))
                .catch_others(|caught| {
                    let (response, log) = sql_error_response(&caught);
                    (response, Some(log))
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
            if matches!(response, Response::Error(_)) {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
            } else {
                pg_sys::ReleaseCurrentSubTransaction();
            }
        }
        pg_sys::MemoryContextSwitchTo(outer_context);
        pg_sys::CurrentResourceOwner = outer_owner;

        // Advance the command counter so the *next* command's snapshot sees
        // what this one wrote.
        pg_sys::CommandCounterIncrement();

        if let Some(log) = raised {
            pgrx::warning!("pg_redis: {log}");
        }

        response
    }
}

fn run_dispatch_batch(cmds: &[(Command, u8)], mem_mode: bool) -> Vec<Response> {
    // `FLUSHALL` reaches both halves, `COPY` may, and the routed-table INSERT
    // reaches user tables — none can be served from shared memory alone.
    let all_mem = mem_mode
        && cmds.iter().all(|(cmd, db)| {
            crate::commands::is_ephemeral(*db)
                && !matches!(
                    cmd,
                    Command::TablePublish { .. } | Command::FlushAll | Command::Copy { .. }
                )
        });
    let responses: Vec<Response> = if all_mem {
        unsafe {
            pg_sys::CurrentMemoryContext = pg_sys::TopMemoryContext;
        }
        cmds.iter().map(|(cmd, db)| cmd.execute_mem(*db)).collect()
    } else if cmds.len() == 1 {
        // Still a subtransaction with no sibling to protect: an SPI error
        // unwinds by longjmp past the `Err` arms that look like they handle it
        // and kills the worker. The routed-table INSERT reaches user tables
        // this extension never validated, so it is a reachable path.
        let (cmd, db) = &cmds[0];
        vec![BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| unsafe { in_subtransaction(|| cmd.execute(client, *db)) })
        })]
    } else {
        BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                cmds.iter()
                    .map(|(cmd, db)| unsafe { in_subtransaction(|| cmd.execute(client, *db)) })
                    .collect()
            })
        })
    };
    // Publish the writes so a WATCH on any worker sees them. One `write_keys()`
    // pass, not two — it allocates a Vec per command.
    if let Some(watch_ctl) = crate::watch_state() {
        for (cmd, db) in cmds {
            // A flush writes to keys it cannot name, so every counter moves.
            if matches!(cmd, Command::FlushDb | Command::FlushAll) {
                unsafe { crate::watch::bump_all(watch_ctl) };
                continue;
            }
            for key in cmd.write_keys() {
                unsafe { crate::watch::bump(watch_ctl, *db, key) };
            }
        }
    }
    responses
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
    let mut writer = BufWriter::new(write_stream);
    let mut parser = RespParser::new(stream);
    let mut db = default_db;
    let mut proto: u8 = 2;
    let mut authenticated = required_password.is_none();
    let mut multi = false;
    let mut queue: Vec<Result<Command, String>> = Vec::new();
    let mut watched: HashMap<(u8, Vec<u8>), u64> = HashMap::new();

    loop {
        let parts = match parser.read_command() {
            Ok(p) if p.is_empty() => continue,
            Ok(p) => p,
            Err(_) => break,
        };

        let cmd_name = parts
            .first()
            .map(|p| String::from_utf8_lossy(p).to_uppercase())
            .unwrap_or_default();

        if multi {
            match cmd_name.as_str() {
                "EXEC" => {
                    multi = false;
                    let has_errors = queue.iter().any(|r| r.is_err());
                    if has_errors {
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
                        Some(ctl) => watched.iter().any(|((w_db, key), &snapshot)| {
                            let now = unsafe { crate::watch::version(ctl, *w_db, key) };
                            now != snapshot
                        }),
                        // Without shared memory there is nothing to compare against.
                        // WATCH already refused, so nothing can be watched here.
                        None => false,
                    };
                    let cmds: Vec<(Command, u8)> = queue
                        .drain(..)
                        .filter_map(|r| r.ok())
                        .map(|cmd| (cmd, db))
                        .collect();
                    watched.clear();
                    if watch_dirty {
                        if proto == 3 {
                            write!(&mut writer, "_\r\n").ok();
                        } else {
                            write_null_array(&mut writer).ok();
                        }
                        flush(&mut writer);
                        continue;
                    }
                    let (resp_tx, resp_rx) = mpsc::sync_channel(1);
                    if cmd_tx.send(DispatchMsg::Batch(cmds, resp_tx)).is_err() {
                        break;
                    }
                    match resp_rx.recv() {
                        Ok(responses) => {
                            write_array_header(&mut writer, responses.len()).ok();
                            for resp in responses {
                                write_response(&mut writer, resp).ok();
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
                    write_error(&mut writer, "Command not allowed inside a transaction").ok();
                    flush(&mut writer);
                }
                _ => {
                    if queue.len() >= QUEUE_LIMIT {
                        multi = false;
                        queue.clear();
                        watched.clear();
                        write_error(&mut writer, "transaction queue limit exceeded").ok();
                        flush(&mut writer);
                    } else {
                        match Command::parse(parts) {
                            Ok(cmd) => {
                                queue.push(Ok(cmd));
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

        let cmd = match Command::parse(parts) {
            Ok(c) => c,
            Err(e) => {
                write_error(&mut writer, &e).ok();
                flush(&mut writer);
                continue;
            }
        };

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
                }
                let negotiated = proto;
                if negotiated == 3 {
                    reply(&mut writer, |w| {
                        write_map_header(w, 7)?;
                        write_bulk_string(w, b"server")?;
                        write_bulk_string(w, b"pg_redis")?;
                        write_bulk_string(w, b"version")?;
                        write_bulk_string(w, b"7.0.0")?;
                        write_bulk_string(w, b"proto")?;
                        write_integer(w, 3)?;
                        write_bulk_string(w, b"id")?;
                        write_integer(w, 0)?;
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
                        write_bulk_string(w, b"7.0.0")?;
                        write_bulk_string(w, b"proto")?;
                        write_integer(w, 2)?;
                        write_bulk_string(w, b"id")?;
                        write_integer(w, 0)?;
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
                proto = 2;
                authenticated = required_password.is_none();
                reply(&mut writer, |w| write_simple_string(w, "RESET"));
                continue;
            }
            Command::Quit => {
                reply(&mut writer, |w| write_simple_string(w, "OK"));
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
            Command::ClientId => {
                reply(&mut writer, |w| write_integer(w, 0));
                continue;
            }
            Command::ClientGetname => {
                reply(&mut writer, write_null_bulk);
                continue;
            }
            Command::ClientSetname { .. } => {
                reply(&mut writer, |w| write_simple_string(w, "OK"));
                continue;
            }
            Command::ClientSetinfo => {
                reply(&mut writer, |w| write_simple_string(w, "OK"));
                continue;
            }
            Command::ClientList => {
                let info = b"id=0 addr=127.0.0.1:0 name= db=0 cmd=client\n";
                reply(&mut writer, |w| write_bulk_string(w, info));
                continue;
            }
            Command::ClientInfo => {
                let info = b"id=0 addr=127.0.0.1:0 name= db=0 cmd=client\n";
                reply(&mut writer, |w| write_bulk_string(w, info));
                continue;
            }
            Command::ClientNoEvict => {
                reply(&mut writer, |w| write_simple_string(w, "OK"));
                continue;
            }
            Command::ClientNoTouch => {
                reply(&mut writer, |w| write_simple_string(w, "OK"));
                continue;
            }
            Command::ClientOther => {
                reply(&mut writer, |w| write_simple_string(w, "OK"));
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
            Command::Info => {
                let info = format!(
                    "# Server\r\nredis_version:7.0.0\r\nmode:standalone\r\nos:PostgreSQL\r\ndb:{}\r\ntable_mode:{}\r\n",
                    db,
                    if db.is_multiple_of(2) {
                        "unlogged"
                    } else {
                        "logged"
                    }
                );
                reply(&mut writer, |w| write_bulk_string(w, info.as_bytes()));
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
                    for key in keys {
                        let version = unsafe { crate::watch::version(ctl, db, key) };
                        watched.insert((db, key.clone()), version);
                    }
                    reply(&mut writer, |w| write_simple_string(w, "OK"));
                }
            },
            Command::Unwatch => {
                watched.clear();
                reply(&mut writer, |w| write_simple_string(w, "OK"));
            }
            Command::CmdCount => reply(&mut writer, |w| write_integer(w, 100)),
            Command::CmdInfo | Command::CmdDocs | Command::CmdList | Command::CmdOther => {
                reply(&mut writer, |w| write_array_header(w, 0))
            }
            Command::ConfigGet { .. } => reply(&mut writer, |w| write_array_header(w, 0)),
            Command::ConfigSet | Command::ConfigOther => {
                reply(&mut writer, |w| write_simple_string(w, "OK"))
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
                    let n = unsafe { crate::pubsub::publish(ctl, slots, channel, message) };
                    if let Some(route_ctl) = crate::route_state()
                        && let Some((schema, table)) =
                            unsafe { crate::pubsub::route_lookup(route_ctl, channel) }
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
            Command::PubSubChannels { pattern } => match crate::pubsub_state() {
                None => reply(&mut writer, |w| write_error(w, PUBSUB_NOT_LOADED)),
                Some((ctl, slots)) => {
                    let channels =
                        unsafe { crate::pubsub::pubsub_channels(ctl, slots, pattern.as_deref()) };
                    reply(&mut writer, |w| {
                        write_array_header(w, channels.len())?;
                        for ch in &channels {
                            write_bulk_string(w, ch)?;
                        }
                        Ok(())
                    });
                }
            },
            Command::PubSubNumSub { channels } => match crate::pubsub_state() {
                None => reply(&mut writer, |w| write_error(w, PUBSUB_NOT_LOADED)),
                Some((ctl, slots)) => {
                    let counts = unsafe { crate::pubsub::pubsub_numsub(ctl, slots, channels) };
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
                    let n = unsafe { crate::pubsub::pubsub_numpat(ctl, slots) };
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
                let cont = enter_subscribe_mode(
                    &mut writer,
                    &mut parser,
                    proto,
                    channels,
                    b"subscribe",
                    |ctl, slots, items| unsafe {
                        crate::pubsub::slot_alloc_and_subscribe(ctl, slots, items)
                    },
                );
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
                let cont = enter_subscribe_mode(
                    &mut writer,
                    &mut parser,
                    proto,
                    patterns,
                    b"psubscribe",
                    |ctl, slots, items| unsafe {
                        crate::pubsub::slot_alloc_and_psubscribe(ctl, slots, items)
                    },
                );
                if !cont {
                    break;
                }
            }
            Command::Unsubscribe | Command::PUnsubscribe => {
                // Called outside subscribe mode: send empty reply per Redis spec
                write_pubsub_header(&mut writer, 3, proto).ok();
                write_bulk_string(&mut writer, b"unsubscribe").ok();
                write_null_bulk(&mut writer).ok();
                write_integer(&mut writer, 0).ok();
                flush(&mut writer);
            }
            _ => {
                let (resp_tx, resp_rx) = mpsc::sync_channel(1);
                if cmd_tx.send(DispatchMsg::Cmd(cmd, db, resp_tx)).is_err() {
                    break;
                }
                match resp_rx.recv() {
                    Ok(response) => {
                        write_response(&mut writer, response).ok();
                        flush(&mut writer);
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn enter_subscribe_mode(
    writer: &mut BufWriter<TcpStream>,
    parser: &mut crate::resp::RespParser,
    proto: u8,
    items: &[Vec<u8>],
    verb: &[u8],
    alloc_fn: impl Fn(
        *mut crate::pubsub::PubsubCtl,
        *mut crate::pubsub::PubsubSlot,
        &[Vec<u8>],
    ) -> Option<(usize, u32)>,
) -> bool {
    let Some((ctl, slots)) = crate::pubsub_state() else {
        reply(writer, |w| write_error(w, PUBSUB_NOT_LOADED));
        return true;
    };
    match alloc_fn(ctl, slots, items) {
        None => {
            reply(writer, |w| {
                write_error(w, "max pub/sub subscribers reached")
            });
            true
        }
        Some((idx, _)) => {
            for (i, item) in items.iter().enumerate() {
                write_pubsub_header(writer, 3, proto).ok();
                write_bulk_string(writer, verb).ok();
                write_bulk_string(writer, item).ok();
                write_integer(writer, (i + 1) as i64).ok();
            }
            flush(writer);
            subscribe_loop(writer, parser, idx, proto)
        }
    }
}

fn subscribe_loop(
    writer: &mut BufWriter<TcpStream>,
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
        let seen_tail = unsafe { crate::pubsub::current_tail(slots, slot_idx) };

        // Drain ring buffer — lock-free, safe from any thread (AtomicU32, no pg_sys)
        loop {
            match unsafe { crate::pubsub::poll_message(slots, slot_idx) } {
                None => break,
                Some((channel, pattern, payload)) => {
                    if pattern.is_empty() {
                        write_pubsub_header(writer, 3, proto).ok();
                        write_bulk_string(writer, b"message").ok();
                        write_bulk_string(writer, &channel).ok();
                        write_bulk_string(writer, &payload).ok();
                    } else {
                        write_pubsub_header(writer, 4, proto).ok();
                        write_bulk_string(writer, b"pmessage").ok();
                        write_bulk_string(writer, &pattern).ok();
                        write_bulk_string(writer, &channel).ok();
                        write_bulk_string(writer, &payload).ok();
                    }
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
                unsafe { crate::pubsub::wait_for_message(slots, slot_idx, seen_tail, idle_wait) };
                idle_wait = (idle_wait * 2).min(SUBSCRIBE_WAIT_MAX);
                continue;
            }
            Err(_) => {
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
                let (ch_c, pat_c) = unsafe { crate::pubsub::subscribe(ctl, slots, slot_idx, args) };
                let total = ch_c as i64 + pat_c as i64;
                for (i, ch) in args.iter().enumerate() {
                    write_pubsub_header(writer, 3, proto).ok();
                    write_bulk_string(writer, b"subscribe").ok();
                    write_bulk_string(writer, ch).ok();
                    write_integer(writer, total - args.len() as i64 + i as i64 + 1).ok();
                }
                writer.flush().ok();
            }
            "PSUBSCRIBE" if !args.is_empty() => {
                let (ch_c, pat_c) =
                    unsafe { crate::pubsub::psubscribe(ctl, slots, slot_idx, args) };
                let total = ch_c as i64 + pat_c as i64;
                for (i, pat) in args.iter().enumerate() {
                    write_pubsub_header(writer, 3, proto).ok();
                    write_bulk_string(writer, b"psubscribe").ok();
                    write_bulk_string(writer, pat).ok();
                    write_integer(writer, total - args.len() as i64 + i as i64 + 1).ok();
                }
                writer.flush().ok();
            }
            "UNSUBSCRIBE" => {
                if handle_unsub(
                    writer,
                    ctl,
                    slots,
                    slot_idx,
                    proto,
                    args,
                    b"unsubscribe",
                    |ctl, slots, idx, items| unsafe {
                        crate::pubsub::unsubscribe(ctl, slots, idx, items)
                    },
                    |ctl, slots, idx| unsafe { crate::pubsub::channel_names(ctl, slots, idx) },
                    &mut slot_freed,
                ) {
                    break 'outer;
                }
            }
            "PUNSUBSCRIBE" => {
                if handle_unsub(
                    writer,
                    ctl,
                    slots,
                    slot_idx,
                    proto,
                    args,
                    b"punsubscribe",
                    |ctl, slots, idx, items| unsafe {
                        crate::pubsub::punsubscribe(ctl, slots, idx, items)
                    },
                    |ctl, slots, idx| unsafe { crate::pubsub::pattern_names(ctl, slots, idx) },
                    &mut slot_freed,
                ) {
                    break 'outer;
                }
            }
            "PING" => {
                let msg = args.first().map(|v| v.as_slice()).unwrap_or(b"");
                write_pubsub_header(writer, 3, proto).ok();
                write_bulk_string(writer, b"pong").ok();
                write_bulk_string(writer, b"").ok();
                write_bulk_string(writer, msg).ok();
                writer.flush().ok();
            }
            "RESET" => {
                unsafe { crate::pubsub::slot_free(ctl, slots, slot_idx) };
                slot_freed = true;
                write_simple_string(writer, "RESET").ok();
                writer.flush().ok();
                break 'outer;
            }
            "QUIT" => {
                unsafe { crate::pubsub::slot_free(ctl, slots, slot_idx) };
                slot_freed = true;
                write_simple_string(writer, "OK").ok();
                writer.flush().ok();
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
        unsafe { crate::pubsub::slot_free(ctl, slots, slot_idx) };
    }
    parser.set_read_timeout(Some(Duration::from_secs(30))).ok();
    keep_conn
}

/// Returns `true` when the caller should exit subscribe mode.
#[allow(clippy::too_many_arguments)]
fn handle_unsub(
    writer: &mut BufWriter<TcpStream>,
    ctl: *mut crate::pubsub::PubsubCtl,
    slots: *mut crate::pubsub::PubsubSlot,
    slot_idx: usize,
    proto: u8,
    args: &[Vec<u8>],
    verb: &[u8],
    unsub_fn: impl Fn(
        *mut crate::pubsub::PubsubCtl,
        *mut crate::pubsub::PubsubSlot,
        usize,
        &[Vec<u8>],
    ) -> (u32, u32),
    names_fn: impl Fn(
        *mut crate::pubsub::PubsubCtl,
        *mut crate::pubsub::PubsubSlot,
        usize,
    ) -> Vec<Vec<u8>>,
    slot_freed: &mut bool,
) -> bool {
    let names_buf: Vec<Vec<u8>>;
    let to_unsub: &[Vec<u8>] = if args.is_empty() {
        names_buf = names_fn(ctl, slots, slot_idx);
        &names_buf
    } else {
        args
    };
    if to_unsub.is_empty() {
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, verb).ok();
        write_null_bulk(writer).ok();
        write_integer(writer, 0).ok();
        writer.flush().ok();
        // Do not mark slot_freed — let subscribe_loop's cleanup call slot_free.
        return true;
    }
    let (ch_c, pat_c) = unsub_fn(ctl, slots, slot_idx, to_unsub);
    let final_total = ch_c as i64 + pat_c as i64;
    let n = to_unsub.len();
    for (i, item) in to_unsub.iter().enumerate() {
        write_pubsub_header(writer, 3, proto).ok();
        write_bulk_string(writer, verb).ok();
        write_bulk_string(writer, item).ok();
        write_integer(writer, final_total + (n - 1 - i) as i64).ok();
    }
    writer.flush().ok();
    if final_total == 0 {
        *slot_freed = true;
        return true;
    }
    false
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
                    DispatchMsg::Cmd(_, db, resp_tx) => {
                        db_tx.send(db).ok();
                        resp_tx.send(Response::Ok).ok();
                    }
                    DispatchMsg::Batch(cmds, resp_tx) => {
                        resp_tx
                            .send(cmds.iter().map(|_| Response::Ok).collect())
                            .ok();
                    }
                    DispatchMsg::FireAndForget(..) => {}
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
