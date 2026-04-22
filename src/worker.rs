use crate::commands::{Command, Response};
use crate::resp::*;
use pgrx::bgworkers::*;
use pgrx::pg_sys;
use pgrx::prelude::*;
use socket2::{Domain, Socket, Type};
use std::collections::HashMap;
use std::io::{self, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

type VersionMap = Arc<RwLock<HashMap<(u8, String), u64>>>;
type PendingBatch = Option<(Vec<(Command, u8)>, mpsc::SyncSender<Vec<Response>>)>;

enum DispatchMsg {
    Cmd(Command, u8, mpsc::SyncSender<Response>),
    Batch(Vec<(Command, u8)>, mpsc::SyncSender<Vec<Response>>),
    FireAndForget(Command, u8),
}

const QUEUE_LIMIT: usize = 10_000;

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
    let default_db: u8 = if crate::USE_LOGGED.get() { 1 } else { 0 };
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
    let version_map: VersionMap = Arc::new(RwLock::new(HashMap::new()));
    let version_map_accept = Arc::clone(&version_map);

    std::thread::spawn(move || {
        accept_loop(
            port,
            cmd_tx,
            default_db,
            listen_addr,
            max_conn,
            password,
            version_map_accept,
        )
    });

    let mut last_expiry_scan = Instant::now();

    loop {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        match cmd_rx.recv_timeout(Duration::from_millis(250)) {
            Ok(first) => match first {
                DispatchMsg::FireAndForget(cmd, db) => {
                    run_dispatch_batch(&[(cmd, db)], mem_mode, &version_map);
                }
                DispatchMsg::Batch(cmds, resp_tx) => {
                    let responses = run_dispatch_batch(&cmds, mem_mode, &version_map);
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

                    let responses = run_dispatch_batch(&cmds, mem_mode, &version_map);
                    for (tx, resp) in txs.into_iter().zip(responses) {
                        tx.send(resp).ok();
                    }

                    if !faf_cmds.is_empty() {
                        run_dispatch_batch(&faf_cmds, mem_mode, &version_map);
                    }

                    if let Some((batch_cmds, batch_tx)) = pending_batch {
                        let responses = run_dispatch_batch(&batch_cmds, mem_mode, &version_map);
                        batch_tx.send(responses).ok();
                    }
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                std::thread::sleep(Duration::from_millis(250));
            }
        }

        if last_expiry_scan.elapsed() >= Duration::from_secs(1) {
            last_expiry_scan = Instant::now();
            for db in 0u8..16 {
                if mem_mode && db % 2 == 0 {
                    unsafe {
                        pg_sys::CurrentMemoryContext = pg_sys::TopMemoryContext;
                        crate::mem::mem_sweep_expired((db / 2) as usize);
                    }
                } else {
                    BackgroundWorker::transaction(|| {
                        Spi::connect_mut(|client| {
                            client
                                .update(
                                    &format!(
                                        "DELETE FROM redis.kv_{db} \
                                         WHERE expires_at IS NOT NULL AND expires_at <= now()"
                                    ),
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

fn run_dispatch_batch(
    cmds: &[(Command, u8)],
    mem_mode: bool,
    version_map: &VersionMap,
) -> Vec<Response> {
    let all_mem = mem_mode
        && cmds
            .iter()
            .all(|(cmd, db)| db % 2 == 0 && !matches!(cmd, Command::TablePublish { .. }));
    let responses: Vec<Response> = if all_mem {
        unsafe {
            pg_sys::CurrentMemoryContext = pg_sys::TopMemoryContext;
        }
        cmds.iter().map(|(cmd, db)| cmd.execute_mem(*db)).collect()
    } else if cmds.len() == 1 {
        let (cmd, db) = &cmds[0];
        vec![BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| cmd.execute(client, *db))
        })]
    } else {
        BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                cmds.iter()
                    .map(|(cmd, db)| {
                        client.update("SAVEPOINT pgr", None, &[]).ok();
                        let resp = cmd.execute(client, *db);
                        if matches!(resp, Response::Error(_)) {
                            client.update("ROLLBACK TO SAVEPOINT pgr", None, &[]).ok();
                        }
                        client.update("RELEASE SAVEPOINT pgr", None, &[]).ok();
                        resp
                    })
                    .collect()
            })
        })
    };
    {
        let mut versions = version_map.write().unwrap();
        for (cmd, db) in cmds {
            for key in cmd.write_keys() {
                *versions.entry((*db, key.to_string())).or_insert(0) += 1;
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
    version_map: VersionMap,
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
                let tx = cmd_tx.clone();
                let counter = Arc::clone(&conn_count);
                let pw = password.clone();
                let vm = Arc::clone(&version_map);
                counter.fetch_add(1, Ordering::Relaxed);
                std::thread::spawn(move || {
                    conn_loop(s, tx, default_db, pw, vm);
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
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
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
    version_map: VersionMap,
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
    let mut watched: HashMap<(u8, String), u64> = HashMap::new();

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
                    let watch_dirty = {
                        let versions = version_map.read().unwrap();
                        watched.iter().any(|((w_db, key), &snap_ver)| {
                            versions.get(&(*w_db, key.clone())).copied().unwrap_or(0) != snap_ver
                        })
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
            Command::Watch { keys } => {
                let versions = version_map.read().unwrap();
                for key in keys {
                    let ver = versions.get(&(db, key.clone())).copied().unwrap_or(0);
                    watched.insert((db, key.clone()), ver);
                }
                drop(versions);
                reply(&mut writer, |w| write_simple_string(w, "OK"));
            }
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
            Command::Publish { channel, message } => match crate::pubsub_state() {
                None => reply(&mut writer, |w| {
                    write_error(w, "pub/sub requires shared_preload_libraries = 'pg_redis'")
                }),
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
                None => reply(&mut writer, |w| {
                    write_error(w, "pub/sub requires shared_preload_libraries = 'pg_redis'")
                }),
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
                None => reply(&mut writer, |w| {
                    write_error(w, "pub/sub requires shared_preload_libraries = 'pg_redis'")
                }),
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
                None => reply(&mut writer, |w| {
                    write_error(w, "pub/sub requires shared_preload_libraries = 'pg_redis'")
                }),
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
        reply(writer, |w| {
            write_error(w, "pub/sub requires shared_preload_libraries = 'pg_redis'")
        });
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
    parser.set_read_timeout(Some(Duration::from_millis(5))).ok();
    let mut keep_conn = true;
    let mut slot_freed = false;

    'outer: loop {
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
                continue;
            }
            Err(_) => {
                keep_conn = false;
                break;
            }
            Ok(p) if p.is_empty() => continue,
            Ok(p) => p,
        };

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
                        let responses = cmds.iter().map(|_| Response::Ok).collect();
                        resp_tx.send(responses).ok();
                    }
                    DispatchMsg::FireAndForget(..) => {}
                }
            }
        });
        db_rx
    }

    fn connect_conn_loop(default_db: u8) -> (TcpStream, mpsc::Receiver<u8>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<DispatchMsg>(256);
        let db_rx = fake_dispatcher(cmd_rx);
        let version_map: VersionMap = Arc::new(RwLock::new(HashMap::new()));
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            conn_loop(stream, cmd_tx, default_db, None, version_map);
        });
        let client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        (client, db_rx)
    }

    fn send_cmd(client: &mut TcpStream, parts: &[&str]) {
        let mut buf = format!("*{}\r\n", parts.len());
        for p in parts {
            buf.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
        }
        client.write_all(buf.as_bytes()).unwrap();
    }

    fn read_line(client: &mut TcpStream) -> String {
        let mut out = String::new();
        let mut byte = [0u8; 1];
        loop {
            client.read_exact(&mut byte).unwrap();
            if byte[0] == b'\n' {
                break;
            }
            if byte[0] != b'\r' {
                out.push(byte[0] as char);
            }
        }
        out
    }

    #[test]
    fn conn_loop_uses_default_db_1_for_first_command() {
        let (mut client, db_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["SET", "k", "v"]);
        let db = db_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(db, 1, "expected db=1 (default logged), got {}", db);
    }

    #[test]
    fn conn_loop_uses_default_db_0_for_first_command() {
        let (mut client, db_rx) = connect_conn_loop(0);
        send_cmd(&mut client, &["SET", "k", "v"]);
        let db = db_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(db, 0, "expected db=0 (default unlogged), got {}", db);
    }

    #[test]
    fn select_0_routes_to_db_0_unlogged() {
        let (mut client, db_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["SELECT", "0"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+OK", "SELECT 0 should return OK");
        send_cmd(&mut client, &["SET", "k", "v"]);
        let db = db_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(db, 0, "after SELECT 0, db should be 0 (unlogged)");
    }

    #[test]
    fn select_1_routes_to_db_1_logged() {
        let (mut client, db_rx) = connect_conn_loop(0);
        send_cmd(&mut client, &["SELECT", "1"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+OK", "SELECT 1 should return OK");
        send_cmd(&mut client, &["SET", "k", "v"]);
        let db = db_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(db, 1, "after SELECT 1, db should be 1 (logged)");
    }

    #[test]
    fn select_db_is_independent_per_connection() {
        let (mut client_a, db_rx_a) = connect_conn_loop(1);
        let (mut client_b, db_rx_b) = connect_conn_loop(0);

        send_cmd(&mut client_a, &["SELECT", "2"]);
        read_line(&mut client_a);

        send_cmd(&mut client_b, &["SELECT", "3"]);
        read_line(&mut client_b);

        send_cmd(&mut client_a, &["SET", "k", "v"]);
        send_cmd(&mut client_b, &["SET", "k", "v"]);

        let a_db = db_rx_a.recv_timeout(Duration::from_secs(2)).unwrap();
        let b_db = db_rx_b.recv_timeout(Duration::from_secs(2)).unwrap();

        assert_eq!(
            a_db, 2,
            "connection A: after SELECT 2 should use db 2 (unlogged)"
        );
        assert_eq!(
            b_db, 3,
            "connection B: after SELECT 3 should use db 3 (logged)"
        );
    }

    #[test]
    fn ping_is_handled_without_hitting_dispatcher() {
        let (mut client, logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["PING"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+PONG");
        assert!(
            logged_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "PING should not reach the SPI dispatcher"
        );
    }

    #[test]
    fn select_does_not_reach_dispatcher() {
        let (mut client, logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["SELECT", "0"]);
        read_line(&mut client);
        assert!(
            logged_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "SELECT should not reach the SPI dispatcher"
        );
    }

    #[test]
    fn set_reaches_dispatcher() {
        let (mut client, logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["SET", "k", "v"]);
        assert!(
            logged_rx.recv_timeout(Duration::from_secs(2)).is_ok(),
            "SET must reach the SPI dispatcher"
        );
    }

    #[test]
    fn get_reaches_dispatcher() {
        let (mut client, logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["GET", "k"]);
        assert!(
            logged_rx.recv_timeout(Duration::from_secs(2)).is_ok(),
            "GET must reach the SPI dispatcher"
        );
    }

    #[test]
    fn del_reaches_dispatcher() {
        let (mut client, logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["DEL", "k"]);
        assert!(
            logged_rx.recv_timeout(Duration::from_secs(2)).is_ok(),
            "DEL must reach the SPI dispatcher"
        );
    }

    #[test]
    fn dispatcher_reply_is_forwarded_to_client() {
        let (mut client, _logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["SET", "k", "v"]);
        let resp = read_line(&mut client);
        assert_eq!(
            resp, "+OK",
            "dispatcher response must be forwarded to the client"
        );
    }

    #[test]
    fn constant_time_eq_matches_equal_slices() {
        assert!(constant_time_eq(b"secret", b"secret"));
    }

    #[test]
    fn constant_time_eq_rejects_different_slices() {
        assert!(!constant_time_eq(b"secret", b"wroong"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"longer"));
    }

    #[test]
    fn constant_time_eq_empty_matches_empty() {
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn ping_is_allowed_without_auth() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["PING"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+PONG", "PING must respond without authentication");
    }

    #[test]
    fn auth_with_no_password_configured_always_succeeds() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["AUTH", "anything"]);
        let resp = read_line(&mut client);
        assert_eq!(
            resp, "+OK",
            "AUTH must return OK when no password is configured"
        );
    }

    #[test]
    fn connection_counter_increments_and_decrements() {
        let counter = Arc::new(AtomicUsize::new(0));
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        counter.fetch_add(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        counter.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn multi_returns_ok() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["MULTI"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+OK", "MULTI must return OK");
    }

    #[test]
    fn multi_nested_returns_error() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["MULTI"]);
        read_line(&mut client);
        send_cmd(&mut client, &["MULTI"]);
        let resp = read_line(&mut client);
        assert!(
            resp.starts_with("-ERR"),
            "nested MULTI must return error, got: {}",
            resp
        );
    }

    #[test]
    fn discard_outside_multi_returns_error() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["DISCARD"]);
        let resp = read_line(&mut client);
        assert!(
            resp.starts_with("-ERR"),
            "DISCARD without MULTI must return error, got: {}",
            resp
        );
    }

    #[test]
    fn exec_outside_multi_returns_error() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["EXEC"]);
        let resp = read_line(&mut client);
        assert!(
            resp.starts_with("-ERR"),
            "EXEC without MULTI must return error, got: {}",
            resp
        );
    }

    #[test]
    fn commands_inside_multi_return_queued() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["MULTI"]);
        read_line(&mut client);
        send_cmd(&mut client, &["SET", "k", "v"]);
        let resp = read_line(&mut client);
        assert_eq!(
            resp, "+QUEUED",
            "commands inside MULTI must return QUEUED, got: {}",
            resp
        );
    }

    #[test]
    fn discard_inside_multi_returns_ok_and_clears_queue() {
        let (mut client, logged_rx) = connect_conn_loop(1);
        send_cmd(&mut client, &["MULTI"]);
        read_line(&mut client);
        send_cmd(&mut client, &["SET", "k", "v"]);
        read_line(&mut client);
        send_cmd(&mut client, &["DISCARD"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+OK", "DISCARD must return OK, got: {}", resp);
        assert!(
            logged_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "queued commands must not reach dispatcher after DISCARD"
        );
    }

    #[test]
    fn exec_inside_multi_dispatches_batch_and_returns_array() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["MULTI"]);
        read_line(&mut client);
        send_cmd(&mut client, &["SET", "k", "v"]);
        read_line(&mut client);
        send_cmd(&mut client, &["EXEC"]);
        let array_header = read_line(&mut client);
        assert_eq!(
            array_header, "*1",
            "EXEC must return array with one element, got: {}",
            array_header
        );
        let item_resp = read_line(&mut client);
        assert_eq!(
            item_resp, "+OK",
            "SET response in EXEC array must be OK, got: {}",
            item_resp
        );
    }

    #[test]
    fn watch_returns_ok() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["WATCH", "mykey"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+OK", "WATCH must return OK, got: {}", resp);
    }

    #[test]
    fn unwatch_returns_ok() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["UNWATCH"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+OK", "UNWATCH must return OK, got: {}", resp);
    }

    #[test]
    fn watch_inside_multi_returns_error() {
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["MULTI"]);
        read_line(&mut client);
        send_cmd(&mut client, &["WATCH", "k"]);
        let resp = read_line(&mut client);
        assert!(
            resp.starts_with("-ERR"),
            "WATCH inside MULTI must return error, got: {}",
            resp
        );
    }
}
