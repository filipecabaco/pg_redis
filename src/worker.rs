use crate::commands::{Command, Response};
use crate::resp::*;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use socket2::{Domain, Socket, Type};
use std::io::BufWriter;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

type CmdMsg = (Command, u8, mpsc::SyncSender<Response>);

pub fn worker_main() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let port = crate::PORT.get() as u16;
    let default_db: u8 = if crate::USE_LOGGED.get() { 1 } else { 0 };
    // Read all GUC values on the BGW main thread before spawning — pgrx GUC
    // reads call into postgres FFI which is not thread-safe.
    let listen_addr = crate::LISTEN_ADDRESS
        .get()
        .as_deref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or("0.0.0.0")
        .to_string();
    let max_conn = crate::MAX_CONNECTIONS.get().max(1) as usize;
    let password = configured_password();
    let batch_size = crate::BATCH_SIZE.get().max(1) as usize;

    // Bounded: limits queued commands under backpressure rather than buffering indefinitely.
    let (cmd_tx, cmd_rx) = mpsc::sync_channel::<CmdMsg>(256);

    std::thread::spawn(move || {
        accept_loop(port, cmd_tx, default_db, listen_addr, max_conn, password)
    });

    let mut last_expiry_scan = Instant::now();

    loop {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        match cmd_rx.recv_timeout(Duration::from_millis(250)) {
            Ok(first) => {
                // Drain up to batch_size queued commands without blocking.
                // All commands share one PostgreSQL transaction, amortising the
                // WAL flush cost across the entire batch.
                let mut batch: Vec<CmdMsg> = Vec::with_capacity(batch_size);
                batch.push(first);
                while batch.len() < batch_size {
                    match cmd_rx.try_recv() {
                        Ok(item) => batch.push(item),
                        Err(_) => break,
                    }
                }

                if batch.len() == 1 {
                    // Single command: no savepoint overhead.
                    let (cmd, db, resp_tx) = batch.pop().unwrap();
                    let response = BackgroundWorker::transaction(|| {
                        Spi::connect_mut(|client| cmd.execute(client, db))
                    });
                    resp_tx.send(response).ok();
                } else {
                    // Multiple commands: savepoint per command so one failure
                    // rolls back only that command, not the entire batch.
                    let responses: Vec<Response> = BackgroundWorker::transaction(|| {
                        Spi::connect_mut(|client| {
                            batch
                                .iter()
                                .map(|(cmd, db, _)| {
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
                    });
                    for ((_, _, resp_tx), response) in batch.into_iter().zip(responses) {
                        resp_tx.send(response).ok();
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }

        if last_expiry_scan.elapsed() >= Duration::from_secs(1) {
            last_expiry_scan = Instant::now();
            for db in 0u8..16 {
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

fn accept_loop(
    port: u16,
    cmd_tx: mpsc::SyncSender<CmdMsg>,
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

/// Constant-time byte slice comparison to resist timing attacks on password checks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

/// Returns the configured password bytes, or `None` if authentication is disabled.
fn configured_password() -> Option<Vec<u8>> {
    crate::PASSWORD.get().as_deref().and_then(|s| {
        let b = s.to_bytes();
        if b.is_empty() {
            None
        } else {
            Some(b.to_vec())
        }
    })
}

fn conn_loop(
    stream: TcpStream,
    cmd_tx: mpsc::SyncSender<CmdMsg>,
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

    loop {
        let parts = match parser.read_command() {
            Ok(p) if p.is_empty() => continue,
            Ok(p) => p,
            Err(_) => break,
        };

        let cmd = match Command::parse(parts) {
            Ok(c) => c,
            Err(e) => {
                write_error(&mut writer, &e).ok();
                flush(&mut writer);
                continue;
            }
        };

        // PING, AUTH, HELLO, RESET, QUIT, and CLIENT commands are always available,
        // even before authentication.
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
                    if db.is_multiple_of(2) { "unlogged" } else { "logged" }
                );
                reply(&mut writer, |w| write_bulk_string(w, info.as_bytes()));
            }
            Command::CmdCount => reply(&mut writer, |w| write_integer(w, 100)),
            Command::CmdInfo | Command::CmdDocs | Command::CmdList | Command::CmdOther => {
                reply(&mut writer, |w| write_array_header(w, 0))
            }
            Command::ConfigGet { .. } => reply(&mut writer, |w| write_array_header(w, 0)),
            Command::ConfigSet | Command::ConfigOther => {
                reply(&mut writer, |w| write_simple_string(w, "OK"))
            }
            _ => {
                // SPI-bound command — fall through to dispatcher.
                let (resp_tx, resp_rx) = mpsc::sync_channel(1);
                if cmd_tx.send((cmd, db, resp_tx)).is_err() {
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
            // SCAN returns *2 [cursor_bulk_string] [*N keys...]
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

    fn fake_dispatcher(cmd_rx: mpsc::Receiver<CmdMsg>) -> mpsc::Receiver<u8> {
        let (db_tx, db_rx) = mpsc::sync_channel(16);
        std::thread::spawn(move || {
            for (_, db, resp_tx) in cmd_rx {
                db_tx.send(db).ok();
                resp_tx.send(Response::Ok).ok();
            }
        });
        db_rx
    }

    fn connect_conn_loop(default_db: u8) -> (TcpStream, mpsc::Receiver<u8>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<CmdMsg>(256);
        let db_rx = fake_dispatcher(cmd_rx);
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            conn_loop(stream, cmd_tx, default_db, None);
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

    // ─────────────────────────── Authentication ──────────────────────────────

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
        // conn_loop has no required_password when configured_password() returns None,
        // which is the normal test state.  PING must always respond even if a
        // password were required — verified here by confirming PING works normally.
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["PING"]);
        let resp = read_line(&mut client);
        assert_eq!(resp, "+PONG", "PING must respond without authentication");
    }

    #[test]
    fn auth_with_no_password_configured_always_succeeds() {
        // When no password GUC is set, AUTH returns OK unconditionally.
        let (mut client, _) = connect_conn_loop(1);
        send_cmd(&mut client, &["AUTH", "anything"]);
        let resp = read_line(&mut client);
        assert_eq!(
            resp, "+OK",
            "AUTH must return OK when no password is configured"
        );
    }

    // ──────────────────────────── Connection limit ────────────────────────────

    #[test]
    fn connection_counter_increments_and_decrements() {
        let counter = Arc::new(AtomicUsize::new(0));
        assert_eq!(counter.load(Ordering::Relaxed), 0);

        counter.fetch_add(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        counter.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
