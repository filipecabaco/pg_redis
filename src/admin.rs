//! The server-administration commands: `CLIENT`, `CONFIG`, `INFO`,
//! `COMMAND`, `SLOWLOG`, `LATENCY`, `ACL`, `DEBUG` and the replication and
//! persistence commands a single node answers trivially.
//!
//! They act on the connection and on server-wide state (`server.rs`), not on
//! keys, so they run on the connection's own thread. The few that need data —
//! `INFO keyspace` — reach the dispatcher through the `data` callback.
#![forbid(unsafe_code)]

use crate::commands::{Command, Response, Value};
use crate::redis_tables::{Begin, CommandSpec, Find, KeySpec};
use crate::server::{self, ClientHandle};

/// An admin command, kept as its arguments: most validation is the reply.
#[derive(Debug, Clone)]
pub struct Admin {
    pub name: String,
    pub args: Vec<Vec<u8>>,
    /// `SAVE`: the SQL path has already snapshotted the memory+snapshot
    /// databases (`storage::save_now`), and found at least one.
    pub saved: bool,
}

const NAMES: &[&str] = &[
    "CLIENT",
    "CONFIG",
    "INFO",
    "COMMAND",
    "SLOWLOG",
    "LATENCY",
    "ACL",
    "DEBUG",
    "LASTSAVE",
    "SAVE",
    "BGSAVE",
    "BGREWRITEAOF",
    "ROLE",
    "REPLICAOF",
    "SLAVEOF",
    "WAIT",
    "WAITAOF",
    "READONLY",
    "CLUSTER",
    "ASKING",
    "READWRITE",
    "LOLWUT",
    "SHUTDOWN",
    "FAILOVER",
    "MEMORY",
];

impl Admin {
    pub fn parse(name: &str, args: &[Vec<u8>]) -> Option<Admin> {
        NAMES.contains(&name).then(|| Admin {
            name: name.to_string(),
            args: args.to_vec(),
            saved: false,
        })
    }

    fn sub(&self) -> String {
        self.args
            .first()
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
            .unwrap_or_default()
    }
}

/// Per-connection state the admin commands read or change.
pub struct Conn<'a> {
    pub client: &'a ClientHandle,
    pub db: u8,
    /// `CLIENT REPLY`: replies are written only while this is `On`.
    pub reply: ReplyMode,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplyMode {
    On,
    Off,
    /// Skip the next reply, then back to `On`.
    Skip,
}

/// Runs a data command on the dispatcher, for the few admin commands that
/// need one.
pub type Data<'a> = dyn FnMut(Command, u8) -> Response + 'a;

fn err(msg: impl Into<String>) -> Response {
    Response::Error(msg.into())
}

fn syntax() -> Response {
    err("ERR syntax error")
}

fn bulk(s: impl Into<Vec<u8>>) -> Response {
    Response::BulkString(s.into())
}

fn int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

fn help(lines: &[&str]) -> Response {
    Response::Value(Value::Array(
        lines
            .iter()
            .map(|l| Value::Simple((*l).to_string()))
            .collect(),
    ))
}

pub fn execute(a: &Admin, conn: &mut Conn, data: &mut Data) -> Response {
    match a.name.as_str() {
        "SCRIPT" | "FUNCTION" => crate::scripting::admin_execute(a),
        "CLIENT" => client(a, conn),
        "CONFIG" => config(a),
        "INFO" => info(&a.args, conn, data),
        "COMMAND" => command(a),
        "SLOWLOG" => slowlog(a),
        "LATENCY" => latency(a),
        "ACL" => acl(a),
        "DEBUG" => debug(a, conn, data),
        "MEMORY" => memory(a),
        "LASTSAVE" => Response::Integer(
            server::ctl().map_or(0, |c| c.lastsave.load(std::sync::atomic::Ordering::Relaxed)),
        ),
        // A memory+snapshot database is saved for real (docs/STORAGE.md).
        // Every other one is written through to PostgreSQL as it goes, or has
        // no disk image by choice, so there is nothing more to save; say so
        // the way Redis does when it succeeds.
        // The snapshots themselves are taken by the SQL path before this runs
        // (`Command::execute_inner`), where SPI is to hand; what is left is
        // LASTSAVE when there was nothing to snapshot.
        "SAVE" => {
            if !a.saved
                && let Some(c) = server::ctl()
            {
                c.lastsave.store(
                    server::now_ms() / 1000,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Response::Ok
        }
        "BGSAVE" => {
            if !crate::storage::request_snapshots()
                && let Some(c) = server::ctl()
            {
                c.lastsave.store(
                    server::now_ms() / 1000,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Response::SimpleString("Background saving started".into())
        }
        "BGREWRITEAOF" => {
            Response::SimpleString("Background append only file rewriting started".into())
        }
        "ROLE" => Response::Value(Value::Array(vec![
            Value::str("master"),
            Value::Int(0),
            Value::Array(vec![]),
        ])),
        "REPLICAOF" | "SLAVEOF" => {
            if a.args.len() == 2
                && a.args[0].eq_ignore_ascii_case(b"NO")
                && a.args[1].eq_ignore_ascii_case(b"ONE")
            {
                Response::Ok
            } else {
                err(
                    "ERR pg_redis does not replicate from another Redis; use PostgreSQL replication",
                )
            }
        }
        // No replicas, so none acknowledge: the count is 0, at once.
        "WAIT" => match (
            a.args.first().and_then(|v| int(v)),
            a.args.get(1).and_then(|v| int(v)),
        ) {
            (Some(_), Some(t)) if t >= 0 => Response::Integer(0),
            (Some(_), Some(_)) => err("ERR timeout is negative"),
            _ => err("ERR value is not an integer or out of range"),
        },
        "WAITAOF" => {
            err("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
        }
        "READONLY" | "READWRITE" | "CLUSTER" | "ASKING" => {
            err("ERR This instance has cluster support disabled")
        }
        "FAILOVER" => err("ERR FAILOVER requires connected replicas."),
        "SHUTDOWN" => {
            err("ERR pg_redis does not shut down over the Redis protocol; stop PostgreSQL instead")
        }
        "LOLWUT" => bulk(format!(
            "pg_redis — Redis {} over PostgreSQL\n",
            crate::commands::REDIS_VERSION
        )),
        _ => err(format!("ERR unknown command '{}'", a.name)),
    }
}

// ───────────────────────────────── CLIENT ───────────────────────────────────

fn client(a: &Admin, conn: &mut Conn) -> Response {
    let args = &a.args;
    match a.sub().as_str() {
        "ID" => Response::Integer(conn.client.id as i64),
        "GETNAME" => {
            let n = conn.client.name();
            if n.is_empty() {
                Response::Null
            } else {
                bulk(n)
            }
        }
        "SETNAME" => {
            let name = &args[1];
            if name.iter().any(|&c| c <= b' ' || c > b'~') {
                return err(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                );
            }
            conn.client.set_name(name);
            Response::Ok
        }
        "SETINFO" => {
            let (attr, val) = (&args[1], &args[2]);
            if val.iter().any(|&c| c <= b' ' || c > b'~') {
                return err(format!(
                    "ERR {} cannot contain spaces, newlines or special characters.",
                    String::from_utf8_lossy(attr).to_ascii_lowercase()
                ));
            }
            if attr.eq_ignore_ascii_case(b"lib-name") {
                conn.client.set_lib(Some(val), None);
            } else if attr.eq_ignore_ascii_case(b"lib-ver") {
                conn.client.set_lib(None, Some(val));
            } else {
                return err(format!(
                    "ERR Unrecognized option '{}'",
                    String::from_utf8_lossy(attr)
                ));
            }
            Response::Ok
        }
        "INFO" => {
            let me = server::clients()
                .into_iter()
                .find(|c| c.id == conn.client.id);
            bulk(me.map(|c| format!("{}\n", c.line())).unwrap_or_default())
        }
        "LIST" => client_list(&args[1..]),
        "KILL" => client_kill(&args[1..], conn),
        "REPLY" => match args
            .get(1)
            .map(|v| String::from_utf8_lossy(v).to_ascii_uppercase())
            .as_deref()
        {
            Some("ON") => {
                conn.reply = ReplyMode::On;
                Response::Ok
            }
            Some("OFF") => {
                conn.reply = ReplyMode::Off;
                Response::Ok
            }
            Some("SKIP") => {
                conn.reply = ReplyMode::Skip;
                Response::Ok
            }
            _ => syntax(),
        },
        "PAUSE" => {
            let Some(ms) = int(&args[1]).filter(|n| *n >= 0) else {
                return err("ERR timeout is not an integer or out of range");
            };
            let writes = match args.get(2) {
                None => false,
                Some(m) if m.eq_ignore_ascii_case(b"WRITE") => true,
                Some(m) if m.eq_ignore_ascii_case(b"ALL") => false,
                Some(_) => return err("ERR CLIENT PAUSE mode must be WRITE or ALL"),
            };
            if let Some(c) = server::ctl() {
                use std::sync::atomic::Ordering::Relaxed;
                c.pause_writes_only.store(writes as u32, Relaxed);
                c.pause_until_ms.store(server::now_ms() + ms, Relaxed);
            }
            Response::Ok
        }
        "UNPAUSE" => {
            if let Some(c) = server::ctl() {
                c.pause_until_ms
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
            Response::Ok
        }
        "NO-EVICT" | "NO-TOUCH" => {
            let flag = if a.sub() == "NO-EVICT" {
                server::flag::NO_EVICT
            } else {
                server::flag::NO_TOUCH
            };
            match String::from_utf8_lossy(&args[1])
                .to_ascii_uppercase()
                .as_str()
            {
                "ON" => conn.client.set_flag(flag, true),
                "OFF" => conn.client.set_flag(flag, false),
                _ => return syntax(),
            }
            Response::Ok
        }
        "GETREDIR" => crate::tracking::getredir(conn.client),
        "TRACKINGINFO" => crate::tracking::trackinginfo(conn.client),
        "UNBLOCK" => {
            let Some(id) = int(&args[1]) else {
                return err("ERR value is not an integer or out of range");
            };
            let as_error = match args.get(2) {
                None => false,
                Some(m) if m.eq_ignore_ascii_case(b"TIMEOUT") => false,
                Some(m) if m.eq_ignore_ascii_case(b"ERROR") => true,
                Some(_) => return err("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR"),
            };
            let done = server::clients()
                .into_iter()
                .find(|c| c.id == id as u64)
                .is_some_and(|c| c.unblock(as_error));
            Response::Integer(done as i64)
        }
        "CACHING" => crate::tracking::client_caching(conn.client, &args[1]),
        "TRACKING" => crate::tracking::client_tracking(conn.client, &args[1..]),
        "HELP" => help(&[
            "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "ID",
            "    Return the ID of the current connection.",
            "INFO",
            "    Return information about the current client connection.",
            "KILL <ip:port>",
            "    Kill connection made from <ip:port>.",
            "LIST [options ...]",
            "    Return information about client connections.",
            "GETNAME",
            "    Return the name of the current connection.",
            "SETNAME <name>",
            "    Assign the name <name> to the current connection.",
            "PAUSE <timeout> [WRITE|ALL]",
            "    Suspend all, or just write, clients for <timeout> milliseconds.",
            "UNPAUSE",
            "    Stop the current client pause, resuming traffic.",
            "REPLY (ON|OFF|SKIP)",
            "    Control the replies sent to the current connection.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand '{other}'. Try CLIENT HELP."
        )),
    }
}

fn client_list(opts: &[Vec<u8>]) -> Response {
    let mut all = server::clients();
    match opts {
        [] => {}
        [t, kind] if t.eq_ignore_ascii_case(b"TYPE") => {
            match String::from_utf8_lossy(kind).to_ascii_lowercase().as_str() {
                "normal" => all.retain(|c| c.flags & server::flag::PUBSUB == 0),
                "pubsub" => all.retain(|c| c.flags & server::flag::PUBSUB != 0),
                "master" | "replica" | "slave" => all.clear(),
                other => return err(format!("ERR Unknown client type '{other}'")),
            }
        }
        [t, ids @ ..] if t.eq_ignore_ascii_case(b"ID") && !ids.is_empty() => {
            let mut want = Vec::new();
            for id in ids {
                match int(id).filter(|n| *n > 0) {
                    Some(n) => want.push(n as u64),
                    None => return err("ERR Invalid client ID"),
                }
            }
            all.retain(|c| want.contains(&c.id));
        }
        _ => return syntax(),
    }
    bulk(
        all.iter()
            .map(|c| format!("{}\n", c.line()))
            .collect::<String>(),
    )
}

/// `CLIENT KILL addr` (the old form, which errors when nobody matched) or the
/// filter form, which answers how many it killed.
fn client_kill(opts: &[Vec<u8>], conn: &Conn) -> Response {
    let all = server::clients();
    if opts.len() == 1 {
        let addr = String::from_utf8_lossy(&opts[0]);
        return match all.iter().find(|c| c.addr == addr) {
            Some(c) => {
                c.kill();
                Response::Ok
            }
            None => err("ERR No such client"),
        };
    }
    if !opts.len().is_multiple_of(2) {
        return syntax();
    }
    let mut targets: Vec<&server::ClientInfo> = all.iter().collect();
    let mut skipme = true;
    for pair in opts.chunks(2) {
        let (k, v) = (
            String::from_utf8_lossy(&pair[0]).to_ascii_uppercase(),
            &pair[1],
        );
        let vs = String::from_utf8_lossy(v).to_string();
        match k.as_str() {
            "ID" => match int(v).filter(|n| *n > 0) {
                Some(id) => targets.retain(|c| c.id == id as u64),
                None => return err("ERR client-id should be greater than 0"),
            },
            "ADDR" => targets.retain(|c| c.addr == vs),
            "LADDR" => targets.retain(|c| c.laddr == vs),
            "USER" => {
                if vs != "default" {
                    return err(format!("ERR No such user '{vs}'"));
                }
            }
            "TYPE" => match vs.to_ascii_lowercase().as_str() {
                "normal" => targets.retain(|c| c.flags & server::flag::PUBSUB == 0),
                "pubsub" => targets.retain(|c| c.flags & server::flag::PUBSUB != 0),
                "master" | "replica" | "slave" => targets.clear(),
                other => return err(format!("ERR Unknown client type '{other}'")),
            },
            "SKIPME" => match vs.to_ascii_lowercase().as_str() {
                "yes" => skipme = true,
                "no" => skipme = false,
                _ => return syntax(),
            },
            "MAXAGE" => match int(v) {
                Some(age) => targets.retain(|c| c.age_s >= age),
                None => return syntax(),
            },
            _ => return syntax(),
        }
    }
    if skipme {
        targets.retain(|c| c.id != conn.client.id);
    }
    for c in &targets {
        c.kill();
    }
    // Redis closes them before it replies; here each connection closes on its
    // own thread, so wait — briefly — until the killed ones have gone.
    let ids: Vec<u64> = targets
        .iter()
        .map(|c| c.id)
        .filter(|id| *id != conn.client.id)
        .collect();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    while !ids.is_empty()
        && std::time::Instant::now() < deadline
        && server::clients().iter().any(|c| ids.contains(&c.id))
    {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    Response::Integer(targets.len() as i64)
}

// ───────────────────────────────── CONFIG ───────────────────────────────────

fn config(a: &Admin) -> Response {
    match a.sub().as_str() {
        "GET" => {
            let pairs = crate::config::get(&a.args[1..]);
            Response::Value(Value::Array(
                pairs
                    .into_iter()
                    .flat_map(|(k, v)| [Value::Bulk(k.into_bytes()), Value::Bulk(v.into_bytes())])
                    .collect(),
            ))
        }
        "SET" => {
            if !(a.args.len() - 1).is_multiple_of(2) {
                return err("ERR wrong number of arguments for 'config|set' command");
            }
            match crate::config::set(&a.args[1..]) {
                Ok(()) => Response::Ok,
                Err(e) => err(e),
            }
        }
        "RESETSTAT" => {
            server::reset_stats();
            Response::Ok
        }
        "REWRITE" => err("ERR The server is running without a config file"),
        "HELP" => help(&[
            "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "GET <pattern>",
            "    Return parameters matching the glob-like <pattern> and their values.",
            "SET <directive> <value>",
            "    Set the configuration <directive> to <value>.",
            "RESETSTAT",
            "    Reset statistics reported by the INFO command.",
            "REWRITE",
            "    Rewrite the configuration file.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand '{other}'. Try CONFIG HELP."
        )),
    }
}

// ────────────────────────────────── INFO ────────────────────────────────────

const DEFAULT_SECTIONS: &[&str] = &[
    "server",
    "clients",
    "memory",
    "persistence",
    "stats",
    "replication",
    "cpu",
    "modules",
    "errorstats",
    "cluster",
    "keyspace",
];
const ALL_EXTRA: &[&str] = &["commandstats", "latencystats"];

/// `INFO [section ...]`, with Redis's `default`, `all` and `everything`.
pub fn info(args: &[Vec<u8>], conn: &Conn, data: &mut Data) -> Response {
    let asked: Vec<String> = args
        .iter()
        .map(|a| String::from_utf8_lossy(a).to_lowercase())
        .collect();
    let mut sections: Vec<&str> = Vec::new();
    let add = |s: &'static str, v: &mut Vec<&str>| {
        if !v.contains(&s) {
            v.push(s);
        }
    };
    if asked.is_empty() {
        for s in DEFAULT_SECTIONS {
            add(s, &mut sections);
        }
    }
    for a in &asked {
        match a.as_str() {
            "default" => DEFAULT_SECTIONS.iter().for_each(|s| add(s, &mut sections)),
            "all" | "everything" => {
                DEFAULT_SECTIONS.iter().for_each(|s| add(s, &mut sections));
                ALL_EXTRA.iter().for_each(|s| add(s, &mut sections));
            }
            other => {
                for s in DEFAULT_SECTIONS.iter().chain(ALL_EXTRA) {
                    if *s == other {
                        add(s, &mut sections);
                    }
                }
            }
        }
    }
    // Redis prints sections in its own fixed order, not the order asked.
    let order: Vec<&str> = [
        "server",
        "clients",
        "memory",
        "persistence",
        "stats",
        "replication",
        "cpu",
        "modules",
        "commandstats",
        "errorstats",
        "latencystats",
        "cluster",
        "keyspace",
    ]
    .into_iter()
    .filter(|s| sections.contains(s))
    .collect();
    let mut out = String::new();
    for (i, s) in order.iter().enumerate() {
        if i > 0 {
            out.push_str("\r\n");
        }
        out.push_str(&info_section(s, conn, data));
    }
    bulk(out)
}

fn info_section(section: &str, conn: &Conn, data: &mut Data) -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let ctl = server::ctl();
    let stat = |f: fn(&server::ServerCtl) -> &std::sync::atomic::AtomicU64| {
        ctl.map_or(0, |c| f(c).load(Relaxed))
    };
    let now = server::now_ms();
    let started = ctl.map_or(now, |c| c.started_ms.load(Relaxed));
    let uptime = (now - started) / 1000;
    let mut lines: Vec<String> = Vec::new();
    let title = match section {
        "server" => "Server",
        "clients" => "Clients",
        "memory" => "Memory",
        "persistence" => "Persistence",
        "stats" => "Stats",
        "replication" => "Replication",
        "cpu" => "CPU",
        "modules" => "Modules",
        "commandstats" => "Commandstats",
        "errorstats" => "Errorstats",
        "latencystats" => "Latencystats",
        "cluster" => "Cluster",
        _ => "Keyspace",
    };
    match section {
        "server" => {
            lines.extend([
                format!("redis_version:{}", crate::commands::REDIS_VERSION),
                "redis_git_sha1:00000000".into(),
                "redis_git_dirty:0".into(),
                "redis_build_id:0".into(),
                "redis_mode:standalone".into(),
                "os:PostgreSQL".into(),
                "arch_bits:64".into(),
                "monotonic_clock:POSIX clock_gettime".into(),
                "multiplexing_api:threads".into(),
                "atomicvar_api:c11-builtin".into(),
                "gcc_version:0.0.0".into(),
                format!("process_id:{}", std::process::id()),
                "process_supervised:no".into(),
                "run_id:0000000000000000000000000000000000000000".into(),
                format!("tcp_port:{}", crate::config::live_gucs().port),
                format!("server_time_usec:{}", now * 1000),
                format!("uptime_in_seconds:{uptime}"),
                format!("uptime_in_days:{}", uptime / 86400),
                format!("hz:{}", crate::config::get_i64("hz")),
                format!("configured_hz:{}", crate::config::get_i64("hz")),
                format!("lru_clock:{}", (now / 1000) & ((1 << 24) - 1)),
                "executable:".into(),
                "config_file:".into(),
                "io_threads_active:0".into(),
                format!("pg_redis_db:{}", conn.db),
                format!(
                    "pg_redis_table_mode:{}",
                    crate::storage::policy(conn.db).name()
                ),
            ]);
        }
        "clients" => {
            let clients = server::clients();
            let pubsub = clients
                .iter()
                .filter(|c| c.flags & server::flag::PUBSUB != 0)
                .count();
            lines.extend([
                format!("connected_clients:{}", clients.len()),
                "cluster_connections:0".into(),
                format!("maxclients:{}", crate::config::live_gucs().maxclients),
                "client_recent_max_input_buffer:0".into(),
                "client_recent_max_output_buffer:0".into(),
                format!(
                    "blocked_clients:{}",
                    clients
                        .iter()
                        .filter(|c| c.flags & server::flag::BLOCKED != 0)
                        .count()
                ),
                format!(
                    "tracking_clients:{}",
                    server::ctl().map_or(0, |c| c
                        .tracking_clients
                        .load(std::sync::atomic::Ordering::Relaxed))
                ),
                format!("pubsub_clients:{pubsub}"),
                "clients_in_timeout_table:0".into(),
                format!("total_blocking_keys:{}", server::blocking_key_counts().0),
                format!(
                    "total_blocking_keys_on_nokey:{}",
                    server::blocking_key_counts().1
                ),
            ]);
        }
        "memory" => {
            let maxmemory = crate::config::get_i64("maxmemory");
            // The memory engine's arena; the durable half's memory is
            // PostgreSQL's and not counted.
            let u = crate::shm::usage().unwrap_or_default();
            lines.extend([
                format!("used_memory:{}", u.used),
                format!("used_memory_human:{}", human_bytes(u.used)),
                format!("used_memory_rss:{}", u.used),
                format!("used_memory_peak:{}", u.peak),
                format!("used_memory_peak_human:{}", human_bytes(u.peak)),
                "used_memory_lua:0".into(),
                "used_memory_scripts:0".into(),
                format!(
                    "number_of_cached_scripts:{}",
                    crate::server::cached_scripts()
                ),
                "number_of_functions:0".into(),
                "number_of_libraries:0".into(),
                format!("maxmemory:{maxmemory}"),
                format!("maxmemory_human:{}", human_bytes(maxmemory.max(0) as usize)),
                format!(
                    "maxmemory_policy:{}",
                    crate::config::get_str("maxmemory-policy")
                ),
                "mem_fragmentation_ratio:1.00".into(),
                // No replication or AOF buffers are counted in the arena.
                "mem_not_counted_for_evict:0".into(),
                "mem_allocator:postgresql".into(),
                "lazyfree_pending_objects:0".into(),
                "lazyfreed_objects:0".into(),
            ]);
        }
        "persistence" => {
            let lastsave = ctl.map_or(0, |c| c.lastsave.load(Relaxed));
            lines.extend([
                "loading:0".into(),
                "async_loading:0".into(),
                "rdb_changes_since_last_save:0".into(),
                "rdb_bgsave_in_progress:0".into(),
                format!("rdb_last_save_time:{lastsave}"),
                "rdb_last_bgsave_status:ok".into(),
                format!(
                    "aof_enabled:{}",
                    crate::config::get_bool("appendonly") as u8
                ),
                "aof_rewrite_in_progress:0".into(),
                "aof_rewrite_scheduled:0".into(),
                "aof_last_bgrewrite_status:ok".into(),
                "aof_last_write_status:ok".into(),
            ]);
        }
        "stats" => {
            lines.extend([
                format!(
                    "total_connections_received:{}",
                    stat(|c| &c.total_connections)
                ),
                format!("total_commands_processed:{}", stat(|c| &c.total_commands)),
                "instantaneous_ops_per_sec:0".into(),
                format!("total_net_input_bytes:{}", stat(|c| &c.net_input)),
                format!("total_net_output_bytes:{}", stat(|c| &c.net_output)),
                format!("rejected_connections:{}", stat(|c| &c.rejected_connections)),
                format!("expired_keys:{}", stat(|c| &c.expired_keys)),
                format!("evicted_keys:{}", stat(|c| &c.evicted_keys)),
                "evicted_clients:0".into(),
                format!(
                    "total_eviction_exceeded_time:{}",
                    server::eviction_exceeded_times().0
                ),
                format!(
                    "current_eviction_exceeded_time:{}",
                    server::eviction_exceeded_times().1
                ),
                format!("keyspace_hits:{}", stat(|c| &c.keyspace_hits)),
                format!("keyspace_misses:{}", stat(|c| &c.keyspace_misses)),
                "pubsub_channels:0".into(),
                "pubsub_patterns:0".into(),
                "pubsubshard_channels:0".into(),
                "latest_fork_usec:0".into(),
                "total_forks:0".into(),
                "migrate_cached_sockets:0".into(),
                format!("tracking_total_keys:{}", crate::tracking::table_counts().1),
                format!("tracking_total_items:{}", crate::tracking::table_counts().0),
                format!(
                    "tracking_total_prefixes:{}",
                    crate::tracking::prefix_count()
                ),
                "unexpected_error_replies:0".into(),
                format!("total_error_replies:{}", stat(|c| &c.total_error_replies)),
                "dump_payload_sanitizations:0".into(),
                format!("total_reads_processed:{}", stat(|c| &c.total_commands)),
                format!("total_writes_processed:{}", stat(|c| &c.total_commands)),
                "acl_access_denied_auth:0".into(),
                "acl_access_denied_cmd:0".into(),
                "acl_access_denied_key:0".into(),
                "acl_access_denied_channel:0".into(),
            ]);
        }
        "replication" => lines.extend([
            "role:master".into(),
            "connected_slaves:0".into(),
            "master_failover_state:no-failover".into(),
            "master_replid:0000000000000000000000000000000000000000".into(),
            "master_replid2:0000000000000000000000000000000000000000".into(),
            "master_repl_offset:0".into(),
            "second_repl_offset:-1".into(),
            "repl_backlog_active:0".into(),
            "repl_backlog_size:1048576".into(),
            "repl_backlog_first_byte_offset:0".into(),
            "repl_backlog_histlen:0".into(),
        ]),
        "cpu" => {
            let (user, sys) = crate::sys::cpu_times();
            lines.extend([
                format!("used_cpu_sys:{sys:.6}"),
                format!("used_cpu_user:{user:.6}"),
                "used_cpu_sys_children:0.000000".into(),
                "used_cpu_user_children:0.000000".into(),
                format!("used_cpu_sys_main_thread:{sys:.6}"),
                format!("used_cpu_user_main_thread:{user:.6}"),
            ]);
        }
        "modules" => {}
        "commandstats" => {
            if let Some(c) = ctl {
                for (i, spec) in crate::redis_tables::COMMANDS.iter().enumerate() {
                    let Some(s) = c.cmdstats.get(i) else { break };
                    let calls = s.calls.load(Relaxed);
                    let (rej, fail) = (s.rejected.load(Relaxed), s.failed.load(Relaxed));
                    if calls == 0 && rej == 0 && fail == 0 {
                        continue;
                    }
                    let usec = s.usec.load(Relaxed);
                    let per = if calls > 0 {
                        usec as f64 / calls as f64
                    } else {
                        0.0
                    };
                    lines.push(format!(
                        "cmdstat_{}:calls={calls},usec={usec},usec_per_call={per:.2},rejected_calls={rej},failed_calls={fail}",
                        spec.name
                    ));
                }
            }
        }
        "errorstats" => {
            for (code, n) in server::errstats() {
                lines.push(format!("errorstat_{code}:count={n}"));
            }
        }
        "latencystats" => {
            let percentiles: Vec<f64> = crate::config::get_str("latency-tracking-info-percentiles")
                .split_whitespace()
                .filter_map(|p| p.parse().ok())
                .collect();
            if let Some(c) = ctl.filter(|_| crate::config::get_bool("latency-tracking")) {
                for (i, spec) in crate::redis_tables::COMMANDS.iter().enumerate() {
                    let Some(s) = c.cmdstats.get(i) else { break };
                    if s.calls.load(Relaxed) == 0 {
                        continue;
                    }
                    let parts: Vec<String> = percentiles
                        .iter()
                        .filter_map(|p| {
                            server::percentile(s, *p).map(|v| format!("p{}={:.3}", fmt_pct(*p), v))
                        })
                        .collect();
                    lines.push(format!(
                        "latency_percentiles_usec_{}:{}",
                        spec.name,
                        parts.join(",")
                    ));
                }
            }
        }
        "cluster" => lines.push("cluster_enabled:0".into()),
        _ => {
            for db in 0..crate::commands::NUM_DBS as u8 {
                if let Response::IntegerArray(v) = data(Command::Keyspace, db)
                    && v.len() == 2
                    && v[0] > 0
                {
                    lines.push(format!("db{db}:keys={},expires={},avg_ttl=0", v[0], v[1]));
                }
            }
        }
    }
    let mut s = format!("# {title}\r\n");
    for l in lines {
        s.push_str(&l);
        s.push_str("\r\n");
    }
    s
}

/// `99`, `99.9`: a percentile as Redis prints it in a field name.
fn fmt_pct(p: f64) -> String {
    let s = format!("{p}");
    s.trim_end_matches(".0").to_string()
}

// ───────────────────────────────── COMMAND ──────────────────────────────────

/// Commands pg_redis implements, as COMMAND LIST names them, subcommands too.
fn implemented() -> impl Iterator<Item = &'static CommandSpec> {
    crate::redis_tables::COMMANDS.iter().filter(|c| {
        let top = if c.container.is_empty() {
            c.name
        } else {
            c.container
        };
        crate::commands::documented_commands().any(|d| d.eq_ignore_ascii_case(top))
    })
}

/// `implemented` by name, built once: `get_keys` runs after every command on
/// a logged database (`access::after_command`), and the filter above walks
/// the coverage page for each entry of the command table.
static BY_NAME: std::sync::LazyLock<std::collections::HashMap<&'static str, &'static CommandSpec>> =
    std::sync::LazyLock::new(|| implemented().map(|c| (c.name, c)).collect());

fn find_spec(name: &[u8]) -> Option<&'static CommandSpec> {
    let lower = String::from_utf8_lossy(name).to_ascii_lowercase();
    BY_NAME.get(lower.as_str()).copied()
}

/// Redis's `populateCommandLegacyRangeSpec`: the single first/last/step
/// range COMMAND INFO still reports, and whether keys are movable. Movability
/// comes from the key specs alone — a native command with its own key
/// extraction function (SET, BITFIELD) is not movable for having one.
fn legacy_range(c: &CommandSpec) -> (i64, i64, i64, bool) {
    let mut movable = false;
    if c.key_specs.is_empty() {
        return (0, 0, 0, movable);
    }
    let range = |k: &KeySpec| match (&k.begin, &k.find) {
        (Begin::Index(pos), Find::Range { lastkey, step, .. }) => Some((*pos, *lastkey, *step)),
        _ => None,
    };
    let incomplete = |k: &KeySpec| k.flags.iter().any(|f| f.eq_ignore_ascii_case("INCOMPLETE"));
    if c.key_specs.len() == 1
        && let Some((pos, last, step)) = range(&c.key_specs[0])
    {
        movable |= incomplete(&c.key_specs[0]);
        let lastkey = if last < 0 { last } else { pos + last };
        return (pos as i64, lastkey as i64, step as i64, movable);
    }
    let (mut first, mut last, mut prev_last) = (i32::MAX, 0i32, 0i32);
    for k in c.key_specs {
        let Some((pos, lk, step)) = range(k) else {
            movable = true;
            continue;
        };
        if step != 1 || (prev_last != 0 && prev_last != pos - 1) {
            movable = true;
            continue;
        }
        movable |= incomplete(k);
        first = first.min(pos);
        let abs = if lk >= 0 { lk + pos } else { lk };
        last = (last as u32).max(abs as u32) as i32;
        prev_last = last;
    }
    if first == i32::MAX {
        return (0, 0, 0, true);
    }
    (first as i64, last as i64, 1, movable)
}

const ACL_ORDER: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

/// The command's ACL categories, the implicit ones its flags imply included,
/// in Redis's fixed order.
pub fn acl_categories(c: &CommandSpec) -> Vec<&'static str> {
    let has = |f: &str| c.flags.contains(&f);
    let mut cats: Vec<&str> = c.acl_categories.to_vec();
    if has("write") {
        cats.push("write");
    }
    if has("readonly") && !cats.contains(&"scripting") {
        cats.push("read");
    }
    if has("admin") {
        cats.extend(["admin", "dangerous"]);
    }
    if has("pubsub") {
        cats.push("pubsub");
    }
    if has("fast") {
        cats.push("fast");
    }
    if has("blocking") {
        cats.push("blocking");
    }
    if !cats.contains(&"fast") {
        cats.push("slow");
    }
    ACL_ORDER
        .iter()
        .copied()
        .filter(|c| cats.contains(c))
        .collect()
}

fn key_spec_value(k: &KeySpec) -> Value {
    let mut m = Vec::new();
    if !k.notes.is_empty() {
        m.push((Value::str("notes"), Value::str(k.notes)));
    }
    m.push((
        Value::str("flags"),
        Value::Array(
            k.flags
                .iter()
                .map(|f| {
                    let f = if matches!(*f, "RW" | "RO" | "OW" | "RM") {
                        f.to_string()
                    } else {
                        f.to_ascii_lowercase()
                    };
                    Value::Simple(f)
                })
                .collect(),
        ),
    ));
    let begin = match &k.begin {
        Begin::Index(p) => Value::Map(vec![
            (Value::str("type"), Value::str("index")),
            (
                Value::str("spec"),
                Value::Map(vec![(Value::str("index"), Value::Int(*p as i64))]),
            ),
        ]),
        Begin::Keyword(kw, from) => Value::Map(vec![
            (Value::str("type"), Value::str("keyword")),
            (
                Value::str("spec"),
                Value::Map(vec![
                    (Value::str("keyword"), Value::str(kw)),
                    (Value::str("startfrom"), Value::Int(*from as i64)),
                ]),
            ),
        ]),
        Begin::Unknown => Value::Map(vec![
            (Value::str("type"), Value::str("unknown")),
            (Value::str("spec"), Value::Map(vec![])),
        ]),
    };
    let find = match &k.find {
        Find::Range {
            lastkey,
            step,
            limit,
        } => Value::Map(vec![
            (Value::str("type"), Value::str("range")),
            (
                Value::str("spec"),
                Value::Map(vec![
                    (Value::str("lastkey"), Value::Int(*lastkey as i64)),
                    (Value::str("keystep"), Value::Int(*step as i64)),
                    (Value::str("limit"), Value::Int(*limit as i64)),
                ]),
            ),
        ]),
        Find::Keynum {
            keynumidx,
            firstkey,
            step,
        } => Value::Map(vec![
            (Value::str("type"), Value::str("keynum")),
            (
                Value::str("spec"),
                Value::Map(vec![
                    (Value::str("keynumidx"), Value::Int(*keynumidx as i64)),
                    (Value::str("firstkey"), Value::Int(*firstkey as i64)),
                    (Value::str("keystep"), Value::Int(*step as i64)),
                ]),
            ),
        ]),
        Find::Unknown => Value::Map(vec![
            (Value::str("type"), Value::str("unknown")),
            (Value::str("spec"), Value::Map(vec![])),
        ]),
    };
    m.push((Value::str("begin_search"), begin));
    m.push((Value::str("find_keys"), find));
    Value::Map(m)
}

fn command_info(c: &CommandSpec) -> Value {
    let (first, last, step, movable) = legacy_range(c);
    let mut flags: Vec<Value> = c
        .flags
        .iter()
        .map(|f| Value::Simple(f.to_string()))
        .collect();
    if movable {
        flags.push(Value::Simple("movablekeys".into()));
    }
    let subs: Vec<Value> = implemented()
        .filter(|s| s.container == c.name)
        .map(command_info)
        .collect();
    Value::Array(vec![
        Value::str(c.name),
        Value::Int(c.arity as i64),
        Value::Array(flags),
        Value::Int(first),
        Value::Int(last),
        Value::Int(step),
        Value::Array(
            acl_categories(c)
                .into_iter()
                .map(|a| Value::Simple(format!("@{a}")))
                .collect(),
        ),
        Value::Array(c.tips.iter().map(|t| Value::str(t)).collect()),
        Value::Array(c.key_specs.iter().map(key_spec_value).collect()),
        Value::Array(subs),
    ])
}

fn arg_doc(a: &crate::redis_tables::Arg) -> Value {
    let mut m = vec![
        (Value::str("name"), Value::str(a.name)),
        (Value::str("type"), Value::str(a.kind)),
    ];
    if a.kind != "oneof" && a.kind != "block" {
        let display = if a.display.is_empty() {
            a.name
        } else {
            a.display
        };
        m.push((Value::str("display_text"), Value::str(display)));
    }
    if a.key_spec_index >= 0 {
        m.push((
            Value::str("key_spec_index"),
            Value::Int(a.key_spec_index as i64),
        ));
    }
    for (k, v) in [
        ("token", a.token),
        ("summary", a.summary),
        ("since", a.since),
        ("deprecated_since", a.deprecated_since),
    ] {
        if !v.is_empty() {
            m.push((Value::str(k), Value::str(v)));
        }
    }
    let flags: Vec<Value> = [
        (a.optional, "optional"),
        (a.multiple, "multiple"),
        (a.multiple_token, "multiple_token"),
    ]
    .into_iter()
    .filter(|(on, _)| *on)
    .map(|(_, f)| Value::Simple(f.to_string()))
    .collect();
    if !flags.is_empty() {
        m.push((Value::str("flags"), Value::Array(flags)));
    }
    if !a.args.is_empty() {
        m.push((
            Value::str("arguments"),
            Value::Array(a.args.iter().map(arg_doc).collect()),
        ));
    }
    Value::Map(m)
}

fn command_docs(c: &CommandSpec) -> Value {
    let mut m = vec![
        (Value::str("summary"), Value::str(c.summary)),
        (Value::str("since"), Value::str(c.since)),
        (Value::str("group"), Value::str(c.group)),
        (Value::str("complexity"), Value::str(c.complexity)),
    ];
    if !c.doc_flags.is_empty() {
        m.push((
            Value::str("doc_flags"),
            Value::Array(
                c.doc_flags
                    .iter()
                    .map(|f| Value::Simple(f.to_string()))
                    .collect(),
            ),
        ));
    }
    if !c.deprecated_since.is_empty() {
        m.push((
            Value::str("deprecated_since"),
            Value::str(c.deprecated_since),
        ));
    }
    if !c.replaced_by.is_empty() {
        m.push((Value::str("replaced_by"), Value::str(c.replaced_by)));
    }
    if !c.history.is_empty() {
        m.push((
            Value::str("history"),
            Value::Array(
                c.history
                    .iter()
                    .map(|(v, d)| Value::Array(vec![Value::str(v), Value::str(d)]))
                    .collect(),
            ),
        ));
    }
    if !c.args.is_empty() {
        m.push((
            Value::str("arguments"),
            Value::Array(c.args.iter().map(arg_doc).collect()),
        ));
    }
    let subs: Vec<(Value, Value)> = implemented()
        .filter(|s| s.container == c.name)
        .map(|s| (Value::str(s.name), command_docs(s)))
        .collect();
    if !subs.is_empty() {
        m.push((Value::str("subcommands"), Value::Map(subs)));
    }
    Value::Map(m)
}

fn command(a: &Admin) -> Response {
    let args = &a.args;
    let tops = || implemented().filter(|c| c.container.is_empty());
    match a.sub().as_str() {
        "" => Response::Value(Value::Array(tops().map(command_info).collect())),
        "COUNT" => Response::Integer(*crate::commands::COMMAND_COUNT),
        "INFO" => {
            if args.len() == 1 {
                return Response::Value(Value::Array(tops().map(command_info).collect()));
            }
            Response::Value(Value::Array(
                args[1..]
                    .iter()
                    .map(|n| find_spec(n).map_or(Value::NilArray, command_info))
                    .collect(),
            ))
        }
        "DOCS" => {
            let pick: Vec<&CommandSpec> = if args.len() == 1 {
                tops().collect()
            } else {
                args[1..].iter().filter_map(|n| find_spec(n)).collect()
            };
            Response::Value(Value::Map(
                pick.into_iter()
                    .map(|c| (Value::str(c.name), command_docs(c)))
                    .collect(),
            ))
        }
        "LIST" => {
            let all: Vec<&CommandSpec> = implemented().collect();
            let names: Vec<&CommandSpec> = match &args[1..] {
                [] => all,
                [f, kind, v] if f.eq_ignore_ascii_case(b"FILTERBY") => {
                    let v = String::from_utf8_lossy(v).to_ascii_lowercase();
                    match String::from_utf8_lossy(kind).to_ascii_uppercase().as_str() {
                        "MODULE" => vec![],
                        "ACLCAT" => all
                            .into_iter()
                            .filter(|c| acl_categories(c).contains(&v.as_str()))
                            .collect(),
                        "PATTERN" => all
                            .into_iter()
                            .filter(|c| crate::pubsub::glob_match(v.as_bytes(), c.name.as_bytes()))
                            .collect(),
                        _ => return syntax(),
                    }
                }
                _ => return syntax(),
            };
            Response::Value(Value::Array(
                names.into_iter().map(|c| Value::str(c.name)).collect(),
            ))
        }
        "GETKEYS" | "GETKEYSANDFLAGS" => {
            let with_flags = a.sub() == "GETKEYSANDFLAGS";
            match get_keys(&args[1..]) {
                Err(e) => err(e),
                Ok(keys) => Response::Value(Value::Array(
                    keys.into_iter()
                        .map(|(k, flags)| {
                            if with_flags {
                                Value::Array(vec![
                                    Value::Bulk(k),
                                    Value::Array(
                                        flags
                                            .into_iter()
                                            .map(|f| Value::Simple(f.to_string()))
                                            .collect(),
                                    ),
                                ])
                            } else {
                                Value::Bulk(k)
                            }
                        })
                        .collect(),
                )),
            }
        }
        "HELP" => help(&[
            "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "(no subcommand)",
            "    Return details about all Redis commands.",
            "COUNT",
            "    Return the total number of commands in this Redis server.",
            "LIST",
            "    Return a list of all commands in this Redis server.",
            "INFO [<command-name> ...]",
            "    Return details about multiple Redis commands.",
            "DOCS [<command-name> ...]",
            "    Return documentation details about multiple Redis commands.",
            "GETKEYS <full-command>",
            "    Return the keys from a full Redis command.",
            "GETKEYSANDFLAGS <full-command>",
            "    Return the keys and the access flags from a full Redis command.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand '{other}'. Try COMMAND HELP."
        )),
    }
}

/// The keys a full command line names, by Redis's key-spec walk.
/// Commands Redis gives a native key-extraction function (`get_keys_function`
/// in its command table), used where the key specs cannot answer.
const NATIVE_GETKEYS: &[&str] = &[
    "bitfield",
    "blmpop",
    "bzmpop",
    "eval",
    "eval_ro",
    "evalsha",
    "evalsha_ro",
    "fcall",
    "fcall_ro",
    "georadius",
    "georadiusbymember",
    "lmpop",
    "migrate",
    "set",
    "sintercard",
    "sort",
    "sort_ro",
    "xread",
    "xreadgroup",
    "zdiff",
    "zdiffstore",
    "zinter",
    "zintercard",
    "zinterstore",
    "zmpop",
    "zunion",
    "zunionstore",
];

/// Each key a command line names, with its key-spec flags.
pub(crate) type KeysWithFlags = Vec<(Vec<u8>, Vec<&'static str>)>;

/// `COMMAND GETKEYS`'s answer for one command line: each key with its
/// flags. Redis's `getKeysSubcommandImpl` over `getKeysFromCommandWithSpecs`:
/// the key specs when they are reliable, the command's native function when a
/// spec cannot be followed (unknown, incomplete) or its flags vary with the
/// arguments.
pub(crate) fn get_keys(argv: &[Vec<u8>]) -> Result<KeysWithFlags, String> {
    let Some(name) = argv.first() else {
        return Err("ERR Invalid command specified".into());
    };
    let mut spec = find_spec(name).ok_or("ERR Invalid command specified")?;
    if argv.len() >= 2 {
        let full = format!(
            "{}|{}",
            spec.name,
            String::from_utf8_lossy(&argv[1]).to_ascii_lowercase()
        );
        if let Some(s) = BY_NAME.get(full.as_str()) {
            spec = s;
        }
    }
    let native = NATIVE_GETKEYS.contains(&spec.name);
    let has_flag = |k: &KeySpec, f: &str| k.flags.iter().any(|x| x.eq_ignore_ascii_case(f));
    let has_keyspec = spec.key_specs.iter().any(|k| !has_flag(k, "NOT_KEY"));
    if !native && !has_keyspec {
        return Err("ERR The command has no key arguments".into());
    }
    if !crate::commands::arity_ok(spec.arity, argv.len()) {
        return Err("ERR Invalid number of arguments specified for command".into());
    }
    let varflags = spec.key_specs.iter().any(|k| has_flag(k, "VARIABLE_FLAGS"));
    let from_specs = if has_keyspec && !varflags {
        keys_from_specs(spec, argv)
    } else {
        None
    };
    let positions = match from_specs {
        Some(p) => p,
        None if native => native_keys(spec.name, argv),
        None => Vec::new(),
    };
    if positions.is_empty() {
        if spec.flags.contains(&"no_mandatory_keys") {
            return Ok(Vec::new());
        }
        return Err("ERR Invalid arguments specified for command".into());
    }
    Ok(positions
        .into_iter()
        .map(|(i, flags)| {
            (
                argv[i].clone(),
                flags.iter().map(|f| key_flag_name(f)).collect(),
            )
        })
        .collect())
}

/// `addReplyFlagsForKeyArgs`: the access kind in capitals, the rest not.
fn key_flag_name(f: &str) -> &'static str {
    match f.to_ascii_uppercase().as_str() {
        "RO" => "RO",
        "RW" => "RW",
        "OW" => "OW",
        "RM" => "RM",
        "ACCESS" => "access",
        "UPDATE" => "update",
        "INSERT" => "insert",
        "DELETE" => "delete",
        "NOT_KEY" => "not_key",
        "INCOMPLETE" => "incomplete",
        "VARIABLE_FLAGS" => "variable_flags",
        _ => "unknown",
    }
}

/// `getKeysUsingKeySpecs`: `None` when a spec cannot be followed — unknown,
/// incomplete, a bad key count, positions past the arguments.
fn keys_from_specs(
    spec: &CommandSpec,
    argv: &[Vec<u8>],
) -> Option<Vec<(usize, Vec<&'static str>)>> {
    let argc = argv.len() as i64;
    let mut out = Vec::new();
    for k in spec.key_specs {
        if k.flags.iter().any(|f| f.eq_ignore_ascii_case("NOT_KEY")) {
            continue;
        }
        let first = match &k.begin {
            Begin::Index(p) => *p as i64,
            Begin::Keyword(kw, from) => {
                let start = if *from > 0 {
                    *from as i64
                } else {
                    argc + *from as i64
                };
                let end = if *from > 0 { argc - 1 } else { 1 };
                let mut found = None;
                let mut i = start;
                while i != end {
                    if i >= argc || i < 1 {
                        break;
                    }
                    if argv[i as usize].eq_ignore_ascii_case(kw.as_bytes()) {
                        found = Some(i + 1);
                        break;
                    }
                    i = if start <= end { i + 1 } else { i - 1 };
                }
                match found {
                    Some(f) => f,
                    None => continue,
                }
            }
            Begin::Unknown => return None,
        };
        let (first, last, step) = match &k.find {
            Find::Range {
                lastkey,
                step,
                limit,
            } => {
                let last = if *lastkey >= 0 {
                    first + *lastkey as i64
                } else if *limit == 0 {
                    argc + *lastkey as i64
                } else {
                    first + ((argc - first) / *limit as i64 + *lastkey as i64)
                };
                (first, last, *step as i64)
            }
            Find::Keynum {
                keynumidx,
                firstkey,
                step,
            } => {
                if *keynumidx as i64 >= argc {
                    return None;
                }
                let n = argv
                    .get((first + *keynumidx as i64) as usize)
                    .and_then(|v| int(v))
                    .filter(|n| *n >= 0)?;
                let f = first + *firstkey as i64;
                (f, f + n - 1, *step as i64)
            }
            Find::Unknown => return None,
        };
        if last >= argc || last < first || first >= argc {
            return None;
        }
        let mut i = first;
        while i <= last {
            out.push((i as usize, k.flags.to_vec()));
            i += step.max(1);
        }
        if k.flags.iter().any(|f| f.eq_ignore_ascii_case("INCOMPLETE")) {
            return None;
        }
    }
    Some(out)
}

/// The native functions whose answer differs from the specs': `sortGetKeys`,
/// `sortROGetKeys`, `setGetKeys`, `bitfieldGetKeys`. Every other one finds no
/// keys exactly where its specs fail.
fn native_keys(name: &str, argv: &[Vec<u8>]) -> Vec<(usize, Vec<&'static str>)> {
    let is = |i: usize, w: &str| {
        argv.get(i)
            .is_some_and(|a| a.eq_ignore_ascii_case(w.as_bytes()))
    };
    match name {
        "sort_ro" => vec![(1, vec!["RO", "ACCESS"])],
        "sort" => {
            let mut out = vec![(1, vec!["RO", "ACCESS"])];
            let mut store = None;
            let mut i = 2;
            while i < argv.len() {
                if is(i, "limit") {
                    i += 2;
                } else if is(i, "get") || is(i, "by") {
                    i += 1;
                } else if is(i, "store") && i + 1 < argv.len() {
                    store = Some(i + 1);
                }
                i += 1;
            }
            out.extend(store.map(|p| (p, vec!["OW", "UPDATE"])));
            out
        }
        "set" => {
            let get = (3..argv.len()).any(|i| is(i, "get"));
            if get {
                vec![(1, vec!["RW", "ACCESS", "UPDATE"])]
            } else {
                vec![(1, vec!["OW", "UPDATE"])]
            }
        }
        "bitfield" => {
            let mut readonly = true;
            let mut i = 2;
            while i < argv.len() {
                let rem = argv.len() - i - 1;
                if is(i, "get") && rem >= 2 {
                    i += 2;
                } else if (is(i, "set") || is(i, "incrby")) && rem >= 3 {
                    readonly = false;
                    break;
                } else if is(i, "overflow") && rem >= 1 {
                    i += 1;
                } else {
                    readonly = false;
                    break;
                }
                i += 1;
            }
            if readonly {
                vec![(1, vec!["RO", "ACCESS"])]
            } else {
                vec![(1, vec!["RW", "ACCESS", "UPDATE"])]
            }
        }
        _ => Vec::new(),
    }
}

// ───────────────────────────────── SLOWLOG ──────────────────────────────────

fn slowlog(a: &Admin) -> Response {
    let max_len = crate::config::get_i64("slowlog-max-len").max(0) as usize;
    match a.sub().as_str() {
        "GET" => {
            let count = match a.args.get(1) {
                None => 10,
                Some(n) => match int(n) {
                    Some(-1) => usize::MAX,
                    Some(n) if n >= 0 => n as usize,
                    _ => return err("ERR count should be greater than or equal to -1"),
                },
            };
            Response::Value(Value::Array(
                server::slowlog_get(count, max_len)
                    .into_iter()
                    .map(|e| {
                        Value::Array(vec![
                            Value::Int(e.id as i64),
                            Value::Int(e.time),
                            Value::Int(e.duration_us),
                            Value::Array(e.args.into_iter().map(Value::Bulk).collect()),
                            Value::str(&e.addr),
                            Value::Bulk(e.name),
                        ])
                    })
                    .collect(),
            ))
        }
        "LEN" => Response::Integer(server::slowlog_len(max_len) as i64),
        "RESET" => {
            server::slowlog_reset();
            Response::Ok
        }
        "HELP" => help(&[
            "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "GET [<count>]",
            "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
            "    Entries are made of:",
            "    id, timestamp, time in microseconds, arguments array, client IP and port,",
            "    client name",
            "LEN",
            "    Return the length of the slowlog.",
            "RESET",
            "    Reset the slowlog.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand '{other}'. Try SLOWLOG HELP."
        )),
    }
}

// ───────────────────────────────── LATENCY ──────────────────────────────────

/// Latency spikes are not sampled here — pg_redis has no event loop to
/// measure — so the monitor always reports none; the histogram reuses the
/// per-command latency `INFO latencystats` keeps.
fn latency(a: &Admin) -> Response {
    match a.sub().as_str() {
        "LATEST" => Response::Value(Value::Array(vec![])),
        "HISTORY" => Response::Value(Value::Array(vec![])),
        "RESET" => Response::Integer(0),
        "GRAPH" => err(format!(
            "ERR No samples available for event '{}'",
            a.args
                .get(1)
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default()
        )),
        "DOCTOR" => bulk(
            "Dave, no latency spike was observed during the lifetime of this Redis instance, not in the slightest bit. I honestly think you ought to sleep tonight.\n",
        ),
        "HISTOGRAM" => {
            use std::sync::atomic::Ordering::Relaxed;
            let Some(ctl) = server::ctl() else {
                return Response::Value(Value::Map(vec![]));
            };
            let wanted: Vec<String> = a.args[1..]
                .iter()
                .map(|n| String::from_utf8_lossy(n).to_ascii_lowercase())
                .collect();
            let mut out = Vec::new();
            for (i, spec) in crate::redis_tables::COMMANDS.iter().enumerate() {
                let Some(s) = ctl.cmdstats.get(i) else { break };
                let calls = s.calls.load(Relaxed);
                if calls == 0 || (!wanted.is_empty() && !wanted.iter().any(|w| w == spec.name)) {
                    continue;
                }
                let buckets: Vec<(Value, Value)> = s
                    .hist
                    .iter()
                    .enumerate()
                    .scan(0u64, |cum, (b, h)| {
                        let n = h.load(Relaxed);
                        *cum += n;
                        Some((b, n, *cum))
                    })
                    .filter(|(_, n, _)| *n > 0)
                    .map(|(b, _, cum)| {
                        (
                            Value::Int(2f64.powf(b as f64 / 2.0) as i64),
                            Value::Int(cum as i64),
                        )
                    })
                    .collect();
                out.push((
                    Value::str(spec.name),
                    Value::Map(vec![
                        (Value::str("calls"), Value::Int(calls as i64)),
                        (Value::str("histogram_usec"), Value::Map(buckets)),
                    ]),
                ));
            }
            Response::Value(Value::Map(out))
        }
        "HELP" => help(&[
            "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "DOCTOR",
            "    Return a human readable latency analysis report.",
            "GRAPH <event>",
            "    Return an ASCII latency graph for the <event> class.",
            "HISTORY <event>",
            "    Return time-latency samples for the <event> class.",
            "LATEST",
            "    Return the latest latency samples for all events.",
            "RESET [<event> ...]",
            "    Reset latency data of one or more <event> classes.",
            "HISTOGRAM [COMMAND ...]",
            "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand '{other}'. Try LATENCY HELP."
        )),
    }
}

// ─────────────────────────────────── ACL ────────────────────────────────────

/// One user, `default`, with every permission — the only user pg_redis has.
/// `redis.password` is its password.
fn acl(a: &Admin) -> Response {
    let user_line = "user default on nopass sanitize-payload ~* &* +@all";
    match a.sub().as_str() {
        "WHOAMI" => bulk("default"),
        "USERS" => Response::Value(Value::Array(vec![Value::str("default")])),
        "LIST" => Response::Value(Value::Array(vec![Value::str(user_line)])),
        "CAT" => match a.args.get(1) {
            None => Response::Value(Value::Array(
                ACL_ORDER.iter().map(|c| Value::str(c)).collect(),
            )),
            Some(cat) => {
                let cat = String::from_utf8_lossy(cat).to_ascii_lowercase();
                if !ACL_ORDER.contains(&cat.as_str()) {
                    return err(format!("ERR Unknown category '{cat}'"));
                }
                Response::Value(Value::Array(
                    implemented()
                        .filter(|c| acl_categories(c).contains(&cat.as_str()))
                        .map(|c| Value::str(c.name))
                        .collect(),
                ))
            }
        },
        "GETUSER" => {
            if !a.args[1].eq_ignore_ascii_case(b"default") {
                return Response::Null;
            }
            Response::Value(Value::Map(vec![
                (
                    Value::str("flags"),
                    Value::Array(vec![
                        Value::str("on"),
                        Value::str("nopass"),
                        Value::str("sanitize-payload"),
                    ]),
                ),
                (Value::str("passwords"), Value::Array(vec![])),
                (Value::str("commands"), Value::str("+@all")),
                (Value::str("keys"), Value::str("~*")),
                (Value::str("channels"), Value::str("&*")),
                (Value::str("selectors"), Value::Array(vec![])),
            ]))
        }
        "LOG" => match a.args.get(1) {
            Some(r) if r.eq_ignore_ascii_case(b"RESET") => Response::Ok,
            _ => Response::Value(Value::Array(vec![])),
        },
        "GENPASS" => {
            let bits = a
                .args
                .get(1)
                .and_then(|v| int(v))
                .unwrap_or(256)
                .clamp(1, 4096) as usize;
            let hex: String = (0..bits.div_ceil(4))
                .map(|_| format!("{:x}", crate::memdb::fast_random() & 0xf))
                .collect();
            bulk(hex)
        }
        "DRYRUN" => Response::Ok,
        "SETUSER" | "DELUSER" | "LOAD" | "SAVE" => {
            err("ERR pg_redis has a single user, default, configured with redis.password")
        }
        "HELP" => help(&[
            "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CAT [<category>]",
            "    List all commands that belong to <category>, or all command categories",
            "    when no category is specified.",
            "GETUSER <username>",
            "    Get the user's details.",
            "LIST",
            "    Show users details in config file format.",
            "USERS",
            "    List all the registered usernames.",
            "WHOAMI",
            "    Return the current connection username.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!("ERR unknown subcommand '{other}'. Try ACL HELP.")),
    }
}

// ───────────────────────────────── DEBUG ────────────────────────────────────

fn debug(a: &Admin, _conn: &mut Conn, _data: &mut Data) -> Response {
    match a.sub().as_str() {
        "SLEEP" => {
            let secs: f64 = a
                .args
                .get(1)
                .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
                .unwrap_or(0.0);
            std::thread::sleep(std::time::Duration::from_secs_f64(secs.max(0.0)));
            Response::Ok
        }
        "SET-ACTIVE-EXPIRE" => {
            let on = a.args.get(1).and_then(|v| int(v)).unwrap_or(1) != 0;
            if let Some(c) = server::ctl() {
                c.active_expire
                    .store(on as u32, std::sync::atomic::Ordering::Relaxed);
            }
            Response::Ok
        }
        // Reloading is a no-op: nothing here lives only in a dump.
        "RELOAD"
        | "JMAP"
        | "CHANGE-REPL-ID"
        | "LOG"
        | "PAUSE-CRON"
        | "REPLYBUFFER"
        | "QUICKLIST-PACKED-THRESHOLD"
        | "LISTPACK"
        | "QUICKLIST"
        | "SET-SKIP-CHECKSUM-VALIDATION"
        | "STRINGMATCH-LEN"
        | "DICT-RESIZING" => Response::Ok,
        "PROTOCOL" => match a
            .args
            .get(1)
            .map(|v| String::from_utf8_lossy(v).to_ascii_lowercase())
            .as_deref()
        {
            Some("string") => bulk("Hello World"),
            Some("integer") => Response::Integer(12345),
            Some("double") => bulk("3.141"),
            Some("bignum") => bulk("1234567999999999999999999999999999999"),
            Some("null") => Response::Null,
            Some("array") => Response::Value(Value::Array((0..3).map(Value::Int).collect())),
            Some("set") => Response::Value(Value::Array((0..3).map(Value::Int).collect())),
            Some("map") => Response::Value(Value::Array(
                (0..3)
                    .flat_map(|i| [Value::Int(i), Value::Int(i % 2)])
                    .collect(),
            )),
            Some("true") => Response::Integer(1),
            Some("false") => Response::Integer(0),
            Some("verbatim") => bulk("This is a verbatim\nstring"),
            _ => err(
                "ERR Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false",
            ),
        },
        "HELP" => help(&[
            "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "SLEEP <seconds>",
            "    Stop the server for <seconds>. Decimals allowed.",
            "SET-ACTIVE-EXPIRE <0|1>",
            "    Setting it to 0 disables expiring keys in background.",
            "RELOAD",
            "    Accepted; pg_redis keeps no dump to reload from.",
            "PROTOCOL <type>",
            "    Reply with a test value of the specified type.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand or wrong number of arguments for '{}'. Try DEBUG HELP.",
            other.to_ascii_lowercase()
        )),
    }
}

// ───────────────────────────────── MEMORY ───────────────────────────────────

/// `MEMORY` beyond `USAGE` (which needs the key, and is a data command).
fn memory(a: &Admin) -> Response {
    match a.sub().as_str() {
        "DOCTOR" => bulk(
            "Hi Sam, I can't find any memory issue in your instance. I can only account for what occurs on this base.\n",
        ),
        "MALLOC-STATS" => bulk("Stats not supported for the current allocator\n"),
        "PURGE" => Response::Ok,
        "STATS" => Response::Value(Value::Map(vec![
            (Value::str("peak.allocated"), Value::Int(0)),
            (Value::str("total.allocated"), Value::Int(0)),
            (Value::str("startup.allocated"), Value::Int(0)),
            (Value::str("keys.count"), Value::Int(0)),
            (Value::str("dataset.bytes"), Value::Int(0)),
        ])),
        "HELP" => help(&[
            "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "DOCTOR",
            "    Return memory problems reports.",
            "MALLOC-STATS",
            "    Return internal statistics report from the memory allocator.",
            "PURGE",
            "    Attempt to purge dirty pages for reclamation by the allocator.",
            "STATS",
            "    Return information about the memory usage of the server.",
            "USAGE <key> [SAMPLES <count>]",
            "    Return memory in bytes used by <key> and its value.",
            "HELP",
            "    Print this help.",
        ]),
        other => err(format!(
            "ERR unknown subcommand '{other}'. Try MEMORY HELP."
        )),
    }
}

/// Redis's `bytesToHuman`: two decimals in the largest unit that fits.
fn human_bytes(n: usize) -> String {
    const UNITS: [&str; 5] = ["K", "M", "G", "T", "P"];
    if n < 1024 {
        return format!("{n}B");
    }
    let mut v = n as f64 / 1024.0;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.2}{}", UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str) -> &'static CommandSpec {
        crate::redis_tables::COMMANDS
            .iter()
            .find(|c| c.name == name)
            .unwrap()
    }

    #[test]
    fn legacy_ranges_match_redis() {
        // `COMMAND INFO getex zunionstore client|kill` on Redis 7.2.
        assert_eq!(legacy_range(spec("getex")), (1, 1, 1, false));
        assert_eq!(legacy_range(spec("zunionstore")), (1, 1, 1, true));
        assert_eq!(legacy_range(spec("client|kill")), (0, 0, 0, false));
        assert_eq!(legacy_range(spec("mset")), (1, -1, 2, false));
    }

    #[test]
    fn acl_categories_match_redis() {
        assert_eq!(
            acl_categories(spec("getex")),
            vec!["write", "string", "fast"]
        );
        assert_eq!(
            acl_categories(spec("zunionstore")),
            vec!["write", "sortedset", "slow"]
        );
        assert_eq!(
            acl_categories(spec("client|kill")),
            vec!["admin", "slow", "dangerous", "connection"]
        );
    }

    #[test]
    fn keys_come_from_key_specs() {
        let a = |v: &[&str]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        let keys =
            |v: &[&str]| get_keys(&a(v)).map(|k| k.into_iter().map(|(k, _)| k).collect::<Vec<_>>());
        assert_eq!(keys(&["get", "k"]).unwrap(), vec![b"k".to_vec()]);
        assert_eq!(
            keys(&["mset", "a", "1", "b", "2"]).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(
            keys(&["zunionstore", "d", "2", "a", "b", "WEIGHTS", "1", "2"]).unwrap(),
            vec![b"d".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert!(keys(&["ping"]).unwrap_err().contains("no key arguments"));
        assert!(
            keys(&["get"])
                .unwrap_err()
                .contains("Invalid number of arguments")
        );
        assert!(
            keys(&["nope", "x"])
                .unwrap_err()
                .contains("Invalid command")
        );
    }
}
