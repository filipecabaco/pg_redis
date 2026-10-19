//! Server-wide state that every worker shares: the connected clients, the
//! command and error counters behind `INFO`, `CONFIG SET` values, the slow
//! log, and the pause and monitor flags.
//!
//! In shared memory for the same reason as WATCH's counters: the listener
//! uses `SO_REUSEPORT`, so `CLIENT LIST` is served by whichever worker took
//! the connection and must still see every other worker's clients.
//!
//! Everything is fixed-size and lock-free on the per-command path. Strings a
//! single owner writes and anyone reads — a client's name, its last command,
//! a config value — sit behind a sequence counter: the writer bumps it to odd,
//! writes, bumps it to even, and a reader retries while it is odd or moved.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

/// Clients across all workers. Past this, a connection still works but is
/// absent from `CLIENT LIST` and cannot be killed by id.
pub const MAX_CLIENTS: usize = 2048;
/// Commands with their own `INFO commandstats` line: one per entry of the
/// generated command table, rounded up.
pub const CMD_SLOTS: usize = 512;
/// Distinct error codes counted for `INFO errorstats` — Redis caps it at 128.
pub const ERR_SLOTS: usize = 128;
pub const CONFIG_SLOTS: usize = 256;
pub const SLOWLOG_CAP: usize = 256;
pub const SLOWLOG_ARGS: usize = 32;
pub const SLOWLOG_ARG_LEN: usize = 128;

const NAME_LEN: usize = 64;
const ADDR_LEN: usize = 64;
const CMD_LEN: usize = 48;
const CONFIG_NAME_LEN: usize = 64;
const CONFIG_VALUE_LEN: usize = 512;
const TRACK_PREFIXES_LEN: usize = 512;

/// A string field with its length, written only under its owner's seqlock.
#[repr(C)]
pub struct Field<const N: usize> {
    len: AtomicU32,
    bytes: [std::sync::atomic::AtomicU8; N],
}

impl<const N: usize> Field<N> {
    fn store(&self, v: &[u8]) {
        let n = v.len().min(N);
        for (i, &b) in v[..n].iter().enumerate() {
            self.bytes[i].store(b, Ordering::Relaxed);
        }
        self.len.store(n as u32, Ordering::Relaxed);
    }

    fn load(&self) -> Vec<u8> {
        let n = (self.len.load(Ordering::Relaxed) as usize).min(N);
        (0..n)
            .map(|i| self.bytes[i].load(Ordering::Relaxed))
            .collect()
    }
}

/// Read `f` consistently with respect to `seq`'s writer.
fn read_consistent<T>(seq: &AtomicU32, f: impl Fn() -> T) -> T {
    loop {
        let before = seq.load(Ordering::Acquire);
        if before & 1 == 1 {
            std::hint::spin_loop();
            continue;
        }
        let v = f();
        std::sync::atomic::fence(Ordering::Acquire);
        if seq.load(Ordering::Relaxed) == before {
            return v;
        }
    }
}

fn write_consistent(seq: &AtomicU32, f: impl FnOnce()) {
    seq.fetch_add(1, Ordering::AcqRel);
    std::sync::atomic::fence(Ordering::Release);
    f();
    seq.fetch_add(1, Ordering::Release);
}

/// Client flag bits, reported as `CLIENT LIST`'s `flags=` letters.
pub mod flag {
    pub const PUBSUB: u32 = 1 << 0; // P
    pub const MULTI: u32 = 1 << 1; // x
    pub const MONITOR: u32 = 1 << 2; // O
    pub const NO_EVICT: u32 = 1 << 3; // e
    pub const NO_TOUCH: u32 = 1 << 4; // T
    pub const CLOSE_ASAP: u32 = 1 << 5; // A
    pub const BLOCKED: u32 = 1 << 6; // b
    pub const TRACKING: u32 = 1 << 7; // t
}

#[repr(C)]
pub struct ClientSlot {
    /// 0 free, 1 taken.
    state: AtomicU32,
    seq: AtomicU32,
    pub id: AtomicU64,
    pub created_ms: AtomicI64,
    pub last_ms: AtomicI64,
    pub db: AtomicU32,
    pub flags: AtomicU32,
    /// Set by `CLIENT KILL` on any worker; the owner closes when it sees it.
    pub kill: AtomicU32,
    pub resp: AtomicU32,
    pub sub: AtomicU32,
    pub psub: AtomicU32,
    pub multi: AtomicI32,
    pub fd: AtomicI32,
    /// `CLIENT UNBLOCK`: 1 to end the wait as a timeout, 2 as an error.
    pub unblock: AtomicU32,
    /// The bytes of the command being run, as `CLIENT LIST`'s `qbuf`.
    pub qbuf: AtomicU32,
    /// `CLIENT TRACKING`: the `track::` mode bits, 0 when off.
    pub tracking: AtomicU32,
    /// Where this client's invalidations go; 0 for itself.
    pub track_redirect: AtomicU64,
    /// The negotiated protocol, which decides how an invalidation is framed.
    pub proto: AtomicU32,
    /// 1 while the connection hears its own tracking channel.
    pub track_listening: AtomicU32,
    /// A pub/sub slot another connection subscribed to this one's tracking
    /// channel for it to take over, or `HANDOFF_NONE` / `HANDOFF_CLOSED`.
    pub track_handoff: AtomicI32,
    /// The serial of the ACL user the client is authenticated as.
    pub user_serial: AtomicU64,
    /// Set when the client's user lost channel permissions: a subscriber
    /// checks its subscriptions and closes if one is no longer allowed, then
    /// clears it.
    pub acl_recheck: AtomicU32,
    /// `BCAST` prefixes, NUL-separated, behind `seq`.
    track_prefixes: Field<TRACK_PREFIXES_LEN>,
    name: Field<NAME_LEN>,
    addr: Field<ADDR_LEN>,
    laddr: Field<ADDR_LEN>,
    lib_name: Field<NAME_LEN>,
    lib_ver: Field<NAME_LEN>,
    cmd: Field<CMD_LEN>,
}

/// Latency histogram buckets: bucket `b` holds durations up to 2^(b/2) µs,
/// so two buckets per power of two from 1 µs to about 2^31 µs.
pub const HIST_BUCKETS: usize = 64;

#[repr(C)]
pub struct CmdStat {
    pub calls: AtomicU64,
    pub usec: AtomicU64,
    pub rejected: AtomicU64,
    pub failed: AtomicU64,
    pub hist: [AtomicU64; HIST_BUCKETS],
}

fn bucket_of(usec: u64) -> usize {
    if usec <= 1 {
        return 0;
    }
    // ceil(2 * log2(usec)), clamped.
    let b = (2.0 * (usec as f64).log2()).ceil() as usize;
    b.min(HIST_BUCKETS - 1)
}

/// The upper bound of the bucket holding the `p`-th percentile, in µs.
pub fn percentile(s: &CmdStat, p: f64) -> Option<f64> {
    let counts: Vec<u64> = s.hist.iter().map(|h| h.load(Ordering::Relaxed)).collect();
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }
    let want = ((p / 100.0) * total as f64).ceil().max(1.0) as u64;
    let mut cum = 0;
    for (b, c) in counts.iter().enumerate() {
        cum += c;
        if cum >= want {
            return Some(2f64.powf(b as f64 / 2.0));
        }
    }
    None
}

#[repr(C)]
pub struct ErrStat {
    used: AtomicU32,
    name: Field<32>,
    pub count: AtomicU64,
}

#[repr(C)]
pub struct ConfigSlot {
    used: AtomicU32,
    name: Field<CONFIG_NAME_LEN>,
    value: Field<CONFIG_VALUE_LEN>,
}

#[repr(C)]
pub struct SlowEntry {
    seq: AtomicU32,
    pub id: AtomicU64,
    pub time: AtomicI64,
    pub duration_us: AtomicI64,
    argc: AtomicU32,
    /// The real argument count, which `argc` truncates to SLOWLOG_ARGS.
    total_args: AtomicU32,
    args: [Field<SLOWLOG_ARG_LEN>; SLOWLOG_ARGS],
    arg_lens: [AtomicU64; SLOWLOG_ARGS],
    addr: Field<ADDR_LEN>,
    name: Field<NAME_LEN>,
}

#[repr(C)]
pub struct ServerCtl {
    initialised: AtomicU32,
    /// `INFO`'s `run_id`: random, new at every server start, as in Redis.
    run_id: [AtomicU64; 3],
    pub next_client_id: AtomicU64,
    pub started_ms: AtomicI64,
    pub stat_reset_ms: AtomicI64,
    pub total_connections: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub total_commands: AtomicU64,
    pub total_error_replies: AtomicU64,
    pub keyspace_hits: AtomicU64,
    pub keyspace_misses: AtomicU64,
    pub expired_keys: AtomicU64,
    /// INFO's `evicted_keys`.
    pub evicted_keys: AtomicU64,
    /// When the arena went over the limit and stayed there (unix ms, 0 when
    /// under), and the milliseconds spent over it before: Redis's
    /// `current_eviction_exceeded_time` and `total_eviction_exceeded_time`.
    pub eviction_exceeded_since: AtomicI64,
    pub eviction_exceeded_total: AtomicU64,
    /// Clients in `CLIENT NO-TOUCH` mode: while 0, no command looks its
    /// client up to ask.
    pub no_touch_clients: AtomicU32,
    pub net_input: AtomicU64,
    pub net_output: AtomicU64,
    pub cmdstats: [CmdStat; CMD_SLOTS],
    errstat_lock: AtomicU32,
    pub errstats: [ErrStat; ERR_SLOTS],
    config_lock: AtomicU32,
    config_seq: AtomicU32,
    config: [ConfigSlot; CONFIG_SLOTS],
    pub clients: [ClientSlot; MAX_CLIENTS],
    slowlog_lock: AtomicU32,
    pub slowlog_next_id: AtomicU64,
    slowlog_len: AtomicU32,
    slowlog: [SlowEntry; SLOWLOG_CAP],
    /// `CLIENT PAUSE` deadline in unix ms, 0 when not paused.
    pub pause_until_ms: AtomicI64,
    pub pause_writes_only: AtomicU32,
    pub monitors: AtomicU32,
    pub active_expire: AtomicU32,
    pub lastsave: AtomicI64,
    next_ticket: AtomicU64,
    waiters: [Waiter; MAX_CLIENTS],
    /// Waiters registered right now, so a write with nobody blocked skips
    /// the registry entirely.
    active_waiters: AtomicU32,
    /// Bumped whenever `SCRIPT FLUSH` empties the script cache, so every
    /// worker drops the scripts it compiled.
    pub scripts_gen: AtomicU64,
    /// Bumped whenever the function libraries change, so every worker
    /// reloads them before its next `FCALL`.
    pub functions_gen: AtomicU64,
    scripts: [ScriptRun; SCRIPT_SLOTS],
    /// What `FUNCTION STATS` reports, kept by whichever dispatcher last
    /// loaded the libraries.
    functions_libs: AtomicI64,
    functions_count: AtomicI64,
    /// Rows in `redis.scripts`, for `INFO memory`.
    cached_scripts: AtomicI64,
    /// Bumped by `FLUSHDB` / `FLUSHALL`, per database: a WATCH on a key that
    /// existed is broken by a flush, one on a missing key is not.
    flush_epochs: [AtomicU64; 16],
    /// `notify-keyspace-events`, parsed: read before every command.
    notify_flags: AtomicU32,
    /// The `*-max-listpack-*` and `set-max-intset-entries` thresholds, read
    /// on every collection write (`memdb::encoding::Limits`). Written
    /// together under the config lock, and `enc_limits_set` is 0 until
    /// a `CONFIG SET` names one, which means Redis's defaults.
    enc_limits: [AtomicI64; crate::memdb::encoding::LIMIT_COUNT],
    enc_limits_set: AtomicU32,
    /// Clients with tracking on: when 0, no write looks at the table.
    pub tracking_clients: AtomicU32,
    /// Each database's storage policy and memory slot (`storage.rs`).
    pub storage: crate::storage::PolicyTable,
}

/// Keys one blocked client is recorded under; past this its later keys still
/// work, they just do not take part in the oldest-first ordering.
pub const WAITER_KEYS: usize = 16;

#[repr(C)]
pub struct Waiter {
    /// 0 when free; otherwise the waiter's place in line.
    ticket: AtomicU64,
    nkeys: AtomicU32,
    keys: [AtomicU64; WAITER_KEYS],
    /// Per key, the write counter the waiter last acted on: a write at or
    /// below it has been tried against.
    seen: [AtomicU64; WAITER_KEYS],
    /// Served, and now waiting for the clients its own write wakes.
    settling: AtomicU32,
    /// Woken by the key's deletion too — a blocked XREADGROUP, whose group
    /// goes with the key (`unblock_on_nokey`).
    nokey: AtomicU32,
}

pub fn server_ctl_size() -> usize {
    std::mem::size_of::<ServerCtl>()
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The shared block, once the postmaster has set it up.
pub fn ctl() -> Option<&'static ServerCtl> {
    crate::server_state()
}

/// First-touch initialisation of the zeroed block, done once whichever
/// process gets there first.
pub fn init(ctl: &ServerCtl) {
    if ctl.initialised.swap(1, Ordering::AcqRel) == 0 {
        let now = now_ms();
        ctl.next_client_id.store(1, Ordering::Relaxed);
        ctl.started_ms.store(now, Ordering::Relaxed);
        ctl.stat_reset_ms.store(now, Ordering::Relaxed);
        ctl.active_expire.store(1, Ordering::Relaxed);
        ctl.lastsave.store(now / 1000, Ordering::Relaxed);
        let mut bytes = [0u8; 24];
        crate::sys::random_bytes(&mut bytes);
        for (word, chunk) in ctl.run_id.iter().zip(bytes.as_chunks::<8>().0) {
            word.store(u64::from_le_bytes(*chunk), Ordering::Relaxed);
        }
    }
}

/// The 40 hex digits `INFO` reports as `run_id`.
pub fn run_id() -> String {
    let Some(ctl) = ctl() else {
        return "0".repeat(40);
    };
    let hex: String = ctl
        .run_id
        .iter()
        .map(|w| format!("{:016x}", w.load(Ordering::Relaxed)))
        .collect();
    hex[..40].to_string()
}

fn spin_lock(l: &AtomicU32) {
    while l
        .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::hint::spin_loop();
    }
}

fn spin_unlock(l: &AtomicU32) {
    l.store(0, Ordering::Release);
}

// ───────────────────────────────── clients ──────────────────────────────────

/// A registered connection. Dropping it frees the slot.
pub struct ClientHandle {
    pub id: u64,
    slot: Option<&'static ClientSlot>,
}

impl ClientHandle {
    /// A handle on a client without its slot — what a script's admin
    /// sub-commands run as. Dropping it frees nothing.
    pub fn detached(id: u64) -> ClientHandle {
        ClientHandle { id, slot: None }
    }

    /// Register a new connection. Every connection gets an id; one past
    /// MAX_CLIENTS simply has no slot.
    pub fn register(addr: &str, laddr: &str, fd: i32) -> ClientHandle {
        let Some(ctl) = ctl() else {
            return ClientHandle { id: 0, slot: None };
        };
        let id = ctl.next_client_id.fetch_add(1, Ordering::Relaxed);
        ctl.total_connections.fetch_add(1, Ordering::Relaxed);
        let slot = ctl.clients.iter().find(|s| {
            s.state
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        });
        if let Some(s) = slot {
            let now = now_ms();
            write_consistent(&s.seq, || {
                s.name.store(b"");
                s.lib_name.store(b"");
                s.lib_ver.store(b"");
                s.cmd.store(b"NULL");
                s.addr.store(addr.as_bytes());
                s.laddr.store(laddr.as_bytes());
            });
            s.created_ms.store(now, Ordering::Relaxed);
            s.last_ms.store(now, Ordering::Relaxed);
            s.flags.store(0, Ordering::Relaxed);
            s.kill.store(0, Ordering::Relaxed);
            s.resp.store(2, Ordering::Relaxed);
            s.sub.store(0, Ordering::Relaxed);
            s.psub.store(0, Ordering::Relaxed);
            s.multi.store(-1, Ordering::Relaxed);
            s.fd.store(fd, Ordering::Relaxed);
            s.unblock.store(0, Ordering::Relaxed);
            s.track_listening.store(0, Ordering::Relaxed);
            s.track_handoff.store(HANDOFF_NONE, Ordering::Relaxed);
            s.user_serial
                .store(crate::acl::DEFAULT_SERIAL, Ordering::Relaxed);
            s.acl_recheck.store(0, Ordering::Relaxed);
            s.id.store(id, Ordering::Release);
        }
        ClientHandle { id, slot }
    }

    pub fn killed(&self) -> bool {
        self.slot
            .is_some_and(|s| s.kill.load(Ordering::Relaxed) != 0)
    }

    /// Close this connection once the command now running has replied.
    pub fn kill(&self) {
        if let Some(s) = self.slot {
            s.kill.store(1, Ordering::Relaxed);
        }
    }

    /// The ACL user the client now runs as.
    pub fn set_user(&self, serial: u64) {
        if let Some(s) = self.slot {
            s.user_serial.store(serial, Ordering::Relaxed);
        }
    }

    /// Whether a channel-permission check was asked for.
    pub fn acl_recheck_asked(&self) -> bool {
        self.slot
            .is_some_and(|s| s.acl_recheck.load(Ordering::Acquire) != 0)
    }

    pub fn acl_recheck_done(&self) {
        if let Some(s) = self.slot {
            s.acl_recheck.store(0, Ordering::Release);
        }
    }

    /// The size of the command now being run; 0 between commands.
    pub fn set_qbuf(&self, bytes: usize) {
        if let Some(s) = self.slot {
            s.qbuf
                .store(bytes.min(u32::MAX as usize) as u32, Ordering::Relaxed);
        }
    }

    /// Called once per command: when it ran, what it was, and where.
    pub fn touch(&self, cmd: &str, db: u8) {
        if let Some(s) = self.slot {
            s.last_ms.store(now_ms(), Ordering::Relaxed);
            s.db.store(db as u32, Ordering::Relaxed);
            write_consistent(&s.seq, || s.cmd.store(cmd.as_bytes()));
        }
    }

    pub fn set_name(&self, name: &[u8]) {
        if let Some(s) = self.slot {
            write_consistent(&s.seq, || s.name.store(name));
        }
    }

    pub fn name(&self) -> Vec<u8> {
        self.slot
            .map(|s| read_consistent(&s.seq, || s.name.load()))
            .unwrap_or_default()
    }

    pub fn set_lib(&self, name: Option<&[u8]>, ver: Option<&[u8]>) {
        if let Some(s) = self.slot {
            write_consistent(&s.seq, || {
                if let Some(n) = name {
                    s.lib_name.store(n);
                }
                if let Some(v) = ver {
                    s.lib_ver.store(v);
                }
            });
        }
    }

    pub fn set_flag(&self, f: u32, on: bool) {
        if let Some(s) = self.slot {
            let was = if on {
                s.flags.fetch_or(f, Ordering::Relaxed)
            } else {
                s.flags.fetch_and(!f, Ordering::Relaxed)
            };
            if f & flag::NO_TOUCH != 0
                && (was & flag::NO_TOUCH != 0) != on
                && let Some(ctl) = ctl()
            {
                if on {
                    ctl.no_touch_clients.fetch_add(1, Ordering::Relaxed);
                } else {
                    ctl.no_touch_clients.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// A pending `CLIENT UNBLOCK`, consumed: `Some(true)` to answer as an
    /// error, `Some(false)` as a timeout.
    /// A pending `CLIENT UNBLOCK`, taken: the slot stays marked until
    /// `unblock_answered`, which `CLIENT UNBLOCK` waits for.
    pub fn take_unblock(&self) -> Option<bool> {
        let s = self.slot?;
        let v = s.unblock.load(Ordering::Acquire);
        if !matches!(v, 1 | 2) {
            return None;
        }
        s.unblock
            .compare_exchange(v, UNBLOCK_TAKEN, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|v| v == 2)
    }

    /// The unblocked command has replied and been counted.
    pub fn unblock_answered(&self) {
        if let Some(s) = self.slot {
            let _ =
                s.unblock
                    .compare_exchange(UNBLOCK_TAKEN, 0, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    pub fn addr(&self) -> String {
        self.slot
            .map(|s| String::from_utf8_lossy(&read_consistent(&s.seq, || s.addr.load())).into())
            .unwrap_or_default()
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        if let Some(s) = self.slot {
            self.set_flag(flag::NO_TOUCH, false);
            s.id.store(0, Ordering::Relaxed);
            s.state.store(0, Ordering::Release);
        }
    }
}

/// A point-in-time copy of one client, for `CLIENT LIST` and `CLIENT KILL`.
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    pub laddr: String,
    pub fd: i32,
    pub name: String,
    pub age_s: i64,
    pub idle_s: i64,
    pub flags: u32,
    pub db: u32,
    pub sub: u32,
    pub psub: u32,
    pub multi: i32,
    pub cmd: String,
    pub resp: u32,
    pub lib_name: String,
    pub lib_ver: String,
    pub qbuf: u32,
    pub user_serial: u64,
    pub user: String,
    slot: &'static ClientSlot,
}

impl ClientInfo {
    pub fn kill(&self) {
        self.slot.kill.store(1, Ordering::Relaxed);
    }

    /// Whether this client was told to close and is still there.
    pub fn killed(&self) -> bool {
        self.slot.id.load(Ordering::Acquire) == self.id
            && self.slot.kill.load(Ordering::Relaxed) != 0
    }

    /// Have a subscriber check its subscriptions against its user's
    /// channel permissions.
    pub fn request_acl_recheck(&self) {
        self.slot.acl_recheck.store(1, Ordering::Release);
    }

    pub fn acl_recheck_pending(&self) -> bool {
        self.slot.id.load(Ordering::Acquire) == self.id
            && self.slot.acl_recheck.load(Ordering::Acquire) != 0
    }

    /// `CLIENT UNBLOCK`: end this client's wait, if it is waiting.
    /// `CLIENT UNBLOCK`. Returns once the client has replied and its command
    /// been counted, as Redis has by the time it answers — or once it is
    /// gone, or after half a second.
    pub fn unblock(&self, as_error: bool) -> bool {
        if self.flags & flag::BLOCKED == 0 {
            return false;
        }
        self.slot
            .unblock
            .store(if as_error { 2 } else { 1 }, Ordering::Release);
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        while self.slot.id.load(Ordering::Acquire) == self.id
            && self.slot.unblock.load(Ordering::Acquire) != 0
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        true
    }

    /// Redis's `CLIENT LIST` line, field for field.
    pub fn line(&self) -> String {
        let mut f = String::new();
        for (bit, ch) in [
            (flag::PUBSUB, 'P'),
            (flag::MULTI, 'x'),
            (flag::MONITOR, 'O'),
            (flag::BLOCKED, 'b'),
            (flag::TRACKING, 't'),
            (flag::NO_EVICT, 'e'),
            (flag::NO_TOUCH, 'T'),
            (flag::CLOSE_ASAP, 'A'),
        ] {
            if self.flags & bit != 0 {
                f.push(ch);
            }
        }
        if f.is_empty() {
            f.push('N');
        }
        format!(
            "id={} addr={} laddr={} fd={} name={} age={} idle={} flags={} db={} sub={} psub={} ssub=0 multi={} qbuf={} qbuf-free=0 argv-mem=0 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={} redir=-1 resp={} lib-name={} lib-ver={}",
            self.id,
            self.addr,
            self.laddr,
            self.fd,
            self.name,
            self.age_s,
            self.idle_s,
            f,
            self.db,
            self.sub,
            self.psub,
            self.multi,
            self.qbuf,
            self.cmd,
            self.user,
            self.resp,
            self.lib_name,
            self.lib_ver,
        )
    }
}

/// Every registered client, ordered by id as Redis lists them.
pub fn clients() -> Vec<ClientInfo> {
    let Some(ctl) = ctl() else { return vec![] };
    let now = now_ms();
    let mut out: Vec<ClientInfo> = ctl
        .clients
        .iter()
        .filter(|s| s.state.load(Ordering::Acquire) == 1 && s.id.load(Ordering::Acquire) != 0)
        .map(|s| {
            let (name, addr, laddr, cmd, lib_name, lib_ver) = read_consistent(&s.seq, || {
                (
                    s.name.load(),
                    s.addr.load(),
                    s.laddr.load(),
                    s.cmd.load(),
                    s.lib_name.load(),
                    s.lib_ver.load(),
                )
            });
            let text = |v: Vec<u8>| String::from_utf8_lossy(&v).into_owned();
            ClientInfo {
                id: s.id.load(Ordering::Relaxed),
                addr: text(addr),
                laddr: text(laddr),
                fd: s.fd.load(Ordering::Relaxed),
                name: text(name),
                age_s: (now - s.created_ms.load(Ordering::Relaxed)) / 1000,
                idle_s: (now - s.last_ms.load(Ordering::Relaxed)) / 1000,
                flags: s.flags.load(Ordering::Relaxed),
                db: s.db.load(Ordering::Relaxed),
                sub: s.sub.load(Ordering::Relaxed),
                psub: s.psub.load(Ordering::Relaxed),
                multi: s.multi.load(Ordering::Relaxed),
                cmd: text(cmd),
                resp: s.resp.load(Ordering::Relaxed),
                lib_name: text(lib_name),
                lib_ver: text(lib_ver),
                qbuf: s.qbuf.load(Ordering::Relaxed),
                user_serial: s.user_serial.load(Ordering::Relaxed),
                user: crate::acl::name_of(s.user_serial.load(Ordering::Relaxed)),
                slot: s,
            }
        })
        .collect();
    out.sort_by_key(|c| c.id);
    out
}

// ───────────────────────────────── stats ────────────────────────────────────

/// Count one command for `INFO commandstats`, by its index in the generated
/// command table.
pub fn count_command(index: usize, usec: u64, failed: bool, rejected: bool) {
    let Some(ctl) = ctl() else { return };
    ctl.total_commands.fetch_add(1, Ordering::Relaxed);
    if let Some(s) = ctl.cmdstats.get(index) {
        if rejected {
            s.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        s.calls.fetch_add(1, Ordering::Relaxed);
        s.usec.fetch_add(usec, Ordering::Relaxed);
        s.hist[bucket_of(usec)].fetch_add(1, Ordering::Relaxed);
        if failed {
            s.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Count an error reply by its code — the first word, `ERR`, `WRONGTYPE` ...
pub fn count_error(code: &str) {
    let Some(ctl) = ctl() else { return };
    ctl.total_error_replies.fetch_add(1, Ordering::Relaxed);
    let code = code.as_bytes();
    if let Some(e) = ctl
        .errstats
        .iter()
        .find(|e| e.used.load(Ordering::Acquire) == 1 && e.name.load() == code)
    {
        e.count.fetch_add(1, Ordering::Relaxed);
        return;
    }
    spin_lock(&ctl.errstat_lock);
    let existing = ctl
        .errstats
        .iter()
        .find(|e| e.used.load(Ordering::Acquire) == 1 && e.name.load() == code);
    let slot = existing.or_else(|| {
        ctl.errstats
            .iter()
            .find(|e| e.used.load(Ordering::Relaxed) == 0)
            .inspect(|e| {
                e.name.store(code);
                e.used.store(1, Ordering::Release);
            })
    });
    spin_unlock(&ctl.errstat_lock);
    if let Some(e) = slot {
        e.count.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn errstats() -> Vec<(String, u64)> {
    let Some(ctl) = ctl() else { return vec![] };
    ctl.errstats
        .iter()
        .filter(|e| e.used.load(Ordering::Acquire) == 1)
        .map(|e| {
            (
                String::from_utf8_lossy(&e.name.load()).into_owned(),
                e.count.load(Ordering::Relaxed),
            )
        })
        .filter(|(_, n)| *n > 0)
        .collect()
}

/// `CONFIG RESETSTAT`.
pub fn reset_stats() {
    let Some(ctl) = ctl() else { return };
    for a in [
        &ctl.total_connections,
        &ctl.rejected_connections,
        &ctl.total_commands,
        &ctl.total_error_replies,
        &ctl.keyspace_hits,
        &ctl.keyspace_misses,
        &ctl.expired_keys,
        &ctl.evicted_keys,
        &ctl.eviction_exceeded_total,
        &ctl.net_input,
        &ctl.net_output,
    ] {
        a.store(0, Ordering::Relaxed);
    }
    for s in &ctl.cmdstats {
        for a in [&s.calls, &s.usec, &s.rejected, &s.failed] {
            a.store(0, Ordering::Relaxed);
        }
        for h in &s.hist {
            h.store(0, Ordering::Relaxed);
        }
    }
    for e in &ctl.errstats {
        e.count.store(0, Ordering::Relaxed);
    }
    ctl.stat_reset_ms.store(now_ms(), Ordering::Relaxed);
}

// ───────────────────────────────── config ───────────────────────────────────

/// The value `CONFIG SET` stored for `name`, if any; `None` means the default.
pub fn config_get(name: &str) -> Option<String> {
    let ctl = ctl()?;
    read_consistent(&ctl.config_seq, || {
        ctl.config
            .iter()
            .find(|c| c.used.load(Ordering::Relaxed) == 1 && c.name.load() == name.as_bytes())
            .map(|c| String::from_utf8_lossy(&c.value.load()).into_owned())
    })
}

/// Changes with every `CONFIG SET`; odd while one is being written.
pub fn config_generation() -> u32 {
    ctl().map_or(0, |c| c.config_seq.load(Ordering::Acquire))
}

/// Store several values at once, all visible together.
pub fn config_set(pairs: &[(&str, String)]) -> Result<(), String> {
    let Some(ctl) = ctl() else {
        return Err("ERR CONFIG SET needs pg_redis in shared_preload_libraries".to_string());
    };
    if pairs.iter().any(|(_, v)| v.len() > CONFIG_VALUE_LEN) {
        return Err(format!(
            "ERR CONFIG SET failed - value longer than {CONFIG_VALUE_LEN} bytes"
        ));
    }
    spin_lock(&ctl.config_lock);
    let mut result = Ok(());
    write_consistent(&ctl.config_seq, || {
        for (name, value) in pairs {
            let slot = ctl
                .config
                .iter()
                .find(|c| c.used.load(Ordering::Relaxed) == 1 && c.name.load() == name.as_bytes())
                .or_else(|| {
                    ctl.config
                        .iter()
                        .find(|c| c.used.load(Ordering::Relaxed) == 0)
                });
            match slot {
                Some(c) => {
                    c.name.store(name.as_bytes());
                    c.value.store(value.as_bytes());
                    c.used.store(1, Ordering::Relaxed);
                }
                None => result = Err("ERR CONFIG SET failed - too many changed settings".into()),
            }
        }
    });
    spin_unlock(&ctl.config_lock);
    result
}

// ──────────────────────────────── slowlog ───────────────────────────────────

pub fn slowlog_push(args: &[Vec<u8>], duration_us: i64, addr: &str, name: &[u8], max_len: usize) {
    let Some(ctl) = ctl() else { return };
    if max_len == 0 {
        return;
    }
    spin_lock(&ctl.slowlog_lock);
    let id = ctl.slowlog_next_id.fetch_add(1, Ordering::Relaxed);
    let e = &ctl.slowlog[(id as usize) % SLOWLOG_CAP];
    write_consistent(&e.seq, || {
        e.id.store(id, Ordering::Relaxed);
        e.time.store(now_ms() / 1000, Ordering::Relaxed);
        e.duration_us.store(duration_us, Ordering::Relaxed);
        let argc = args.len().min(SLOWLOG_ARGS);
        e.argc.store(argc as u32, Ordering::Relaxed);
        e.total_args.store(args.len() as u32, Ordering::Relaxed);
        for (i, a) in args.iter().take(argc).enumerate() {
            e.args[i].store(a);
            e.arg_lens[i].store(a.len() as u64, Ordering::Relaxed);
        }
        e.addr.store(addr.as_bytes());
        e.name.store(name);
    });
    let len = ctl.slowlog_len.load(Ordering::Relaxed);
    ctl.slowlog_len.store(
        (len + 1).min(max_len.min(SLOWLOG_CAP) as u32),
        Ordering::Relaxed,
    );
    spin_unlock(&ctl.slowlog_lock);
}

pub struct SlowlogEntry {
    pub id: u64,
    pub time: i64,
    pub duration_us: i64,
    /// Arguments as SLOWLOG GET shows them, truncation markers included.
    pub args: Vec<Vec<u8>>,
    pub addr: String,
    pub name: Vec<u8>,
}

/// Newest first, at most `count` (and at most the configured max length).
pub fn slowlog_get(count: usize, max_len: usize) -> Vec<SlowlogEntry> {
    let Some(ctl) = ctl() else { return vec![] };
    let len = (ctl.slowlog_len.load(Ordering::Relaxed) as usize).min(max_len);
    let next = ctl.slowlog_next_id.load(Ordering::Relaxed);
    (0..len.min(count))
        .filter_map(|i| {
            let id = next.checked_sub(1 + i as u64)?;
            let e = &ctl.slowlog[(id as usize) % SLOWLOG_CAP];
            Some(read_consistent(&e.seq, || {
                let argc = e.argc.load(Ordering::Relaxed) as usize;
                let total = e.total_args.load(Ordering::Relaxed) as usize;
                let mut args: Vec<Vec<u8>> = (0..argc)
                    .map(|j| {
                        let mut a = e.args[j].load();
                        let full = e.arg_lens[j].load(Ordering::Relaxed) as usize;
                        if full > SLOWLOG_ARG_LEN {
                            a.extend_from_slice(
                                format!("... ({} more bytes)", full - SLOWLOG_ARG_LEN).as_bytes(),
                            );
                        }
                        a
                    })
                    .collect();
                // Redis keeps 31 arguments and replaces the rest with a count.
                if total > SLOWLOG_ARGS {
                    args.truncate(SLOWLOG_ARGS - 1);
                    args.push(
                        format!("... ({} more arguments)", total - SLOWLOG_ARGS + 1).into_bytes(),
                    );
                }
                SlowlogEntry {
                    id: e.id.load(Ordering::Relaxed),
                    time: e.time.load(Ordering::Relaxed),
                    duration_us: e.duration_us.load(Ordering::Relaxed),
                    args,
                    addr: String::from_utf8_lossy(&e.addr.load()).into_owned(),
                    name: e.name.load(),
                }
            }))
        })
        .collect()
}

pub fn slowlog_len(max_len: usize) -> usize {
    ctl().map_or(0, |c| {
        (c.slowlog_len.load(Ordering::Relaxed) as usize).min(max_len)
    })
}

pub fn slowlog_reset() {
    if let Some(c) = ctl() {
        c.slowlog_len.store(0, Ordering::Relaxed);
    }
}

// ──────────────────────────────── waiters ───────────────────────────────────

/// A blocked client's place in line. Dropping it leaves the line.
pub struct WaiterHandle {
    slot: Option<&'static Waiter>,
    ticket: u64,
    keys: Vec<u64>,
}

impl WaiterHandle {
    /// `versions` are the keys' write counters as the client starts to wait:
    /// a write before then is not one it has to be woken for.
    pub fn register(keys: &[u64], versions: &[u64], nokey: bool) -> WaiterHandle {
        let Some(ctl) = ctl() else {
            return WaiterHandle {
                slot: None,
                ticket: 0,
                keys: keys.to_vec(),
            };
        };
        let ticket = ctl.next_ticket.fetch_add(1, Ordering::AcqRel) + 1;
        // Claim a free slot with a placeholder, fill it, then publish the
        // real ticket: a reader never sees a ticket with stale keys.
        let slot = ctl.waiters.iter().find(|w| {
            w.ticket
                .compare_exchange(0, u64::MAX, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        });
        if let Some(w) = slot {
            let n = keys.len().min(WAITER_KEYS);
            for (i, k) in keys.iter().take(n).enumerate() {
                w.keys[i].store(*k, Ordering::Relaxed);
                w.seen[i].store(versions.get(i).copied().unwrap_or(0), Ordering::Relaxed);
            }
            w.nkeys.store(n as u32, Ordering::Relaxed);
            w.settling.store(0, Ordering::Relaxed);
            w.nokey.store(u32::from(nokey), Ordering::Relaxed);
            w.ticket.store(ticket, Ordering::Release);
            ctl.active_waiters.fetch_add(1, Ordering::AcqRel);
        }
        WaiterHandle {
            slot,
            ticket,
            keys: keys.to_vec(),
        }
    }

    /// Whether every older waiter sharing a key with this one has tried that
    /// key at its current write counter (`current`, one per key of this
    /// waiter's). The oldest client blocked on a key is served first, as in
    /// Redis — but one the write cannot serve, a BLPOP on a key that became a
    /// stream, is passed over rather than holding the clients behind it.
    pub fn my_turn(&self, current: &[u64]) -> bool {
        let Some(ctl) = ctl() else { return true };
        !ctl.waiters.iter().any(|w| {
            let t = w.ticket.load(Ordering::Acquire);
            // One already served competes for nothing.
            if t == 0
                || t == u64::MAX
                || t >= self.ticket
                || w.settling.load(Ordering::Acquire) != 0
            {
                return false;
            }
            let n = (w.nkeys.load(Ordering::Relaxed) as usize).min(WAITER_KEYS);
            (0..n).any(|i| {
                let id = w.keys[i].load(Ordering::Relaxed);
                self.keys
                    .iter()
                    .zip(current)
                    .any(|(k, v)| *k == id && w.seen[i].load(Ordering::Acquire) < *v)
            })
        })
    }
}

impl WaiterHandle {
    /// The waiter has tried against the keys at these write counters.
    pub fn mark_seen(&self, versions: &[u64]) {
        if let Some(w) = self.slot {
            for (i, v) in versions.iter().take(WAITER_KEYS).enumerate() {
                w.seen[i].store(*v, Ordering::Release);
            }
        }
    }

    /// The waiter has been served and is settling its own write.
    pub fn set_settling(&self) {
        if let Some(w) = self.slot {
            w.settling.store(1, Ordering::Release);
        }
    }
}

impl Drop for WaiterHandle {
    fn drop(&mut self) {
        if let Some(w) = self.slot {
            w.ticket.store(0, Ordering::Release);
            if let Some(ctl) = ctl() {
                ctl.active_waiters.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

/// Whether any client is blocked right now.
pub fn waiters_active() -> bool {
    ctl().is_some_and(|c| c.active_waiters.load(Ordering::Acquire) > 0)
}

/// Redis serves the clients a write unblocks before the writer hears back:
/// `RPUSH` returns once a blocked `BLMOVE` has taken the element and moved it.
/// Here the blocked clients live on other threads and other workers, so the
/// writer waits — up to `limit` — until every client blocked on a key it wrote
/// has tried against the write. `targets` pairs each key's id with its write
/// counter after the write.
///
/// A served client settling its own write (a move) waits only for clients that
/// have not yet tried; the original writer waits for those settling too, so a
/// chain of moves completes before it replies, and a cycle of them cannot wait
/// on itself.
pub fn settle_waiters(targets: &[(u64, u64)], wait_for_settling: bool, limit: Duration) {
    let Some(ctl) = ctl() else { return };
    if targets.is_empty() || ctl.active_waiters.load(Ordering::Acquire) == 0 {
        return;
    }
    let deadline = std::time::Instant::now() + limit;
    let pending = || {
        ctl.waiters.iter().any(|w| {
            let t = w.ticket.load(Ordering::Acquire);
            if t == 0 || t == u64::MAX {
                return false;
            }
            if w.settling.load(Ordering::Acquire) != 0 {
                return wait_for_settling;
            }
            let n = (w.nkeys.load(Ordering::Relaxed) as usize).min(WAITER_KEYS);
            (0..n).any(|i| {
                let id = w.keys[i].load(Ordering::Relaxed);
                targets
                    .iter()
                    .any(|(k, v)| *k == id && w.seen[i].load(Ordering::Acquire) < *v)
            })
        })
    };
    while pending() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_micros(200));
    }
}

// ──────────────────────────────── scripts ───────────────────────────────────

/// Scripts running at once across all workers: one per dispatcher at most.
pub const SCRIPT_SLOTS: usize = 64;
const SCRIPT_CMD_LEN: usize = 512;

const SCRIPT_EVAL: u32 = 1;
const SCRIPT_DIRTY: u32 = 2;
const SCRIPT_KILLED: u32 = 4;

/// One running script, for `SCRIPT KILL`, `FUNCTION STATS` and the BUSY
/// reply other clients get once it has run past `busy-reply-threshold`.
#[repr(C)]
pub struct ScriptRun {
    /// 0 while the slot is free; claimed with a compare-and-swap.
    started_ms: AtomicI64,
    flags: AtomicU32,
    seq: AtomicU32,
    name: Field<NAME_LEN>,
    /// The command that started it, arguments separated by NUL.
    cmd: Field<SCRIPT_CMD_LEN>,
}

/// A script this dispatcher is running. Dropping it frees the slot.
pub struct ScriptRunHandle {
    slot: Option<&'static ScriptRun>,
}

/// What `FUNCTION STATS` reports of a running script.
pub struct RunningScript {
    pub eval: bool,
    pub name: Vec<u8>,
    pub argv: Vec<Vec<u8>>,
    pub duration_ms: i64,
}

impl ScriptRunHandle {
    pub fn start(eval: bool, name: &[u8], argv: &[Vec<u8>]) -> ScriptRunHandle {
        let now = now_ms().max(1);
        let slot = ctl().and_then(|ctl| {
            ctl.scripts.iter().find(|s| {
                s.started_ms
                    .compare_exchange(0, -1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            })
        });
        if let Some(s) = slot {
            s.seq.fetch_add(1, Ordering::AcqRel);
            s.name.store(name);
            s.cmd.store(&argv.join(&0u8));
            s.seq.fetch_add(1, Ordering::AcqRel);
            s.flags
                .store(if eval { SCRIPT_EVAL } else { 0 }, Ordering::Relaxed);
            s.started_ms.store(now, Ordering::Release);
        }
        ScriptRunHandle { slot }
    }

    /// The script has run a write command: from here on it cannot be killed.
    pub fn mark_dirty(&self) {
        if let Some(s) = self.slot {
            s.flags.fetch_or(SCRIPT_DIRTY, Ordering::AcqRel);
        }
    }
}

impl Drop for ScriptRunHandle {
    fn drop(&mut self) {
        if let Some(s) = self.slot {
            s.flags.store(0, Ordering::Relaxed);
            s.started_ms.store(0, Ordering::Release);
        }
    }
}

/// The longest-running script, if any.
fn oldest_script() -> Option<(&'static ScriptRun, i64)> {
    ctl()?
        .scripts
        .iter()
        .filter_map(|s| {
            let started = s.started_ms.load(Ordering::Acquire);
            (started > 0).then_some((s, started))
        })
        .min_by_key(|(_, started)| *started)
}

/// Whether some script has been running for longer than the threshold
/// `threshold_ms` reads, and if so whether it is an `EVAL` — which decides the
/// wording of the BUSY reply. The threshold is only read once a script is
/// running: this is asked before every command.
pub fn busy_script(threshold_ms: impl FnOnce() -> i64) -> Option<bool> {
    let (s, started) = oldest_script()?;
    let threshold_ms = threshold_ms();
    if threshold_ms <= 0 {
        return None;
    }
    (now_ms() - started >= threshold_ms).then(|| s.flags.load(Ordering::Acquire) & SCRIPT_EVAL != 0)
}

/// `SCRIPT KILL` (`eval`) or `FUNCTION KILL`, with Redis's refusals.
pub fn kill_script(eval: bool) -> Result<(), String> {
    let Some((s, _)) = oldest_script() else {
        return Err("NOTBUSY No scripts in execution right now.".into());
    };
    let flags = s.flags.load(Ordering::Acquire);
    if flags & SCRIPT_DIRTY != 0 {
        return Err(
            "UNKILLABLE Sorry the script already executed write commands against \
                    the dataset. You can either wait the script termination or kill the \
                    server in a hard way using the SHUTDOWN NOSAVE command."
                .into(),
        );
    }
    let is_eval = flags & SCRIPT_EVAL != 0;
    if eval && !is_eval {
        return Err(
            "BUSY Redis is busy running a script. You can only call FUNCTION KILL \
                    or SHUTDOWN NOSAVE."
                .into(),
        );
    }
    if !eval && is_eval {
        return Err(
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL \
                    or SHUTDOWN NOSAVE."
                .into(),
        );
    }
    s.flags.fetch_or(SCRIPT_KILLED, Ordering::AcqRel);
    Ok(())
}

pub fn running_script() -> Option<RunningScript> {
    let (s, started) = oldest_script()?;
    let (name, cmd) = read_consistent(&s.seq, || (s.name.load(), s.cmd.load()));
    Some(RunningScript {
        eval: s.flags.load(Ordering::Acquire) & SCRIPT_EVAL != 0,
        name,
        argv: cmd.split(|b| *b == 0).map(<[u8]>::to_vec).collect(),
        duration_ms: now_ms() - started,
    })
}

/// Bump a generation counter: the script cache or the function libraries
/// changed, and every worker's compiled copy is stale.
pub fn bump_scripts_gen() {
    if let Some(ctl) = ctl() {
        ctl.scripts_gen.fetch_add(1, Ordering::AcqRel);
    }
}

pub fn bump_functions_gen() {
    if let Some(ctl) = ctl() {
        ctl.functions_gen.fetch_add(1, Ordering::AcqRel);
    }
}

pub fn scripts_gen() -> u64 {
    ctl().map_or(0, |c| c.scripts_gen.load(Ordering::Acquire))
}

pub fn functions_gen() -> u64 {
    ctl().map_or(0, |c| c.functions_gen.load(Ordering::Acquire))
}

pub fn set_function_counts(libraries: i64, functions: i64) {
    if let Some(ctl) = ctl() {
        ctl.functions_libs.store(libraries, Ordering::Relaxed);
        ctl.functions_count.store(functions, Ordering::Relaxed);
    }
}

pub fn function_counts() -> (i64, i64) {
    ctl().map_or((0, 0), |c| {
        (
            c.functions_libs.load(Ordering::Relaxed),
            c.functions_count.load(Ordering::Relaxed),
        )
    })
}

/// A cheap, copyable view of whether a running script has been killed — what
/// the Lua instruction hook polls.
#[derive(Clone, Copy)]
pub struct KillProbe(Option<&'static ScriptRun>);

impl KillProbe {
    pub fn killed(self) -> bool {
        self.0
            .is_some_and(|s| s.flags.load(Ordering::Acquire) & SCRIPT_KILLED != 0)
    }
}

impl ScriptRunHandle {
    pub fn probe(&self) -> KillProbe {
        KillProbe(self.slot)
    }
}

pub fn set_cached_scripts(n: i64) {
    if let Some(ctl) = ctl() {
        ctl.cached_scripts.store(n, Ordering::Relaxed);
    }
}

pub fn cached_scripts() -> i64 {
    ctl().map_or(0, |c| c.cached_scripts.load(Ordering::Relaxed))
}

/// A database was flushed.
pub fn bump_flush_epoch(db: u8) {
    if let Some(ctl) = ctl()
        && let Some(e) = ctl.flush_epochs.get(db as usize)
    {
        e.fetch_add(1, Ordering::AcqRel);
    }
}

pub fn flush_epoch(db: u8) -> u64 {
    ctl()
        .and_then(|c| c.flush_epochs.get(db as usize))
        .map_or(0, |e| e.load(Ordering::Acquire))
}

/// Publish the encoding thresholds a `CONFIG SET` changed.
pub fn set_enc_limits(values: [i64; crate::memdb::encoding::LIMIT_COUNT]) {
    if let Some(ctl) = ctl() {
        spin_lock(&ctl.config_lock);
        for (slot, v) in ctl.enc_limits.iter().zip(values) {
            slot.store(v, Ordering::Relaxed);
        }
        ctl.enc_limits_set.store(1, Ordering::Release);
        spin_unlock(&ctl.config_lock);
    }
}

/// The encoding thresholds, or `None` while they are Redis's defaults.
pub fn enc_limits() -> Option<[i64; crate::memdb::encoding::LIMIT_COUNT]> {
    let ctl = ctl()?;
    if ctl.enc_limits_set.load(Ordering::Acquire) == 0 {
        return None;
    }
    Some(std::array::from_fn(|i| {
        ctl.enc_limits[i].load(Ordering::Relaxed)
    }))
}

pub fn set_notify_flags(flags: u32) {
    if let Some(ctl) = ctl() {
        ctl.notify_flags.store(flags, Ordering::Relaxed);
    }
}

pub fn notify_flags() -> u32 {
    ctl().map_or(0, |c| c.notify_flags.load(Ordering::Relaxed))
}

/// Keys the expiry sweep removed, for INFO's `expired_keys`.
pub fn count_expired(n: u64) {
    if n > 0
        && let Some(ctl) = ctl()
    {
        ctl.expired_keys.fetch_add(n, Ordering::Relaxed);
    }
}

/// Keys evicted under `maxmemory-policy`, for INFO's `evicted_keys`.
pub fn count_evicted(n: u64) {
    if let Some(ctl) = ctl() {
        ctl.evicted_keys.fetch_add(n, Ordering::Relaxed);
    }
}

/// After an eviction round: whether the arena is still over the limit.
pub fn eviction_exceeded(over: bool) {
    let Some(ctl) = ctl() else { return };
    let since = ctl.eviction_exceeded_since.load(Ordering::Relaxed);
    if over {
        if since == 0 {
            ctl.eviction_exceeded_since
                .store(now_ms(), Ordering::Relaxed);
        }
    } else if since != 0
        && ctl
            .eviction_exceeded_since
            .compare_exchange(since, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        ctl.eviction_exceeded_total
            .fetch_add((now_ms() - since).max(0) as u64, Ordering::Relaxed);
    }
}

/// INFO's `total_eviction_exceeded_time` and `current_eviction_exceeded_time`,
/// in milliseconds.
pub fn eviction_exceeded_times() -> (u64, u64) {
    let Some(ctl) = ctl() else { return (0, 0) };
    let since = ctl.eviction_exceeded_since.load(Ordering::Relaxed);
    let current = if since == 0 {
        0
    } else {
        (now_ms() - since).max(0) as u64
    };
    (
        ctl.eviction_exceeded_total.load(Ordering::Relaxed) + current,
        current,
    )
}

/// Whether any client is in `CLIENT NO-TOUCH` mode.
pub fn any_no_touch() -> bool {
    ctl().is_some_and(|c| c.no_touch_clients.load(Ordering::Relaxed) > 0)
}

/// Whether a `CLIENT PAUSE` is in force.
pub fn paused() -> bool {
    ctl().is_some_and(|c| {
        let until = c.pause_until_ms.load(Ordering::Relaxed);
        until != 0 && now_ms() < until
    })
}

// ──────────────────────────────── tracking ──────────────────────────────────

/// The live slot of client `id`, if it is connected.
/// `ClientSlot::track_handoff` with no slot waiting.
pub const HANDOFF_NONE: i32 = -1;
/// `ClientSlot::track_handoff` once the connection has gone: nothing more
/// may be handed to it.
pub const HANDOFF_CLOSED: i32 = -2;

/// Whether a tracking client sends its invalidations to `id`.
pub fn redirected_to(id: u64) -> bool {
    let Some(ctl) = ctl() else { return false };
    ctl.clients.iter().any(|s| {
        s.state.load(Ordering::Acquire) == 1
            && s.tracking.load(Ordering::Relaxed) & crate::tracking::ON != 0
            && s.track_redirect.load(Ordering::Relaxed) == id
    })
}

/// `ClientSlot::unblock` once the blocked client has taken the request and
/// not yet answered it.
const UNBLOCK_TAKEN: u32 = 3;

pub fn slot_of(id: u64) -> Option<&'static ClientSlot> {
    ctl()?
        .clients
        .iter()
        .find(|s| s.state.load(Ordering::Acquire) == 1 && s.id.load(Ordering::Acquire) == id)
}

impl ClientSlot {
    pub fn prefixes(&self) -> Vec<Vec<u8>> {
        let raw = read_consistent(&self.seq, || self.track_prefixes.load());
        // A lone NUL is the one empty prefix: every key.
        if raw == [0] {
            return vec![Vec::new()];
        }
        raw.split(|b| *b == 0)
            .filter(|p| !p.is_empty())
            .map(<[u8]>::to_vec)
            .collect()
    }
}

impl ClientHandle {
    pub fn slot(&self) -> Option<&'static ClientSlot> {
        self.slot
    }

    /// Store the `BCAST` prefixes; an empty prefix is kept as a lone NUL.
    pub fn set_prefixes(&self, prefixes: &[Vec<u8>]) {
        if let Some(s) = self.slot {
            let joined = if prefixes.len() == 1 && prefixes[0].is_empty() {
                vec![0]
            } else {
                prefixes.join(&0u8)
            };
            s.seq.fetch_add(1, Ordering::AcqRel);
            s.track_prefixes.store(&joined);
            s.seq.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub fn set_proto(&self, proto: u8) {
        if let Some(s) = self.slot {
            s.proto.store(proto as u32, Ordering::Relaxed);
        }
    }
}

pub fn tracking_active() -> bool {
    ctl().is_some_and(|c| c.tracking_clients.load(Ordering::Relaxed) > 0)
}

/// INFO's `total_blocking_keys` and `total_blocking_keys_on_nokey`: the
/// distinct keys clients are blocked on, and those of them with a client that
/// is woken by the key's deletion too.
pub fn blocking_key_counts() -> (usize, usize) {
    let Some(ctl) = ctl() else { return (0, 0) };
    let mut all = std::collections::HashSet::new();
    let mut nokey = std::collections::HashSet::new();
    for w in ctl.waiters.iter() {
        let t = w.ticket.load(Ordering::Acquire);
        if t == 0 || t == u64::MAX {
            continue;
        }
        let n = (w.nkeys.load(Ordering::Relaxed) as usize).min(WAITER_KEYS);
        for i in 0..n {
            let k = w.keys[i].load(Ordering::Relaxed);
            all.insert(k);
            if w.nokey.load(Ordering::Relaxed) != 0 {
                nokey.insert(k);
            }
        }
    }
    (all.len(), nokey.len())
}
