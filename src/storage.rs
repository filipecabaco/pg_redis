//! Storage policies: which backend serves each database, the key-level
//! operations every backend offers, and the migration that moves a database
//! from one backend to another while it stays in use. The design is in
//! docs/STORAGE.md.
//!
//! A database's policy lives in shared memory, where every worker reads it per
//! command. The block starts zeroed and zero means "the default", so the
//! defaults — the old fixed split — hold without anything initialising them.
#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU32, Ordering};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;

use crate::commands::{Command, DURABLE_FROM, KeyDump, NUM_DBS};
use crate::memdb::KeyKind;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    /// A shared-memory slot.
    Memory,
    /// A shared-memory slot, copied to `redis.snapshot` on an interval and
    /// reloaded from it when the server starts.
    Snapshot,
    /// WAL-logged tables.
    Logged,
}

impl Policy {
    pub fn name(self) -> &'static str {
        match self {
            Policy::Memory => "memory",
            Policy::Snapshot => "memory+snapshot",
            Policy::Logged => "logged",
        }
    }

    pub fn parse(s: &str) -> Option<Policy> {
        match s.to_ascii_lowercase().as_str() {
            "memory" => Some(Policy::Memory),
            "memory+snapshot" => Some(Policy::Snapshot),
            "logged" => Some(Policy::Logged),
            _ => None,
        }
    }

    fn code(self) -> u8 {
        match self {
            Policy::Memory => 1,
            Policy::Logged => 3,
            Policy::Snapshot => 4,
        }
    }

    /// Served from a shared-memory slot.
    pub fn in_memory(self) -> bool {
        matches!(self, Policy::Memory | Policy::Snapshot)
    }

    fn from_code(c: u8) -> Option<Policy> {
        match c {
            1 => Some(Policy::Memory),
            3 => Some(Policy::Logged),
            4 => Some(Policy::Snapshot),
            _ => None,
        }
    }
}

/// Moves of keys sharing a stripe are serialised; distinct keys rarely share.
const STRIPES: usize = 256;

/// Each database's policy and memory slot, and the migration it may be in the
/// middle of, in `ServerCtl`. Slots are stored plus one, so zero is "none"
/// like a policy's zero.
#[repr(C)]
pub struct PolicyTable {
    policy: [AtomicU8; NUM_DBS],
    slot: [AtomicU8; NUM_DBS],
    /// The policy a database is moving away from, while it moves.
    from: [AtomicU8; NUM_DBS],
    from_slot: [AtomicU8; NUM_DBS],
    /// Databases with a migration under way, so the common case — none — is
    /// one load.
    migrations: AtomicU32,
    stripes: [AtomicU32; STRIPES],
    /// When each snapshot database was last saved; 0 asks for one now.
    last_snapshot_ms: [AtomicI64; NUM_DBS],
    /// A worker taking a snapshot of the database.
    snapshot_busy: [AtomicU32; NUM_DBS],
    /// Reloading a snapshot database after a start: 0 not yet, 1 under way,
    /// 2 done. No snapshot is taken before 2, or it would save the empty slot
    /// over the one it is about to be filled from.
    restored: [AtomicU8; NUM_DBS],
    /// Workers between start and a clean exit. The last one out snapshots.
    workers_alive: AtomicU32,
}

fn table() -> Option<&'static PolicyTable> {
    crate::server::ctl().map(|c| &c.storage)
}

/// The defaults: 0–7 in memory, each in the slot of its own number, and 8–15
/// logged.
fn default_policy(db: u8) -> Policy {
    if db >= DURABLE_FROM {
        Policy::Logged
    } else {
        Policy::Memory
    }
}

pub fn policy(db: u8) -> Policy {
    table()
        .and_then(|t| Policy::from_code(t.policy[db as usize].load(Ordering::Acquire)))
        .unwrap_or_else(|| default_policy(db))
}

/// The shared-memory slot that serves `db`, when its policy is `memory`.
pub fn slot(db: u8) -> Option<usize> {
    if !policy(db).in_memory() {
        return None;
    }
    match table().map(|t| t.slot[db as usize].load(Ordering::Acquire)) {
        Some(n) if n > 0 => Some(n as usize - 1),
        _ => Some(db as usize),
    }
}

/// The database a memory slot serves, or is moving out of: what an eviction
/// from it is reported against.
pub fn db_of_slot(s: usize) -> Option<u8> {
    let dbs = 0..crate::commands::NUM_DBS as u8;
    dbs.clone().find(|&db| slot(db) == Some(s)).or_else(|| {
        dbs.into_iter()
            .find(|&db| source(db) == Some(At::Memory(s)))
    })
}

// ───────────────────────────── where a key lives ────────────────────────────

/// One backend of one database.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum At {
    Memory(usize),
    Tables,
}

/// What serves `db` now.
pub fn here(db: u8) -> At {
    slot(db).map_or(At::Tables, At::Memory)
}

/// What `db` is moving away from, while it moves.
pub fn source(db: u8) -> Option<At> {
    let t = table()?;
    let from = Policy::from_code(t.from[db as usize].load(Ordering::Acquire))?;
    Some(match from {
        Policy::Memory | Policy::Snapshot => {
            At::Memory((t.from_slot[db as usize].load(Ordering::Acquire) as usize).checked_sub(1)?)
        }
        _ => At::Tables,
    })
}

pub fn migrating(db: u8) -> bool {
    source(db).is_some()
}

pub fn any_migrating() -> bool {
    table().is_some_and(|t| t.migrations.load(Ordering::Acquire) > 0)
}

pub fn kind_at(client: &mut SpiClient<'_>, db: u8, at: At, key: &[u8]) -> Option<KeyKind> {
    match at {
        At::Memory(s) => crate::shm::with_db(s, |m| m.key_kind(key)).ok().flatten(),
        At::Tables => Command::sql_key_kinds(client, db, &[key])
            .first()
            .and_then(|(_, k)| *k),
    }
}

/// The value and its deadline in microseconds since the epoch, 0 for none.
pub fn dump_at(client: &mut SpiClient<'_>, db: u8, at: At, key: &[u8]) -> Option<(KeyDump, i64)> {
    let mem = match at {
        At::Memory(s) => Some(s),
        At::Tables => None,
    };
    Command::dump_key_at(client, db, key, mem)
}

/// Remove every trace of `key`, live or past its deadline.
pub fn clear_at(client: &mut SpiClient<'_>, db: u8, at: At, key: &[u8]) {
    match at {
        At::Memory(s) => {
            let _ = crate::shm::with_db(s, |m| m.clear(key));
        }
        At::Tables => Command::sql_drop_key(client, db, key),
    }
}

/// Write `dump` as `key` with a deadline (0 for none). The key must have been
/// cleared first.
pub fn restore_at(
    client: &mut SpiClient<'_>,
    db: u8,
    at: At,
    key: &[u8],
    dump: &KeyDump,
    deadline_us: i64,
) {
    if let At::Memory(s) = at {
        let _ = crate::shm::with_db(s, |m| m.restore(key, dump, deadline_us));
        return;
    }
    Command::restore_key_at(client, db, key, dump, None);
    if deadline_us == 0 {
        return;
    }
    match at {
        At::Memory(_) => {}
        At::Tables => {
            let sql = crate::commands::sql::set_expiry(
                db as usize,
                "to_timestamp(0) + ($2::bigint * interval '1 microsecond')",
                crate::commands::ExpireCond::Always,
            );
            let _ = client.update(&sql, None, &[key.into(), deadline_us.into()]);
        }
    }
}

/// Up to `limit` keys held at `at`, past their deadline or not: a migration
/// has to clear the dead ones too before the source counts as empty.
fn list_at(client: &mut SpiClient<'_>, db: u8, at: At, limit: usize) -> Vec<Vec<u8>> {
    match at {
        At::Memory(s) => crate::shm::with_db(s, |m| m.keys_upto(limit)).unwrap_or_default(),
        At::Tables => {
            let union = std::iter::once("kv")
                .chain(crate::commands::sql::COLLECTION_TABLES)
                .chain(std::iter::once("expiry"))
                .map(|t| format!("SELECT key FROM redis.{t}_{db}"))
                .collect::<Vec<_>>()
                .join(" UNION ");
            client
                .select(&format!("{union} LIMIT $1"), None, &[(limit as i64).into()])
                .map(|t| {
                    t.filter_map(|r| r.get::<Vec<u8>>(1).ok().flatten())
                        .collect()
                })
                .unwrap_or_default()
        }
    }
}

// The operations on whatever serves the database now — what COPY, MOVE and
// RESTORE use.

pub fn kind(client: &mut SpiClient<'_>, db: u8, key: &[u8]) -> Option<KeyKind> {
    kind_at(client, db, here(db), key)
}

pub fn dump(client: &mut SpiClient<'_>, db: u8, key: &[u8]) -> Option<(KeyDump, i64)> {
    dump_at(client, db, here(db), key)
}

pub fn clear(client: &mut SpiClient<'_>, db: u8, key: &[u8]) {
    clear_at(client, db, here(db), key)
}

pub fn restore(client: &mut SpiClient<'_>, db: u8, key: &[u8], dump: &KeyDump, deadline_us: i64) {
    restore_at(client, db, here(db), key, dump, deadline_us)
}

// ───────────────────────────── moving keys ──────────────────────────────────

/// The stripes for a set of keys, taken in index order so two movers can
/// never each hold one the other waits for. Released on drop.
struct Stripes(Vec<usize>);

impl Stripes {
    fn lock(keys: &[(u8, Vec<u8>)]) -> Stripes {
        let mut idx: Vec<usize> = keys
            .iter()
            .map(|(db, k)| crate::blocking::key_id(*db, k) as usize % STRIPES)
            .collect();
        idx.sort_unstable();
        idx.dedup();
        if let Some(t) = table() {
            for &i in &idx {
                while t.stripes[i]
                    .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
            }
        }
        Stripes(idx)
    }
}

impl Drop for Stripes {
    fn drop(&mut self) {
        if let Some(t) = table() {
            for &i in &self.0 {
                t.stripes[i].store(0, Ordering::Release);
            }
        }
    }
}

/// Move `keys` — each named with its database — to whatever serves their
/// database now, wherever that database is migrating. Run by a dispatcher
/// outside any transaction: the copies commit in one, and only then are the
/// sources deleted, in another. A key already at its destination wins; its
/// source copy is just deleted. Moves raise no events and break no WATCH: the
/// value is the same before and after.
pub fn move_keys(keys: &[(u8, Vec<u8>)]) {
    let keys: Vec<(u8, Vec<u8>)> = keys
        .iter()
        .filter(|(db, _)| migrating(*db))
        .cloned()
        .collect();
    if keys.is_empty() {
        return;
    }
    let _held = Stripes::lock(&keys);
    BackgroundWorker::transaction(|| {
        Spi::connect_mut(|c| {
            for (db, key) in &keys {
                let Some(from) = source(*db) else { continue };
                let to = here(*db);
                if kind_at(c, *db, to, key).is_some() {
                    continue;
                }
                if let Some((dump, deadline)) = dump_at(c, *db, from, key) {
                    clear_at(c, *db, to, key);
                    restore_at(c, *db, to, key, &dump, deadline);
                }
            }
        })
    });
    BackgroundWorker::transaction(|| {
        Spi::connect_mut(|c| {
            for (db, key) in &keys {
                if let Some(from) = source(*db) {
                    clear_at(c, *db, from, key);
                }
            }
        })
    });
}

/// One step of `db`'s migration: move a batch of the keys still at the
/// source. When none are left, the migration ends: the source is emptied of
/// whatever dead entries it still held, and the new policy is final. Returns
/// whether the migration is still under way.
pub fn sweep(db: u8, batch: usize) -> bool {
    let Some(from) = source(db) else { return false };
    let keys: Vec<Vec<u8>> =
        BackgroundWorker::transaction(|| Spi::connect_mut(|c| list_at(c, db, from, batch)));
    if !keys.is_empty() {
        let named: Vec<(u8, Vec<u8>)> = keys.into_iter().map(|k| (db, k)).collect();
        move_keys(&named);
        return true;
    }
    finish(db, from);
    false
}

/// Run `db`'s migration to the end, for a command that has to see the whole
/// database or cannot name its keys in advance.
pub fn drain(db: u8) {
    while sweep(db, 512) {}
}

fn finish(db: u8, from: At) {
    let Some(t) = table() else { return };
    // Whoever clears the flag finishes; a second worker arriving finds none.
    let code = t.from[db as usize].swap(0, Ordering::AcqRel);
    if code == 0 {
        return;
    }
    t.from_slot[db as usize].store(0, Ordering::Release);
    t.migrations.fetch_sub(1, Ordering::AcqRel);
    match from {
        At::Memory(s) => {
            let _ = crate::shm::with_db(s, |m| m.clear_all());
        }
        At::Tables => {
            BackgroundWorker::transaction(|| {
                Spi::connect_mut(|c| {
                    let _ = c.update(&crate::commands::sql::flush_dbs(&[db as usize]), None, &[]);
                })
            });
        }
    }
    BackgroundWorker::transaction(|| {
        Spi::connect_mut(|c| {
            let _ = c.update(
                "UPDATE redis.db_policy SET from_policy = NULL, from_slot = NULL WHERE db = $1",
                None,
                &[(db as i16).into()],
            );
        })
    });
}

/// What a command line needs before it runs while databases are migrating:
/// its keys moved — each with its database — and the databases it has to see
/// whole drained. A command that cannot name its keys in advance — a script,
/// `SORT … BY` — drains too.
pub fn plan(db: u8, argv: &[Vec<u8>]) -> (Vec<(u8, Vec<u8>)>, Vec<u8>) {
    let Some(first) = argv.first() else {
        return (Vec::new(), Vec::new());
    };
    let name = String::from_utf8_lossy(first).to_ascii_uppercase();
    let every = || {
        (0..NUM_DBS as u8)
            .filter(|d| migrating(*d))
            .collect::<Vec<u8>>()
    };
    let has = |w: &str| {
        argv[1..]
            .iter()
            .any(|a| a.eq_ignore_ascii_case(w.as_bytes()))
    };
    let int_at = |i: usize| {
        argv.get(i)
            .and_then(|a| std::str::from_utf8(a).ok()?.parse::<u8>().ok())
    };
    let drains: Vec<u8> = match name.as_str() {
        // A script may SELECT another database and touch any key.
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" | "FLUSHALL" => {
            every()
        }
        "KEYS" | "SCAN" | "DBSIZE" | "RANDOMKEY" | "FLUSHDB" => vec![db],
        "SORT" | "SORT_RO" if has("BY") || has("GET") => vec![db],
        "SWAPDB" => [int_at(1), int_at(2)].into_iter().flatten().collect(),
        _ => Vec::new(),
    };
    let drains: Vec<u8> = drains.into_iter().filter(|d| migrating(*d)).collect();
    let mut keys: Vec<(u8, Vec<u8>)> = crate::admin::get_keys(argv)
        .map(|ks| ks.into_iter().map(|(k, _)| (db, k)).collect())
        .unwrap_or_default();
    // The destination of a move or copy to another database lives there.
    match name.as_str() {
        "MOVE" => {
            if let (Some(k), Some(to)) = (argv.get(1), int_at(2)) {
                keys.push((to, k.clone()));
            }
        }
        "COPY" => {
            let to = argv
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"DB"))
                .and_then(|i| int_at(i + 1));
            if let (Some(dst), Some(to)) = (argv.get(2), to) {
                keys.push((to, dst.clone()));
            }
        }
        _ => {}
    }
    keys.retain(|(d, _)| migrating(*d) && !drains.contains(d));
    (keys, drains)
}

// ──────────────────────────── changing policy ───────────────────────────────

fn store(db: u8, policy: Policy, slot: Option<usize>, from: Option<(Policy, Option<usize>)>) {
    let Some(t) = table() else { return };
    let i = db as usize;
    t.slot[i].store(slot.map_or(0, |s| s as u8 + 1), Ordering::Release);
    t.from_slot[i].store(
        from.and_then(|(_, s)| s).map_or(0, |s| s as u8 + 1),
        Ordering::Release,
    );
    let was = t.from[i].swap(from.map_or(0, |(p, _)| p.code()), Ordering::AcqRel);
    match (was != 0, from.is_some()) {
        (false, true) => {
            t.migrations.fetch_add(1, Ordering::AcqRel);
        }
        (true, false) => {
            t.migrations.fetch_sub(1, Ordering::AcqRel);
        }
        _ => {}
    }
    // Published last: workers route by it.
    t.policy[i].store(policy.code(), Ordering::Release);
}

fn persist(
    client: &mut SpiClient<'_>,
    db: u8,
    policy: Policy,
    slot: Option<usize>,
    from: Option<(Policy, Option<usize>)>,
) -> Result<(), String> {
    let slot = slot.map(|s| s as i16);
    let from_policy = from.map(|(p, _)| p.name());
    let from_slot = from.and_then(|(_, s)| s).map(|s| s as i16);
    client
        .update(
            "INSERT INTO redis.db_policy (db, policy, slot, from_policy, from_slot) VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (db) DO UPDATE SET policy = EXCLUDED.policy, slot = EXCLUDED.slot, \
               from_policy = EXCLUDED.from_policy, from_slot = EXCLUDED.from_slot",
            None,
            &[(db as i16).into(), policy.name().into(), slot.into(), from_policy.into(), from_slot.into()],
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Bring the policies `redis.db_policy` records into shared memory — a
/// migration cut short by a restart included, which carries on. Each worker
/// does it as it starts, before it serves anyone; doing it twice writes the
/// same values.
pub fn load(client: &mut SpiClient<'_>) {
    let present = client
        .select(
            "SELECT to_regclass('redis.db_policy') IS NOT NULL",
            None,
            &[],
        )
        .ok()
        .and_then(|t| t.first().get::<bool>(1).ok().flatten())
        .unwrap_or(false);
    if !present {
        return;
    }
    let Ok(rows) = client.select(
        "SELECT db, policy, slot, from_policy, from_slot FROM redis.db_policy",
        None,
        &[],
    ) else {
        return;
    };
    for row in rows {
        let (Ok(Some(db)), Ok(Some(policy))) = (row.get::<i16>(1), row.get::<String>(2)) else {
            continue;
        };
        let Some(policy) = Policy::parse(&policy) else {
            continue;
        };
        if !(0..NUM_DBS as i16).contains(&db) {
            continue;
        }
        let slot = row.get::<i16>(3).ok().flatten().map(|s| s as usize);
        let from = row
            .get::<String>(4)
            .ok()
            .flatten()
            .and_then(|p| Policy::parse(&p))
            .map(|p| (p, row.get::<i16>(5).ok().flatten().map(|s| s as usize)));
        store(db as u8, policy, slot, from);
    }
}

/// The memory slots no database holds or is leaving.
fn free_slot() -> Option<usize> {
    let mut used = [false; crate::shm::NUM_SLOTS];
    for db in 0..NUM_DBS as u8 {
        if let Some(s) = slot(db) {
            used[s] = true;
        }
        if let Some(At::Memory(s)) = source(db) {
            used[s] = true;
        }
    }
    used.iter().position(|u| !u)
}

/// `redis.set_policy`: give `db` a new policy.
///
/// - Between `memory` and `memory+snapshot` nothing moves: snapshots start,
///   or stop and are dropped.
/// - Between memory and `logged` the data has to move: the database starts
///   migrating, served from the new backend at once, and the workers move its
///   keys over (docs/STORAGE.md). Asking a migrating database for the policy
///   it is leaving turns the migration round.
pub fn set_policy(client: &mut SpiClient<'_>, db: u8, to: Policy) -> Result<Policy, String> {
    let from = policy(db);
    if let Some(src) = source(db) {
        let src_policy =
            Policy::from_code(table().map_or(0, |t| t.from[db as usize].load(Ordering::Acquire)))
                .unwrap_or(Policy::Logged);
        if to == from {
            return Ok(to);
        }
        if to != src_policy {
            return Err(format!(
                "database {db} is moving from {} to {}; let it finish, or ask for {} to turn it round",
                src_policy.name(),
                from.name(),
                src_policy.name()
            ));
        }
        // Round it goes: what was the destination is now the source.
        let (new_slot, src_slot) = (
            match src {
                At::Memory(s) => Some(s),
                At::Tables => None,
            },
            slot(db),
        );
        persist(client, db, to, new_slot, Some((from, src_slot)))?;
        store(db, to, new_slot, Some((from, src_slot)));
        return Ok(to);
    }
    if from == to {
        return Ok(to);
    }
    match (from, to) {
        // The same slot either way: snapshots start, or stop and go.
        (f, t) if f.in_memory() && t.in_memory() => {
            let s = slot(db);
            persist(client, db, to, s, None)?;
            if t == Policy::Snapshot {
                arm_snapshots(db);
            } else {
                drop_snapshot(client, db)?;
            }
            store(db, to, s, None);
        }
        (_, t) if t.in_memory() => {
            let Some(s) = free_slot() else {
                return Err(format!(
                    "every one of the {} memory slots is in use: move a memory database to logged first",
                    crate::shm::NUM_SLOTS
                ));
            };
            persist(client, db, to, Some(s), Some((from, None)))?;
            if t == Policy::Snapshot {
                arm_snapshots(db);
            }
            store(db, to, Some(s), Some((from, None)));
        }
        (f, _) if f.in_memory() => {
            if f == Policy::Snapshot {
                drop_snapshot(client, db)?;
            }
            let src_slot = slot(db);
            persist(client, db, to, None, Some((from, src_slot)))?;
            store(db, to, None, Some((from, src_slot)));
        }
        _ => unreachable!("every other pair is equal or handled above"),
    }
    Ok(to)
}

/// Each database's policy, and the one it is moving from.
pub fn status() -> Vec<(u8, Policy, Option<Policy>)> {
    (0..NUM_DBS as u8)
        .map(|db| {
            let from = table()
                .and_then(|t| Policy::from_code(t.from[db as usize].load(Ordering::Acquire)));
            (db, policy(db), from)
        })
        .collect()
}

// ───────────────────────────────── snapshots ─────────────────────────────────
//
// `memory+snapshot` (docs/STORAGE.md, phase 4): the slot is copied into
// `redis.snapshot` on an interval and reloaded from it when the server
// starts. Unlike Redis's forked RDB the copy is not one instant — each key is
// read whole, but keys written while it runs may land on either side of it.

/// A database that has just become `memory+snapshot` has its data in memory
/// already, nothing to reload; its first snapshot is due now.
fn arm_snapshots(db: u8) {
    if let Some(t) = table() {
        t.restored[db as usize].store(2, Ordering::Release);
        t.last_snapshot_ms[db as usize].store(0, Ordering::Release);
    }
}

fn drop_snapshot(client: &mut SpiClient<'_>, db: u8) -> Result<(), String> {
    for sql in [
        "DELETE FROM redis.snapshot WHERE db = $1",
        "DELETE FROM redis.snapshot_taken WHERE db = $1",
    ] {
        client
            .update(sql, None, &[(db as i16).into()])
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Replace `db`'s snapshot with the slot as it is now. Returns how many keys
/// it holds.
fn take_snapshot(client: &mut SpiClient<'_>, db: u8, slot: usize) -> usize {
    // A `SAVE` and a worker's round can snapshot the same database at once,
    // and `SAVE`'s transaction outlives the round's claim. Take turns until
    // commit, so the second one's DELETE sees the first one's rows rather
    // than inserting beside them.
    let _ = client.update(
        "SELECT pg_advisory_xact_lock(43, $1)",
        None,
        &[(db as i32).into()],
    );
    let keys = crate::shm::with_db(slot, |m| m.keys_upto(usize::MAX)).unwrap_or_default();
    let mut names: Vec<Option<Vec<u8>>> = Vec::with_capacity(keys.len());
    let mut payloads: Vec<Option<Vec<u8>>> = Vec::with_capacity(keys.len());
    let mut deadlines: Vec<i64> = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some((dump, deadline)) = dump_at(client, db, At::Memory(slot), &key) {
            payloads.push(Some(crate::rdb::serialize(&dump)));
            deadlines.push(deadline);
            names.push(Some(key));
        }
    }
    let _ = client.update(
        "DELETE FROM redis.snapshot WHERE db = $1",
        None,
        &[(db as i16).into()],
    );
    let n = names.len();
    let mut at = 0;
    while at < n {
        let end = (at + 1000).min(n);
        let _ = client.update(
            "INSERT INTO redis.snapshot (db, key, payload, deadline_us) \
             SELECT $1, k, p, d FROM unnest($2::bytea[], $3::bytea[], $4::bigint[]) AS t(k, p, d)",
            None,
            &[
                (db as i16).into(),
                names[at..end].to_vec().into(),
                payloads[at..end].to_vec().into(),
                deadlines[at..end].to_vec().into(),
            ],
        );
        at = end;
    }
    let _ = client.update(
        "INSERT INTO redis.snapshot_taken (db, taken_at, keys) VALUES ($1, now(), $2) \
         ON CONFLICT (db) DO UPDATE SET taken_at = EXCLUDED.taken_at, keys = EXCLUDED.keys",
        None,
        &[(db as i16).into(), (n as i64).into()],
    );
    n
}

/// The snapshot databases that may be saved now: in a slot, filled back in
/// after the start, and not mid-move.
fn snapshot_slot(db: u8) -> Option<usize> {
    let t = table()?;
    if policy(db) != Policy::Snapshot
        || migrating(db)
        || t.restored[db as usize].load(Ordering::Acquire) != 2
    {
        return None;
    }
    slot(db)
}

fn saved(db: u8) {
    let now = crate::server::now_ms();
    if let Some(t) = table() {
        t.last_snapshot_ms[db as usize].store(now, Ordering::Release);
    }
    if let Some(c) = crate::server::ctl() {
        c.lastsave.store(now / 1000, Ordering::Relaxed);
    }
}

/// Save every snapshot database whose interval has passed, or that asked for
/// it. Each is claimed first, so of all the workers only one saves it.
pub fn snapshots_round() {
    let Some(t) = table() else { return };
    let interval_ms = crate::SNAPSHOT_INTERVAL.get().max(1) as i64 * 1000;
    for db in 0..NUM_DBS as u8 {
        let Some(s) = snapshot_slot(db) else { continue };
        let last = t.last_snapshot_ms[db as usize].load(Ordering::Acquire);
        if last != 0 && crate::server::now_ms() - last < interval_ms {
            continue;
        }
        if t.snapshot_busy[db as usize]
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            continue;
        }
        // Shared, like a command: the snapshot falls between scripts and
        // transactions on the database, never inside one. While one holds it
        // the snapshot waits for a later round rather than for the lock.
        let Some(held) = crate::dblock::try_hold_shared(db) else {
            t.snapshot_busy[db as usize].store(0, Ordering::Release);
            continue;
        };
        BackgroundWorker::transaction(|| Spi::connect_mut(|c| take_snapshot(c, db, s)));
        drop(held);
        saved(db);
        t.snapshot_busy[db as usize].store(0, Ordering::Release);
    }
}

/// A worker is about to serve.
pub fn worker_started() {
    if let Some(t) = table() {
        t.workers_alive.fetch_add(1, Ordering::AcqRel);
    }
}

/// A worker leaving cleanly: shutdown, or `redis.remove_workers`. The last
/// one out saves every snapshot database, after every worker has stopped
/// writing to it. On a shutdown PostgreSQL waits for this before its
/// checkpoint and streams the WAL to standbys before it stops, so a planned
/// switchover loses nothing from a `memory+snapshot` database. A crash takes
/// no snapshot; the database reloads the last one taken.
pub fn worker_exiting() {
    let Some(t) = table() else { return };
    if t.workers_alive.fetch_sub(1, Ordering::AcqRel) != 1 {
        return;
    }
    for db in 0..NUM_DBS as u8 {
        let Some(s) = snapshot_slot(db) else { continue };
        let taken = crate::pg::transaction_or_abort(|| {
            Spi::connect_mut(|c| take_snapshot(c, db, s));
            Some(())
        });
        match taken {
            Some(()) => {
                saved(db);
                pgrx::log!("pg_redis: database {db} snapshotted at shutdown");
            }
            None => pgrx::warning!("pg_redis: database {db} could not be snapshotted at shutdown"),
        }
    }
}

/// `SAVE`: every snapshot database, now, inside the caller's transaction.
/// False when there is none, which leaves `LASTSAVE` to the caller.
pub fn save_now(client: &mut SpiClient<'_>) -> bool {
    let mut any = false;
    for db in 0..NUM_DBS as u8 {
        if let Some(s) = snapshot_slot(db) {
            take_snapshot(client, db, s);
            saved(db);
            any = true;
        }
    }
    any
}

/// `BGSAVE`: every snapshot database is due at the workers' next round.
pub fn request_snapshots() -> bool {
    let mut any = false;
    if let Some(t) = table() {
        for db in 0..NUM_DBS as u8 {
            if snapshot_slot(db).is_some() {
                t.last_snapshot_ms[db as usize].store(0, Ordering::Release);
                any = true;
            }
        }
    }
    any
}

/// Fill every snapshot database's slot back in from its snapshot, once per
/// start: the first worker to arrive does it and the others wait for it, so
/// none serves the database half loaded. A worker that comes up later, with
/// the slots already filled, finds the work done.
pub fn restore_snapshots() {
    let Some(t) = table() else { return };
    for db in 0..NUM_DBS as u8 {
        if policy(db) != Policy::Snapshot {
            continue;
        }
        let Some(s) = slot(db) else { continue };
        let flag = &t.restored[db as usize];
        if flag
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let n =
                BackgroundWorker::transaction(|| Spi::connect_mut(|c| restore_snapshot(c, db, s)));
            pgrx::log!("pg_redis: database {db} reloaded {n} keys from its snapshot");
            saved(db);
            flag.store(2, Ordering::Release);
        } else {
            let until = std::time::Instant::now() + std::time::Duration::from_secs(120);
            while flag.load(Ordering::Acquire) != 2 && std::time::Instant::now() < until {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

fn restore_snapshot(client: &mut SpiClient<'_>, db: u8, slot: usize) -> usize {
    let now_us = crate::server::now_ms() * 1000;
    let Ok(rows) = client.select(
        "SELECT key, payload, deadline_us FROM redis.snapshot WHERE db = $1",
        None,
        &[(db as i16).into()],
    ) else {
        return 0;
    };
    let rows: Vec<(Vec<u8>, Vec<u8>, i64)> = rows
        .filter_map(|r| {
            Some((
                r.get::<Vec<u8>>(1).ok()??,
                r.get::<Vec<u8>>(2).ok()??,
                r.get::<i64>(3).ok().flatten().unwrap_or(0),
            ))
        })
        .collect();
    let mut n = 0;
    for (key, payload, deadline) in rows {
        if deadline != 0 && deadline <= now_us {
            continue;
        }
        let Ok(dump) = crate::rdb::deserialize(&payload) else {
            continue;
        };
        clear_at(client, db, At::Memory(slot), &key);
        restore_at(client, db, At::Memory(slot), &key, &dump, deadline);
        n += 1;
    }
    n
}

/// Empty every memory slot — `FLUSHALL`'s share of the memory half.
pub fn flush_memory() {
    for s in 0..crate::shm::NUM_SLOTS {
        let _ = crate::shm::with_db(s, |m| m.clear_all());
    }
}
