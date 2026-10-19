//! Client-side caching: `CLIENT TRACKING`, `CLIENT CACHING`, and the
//! invalidation messages they ask for.
//!
//! A tracking client's reads are remembered in a table of key → readers in
//! the pub/sub heap — every worker's writes have to find them — and a write
//! to a remembered key sends each reader one invalidation and forgets the
//! key, as Redis does. The table holds whole key names with no fixed size;
//! `tracking-table-max-keys` bounds it as in Redis. In `BCAST` mode nothing is remembered: every
//! write to a key under one of the client's prefixes is announced, one
//! message per prefix per command or transaction.
//!
//! The messages travel the pub/sub machinery, on a channel of the target
//! client's own (`\x01track:<id>`, internal like the sharded channels). A
//! RESP3 client listens on it from the moment it turns tracking on; any
//! client in pub/sub mode listens on it too, which is what a RESP2
//! `REDIRECT` target is. The frame — an `invalidate` push, or a message on
//! `__redis__:invalidate` — is chosen when it is delivered, by the protocol
//! the receiving connection speaks then.
#![forbid(unsafe_code)]

use std::cell::Cell;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::commands::{Response, Value};
use crate::pubheap::{HVec, PubHeap, Shared, catching};
use crate::server;

pub const ON: u32 = 1;
pub const BCAST: u32 = 2;
pub const OPTIN: u32 = 4;
pub const OPTOUT: u32 = 8;
pub const NOLOOP: u32 = 16;
/// `CLIENT CACHING yes|no`, which lasts for the next command.
pub const CACHING: u32 = 32;
pub const BROKEN_REDIR: u32 = 64;

/// The channel `__redis__:invalidate` subscribers and RESP3 trackers listen on
/// for client `id`.
pub const TRACK_PREFIX: &[u8] = b"\x01track:";

pub fn channel(id: u64) -> Vec<u8> {
    [TRACK_PREFIX, id.to_string().as_bytes()].concat()
}

thread_local! {
    /// The client whose command the dispatcher is running: NOLOOP skips the
    /// invalidations a client's own writes would send it.
    static ORIGIN: Cell<u64> = const { Cell::new(0) };
}

pub fn set_origin(id: u64) {
    ORIGIN.with(|o| o.set(id));
}

pub fn origin() -> u64 {
    ORIGIN.with(|o| o.get())
}

// ─────────────────────────────── messages ───────────────────────────────────

/// What a tracking message carries: keys, a flush (every key), or the news
/// that the redirect target is gone.
pub enum Message {
    Keys(Vec<Vec<u8>>),
    Flush,
    RedirectBroken(u64),
}

impl Message {
    fn encode(&self) -> Vec<u8> {
        match self {
            Message::Keys(keys) => {
                let mut out = vec![b'K'];
                for k in keys {
                    out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    out.extend_from_slice(k);
                }
                out
            }
            Message::Flush => vec![b'F'],
            Message::RedirectBroken(id) => [b"B".as_slice(), &id.to_le_bytes()].concat(),
        }
    }

    pub fn decode(payload: &[u8]) -> Option<Message> {
        match payload.first()? {
            b'K' => {
                let mut keys = Vec::new();
                let mut rest = &payload[1..];
                while rest.len() >= 4 {
                    let n = u32::from_le_bytes(rest[..4].try_into().ok()?) as usize;
                    let k = rest.get(4..4 + n)?;
                    keys.push(k.to_vec());
                    rest = &rest[4 + n..];
                }
                Some(Message::Keys(keys))
            }
            b'F' => Some(Message::Flush),
            b'B' => Some(Message::RedirectBroken(u64::from_le_bytes(
                payload.get(1..9)?.try_into().ok()?,
            ))),
            _ => None,
        }
    }
}

/// `sendTrackingMessage`: to the tracker's redirect target, or the tracker.
fn send(tracker: u64, msg: Message) {
    let Some(slot) = server::slot_of(tracker) else {
        return;
    };
    let redirect = slot.track_redirect.load(Ordering::Relaxed);
    let target = if redirect != 0 {
        if server::slot_of(redirect).is_none() {
            slot.tracking.fetch_or(BROKEN_REDIR, Ordering::Relaxed);
            crate::pubsub::shared::publish(
                &channel(tracker),
                &Message::RedirectBroken(redirect).encode(),
            );
            return;
        }
        redirect
    } else {
        tracker
    };
    crate::pubsub::shared::publish(&channel(target), &msg.encode());
}

/// How a delivered tracking message is framed for the connection receiving
/// it, in the protocol it speaks now: `(kind, value)` for a RESP3 push, or the
/// `__redis__:invalidate` message a RESP2 subscriber gets. `None` when the
/// protocol has no frame for it.
pub fn frame(payload: &[u8], proto: u8) -> Option<(&'static str, Value)> {
    let msg = Message::decode(payload)?;
    let value = match msg {
        Message::Keys(keys) => Value::Array(keys.into_iter().map(Value::Bulk).collect()),
        Message::Flush => Value::Nil,
        Message::RedirectBroken(id) if proto == 3 => {
            return Some(("tracking-redir-broken", Value::Int(id as i64)));
        }
        Message::RedirectBroken(_) => return None,
    };
    Some(if proto == 3 {
        ("invalidate", value)
    } else {
        ("message", value)
    })
}

// ──────────────────────────────── the table ─────────────────────────────────

/// A tracked key and the clients that read it since it last changed.
struct Entry {
    key: HVec<u8>,
    readers: HVec<u64>,
}

/// Redis's `TrackingTable`: every key a tracking client read, with who read
/// it, in the pub/sub heap so every worker's writes find it. The entries are
/// kept dense so eviction can pick one at random; the index finds a key's
/// entry by name.
///
/// As in Redis, turning tracking off forgets nothing: a key's stale readers
/// go when the key is invalidated, and are skipped then if they no longer
/// track it.
pub struct Table {
    entries: HVec<Entry>,
    index: hashbrown::HashTable<u32, PubHeap>,
    hasher: RandomState,
    /// (key, client) pairs: `TrackingTableTotalItems`.
    items: usize,
}

fn hash(hasher: &RandomState, key: &[u8]) -> u64 {
    hasher.hash_one(key)
}

impl Table {
    pub(crate) fn new() -> Table {
        Table::with_hasher(RandomState::new())
    }

    fn with_hasher(hasher: RandomState) -> Table {
        Table {
            entries: HVec::new_in(PubHeap),
            index: hashbrown::HashTable::new_in(PubHeap),
            hasher,
            items: 0,
        }
    }

    fn find(&self, key: &[u8]) -> Option<usize> {
        let entries = &self.entries;
        self.index
            .find(hash(&self.hasher, key), |&i| {
                entries[i as usize].key[..] == *key
            })
            .map(|&i| i as usize)
    }

    /// Note that `client` read `key`. Every allocation comes before any
    /// change, so a full heap (a panic) leaves the table as it was.
    fn remember(&mut self, client: u64, key: &[u8]) {
        if let Some(i) = self.find(key) {
            let readers = &mut self.entries[i].readers;
            if !readers.contains(&client) {
                readers.push(client);
                self.items += 1;
            }
            return;
        }
        let mut name = HVec::with_capacity_in(key.len(), PubHeap);
        name.extend_from_slice(key);
        let mut readers = HVec::with_capacity_in(1, PubHeap);
        readers.push(client);
        let Table {
            entries,
            index,
            hasher,
            ..
        } = self;
        entries.reserve(1);
        index.reserve(1, |&i| hash(hasher, &entries[i as usize].key));
        index.insert_unique(hash(hasher, key), entries.len() as u32, |&i| {
            hash(hasher, &entries[i as usize].key)
        });
        entries.push(Entry { key: name, readers });
        self.items += 1;
    }

    /// Forget `key`, returning who read it.
    fn take(&mut self, key: &[u8]) -> Vec<u64> {
        let Some(i) = self.find(key) else {
            return Vec::new();
        };
        let h = hash(&self.hasher, key);
        if let Ok(e) = self.index.find_entry(h, |&j| j as usize == i) {
            e.remove();
        }
        let gone = self.entries.swap_remove(i);
        // The last entry moved into `i`: point its index there.
        if let Some(moved) = self.entries.get(i) {
            let from = self.entries.len() as u32;
            if let Some(j) = self
                .index
                .find_mut(hash(&self.hasher, &moved.key), |&j| j == from)
            {
                *j = i as u32;
            }
        }
        self.items -= gone.readers.len();
        gone.readers.to_vec()
    }

    /// A tracked key chosen by `r`, for eviction.
    fn pick(&self, r: usize) -> Option<Vec<u8>> {
        (!self.entries.is_empty()).then(|| self.entries[r % self.entries.len()].key.to_vec())
    }
}

fn table() -> Option<&'static Shared<Table>> {
    crate::pubheap::root().map(|r| &r.tracking)
}

/// `trackingRememberKeys`. A heap too full for the pair makes room by
/// invalidating tracked keys picked at random, as the table's own limit
/// does: dropping the read instead would leave the client caching a key it
/// is never told about.
fn remember(client: u64, keys: &[Vec<u8>]) {
    let Some(t) = table() else { return };
    for key in keys {
        while catching(|| t.lock().remember(client, key)).is_none() {
            if !evict_one() {
                break;
            }
        }
    }
}

/// Invalidate one tracked key picked at random, as if it had been written:
/// its readers are told, and it is forgotten. False when nothing is tracked.
fn evict_one() -> bool {
    let Some(t) = table() else { return false };
    let Some(key) = t.lock().pick(crate::memdb::fast_random() as usize) else {
        return false;
    };
    invalidate_key(&key, 0);
    true
}

/// `trackingInvalidateKey` for the table: forget `key`, and tell each client
/// that read it, unless it no longer tracks, has since switched to BCAST, or
/// wrote the key itself with NOLOOP on.
fn invalidate_key(key: &[u8], origin: u64) {
    let Some(t) = table() else { return };
    let readers = t.lock().take(key);
    for id in readers {
        let Some(slot) = server::slot_of(id) else {
            continue;
        };
        let flags = slot.tracking.load(Ordering::Relaxed);
        if flags & (ON | BCAST) != ON || (flags & NOLOOP != 0 && id == origin) {
            continue;
        }
        send(id, Message::Keys(vec![key.to_vec()]));
    }
}

fn clear_table() {
    let Some(t) = table() else { return };
    let old = {
        let mut table = t.lock();
        let hasher = table.hasher.clone();
        std::mem::replace(&mut *table, Table::with_hasher(hasher))
    };
    // Let go of the old entries without holding the lock.
    drop(old);
}

/// Runs in a row that ended with the table still over its limit; each one
/// tries harder, as Redis's `timeout_counter`.
static OVER_LIMIT_RUNS: AtomicUsize = AtomicUsize::new(0);

/// `trackingLimitUsedSlots`: while the table holds more keys than
/// `tracking-table-max-keys`, invalidate keys picked at random, as if each
/// had been written, so their readers are told. At most 100 keys a run, and
/// 100 more for every run before it that could not get under the limit.
pub fn limit_used_slots() {
    if !server::tracking_active() {
        return;
    }
    let max = crate::config::get_i64("tracking-table-max-keys");
    let over = || table_counts().1 as i64 > max;
    if max <= 0 || !over() {
        OVER_LIMIT_RUNS.store(0, Ordering::Relaxed);
        return;
    }
    let effort = 100 * (OVER_LIMIT_RUNS.load(Ordering::Relaxed) + 1);
    for _ in 0..effort {
        if !evict_one() || !over() {
            OVER_LIMIT_RUNS.store(0, Ordering::Relaxed);
            return;
        }
    }
    OVER_LIMIT_RUNS.fetch_add(1, Ordering::Relaxed);
}

/// INFO's `tracking_total_items` and `tracking_total_keys`.
pub fn table_counts() -> (usize, usize) {
    table().map_or((0, 0), |t| {
        let t = t.lock();
        (t.items, t.entries.len())
    })
}

/// INFO's `tracking_total_prefixes`: distinct prefixes across BCAST clients.
pub fn prefix_count() -> usize {
    let mut all = std::collections::HashSet::new();
    for c in server::clients() {
        if let Some(slot) = server::slot_of(c.id)
            && slot.tracking.load(Ordering::Relaxed) & BCAST != 0
        {
            all.extend(slot.prefixes());
        }
    }
    all.len()
}

// ─────────────────────────────── commands ───────────────────────────────────

fn err(msg: impl Into<String>) -> Response {
    Response::Error(msg.into())
}

/// `CLIENT TRACKING on|off [REDIRECT id] [BCAST] [PREFIX p ...] [OPTIN]
/// [OPTOUT] [NOLOOP]`, with Redis's checks in Redis's order.
pub fn client_tracking(client: &server::ClientHandle, args: &[Vec<u8>]) -> Response {
    let Some(slot) = client.slot() else {
        return Response::Ok;
    };
    let mut redirect: u64 = 0;
    let mut options = 0;
    let mut prefixes: Vec<Vec<u8>> = Vec::new();
    let mut j = 1;
    while j < args.len() {
        let a = &args[j];
        let more = j + 1 < args.len();
        if a.eq_ignore_ascii_case(b"REDIRECT") && more {
            j += 1;
            if redirect != 0 {
                return err("ERR A client can only redirect to a single other client");
            }
            let Some(id) = std::str::from_utf8(&args[j])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            else {
                return err("ERR value is not an integer or out of range");
            };
            if id <= 0 || server::slot_of(id as u64).is_none() {
                return err("ERR The client ID you want redirect to does not exist");
            }
            redirect = id as u64;
        } else if a.eq_ignore_ascii_case(b"BCAST") {
            options |= BCAST;
        } else if a.eq_ignore_ascii_case(b"OPTIN") {
            options |= OPTIN;
        } else if a.eq_ignore_ascii_case(b"OPTOUT") {
            options |= OPTOUT;
        } else if a.eq_ignore_ascii_case(b"NOLOOP") {
            options |= NOLOOP;
        } else if a.eq_ignore_ascii_case(b"PREFIX") && more {
            j += 1;
            prefixes.push(args[j].clone());
        } else {
            return err("ERR syntax error");
        }
        j += 1;
    }
    let current = slot.tracking.load(Ordering::Relaxed);
    if args[0].eq_ignore_ascii_case(b"OFF") {
        disable(client);
        return Response::Ok;
    }
    if !args[0].eq_ignore_ascii_case(b"ON") {
        return err("ERR syntax error");
    }
    if options & BCAST == 0 && !prefixes.is_empty() {
        return err("ERR PREFIX option requires BCAST mode to be enabled");
    }
    if current & ON != 0 && (current & BCAST != 0) != (options & BCAST != 0) {
        return err(
            "ERR You can't switch BCAST mode on/off before disabling tracking for this client, \
             and then re-enabling it with a different mode.",
        );
    }
    if options & BCAST != 0 && options & (OPTIN | OPTOUT) != 0 {
        return err("ERR OPTIN and OPTOUT are not compatible with BCAST");
    }
    if options & OPTIN != 0 && options & OPTOUT != 0 {
        return err("ERR You can't specify both OPTIN mode and OPTOUT mode");
    }
    if (options & OPTIN != 0 && current & OPTOUT != 0)
        || (options & OPTOUT != 0 && current & OPTIN != 0)
    {
        return err(
            "ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, \
             and then re-enabling it with a different mode.",
        );
    }
    let mut existing = if current & BCAST != 0 {
        slot.prefixes()
    } else {
        Vec::new()
    };
    if options & BCAST != 0 {
        if let Some(e) = prefix_collision(&existing, &prefixes) {
            return err(e);
        }
        if prefixes.is_empty() {
            prefixes.push(Vec::new());
        }
        for p in prefixes {
            if !existing.contains(&p) {
                existing.push(p);
            }
        }
    }
    if current & ON == 0
        && let Some(ctl) = server::ctl()
    {
        ctl.tracking_clients.fetch_add(1, Ordering::Relaxed);
    }
    client.set_prefixes(&existing);
    slot.track_redirect.store(redirect, Ordering::Relaxed);
    slot.tracking.store(ON | options, Ordering::Relaxed);
    client.set_flag(server::flag::TRACKING, true);
    if redirect != 0 {
        offer_listener(redirect);
    }
    Response::Ok
}

/// A RESP3 client receives the invalidations redirected to it as push
/// messages, with no subscription of its own. One not yet listening on its
/// channel is handed a pub/sub slot subscribed to it, which holds whatever
/// arrives until the connection takes it over between commands — so none
/// is lost in between.
fn offer_listener(target: u64) {
    let Some(t) = server::slot_of(target) else {
        return;
    };
    if t.proto.load(Ordering::Relaxed) != 3
        || t.track_listening.load(Ordering::Acquire) != 0
        || t.track_handoff.load(Ordering::Acquire) != server::HANDOFF_NONE
    {
        return;
    }
    let Some(slots) = crate::pubsub_state() else {
        return;
    };
    let channel = [channel(target)];
    let Some((idx, _)) = crate::pubsub::slot_alloc_and_subscribe(slots, target, &channel) else {
        return;
    };
    let handed = t
        .track_handoff
        .compare_exchange(
            server::HANDOFF_NONE,
            idx as i32,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok();
    // Lost the race, or the client went and its slot was taken by another
    // in the meantime: take the offer back.
    let withdrawn = !handed
        || (t.id.load(Ordering::Acquire) != target
            && t.track_handoff
                .compare_exchange(
                    idx as i32,
                    server::HANDOFF_NONE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok());
    if withdrawn {
        crate::pubsub::slot_free(slots, idx);
    }
}

/// `checkPrefixCollisionsOrReply`.
fn prefix_collision(existing: &[Vec<u8>], new: &[Vec<u8>]) -> Option<String> {
    let overlap = |a: &[u8], b: &[u8]| a.starts_with(b) || b.starts_with(a);
    let s = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    for (i, p) in new.iter().enumerate() {
        if let Some(e) = existing.iter().find(|e| overlap(e, p)) {
            return Some(format!(
                "ERR Prefix '{}' overlaps with an existing prefix '{}'. Prefixes for a single client must not overlap.",
                s(p),
                s(e)
            ));
        }
        if let Some(q) = new[i + 1..].iter().find(|q| overlap(p, q)) {
            return Some(format!(
                "ERR Prefix '{}' overlaps with another provided prefix '{}'. Prefixes for a single client must not overlap.",
                s(p),
                s(q)
            ));
        }
    }
    None
}

/// Turn tracking off. What the client read stays in the table, as in
/// Redis, and is skipped when those keys change.
pub fn disable(client: &server::ClientHandle) {
    let Some(slot) = client.slot() else { return };
    if slot.tracking.swap(0, Ordering::Relaxed) & ON != 0
        && let Some(ctl) = server::ctl()
    {
        ctl.tracking_clients.fetch_sub(1, Ordering::Relaxed);
    }
    slot.track_redirect.store(0, Ordering::Relaxed);
    client.set_prefixes(&[]);
    client.set_flag(server::flag::TRACKING, false);
}

/// `CLIENT CACHING yes|no`.
pub fn client_caching(client: &server::ClientHandle, arg: &[u8]) -> Response {
    let Some(slot) = client.slot() else {
        return Response::Ok;
    };
    let t = slot.tracking.load(Ordering::Relaxed);
    if t & ON == 0 {
        return err(
            "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        );
    }
    if arg.eq_ignore_ascii_case(b"YES") {
        if t & OPTIN == 0 {
            return err(
                "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            );
        }
    } else if arg.eq_ignore_ascii_case(b"NO") {
        if t & OPTOUT == 0 {
            return err(
                "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            );
        }
    } else {
        return err("ERR syntax error");
    }
    slot.tracking.fetch_or(CACHING, Ordering::Relaxed);
    Response::Ok
}

/// `CLIENT GETREDIR`.
pub fn getredir(client: &server::ClientHandle) -> Response {
    match client.slot() {
        Some(s) if s.tracking.load(Ordering::Relaxed) & ON != 0 => {
            Response::Integer(s.track_redirect.load(Ordering::Relaxed) as i64)
        }
        _ => Response::Integer(-1),
    }
}

/// `CLIENT TRACKINGINFO`.
pub fn trackinginfo(client: &server::ClientHandle) -> Response {
    let (t, redirect, prefixes) = match client.slot() {
        Some(s) => (
            s.tracking.load(Ordering::Relaxed),
            s.track_redirect.load(Ordering::Relaxed),
            s.prefixes(),
        ),
        None => (0, 0, Vec::new()),
    };
    let mut flags = vec![Value::str(if t & ON != 0 { "on" } else { "off" })];
    for (bit, name) in [(BCAST, "bcast"), (OPTIN, "optin")] {
        if t & bit != 0 {
            flags.push(Value::str(name));
        }
    }
    if t & OPTIN != 0 && t & CACHING != 0 {
        flags.push(Value::str("caching-yes"));
    }
    if t & OPTOUT != 0 {
        flags.push(Value::str("optout"));
        if t & CACHING != 0 {
            flags.push(Value::str("caching-no"));
        }
    }
    if t & NOLOOP != 0 {
        flags.push(Value::str("noloop"));
    }
    if t & BROKEN_REDIR != 0 {
        flags.push(Value::str("broken_redirect"));
    }
    Response::Value(Value::Map(vec![
        (Value::str("flags"), Value::Set(flags)),
        (
            Value::str("redirect"),
            Value::Int(if t & ON != 0 { redirect as i64 } else { -1 }),
        ),
        (
            Value::str("prefixes"),
            Value::Array(if t & BCAST != 0 {
                prefixes.into_iter().map(Value::Bulk).collect()
            } else {
                Vec::new()
            }),
        ),
    ]))
}

// ─────────────────────────── reads and writes ───────────────────────────────

/// After client `id` ran a read-only command naming `keys`:
/// `trackingRememberKeys`, OPTIN/OPTOUT and CACHING included.
pub fn after_read(id: u64, keys: Vec<Vec<u8>>) {
    let Some(slot) = server::slot_of(id) else {
        return;
    };
    let t = slot.tracking.load(Ordering::Relaxed);
    if t & ON == 0 || t & BCAST != 0 || keys.is_empty() {
        return;
    }
    let caching = t & CACHING != 0;
    if (t & OPTIN != 0 && !caching) || (t & OPTOUT != 0 && caching) {
        return;
    }
    remember(id, &keys);
}

/// The CACHING flag lasts one command (or one transaction), as in Redis.
pub fn command_done(client: &server::ClientHandle, was_client_cmd: bool, in_multi: bool) {
    if let Some(s) = client.slot()
        && !was_client_cmd
        && !in_multi
    {
        s.tracking.fetch_and(!CACHING, Ordering::Relaxed);
    }
}

/// `keys` were written by client `origin` (0 for the server itself):
/// invalidate them for whoever read them, and announce them to every BCAST
/// client watching a prefix they fall under.
pub fn invalidate(keys: &[Vec<u8>], origin: u64) {
    if keys.is_empty() || !server::tracking_active() {
        return;
    }
    let noloop = |id: u64| {
        id == origin
            && server::slot_of(id).is_some_and(|s| s.tracking.load(Ordering::Relaxed) & NOLOOP != 0)
    };
    for key in keys {
        invalidate_key(key, origin);
    }
    for c in server::clients() {
        let Some(slot) = server::slot_of(c.id) else {
            continue;
        };
        if slot.tracking.load(Ordering::Relaxed) & (ON | BCAST) != ON | BCAST || noloop(c.id) {
            continue;
        }
        for prefix in slot.prefixes() {
            let hit: Vec<Vec<u8>> = keys
                .iter()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            if !hit.is_empty() {
                send(c.id, Message::Keys(hit));
            }
        }
    }
}

/// A flush: every tracking client is told every key is gone.
pub fn invalidate_all() {
    if !server::tracking_active() {
        return;
    }
    clear_table();
    for c in server::clients() {
        if server::slot_of(c.id).is_some_and(|s| s.tracking.load(Ordering::Relaxed) & ON != 0) {
            send(c.id, Message::Flush);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip() {
        let keys = vec![b"a".to_vec(), b"".to_vec(), vec![0, 1, 2]];
        match Message::decode(&Message::Keys(keys.clone()).encode()) {
            Some(Message::Keys(k)) => assert_eq!(k, keys),
            _ => panic!("keys did not round trip"),
        }
        assert!(matches!(
            Message::decode(&Message::Flush.encode()),
            Some(Message::Flush)
        ));
        assert!(matches!(
            Message::decode(&Message::RedirectBroken(7).encode()),
            Some(Message::RedirectBroken(7))
        ));
    }

    fn v(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn a_key_remembers_each_reader_once_and_forgets_them_together() {
        crate::pubheap::for_tests();
        let mut t = Table::new();
        t.remember(1, b"k");
        t.remember(1, b"k");
        t.remember(2, b"k");
        t.remember(1, b"other");
        assert_eq!((t.items, t.entries.len()), (3, 2));
        let mut readers = t.take(b"k");
        readers.sort();
        assert_eq!(readers, vec![1, 2]);
        assert_eq!((t.items, t.entries.len()), (1, 1));
        assert!(t.take(b"k").is_empty());
        assert_eq!(t.take(b"other"), vec![1]);
        assert_eq!((t.items, t.entries.len()), (0, 0));
    }

    /// Redis keeps whole key names, however long, and as many as
    /// `tracking-table-max-keys` allows; so does this.
    #[test]
    fn the_table_holds_long_names_and_many_keys() {
        crate::pubheap::for_tests();
        let mut t = Table::new();
        let long = vec![b'x'; 10_000];
        t.remember(7, &long);
        for i in 0..50_000 {
            t.remember(7, format!("key:{i}").as_bytes());
        }
        assert_eq!(t.entries.len(), 50_001);
        assert_eq!(t.pick(0), Some(long.clone()));
        assert_eq!(t.take(&long), vec![7]);
        // Removing entries out of order keeps every other key findable.
        for i in (0..50_000).step_by(3) {
            assert_eq!(t.take(format!("key:{i}").as_bytes()), vec![7]);
        }
        for i in 0..50_000 {
            let found = t.find(format!("key:{i}").as_bytes()).is_some();
            assert_eq!(found, i % 3 != 0, "key:{i}");
        }
        assert_eq!(t.items, t.entries.len());
        assert!(t.pick(12345).is_some());
    }

    #[test]
    fn a_key_names_itself_by_bytes() {
        crate::pubheap::for_tests();
        let mut t = Table::new();
        t.remember(1, &v("a\0b"));
        t.remember(1, b"");
        assert!(t.find(b"a").is_none());
        assert!(t.find(b"a\0b").is_some());
        assert_eq!(t.take(b""), vec![1]);
    }

    #[test]
    fn overlapping_prefixes_are_refused() {
        assert!(prefix_collision(&[], &[b"a".to_vec(), b"ab".to_vec()]).is_some());
        assert!(prefix_collision(&[b"foo".to_vec()], &[b"f".to_vec()]).is_some());
        assert!(prefix_collision(&[], &[b"a:".to_vec(), b"b:".to_vec()]).is_none());
    }
}
