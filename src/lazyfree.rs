//! Redis's lazy freeing, as `INFO memory` reports it.
//!
//! Redis hands a value to a background thread to free when freeing it costs
//! more than [`THRESHOLD`] allocations (`lazyfreeGetFreeEffort`): on
//! `UNLINK`, on `DEL` with `lazyfree-lazy-user-del`, and every key of a
//! `FLUSHDB` or `FLUSHALL` that runs `ASYNC`. pg_redis frees each value
//! where it is deleted, so nothing is ever pending; what it keeps is the
//! count of what Redis would have freed that way, `lazyfreed_objects`.

use crate::commands::{Command, Response, Value};

/// `LAZYFREE_THRESHOLD`.
const THRESHOLD: u64 = 64;

/// Runs a command on database `db` of whichever half serves it, for the
/// lookups the count needs.
pub type Probe<'a> = dyn FnMut(u8, Command) -> Response + 'a;

/// What `cmd` will free lazily if it succeeds, worked out before it runs.
pub fn before(cmd: &Command, db: u8, probe: &mut Probe<'_>) -> u64 {
    match cmd {
        Command::Unlink { keys } => keys
            .iter()
            .filter(|k| effort(k, db, probe) > THRESHOLD)
            .count() as u64,
        Command::Del { keys } if crate::config::get_bool("lazyfree-lazy-user-del") => {
            keys.iter()
                .filter(|k| effort(k, db, probe) > THRESHOLD)
                .count() as u64
        }
        Command::FlushDb { lazy: true } => dbsize(db, probe),
        Command::FlushAll { lazy: true } => (0..crate::commands::NUM_DBS as u8)
            .map(|d| dbsize(d, probe))
            .sum(),
        _ => 0,
    }
}

/// Count what `before` found, now that the command has replied `resp`.
pub fn after(pending: u64, resp: &Response) {
    if pending > 0 && !matches!(resp, Response::Error(_)) {
        crate::server::count_lazyfreed(pending);
    }
}

fn run(db: u8, probe: &mut Probe<'_>, args: &[&[u8]]) -> Option<Value> {
    let cmd = Command::parse(args.iter().map(|a| a.to_vec()).collect::<Vec<_>>()).ok()?;
    match probe(db, cmd).into_value() {
        Value::Error(_) => None,
        v => Some(v),
    }
}

fn text(v: &Value) -> Option<&[u8]> {
    match v {
        Value::Bulk(b) => Some(b),
        Value::Simple(s) => Some(s.as_bytes()),
        _ => None,
    }
}

fn int(v: Option<Value>) -> u64 {
    match v {
        Some(Value::Int(n)) => n.max(0) as u64,
        _ => 0,
    }
}

/// The integer after `name` in a reply of alternating names and values.
fn field(v: &Value, name: &[u8]) -> u64 {
    let items: Vec<&Value> = match v {
        Value::Array(items) => items.iter().collect(),
        Value::Map(pairs) => pairs.iter().flat_map(|(k, v)| [k, v]).collect(),
        _ => return 0,
    };
    items
        .windows(2)
        .find(|w| text(w[0]) == Some(name))
        .map_or(0, |w| int(Some(w[1].clone())))
}

fn dbsize(db: u8, probe: &mut Probe<'_>) -> u64 {
    int(run(db, probe, &[b"DBSIZE"]))
}

/// `lazyfreeGetFreeEffort`: a collection in its large encoding costs one
/// allocation per element (per node, for a list), a stream one per radix
/// tree node and per consumer group entry; anything else one.
fn effort(key: &[u8], db: u8, probe: &mut Probe<'_>) -> u64 {
    let Some(kind) = run(db, probe, &[b"TYPE", key]) else {
        return 0;
    };
    // Each collection's large encoding, and what counts its elements.
    let large: Option<(&[u8], &[u8])> = match text(&kind) {
        Some(b"set") => Some((b"hashtable", b"SCARD")),
        Some(b"hash") => Some((b"hashtable", b"HLEN")),
        Some(b"zset") => Some((b"skiplist", b"ZCARD")),
        _ => None,
    };
    if let Some((encoding, card)) = large {
        let is = run(db, probe, &[b"OBJECT", b"ENCODING", key]);
        return if is.as_ref().and_then(text) == Some(encoding) {
            int(run(db, probe, &[card, key]))
        } else {
            1
        };
    }
    match text(&kind) {
        Some(b"none") | None => 0,
        // A list of no more elements than the threshold has no more nodes.
        Some(b"list") if int(run(db, probe, &[b"LLEN", key])) > THRESHOLD => {
            match run(db, probe, &[b"LRANGE", key, b"0", b"-1"]) {
                Some(Value::Array(items)) => crate::memdb::encoding::list_nodes(
                    items.iter().filter_map(text),
                    &crate::memdb::encoding::Limits::current(),
                ) as u64,
                _ => 1,
            }
        }
        Some(b"stream") => {
            let Some(info) = run(db, probe, &[b"XINFO", b"STREAM", key]) else {
                return 1;
            };
            let groups = field(&info, b"groups");
            // Redis takes the first group's PEL as every group's.
            let pel = match run(db, probe, &[b"XINFO", b"GROUPS", key]) {
                Some(Value::Array(gs)) => gs.first().map_or(0, |g| field(g, b"pending")),
                _ => 0,
            };
            field(&info, b"radix-tree-nodes") + groups * (1 + pel)
        }
        Some(_) => 1,
    }
}
