//! `keyspace_hits` and `keyspace_misses`, and the `keymiss` event: what
//! Redis's `lookupKey` counts for every read lookup of a key (`db.c`). A
//! lookup made to write (`lookupKeyWrite`) counts nothing, so the keys a
//! command reads are the ones its key specs mark `RO`, plus the key of the
//! `GET` family, which reads its key first whatever it then writes.
//!
//! A key counts as a hit when it is there, whatever the command finds in it:
//! `HGET` of a missing field in a hash that exists is a hit. Most replies say
//! so already, and only a reply that cannot tell (empty, zero, nil, or one
//! that answers the same with or without the key) costs a look at the key.
#![forbid(unsafe_code)]

use crate::commands::Response;

/// The keys one command line reads, and its name, worked out on the
/// connection thread from the line itself and carried with the command.
#[derive(Clone, Debug)]
pub struct Lookups {
    pub name: String,
    pub keys: Vec<Vec<u8>>,
}

/// The read lookups `argv` makes, or `None` for a command that makes none.
pub fn lookups(argv: &[Vec<u8>]) -> Option<Lookups> {
    let name = String::from_utf8_lossy(argv.first()?).to_ascii_uppercase();
    // `SCAN ... TYPE` names no key: the keys its step finds are counted as
    // the store looks them up (`scanned_types`).
    if name == "SCAN" {
        let typed = argv.iter().skip(2).any(|a| a.eq_ignore_ascii_case(b"TYPE"));
        return typed.then(|| Lookups {
            name,
            keys: Vec::new(),
        });
    }
    // A read-only script's keys are looked up by the commands it runs.
    if matches!(name.as_str(), "EVAL_RO" | "EVALSHA_RO" | "FCALL_RO") {
        return None;
    }
    // `getGenericCommand` and `GETEX` read the key with `lookupKeyRead`.
    let get_family = match name.as_str() {
        "GETDEL" | "GETEX" | "GETSET" => true,
        "SET" => argv.iter().skip(3).any(|a| a.eq_ignore_ascii_case(b"GET")),
        _ => false,
    };
    let keys: Vec<Vec<u8>> = if get_family {
        vec![argv.get(1)?.clone()]
    } else {
        crate::admin::get_keys(argv)
            .ok()?
            .into_iter()
            .filter(|(_, flags)| flags.contains(&"RO"))
            .map(|(k, _)| k)
            .collect()
    };
    (!keys.is_empty()).then_some(Lookups { name, keys })
}

/// What one command's lookups found: added to the counters only once the
/// command's run is the one that stands (a batch rolled back and run again
/// counts once).
#[derive(Default, Debug)]
pub struct Tally {
    db: u8,
    hits: u64,
    /// The keys missed, for the `keymiss` event.
    missed: Vec<Vec<u8>>,
    notify: bool,
}

/// Replies the same with or without the key, or one too plain to say: these
/// always look.
const ALWAYS_LOOK: &[&str] = &[
    "HMGET",
    "SMISMEMBER",
    "ZMSCORE",
    "GEOPOS",
    "GEOHASH",
    "SSCAN",
    "HSCAN",
    "ZSCAN",
    "BITPOS",
];

thread_local! {
    /// What the `SCAN ... TYPE` that just ran looked up: keys that were
    /// there, keys past their deadline.
    static SCANNED: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

/// `SCAN ... TYPE` reads every key its step found, after MATCH, to compare
/// its type, and Redis counts each lookup: a hit for one that is there
/// whatever its type, a miss for one past its deadline, never a `keymiss`
/// (`LOOKUP_NONOTIFY`). Called by the scan, read by its tally.
pub fn scanned_types(hits: u64, misses: u64) {
    SCANNED.set((hits, misses));
}

/// Answer nil, nothing or 0 only without the key: `GET` meets another type
/// with WRONGTYPE, and Redis keeps no empty collection.
const EMPTY_IS_ABSENT: &[&str] = &[
    "GET", "SMEMBERS", "HGETALL", "HKEYS", "HVALS", "HLEN", "LLEN", "SCARD", "ZCARD", "DUMP",
];

/// Whether `reply` could only have come from a key that is there.
fn proves_key(reply: &Response) -> bool {
    match reply {
        Response::BulkString(b) => !b.is_empty(),
        Response::Integer(n) => *n != 0,
        Response::Array(items) => !items.is_empty(),
        Response::Null | Response::NullArray | Response::Error(_) => false,
        // Anything richer (a map, a double, a nested array) is a value read
        // from a key.
        _ => true,
    }
}

/// Whether each of a database's keys is there, looked at without touching.
pub type ExistsMany<'a> = dyn FnMut(u8, &[Vec<u8>]) -> Vec<bool> + 'a;

/// Count `l`'s lookups of `db`, run with reply `reply`. `exists` looks at
/// keys all in one go: a multi-key read of keys that are not there (an `MGET`
/// of a thousand) must not cost a statement each.
pub fn tally(db: u8, l: &Lookups, reply: &Response, exists: &mut ExistsMany<'_>) -> Tally {
    if l.name == "SCAN" {
        let (hits, misses) = SCANNED.take();
        return Tally {
            db,
            hits,
            missed: vec![Vec::new(); misses as usize],
            notify: false,
        };
    }
    let mut t = Tally {
        db,
        notify: l.name != "OBJECT",
        ..Tally::default()
    };
    let mut found = |key: &[u8], there: bool| {
        if there {
            t.hits += 1;
        } else {
            t.missed.push(key.to_vec());
        }
    };
    let single = l.keys.len() == 1;
    let by_reply = match (l.name.as_str(), reply) {
        // They write the key after reading it, so only the reply says what
        // the read found: nil for no key, a value or WRONGTYPE for one.
        ("GETDEL" | "GETEX" | "GETSET" | "SET", r) => Some(!matches!(r, Response::Null)),
        ("TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME", Response::Integer(n)) => Some(*n != -2),
        ("TYPE", Response::SimpleString(s)) => Some(s != "none"),
        ("OBJECT", Response::Null) => Some(false),
        ("EXISTS" | "TOUCH", Response::Integer(n)) if single => Some(*n > 0),
        _ if single && !ALWAYS_LOOK.contains(&l.name.as_str()) && proves_key(reply) => Some(true),
        (name, r)
            if single && EMPTY_IS_ABSENT.contains(&name) && !matches!(r, Response::Error(_)) =>
        {
            Some(false)
        }
        _ => None,
    };
    // `MGET` names its keys in order: a value is a string that is there, and
    // only a nil (no key, or another type) needs a look.
    let unknown: Vec<Vec<u8>> = match (l.name.as_str(), reply, by_reply) {
        (_, _, Some(there)) if single => {
            found(&l.keys[0], there);
            Vec::new()
        }
        ("MGET", Response::Array(items), _) if items.len() == l.keys.len() => {
            let mut unknown = Vec::new();
            for (key, item) in l.keys.iter().zip(items) {
                match item {
                    Some(_) => found(key, true),
                    None => unknown.push(key.clone()),
                }
            }
            unknown
        }
        _ => l.keys.clone(),
    };
    if !unknown.is_empty() {
        for (key, there) in unknown.iter().zip(exists(db, &unknown)) {
            found(key, there);
        }
    }
    t
}

impl Tally {
    /// Add it to INFO's counters, and raise `keymiss` for each key missed.
    pub fn apply(self) {
        apply_all(std::iter::once(self));
    }
}

/// Add a batch's tallies to INFO's counters in one step, which every worker
/// shares, and raise `keymiss` for each key missed.
pub fn apply_all(tallies: impl IntoIterator<Item = Tally>) {
    let flags = crate::notify::flags();
    let (mut hits, mut misses) = (0, 0);
    for t in tallies {
        hits += t.hits;
        misses += t.missed.len() as u64;
        if t.notify && flags & crate::notify::KEY_MISS != 0 {
            for key in &t.missed {
                crate::notify::emit(flags, crate::notify::KEY_MISS, "keymiss", key, t.db);
            }
        }
    }
    if hits + misses > 0 {
        crate::server::count_lookups(hits, misses);
    }
}
