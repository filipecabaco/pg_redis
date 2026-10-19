//! Keyspace notifications: `notify-keyspace-events`.
//!
//! Redis publishes an event on `__keyspace@<db>__:<key>` and
//! `__keyevent@<db>__:<event>` wherever a command changes a key. Here the
//! events are worked out around each command the dispatcher runs: a table
//! names the event a command raises given its reply, and the keys it writes
//! are looked up before and after — which gives the `new` a created key
//! raises and the `del` a collection emptied by a pop raises, as Redis's
//! `dbAdd` and `dbDelete` do. Streams name their own events as they make
//! them (a consumer created by `XREADGROUP` is not visible from outside), and
//! the expiry sweepers raise `expired`.
//!
//! None of this runs unless the setting is on: the flags are cached in shared
//! memory, and the check before every command is one atomic load.
#![forbid(unsafe_code)]

use std::cell::RefCell;

use crate::commands::{Command, Response, Value};

pub const KEYSPACE: u32 = 1 << 0;
pub const KEYEVENT: u32 = 1 << 1;
pub const GENERIC: u32 = 1 << 2;
pub const STRING: u32 = 1 << 3;
pub const LIST: u32 = 1 << 4;
pub const SET: u32 = 1 << 5;
pub const HASH: u32 = 1 << 6;
pub const ZSET: u32 = 1 << 7;
pub const EXPIRED: u32 = 1 << 8;
pub const EVICTED: u32 = 1 << 9;
pub const STREAM: u32 = 1 << 10;
pub const KEY_MISS: u32 = 1 << 11;
pub const MODULE: u32 = 1 << 12;
pub const NEW: u32 = 1 << 13;

/// `keyspaceEventsStringToFlags`.
pub fn parse_flags(v: &str) -> u32 {
    v.chars().fold(0, |acc, c| {
        acc | match c {
            'A' => {
                GENERIC | STRING | LIST | SET | HASH | ZSET | EXPIRED | EVICTED | STREAM | MODULE
            }
            'g' => GENERIC,
            '$' => STRING,
            'l' => LIST,
            's' => SET,
            'h' => HASH,
            'z' => ZSET,
            'x' => EXPIRED,
            'e' => EVICTED,
            'K' => KEYSPACE,
            'E' => KEYEVENT,
            't' => STREAM,
            'm' => KEY_MISS,
            'd' => MODULE,
            'n' => NEW,
            _ => 0,
        }
    })
}

/// The flags in force, or 0 when nothing is published.
pub fn flags() -> u32 {
    let f = crate::server::notify_flags();
    // An event of no kind, or a kind with nowhere to go, publishes nothing.
    if f & (KEYSPACE | KEYEVENT) == 0 { 0 } else { f }
}

/// `notifyKeyspaceEvent`.
pub fn emit(flags: u32, class: u32, event: &str, key: &[u8], db: u8) {
    if flags & class == 0 {
        return;
    }
    if flags & KEYSPACE != 0 {
        let channel = [format!("__keyspace@{db}__:").as_bytes(), key].concat();
        crate::pubsub::shared::publish(&channel, event.as_bytes());
    }
    if flags & KEYEVENT != 0 {
        let channel = format!("__keyevent@{db}__:{event}");
        crate::pubsub::shared::publish(channel.as_bytes(), key);
    }
}

thread_local! {
    /// Events a command's own code raised while it ran — the stream commands.
    static RAISED: RefCell<Vec<(u32, &'static str, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
}

/// Called by a command's implementation where it changes a key.
pub fn raise(class: u32, event: &'static str, key: &[u8]) {
    RAISED.with(|r| r.borrow_mut().push((class, event, key.to_vec())));
}

thread_local! {
    static UNTOUCHED: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

/// Called by a command's implementation for a key it names as written and
/// left as it was — SMOVE's destination when the member was already there,
/// both keys of an SMOVE onto itself. Redis never signals such a key: no
/// event for it, no WATCH broken, no tracking invalidation.
pub fn untouched(key: &[u8]) {
    UNTOUCHED.with(|u| u.borrow_mut().push(key.to_vec()));
}

/// The keys the command just run left untouched, clearing the list.
pub fn take_untouched() -> Vec<Vec<u8>> {
    UNTOUCHED.with(|u| std::mem::take(&mut *u.borrow_mut()))
}

fn is_untouched(key: &[u8]) -> bool {
    UNTOUCHED.with(|u| u.borrow().iter().any(|k| k == key))
}

fn take_raised() -> Vec<(u32, &'static str, Vec<u8>)> {
    RAISED.with(|r| std::mem::take(&mut *r.borrow_mut()))
}

/// What was looked up before a command ran.
pub struct Before {
    flags: u32,
    keys: Vec<(u8, Vec<u8>)>,
    existed: Vec<bool>,
}

/// Look up the keys `cmd` writes, when the events in force need it. `exists`
/// answers whether a key is there.
pub fn before(cmd: &Command, db: u8, exists: &mut dyn FnMut(u8, &[u8]) -> bool) -> Option<Before> {
    let flags = flags();
    // Clear what an earlier command may have left, notifications off or on.
    take_raised();
    if flags == 0 {
        return None;
    }
    let keys = written_keys(cmd, db);
    let existed = if flags & (NEW | GENERIC) != 0 {
        keys.iter().map(|(d, k)| exists(*d, k)).collect()
    } else {
        Vec::new()
    };
    Some(Before {
        flags,
        keys,
        existed,
    })
}

/// Publish what `cmd` did, now that it has replied `resp`.
pub fn after(
    before: Option<Before>,
    cmd: &Command,
    db: u8,
    resp: &Response,
    exists: &mut dyn FnMut(u8, &[u8]) -> bool,
) {
    let raised = take_raised();
    let Some(b) = before else { return };
    if matches!(resp, Response::Error(_)) {
        return;
    }
    let flags = b.flags;
    let now: Vec<bool> = if b.existed.is_empty() {
        Vec::new()
    } else {
        b.keys.iter().map(|(d, k)| exists(*d, k)).collect()
    };
    // A key a command created: `dbAdd`'s event, ahead of the command's own.
    for (i, (d, k)) in b.keys.iter().enumerate() {
        if !b.existed.is_empty() && !b.existed[i] && now[i] {
            emit(flags, NEW, "new", k, *d);
        }
    }
    // An expire whose deadline had already passed deleted the key: Redis
    // announces that as the `del` below, not as an `expire`.
    let expire_deleted = |key: &[u8]| {
        matches!(
            cmd,
            Command::Expire { .. }
                | Command::PExpire { .. }
                | Command::ExpireAt { .. }
                | Command::PExpireAt { .. }
        ) && b
            .keys
            .iter()
            .zip(&now)
            .any(|((d, k), live)| *d == db && k == key && !live)
    };
    for (class, event, key, d) in command_events(cmd, db, resp) {
        if d == db && (is_untouched(&key) || (event == "expire" && expire_deleted(&key))) {
            continue;
        }
        emit(flags, class, event, &key, d);
    }
    for (class, event, key) in raised {
        emit(flags, class, event, &key, db);
    }
    // A key a command removed: `dbDelete`'s event — DEL's own, or a
    // collection emptied by a pop. A rename or a move names its own events.
    if !matches!(
        cmd,
        Command::Rename { .. }
            | Command::RenameNx { .. }
            | Command::Composite(crate::composite::Op::Move { .. })
    ) {
        for (i, (d, k)) in b.keys.iter().enumerate() {
            if !b.existed.is_empty() && b.existed[i] && !now[i] {
                emit(flags, GENERIC, "del", k, *d);
            }
        }
    }
}

/// The keys `cmd` may write, by database.
fn written_keys(cmd: &Command, db: u8) -> Vec<(u8, Vec<u8>)> {
    let owned = |v: Vec<(u8, &[u8])>| v.into_iter().map(|(d, k)| (d, k.to_vec())).collect();
    match cmd {
        Command::Block(op) | Command::BlockTry { op, .. } => owned(op.write_targets(db)),
        Command::Composite(crate::composite::Op::MPop { keys, .. }) => {
            keys.iter().map(|k| (db, k.clone())).collect()
        }
        Command::Composite(crate::composite::Op::Move { key, db: to }) => {
            vec![(db, key.clone()), (*to, key.clone())]
        }
        Command::Composite(op) => owned(op.write_targets(db)),
        _ => crate::worker::watch_targets(cmd, db).unwrap_or_default(),
    }
}

fn int(r: &Response) -> i64 {
    match r {
        Response::Integer(n) => *n,
        _ => 0,
    }
}

fn is_nil(r: &Response) -> bool {
    matches!(r, Response::Null | Response::NullArray)
        || matches!(r, Response::Array(v) if v.is_empty())
        || matches!(r, Response::Value(Value::Array(v)) if v.is_empty())
        || matches!(r, Response::Value(Value::Nil | Value::NilArray))
}

/// The key named first in an array reply: what a blocking pop served.
fn served_key(r: &Response) -> Option<Vec<u8>> {
    match r {
        Response::Value(Value::Array(v)) => match v.first() {
            Some(Value::Bulk(k)) => Some(k.clone()),
            _ => None,
        },
        Response::Array(v) => v.first().cloned().flatten(),
        _ => None,
    }
}

/// The events a command raises, given what it replied: Redis's
/// `notifyKeyspaceEvent` calls, command by command.
fn command_events(cmd: &Command, db: u8, resp: &Response) -> Vec<(u32, &'static str, Vec<u8>, u8)> {
    use crate::blocking::Kind;
    use crate::composite::Op;
    let one = |class: u32, event: &'static str, key: &[u8]| vec![(class, event, key.to_vec(), db)];
    let none = Vec::new;
    let side = |left: bool, l: &'static str, r: &'static str| if left { l } else { r };
    match cmd {
        // ── strings ──
        Command::Set {
            key,
            ex_ms,
            nx,
            xx,
            get,
            ..
        } => {
            if (*nx || *xx) && !*get && is_nil(resp) {
                return none();
            }
            let mut v = one(STRING, "set", key);
            if ex_ms.is_some() {
                v.push((GENERIC, "expire", key.clone(), db));
            }
            v
        }
        Command::SetEx { key, .. } | Command::PSetEx { key, .. } => {
            let mut v = one(STRING, "set", key);
            v.push((GENERIC, "expire", key.clone(), db));
            v
        }
        Command::SetNx { key, .. } if int(resp) == 1 => one(STRING, "set", key),
        Command::GetSet { key, .. } => one(STRING, "set", key),
        Command::MSet { pairs } => pairs
            .iter()
            .map(|(k, _)| (STRING, "set", k.clone(), db))
            .collect(),
        Command::MSetNx { pairs } if int(resp) == 1 => pairs
            .iter()
            .map(|(k, _)| (STRING, "set", k.clone(), db))
            .collect(),
        Command::Append { key, .. } => one(STRING, "append", key),
        Command::Incr { key }
        | Command::Decr { key }
        | Command::IncrBy { key, .. }
        | Command::DecrBy { key, .. } => one(STRING, "incrby", key),
        Command::IncrByFloat { key, .. } => one(STRING, "incrbyfloat", key),
        Command::SetRange { key, .. } => one(STRING, "setrange", key),
        Command::SetBit { key, .. } => one(STRING, "setbit", key),
        // ── generic ──
        Command::Expire { key, .. }
        | Command::PExpire { key, .. }
        | Command::ExpireAt { key, .. }
        | Command::PExpireAt { key, .. }
            if int(resp) == 1 =>
        {
            one(GENERIC, "expire", key)
        }
        Command::Persist { key } if int(resp) == 1 => one(GENERIC, "persist", key),
        Command::GetEx { key, expiry } if !is_nil(resp) => match expiry {
            crate::commands::GetExExpiry::Clear => one(GENERIC, "persist", key),
            crate::commands::GetExExpiry::At(_) => one(GENERIC, "expire", key),
            _ => none(),
        },
        Command::Rename { key, newkey } | Command::RenameNx { key, newkey }
            if matches!(resp, Response::Ok) || int(resp) == 1 =>
        {
            vec![
                (GENERIC, "rename_from", key.clone(), db),
                (GENERIC, "rename_to", newkey.clone(), db),
            ]
        }
        Command::Copy {
            dst, db: target, ..
        } if int(resp) == 1 => {
            vec![(GENERIC, "copy_to", dst.clone(), target.unwrap_or(db))]
        }
        Command::Restore { key, .. } => one(GENERIC, "restore", key),
        // ── lists ──
        Command::LPush { key, .. } | Command::LPushX { key, .. } if int(resp) > 0 => {
            one(LIST, "lpush", key)
        }
        Command::RPush { key, .. } | Command::RPushX { key, .. } if int(resp) > 0 => {
            one(LIST, "rpush", key)
        }
        Command::LPop { key, .. } if !is_nil(resp) => one(LIST, "lpop", key),
        Command::RPop { key, .. } if !is_nil(resp) => one(LIST, "rpop", key),
        Command::LInsert { key, .. } if int(resp) > 0 => one(LIST, "linsert", key),
        Command::LSet { key, .. } => one(LIST, "lset", key),
        Command::LRem { key, .. } if int(resp) > 0 => one(LIST, "lrem", key),
        Command::LTrim { key, .. } => one(LIST, "ltrim", key),
        Command::LMove {
            src,
            dst,
            src_left,
            dst_left,
        } if !is_nil(resp) => vec![
            (LIST, side(*src_left, "lpop", "rpop"), src.clone(), db),
            (LIST, side(*dst_left, "lpush", "rpush"), dst.clone(), db),
        ],
        // ── sets ──
        Command::SAdd { key, .. } if int(resp) > 0 => one(SET, "sadd", key),
        Command::SRem { key, .. } if int(resp) > 0 => one(SET, "srem", key),
        Command::SPop { key, .. } if !is_nil(resp) => one(SET, "spop", key),
        Command::SMove { src, dst, .. } if int(resp) == 1 => vec![
            (SET, "srem", src.clone(), db),
            (SET, "sadd", dst.clone(), db),
        ],
        Command::SInterStore { dst, .. } if int(resp) > 0 => one(SET, "sinterstore", dst),
        Command::SUnionStore { dst, .. } if int(resp) > 0 => one(SET, "sunionstore", dst),
        Command::SDiffStore { dst, .. } if int(resp) > 0 => one(SET, "sdiffstore", dst),
        // ── hashes ──
        Command::HSet { key, .. } | Command::HMSet { key, .. } => one(HASH, "hset", key),
        Command::HSetNx { key, .. } if int(resp) == 1 => one(HASH, "hset", key),
        Command::HDel { key, .. } if int(resp) > 0 => one(HASH, "hdel", key),
        Command::HIncrBy { key, .. } => one(HASH, "hincrby", key),
        Command::HIncrByFloat { key, .. } => one(HASH, "hincrbyfloat", key),
        // ── sorted sets ──
        Command::ZAdd {
            key, incr: true, ..
        } if !is_nil(resp) => one(ZSET, "zincr", key),
        Command::ZAdd { key, nx, xx, .. } if int(resp) > 0 || !(*nx || *xx) => {
            one(ZSET, "zadd", key)
        }
        Command::ZIncrBy { key, .. } => one(ZSET, "zincr", key),
        Command::ZRem { key, .. } if int(resp) > 0 => one(ZSET, "zrem", key),
        Command::ZPopMin { key, .. } if !is_nil(resp) => one(ZSET, "zpopmin", key),
        Command::ZPopMax { key, .. } if !is_nil(resp) => one(ZSET, "zpopmax", key),
        Command::ZRemRangeByRank { key, .. } if int(resp) > 0 => one(ZSET, "zremrangebyrank", key),
        Command::ZRemRangeByScore { key, .. } if int(resp) > 0 => {
            one(ZSET, "zremrangebyscore", key)
        }
        Command::ZRemRangeByLex { key, .. } if int(resp) > 0 => one(ZSET, "zremrangebylex", key),
        // ── blocking pops, once served ──
        Command::Block(op) | Command::BlockTry { op, .. } if !is_nil(resp) => match &op.kind {
            Kind::Pop { left } => served_key(resp)
                .map(|k| vec![(LIST, side(*left, "lpop", "rpop"), k, db)])
                .unwrap_or_default(),
            Kind::ZPop { min } => served_key(resp)
                .map(|k| vec![(ZSET, side(*min, "zpopmin", "zpopmax"), k, db)])
                .unwrap_or_default(),
            Kind::Move {
                dst,
                from_left,
                to_left,
            } => vec![
                (
                    LIST,
                    side(*from_left, "lpop", "rpop"),
                    op.keys[0].clone(),
                    db,
                ),
                (LIST, side(*to_left, "lpush", "rpush"), dst.clone(), db),
            ],
            Kind::MPop { zset, front, .. } => served_key(resp)
                .map(|k| {
                    let (class, event) = match (zset, front) {
                        (true, true) => (ZSET, "zpopmin"),
                        (true, false) => (ZSET, "zpopmax"),
                        (false, true) => (LIST, "lpop"),
                        (false, false) => (LIST, "rpop"),
                    };
                    vec![(class, event, k, db)]
                })
                .unwrap_or_default(),
        },
        // ── commands built from others: their own event, not their parts' ──
        Command::Composite(op) => match op {
            Op::ZAggregate(z) => match (&z.dst, z.op) {
                (Some(dst), crate::composite::SetOp::Union) if int(resp) > 0 => {
                    one(ZSET, "zunionstore", dst)
                }
                (Some(dst), crate::composite::SetOp::Inter) if int(resp) > 0 => {
                    one(ZSET, "zinterstore", dst)
                }
                (Some(dst), crate::composite::SetOp::Diff) if int(resp) > 0 => {
                    one(ZSET, "zdiffstore", dst)
                }
                _ => none(),
            },
            Op::ZRangeStore { dst, .. } if int(resp) > 0 => one(ZSET, "zrangestore", dst),
            Op::Bitop { dest, .. } if int(resp) > 0 => one(STRING, "set", dest),
            Op::Sort(spec) => spec
                .store
                .as_ref()
                .filter(|_| int(resp) > 0)
                .map(|k| one(LIST, "sortstore", k))
                .unwrap_or_default(),
            Op::Move { key, db: to } if int(resp) == 1 => vec![
                (GENERIC, "move_from", key.clone(), db),
                (GENERIC, "move_to", key.clone(), *to),
            ],
            Op::MPop { zset, front, .. } if !is_nil(resp) => served_key(resp)
                .map(|k| {
                    let (class, event) = match (zset, front) {
                        (true, true) => (ZSET, "zpopmin"),
                        (true, false) => (ZSET, "zpopmax"),
                        (false, true) => (LIST, "lpop"),
                        (false, false) => (LIST, "rpop"),
                    };
                    vec![(class, event, k, db)]
                })
                .unwrap_or_default(),
            Op::PfAdd { key, .. } if int(resp) == 1 => one(STRING, "pfadd", key),
            Op::PfMerge { dest, .. } => one(STRING, "pfadd", dest),
            Op::GeoAdd { key, .. } if int(resp) > 0 => one(ZSET, "zadd", key),
            Op::BitField { key, .. }
                if op
                    .write_targets(db)
                    .iter()
                    .any(|(_, k)| *k == key.as_slice()) =>
            {
                one(STRING, "setbit", key)
            }
            _ => none(),
        },
        _ => none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_parse_like_redis() {
        assert_eq!(parse_flags(""), 0);
        assert_eq!(
            parse_flags("KEA") & (KEYSPACE | KEYEVENT | GENERIC | STREAM),
            KEYSPACE | KEYEVENT | GENERIC | STREAM
        );
        // `A` is every class but key-miss and new.
        assert_eq!(parse_flags("A") & (KEY_MISS | NEW), 0);
        assert_eq!(parse_flags("Kl"), KEYSPACE | LIST);
    }

    fn events(argv: &[&str], resp: Response) -> Vec<&'static str> {
        let argv: Vec<Vec<u8>> = argv.iter().map(|a| a.as_bytes().to_vec()).collect();
        let cmd = Command::parse(&argv).expect("parses");
        command_events(&cmd, 9, &resp)
            .into_iter()
            .map(|(_, e, _, _)| e)
            .collect()
    }

    #[test]
    fn commands_raise_redis_events_only_when_they_changed_something() {
        assert_eq!(events(&["SET", "k", "v"], Response::Ok), ["set"]);
        assert_eq!(
            events(&["SET", "k", "v", "EX", "10"], Response::Ok),
            ["set", "expire"]
        );
        assert!(events(&["SET", "k", "v", "NX"], Response::Null).is_empty());
        assert_eq!(events(&["SADD", "s", "a"], Response::Integer(1)), ["sadd"]);
        assert!(events(&["SADD", "s", "a"], Response::Integer(0)).is_empty());
        assert!(events(&["SREM", "s", "x"], Response::Integer(0)).is_empty());
        assert_eq!(events(&["HMSET", "h", "f", "v"], Response::Ok), ["hset"]);
        assert_eq!(
            events(&["HINCRBY", "h", "f", "1"], Response::Integer(1)),
            ["hincrby"]
        );
        assert_eq!(
            events(&["RPOP", "l"], Response::BulkString(b"a".to_vec())),
            ["rpop"]
        );
        assert!(events(&["RPOP", "l"], Response::Null).is_empty());
        assert_eq!(
            events(
                &["LMOVE", "a", "b", "LEFT", "RIGHT"],
                Response::BulkString(b"x".to_vec())
            ),
            ["lpop", "rpush"]
        );
        assert_eq!(
            events(&["RENAME", "a", "b"], Response::Ok),
            ["rename_from", "rename_to"]
        );
        assert_eq!(
            events(
                &["ZINCRBY", "z", "1", "m"],
                Response::BulkString(b"1".to_vec())
            ),
            ["zincr"]
        );
        assert_eq!(
            events(&["ZUNIONSTORE", "d", "1", "z"], Response::Integer(2)),
            ["zunionstore"]
        );
        assert!(events(&["ZUNIONSTORE", "d", "1", "z"], Response::Integer(0)).is_empty());
    }
}
