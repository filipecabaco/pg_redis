//! Redis's dirty counter: the changes made since the last save, which INFO
//! reports as `rdb_changes_since_last_save`.
//!
//! Redis has each command add its own count as it runs: `SADD` the members
//! it added, `HSET` the pairs it was given, `DEL` the keys it removed, most
//! writes one. Here [`of`] reads the same count off the command and its
//! reply, once `store::run` has run it on either half, so every way a
//! command arrives (a client, `EXEC`, a script, a blocked client being
//! served) counts alike. The few whose count no reply carries (`SETBIT`,
//! `BITFIELD`, `FLUSHDB`) report it themselves through [`force`]. A
//! multi-step command counts as Redis counts it, not as the steps it is
//! built from.
#![forbid(unsafe_code)]

use std::cell::Cell;

use crate::commands::{Command, GetExExpiry, Response, Value};
use crate::composite::Op;

thread_local! {
    /// Inside a multi-step command, whose steps do not count.
    static QUIET: Cell<u32> = const { Cell::new(0) };
}

/// Run `f`, the steps of one command, without counting them.
pub fn quietly<R>(f: impl FnOnce() -> R) -> R {
    /// Leaves the quiet section on unwinding too.
    struct Leave;
    impl Drop for Leave {
        fn drop(&mut self) {
            QUIET.with(|q| q.set(q.get() - 1));
        }
    }
    QUIET.with(|q| q.set(q.get() + 1));
    let _leave = Leave;
    f()
}

/// Count `n` changes made by a command run on its own.
pub fn add(n: u64) {
    if n > 0 && QUIET.with(Cell::get) == 0 {
        force(n);
    }
}

/// Count `n` changes whatever runs them: a multi-step command's own count.
pub fn force(n: u64) {
    if n > 0 {
        crate::server::add_dirty(n);
    }
}

fn int(r: &Response) -> u64 {
    match r {
        Response::Integer(n) => (*n).max(0) as u64,
        _ => 0,
    }
}

fn one_if(yes: bool) -> u64 {
    u64::from(yes)
}

fn popped(r: &Response) -> u64 {
    match r {
        Response::BulkString(_) => 1,
        Response::Array(v) => v.len() as u64,
        _ => 0,
    }
}

/// The changes Redis counts for `cmd`, which answered `reply`.
pub fn of(cmd: &Command, reply: &Response) -> u64 {
    if matches!(reply, Response::Error(_)) {
        return 0;
    }
    let found = !matches!(reply, Response::Null | Response::NullArray);
    match cmd {
        Command::Set { nx, xx, get, .. } => match (get, reply) {
            (false, r) => one_if(matches!(r, Response::Ok)),
            // With GET the reply is the old value: NX set only where there
            // was none, XX only where there was one.
            (true, r) if *nx => one_if(matches!(r, Response::Null)),
            (true, r) if *xx => one_if(matches!(r, Response::BulkString(_))),
            (true, _) => 1,
        },
        Command::SetEx { .. }
        | Command::PSetEx { .. }
        | Command::GetSet { .. }
        | Command::Append { .. }
        | Command::Incr { .. }
        | Command::Decr { .. }
        | Command::IncrBy { .. }
        | Command::DecrBy { .. }
        | Command::IncrByFloat { .. }
        | Command::HIncrBy { .. }
        | Command::HIncrByFloat { .. }
        | Command::ZIncrBy { .. }
        | Command::Restore { .. }
        | Command::Rename { .. }
        | Command::SUnionStore { .. }
        | Command::SInterStore { .. }
        | Command::SDiffStore { .. }
        | Command::LSet { .. }
        | Command::LTrim { .. } => 1,
        Command::SetRange { value, .. } => one_if(!value.is_empty()),
        Command::GetDel { .. } => one_if(found),
        Command::GetEx { expiry, .. } => one_if(found && !matches!(expiry, GetExExpiry::Keep)),
        Command::LMove { .. } => one_if(found),
        Command::MSet { pairs } => pairs.len() as u64,
        Command::MSetNx { pairs } => pairs.len() as u64 * int(reply),
        Command::HSet { pairs, .. } | Command::HMSet { pairs, .. } => pairs.len() as u64,
        Command::LPush { values, .. } | Command::RPush { values, .. } => values.len() as u64,
        Command::LPushX { values, .. } | Command::RPushX { values, .. } => {
            values.len() as u64 * one_if(int(reply) > 0)
        }
        Command::LInsert { .. } => one_if(matches!(reply, Response::Integer(n) if *n > 0)),
        Command::LPop { .. } | Command::RPop { .. } | Command::SPop { .. } => popped(reply),
        // Member and score for each.
        Command::ZPopMin { .. } | Command::ZPopMax { .. } => popped(reply) / 2,
        Command::SetNx { .. }
        | Command::RenameNx { .. }
        | Command::Copy { .. }
        | Command::Expire { .. }
        | Command::PExpire { .. }
        | Command::ExpireAt { .. }
        | Command::PExpireAt { .. }
        | Command::Persist { .. }
        | Command::HSetNx { .. }
        | Command::SMove { .. }
        | Command::Del { .. }
        | Command::Unlink { .. }
        | Command::HDel { .. }
        | Command::LRem { .. }
        | Command::SAdd { .. }
        | Command::SRem { .. }
        | Command::ZRem { .. }
        | Command::ZRemRangeByRank { .. }
        | Command::ZRemRangeByScore { .. }
        | Command::ZRemRangeByLex { .. } => int(reply),
        _ => 0,
    }
}

/// The changes Redis counts for a multi-step command, which answered
/// `reply`. `BITFIELD` counts its own.
pub fn of_op(op: &Op, reply: &Response) -> u64 {
    if matches!(reply, Response::Error(_)) {
        return 0;
    }
    match op {
        Op::Bitop { .. }
        | Op::ZRangeStore { .. }
        | Op::SwapDb { .. }
        | Op::PfMerge { .. }
        | Op::ZAggregate { .. } => 1,
        Op::Move { .. } | Op::PfAdd { .. } => int(reply),
        Op::GeoAdd { .. } => int(reply),
        // `[key, [elements]]`, or `[key, [[member, score], ...]]`.
        Op::MPop { .. } => match reply {
            Response::Value(Value::Array(v)) => match v.get(1) {
                Some(Value::Array(items)) => items.len() as u64,
                _ => 0,
            },
            _ => 0,
        },
        // With STORE, the elements stored, or the destination removed.
        Op::Sort(spec) if spec.store.is_some() => int(reply).max(1),
        _ => 0,
    }
}
