//! Commands written once (docs/STORAGE.md, phase 5).
//!
//! A command's meaning — how its arguments are read, its edge cases, the reply
//! it builds — is written here, once, against [`Store`]. The two backends
//! implement `Store` underneath: `memdb::Db` over a database in shared memory,
//! [`SqlStore`] over a database's tables. Each operation is atomic in its
//! backend — one statement, or one critical section under the database lock —
//! so the interface is drawn at the level of what a command does to a key
//! (`hincrby`), not of reads and writes a command would have to compose
//! (`hget` then `hset`), which another worker could interleave: an ordinary
//! command holds its database's server-wide lock shared (`src/dblock.rs`).
//!
//! `run` returns `None` for what is not a single-backend data command —
//! streams, composites, connection and server commands — which the executors
//! in `commands.rs` answer as before.
#![forbid(unsafe_code)]

use pgrx::datum::DatumWithOid;
use pgrx::spi::SpiClient;

use crate::commands::{
    Command, ExpireCond, GetExExpiry, LexBound, RangeBy, Response, ScoreBound, format_score,
};
use crate::longdouble::LongDouble;
use crate::memdb::KeyKind;
use crate::memdb::encoding::{self, Enc, Limits, SetBuilder};
use crate::plans::Kept;

/// A store's answer, or the reply for a failure of the store itself.
pub type Out<T> = Result<T, Response>;

/// The outcome of arithmetic on a stored value.
pub enum Num<T> {
    Value(T),
    /// The stored value is not a number of the kind asked for.
    NotNumber,
    /// The result would not fit — or, for floats, is not finite.
    Overflow,
}

/// What `ZADD … INCR` or `ZINCRBY` did.
pub enum Incr {
    Score(f64),
    /// Refused by NX, XX, GT or LT; nothing changed.
    Skipped,
    /// The sum is not a number (an infinity plus its opposite); nothing
    /// changed.
    NotANumber,
}

/// `SET`'s options. The expiry is in milliseconds: a TTL, or a Unix time
/// when `ex_abs`.
pub struct SetArgs {
    pub ex_ms: Option<i64>,
    pub ex_abs: bool,
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
    pub keepttl: bool,
}

/// What `SET` did: whether it wrote, and — asked with GET — the value the key
/// held, whether or not NX or XX let the write through.
pub struct SetDone {
    pub written: bool,
    pub old: Option<Vec<u8>>,
}

/// What `LSET` found.
pub enum LSet {
    Done,
    NoKey,
    OutOfRange,
}

/// `ZADD`'s conditions.
#[derive(Clone, Copy)]
pub struct ZaddOpts {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
}

/// What a backend does to one key, each operation atomic.
pub trait Store {
    // ── keys ──
    /// How many of `keys` existed, each counted once.
    fn del(&mut self, keys: &[Vec<u8>]) -> Out<i64>;
    /// How many of `keys` exist, once per argument: `EXISTS k k` is 2.
    fn exists(&mut self, keys: &[Vec<u8>]) -> Out<i64>;
    fn kind(&mut self, key: &[u8]) -> Out<Option<KeyKind>>;
    /// Each key's type, in order — for `SCAN … TYPE` over many at once.
    fn kinds(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Option<KeyKind>>>;
    /// `None` when the key is not there; then its deadline in microseconds
    /// since the epoch, or `None` for no expiry.
    fn deadline(&mut self, key: &[u8]) -> Out<Option<Option<i64>>>;
    /// `TOUCH`: how many of `keys` exist, looked up as a read that updates
    /// each one's access field.
    fn touch(&mut self, keys: &[Vec<u8>]) -> Out<i64> {
        self.exists(keys)
    }
    /// What `OBJECT` reads of a live key, without touching it: its access
    /// field (`memdb::lru`), and the integer it holds when it is a string
    /// that reads as one.
    fn access(&mut self, key: &[u8]) -> Out<Option<(u32, Option<i64>)>>;
    /// Give a live key the deadline when `cond` allows; whether it did.
    fn expire(&mut self, key: &[u8], deadline_us: i64, cond: ExpireCond) -> Out<bool>;
    fn persist(&mut self, key: &[u8]) -> Out<bool>;
    /// Move `key`, its value and its expiry, to `newkey`, replacing whatever
    /// was there. False when `key` is not there.
    fn rename(&mut self, key: &[u8], newkey: &[u8]) -> Out<bool>;
    /// Every live key matching the glob, or every one without it.
    fn keys(&mut self, pattern: Option<&[u8]>) -> Out<Vec<Vec<u8>>>;
    /// One step of `SCAN`: about `count` keys from `cursor` (0 starts), live
    /// ones only, and the cursor to continue from, 0 when the iteration is
    /// done. A key present for the whole iteration is returned at least once.
    fn scan_keys(&mut self, cursor: u64, count: usize) -> Out<(u64, Vec<Vec<u8>>)>;
    fn dbsize(&mut self) -> Out<i64>;
    /// INFO keyspace's keys and keys with an expiry.
    fn keyspace(&mut self) -> Out<(i64, i64)>;
    fn random_key(&mut self) -> Out<Option<Vec<u8>>>;
    fn flushdb(&mut self) -> Out<()>;

    // ── strings ──
    fn get(&mut self, key: &[u8]) -> Out<Option<Vec<u8>>>;
    fn set(&mut self, key: &[u8], value: &[u8], args: &SetArgs) -> Out<SetDone>;
    /// `SETEX`/`PSETEX`: write with a TTL in milliseconds, replacing whatever
    /// held the key.
    fn setex(&mut self, key: &[u8], value: &[u8], ttl_ms: i64) -> Out<()>;
    fn setnx(&mut self, key: &[u8], value: &[u8]) -> Out<bool>;
    fn getset(&mut self, key: &[u8], value: &[u8]) -> Out<Option<Vec<u8>>>;
    fn getdel(&mut self, key: &[u8]) -> Out<Option<Vec<u8>>>;
    fn getex(&mut self, key: &[u8], expiry: &GetExExpiry) -> Out<Option<Vec<u8>>>;
    fn mget(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>>;
    fn mset(&mut self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<()>;
    /// Set every pair only if no key named exists; whether it did.
    fn msetnx(&mut self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<bool>;
    fn append(&mut self, key: &[u8], value: &[u8]) -> Out<i64>;
    fn strlen(&mut self, key: &[u8]) -> Out<i64>;
    /// Overwrite from `offset`, padding a gap with NULs; the new length.
    /// `value` is not empty.
    fn setrange(&mut self, key: &[u8], offset: i64, value: &[u8]) -> Out<i64>;
    fn incrby(&mut self, key: &[u8], delta: i64) -> Out<Num<i64>>;
    fn incrbyfloat(&mut self, key: &[u8], delta: LongDouble) -> Out<Num<String>>;
    /// Set or clear one bit; what it was.
    /// The bit as it was, and whether anything changed: the bit, or the
    /// string created or grown to reach it.
    fn setbit(&mut self, key: &[u8], offset: i64, on: bool) -> Out<(bool, bool)>;

    // ── hashes ──
    fn hget(&mut self, key: &[u8], field: &[u8]) -> Out<Option<Vec<u8>>>;
    fn hmget(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>>;
    fn hgetall(&mut self, key: &[u8]) -> Out<Vec<(Vec<u8>, Vec<u8>)>>;
    /// One step of `HSCAN`, as `scan_keys` is of `SCAN`: fields and values.
    fn hscan(&mut self, key: &[u8], cursor: u64, count: usize)
    -> Out<crate::cursor::Page<Vec<u8>>>;
    fn hkeys(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>>;
    fn hvals(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>>;
    fn hlen(&mut self, key: &[u8]) -> Out<i64>;
    fn hexists(&mut self, key: &[u8], field: &[u8]) -> Out<bool>;
    fn hstrlen(&mut self, key: &[u8], field: &[u8]) -> Out<i64> {
        Ok(self.hget(key, field)?.map_or(0, |v| v.len() as i64))
    }
    /// Set every pair — each field once — and count the fields that are new.
    /// `named` is how many pairs the command named, repeats included, which
    /// is what Redis sizes a hash by before writing.
    fn hset(&mut self, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)], named: usize) -> Out<i64>;
    fn hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Out<bool>;
    fn hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<i64>;
    fn hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> Out<Num<i64>>;
    /// The new value as Redis spells it.
    fn hincrbyfloat(&mut self, key: &[u8], field: &[u8], delta: LongDouble) -> Out<Num<String>>;
    /// The encoding Redis would have given the hash, set or sorted set at
    /// `key` (`memdb::encoding`); `None` when there is none.
    fn collection_encoding(&mut self, key: &[u8]) -> Out<Option<Enc>>;
    /// `zsetConvert(zobj, OBJ_ENCODING_SKIPLIST)`: the sorted set at `key`
    /// becomes a skiplist, as `SORT` makes it before reading it.
    fn zset_to_skiplist(&mut self, key: &[u8]) -> Out<()>;

    // ── sets ──
    fn sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64>;
    fn srem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64>;
    fn sismember(&mut self, key: &[u8], member: &[u8]) -> Out<bool>;
    fn smismember(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<bool>>;
    fn smembers(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>>;
    /// One step of `SSCAN`.
    fn sscan(&mut self, key: &[u8], cursor: u64, count: usize) -> Out<(u64, Vec<Vec<u8>>)>;
    fn scard(&mut self, key: &[u8]) -> Out<i64>;
    /// Remove and return up to `n` members picked at random.
    fn spop(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>>;
    /// `n` random members: distinct when `n` is positive, with repeats and
    /// exactly `-n` of them when it is negative.
    fn srandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>>;
    /// Move `member` between two distinct sets: (removed from `src`, added to
    /// `dst`).
    fn smove(&mut self, src: &[u8], dst: &[u8], member: &[u8]) -> Out<(bool, bool)>;
    fn sunion(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>>;
    fn sinter(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>>;
    fn sdiff(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>>;
    /// Replace `dst` with the result; its size.
    fn sunionstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64>;
    fn sinterstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64>;
    fn sdiffstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64>;
    /// The intersection's size, counting no further than `limit`.
    fn sintercard(&mut self, keys: &[Vec<u8>], limit: Option<i64>) -> Out<i64>;

    // ── sorted sets ──
    /// Every pair, in order, under the conditions: how many were added — or,
    /// with CH, added or changed.
    /// Members added, and members added or whose score changed.
    fn zadd(&mut self, key: &[u8], pairs: &[(f64, Vec<u8>)], opts: ZaddOpts) -> Out<(i64, i64)>;
    fn zadd_incr(&mut self, key: &[u8], member: &[u8], delta: f64, opts: ZaddOpts) -> Out<Incr>;
    fn zrem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64>;
    fn zscore(&mut self, key: &[u8], member: &[u8]) -> Out<Option<f64>>;
    fn zmscore(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<Option<f64>>>;
    fn zcard(&mut self, key: &[u8]) -> Out<i64>;
    /// One step of `ZSCAN`: members and their scores.
    fn zscan(&mut self, key: &[u8], cursor: u64, count: usize) -> Out<crate::cursor::Page<f64>>;
    fn zcount(&mut self, key: &[u8], min: ScoreBound, max: ScoreBound) -> Out<i64>;
    fn zlexcount(&mut self, key: &[u8], min: &LexBound, max: &LexBound) -> Out<i64>;
    /// The member's rank, counted from the low end (or the high end, `rev`),
    /// and its score.
    fn zrank(&mut self, key: &[u8], member: &[u8], rev: bool) -> Out<Option<(i64, f64)>>;
    fn zrange_index(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Out<Vec<(Vec<u8>, f64)>>;
    fn zrange_score(
        &mut self,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Out<Vec<(Vec<u8>, f64)>>;
    fn zrange_lex(
        &mut self,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Out<Vec<Vec<u8>>>;
    /// Remove and return up to `n` lowest (`min`) or highest members.
    fn zpop(&mut self, key: &[u8], n: i64, min: bool) -> Out<Vec<(Vec<u8>, f64)>>;
    /// As `srandmember`: distinct for a positive `n`, `-n` with repeats for a
    /// negative one.
    fn zrandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<(Vec<u8>, f64)>>;
    fn zremrange_rank(&mut self, key: &[u8], start: i64, stop: i64) -> Out<i64>;
    fn zremrange_score(&mut self, key: &[u8], min: ScoreBound, max: ScoreBound) -> Out<i64>;
    fn zremrange_lex(&mut self, key: &[u8], min: &LexBound, max: &LexBound) -> Out<i64>;

    // ── lists ──
    /// Push at the head (`left`) or tail, one at a time in order; with
    /// `only_existing`, only onto a list already there. The list's length,
    /// 0 when nothing was pushed for want of one.
    fn lpush(
        &mut self,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
        only_existing: bool,
    ) -> Out<i64>;
    /// Remove and return up to `n` (≥ 1) elements from the head or tail.
    fn lpop(&mut self, key: &[u8], n: i64, left: bool) -> Out<Vec<Vec<u8>>>;
    fn llen(&mut self, key: &[u8]) -> Out<i64>;
    fn lrange(&mut self, key: &[u8], start: i64, stop: i64) -> Out<Vec<Vec<u8>>>;
    fn lindex(&mut self, key: &[u8], index: i64) -> Out<Option<Vec<u8>>>;
    fn lset(&mut self, key: &[u8], index: i64, value: &[u8]) -> Out<LSet>;
    /// The new length, -1 when the pivot is not there, 0 when the list is not.
    fn linsert(&mut self, key: &[u8], before: bool, pivot: &[u8], value: &[u8]) -> Out<i64>;
    fn lrem(&mut self, key: &[u8], count: i64, value: &[u8]) -> Out<i64>;
    fn ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> Out<()>;
    fn lmove(
        &mut self,
        src: &[u8],
        dst: &[u8],
        src_left: bool,
        dst_left: bool,
    ) -> Out<Option<Vec<u8>>>;
    /// Positions of `element`: from the `rank`-th match (from the tail when
    /// negative), at most `count` of them (all for 0, one for none), looking
    /// no further than `maxlen` elements (0: no bound).
    fn lpos(
        &mut self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<i64>,
        maxlen: i64,
    ) -> Out<Vec<i64>>;
}

/// Run `cmd` against `store`, if its family is written here.
pub fn run(cmd: &Command, store: &mut dyn Store) -> Option<Response> {
    let reply = match cmd {
        Command::Del { keys } | Command::Unlink { keys } => store.del(keys).map(Response::Integer),
        Command::Exists { keys } => store.exists(keys).map(Response::Integer),
        // TOUCH counts as EXISTS does, and updates each key's access field.
        Command::Touch { keys } => store.touch(keys).map(Response::Integer),
        Command::Type { key } => store
            .kind(key)
            .map(|k| Response::SimpleString(k.map_or("none", |k| k.name()).to_string())),
        Command::Ttl { key } => ttl(store, key, false, true),
        Command::PTtl { key } => ttl(store, key, true, true),
        Command::ExpireTime { key } => ttl(store, key, false, false),
        Command::PExpireTime { key } => ttl(store, key, true, false),
        Command::Expire { .. }
        | Command::PExpire { .. }
        | Command::ExpireAt { .. }
        | Command::PExpireAt { .. } => expire(store, cmd),
        Command::Persist { key } => store.persist(key).map(|b| Response::Integer(b as i64)),
        Command::Rename { key, newkey } => store.rename(key, newkey).map(|done| {
            if done {
                Response::Ok
            } else {
                Response::Error("ERR no such key".into())
            }
        }),
        Command::RenameNx { key, newkey } => renamenx(store, key, newkey),
        Command::Keys { pattern } => store.keys(Some(pattern)).map(array),
        Command::Scan {
            cursor,
            pattern,
            count,
            kind,
        } => scan(store, *cursor, *count, pattern.as_deref(), *kind),
        Command::CollectionScan {
            kind,
            key,
            cursor,
            count,
            pattern,
        } => collection_scan(store, *kind, key, *cursor, *count, pattern.as_deref()),
        Command::DbSize => store.dbsize().map(Response::Integer),
        Command::Keyspace => store
            .keyspace()
            .map(|(k, e)| Response::IntegerArray(vec![k, e])),
        Command::RandomKey => store.random_key().map(bulk_or_nil),
        Command::FlushDb { .. } => store.dbsize().and_then(|n| {
            store.flushdb()?;
            crate::dirty::add(n.max(0) as u64);
            Ok(Response::Ok)
        }),
        Command::ObjectEncoding { key } => object_encoding(store, key),
        Command::ZsetToSkiplist { key } => store.zset_to_skiplist(key).map(|()| Response::Ok),
        Command::ObjectAccess { key, field } => object_access(store, key, *field),
        Command::ObjectOther(sub) if sub == "HELP" => Ok(Response::Value(crate::commands::Value::Array(
            [
                "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "ENCODING <key>",
                "    Return the kind of internal representation used in order to store the value",
                "    associated with a <key>.",
                "FREQ <key>",
                "    Return the access frequency index of the <key>. The returned integer is",
                "    proportional to the logarithm of the recent access frequency of the key.",
                "IDLETIME <key>",
                "    Return the idle time of the <key>, that is the approximated number of",
                "    seconds elapsed since the last access to the key.",
                "REFCOUNT <key>",
                "    Return the number of references of the value associated with the specified",
                "    <key>.",
                "HELP",
                "    Print this help.",
            ]
            .into_iter()
            .map(|l| crate::commands::Value::Simple(l.to_string()))
            .collect(),
        ))),
        Command::ObjectOther(sub) => Ok(Response::Error(format!(
            "ERR unknown subcommand '{}'. Try OBJECT HELP.",
            sub.to_ascii_lowercase()
        ))),

        Command::Get { key } => store.get(key).map(bulk_or_nil),
        Command::Set {
            key,
            value,
            ex_ms,
            ex_abs,
            nx,
            xx,
            get,
            keepttl,
        } => {
            let args = SetArgs {
                ex_ms: *ex_ms,
                ex_abs: *ex_abs,
                nx: *nx,
                xx: *xx,
                get: *get,
                keepttl: *keepttl,
            };
            store
                .set(key, value, &args)
                .map(|done| match (*get, done.written) {
                    (true, _) => bulk_or_nil(done.old),
                    (false, true) => Response::Ok,
                    (false, false) => Response::Null,
                })
        }
        Command::SetEx {
            key,
            value,
            ex_secs,
        } => store
            .setex(key, value, ex_secs.saturating_mul(1000))
            .map(|()| Response::Ok),
        Command::PSetEx { key, value, ex_ms } => {
            store.setex(key, value, *ex_ms).map(|()| Response::Ok)
        }
        Command::SetNx { key, value } => {
            store.setnx(key, value).map(|b| Response::Integer(b as i64))
        }
        Command::GetSet { key, value } => store.getset(key, value).map(bulk_or_nil),
        Command::GetDel { key } => store.getdel(key).map(bulk_or_nil),
        Command::GetEx { key, expiry } => store.getex(key, expiry).map(bulk_or_nil),
        Command::MGet { keys } => store.mget(keys).map(Response::Array),
        Command::MSet { pairs } => store.mset(pairs).map(|()| Response::Ok),
        Command::MSetNx { pairs } => store.msetnx(pairs).map(|b| Response::Integer(b as i64)),
        Command::Append { key, value } => store.append(key, value).map(Response::Integer),
        Command::Strlen { key } => store.strlen(key).map(Response::Integer),
        Command::GetRange { key, start, end } => store.get(key).map(|v| {
            Response::BulkString(crate::commands::byte_range(
                &v.unwrap_or_default(),
                *start,
                *end,
            ))
        }),
        // An empty value writes nothing: Redis reports the length as it stands.
        Command::SetRange { key, value, .. } if value.is_empty() => {
            store.strlen(key).map(Response::Integer)
        }
        Command::SetRange { key, offset, value } => {
            store.setrange(key, *offset, value).map(Response::Integer)
        }
        Command::Incr { key } => store.incrby(key, 1).map(integer_reply),
        Command::Decr { key } => store.incrby(key, -1).map(integer_reply),
        Command::IncrBy { key, delta } => store.incrby(key, *delta).map(integer_reply),
        // Negating i64::MIN overflows.
        Command::DecrBy { key, delta } => match delta.checked_neg() {
            None => Ok(Response::Error("ERR decrement would overflow".into())),
            Some(d) => store.incrby(key, d).map(integer_reply),
        },
        // Refused here, after the type check, as Redis orders it. Redis
        // reads the stored value first, but a bad one gets the same error.
        Command::IncrByFloat { delta: None, .. } => {
            Ok(Response::Error("ERR value is not a valid float".into()))
        }
        Command::IncrByFloat {
            key,
            delta: Some(delta),
        } => store.incrbyfloat(key, *delta).map(|n| match n {
            Num::Value(text) => Response::BulkString(text.into_bytes()),
            Num::NotNumber => Response::Error("ERR value is not a valid float".into()),
            Num::Overflow => Response::Error("ERR increment would produce NaN or Infinity".into()),
        }),
        Command::GetBit { key, offset } => store.get(key).map(|v| {
            Response::Integer(crate::commands::bit_at(&v.unwrap_or_default(), *offset) as i64)
        }),
        Command::BitCount { key, range } => store.get(key).map(|v| {
            Response::Integer(crate::commands::count_bits(&v.unwrap_or_default(), *range))
        }),
        Command::SetBit { key, offset, value } => {
            store.setbit(key, *offset, *value).map(|(was, changed)| {
                crate::dirty::add(u64::from(changed));
                Response::Integer(was as i64)
            })
        }

        Command::HGet { key, field } => store.hget(key, field).map(bulk_or_nil),
        Command::HMGet { key, fields } => store.hmget(key, fields).map(Response::Array),
        Command::HGetAll { key } => store.hgetall(key).map(|pairs| {
            Response::Array(
                pairs
                    .into_iter()
                    .flat_map(|(f, v)| [Some(f), Some(v)])
                    .collect(),
            )
        }),
        Command::HKeys { key } => store.hkeys(key).map(array),
        Command::HVals { key } => store.hvals(key).map(array),
        Command::HLen { key } => store.hlen(key).map(Response::Integer),
        Command::HExists { key, field } => store
            .hexists(key, field)
            .map(|b| Response::Integer(b as i64)),
        Command::HStrLen { key, field } => store.hstrlen(key, field).map(Response::Integer),
        // A field named twice is set once, to its last value, where it was
        // first named, and counted once if it is new.
        Command::HSet { key, pairs } => store
            .hset(key, &crate::commands::dedup_fields(pairs), pairs.len())
            .map(Response::Integer),
        Command::HMSet { key, pairs } => store
            .hset(key, &crate::commands::dedup_fields(pairs), pairs.len())
            .map(|_| Response::Ok),
        Command::HSetNx { key, field, value } => store
            .hsetnx(key, field, value)
            .map(|b| Response::Integer(b as i64)),
        Command::HDel { key, fields } => store.hdel(key, fields).map(Response::Integer),
        Command::HIncrBy { key, field, delta } => {
            store.hincrby(key, field, *delta).map(|n| match n {
                Num::Value(v) => Response::Integer(v),
                Num::NotNumber => Response::Error("ERR hash value is not an integer".into()),
                Num::Overflow => {
                    Response::Error("ERR increment or decrement would overflow".into())
                }
            })
        }
        Command::HIncrByFloat { key, field, delta } => {
            store.hincrbyfloat(key, field, *delta).map(|n| match n {
                Num::Value(text) => Response::BulkString(text.into_bytes()),
                Num::NotNumber => Response::Error("ERR hash value is not a float".into()),
                Num::Overflow => {
                    Response::Error("ERR increment would produce NaN or Infinity".into())
                }
            })
        }
        Command::HRandField {
            key,
            count,
            withvalues,
        } => hrandfield(store, key, *count, *withvalues),

        Command::SAdd { key, members } => store.sadd(key, members).map(Response::Integer),
        Command::SRem { key, members } => store.srem(key, members).map(Response::Integer),
        Command::SIsMember { key, member } => store
            .sismember(key, member)
            .map(|b| Response::Integer(b as i64)),
        Command::SMisMember { key, members } => store
            .smismember(key, members)
            .map(|v| Response::IntegerArray(v.into_iter().map(i64::from).collect())),
        Command::SMembers { key } => store.smembers(key).map(array),
        Command::SCard { key } => store.scard(key).map(Response::Integer),
        // Without a count, one member or nil; with one, an array.
        Command::SPop { key, count } => match count {
            None => store
                .spop(key, 1)
                .map(|v| bulk_or_nil(v.into_iter().next())),
            Some(n) => store.spop(key, (*n).max(0)).map(array),
        },
        Command::SRandMember { key, count } => match count {
            None => store
                .srandmember(key, 1)
                .map(|v| bulk_or_nil(v.into_iter().next())),
            Some(0) => Ok(Response::Array(vec![])),
            Some(n) => store.srandmember(key, repeats(*n)).map(array),
        },
        // `smoveCommand`: onto itself, only whether the member is there, and
        // nothing changes; otherwise the destination is signalled only if
        // the member was not already in it.
        Command::SMove { src, dst, member } if src == dst => {
            store.sismember(src, member).map(|b| {
                crate::notify::untouched(src);
                Response::Integer(b as i64)
            })
        }
        Command::SMove { src, dst, member } => {
            store.smove(src, dst, member).map(|(moved, added)| {
                if moved && !added {
                    crate::notify::untouched(dst);
                }
                Response::Integer(moved as i64)
            })
        }
        Command::SUnion { keys } => store.sunion(keys).map(array),
        Command::SInter { keys } => store.sinter(keys).map(array),
        Command::SDiff { keys } => store.sdiff(keys).map(array),
        Command::SUnionStore { dst, keys } => store.sunionstore(dst, keys).map(Response::Integer),
        Command::SInterStore { dst, keys } => store.sinterstore(dst, keys).map(Response::Integer),
        Command::SDiffStore { dst, keys } => store.sdiffstore(dst, keys).map(Response::Integer),
        Command::SInterCard { keys, limit } => {
            store.sintercard(keys, *limit).map(Response::Integer)
        }

        Command::ZAdd {
            key,
            nx,
            xx,
            gt,
            lt,
            ch,
            incr,
            pairs,
        } => {
            let opts = ZaddOpts {
                nx: *nx,
                xx: *xx,
                gt: *gt,
                lt: *lt,
                ch: *ch,
            };
            if *incr {
                let (delta, member) = &pairs[0];
                store.zadd_incr(key, member, *delta, opts).map(|r| match r {
                    Incr::Score(s) => {
                        crate::dirty::add(1);
                        score(s)
                    }
                    Incr::Skipped => Response::Null,
                    Incr::NotANumber => Response::Error(crate::commands::NAN_SCORE.into()),
                })
            } else {
                // `server.dirty += added + updated`, whatever CH answers.
                store.zadd(key, pairs, opts).map(|(added, changed)| {
                    crate::dirty::add(changed.max(0) as u64);
                    Response::Integer(if *ch { changed } else { added })
                })
            }
        }
        Command::ZIncrBy {
            key,
            increment,
            member,
        } => {
            let none = ZaddOpts {
                nx: false,
                xx: false,
                gt: false,
                lt: false,
                ch: false,
            };
            store
                .zadd_incr(key, member, *increment, none)
                .map(|r| match r {
                    Incr::Score(s) => score(s),
                    Incr::Skipped => Response::Null,
                    Incr::NotANumber => Response::Error(crate::commands::NAN_SCORE.into()),
                })
        }
        Command::ZRem { key, members } => store.zrem(key, members).map(Response::Integer),
        Command::ZScore { key, member } => store
            .zscore(key, member)
            .map(|s| s.map_or(Response::Null, score)),
        Command::ZMScore { key, members } => store.zmscore(key, members).map(|v| {
            Response::Array(
                v.into_iter()
                    .map(|s| s.map(|s| format_score(s).into_bytes()))
                    .collect(),
            )
        }),
        Command::ZCard { key } => store.zcard(key).map(Response::Integer),
        Command::ZCount { key, min, max } => store.zcount(key, *min, *max).map(Response::Integer),
        Command::ZLexCount { key, min, max } => {
            store.zlexcount(key, min, max).map(Response::Integer)
        }
        // WITHSCORE asks for an array, so its miss is the nil array.
        Command::ZRank {
            key,
            member,
            rev,
            with_score,
        } => store
            .zrank(key, member, *rev)
            .map(|r| match (r, *with_score) {
                (Some((rank, s)), true) => {
                    Response::RankWithScore(rank, format_score(s).into_bytes())
                }
                (Some((rank, _)), false) => Response::Integer(rank),
                (None, true) => Response::NullArray,
                (None, false) => Response::Null,
            }),
        Command::ZRange {
            key,
            start,
            stop,
            by,
            rev,
            limit,
            with_scores,
        } => zrange(
            store,
            ZRangeArgs {
                key,
                start,
                stop,
                by: *by,
                rev: *rev,
                limit: *limit,
                with_scores: *with_scores,
            },
        ),
        Command::ZRangeByScore {
            key,
            min,
            max,
            rev,
            with_scores,
            limit,
        } => store
            .zrange_score(key, *min, *max, *rev, *limit)
            .map(|v| scored(v, *with_scores)),
        Command::ZRangeByLex {
            key,
            min,
            max,
            rev,
            limit,
        } => store.zrange_lex(key, min, max, *rev, *limit).map(array),
        Command::ZPopMin { key, count } => zpop(store, key, *count, true),
        Command::ZPopMax { key, count } => zpop(store, key, *count, false),
        Command::ZRandMember {
            key,
            count,
            with_scores,
        } => match count {
            None => store
                .zrandmember(key, 1)
                .map(|v| bulk_or_nil(v.into_iter().next().map(|(m, _)| m))),
            Some(0) => Ok(Response::Array(vec![])),
            Some(n) => store
                .zrandmember(key, repeats(*n))
                .map(|v| scored(v, *with_scores)),
        },
        Command::ZRemRangeByRank { key, start, stop } => store
            .zremrange_rank(key, *start, *stop)
            .map(Response::Integer),
        Command::ZRemRangeByScore { key, min, max } => store
            .zremrange_score(key, *min, *max)
            .map(Response::Integer),
        Command::ZRemRangeByLex { key, min, max } => {
            store.zremrange_lex(key, min, max).map(Response::Integer)
        }

        Command::LPush { key, values } => {
            store.lpush(key, values, true, false).map(Response::Integer)
        }
        Command::RPush { key, values } => store
            .lpush(key, values, false, false)
            .map(Response::Integer),
        Command::LPushX { key, values } => {
            store.lpush(key, values, true, true).map(Response::Integer)
        }
        Command::RPushX { key, values } => {
            store.lpush(key, values, false, true).map(Response::Integer)
        }
        Command::LPop { key, count } => pop(store, key, *count, true),
        Command::RPop { key, count } => pop(store, key, *count, false),
        Command::LLen { key } => store.llen(key).map(Response::Integer),
        Command::LRange { key, start, stop } => store.lrange(key, *start, *stop).map(array),
        Command::LIndex { key, index } => store.lindex(key, *index).map(bulk_or_nil),
        Command::LSet { key, index, value } => store.lset(key, *index, value).map(|r| match r {
            LSet::Done => Response::Ok,
            LSet::NoKey => Response::Error("ERR no such key".into()),
            LSet::OutOfRange => Response::Error("ERR index out of range".into()),
        }),
        Command::LInsert {
            key,
            before,
            pivot,
            value,
        } => store
            .linsert(key, *before, pivot, value)
            .map(Response::Integer),
        Command::LRem { key, count, value } => {
            store.lrem(key, *count, value).map(Response::Integer)
        }
        // `server.dirty += ltrim + rtrim`: the elements removed.
        Command::LTrim { key, start, stop } => store.llen(key).and_then(|before| {
            store.ltrim(key, *start, *stop)?;
            crate::dirty::add((before - store.llen(key)?).max(0) as u64);
            Ok(Response::Ok)
        }),
        Command::LMove {
            src,
            dst,
            src_left,
            dst_left,
        } => store.lmove(src, dst, *src_left, *dst_left).map(bulk_or_nil),
        Command::LPos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => store
            .lpos(key, element, rank.unwrap_or(1), *count, *maxlen)
            .map(|v| match count {
                None => v.first().map_or(Response::Null, |p| Response::Integer(*p)),
                Some(_) => Response::IntegerArray(v),
            }),
        _ => return None,
    };
    let reply = reply.unwrap_or_else(|e| e);
    crate::dirty::add(crate::dirty::of(cmd, &reply));
    Some(reply)
}

fn score(s: f64) -> Response {
    Response::BulkString(format_score(s).into_bytes())
}

/// Members, each followed by its score when asked for.
fn scored(v: Vec<(Vec<u8>, f64)>, with_scores: bool) -> Response {
    Response::Array(
        v.into_iter()
            .flat_map(|(m, s)| [Some(m), with_scores.then(|| format_score(s).into_bytes())])
            .flatten()
            .map(Some)
            .collect(),
    )
}

/// `ZRANGE` in all three forms. The bounds are read here, once: integers by
/// index, floats by score, `[`/`(`-prefixed strings by lex — and under REV
/// they come high end first.
/// `ZRANGE`'s arguments, as it parsed them.
struct ZRangeArgs<'a> {
    key: &'a [u8],
    start: &'a [u8],
    stop: &'a [u8],
    by: RangeBy,
    rev: bool,
    limit: Option<(i64, i64)>,
    with_scores: bool,
}

fn zrange(store: &mut dyn Store, args: ZRangeArgs<'_>) -> Out<Response> {
    let ZRangeArgs {
        key,
        start,
        stop,
        by,
        rev,
        limit,
        with_scores,
    } = args;
    let not_int = || Response::Error(crate::commands::NOT_AN_INTEGER.to_string());
    let (lo, hi) = if rev { (stop, start) } else { (start, stop) };
    match by {
        RangeBy::Index => {
            let s = crate::commands::int_from_bytes(start).ok_or_else(not_int)?;
            let e = crate::commands::int_from_bytes(stop).ok_or_else(not_int)?;
            store
                .zrange_index(key, s, e, rev)
                .map(|v| scored(v, with_scores))
        }
        RangeBy::Score => {
            let bound = |b| {
                crate::commands::parse_score_bound(b)
                    .ok_or_else(|| Response::Error("ERR min or max is not a float".into()))
            };
            let (min, max) = (bound(lo)?, bound(hi)?);
            store
                .zrange_score(key, min, max, rev, limit)
                .map(|v| scored(v, with_scores))
        }
        RangeBy::Lex => {
            let bound = |b| {
                crate::commands::parse_lex_bound(b).ok_or_else(|| {
                    Response::Error("ERR min or max not valid string range item".into())
                })
            };
            let (min, max) = (bound(lo)?, bound(hi)?);
            store.zrange_lex(key, &min, &max, rev, limit).map(array)
        }
    }
}

/// `ZPOPMIN`/`ZPOPMAX`: an array of member, score pairs, count or none.
fn zpop(store: &mut dyn Store, key: &[u8], count: Option<i64>, min: bool) -> Out<Response> {
    let n = count.unwrap_or(1).max(0);
    if n == 0 {
        return Ok(Response::Array(vec![]));
    }
    store.zpop(key, n, min).map(|v| scored(v, true))
}

/// `LPOP`/`RPOP`. Without a count, one element or nil; with one, an array —
/// empty for a count of 0 on a list that is there, and the nil array for a
/// list that is not.
fn pop(store: &mut dyn Store, key: &[u8], count: Option<i64>, left: bool) -> Out<Response> {
    match count {
        None => store
            .lpop(key, 1, left)
            .map(|v| bulk_or_nil(v.into_iter().next())),
        Some(n) if n <= 0 => Ok(if store.llen(key)? > 0 {
            Response::Array(vec![])
        } else {
            Response::NullArray
        }),
        Some(n) => store.lpop(key, n, left).map(|v| {
            if v.is_empty() {
                Response::NullArray
            } else {
                array(v)
            }
        }),
    }
}

fn now_us() -> i64 {
    crate::server::now_ms().saturating_mul(1000)
}

/// `ttlGenericCommand`: TTL and PTTL count down from now — a key past its
/// deadline is gone — and TTL rounds to the nearest second; EXPIRETIME and
/// PEXPIRETIME give the deadline itself.
fn ttl(store: &mut dyn Store, key: &[u8], ms: bool, relative: bool) -> Out<Response> {
    let n = match store.deadline(key)? {
        None => -2,
        Some(None) => -1,
        Some(Some(at)) if relative => match (at - now_us()) / 1000 {
            left if left < 0 => -2,
            left if ms => left,
            left => (left + 500) / 1000,
        },
        Some(Some(at)) => at / if ms { 1000 } else { 1_000_000 },
    };
    Ok(Response::Integer(n))
}

/// The EXPIRE family: the deadline worked out once, here, and a deadline
/// already past deletes the key there and then (`checkAlreadyExpired`).
fn expire(store: &mut dyn Store, cmd: &Command) -> Out<Response> {
    let now = now_us();
    let (key, at, cond) = match cmd {
        Command::Expire { key, secs, cond } => (
            key,
            now.saturating_add(secs.saturating_mul(1_000_000)),
            *cond,
        ),
        Command::PExpire { key, ms, cond } => {
            (key, now.saturating_add(ms.saturating_mul(1000)), *cond)
        }
        Command::ExpireAt {
            key,
            unix_secs,
            cond,
        } => (key, unix_secs.saturating_mul(1_000_000), *cond),
        Command::PExpireAt { key, unix_ms, cond } => (key, unix_ms.saturating_mul(1000), *cond),
        _ => unreachable!("only the EXPIRE family comes here"),
    };
    let set = store.expire(key, at, cond)?;
    if set && at <= now {
        store.del(std::slice::from_ref(key))?;
    }
    Ok(Response::Integer(set as i64))
}

/// `RENAMENX`: a destination that exists refuses — once the source is known
/// to be there, or Redis says so first.
fn renamenx(store: &mut dyn Store, key: &[u8], newkey: &[u8]) -> Out<Response> {
    if store.kind(newkey)?.is_some() {
        return Ok(match store.kind(key)? {
            None => Response::Error("ERR no such key".into()),
            Some(_) => Response::Integer(0),
        });
    }
    Ok(match store.rename(key, newkey)? {
        true => Response::Integer(1),
        false => Response::Error("ERR no such key".into()),
    })
}

/// `COUNT`'s default, as in Redis.
const SCAN_COUNT: i64 = 10;

fn scan_count(count: Option<i64>) -> usize {
    usize::try_from(count.unwrap_or(SCAN_COUNT)).unwrap_or(usize::MAX)
}

fn matches(pattern: Option<&[u8]>, item: &[u8]) -> bool {
    pattern.is_none_or(|p| crate::pubsub::glob_match(p, item))
}

/// `SCAN`: one step of the backend's cursor. MATCH and TYPE filter what the
/// step found, as in Redis, so a step can return nothing and still not be
/// the last. A TYPE pg_redis has no table for matches nothing, as in Redis
/// 7.2.
fn scan(
    store: &mut dyn Store,
    cursor: u64,
    count: Option<i64>,
    pattern: Option<&[u8]>,
    kind: Option<Option<KeyKind>>,
) -> Out<Response> {
    let (next, mut keys) = store.scan_keys(cursor, scan_count(count))?;
    keys.retain(|k| matches(pattern, k));
    if let Some(want) = kind
        && !keys.is_empty()
    {
        let kinds = store.kinds(&keys)?;
        keys = keys
            .into_iter()
            .zip(kinds)
            .filter(|(_, k)| want.is_some() && *k == want)
            .map(|(key, _)| key)
            .collect();
    }
    Ok(Response::ScanResult {
        cursor: next,
        keys: keys.into_iter().map(Some).collect(),
    })
}

/// `HSCAN`, `SSCAN` and `ZSCAN`: one step over a collection, MATCH applied
/// to fields and members afterwards. A missing key is an empty collection.
fn collection_scan(
    store: &mut dyn Store,
    kind: KeyKind,
    key: &[u8],
    cursor: u64,
    count: Option<i64>,
    pattern: Option<&[u8]>,
) -> Out<Response> {
    let count = scan_count(count);
    let (next, items): (u64, Vec<Vec<u8>>) = match kind {
        KeyKind::Hash => {
            let (next, pairs) = store.hscan(key, cursor, count)?;
            let items = pairs
                .into_iter()
                .filter(|(f, _)| matches(pattern, f))
                .flat_map(|(f, v)| [f, v])
                .collect();
            (next, items)
        }
        KeyKind::Zset => {
            let (next, pairs) = store.zscan(key, cursor, count)?;
            let items = pairs
                .into_iter()
                .filter(|(m, _)| matches(pattern, m))
                .flat_map(|(m, s)| [m, format_score(s).into_bytes()])
                .collect();
            (next, items)
        }
        _ => {
            let (next, mut members) = store.sscan(key, cursor, count)?;
            members.retain(|m| matches(pattern, m));
            (next, members)
        }
    };
    Ok(Response::ScanResult {
        cursor: next,
        keys: items.into_iter().map(Some).collect(),
    })
}

/// `HRANDFIELD`: a positive count below the hash's size picks at random, in
/// the hash's order when it is a listpack.
/// A negative count (repeats allowed) no larger than the client's output
/// limit lets its reply be (`pubsub::cap_repeats`).
fn repeats(n: i64) -> i64 {
    if n < 0 {
        -(crate::pubsub::cap_repeats(n.unsigned_abs()) as i64)
    } else {
        n
    }
}

fn hrandfield(
    store: &mut dyn Store,
    key: &[u8],
    count: Option<i64>,
    withvalues: bool,
) -> Out<Response> {
    let pairs = store.hgetall(key)?;
    let ordered = match count {
        Some(n) if n > 1 && (n as u64) < pairs.len() as u64 => {
            store.collection_encoding(key)? == Some(Enc::Listpack)
        }
        _ => true,
    };
    Ok(crate::commands::random_fields(
        &pairs, count, withvalues, ordered,
    ))
}

/// The encoding Redis would give the key. A hash, set or sorted set carries
/// its own, kept as Redis keeps it (`memdb::encoding`); a list's is worked
/// out from its elements, a string's from its value. A missing key is nil,
/// not an error, as in Redis.
fn object_encoding(store: &mut dyn Store, key: &[u8]) -> Out<Response> {
    let Some(kind) = store.kind(key)? else {
        return Ok(Response::Null);
    };
    let name = match kind {
        KeyKind::Stream => "stream",
        KeyKind::String => crate::commands::string_encoding(&store.get(key)?.unwrap_or_default()),
        KeyKind::List => crate::memdb::encoding::list_encoding(
            store.lrange(key, 0, -1)?.iter().map(Vec::as_slice),
            &crate::memdb::encoding::Limits::current(),
        ),
        KeyKind::Set | KeyKind::Hash | KeyKind::Zset => match store.collection_encoding(key)? {
            Some(enc) => enc.name(),
            None => return Ok(Response::Null),
        },
    };
    // A bulk string, as Redis's `addReplyBulkCString`: `$8 listpack`, not
    // `+listpack`.
    Ok(Response::BulkString(name.as_bytes().to_vec()))
}

/// `OBJECT REFCOUNT | IDLETIME | FREQ`, with Redis's replies and errors.
fn object_access(
    store: &mut dyn Store,
    key: &[u8],
    field: crate::commands::ObjectField,
) -> Out<Response> {
    use crate::commands::ObjectField;
    use crate::memdb::lru;
    const SWITCHING: &str = "Please note that when switching between policies at runtime \
                             LRU and LFU data will take some time to adjust.";
    let Some((lru_field, int)) = store.access(key)? else {
        return Ok(Response::Null);
    };
    let s = crate::access::settings();
    let now = crate::server::now_ms();
    Ok(match field {
        // Redis shares one object for each of 0..9999 unless the policy
        // needs every key to have its own access field.
        ObjectField::Refcount => Response::Integer(
            if int.is_some_and(|n| (0..10_000).contains(&n)) && s.shares_integers() {
                i64::from(i32::MAX)
            } else {
                1
            },
        ),
        ObjectField::Idletime if s.policy.lfu() => Response::Error(format!(
            "ERR An LFU maxmemory policy is selected, idle time not tracked. {SWITCHING}"
        )),
        ObjectField::Idletime => {
            Response::Integer((lru::idle_ms(lru_field, lru::lru_clock(now)) / 1000) as i64)
        }
        ObjectField::Freq if !s.policy.lfu() => Response::Error(format!(
            "ERR An LFU maxmemory policy is not selected, access frequency not tracked. \
             {SWITCHING}"
        )),
        ObjectField::Freq => Response::Integer(i64::from(lru::lfu_decr_and_return(
            lru_field,
            s.decay_time,
            lru::lfu_minutes(now),
        ))),
    })
}

fn integer_reply(n: Num<i64>) -> Response {
    match n {
        Num::Value(v) => Response::Integer(v),
        Num::NotNumber => Response::Error("ERR value is not an integer or out of range".into()),
        Num::Overflow => Response::Error("ERR increment or decrement would overflow".into()),
    }
}

fn bulk_or_nil(v: Option<Vec<u8>>) -> Response {
    v.map_or(Response::Null, Response::BulkString)
}

fn array(items: Vec<Vec<u8>>) -> Response {
    Response::Array(items.into_iter().map(Some).collect())
}

/// What Redis's `string2ll` takes for an integer: no sign but `-`, no leading
/// zero, no `-0`, and within 64 bits.
fn is_integer(v: &[u8]) -> bool {
    std::str::from_utf8(v).is_ok_and(|t| {
        t.parse::<i64>().is_ok()
            && !t.starts_with('+')
            && (t == "0" || !(t.starts_with('0') || t.starts_with("-0")))
    })
}

// ──────────────────────────────── tables ────────────────────────────────────

/// Rows of a first column and an optional second.
type Rows = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// One database's tables.
pub struct SqlStore<'a, 'b> {
    pub client: &'a mut SpiClient<'b>,
    pub db: u8,
}

fn failed(cmd: &str, e: impl std::fmt::Display) -> Response {
    crate::commands::spi_failed(cmd, e)
}

impl SqlStore<'_, '_> {
    fn rows(&mut self, cmd: &str, sql: &str, args: &[DatumWithOid]) -> Out<Rows> {
        let tbl = self
            .client
            .select_kept(sql, args)
            .map_err(|e| failed(cmd, e))?;
        Ok(tbl
            .filter_map(|r| {
                Some((
                    r.get::<Vec<u8>>(1).ok()??,
                    r.get::<Vec<u8>>(2).ok().flatten(),
                ))
            })
            .collect())
    }

    fn column(&mut self, cmd: &str, sql: &str, args: &[DatumWithOid]) -> Out<Vec<Vec<u8>>> {
        let tbl = self
            .client
            .select_kept(sql, args)
            .map_err(|e| failed(cmd, e))?;
        Ok(tbl
            .filter_map(|r| r.get::<Vec<u8>>(1).ok().flatten())
            .collect())
    }

    fn int(&mut self, cmd: &str, sql: &str, args: &[DatumWithOid]) -> Out<i64> {
        let tbl = self
            .client
            .select_kept(sql, args)
            .map_err(|e| failed(cmd, e))?;
        Ok(tbl.first().get::<i64>(1).ok().flatten().unwrap_or(0))
    }

    fn write_int(&mut self, cmd: &str, sql: &str, args: &[DatumWithOid]) -> Out<i64> {
        let tbl = self
            .client
            .update_kept(sql, args)
            .map_err(|e| failed(cmd, e))?;
        Ok(tbl.first().get::<i64>(1).ok().flatten().unwrap_or(0))
    }

    fn write_column(&mut self, cmd: &str, sql: &str, args: &[DatumWithOid]) -> Out<Vec<Vec<u8>>> {
        let tbl = self
            .client
            .update_kept(sql, args)
            .map_err(|e| failed(cmd, e))?;
        Ok(tbl
            .filter_map(|r| r.get::<Vec<u8>>(1).ok().flatten())
            .collect())
    }
}

// ─────────────────────────── encodings on the tables ──────────────────────────
//
// A hash, set or sorted set on the tables keeps the encoding Redis would have
// given it as a row of `redis.encoding_N`, written only when the key changes
// encoding, and the order of a hash's fields and a set's members as `seq`.
// A key with no row is in the encoding its contents imply (`natural_enc`):
// a hash or sorted set that never converted is a listpack, a set of integers
// an intset, any other set a listpack. So the row is needed for a converted
// key, and for a listpack set that holds an integer, which its contents
// would make an intset once its strings were removed.
//
// Every write that can create a key reads the row and the key's size before
// it writes, in the same statement (`pre_cte`), works out the encoding as
// Redis would, and writes the row only if it changed. A key the write finds
// absent starts afresh, so a row left behind by a key that emptied cannot
// leak into the next key of that name.

/// The row `redis.encoding_N` holds for a collection of `kind` in `enc`.
/// `has_int`: whether a set may hold an integer.
pub(crate) fn enc_row(kind: KeyKind, enc: Enc, has_int: bool) -> Option<Enc> {
    match (kind, enc) {
        (KeyKind::Set, Enc::Intset) => None,
        (KeyKind::Set, Enc::Listpack) => has_int.then_some(Enc::Listpack),
        (_, Enc::Listpack) => None,
        (_, e) => Some(e),
    }
}

/// Write `row` as `key`'s encoding row, or remove its row.
pub(crate) fn put_enc_row(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    row: Option<Enc>,
) -> Result<(), pgrx::spi::SpiError> {
    match row {
        Some(e) => client.update_kept(
            &format!(
                "INSERT INTO redis.encoding_{db} (key, encoding) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET encoding = EXCLUDED.encoding"
            ),
            &[key.into(), e.code().into()],
        ),
        None => client.update_kept(
            &format!("DELETE FROM redis.encoding_{db} WHERE key = $1"),
            &[key.into()],
        ),
    }
    .map(|_| ())
}

/// The encoding of a collection the tables hold: its row, or what its
/// contents imply. `items` are a set's members, a hash's fields and values,
/// or a sorted set's members.
pub(crate) fn natural_enc(kind: KeyKind, row: Option<Enc>, items: &[&[u8]]) -> Enc {
    if let Some(e) = row {
        return e;
    }
    let l = crate::memdb::encoding::Limits::current();
    let widest = items.iter().map(|m| m.len()).max().unwrap_or(0);
    match kind {
        KeyKind::Set => crate::memdb::encoding::set_natural(items.iter().copied(), &l),
        KeyKind::Hash => crate::memdb::encoding::hash_shape(items.len() / 2, widest, &l),
        _ => crate::memdb::encoding::zset_shape(items.len(), widest, &l),
    }
}

/// A set's members, read in `seq` order, in the order its encoding keeps
/// them: an intset's sorted.
pub(crate) fn set_order(mut members: Vec<Vec<u8>>, row: Option<Enc>) -> (Vec<Vec<u8>>, Enc) {
    let slices: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let enc = natural_enc(KeyKind::Set, row, &slices);
    if enc == Enc::Intset {
        crate::memdb::encoding::sort_ints(&mut members, |m| m);
    }
    (members, enc)
}

/// The CTE `pre`: the encoding row of the key in parameter `$k`, whether
/// `redis.<table>_N` holds it, and how many rows it has there before the
/// statement's write. The rows are counted no further than parameter `$cap`,
/// only when `counted_if` holds (a write that adds nothing cannot pass a
/// size limit), and not at all once the key converted: a converted key never
/// converts again. Every part reads the snapshot the statement started from,
/// whichever CTE it follows.
fn pre_cte(db: u8, table: &str, k: usize, cap: usize, counted_if: &str) -> String {
    format!(
        "pre AS (SELECT e.enc, \
                        EXISTS (SELECT 1 FROM redis.{table}_{db} WHERE key = ${k}) AS live, \
                        CASE WHEN e.enc IN (3, 4) OR NOT ({counted_if}) THEN NULL \
                             ELSE (SELECT count(*) FROM (SELECT 1 FROM redis.{table}_{db} \
                                                          WHERE key = ${k} LIMIT ${cap}) c) \
                        END AS n \
                 FROM (SELECT (SELECT encoding FROM redis.encoding_{db} WHERE key = ${k}) AS enc) e)"
    )
}

/// The columns a statement with `pre_cte` adds to its reply.
const PRE_COLS: &str = "(SELECT enc FROM pre), (SELECT live FROM pre), (SELECT n FROM pre)";

/// What `pre_cte` found.
#[derive(Clone, Copy)]
struct Pre {
    row: Option<Enc>,
    live: bool,
    /// Rows before the write, up to the cap; `None` once converted.
    n: Option<usize>,
}

/// `Pre` from a reply's columns `$at`, `$at + 1` and `$at + 2`: a row of an
/// iterated table, or a table's current row.
macro_rules! pre_at {
    ($row:expr, $at:expr) => {
        Pre {
            row: $row.get::<i16>($at).ok().flatten().and_then(Enc::from_code),
            live: $row.get::<bool>($at + 1).ok().flatten().unwrap_or(false),
            n: $row
                .get::<i64>($at + 2)
                .ok()
                .flatten()
                .map(|n| n.max(0) as usize),
        }
    };
}

impl Pre {
    /// The encoding the key was in: `None` when it was not there, which
    /// leaves any row behind it out of account.
    fn enc(&self, default: Enc) -> Option<Enc> {
        self.live.then(|| self.row.unwrap_or(default))
    }

    /// How many elements the key held, as far as counted.
    fn len(&self) -> usize {
        if self.live {
            self.n.unwrap_or(usize::MAX / 2)
        } else {
            0
        }
    }
}

/// A cap for `pre_cte`: one past the largest packed size, so a key the count
/// stops at is known to be past it.
fn cap(n: usize) -> i64 {
    i64::try_from(n.saturating_add(1)).unwrap_or(i64::MAX)
}

/// A set on the tables, as the set operations read it.
struct SqlSet {
    members: Vec<Vec<u8>>,
    index: std::collections::HashSet<Vec<u8>>,
}

impl crate::memdb::encoding::SetView for SqlSet {
    fn size(&self) -> usize {
        self.members.len()
    }

    fn has(&self, m: &[u8]) -> bool {
        self.index.contains(m)
    }

    fn members(&self) -> Vec<&[u8]> {
        self.members.iter().map(Vec::as_slice).collect()
    }
}

impl SqlStore<'_, '_> {
    /// Write the encoding row a key in `enc` needs, if it is not the row
    /// `pre` found.
    fn sync_enc(
        &mut self,
        cmd: &str,
        key: &[u8],
        pre: &Pre,
        kind: KeyKind,
        enc: Enc,
        has_int: bool,
    ) -> Out<()> {
        let want = enc_row(kind, enc, has_int);
        if want != pre.row {
            put_enc_row(self.client, self.db, key, want).map_err(|e| failed(cmd, e))?;
        }
        Ok(())
    }

    /// Every named set, its members in its order; `None` for a key with no
    /// set. A key named twice is read twice.
    fn sets_in_order(&mut self, cmd: &str, keys: &[Vec<u8>]) -> Out<Vec<Option<SqlSet>>> {
        let db = self.db;
        let sql = format!(
            "SELECT k.i, s.member, (SELECT encoding FROM redis.encoding_{db} e WHERE e.key = k.key) \
             FROM unnest($1::bytea[]) WITH ORDINALITY AS k(key, i) \
             JOIN redis.set_{db} s ON s.key = k.key \
             ORDER BY k.i, s.seq"
        );
        let tbl = self
            .client
            .select_kept(&sql, &[arr(keys).into()])
            .map_err(|e| failed(cmd, e))?;
        let mut found: Vec<(Vec<Vec<u8>>, Option<Enc>)> = vec![(Vec::new(), None); keys.len()];
        for r in tbl {
            let (Ok(Some(i)), Ok(Some(m))) = (r.get::<i64>(1), r.get::<Vec<u8>>(2)) else {
                continue;
            };
            if let Some(slot) = found.get_mut((i - 1) as usize) {
                slot.0.push(m);
                slot.1 = r.get::<i16>(3).ok().flatten().and_then(Enc::from_code);
            }
        }
        Ok(found
            .into_iter()
            .map(|(members, row)| {
                if members.is_empty() {
                    return None;
                }
                let (members, _) = set_order(members, row);
                let index = members.iter().cloned().collect();
                Some(SqlSet { members, index })
            })
            .collect())
    }

    /// `SINTER` on the tables: the set it walks is read in its order and
    /// filtered by the others there, so only the result comes back. With
    /// the encoding of the set walked.
    fn inter_in_order(&mut self, cmd: &str, keys: &[Vec<u8>]) -> Out<(Vec<Vec<u8>>, Enc)> {
        let db = self.db;
        let sizes_sql = format!(
            "SELECT (SELECT count(*) FROM redis.set_{db} s WHERE s.key = k.key), \
                    (SELECT encoding FROM redis.encoding_{db} e WHERE e.key = k.key), \
                    (SELECT member FROM redis.set_{db} s WHERE s.key = k.key LIMIT 1) \
             FROM unnest($1::bytea[]) WITH ORDINALITY AS k(key, i) ORDER BY k.i"
        );
        let tbl = self
            .client
            .select_kept(&sizes_sql, &[arr(keys).into()])
            .map_err(|e| failed(cmd, e))?;
        let mut sizes = Vec::new();
        let mut kinds = Vec::new();
        for r in tbl {
            sizes.push(r.get::<i64>(1).ok().flatten().unwrap_or(0).max(0) as usize);
            let row = r.get::<i16>(2).ok().flatten().and_then(Enc::from_code);
            let sample = r.get::<Vec<u8>>(3).ok().flatten().unwrap_or_default();
            kinds.push(row.unwrap_or_else(|| {
                if crate::memdb::parse_stored_int(&sample).is_some() {
                    Enc::Intset
                } else {
                    Enc::Listpack
                }
            }));
        }
        if sizes.len() != keys.len() || sizes.contains(&0) {
            return Ok((Vec::new(), Enc::Listpack));
        }
        let walked = crate::memdb::encoding::inter_order(&sizes)[0];
        let others: Vec<Vec<u8>> = keys
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != walked)
            .map(|(_, k)| k.clone())
            .collect();
        let sql = format!(
            "SELECT s.member FROM redis.set_{db} s WHERE s.key = $1 \
               AND NOT EXISTS (SELECT 1 FROM unnest($2::bytea[]) AS o(key) \
                               WHERE NOT EXISTS (SELECT 1 FROM redis.set_{db} t \
                                                 WHERE t.key = o.key AND t.member = s.member)) \
             ORDER BY s.seq"
        );
        let mut members = self.column(
            cmd,
            &sql,
            &[keys[walked].as_slice().into(), arr(&others).into()],
        )?;
        if kinds[walked] == Enc::Intset {
            crate::memdb::encoding::sort_ints(&mut members, |m| m);
        }
        Ok((members, kinds[walked]))
    }

    /// Replace `dst` with a set of `members`, in their order and in `enc`.
    /// Its size.
    fn store_set(&mut self, cmd: &str, dst: &[u8], members: &[Vec<u8>], enc: Enc) -> Out<i64> {
        let db = self.db;
        let has_int = members
            .iter()
            .any(|m| crate::memdb::parse_stored_int(m).is_some());
        let row = enc_row(KeyKind::Set, enc, has_int).map(Enc::code);
        // Members kept are renumbered to the new order, the rest go; the two
        // touch different rows, so they can share a statement.
        let sql = format!(
            "WITH new_data AS (SELECT m, i FROM unnest($2::bytea[]) WITH ORDINALITY AS u(m, i)), \
                  del AS (DELETE FROM redis.set_{db} WHERE key = $1 AND member <> ALL($2::bytea[])), \
                  ins AS (INSERT INTO redis.set_{db} (key, member) \
                          SELECT $1, m FROM new_data ORDER BY i \
                          ON CONFLICT (key, member) DO UPDATE SET seq = EXCLUDED.seq), \
                  {} \
             SELECT count(*)::bigint FROM new_data",
            enc_write_ctes(db, 1, 3)
        );
        self.write_int(cmd, &sql, &[dst.into(), arr(members).into(), row.into()])
    }
}

/// CTEs that make the encoding row of the key in `$k` the value in `$e` (a
/// `smallint`), removing it when that is NULL. The two touch the row under
/// opposite conditions, so they can share a statement.
pub(crate) fn enc_write_ctes(db: u8, k: usize, e: usize) -> String {
    format!(
        "encdel AS (DELETE FROM redis.encoding_{db} WHERE key = ${k} AND ${e}::smallint IS NULL), \
         encput AS (INSERT INTO redis.encoding_{db} (key, encoding) \
                    SELECT ${k}, ${e}::smallint WHERE ${e}::smallint IS NOT NULL \
                    ON CONFLICT (key) DO UPDATE SET encoding = EXCLUDED.encoding)"
    )
}

impl Store for SqlStore<'_, '_> {
    fn del(&mut self, keys: &[Vec<u8>]) -> Out<i64> {
        let db = self.db;
        let deletes = ["kv", "hash", "list", "set", "zset", "stream"]
            .iter()
            .enumerate()
            .map(|(i, t)| format!("d{i} AS (DELETE FROM redis.{t}_{db} WHERE key = ANY($1::bytea[]) RETURNING key)"))
            .collect::<Vec<_>>()
            .join(", ");
        let union = (0..6)
            .map(|i| format!("SELECT key FROM d{i}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let sql = format!(
            "WITH {deletes}, \
                  denc AS (DELETE FROM redis.encoding_{db} WHERE key = ANY($1::bytea[])) \
             SELECT count(DISTINCT key)::bigint FROM ({union}) u"
        );
        self.write_int("DEL", &sql, &[arr(keys).into()])
    }

    fn exists(&mut self, keys: &[Vec<u8>]) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "SELECT count(*)::bigint FROM unnest($1::bytea[]) AS a(key) \
             WHERE EXISTS (SELECT 1 FROM redis.kv_{db} t WHERE t.key = a.key \
                             AND (t.expires_at IS NULL OR t.expires_at > now())) \
                OR (NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = a.key AND e.expires_at <= now()) \
                    AND (EXISTS (SELECT 1 FROM redis.hash_{db} t WHERE t.key = a.key) \
                      OR EXISTS (SELECT 1 FROM redis.list_{db} t WHERE t.key = a.key) \
                      OR EXISTS (SELECT 1 FROM redis.set_{db}  t WHERE t.key = a.key) \
                      OR EXISTS (SELECT 1 FROM redis.zset_{db} t WHERE t.key = a.key) \
                      OR EXISTS (SELECT 1 FROM redis.stream_{db} t WHERE t.key = a.key)))"
        );
        self.int("EXISTS", &sql, &[arr(keys).into()])
    }

    fn kind(&mut self, key: &[u8]) -> Out<Option<KeyKind>> {
        Ok(Command::sql_key_kinds(self.client, self.db, &[key])
            .first()
            .and_then(|(_, k)| *k))
    }

    /// A logged key's access field lives in shared memory, not in its rows
    /// (`crate::access`), so reading it costs no write.
    fn access(&mut self, key: &[u8]) -> Out<Option<(u32, Option<i64>)>> {
        let Some(kind) = self.kind(key)? else {
            return Ok(None);
        };
        let int = match kind {
            KeyKind::String => self
                .get(key)?
                .and_then(|v| crate::memdb::parse_stored_int(&v)),
            _ => None,
        };
        Ok(Some((crate::access::logged_field(self.db, key), int)))
    }

    fn kinds(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Option<KeyKind>>> {
        let named: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let found: std::collections::HashMap<Vec<u8>, Option<KeyKind>> =
            Command::sql_key_kinds(self.client, self.db, &named)
                .into_iter()
                .collect();
        Ok(keys
            .iter()
            .map(|k| found.get(k).copied().flatten())
            .collect())
    }

    fn deadline(&mut self, key: &[u8]) -> Out<Option<Option<i64>>> {
        let sql = &crate::commands::sql::EXPIRE_US[self.db as usize];
        let tbl = self
            .client
            .select_kept(sql, &[key.into()])
            .map_err(|e| failed("TTL", e))?;
        Ok(
            match tbl.first().get::<i64>(1).ok().flatten().unwrap_or(-2) {
                -2 => None,
                -1 => Some(None),
                at => Some(Some(at)),
            },
        )
    }

    /// The deadline in whole milliseconds, clamped to what a PostgreSQL
    /// timestamp can hold: Redis takes any deadline that fits in
    /// milliseconds, and PostgreSQL's arithmetic overflows long before.
    fn expire(&mut self, key: &[u8], deadline_us: i64, cond: ExpireCond) -> Out<bool> {
        let sql = crate::commands::sql::set_expiry(
            self.db as usize,
            "to_timestamp(0) + (GREATEST(LEAST($2::bigint, 9000000000000000), -200000000000000) \
             * interval '1 millisecond')",
            cond,
        );
        let ms = deadline_us.div_euclid(1000);
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), ms.into()])
            .map_err(|e| failed("EXPIRE", e))?;
        Ok(!tbl.is_empty())
    }

    fn persist(&mut self, key: &[u8]) -> Out<bool> {
        let sql = &crate::commands::sql::PERSIST[self.db as usize];
        let tbl = self
            .client
            .update_kept(sql, &[key.into()])
            .map_err(|e| failed("PERSIST", e))?;
        Ok(!tbl.is_empty())
    }

    fn rename(&mut self, key: &[u8], newkey: &[u8]) -> Out<bool> {
        match Command::rename_to(self.client, self.db, key, newkey) {
            Response::Ok => Ok(true),
            Response::Error(e) if e.contains("no such key") => Ok(false),
            other => Err(other),
        }
    }

    fn keys(&mut self, pattern: Option<&[u8]>) -> Out<Vec<Vec<u8>>> {
        Ok(
            crate::commands::matching_keys(self.client, self.db, pattern)
                .into_iter()
                .flatten()
                .collect(),
        )
    }

    /// Keyset pagination over the six tables a key can be in: each yields
    /// its first `count` keys after the bound through its primary key, and
    /// the first `count` of their union are the page. A key in a table that
    /// is not among them would sort after all of them, so the page has no
    /// gap. The cursor follows the page's last key, live or not; a key past
    /// its deadline is dropped from the reply and reaped, as Redis's
    /// `scanCallback` expires each key it looks up.
    fn scan_keys(&mut self, cursor: u64, count: usize) -> Out<(u64, Vec<Vec<u8>>)> {
        let db = self.db;
        let scope = crate::cursor::scope(db, 0, b"");
        let (next, rows) = crate::cursor::step(
            crate::cursor_state(),
            cursor,
            scope,
            count,
            token_random(),
            |range, limit| {
                let (from, upto) = range_args(range);
                let tbl = self
                    .client
                    .select_kept(
                        &scan_page_sql(db, range),
                        &[from.into(), limit.into(), upto.into()],
                    )
                    .map_err(|e| failed("SCAN", e))?;
                Ok(tbl
                    .filter_map(|r| {
                        Some((
                            r.get::<Vec<u8>>(1).ok()??,
                            r.get::<bool>(2).ok().flatten().unwrap_or(true),
                        ))
                    })
                    .collect())
            },
        )?;
        let (live, expired): (Vec<_>, Vec<_>) = rows.into_iter().partition(|(_, live)| *live);
        let expired: Vec<&[u8]> = expired.iter().map(|(k, _)| k.as_slice()).collect();
        Command::reap_keys(self.client, db, &expired);
        Ok((next, live.into_iter().map(|(k, _)| k).collect()))
    }

    fn dbsize(&mut self) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            // Every key held, expired or not, as Redis's dictionary counts
            // them until they are reaped.
            "SELECT count(DISTINCT key)::bigint FROM ( \
                 SELECT key FROM redis.kv_{db} \
                 UNION ALL SELECT DISTINCT key FROM redis.hash_{db} \
                 UNION ALL SELECT DISTINCT key FROM redis.list_{db} \
                 UNION ALL SELECT DISTINCT key FROM redis.set_{db} \
                 UNION ALL SELECT DISTINCT key FROM redis.zset_{db} \
                 UNION ALL SELECT key FROM redis.stream_{db} \
             ) u"
        );
        self.int("DBSIZE", &sql, &[])
    }

    fn keyspace(&mut self) -> Out<(i64, i64)> {
        let db = self.db;
        let sql = format!(
            "SELECT \
               (SELECT count(*) FROM redis.kv_{db}) \
               + (SELECT count(*) FROM (SELECT key FROM redis.hash_{db} UNION SELECT key FROM redis.list_{db} \
                    UNION SELECT key FROM redis.set_{db} UNION SELECT key FROM redis.zset_{db} \
                    UNION SELECT key FROM redis.stream_{db}) c), \
               (SELECT count(*) FROM redis.kv_{db} WHERE expires_at IS NOT NULL) \
               + (SELECT count(*) FROM redis.expiry_{db})"
        );
        let tbl = self
            .client
            .select_kept(&sql, &[])
            .map_err(|e| failed("INFO", e))?;
        let row = tbl.first();
        Ok((
            row.get::<i64>(1).ok().flatten().unwrap_or(0),
            row.get::<i64>(2).ok().flatten().unwrap_or(0),
        ))
    }

    /// Every live key weighs the same, whatever table holds it: `UNION`,
    /// because a list stores a row per element.
    fn random_key(&mut self) -> Out<Option<Vec<u8>>> {
        let db = self.db;
        let sql = format!(
            "SELECT key FROM (SELECT key FROM redis.kv_{db} WHERE (expires_at IS NULL OR expires_at > now()) \
                 UNION SELECT key FROM redis.list_{db} UNION SELECT key FROM redis.set_{db} \
                 UNION SELECT key FROM redis.hash_{db} UNION SELECT key FROM redis.zset_{db} \
                 UNION SELECT key FROM redis.stream_{db}) u \
             WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = u.key AND e.expires_at <= now()) \
             ORDER BY random() LIMIT 1"
        );
        Ok(self.column("RANDOMKEY", &sql, &[])?.into_iter().next())
    }

    fn flushdb(&mut self) -> Out<()> {
        self.client
            .update_kept(
                &crate::commands::sql::truncate_dbs(&[self.db as usize]),
                &[],
            )
            .map_err(|e| failed("FLUSHDB", e))?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Out<Option<Vec<u8>>> {
        let sql = &crate::commands::sql::GET[self.db as usize];
        Ok(self.column("GET", sql, &[key.into()])?.into_iter().next())
    }

    fn set(&mut self, key: &[u8], value: &[u8], a: &SetArgs) -> Out<SetDone> {
        let db = self.db;
        // The NX and XX statements below test `redis.kv_N` alone, which
        // cannot see a list or a set holding the key. Settle that here, where
        // the whole key directory is in reach: NX declines, and XX — whose
        // condition the collection satisfies — replaces it and writes
        // unconditionally.
        let (nx, xx) = if a.nx || a.xx {
            let held = Command::sql_key_kinds(self.client, db, &[key])
                .first()
                .and_then(|(_, k)| *k)
                .filter(|k| *k != crate::memdb::KeyKind::String);
            match held {
                Some(_) if a.nx => {
                    return Ok(SetDone {
                        written: false,
                        old: None,
                    });
                }
                Some(_) => {
                    Command::sql_drop_key(self.client, db, key);
                    (false, false)
                }
                None => (a.nx, a.xx),
            }
        } else {
            (false, false)
        };
        let new_expires = "CASE WHEN $3::bigint IS NULL THEN NULL \
             WHEN $5::bool THEN to_timestamp(0) + ($3::bigint * interval '1 millisecond') \
             ELSE now() + ($3::bigint * interval '1 millisecond') END";
        let conflict_expires = if a.keepttl {
            format!("redis.kv_{db}.expires_at")
        } else {
            "EXCLUDED.expires_at".into()
        };
        let write = if xx {
            // Only a key that exists and has not expired.
            format!(
                "wrote AS (UPDATE redis.kv_{db} SET value = $2, \
                     expires_at = CASE WHEN $4::bool THEN redis.kv_{db}.expires_at ELSE {new_expires} END \
                   WHERE key = $1 AND (expires_at IS NULL OR expires_at > now()) RETURNING 1)"
            )
        } else if nx {
            // One upsert that overwrites an expired row and leaves a live one
            // alone: a NOT EXISTS test ahead of a plain INSERT let two
            // concurrent writers both pass it.
            format!(
                "wrote AS (INSERT INTO redis.kv_{db} AS t (key, value, expires_at) VALUES ($1, $2, {new_expires}) \
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at \
                   WHERE t.expires_at IS NOT NULL AND t.expires_at <= now() RETURNING 1)"
            )
        } else {
            format!(
                "wrote AS (INSERT INTO redis.kv_{db} (key, value, expires_at) VALUES ($1, $2, {new_expires}) \
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = {conflict_expires} \
                   RETURNING 1)"
            )
        };
        let (old_cte, old_select) = if a.get {
            (
                format!(
                    "old AS (SELECT value FROM redis.kv_{db} WHERE key = $1 \
                             AND (expires_at IS NULL OR expires_at > now())), "
                ),
                "(SELECT value FROM old)",
            )
        } else {
            (String::new(), "NULL::bytea")
        };
        // An unconditional write replaces whatever held the key. The
        // conditional forms settled that above, and GET demands a string.
        let clear = if nx || xx || a.get {
            String::new()
        } else {
            crate::commands::sql::clear_for_string(db as usize)
        };
        let sql = format!(
            "WITH {clear}{old_cte}{write} SELECT {old_select}, EXISTS (SELECT 1 FROM wrote)"
        );
        let args: &[DatumWithOid] = &[
            key.into(),
            value.into(),
            a.ex_ms.into(),
            a.keepttl.into(),
            a.ex_abs.into(),
        ];
        let tbl = self
            .client
            .update_kept(&sql, args)
            .map_err(|e| failed("SET", e))?;
        let row = tbl.first();
        Ok(SetDone {
            written: row.get::<bool>(2).ok().flatten().unwrap_or(false),
            old: row.get::<Vec<u8>>(1).ok().flatten(),
        })
    }

    fn setex(&mut self, key: &[u8], value: &[u8], ttl_ms: i64) -> Out<()> {
        let db = self.db;
        let clear = crate::commands::sql::clear_for_string(db as usize);
        let sql = format!(
            "WITH {clear}w AS (INSERT INTO redis.kv_{db} (key, value, expires_at) \
                 VALUES ($1, $2, now() + ($3::bigint * interval '1 millisecond')) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at) \
             SELECT 1"
        );
        self.client
            .update_kept(&sql, &[key.into(), value.into(), ttl_ms.into()])
            .map_err(|e| failed("SETEX", e))?;
        Ok(())
    }

    fn setnx(&mut self, key: &[u8], value: &[u8]) -> Out<bool> {
        // "The key does not exist" is a question about every table: SETNX
        // over a list is 0, not a second value.
        if !Command::sql_key_kinds(self.client, self.db, &[key]).is_empty() {
            return Ok(false);
        }
        let sql = format!(
            "INSERT INTO redis.kv_{} (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
            self.db
        );
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), value.into()])
            .map_err(|e| failed("SETNX", e))?;
        Ok(!tbl.is_empty())
    }

    fn getset(&mut self, key: &[u8], value: &[u8]) -> Out<Option<Vec<u8>>> {
        let db = self.db;
        // The expiry goes with the value it replaces, as SET's does.
        let sql = format!(
            "INSERT INTO redis.kv_{db} (key, value, expires_at) VALUES ($1, $2, NULL) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL \
             RETURNING (SELECT value FROM redis.kv_{db} WHERE key = $1)"
        );
        Ok(self
            .write_column("GETSET", &sql, &[key.into(), value.into()])?
            .into_iter()
            .next())
    }

    fn getdel(&mut self, key: &[u8]) -> Out<Option<Vec<u8>>> {
        let sql = &crate::commands::sql::GETDEL[self.db as usize];
        Ok(self
            .write_column("GETDEL", sql, &[key.into()])?
            .into_iter()
            .next())
    }

    fn getex(&mut self, key: &[u8], expiry: &GetExExpiry) -> Out<Option<Vec<u8>>> {
        let Some(value) = self.get(key)? else {
            return Ok(None);
        };
        let db = self.db as usize;
        match expiry {
            GetExExpiry::Keep => {}
            GetExExpiry::Clear => {
                let _ = self
                    .client
                    .update_kept(&crate::commands::sql::PERSIST[db], &[key.into()]);
            }
            GetExExpiry::At(us) => {
                let sql = crate::commands::sql::set_expiry(
                    db,
                    "to_timestamp(0) + ($2::bigint * interval '1 microsecond')",
                    crate::commands::ExpireCond::Always,
                );
                let _ = self.client.update_kept(&sql, &[key.into(), (*us).into()]);
            }
        }
        Ok(Some(value))
    }

    fn mget(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>> {
        let sql = &crate::commands::sql::MGET[self.db as usize];
        let found: std::collections::HashMap<Vec<u8>, Vec<u8>> = self
            .rows("MGET", sql, &[arr(keys).into()])?
            .into_iter()
            .filter_map(|(k, v)| Some((k, v?)))
            .collect();
        // Each key on its own: `MGET k k` answers k's value twice.
        Ok(keys.iter().map(|k| found.get(k).cloned()).collect())
    }

    fn mset(&mut self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<()> {
        let db = self.db;
        let clear = ["hash", "list", "set", "zset", "stream", "expiry"]
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!("clr{i} AS (DELETE FROM redis.{t}_{db} WHERE key = ANY($1::bytea[]))")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH {clear}, w AS (INSERT INTO redis.kv_{db} (key, value, expires_at) \
                 SELECT unnest($1::bytea[]), unnest($2::bytea[]), NULL \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at) \
             SELECT 1"
        );
        let pairs = crate::commands::dedup_last(pairs, |(k, _)| k.as_slice());
        let keys: Vec<Option<Vec<u8>>> = pairs.iter().map(|(k, _)| Some(k.clone())).collect();
        let vals: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| Some(v.clone())).collect();
        self.client
            .update_kept(&sql, &[keys.into(), vals.into()])
            .map_err(|e| failed("MSET", e))?;
        Ok(())
    }

    fn msetnx(&mut self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<bool> {
        let named: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        if !Command::sql_key_kinds(self.client, self.db, &named).is_empty() {
            return Ok(false);
        }
        let sql = format!(
            "INSERT INTO redis.kv_{} (key, value, expires_at) SELECT unnest($1::bytea[]), unnest($2::bytea[]), NULL \
             ON CONFLICT (key) DO NOTHING",
            self.db
        );
        let keys: Vec<Option<Vec<u8>>> = pairs.iter().map(|(k, _)| Some(k.clone())).collect();
        let vals: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| Some(v.clone())).collect();
        self.client
            .update_kept(&sql, &[keys.into(), vals.into()])
            .map_err(|e| failed("MSETNX", e))?;
        Ok(true)
    }

    fn append(&mut self, key: &[u8], value: &[u8]) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = redis.kv_{db}.value || $2 \
             RETURNING length(redis.kv_{db}.value)::bigint"
        );
        self.write_int("APPEND", &sql, &[key.into(), value.into()])
    }

    fn strlen(&mut self, key: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::STRLEN[self.db as usize];
        self.int("STRLEN", sql, &[key.into()])
    }

    /// Redis pads a gap with NUL bytes. `chr(0)` is not a legal text
    /// character in PostgreSQL, so the gap is built as hex.
    fn setrange(&mut self, key: &[u8], offset: i64, value: &[u8]) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "WITH old AS (SELECT value FROM redis.kv_{db} WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())), \
                  padded AS (SELECT coalesce((SELECT value FROM old), '') \
                     || decode(repeat('00', GREATEST(0, $2::int - coalesce(length((SELECT value FROM old)), 0))), 'hex') AS v) \
             INSERT INTO redis.kv_{db} (key, value) \
             SELECT $1, substring(v from 1 for $2::int) || $3 || coalesce(substring(v from $2::int + length($3) + 1), '') \
             FROM padded \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value \
             RETURNING length(value)::bigint"
        );
        self.write_int(
            "SETRANGE",
            &sql,
            &[key.into(), (offset as i32).into(), value.into()],
        )
    }

    /// One statement; an overflow updates nothing rather than raising
    /// (`sql::counter_like`), and a refusal is told apart by the value.
    fn incrby(&mut self, key: &[u8], delta: i64) -> Out<Num<i64>> {
        let sql = &crate::commands::sql::INCRBY[self.db as usize];
        let tbl = self
            .client
            .update_kept(sql, &[key.into(), delta.into(), delta.into()])
            .map_err(|e| failed("INCRBY", e))?;
        if let Some(v) = tbl.first().get::<i64>(1).ok().flatten() {
            return Ok(Num::Value(v));
        }
        Ok(match self.get(key)? {
            Some(v) if is_integer(&v) => Num::Overflow,
            _ => Num::NotNumber,
        })
    }

    /// As `hincrbyfloat`: seeded if missing, locked, summed in Rust and
    /// written back, spelt as the memory half spells it.
    fn incrbyfloat(&mut self, key: &[u8], delta: LongDouble) -> Out<Num<String>> {
        let db = self.db;
        self.client
            .update_kept(
                &format!("INSERT INTO redis.kv_{db} (key, value) VALUES ($1, '\\x30') ON CONFLICT (key) DO NOTHING"),
                &[key.into()])
            .map_err(|e| failed("INCRBYFLOAT", e))?;
        let current = self
            .client
            .update_kept(
                &format!("SELECT value FROM redis.kv_{db} WHERE key = $1 FOR UPDATE"),
                &[key.into()],
            )
            .map_err(|e| failed("INCRBYFLOAT", e))?
            .first()
            .get::<Vec<u8>>(1)
            .ok()
            .flatten()
            .unwrap_or_default();
        let text = match crate::longdouble::increment(Some(&current), delta) {
            Num::Value(text) => text,
            refused => return Ok(refused),
        };
        self.client
            .update_kept(
                &format!("UPDATE redis.kv_{db} SET value = $2 WHERE key = $1"),
                &[key.into(), text.as_bytes().into()],
            )
            .map_err(|e| failed("INCRBYFLOAT", e))?;
        Ok(Num::Value(text))
    }

    /// Seeded if missing, then locked, edited and written back, so two at
    /// once cannot both edit the old value.
    fn setbit(&mut self, key: &[u8], offset: i64, on: bool) -> Out<(bool, bool)> {
        let db = self.db;
        self.client
            .update_kept(
                &format!("INSERT INTO redis.kv_{db} (key, value) VALUES ($1, '\\x') ON CONFLICT (key) DO NOTHING"),
                &[key.into()])
            .map_err(|e| failed("SETBIT", e))?;
        let mut value = self
            .client
            .update_kept(
                &format!("SELECT value FROM redis.kv_{db} WHERE key = $1 FOR UPDATE"),
                &[key.into()],
            )
            .map_err(|e| failed("SETBIT", e))?
            .first()
            .get::<Vec<u8>>(1)
            .ok()
            .flatten()
            .unwrap_or_default();
        let grows = value.len() <= (offset / 8) as usize;
        let was = crate::commands::set_bit_at(&mut value, offset, on);
        self.client
            .update_kept(
                &format!("UPDATE redis.kv_{db} SET value = $2 WHERE key = $1"),
                &[key.into(), value.into()],
            )
            .map_err(|e| failed("SETBIT", e))?;
        Ok((was, grows || was != on))
    }

    fn hget(&mut self, key: &[u8], field: &[u8]) -> Out<Option<Vec<u8>>> {
        let sql = &crate::commands::sql::HGET[self.db as usize];
        Ok(self
            .column("HGET", sql, &[key.into(), field.into()])?
            .into_iter()
            .next())
    }

    fn hmget(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>> {
        let sql = format!(
            "SELECT field, value FROM redis.hash_{} WHERE key = $1 AND field = ANY($2::bytea[])",
            self.db
        );
        let arg: Vec<Option<Vec<u8>>> = fields.iter().cloned().map(Some).collect();
        let found: std::collections::HashMap<Vec<u8>, Vec<u8>> = self
            .rows("HMGET", &sql, &[key.into(), arg.into()])?
            .into_iter()
            .filter_map(|(f, v)| Some((f, v?)))
            .collect();
        Ok(fields.iter().map(|f| found.get(f).cloned()).collect())
    }

    fn hgetall(&mut self, key: &[u8]) -> Out<Vec<(Vec<u8>, Vec<u8>)>> {
        let sql = &crate::commands::sql::HGETALL[self.db as usize];
        Ok(self
            .rows("HGETALL", sql, &[key.into()])?
            .into_iter()
            .filter_map(|(f, v)| Some((f, v?)))
            .collect())
    }

    /// A listpack comes back whole, in its order, cursor 0, as Redis scans
    /// one; a hash table pages by its primary key.
    fn hscan(
        &mut self,
        key: &[u8],
        cursor: u64,
        count: usize,
    ) -> Out<crate::cursor::Page<Vec<u8>>> {
        let db = self.db;
        let l = Limits::current();
        let whole = format!(
            "SELECT field, value FROM redis.hash_{db} WHERE key = $1 \
               AND NOT EXISTS (SELECT 1 FROM redis.encoding_{db} WHERE key = $1 AND encoding = 3) \
             ORDER BY seq LIMIT $2"
        );
        let rows: Vec<(Vec<u8>, Vec<u8>)> = self
            .rows("HSCAN", &whole, &[key.into(), cap(l.hash_entries).into()])?
            .into_iter()
            .filter_map(|(f, v)| Some((f, v?)))
            .collect();
        let packed = !rows.is_empty()
            && rows.len() <= l.hash_entries
            && rows
                .iter()
                .all(|(f, v)| f.len().max(v.len()) <= l.hash_value);
        if packed {
            return Ok((0, rows));
        }
        let scope = crate::cursor::scope(db, KeyKind::Hash as u8, key);
        crate::cursor::step(
            crate::cursor_state(),
            cursor,
            scope,
            count,
            token_random(),
            |range, limit| {
                let (from, upto) = range_args(range);
                let sql = collection_page_sql(db, "hash", "field, value", "field", range);
                let args = [key.into(), from.into(), limit.into(), upto.into()];
                Ok(self
                    .rows("HSCAN", &sql, &args)?
                    .into_iter()
                    .filter_map(|(f, v)| Some((f, v?)))
                    .collect())
            },
        )
    }

    fn hkeys(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>> {
        let sql = &crate::commands::sql::HKEYS[self.db as usize];
        self.column("HKEYS", sql, &[key.into()])
    }

    fn hvals(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>> {
        let sql = &crate::commands::sql::HVALS[self.db as usize];
        self.column("HVALS", sql, &[key.into()])
    }

    fn hlen(&mut self, key: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::HLEN[self.db as usize];
        self.int("HLEN", sql, &[key.into()])
    }

    fn hexists(&mut self, key: &[u8], field: &[u8]) -> Out<bool> {
        let sql = &crate::commands::sql::HEXISTS[self.db as usize];
        let tbl = self
            .client
            .select_kept(sql, &[key.into(), field.into()])
            .map_err(|e| failed("HEXISTS", e))?;
        Ok(!tbl.is_empty())
    }

    fn hstrlen(&mut self, key: &[u8], field: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::HSTRLEN[self.db as usize];
        self.int("HSTRLEN", sql, &[key.into(), field.into()])
    }

    /// A new field takes the next `seq`; an updated one keeps its own, which
    /// is where a listpack keeps it.
    fn hset(&mut self, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)], named: usize) -> Out<i64> {
        let db = self.db;
        let l = Limits::current();
        // `xmax = 0` marks a row this statement inserted rather than updated,
        // which is the count Redis returns.
        let sql = format!(
            "WITH ins AS ( \
                 INSERT INTO redis.hash_{db} (key, field, value) \
                 SELECT $1, f, v FROM unnest($2::bytea[], $3::bytea[]) WITH ORDINALITY AS t(f, v, i) \
                 ORDER BY i \
                 ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value \
                 RETURNING (xmax = 0) AS added \
             ), {} \
             SELECT count(*) FILTER (WHERE added)::bigint, {PRE_COLS} FROM ins",
            pre_cte(db, "hash", 1, 4, "EXISTS (SELECT 1 FROM ins WHERE added)")
        );
        let fields: Vec<Option<Vec<u8>>> = pairs.iter().map(|(f, _)| Some(f.clone())).collect();
        let vals: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| Some(v.clone())).collect();
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    key.into(),
                    fields.into(),
                    vals.into(),
                    cap(l.hash_entries).into(),
                ],
            )
            .map_err(|e| failed("HSET", e))?;
        let row = tbl.first();
        let added = row.get::<i64>(1).ok().flatten().unwrap_or(0);
        let pre = pre_at!(row, 2);
        let lens = pairs.iter().flat_map(|(f, v)| [f.len(), v.len()]);
        let mut enc = pre.enc(Enc::Listpack).unwrap_or(Enc::Listpack);
        enc = encoding::hash_presize(enc, named, lens, &l);
        if added > 0 {
            enc = encoding::hash_grown(enc, pre.len() + added as usize, &l);
        }
        self.sync_enc("HSET", key, &pre, KeyKind::Hash, enc, false)?;
        Ok(added)
    }

    fn hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Out<bool> {
        let db = self.db;
        let l = Limits::current();
        let sql = format!(
            "WITH ins AS (INSERT INTO redis.hash_{db} (key, field, value) VALUES ($1, $2, $3) \
                          ON CONFLICT (key, field) DO NOTHING RETURNING 1), \
                  {} \
             SELECT (SELECT count(*) FROM ins)::bigint, {PRE_COLS}",
            pre_cte(db, "hash", 1, 4, "EXISTS (SELECT 1 FROM ins)")
        );
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    key.into(),
                    field.into(),
                    value.into(),
                    cap(l.hash_entries).into(),
                ],
            )
            .map_err(|e| failed("HSETNX", e))?;
        let row = tbl.first();
        let added = row.get::<i64>(1).ok().flatten().unwrap_or(0) > 0;
        if added {
            let pre = pre_at!(row, 2);
            let mut enc = pre.enc(Enc::Listpack).unwrap_or(Enc::Listpack);
            enc = encoding::hash_presize(enc, 1, [field.len(), value.len()], &l);
            enc = encoding::hash_grown(enc, pre.len() + 1, &l);
            self.sync_enc("HSETNX", key, &pre, KeyKind::Hash, enc, false)?;
        }
        Ok(added)
    }

    /// The encoding row goes with the last field.
    fn hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "WITH d AS (DELETE FROM redis.hash_{db} WHERE key = $1 AND field = ANY($2::bytea[]) \
                        RETURNING 1), \
                  e AS (DELETE FROM redis.encoding_{db} WHERE key = $1 \
                          AND NOT EXISTS (SELECT 1 FROM redis.hash_{db} h \
                                          WHERE h.key = $1 AND h.field <> ALL($2::bytea[]))) \
             SELECT count(*)::bigint FROM d"
        );
        self.write_int("HDEL", &sql, &[key.into(), arr(fields).into()])
    }

    /// One statement. The sum is taken as `numeric` and must land in range,
    /// so an overflow updates nothing instead of raising; a refusal is then
    /// told apart by looking at the value.
    fn hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> Out<Num<i64>> {
        let db = self.db;
        let l = Limits::current();
        let sql = format!(
            "WITH up AS (INSERT INTO redis.hash_{db} (key, field, value) \
             VALUES ($1, $2, convert_to($3::text, 'UTF8')) \
             ON CONFLICT (key, field) DO UPDATE \
             SET value = convert_to((encode(redis.hash_{db}.value, 'escape')::numeric + $3)::text, 'UTF8') \
             WHERE encode(redis.hash_{db}.value, 'escape') ~ '^(0|-?[1-9][0-9]*)$' \
               AND encode(redis.hash_{db}.value, 'escape')::numeric \
                   BETWEEN -9223372036854775808 AND 9223372036854775807 \
               AND encode(redis.hash_{db}.value, 'escape')::numeric + $3 \
                   BETWEEN -9223372036854775808 AND 9223372036854775807 \
             RETURNING CAST(encode(value, 'escape') AS bigint) AS v, (xmax = 0) AS added), \
                  {} \
             SELECT (SELECT v FROM up), (SELECT added FROM up), {PRE_COLS}",
            pre_cte(db, "hash", 1, 4, "EXISTS (SELECT 1 FROM up WHERE added)")
        );
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    key.into(),
                    field.into(),
                    delta.into(),
                    cap(l.hash_entries).into(),
                ],
            )
            .map_err(|e| failed("HINCRBY", e))?;
        let row = tbl.first();
        if let Some(v) = row.get::<i64>(1).ok().flatten() {
            let added = row.get::<bool>(2).ok().flatten().unwrap_or(false);
            let pre = pre_at!(row, 3);
            let mut enc = pre.enc(Enc::Listpack).unwrap_or(Enc::Listpack);
            let text_len = v.to_string().len();
            enc = encoding::hash_presize(enc, 0, [field.len(), text_len], &l);
            if added {
                enc = encoding::hash_grown(enc, pre.len() + 1, &l);
            }
            self.sync_enc("HINCRBY", key, &pre, KeyKind::Hash, enc, false)?;
            return Ok(Num::Value(v));
        }
        Ok(match self.hget(key, field)? {
            Some(v) if is_integer(&v) => Num::Overflow,
            _ => Num::NotNumber,
        })
    }

    /// Atomic as a read, a sum and a write can be: the field is seeded if it
    /// is missing — so there is a row to lock even for a new field — then
    /// locked, read and written back. The sum is formatted the way Redis
    /// spells it, which SQL's float text is not.
    fn hincrbyfloat(&mut self, key: &[u8], field: &[u8], delta: LongDouble) -> Out<Num<String>> {
        let db = self.db;
        let l = Limits::current();
        let args: &[DatumWithOid] = &[key.into(), field.into()];
        let seeded = self
            .client
            .update_kept(
                &format!(
                    "WITH ins AS (INSERT INTO redis.hash_{db} (key, field, value) \
                                  VALUES ($1, $2, '\\x30') \
                                  ON CONFLICT (key, field) DO NOTHING RETURNING 1), \
                          {} \
                     SELECT (SELECT count(*) FROM ins)::bigint, {PRE_COLS}",
                    pre_cte(db, "hash", 1, 3, "EXISTS (SELECT 1 FROM ins)")
                ),
                &[key.into(), field.into(), cap(l.hash_entries).into()],
            )
            .map_err(|e| failed("HINCRBYFLOAT", e))?;
        let seeded = seeded.first();
        let added = seeded.get::<i64>(1).ok().flatten().unwrap_or(0) > 0;
        let pre = pre_at!(seeded, 2);
        let current = self
            .client
            .update_kept(
                &format!(
                    "SELECT value FROM redis.hash_{db} WHERE key = $1 AND field = $2 FOR UPDATE"
                ),
                args,
            )
            .map_err(|e| failed("HINCRBYFLOAT", e))?
            .first()
            .get::<Vec<u8>>(1)
            .ok()
            .flatten()
            .unwrap_or_default();
        let text = match crate::longdouble::increment(Some(&current), delta) {
            Num::Value(text) => text,
            refused => return Ok(refused),
        };
        self.client
            .update_kept(
                &format!("UPDATE redis.hash_{db} SET value = $3 WHERE key = $1 AND field = $2"),
                &[key.into(), field.into(), text.as_bytes().into()],
            )
            .map_err(|e| failed("HINCRBYFLOAT", e))?;
        let mut enc = pre.enc(Enc::Listpack).unwrap_or(Enc::Listpack);
        enc = encoding::hash_presize(enc, 0, [field.len(), text.len()], &l);
        if added {
            enc = encoding::hash_grown(enc, pre.len() + 1, &l);
        }
        self.sync_enc("HINCRBYFLOAT", key, &pre, KeyKind::Hash, enc, false)?;
        Ok(Num::Value(text))
    }

    fn zset_to_skiplist(&mut self, key: &[u8]) -> Out<()> {
        if self.kind(key)? == Some(KeyKind::Zset) {
            let row = enc_row(KeyKind::Zset, Enc::Skiplist, false);
            put_enc_row(self.client, self.db, key, row).map_err(|e| failed("SORT", e))?;
        }
        Ok(())
    }

    /// The row, or what the contents imply, read no further than a packed
    /// collection could reach: one past that is a hash table or skiplist
    /// whatever the rest holds.
    fn collection_encoding(&mut self, key: &[u8]) -> Out<Option<Enc>> {
        let db = self.db;
        let l = Limits::current();
        let Some(kind) = self.kind(key)? else {
            return Ok(None);
        };
        let (items_sql, limit, converted) = match kind {
            KeyKind::Hash => (
                format!(
                    "SELECT field, value FROM redis.hash_{db} WHERE key = $1 ORDER BY seq LIMIT $2"
                ),
                l.hash_entries,
                Enc::Hashtable,
            ),
            KeyKind::Set => (
                format!("SELECT member, NULL::bytea FROM redis.set_{db} WHERE key = $1 LIMIT $2"),
                l.set_intset.max(l.set_entries),
                Enc::Hashtable,
            ),
            KeyKind::Zset => (
                format!("SELECT member, NULL::bytea FROM redis.zset_{db} WHERE key = $1 LIMIT $2"),
                l.zset_entries,
                Enc::Skiplist,
            ),
            _ => return Ok(None),
        };
        let row = self
            .client
            .select_kept(
                &format!("SELECT encoding FROM redis.encoding_{db} WHERE key = $1"),
                &[key.into()],
            )
            .map_err(|e| failed("OBJECT", e))?
            .first()
            .get::<i16>(1)
            .ok()
            .flatten()
            .and_then(Enc::from_code);
        if let Some(e) = row {
            return Ok(Some(e));
        }
        let rows = self.rows("OBJECT", &items_sql, &[key.into(), cap(limit).into()])?;
        if rows.len() > limit {
            return Ok(Some(converted));
        }
        let items: Vec<&[u8]> = rows
            .iter()
            .flat_map(|(a, b)| std::iter::once(a.as_slice()).chain(b.as_deref()))
            .collect();
        Ok(Some(natural_enc(kind, None, &items)))
    }

    // ── sets ──

    /// The members it adds take the next `seq` in the order named. An intset
    /// that becomes a listpack is put in its sorted order first, which the
    /// listpack then keeps (`setTypeConvert`).
    fn sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let Some(first) = members.first() else {
            return Ok(0);
        };
        let db = self.db;
        let l = Limits::current();
        let sql = format!(
            "WITH {}, \
                  sample AS (SELECT member FROM redis.set_{db} WHERE key = $1 LIMIT 1), \
                  ins AS (INSERT INTO redis.set_{db} (key, member) \
                          SELECT $1, m FROM unnest($2::bytea[]) WITH ORDINALITY AS t(m, i) \
                          ORDER BY i ON CONFLICT DO NOTHING RETURNING member) \
             SELECT i.member, (SELECT member FROM sample), {PRE_COLS} FROM ins i",
            pre_cte(db, "set", 1, 3, "true")
        );
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    key.into(),
                    arr(members).into(),
                    cap(l.set_intset.max(l.set_entries)).into(),
                ],
            )
            .map_err(|e| failed("SADD", e))?;
        // A row per member added, each carrying what the key held before.
        let mut added = std::collections::HashSet::new();
        let mut found = None;
        for r in tbl {
            if let Ok(Some(m)) = r.get::<Vec<u8>>(1) {
                added.insert(m);
            }
            found = Some((r.get::<Vec<u8>>(2).ok().flatten(), pre_at!(r, 3)));
        }
        let Some((sample, pre)) = found else {
            return Ok(0);
        };
        // With no row, a set holding an integer is an intset.
        let sample_int = sample
            .as_deref()
            .is_some_and(|m| crate::memdb::parse_stored_int(m).is_some());
        let before = pre.enc(if sample_int {
            Enc::Intset
        } else {
            Enc::Listpack
        });
        let mut has_int = before.is_some() && (pre.row.is_some() || sample_int);
        let mut enc = match before {
            None => encoding::set_create(first, members.len(), &l),
            Some(e) => encoding::set_presize(e, members.len(), &l),
        };
        let widest = if l.set_value < 20 {
            self.int(
                "SADD",
                &format!(
                    "SELECT max(octet_length(member))::bigint FROM redis.set_{db} WHERE key = $1"
                ),
                &[key.into()],
            )? as usize
        } else {
            0
        };
        let mut len = pre.len();
        let mut seen = std::collections::HashSet::new();
        let mut after_conversion: Vec<Vec<u8>> = Vec::new();
        let mut converted = false;
        for m in members {
            if !added.contains(m) || !seen.insert(m.as_slice()) {
                continue;
            }
            let next = encoding::set_add(enc, len, m, || widest, &l);
            if enc == Enc::Intset && next != Enc::Intset {
                converted = true;
            }
            if converted {
                after_conversion.push(m.clone());
            }
            enc = next;
            len += 1;
            has_int |= crate::memdb::parse_stored_int(m).is_some();
        }
        if converted {
            // The integers held before the conversion take the order they
            // had as an intset, ahead of everything added since.
            let renumber = format!(
                "UPDATE redis.set_{db} s SET seq = o.rn - 9223372036854775807 \
                 FROM (SELECT member, row_number() OVER \
                         (ORDER BY CASE WHEN encode(member, 'escape') ~ '^(0|-?[1-9][0-9]*)$' THEN encode(member, 'escape')::numeric END, seq) AS rn \
                       FROM redis.set_{db} WHERE key = $1 AND member <> ALL($2::bytea[])) o \
                 WHERE s.key = $1 AND s.member = o.member"
            );
            self.client
                .update_kept(&renumber, &[key.into(), arr(&after_conversion).into()])
                .map_err(|e| failed("SADD", e))?;
        }
        self.sync_enc("SADD", key, &pre, KeyKind::Set, enc, has_int)?;
        Ok(added.len() as i64)
    }

    /// The encoding row goes with the last member.
    fn srem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "WITH d AS (DELETE FROM redis.set_{db} WHERE key = $1 AND member = ANY($2::bytea[]) \
                        RETURNING 1), \
                  e AS (DELETE FROM redis.encoding_{db} WHERE key = $1 \
                          AND NOT EXISTS (SELECT 1 FROM redis.set_{db} s \
                                          WHERE s.key = $1 AND s.member <> ALL($2::bytea[]))) \
             SELECT count(*)::bigint FROM d"
        );
        self.write_int("SREM", &sql, &[key.into(), arr(members).into()])
    }

    fn sismember(&mut self, key: &[u8], member: &[u8]) -> Out<bool> {
        let sql = &crate::commands::sql::SISMEMBER[self.db as usize];
        let tbl = self
            .client
            .select_kept(sql, &[key.into(), member.into()])
            .map_err(|e| failed("SISMEMBER", e))?;
        Ok(!tbl.is_empty())
    }

    fn smismember(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<bool>> {
        let sql = format!(
            "SELECT (s.member IS NOT NULL)::int \
             FROM unnest($2::bytea[]) WITH ORDINALITY AS m(member, ord) \
             LEFT JOIN redis.set_{} s ON s.key = $1 AND s.member = m.member \
             ORDER BY m.ord",
            self.db
        );
        let tbl = self
            .client
            .select_kept(&sql, &[key.into(), arr(members).into()])
            .map_err(|e| failed("SMISMEMBER", e))?;
        Ok(tbl
            .map(|r| r.get::<i32>(1).ok().flatten().unwrap_or(0) == 1)
            .collect())
    }

    fn smembers(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>> {
        let sql = &crate::commands::sql::SMEMBERS[self.db as usize];
        let tbl = self
            .client
            .select_kept(sql, &[key.into()])
            .map_err(|e| failed("SMEMBERS", e))?;
        let mut row = None;
        let mut members = Vec::new();
        for r in tbl {
            if let Ok(Some(m)) = r.get::<Vec<u8>>(1) {
                members.push(m);
            }
            row = r.get::<i16>(2).ok().flatten().and_then(Enc::from_code);
        }
        Ok(set_order(members, row).0)
    }

    /// An intset or a listpack comes back whole, in its order, cursor 0, as
    /// Redis scans one; a hash table pages by its primary key.
    fn sscan(&mut self, key: &[u8], cursor: u64, count: usize) -> Out<(u64, Vec<Vec<u8>>)> {
        let db = self.db;
        let l = Limits::current();
        let whole = format!(
            "SELECT member, (SELECT encoding FROM redis.encoding_{db} WHERE key = $1) \
             FROM redis.set_{db} WHERE key = $1 \
               AND NOT EXISTS (SELECT 1 FROM redis.encoding_{db} WHERE key = $1 AND encoding = 3) \
             ORDER BY seq LIMIT $2"
        );
        let limit = l.set_intset.max(l.set_entries);
        let tbl = self
            .client
            .select_kept(&whole, &[key.into(), cap(limit).into()])
            .map_err(|e| failed("SSCAN", e))?;
        let mut row = None;
        let mut members = Vec::new();
        for r in tbl {
            if let Ok(Some(m)) = r.get::<Vec<u8>>(1) {
                members.push(m);
            }
            row = r.get::<i16>(2).ok().flatten().and_then(Enc::from_code);
        }
        if !members.is_empty() && members.len() <= limit {
            let (members, enc) = set_order(members, row);
            if enc.is_packed() {
                return Ok((0, members));
            }
        }
        let scope = crate::cursor::scope(db, KeyKind::Set as u8, key);
        let (next, rows) = crate::cursor::step(
            crate::cursor_state(),
            cursor,
            scope,
            count,
            token_random(),
            |range, limit| {
                let (from, upto) = range_args(range);
                let sql = collection_page_sql(db, "set", "member", "member", range);
                let args = [key.into(), from.into(), limit.into(), upto.into()];
                Ok(self
                    .column("SSCAN", &sql, &args)?
                    .into_iter()
                    .map(|m| (m, ()))
                    .collect())
            },
        )?;
        Ok((next, rows.into_iter().map(|(m, ())| m).collect()))
    }

    fn scard(&mut self, key: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::SCARD[self.db as usize];
        self.int("SCARD", sql, &[key.into()])
    }

    /// `spopWithCountCommand`'s cases, as the memory half follows them
    /// (`memdb::db`): the whole set as `SUNION` gives it, a listpack's choice
    /// in its order, an intset's large share sorted, and a hash table popped
    /// down to a few members rebuilt as a listpack.
    fn spop(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>> {
        if n <= 0 {
            return Ok(Vec::new());
        }
        let db = self.db;
        let l = Limits::current();
        // The size decides two things only: whether the pop takes the whole
        // set, and Redis's choice for popping most of it (`remaining * 5 <=
        // n`). Neither needs more than 6n + 1 members counted, and counting
        // the rest cost a third of a pop from a large set.
        let sql = format!(
            "WITH pre AS (SELECT (SELECT encoding FROM redis.encoding_{db} WHERE key = $1) AS enc, \
                                 (SELECT count(*) FROM (SELECT 1 FROM redis.set_{db} \
                                                         WHERE key = $1 LIMIT $3) c) AS n), \
                  picked AS (SELECT ctid FROM redis.set_{db} WHERE key = $1 ORDER BY random() LIMIT $2), \
                  d AS (DELETE FROM redis.set_{db} t USING picked WHERE t.ctid = picked.ctid \
                        RETURNING t.member, t.seq) \
             SELECT d.member, pre.enc, pre.n FROM d, pre ORDER BY d.seq"
        );
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    key.into(),
                    n.into(),
                    n.saturating_mul(6).saturating_add(1).into(),
                ],
            )
            .map_err(|e| failed("SPOP", e))?;
        let mut popped = Vec::new();
        let (mut row, mut size) = (None, 0usize);
        for r in tbl {
            if let Ok(Some(m)) = r.get::<Vec<u8>>(1) {
                popped.push(m);
            }
            row = r.get::<i16>(2).ok().flatten().and_then(Enc::from_code);
            size = r.get::<i64>(3).ok().flatten().unwrap_or(0).max(0) as usize;
        }
        if popped.is_empty() {
            return Ok(popped);
        }
        let n = n as usize;
        if n >= size {
            put_enc_row(self.client, db, key, None).map_err(|e| failed("SPOP", e))?;
            let (whole, _) = set_order(popped, row);
            let view = SqlSet {
                index: std::collections::HashSet::new(),
                members: whole,
            };
            return Ok(encoding::union(&[Some(view)], l).finish().0);
        }
        let enc = row.unwrap_or_else(|| {
            if crate::memdb::parse_stored_int(&popped[0]).is_some() {
                Enc::Intset
            } else {
                Enc::Listpack
            }
        });
        let remaining = size - n;
        let most = remaining * 5 <= n;
        match enc {
            Enc::Intset if most => encoding::sort_ints(&mut popped, |m| m),
            Enc::Hashtable if most => {
                let left = self.column(
                    "SPOP",
                    &format!("SELECT member FROM redis.set_{db} WHERE key = $1 ORDER BY seq"),
                    &[key.into()],
                )?;
                let mut rebuilt = SetBuilder::new(Enc::Listpack, l);
                for m in &left {
                    rebuilt.add(m);
                }
                let has_int = left
                    .iter()
                    .any(|m| crate::memdb::parse_stored_int(m).is_some());
                let want = enc_row(KeyKind::Set, rebuilt.enc(), has_int);
                if want != row {
                    put_enc_row(self.client, db, key, want).map_err(|e| failed("SPOP", e))?;
                }
            }
            _ => {}
        }
        Ok(popped)
    }

    /// `srandmemberWithCountCommand`: a count covering the set returns all
    /// of it in its order, and a listpack gives its choice in its order.
    fn srandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>> {
        let db = self.db;
        // With a negative count Redis allows repeats: LATERAL re-evaluates
        // random() per outer row, so each pick is independent.
        if n < 0 {
            let sql = format!(
                "WITH cnt AS (SELECT count(*)::int AS c FROM redis.set_{db} WHERE key = $1) \
                 SELECT p.member FROM cnt, generate_series(1, $2::int) g, \
                   LATERAL (SELECT member FROM redis.set_{db} WHERE key = $1 \
                            OFFSET floor(random() * cnt.c)::int LIMIT 1) p \
                 WHERE cnt.c > 0"
            );
            let count = n.unsigned_abs().min(i64::MAX as u64).cast_signed();
            return self.column("SRANDMEMBER", &sql, &[key.into(), count.into()]);
        }
        let sql = format!(
            "SELECT member, (SELECT encoding FROM redis.encoding_{db} WHERE key = $1) \
             FROM (SELECT member, seq FROM redis.set_{db} WHERE key = $1 \
                   ORDER BY random() LIMIT $2) s ORDER BY seq"
        );
        let tbl = self
            .client
            .select_kept(&sql, &[key.into(), n.into()])
            .map_err(|e| failed("SRANDMEMBER", e))?;
        let mut row = None;
        let mut members = Vec::new();
        for r in tbl {
            if let Ok(Some(m)) = r.get::<Vec<u8>>(1) {
                members.push(m);
            }
            row = r.get::<i16>(2).ok().flatten().and_then(Enc::from_code);
        }
        Ok(set_order(members, row).0)
    }

    fn smove(&mut self, src: &[u8], dst: &[u8], member: &[u8]) -> Out<(bool, bool)> {
        let db = self.db;
        let l = Limits::current();
        let sql = format!(
            "WITH {}, \
                  sample AS (SELECT member FROM redis.set_{db} WHERE key = $2 LIMIT 1), \
                  d AS (DELETE FROM redis.set_{db} WHERE key = $1 AND member = $3 RETURNING member), \
                  ins AS (INSERT INTO redis.set_{db} (key, member) SELECT $2, d.member FROM d \
                          ON CONFLICT DO NOTHING RETURNING 1), \
                  e AS (DELETE FROM redis.encoding_{db} WHERE key = $1 AND EXISTS (SELECT 1 FROM d) \
                          AND NOT EXISTS (SELECT 1 FROM redis.set_{db} s \
                                          WHERE s.key = $1 AND s.member <> $3)) \
             SELECT (SELECT count(*)::bigint FROM d), (SELECT count(*)::bigint FROM ins), \
                    (SELECT member FROM sample), {PRE_COLS}",
            pre_cte(db, "set", 2, 4, "true")
        );
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    src.into(),
                    dst.into(),
                    member.into(),
                    cap(l.set_intset.max(l.set_entries)).into(),
                ],
            )
            .map_err(|e| failed("SMOVE", e))?;
        let row = tbl.first();
        let moved = row.get::<i64>(1).ok().flatten().unwrap_or(0) > 0;
        let added = row.get::<i64>(2).ok().flatten().unwrap_or(0) > 0;
        if added {
            let sample = row.get::<Vec<u8>>(3).ok().flatten();
            let pre = pre_at!(row, 4);
            let sample_int = sample
                .as_deref()
                .is_some_and(|m| crate::memdb::parse_stored_int(m).is_some());
            let before = pre.enc(if sample_int {
                Enc::Intset
            } else {
                Enc::Listpack
            });
            let start = before.unwrap_or_else(|| encoding::set_create(member, 1, &l));
            let enc = encoding::set_add(start, pre.len(), member, || 0, &l);
            let is_int = crate::memdb::parse_stored_int(member).is_some();
            let has_int = (before.is_some() && (pre.row.is_some() || sample_int)) || is_int;
            if start == Enc::Intset && enc != Enc::Intset {
                let renumber = format!(
                    "UPDATE redis.set_{db} s SET seq = o.rn - 9223372036854775807 \
                     FROM (SELECT member, row_number() OVER \
                             (ORDER BY CASE WHEN encode(member, 'escape') ~ '^(0|-?[1-9][0-9]*)$' THEN encode(member, 'escape')::numeric END, seq) AS rn \
                           FROM redis.set_{db} WHERE key = $1 AND member <> $2) o \
                     WHERE s.key = $1 AND s.member = o.member"
                );
                self.client
                    .update_kept(&renumber, &[dst.into(), member.into()])
                    .map_err(|e| failed("SMOVE", e))?;
            }
            self.sync_enc("SMOVE", dst, &pre, KeyKind::Set, enc, has_int)?;
        }
        Ok((moved, added))
    }

    fn sunion(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        let sets = self.sets_in_order("SUNION", keys)?;
        Ok(encoding::union(&sets, Limits::current()).finish().0)
    }

    fn sinter(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.inter_in_order("SINTER", keys)?.0)
    }

    fn sdiff(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        let sets = self.sets_in_order("SDIFF", keys)?;
        let same = keys.len() > 1 && keys[1..].contains(&keys[0]);
        Ok(encoding::diff(&sets, same, Limits::current()).finish().0)
    }

    fn sunionstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let sets = self.sets_in_order("SUNIONSTORE", keys)?;
        let (members, enc) = encoding::union(&sets, Limits::current()).finish();
        self.store_set("SUNIONSTORE", dst, &members, enc)
    }

    fn sinterstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let (members, walked) = self.inter_in_order("SINTERSTORE", keys)?;
        let built =
            encoding::inter_store(walked, members.iter().map(Vec::as_slice), Limits::current());
        let (members, enc) = built.finish();
        self.store_set("SINTERSTORE", dst, &members, enc)
    }

    fn sdiffstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let sets = self.sets_in_order("SDIFFSTORE", keys)?;
        let same = keys.len() > 1 && keys[1..].contains(&keys[0]);
        let (members, enc) = encoding::diff(&sets, same, Limits::current()).finish();
        self.store_set("SDIFFSTORE", dst, &members, enc)
    }
    fn sintercard(&mut self, keys: &[Vec<u8>], limit: Option<i64>) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "SELECT count(*)::bigint FROM ( \
                 SELECT member FROM redis.set_{db} WHERE key = $1 \
                  AND member IN (SELECT member FROM redis.set_{db} WHERE key = ANY($2::bytea[]) \
                                 GROUP BY member HAVING count(DISTINCT key) = $3::bigint) \
                  LIMIT $4::bigint) c"
        );
        self.int(
            "SINTERCARD",
            &sql,
            &[
                keys[0].as_slice().into(),
                arr(keys).into(),
                (keys.len() as i64).into(),
                limit.unwrap_or(i64::MAX).into(),
            ],
        )
    }

    fn zadd(&mut self, key: &[u8], pairs: &[(f64, Vec<u8>)], o: ZaddOpts) -> Out<(i64, i64)> {
        let db = self.db;
        let l = Limits::current();
        // Redis sizes the sorted set by the pairs named, repeats included,
        // and a new one by the first member's length.
        let named = pairs.len();
        let first = pairs.first().map_or(0, |(_, m)| m.len());
        let widest = pairs.iter().map(|(_, m)| m.len()).max().unwrap_or(0);
        // One statement can touch a row once, so a member named twice is
        // set once, to its last score.
        let pairs = crate::commands::dedup_last(pairs, |(_, m)| m.as_slice());
        let members: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, m)| Some(m.clone())).collect();
        let scores: Vec<Option<f64>> = pairs.iter().map(|(s, _)| Some(*s)).collect();
        let flags = crate::commands::ZAddFlags {
            nx: o.nx,
            xx: o.xx,
            gt: o.gt,
            lt: o.lt,
            ch: o.ch,
        };
        let sql = crate::commands::zadd_sql(db, &flags)
            .replacen(
                "WITH input AS",
                &format!("WITH {}, input AS", pre_cte(db, "zset", 1, 4, "true")),
                1,
            )
            .replacen("AS changed", &format!("AS changed, {PRE_COLS}"), 1);
        let tbl = self
            .client
            .update_kept(
                &sql,
                &[
                    key.into(),
                    members.into(),
                    scores.into(),
                    cap(l.zset_entries).into(),
                ],
            )
            .map_err(|e| failed("ZADD", e))?;
        let row = tbl.first();
        let added = row.get::<i64>(1).ok().flatten().unwrap_or(0);
        let changed = row.get::<i64>(2).ok().flatten().unwrap_or(0);
        let pre = pre_at!(row, 3);
        if pre.live || added > 0 {
            let mut enc = match pre.enc(Enc::Listpack) {
                Some(e) => encoding::zset_presize(e, named, &l),
                None => encoding::zset_create(named, first, &l),
            };
            // A new member longer than the limit converts it; so does the
            // count passing it. Which members were new is not asked for: a
            // listpack holds none longer than the limit already.
            if added > 0
                && enc == Enc::Listpack
                && (pre.len() + added as usize > l.zset_entries || widest > l.zset_value)
            {
                enc = Enc::Skiplist;
            }
            self.sync_enc("ZADD", key, &pre, KeyKind::Zset, enc, false)?;
        }
        Ok((added, added + changed))
    }

    /// A new member is inserted outright; an existing one is locked, and its
    /// new score worked out and checked against the conditions before it is
    /// written — so two at once cannot both read the old score, and a sum
    /// that is not a number is never stored.
    fn zadd_incr(&mut self, key: &[u8], member: &[u8], delta: f64, o: ZaddOpts) -> Out<Incr> {
        let db = self.db;
        if !o.xx {
            let l = Limits::current();
            let inserted = self
                .client
                .update_kept(
                    &format!(
                        "WITH {}, \
                              ins AS (INSERT INTO redis.zset_{db} (key, member, score) \
                                      VALUES ($1, $2, $3) \
                                      ON CONFLICT (key, member) DO NOTHING RETURNING score) \
                         SELECT (SELECT count(*) FROM ins)::bigint, {PRE_COLS}",
                        pre_cte(db, "zset", 1, 4, "true")
                    ),
                    &[
                        key.into(),
                        member.into(),
                        delta.into(),
                        cap(l.zset_entries).into(),
                    ],
                )
                .map_err(|e| failed("ZADD", e))?;
            let row = inserted.first();
            if row.get::<i64>(1).ok().flatten().unwrap_or(0) > 0 {
                let pre = pre_at!(row, 2);
                let start = match pre.enc(Enc::Listpack) {
                    Some(e) => e,
                    None => encoding::zset_create(1, member.len(), &l),
                };
                let enc = encoding::zset_add(start, pre.len(), member.len(), &l);
                self.sync_enc("ZADD", key, &pre, KeyKind::Zset, enc, false)?;
                return Ok(Incr::Score(delta));
            }
        }
        let old = self
            .client
            .update_kept(
                &format!(
                    "SELECT score FROM redis.zset_{db} WHERE key = $1 AND member = $2 FOR UPDATE"
                ),
                &[key.into(), member.into()],
            )
            .map_err(|e| failed("ZADD", e))?
            .first()
            .get::<f64>(1)
            .ok()
            .flatten();
        let Some(old) = old else {
            return Ok(Incr::Skipped);
        };
        if o.nx {
            return Ok(Incr::Skipped);
        }
        let new = old + delta;
        if new.is_nan() {
            return Ok(Incr::NotANumber);
        }
        if (o.gt && new <= old) || (o.lt && new >= old) {
            return Ok(Incr::Skipped);
        }
        self.client
            .update_kept(
                &format!("UPDATE redis.zset_{db} SET score = $3 WHERE key = $1 AND member = $2"),
                &[key.into(), member.into(), new.into()],
            )
            .map_err(|e| failed("ZADD", e))?;
        Ok(Incr::Score(new))
    }

    /// The encoding row goes with the last member.
    fn zrem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "WITH d AS (DELETE FROM redis.zset_{db} WHERE key = $1 AND member = ANY($2::bytea[]) \
                        RETURNING 1), \
                  e AS (DELETE FROM redis.encoding_{db} WHERE key = $1 \
                          AND NOT EXISTS (SELECT 1 FROM redis.zset_{db} z \
                                          WHERE z.key = $1 AND z.member <> ALL($2::bytea[]))) \
             SELECT count(*)::bigint FROM d"
        );
        self.write_int("ZREM", &sql, &[key.into(), arr(members).into()])
    }
    fn zscore(&mut self, key: &[u8], member: &[u8]) -> Out<Option<f64>> {
        let sql = &crate::commands::sql::ZSCORE[self.db as usize];
        let tbl = self
            .client
            .select_kept(sql, &[key.into(), member.into()])
            .map_err(|e| failed("ZSCORE", e))?;
        Ok(tbl.first().get::<f64>(1).ok().flatten())
    }

    fn zmscore(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<Option<f64>>> {
        let sql = format!(
            "SELECT z.score FROM unnest($2::bytea[]) WITH ORDINALITY AS m(member, ord) \
             LEFT JOIN redis.zset_{} z ON z.key = $1 AND z.member = m.member ORDER BY m.ord",
            self.db
        );
        let tbl = self
            .client
            .select_kept(&sql, &[key.into(), arr(members).into()])
            .map_err(|e| failed("ZMSCORE", e))?;
        Ok(tbl.map(|r| r.get::<f64>(1).ok().flatten()).collect())
    }

    fn zcard(&mut self, key: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::ZCARD[self.db as usize];
        self.int("ZCARD", sql, &[key.into()])
    }

    /// A listpack comes back whole, in score order, cursor 0, as Redis scans
    /// one; a skiplist pages by its primary key.
    fn zscan(&mut self, key: &[u8], cursor: u64, count: usize) -> Out<crate::cursor::Page<f64>> {
        let db = self.db;
        let l = Limits::current();
        let whole = format!(
            "SELECT member, score FROM redis.zset_{db} WHERE key = $1 \
               AND NOT EXISTS (SELECT 1 FROM redis.encoding_{db} WHERE key = $1 AND encoding = 4) \
             ORDER BY score, member LIMIT $2"
        );
        let rows = self.pairs("ZSCAN", &whole, &[key.into(), cap(l.zset_entries).into()])?;
        if !rows.is_empty()
            && rows.len() <= l.zset_entries
            && rows.iter().all(|(m, _)| m.len() <= l.zset_value)
        {
            return Ok((0, rows));
        }
        let scope = crate::cursor::scope(db, KeyKind::Zset as u8, key);
        crate::cursor::step(
            crate::cursor_state(),
            cursor,
            scope,
            count,
            token_random(),
            |range, limit| {
                let (from, upto) = range_args(range);
                let sql = collection_page_sql(db, "zset", "member, score", "member", range);
                let tbl = self
                    .client
                    .select_kept(&sql, &[key.into(), from.into(), limit.into(), upto.into()])
                    .map_err(|e| failed("ZSCAN", e))?;
                Ok(tbl
                    .filter_map(|r| {
                        Some((
                            r.get::<Vec<u8>>(1).ok()??,
                            r.get::<f64>(2).ok().flatten().unwrap_or(0.0),
                        ))
                    })
                    .collect())
            },
        )
    }

    fn zcount(&mut self, key: &[u8], min: ScoreBound, max: ScoreBound) -> Out<i64> {
        let sql = format!(
            "SELECT count(*)::bigint FROM redis.zset_{} WHERE key = $1 AND score {} $2 AND score {} $3",
            self.db,
            crate::commands::score_ge_op(&min),
            crate::commands::score_le_op(&max)
        );
        self.int(
            "ZCOUNT",
            &sql,
            &[key.into(), min.value.into(), max.value.into()],
        )
    }

    fn zlexcount(&mut self, key: &[u8], min: &LexBound, max: &LexBound) -> Out<i64> {
        let (clause, extra) = crate::commands::lex_where(min, max, 2);
        let sql = format!(
            "SELECT count(*)::bigint FROM redis.zset_{} WHERE key = $1{clause}",
            self.db
        );
        self.int("ZLEXCOUNT", &sql, &Self::lex_args(key, &extra))
    }

    fn zrank(&mut self, key: &[u8], member: &[u8], rev: bool) -> Out<Option<(i64, f64)>> {
        let db = self.db;
        let cond = if rev {
            "score > t.score OR (score = t.score AND member > $2)"
        } else {
            "score < t.score OR (score = t.score AND member < $2)"
        };
        let sql = format!(
            "WITH t AS (SELECT score FROM redis.zset_{db} WHERE key = $1 AND member = $2) \
             SELECT t.score, (SELECT count(*)::bigint FROM redis.zset_{db} WHERE key = $1 AND ({cond})) FROM t"
        );
        let tbl = self
            .client
            .select_kept(&sql, &[key.into(), member.into()])
            .map_err(|e| failed("ZRANK", e))?;
        let row = tbl.first();
        Ok(row
            .get::<f64>(1)
            .ok()
            .flatten()
            .map(|s| (row.get::<i64>(2).ok().flatten().unwrap_or(0), s)))
    }

    fn zrange_index(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Out<Vec<(Vec<u8>, f64)>> {
        let db = self.db;
        let order = if rev { "DESC" } else { "ASC" };
        let sql = format!(
            "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.zset_{db} WHERE key = $1), \
             nrm AS (SELECT \
                 CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                 LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
               FROM cnt) \
             SELECT member, score FROM redis.zset_{db}, nrm \
             WHERE key = $1 AND (SELECT n FROM cnt) > 0 AND (SELECT s FROM nrm) <= (SELECT e FROM nrm) \
             ORDER BY score {order}, member {order} \
             OFFSET (SELECT s FROM nrm) LIMIT (SELECT GREATEST(0, e - s + 1) FROM nrm)"
        );
        self.pairs("ZRANGE", &sql, &[key.into(), start.into(), stop.into()])
    }

    fn zrange_score(
        &mut self,
        key: &[u8],
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Out<Vec<(Vec<u8>, f64)>> {
        let order = if rev { "DESC" } else { "ASC" };
        let (offset, count) = limit.unwrap_or((0, -1));
        let sql = format!(
            "SELECT member, score FROM redis.zset_{} WHERE key = $1 AND score {} $2 AND score {} $3 \
             ORDER BY score {order}, member {order} OFFSET $4 LIMIT {}",
            self.db,
            crate::commands::score_ge_op(&min),
            crate::commands::score_le_op(&max),
            if count < 0 { "ALL" } else { "$5" }
        );
        let mut args: Vec<DatumWithOid> = vec![
            key.into(),
            min.value.into(),
            max.value.into(),
            offset.into(),
        ];
        if count >= 0 {
            args.push(count.into());
        }
        self.pairs("ZRANGEBYSCORE", &sql, &args)
    }

    fn zrange_lex(
        &mut self,
        key: &[u8],
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    ) -> Out<Vec<Vec<u8>>> {
        let (clause, extra) = crate::commands::lex_where(min, max, 2);
        let order = if rev { "DESC" } else { "ASC" };
        let (offset, count) = limit.unwrap_or((0, -1));
        let mut args = Self::lex_args(key, &extra);
        let offset_param = args.len() + 1;
        args.push(offset.into());
        let limit_sql = if count < 0 {
            "ALL".to_string()
        } else {
            args.push(count.into());
            format!("${}", args.len())
        };
        let sql = format!(
            "SELECT member FROM redis.zset_{} WHERE key = $1{clause} \
             ORDER BY member {order} OFFSET ${offset_param} LIMIT {limit_sql}",
            self.db
        );
        self.column("ZRANGEBYLEX", &sql, &args)
    }

    fn zpop(&mut self, key: &[u8], n: i64, min: bool) -> Out<Vec<(Vec<u8>, f64)>> {
        let db = self.db;
        let order = if min { "ASC" } else { "DESC" };
        let sql = format!(
            "WITH popped AS (SELECT key, member FROM redis.zset_{db} WHERE key = $1 \
                             ORDER BY score {order}, member {order} LIMIT $2 FOR UPDATE SKIP LOCKED) \
             DELETE FROM redis.zset_{db} z USING popped p WHERE z.key = p.key AND z.member = p.member \
             RETURNING z.member, z.score"
        );
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), n.into()])
            .map_err(|e| failed("ZPOP", e))?;
        let mut rows: Vec<(Vec<u8>, f64)> = tbl
            .map(|r| {
                (
                    r.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default(),
                    r.get::<f64>(2).ok().flatten().unwrap_or(0.0),
                )
            })
            .collect();
        // DELETE … RETURNING keeps no order: put it back.
        rows.sort_by(|a, b| {
            let o =
                a.1.partial_cmp(&b.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0));
            if min { o } else { o.reverse() }
        });
        Ok(rows)
    }

    fn zrandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<(Vec<u8>, f64)>> {
        let db = self.db;
        let sql = if n < 0 {
            format!(
                "WITH cnt AS (SELECT count(*)::int AS c FROM redis.zset_{db} WHERE key = $1) \
                 SELECT p.member, p.score FROM cnt, generate_series(1, $2::int) g, \
                   LATERAL (SELECT member, score FROM redis.zset_{db} WHERE key = $1 \
                            OFFSET floor(random() * cnt.c)::int LIMIT 1) p \
                 WHERE cnt.c > 0"
            )
        } else if n > 1 {
            // A count covering the sorted set returns all of it from the
            // highest score down, as Redis's iterator walks it; a listpack
            // gives its choice in its order; a skiplist's comes as drawn.
            format!(
                "WITH whole AS (SELECT (SELECT count(*) FROM redis.zset_{db} WHERE key = $1) <= $2 AS w), \
                      skip AS (SELECT EXISTS (SELECT 1 FROM redis.encoding_{db} \
                                              WHERE key = $1 AND encoding = 4) AS s) \
                 SELECT member, score FROM (SELECT member, score, random() AS r \
                                            FROM redis.zset_{db} WHERE key = $1 \
                                            ORDER BY r LIMIT $2) p, whole, skip \
                 ORDER BY CASE WHEN whole.w THEN NULL WHEN skip.s THEN r END, \
                          CASE WHEN whole.w THEN score END DESC, \
                          CASE WHEN whole.w THEN member END DESC, \
                          score, member"
            )
        } else {
            format!(
                "SELECT member, score FROM redis.zset_{db} WHERE key = $1 ORDER BY random() LIMIT $2"
            )
        };
        let count = n.unsigned_abs().min(i64::MAX as u64).cast_signed();
        self.pairs("ZRANDMEMBER", &sql, &[key.into(), count.into()])
    }

    fn zremrange_rank(&mut self, key: &[u8], start: i64, stop: i64) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.zset_{db} WHERE key = $1), \
             nrm AS (SELECT \
                 CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                 LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
               FROM cnt), \
             victims AS (SELECT key, member FROM redis.zset_{db}, nrm \
                 WHERE key = $1 AND (SELECT n FROM cnt) > 0 AND (SELECT s FROM nrm) <= (SELECT e FROM nrm) \
                 ORDER BY score ASC, member ASC \
                 OFFSET (SELECT s FROM nrm) LIMIT (SELECT GREATEST(0, e - s + 1) FROM nrm)), \
             d AS (DELETE FROM redis.zset_{db} z USING victims v WHERE z.key = v.key AND z.member = v.member RETURNING 1) \
             SELECT count(*)::bigint FROM d"
        );
        self.write_int(
            "ZREMRANGEBYRANK",
            &sql,
            &[key.into(), start.into(), stop.into()],
        )
    }

    fn zremrange_score(&mut self, key: &[u8], min: ScoreBound, max: ScoreBound) -> Out<i64> {
        let sql = format!(
            "WITH d AS (DELETE FROM redis.zset_{} WHERE key = $1 AND score {} $2 AND score {} $3 RETURNING 1) \
             SELECT count(*)::bigint FROM d",
            self.db,
            crate::commands::score_ge_op(&min),
            crate::commands::score_le_op(&max)
        );
        self.write_int(
            "ZREMRANGEBYSCORE",
            &sql,
            &[key.into(), min.value.into(), max.value.into()],
        )
    }

    fn zremrange_lex(&mut self, key: &[u8], min: &LexBound, max: &LexBound) -> Out<i64> {
        let (clause, extra) = crate::commands::lex_where(min, max, 2);
        let sql = format!(
            "WITH d AS (DELETE FROM redis.zset_{} WHERE key = $1{clause} RETURNING 1) SELECT count(*)::bigint FROM d",
            self.db
        );
        self.write_int("ZREMRANGEBYLEX", &sql, &Self::lex_args(key, &extra))
    }

    fn lpush(
        &mut self,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
        only_existing: bool,
    ) -> Out<i64> {
        self.list_lock(key);
        let db = self.db;
        let (edge, op) = if left { ("lo", "-") } else { ("hi", "+") };
        let guard = if only_existing {
            "WHERE base.n > 0"
        } else {
            ""
        };
        let ends = list_ends(db);
        let sql = format!(
            "WITH b AS ({ends}), \
                  base AS (SELECT COALESCE({edge}, 0) AS p, COALESCE(hi - lo + 1, 0)::bigint AS n FROM b), \
                  ins AS (INSERT INTO redis.list_{db} (key, pos, value) \
                          SELECT $1, base.p {op} u.ord, u.v \
                          FROM base, unnest($2::bytea[]) WITH ORDINALITY AS u(v, ord) {guard} \
                          RETURNING 1) \
             SELECT (SELECT n FROM base) + (SELECT count(*)::bigint FROM ins)"
        );
        let n = self.write_int("LPUSH", &sql, &[key.into(), arr(values).into()])?;
        // PUSHX onto nothing pushed nothing; the length it answers is 0.
        Ok(n)
    }

    fn lpop(&mut self, key: &[u8], n: i64, left: bool) -> Out<Vec<Vec<u8>>> {
        self.list_lock(key);
        let db = self.db;
        let order = if left { "ASC" } else { "DESC" };
        let sql = format!(
            "WITH d AS (SELECT key, pos FROM redis.list_{db} WHERE key = $1 \
                        ORDER BY pos {order} LIMIT $2) \
             DELETE FROM redis.list_{db} t USING d WHERE t.key = d.key AND t.pos = d.pos \
             RETURNING t.value, t.pos"
        );
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), n.into()])
            .map_err(|e| failed("POP", e))?;
        let mut rows: Vec<(i64, Vec<u8>)> = tbl
            .map(|r| {
                (
                    r.get::<i64>(2).ok().flatten().unwrap_or(0),
                    r.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default(),
                )
            })
            .collect();
        // DELETE … RETURNING keeps no order: put it back.
        rows.sort_by_key(|(p, _)| *p);
        if !left {
            rows.reverse();
        }
        Ok(rows.into_iter().map(|(_, v)| v).collect())
    }

    fn llen(&mut self, key: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::LLEN[self.db as usize];
        self.int("LLEN", sql, &[key.into()])
    }

    fn lrange(&mut self, key: &[u8], start: i64, stop: i64) -> Out<Vec<Vec<u8>>> {
        let db = self.db;
        let ends = list_ends(db);
        let sql = format!(
            "WITH b AS ({ends}), cnt AS (SELECT lo, COALESCE(hi - lo + 1, 0)::bigint AS n FROM b), \
             nrm AS (SELECT lo, n, \
                 CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                 LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
               FROM cnt) \
             SELECT l.value FROM redis.list_{db} l, nrm \
             WHERE l.key = $1 AND nrm.n > 0 AND nrm.s <= nrm.e \
               AND l.pos BETWEEN nrm.lo + nrm.s AND nrm.lo + nrm.e \
             ORDER BY l.pos ASC"
        );
        self.column("LRANGE", &sql, &[key.into(), start.into(), stop.into()])
    }

    fn lindex(&mut self, key: &[u8], index: i64) -> Out<Option<Vec<u8>>> {
        let db = self.db;
        let ends = list_ends(db);
        let sql = format!(
            "WITH b AS ({ends}), cnt AS (SELECT lo, COALESCE(hi - lo + 1, 0)::bigint AS n FROM b), \
             idx AS (SELECT lo, n, CASE WHEN $2::bigint < 0 THEN n + $2::bigint ELSE $2::bigint END AS i FROM cnt) \
             SELECT l.value FROM redis.list_{db} l, idx \
             WHERE l.key = $1 AND idx.i >= 0 AND idx.i < idx.n AND l.pos = idx.lo + idx.i"
        );
        Ok(self
            .column("LINDEX", &sql, &[key.into(), index.into()])?
            .into_iter()
            .next())
    }

    fn lset(&mut self, key: &[u8], index: i64, value: &[u8]) -> Out<LSet> {
        let db = self.db;
        let ends = list_ends(db);
        let sql = format!(
            "WITH b AS ({ends}), cnt AS (SELECT lo, COALESCE(hi - lo + 1, 0)::bigint AS n FROM b), \
             chk AS (SELECT lo, n, CASE WHEN $2::bigint < 0 THEN n + $2::bigint ELSE $2::bigint END AS idx FROM cnt), \
             upd AS (UPDATE redis.list_{db} SET value = $3 \
                     WHERE key = $1 AND pos = (SELECT lo + idx FROM chk) \
                       AND (SELECT idx FROM chk) >= 0 AND (SELECT idx FROM chk) < (SELECT n FROM chk) \
                     RETURNING 1) \
             SELECT n, EXISTS(SELECT 1 FROM upd) FROM chk"
        );
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), index.into(), value.into()])
            .map_err(|e| failed("LSET", e))?;
        let row = tbl.first();
        let n = row.get::<i64>(1).ok().flatten().unwrap_or(0);
        let updated = row.get::<bool>(2).ok().flatten().unwrap_or(false);
        Ok(match (n, updated) {
            (0, _) => LSet::NoKey,
            (_, false) => LSet::OutOfRange,
            _ => LSet::Done,
        })
    }

    /// The element goes in at the pivot's position (before) or the one after
    /// it, and everything from there on moves up one, so positions stay
    /// contiguous (`list_ends`).
    fn linsert(&mut self, key: &[u8], before: bool, pivot: &[u8], value: &[u8]) -> Out<i64> {
        self.list_lock(key);
        let db = self.db;
        let pivot_pos = self
            .client
            .select_kept(
                &format!("SELECT pos FROM redis.list_{db} WHERE key = $1 AND value = $2 ORDER BY pos ASC LIMIT 1"),
                &[key.into(), pivot.into()],
            )
            .map_err(|e| failed("LINSERT", e))?
            .first()
            .get::<i64>(1)
            .ok()
            .flatten();
        let Some(pivot_pos) = pivot_pos else {
            return Ok(if self.llen(key)? == 0 { 0 } else { -1 });
        };
        let at = if before { pivot_pos } else { pivot_pos + 1 };
        self.list_shift(key, at, 1, "LINSERT")?;
        self.client
            .update_kept(
                &format!("INSERT INTO redis.list_{db} (key, pos, value) VALUES ($1, $2, $3)"),
                &[key.into(), at.into(), value.into()],
            )
            .map_err(|e| failed("LINSERT", e))?;
        self.llen(key)
    }

    fn lrem(&mut self, key: &[u8], count: i64, value: &[u8]) -> Out<i64> {
        self.list_lock(key);
        let db = self.db;
        let limit = if count == 0 {
            String::new()
        } else {
            format!("LIMIT {}", count.unsigned_abs())
        };
        let order = if count < 0 { "DESC" } else { "ASC" };
        let sql = format!(
            "WITH d AS (SELECT pos FROM redis.list_{db} WHERE key = $1 AND value = $2 ORDER BY pos {order} {limit}), \
                  del AS (DELETE FROM redis.list_{db} t USING d WHERE t.key = $1 AND t.pos = d.pos RETURNING 1) \
             SELECT count(*)::bigint FROM del"
        );
        let removed = self.write_int("LREM", &sql, &[key.into(), value.into()])?;
        if removed > 0 {
            self.list_compact(key, "LREM")?;
        }
        Ok(removed)
    }

    fn ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> Out<()> {
        self.list_lock(key);
        let db = self.db;
        let ends = list_ends(db);
        let sql = format!(
            "WITH b AS ({ends}), cnt AS (SELECT lo, COALESCE(hi - lo + 1, 0)::bigint AS n FROM b), \
             nrm AS (SELECT lo, \
                 CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                 LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
               FROM cnt) \
             DELETE FROM redis.list_{db} USING nrm b \
             WHERE key = $1 AND (b.s > b.e OR pos < b.lo + b.s OR pos > b.lo + b.e)"
        );
        self.client
            .update_kept(&sql, &[key.into(), start.into(), stop.into()])
            .map_err(|e| failed("LTRIM", e))?;
        Ok(())
    }

    fn lmove(
        &mut self,
        src: &[u8],
        dst: &[u8],
        src_left: bool,
        dst_left: bool,
    ) -> Out<Option<Vec<u8>>> {
        // Both keys, in one order whichever way the move goes, so two moves
        // in opposite directions cannot each hold the lock the other wants.
        let (first, second) = if src <= dst { (src, dst) } else { (dst, src) };
        self.list_lock(first);
        if second != first {
            self.list_lock(second);
        }
        let db = self.db;
        let pop_order = if src_left { "ASC" } else { "DESC" };
        let (edge, op) = if dst_left { ("MIN", "-") } else { ("MAX", "+") };
        let sql = format!(
            "WITH popped AS (DELETE FROM redis.list_{db} t \
                 USING (SELECT key, pos FROM redis.list_{db} WHERE key = $1 \
                        ORDER BY pos {pop_order} LIMIT 1) p \
                 WHERE t.key = p.key AND t.pos = p.pos RETURNING t.value), \
             edge AS (SELECT COALESCE({edge}(pos), 0) AS p FROM redis.list_{db} WHERE key = $2), \
             ins AS (INSERT INTO redis.list_{db} (key, pos, value) \
                     SELECT $2, edge.p {op} 1, popped.value FROM popped, edge RETURNING value) \
             SELECT value FROM ins"
        );
        Ok(self
            .write_column("LMOVE", &sql, &[src.into(), dst.into()])?
            .into_iter()
            .next())
    }

    fn lpos(
        &mut self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<i64>,
        maxlen: i64,
    ) -> Out<Vec<i64>> {
        let db = self.db;
        let order = if rank < 0 { "DESC" } else { "ASC" };
        let skip = rank.unsigned_abs() as i64 - 1;
        let limit = match count {
            None => "LIMIT 1".to_string(),
            Some(0) => String::new(),
            Some(n) => format!("LIMIT {n}"),
        };
        // `scan` numbers the elements in search order, so MAXLEN bounds how
        // far the search reaches from whichever end it starts at.
        let sql = format!(
            "WITH ordered AS (SELECT value, ROW_NUMBER() OVER (ORDER BY pos ASC) - 1 AS idx, \
                                     ROW_NUMBER() OVER (ORDER BY pos {order}) AS scan \
                              FROM redis.list_{db} WHERE key = $1) \
             SELECT idx FROM ordered WHERE value = $2 AND ($4::bigint = 0 OR scan <= $4::bigint) \
             ORDER BY idx {order} OFFSET $3 {limit}"
        );
        let tbl = self
            .client
            .select_kept(
                &sql,
                &[key.into(), element.into(), skip.into(), maxlen.into()],
            )
            .map_err(|e| failed("LPOS", e))?;
        Ok(tbl.filter_map(|r| r.get::<i64>(1).ok().flatten()).collect())
    }
}

/// A list's first and last position, as `lo` and `hi`, each one index probe.
///
/// A list's positions are contiguous: pushes and pops work at the ends,
/// `LINSERT` shifts what follows the new element and `LREM` closes the gaps it
/// leaves. So a list's length is `hi - lo + 1` and its element `i` is at
/// `lo + i`: the length costs two probes rather than a count of every element,
/// which made each push O(n) and building a list O(n²), and a range is read
/// straight off the index rather than skipped through with OFFSET. Every
/// writer that moves positions holds the list's lock (`list_lock`), so no two
/// can leave a gap between them.
fn list_ends(db: u8) -> String {
    format!(
        "SELECT (SELECT pos FROM redis.list_{db} WHERE key = $1 ORDER BY pos ASC LIMIT 1) AS lo, \
                (SELECT pos FROM redis.list_{db} WHERE key = $1 ORDER BY pos DESC LIMIT 1) AS hi"
    )
}

/// Far from any position a list reaches, so rows parked there for a renumber
/// never meet one that is not moving: `(key, pos)` is unique, checked row by
/// row, and a renumber in place would collide with itself half way.
const PARK: i64 = 1 << 62;

/// Where parked rows start. Positions go negative as `LPUSH` grows a list
/// down, so a parked row can sit just under `PARK`: anything past half of it
/// is parked, since no list's positions come near ±2^61.
const PARKED: i64 = PARK / 2;

impl SqlStore<'_, '_> {
    /// Move every element of `key` at or after `from` up by `by`, in two
    /// passes through `PARK`.
    fn list_shift(&mut self, key: &[u8], from: i64, by: i64, cmd: &str) -> Out<()> {
        let db = self.db;
        self.client
            .update_kept(
                &format!("UPDATE redis.list_{db} SET pos = pos + {PARK} + $3 WHERE key = $1 AND pos >= $2"),
                &[key.into(), from.into(), by.into()],
            )
            .map_err(|e| failed(cmd, e))?;
        self.client
            .update_kept(
                &format!(
                    "UPDATE redis.list_{db} SET pos = pos - {PARK} WHERE key = $1 AND pos >= {PARKED}"
                ),
                &[key.into()],
            )
            .map_err(|e| failed(cmd, e))?;
        Ok(())
    }

    /// Close the gaps in `key`'s positions, keeping its first one.
    fn list_compact(&mut self, key: &[u8], cmd: &str) -> Out<()> {
        let db = self.db;
        self.client
            .update_kept(
                &format!(
                    "UPDATE redis.list_{db} t SET pos = r.lo + r.i + {PARK} FROM ( \
                         SELECT ctid, MIN(pos) OVER () AS lo, ROW_NUMBER() OVER (ORDER BY pos) - 1 AS i \
                         FROM redis.list_{db} WHERE key = $1) r \
                     WHERE t.ctid = r.ctid"
                ),
                &[key.into()],
            )
            .map_err(|e| failed(cmd, e))?;
        self.client
            .update_kept(
                &format!(
                    "UPDATE redis.list_{db} SET pos = pos - {PARK} WHERE key = $1 AND pos >= {PARKED}"
                ),
                &[key.into()],
            )
            .map_err(|e| failed(cmd, e))?;
        Ok(())
    }

    /// Writers on one list key take turns, or two pushes compute the same
    /// edge position. Namespace 42 is pg_redis's lists; 43 its snapshots.
    fn list_lock(&mut self, key: &[u8]) {
        let _ = self.client.update_kept(
            "SELECT pg_advisory_xact_lock(42, hashtext(encode($1, 'escape')))",
            &[key.into()],
        );
    }
}

impl SqlStore<'_, '_> {
    fn pairs(&mut self, cmd: &str, sql: &str, args: &[DatumWithOid]) -> Out<Vec<(Vec<u8>, f64)>> {
        let tbl = self
            .client
            .select_kept(sql, args)
            .map_err(|e| failed(cmd, e))?;
        Ok(tbl
            .map(|r| {
                (
                    r.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default(),
                    r.get::<f64>(2).ok().flatten().unwrap_or(0.0),
                )
            })
            .collect())
    }

    /// The key and the bound values as parameters, after `lex_where`'s.
    fn lex_args(key: &[u8], extra: &[Vec<u8>]) -> Vec<DatumWithOid<'static>> {
        let mut args: Vec<DatumWithOid> = vec![key.to_vec().into()];
        args.extend(extra.iter().map(|a| DatumWithOid::from(a.clone())));
        args
    }
}

/// Byte strings as a `bytea[]` parameter.
fn arr(items: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
    items.iter().cloned().map(Some).collect()
}

// ───────────────────────────── keyset cursors ───────────────────────────────

/// Random bits for a cursor token. Each worker's generator is seeded from
/// the clock and may have been seeded before the fork, so the pid is mixed
/// in for workers drawing at once.
fn token_random() -> u64 {
    crate::memdb::fast_random() ^ u64::from(std::process::id()).rotate_left(32)
}

/// The predicate a page's range puts on `col`, the bound being parameter
/// `$from` and the upper bound `$upto`. Each range is its own statement text,
/// so each keeps a plan that can use the index.
fn range_sql(range: &crate::cursor::Range, col: &str, from: usize, upto: usize) -> String {
    use crate::cursor::Range;
    match range {
        Range::All => "TRUE".to_string(),
        Range::After(_) => format!("{col} > ${from}"),
        Range::From(_) => format!("{col} >= ${from}"),
        Range::Between(..) => format!("{col} > ${from} AND {col} < ${upto}"),
    }
}

/// The two bounds as parameters, empty where the range has none: a
/// statement may leave a parameter unused, so every range takes the same
/// list.
fn range_args(range: &crate::cursor::Range) -> (Vec<u8>, Vec<u8>) {
    use crate::cursor::Range;
    match range {
        Range::All => (Vec::new(), Vec::new()),
        Range::After(b) | Range::From(b) => (b.clone(), Vec::new()),
        Range::Between(a, b) => (a.clone(), b.clone()),
    }
}

/// One `SCAN` page of database `db`: the next `$2` keys in `range` (all of
/// them for a NULL `$2`) across every table a key can be in, each with
/// whether it is live. Parameters: `$1` the bound, `$3` the upper bound.
fn scan_page_sql(db: u8, range: &crate::cursor::Range) -> String {
    let pred = range_sql(range, "key", 1, 3);
    let from = |tbl: &str, distinct: &str| {
        format!("(SELECT {distinct}key FROM redis.{tbl}_{db} WHERE {pred} ORDER BY key LIMIT $2)")
    };
    // A collection keeps a row per element, so its keys are DISTINCT; the
    // index on (key, ...) still delivers them in order.
    let tables = [
        from("kv", ""),
        from("hash", "DISTINCT "),
        from("list", "DISTINCT "),
        from("set", "DISTINCT "),
        from("zset", "DISTINCT "),
        from("stream", ""),
    ]
    .join(" UNION ");
    format!(
        "WITH page AS (SELECT key FROM ({tables}) u ORDER BY key LIMIT $2) \
         SELECT p.key, \
                NOT EXISTS (SELECT 1 FROM redis.kv_{db} t \
                             WHERE t.key = p.key AND t.expires_at <= now()) \
            AND NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e \
                             WHERE e.key = p.key AND e.expires_at <= now()) \
           FROM page p ORDER BY p.key"
    )
}

/// One `HSCAN`/`SSCAN`/`ZSCAN` page: `cols` of the collection `$1` in table
/// `tbl`, ordered by `col`, the next `$3` of them in `range` (all for a NULL
/// `$3`), nothing once the key has expired. `$2` is the bound and `$4` the
/// upper bound.
fn collection_page_sql(
    db: u8,
    tbl: &str,
    cols: &str,
    col: &str,
    range: &crate::cursor::Range,
) -> String {
    format!(
        "SELECT {cols} FROM redis.{tbl}_{db} WHERE key = $1 AND {pred} AND {live} \
         ORDER BY {col} LIMIT $3",
        pred = range_sql(range, col, 2, 4),
        live = crate::commands::sql::live(db as usize),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_are_what_string2ll_takes() {
        for ok in [
            "0",
            "-1",
            "42",
            "9223372036854775807",
            "-9223372036854775808",
        ] {
            assert!(is_integer(ok.as_bytes()), "{ok}");
        }
        for bad in [
            "",
            "+1",
            "01",
            "-0",
            "1.0",
            "9223372036854775808",
            " 1",
            "abc",
        ] {
            assert!(!is_integer(bad.as_bytes()), "{bad}");
        }
    }
}
