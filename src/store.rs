//! Commands written once (docs/STORAGE.md, phase 5).
//!
//! A command's meaning — how its arguments are read, its edge cases, the reply
//! it builds — is written here, once, against [`Store`]. The two backends
//! implement `Store` underneath: `memdb::Db` over a database in shared memory,
//! [`SqlStore`] over a database's tables. Each operation is atomic in its
//! backend — one statement, or one critical section under the database lock —
//! so the interface is drawn at the level of what a command does to a key
//! (`hincrby`), not of reads and writes a command would have to compose
//! (`hget` then `hset`), which another worker could interleave.
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
use crate::memdb::KeyKind;
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
    /// Give a live key the deadline when `cond` allows; whether it did.
    fn expire(&mut self, key: &[u8], deadline_us: i64, cond: ExpireCond) -> Out<bool>;
    fn persist(&mut self, key: &[u8]) -> Out<bool>;
    /// Move `key`, its value and its expiry, to `newkey`, replacing whatever
    /// was there. False when `key` is not there.
    fn rename(&mut self, key: &[u8], newkey: &[u8]) -> Out<bool>;
    /// Every live key matching the glob, or every one without it.
    fn keys(&mut self, pattern: Option<&[u8]>) -> Out<Vec<Vec<u8>>>;
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
    fn incrbyfloat(&mut self, key: &[u8], delta: f64) -> Out<Num<String>>;
    /// Set or clear one bit; what it was.
    fn setbit(&mut self, key: &[u8], offset: i64, on: bool) -> Out<bool>;

    // ── hashes ──
    fn hget(&mut self, key: &[u8], field: &[u8]) -> Out<Option<Vec<u8>>>;
    fn hmget(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<Vec<Option<Vec<u8>>>>;
    fn hgetall(&mut self, key: &[u8]) -> Out<Vec<(Vec<u8>, Vec<u8>)>>;
    fn hkeys(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>>;
    fn hvals(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>>;
    fn hlen(&mut self, key: &[u8]) -> Out<i64>;
    fn hexists(&mut self, key: &[u8], field: &[u8]) -> Out<bool>;
    fn hstrlen(&mut self, key: &[u8], field: &[u8]) -> Out<i64> {
        Ok(self.hget(key, field)?.map_or(0, |v| v.len() as i64))
    }
    /// Set every pair — each field once — and count the fields that are new.
    fn hset(&mut self, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<i64>;
    fn hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Out<bool>;
    fn hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<i64>;
    fn hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> Out<Num<i64>>;
    /// The new value as Redis spells it.
    fn hincrbyfloat(&mut self, key: &[u8], field: &[u8], delta: f64) -> Out<Num<String>>;

    // ── sets ──
    fn sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64>;
    fn srem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64>;
    fn sismember(&mut self, key: &[u8], member: &[u8]) -> Out<bool>;
    fn smismember(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<bool>>;
    fn smembers(&mut self, key: &[u8]) -> Out<Vec<Vec<u8>>>;
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
    fn zadd(&mut self, key: &[u8], pairs: &[(f64, Vec<u8>)], opts: ZaddOpts) -> Out<i64>;
    fn zadd_incr(&mut self, key: &[u8], member: &[u8], delta: f64, opts: ZaddOpts) -> Out<Incr>;
    fn zrem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64>;
    fn zscore(&mut self, key: &[u8], member: &[u8]) -> Out<Option<f64>>;
    fn zmscore(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<Vec<Option<f64>>>;
    fn zcard(&mut self, key: &[u8]) -> Out<i64>;
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
        // TOUCH answers as EXISTS does; the access time it refreshes in Redis
        // feeds an LRU clock this server does not keep.
        Command::Exists { keys } | Command::Touch { keys } => {
            store.exists(keys).map(Response::Integer)
        }
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
        Command::Scan { pattern, kind, .. } => scan(store, pattern.as_deref(), *kind),
        Command::DbSize => store.dbsize().map(Response::Integer),
        Command::Keyspace => store
            .keyspace()
            .map(|(k, e)| Response::IntegerArray(vec![k, e])),
        Command::RandomKey => store.random_key().map(bulk_or_nil),
        Command::FlushDb => store.flushdb().map(|()| Response::Ok),
        Command::ObjectEncoding { key } => object_encoding(store, key),
        Command::ObjectOther => Ok(Response::Null),

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
        Command::IncrByFloat { key, delta } => store.incrbyfloat(key, *delta).map(|n| match n {
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
        Command::SetBit { key, offset, value } => store
            .setbit(key, *offset, *value)
            .map(|b| Response::Integer(b as i64)),

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
        // A field named twice is set once, to its last value, and counted
        // once if it is new.
        Command::HSet { key, pairs } => store
            .hset(
                key,
                &crate::commands::dedup_last(pairs, |(f, _)| f.as_slice()),
            )
            .map(Response::Integer),
        Command::HMSet { key, pairs } => store
            .hset(
                key,
                &crate::commands::dedup_last(pairs, |(f, _)| f.as_slice()),
            )
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
        } => store
            .hgetall(key)
            .map(|pairs| crate::commands::random_fields(&pairs, *count, *withvalues)),

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
            Some(n) => store.srandmember(key, *n).map(array),
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
                    Incr::Score(s) => score(s),
                    Incr::Skipped => Response::Null,
                    Incr::NotANumber => Response::Error(crate::commands::NAN_SCORE.into()),
                })
            } else {
                store.zadd(key, pairs, opts).map(Response::Integer)
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
            Some(n) => store.zrandmember(key, *n).map(|v| scored(v, *with_scores)),
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
        Command::LTrim { key, start, stop } => {
            store.ltrim(key, *start, *stop).map(|()| Response::Ok)
        }
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
    Some(reply.unwrap_or_else(|e| e))
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

/// `SCAN`: one pass, cursor 0. A TYPE pg_redis has no table for matches
/// nothing, as in Redis 7.2.
fn scan(
    store: &mut dyn Store,
    pattern: Option<&[u8]>,
    kind: Option<Option<KeyKind>>,
) -> Out<Response> {
    let mut keys = store.keys(pattern)?;
    if let Some(want) = kind {
        let kinds = store.kinds(&keys)?;
        keys = keys
            .into_iter()
            .zip(kinds)
            .filter(|(_, k)| want.is_some() && *k == want)
            .map(|(key, _)| key)
            .collect();
    }
    Ok(Response::ScanResult {
        keys: keys.into_iter().map(Some).collect(),
    })
}

/// The encoding Redis would use, from the key's shape against Redis's default
/// thresholds — this server stores none of them. A missing key is nil, not an
/// error, as in Redis.
fn object_encoding(store: &mut dyn Store, key: &[u8]) -> Out<Response> {
    use crate::commands::collection_encoding;
    let Some(kind) = store.kind(key)? else {
        return Ok(Response::Null);
    };
    let name = match kind {
        KeyKind::Stream => "stream",
        KeyKind::String => crate::commands::string_encoding(&store.get(key)?.unwrap_or_default()),
        KeyKind::List => collection_encoding(kind, &store.lrange(key, 0, -1)?),
        KeyKind::Set => collection_encoding(kind, &store.smembers(key)?),
        // Fields and values both, which is what Redis measures.
        KeyKind::Hash => collection_encoding(
            kind,
            &store
                .hgetall(key)?
                .into_iter()
                .flat_map(|(f, v)| [f, v])
                .collect::<Vec<_>>(),
        ),
        KeyKind::Zset => collection_encoding(
            kind,
            &store
                .zrange_index(key, 0, -1, false)?
                .into_iter()
                .map(|(m, _)| m)
                .collect::<Vec<_>>(),
        ),
    };
    // A bulk string, as Redis's `addReplyBulkCString`: `$8 listpack`, not
    // `+listpack`.
    Ok(Response::BulkString(name.as_bytes().to_vec()))
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
        let sql = format!("WITH {deletes} SELECT count(DISTINCT key)::bigint FROM ({union}) u");
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

    fn dbsize(&mut self) -> Out<i64> {
        let db = self.db;
        let sql = format!(
            "SELECT count(DISTINCT key)::bigint FROM ( \
                 SELECT key FROM redis.kv_{db} WHERE (expires_at IS NULL OR expires_at > now()) \
                 UNION ALL \
                 SELECT c.key FROM (SELECT DISTINCT key FROM redis.hash_{db} \
                                    UNION SELECT DISTINCT key FROM redis.list_{db} \
                                    UNION SELECT DISTINCT key FROM redis.set_{db} \
                                    UNION SELECT DISTINCT key FROM redis.zset_{db} \
                                    UNION SELECT key FROM redis.stream_{db}) c \
                  WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = c.key AND e.expires_at <= now()) \
             ) u"
        );
        self.int("DBSIZE", &sql, &[])
    }

    fn keyspace(&mut self) -> Out<(i64, i64)> {
        let db = self.db;
        let sql = format!(
            "SELECT \
               (SELECT count(*) FROM redis.kv_{db} WHERE expires_at IS NULL OR expires_at > now()) \
               + (SELECT count(*) FROM (SELECT key FROM redis.hash_{db} UNION SELECT key FROM redis.list_{db} \
                    UNION SELECT key FROM redis.set_{db} UNION SELECT key FROM redis.zset_{db} \
                    UNION SELECT key FROM redis.stream_{db}) c \
                  WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = c.key AND e.expires_at <= now())), \
               (SELECT count(*) FROM redis.kv_{db} WHERE expires_at > now()) \
               + (SELECT count(*) FROM redis.expiry_{db} WHERE expires_at > now())"
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
            .update_kept(&crate::commands::sql::flush_dbs(&[self.db as usize]), &[])
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
    fn incrbyfloat(&mut self, key: &[u8], delta: f64) -> Out<Num<String>> {
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
        let Some(base) = std::str::from_utf8(&current)
            .ok()
            .and_then(|t| t.parse::<f64>().ok())
        else {
            return Ok(Num::NotNumber);
        };
        let sum = base + delta;
        if !sum.is_finite() {
            return Ok(Num::Overflow);
        }
        let text = crate::memdb::format_float(sum);
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
    fn setbit(&mut self, key: &[u8], offset: i64, on: bool) -> Out<bool> {
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
        let was = crate::commands::set_bit_at(&mut value, offset, on);
        self.client
            .update_kept(
                &format!("UPDATE redis.kv_{db} SET value = $2 WHERE key = $1"),
                &[key.into(), value.into()],
            )
            .map_err(|e| failed("SETBIT", e))?;
        Ok(was)
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

    fn hset(&mut self, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> Out<i64> {
        // `xmax = 0` marks a row this statement inserted rather than updated,
        // which is the count Redis returns.
        let sql = format!(
            "WITH ins AS ( \
                 INSERT INTO redis.hash_{db} (key, field, value) \
                 SELECT $1, unnest($2::bytea[]), unnest($3::bytea[]) \
                 ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value \
                 RETURNING (xmax = 0) AS added \
             ) \
             SELECT count(*) FILTER (WHERE added)::bigint FROM ins",
            db = self.db
        );
        let fields: Vec<Option<Vec<u8>>> = pairs.iter().map(|(f, _)| Some(f.clone())).collect();
        let vals: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| Some(v.clone())).collect();
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), fields.into(), vals.into()])
            .map_err(|e| failed("HSET", e))?;
        Ok(tbl.first().get::<i64>(1).ok().flatten().unwrap_or(0))
    }

    fn hsetnx(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Out<bool> {
        let sql = format!(
            "INSERT INTO redis.hash_{} (key, field, value) VALUES ($1, $2, $3) \
             ON CONFLICT (key, field) DO NOTHING",
            self.db
        );
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), field.into(), value.into()])
            .map_err(|e| failed("HSETNX", e))?;
        Ok(!tbl.is_empty())
    }

    fn hdel(&mut self, key: &[u8], fields: &[Vec<u8>]) -> Out<i64> {
        let sql = format!(
            "DELETE FROM redis.hash_{} WHERE key = $1 AND field = ANY($2::bytea[])",
            self.db
        );
        let arg: Vec<Option<Vec<u8>>> = fields.iter().cloned().map(Some).collect();
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), arg.into()])
            .map_err(|e| failed("HDEL", e))?;
        Ok(tbl.len() as i64)
    }

    /// One statement. The sum is taken as `numeric` and must land in range,
    /// so an overflow updates nothing instead of raising; a refusal is then
    /// told apart by looking at the value.
    fn hincrby(&mut self, key: &[u8], field: &[u8], delta: i64) -> Out<Num<i64>> {
        let db = self.db;
        let sql = format!(
            "INSERT INTO redis.hash_{db} (key, field, value) \
             VALUES ($1, $2, convert_to($3::text, 'UTF8')) \
             ON CONFLICT (key, field) DO UPDATE \
             SET value = convert_to((encode(redis.hash_{db}.value, 'escape')::numeric + $3)::text, 'UTF8') \
             WHERE encode(redis.hash_{db}.value, 'escape') ~ '^(0|-?[1-9][0-9]*)$' \
               AND encode(redis.hash_{db}.value, 'escape')::numeric \
                   BETWEEN -9223372036854775808 AND 9223372036854775807 \
               AND encode(redis.hash_{db}.value, 'escape')::numeric + $3 \
                   BETWEEN -9223372036854775808 AND 9223372036854775807 \
             RETURNING CAST(encode(value, 'escape') AS bigint)"
        );
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), field.into(), delta.into()])
            .map_err(|e| failed("HINCRBY", e))?;
        if let Some(v) = tbl.first().get::<i64>(1).ok().flatten() {
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
    fn hincrbyfloat(&mut self, key: &[u8], field: &[u8], delta: f64) -> Out<Num<String>> {
        let db = self.db;
        let args: &[DatumWithOid] = &[key.into(), field.into()];
        self.client
            .update_kept(
                &format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) VALUES ($1, $2, '\\x30') \
                     ON CONFLICT (key, field) DO NOTHING"
                ),
                args,
            )
            .map_err(|e| failed("HINCRBYFLOAT", e))?;
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
        let base = match std::str::from_utf8(&current)
            .ok()
            .and_then(|t| t.parse::<f64>().ok())
        {
            Some(v) if v.is_finite() => v,
            _ => return Ok(Num::NotNumber),
        };
        let sum = base + delta;
        if !sum.is_finite() {
            return Ok(Num::Overflow);
        }
        let text = crate::memdb::format_float(sum);
        self.client
            .update_kept(
                &format!("UPDATE redis.hash_{db} SET value = $3 WHERE key = $1 AND field = $2"),
                &[key.into(), field.into(), text.as_bytes().into()],
            )
            .map_err(|e| failed("HINCRBYFLOAT", e))?;
        Ok(Num::Value(text))
    }

    fn sadd(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let sql = format!(
            "WITH ins AS (INSERT INTO redis.set_{} (key, member) SELECT $1, unnest($2::bytea[]) \
             ON CONFLICT DO NOTHING RETURNING 1) SELECT count(*)::bigint FROM ins",
            self.db
        );
        self.write_int("SADD", &sql, &[key.into(), arr(members).into()])
    }

    fn srem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let sql = format!(
            "WITH d AS (DELETE FROM redis.set_{} WHERE key = $1 AND member = ANY($2::bytea[]) RETURNING 1) \
             SELECT count(*)::bigint FROM d",
            self.db
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
        self.column("SMEMBERS", sql, &[key.into()])
    }

    fn scard(&mut self, key: &[u8]) -> Out<i64> {
        let sql = &crate::commands::sql::SCARD[self.db as usize];
        self.int("SCARD", sql, &[key.into()])
    }

    fn spop(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let db = self.db;
        let sql = format!(
            "WITH picked AS (SELECT ctid FROM redis.set_{db} WHERE key = $1 ORDER BY random() LIMIT $2) \
             DELETE FROM redis.set_{db} t USING picked WHERE t.ctid = picked.ctid RETURNING t.member"
        );
        self.write_column("SPOP", &sql, &[key.into(), n.into()])
    }

    fn srandmember(&mut self, key: &[u8], n: i64) -> Out<Vec<Vec<u8>>> {
        let db = self.db;
        // With a negative count Redis allows repeats: LATERAL re-evaluates
        // random() per outer row, so each pick is independent.
        let sql = if n < 0 {
            format!(
                "WITH cnt AS (SELECT count(*)::int AS c FROM redis.set_{db} WHERE key = $1) \
                 SELECT p.member FROM cnt, generate_series(1, $2::int) g, \
                   LATERAL (SELECT member FROM redis.set_{db} WHERE key = $1 \
                            OFFSET floor(random() * cnt.c)::int LIMIT 1) p \
                 WHERE cnt.c > 0"
            )
        } else {
            format!("SELECT member FROM redis.set_{db} WHERE key = $1 ORDER BY random() LIMIT $2")
        };
        self.column(
            "SRANDMEMBER",
            &sql,
            &[
                key.into(),
                n.unsigned_abs().min(i64::MAX as u64).cast_signed().into(),
            ],
        )
    }

    fn smove(&mut self, src: &[u8], dst: &[u8], member: &[u8]) -> Out<(bool, bool)> {
        let db = self.db;
        let sql = format!(
            "WITH d AS (DELETE FROM redis.set_{db} WHERE key = $1 AND member = $3 RETURNING member), \
                  ins AS (INSERT INTO redis.set_{db} (key, member) SELECT $2, d.member FROM d \
                          ON CONFLICT DO NOTHING RETURNING 1) \
             SELECT (SELECT count(*)::bigint FROM d), (SELECT count(*)::bigint FROM ins)"
        );
        let tbl = self
            .client
            .update_kept(&sql, &[src.into(), dst.into(), member.into()])
            .map_err(|e| failed("SMOVE", e))?;
        let row = tbl.first();
        let moved = row.get::<i64>(1).ok().flatten().unwrap_or(0) > 0;
        let added = row.get::<i64>(2).ok().flatten().unwrap_or(0) > 0;
        Ok((moved, added))
    }

    fn sunion(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        let sql = format!(
            "SELECT DISTINCT member FROM redis.set_{} WHERE key = ANY($1::bytea[])",
            self.db
        );
        self.column("SUNION", &sql, &[arr(keys).into()])
    }

    fn sinter(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let sql = crate::commands::build_inter_sql(self.db, keys.len(), 1);
        let args: Vec<DatumWithOid> = keys.iter().map(|k| k.as_slice().into()).collect();
        self.column("SINTER", &sql, &args)
    }

    fn sdiff(&mut self, keys: &[Vec<u8>]) -> Out<Vec<Vec<u8>>> {
        let Some((first, rest)) = keys.split_first() else {
            return Ok(Vec::new());
        };
        let db = self.db;
        let sql = format!(
            "SELECT DISTINCT member FROM redis.set_{db} WHERE key = $1 \
             EXCEPT SELECT member FROM redis.set_{db} WHERE key = ANY($2::bytea[])"
        );
        self.column("SDIFF", &sql, &[first.as_slice().into(), arr(rest).into()])
    }

    fn sunionstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let db = self.db;
        let sql = crate::commands::set_store_sql(
            db,
            &format!("SELECT DISTINCT member FROM redis.set_{db} WHERE key = ANY($2::bytea[])"),
        );
        self.write_int("SUNIONSTORE", &sql, &[dst.into(), arr(keys).into()])
    }

    fn sinterstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let sql = crate::commands::set_store_sql(
            self.db,
            &crate::commands::build_inter_sql(self.db, keys.len(), 2),
        );
        let mut args: Vec<DatumWithOid> = vec![dst.into()];
        args.extend(keys.iter().map(|k| DatumWithOid::from(k.as_slice())));
        self.write_int("SINTERSTORE", &sql, &args)
    }

    fn sdiffstore(&mut self, dst: &[u8], keys: &[Vec<u8>]) -> Out<i64> {
        let Some((first, rest)) = keys.split_first() else {
            return Ok(0);
        };
        let db = self.db;
        let sql = crate::commands::set_store_sql(
            db,
            &format!(
                "SELECT DISTINCT member FROM redis.set_{db} WHERE key = $2 \
                 EXCEPT SELECT member FROM redis.set_{db} WHERE key = ANY($3::bytea[])"
            ),
        );
        self.write_int(
            "SDIFFSTORE",
            &sql,
            &[dst.into(), first.as_slice().into(), arr(rest).into()],
        )
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

    fn zadd(&mut self, key: &[u8], pairs: &[(f64, Vec<u8>)], o: ZaddOpts) -> Out<i64> {
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
        let sql = crate::commands::zadd_sql(self.db, &flags);
        let tbl = self
            .client
            .update_kept(&sql, &[key.into(), members.into(), scores.into()])
            .map_err(|e| failed("ZADD", e))?;
        let row = tbl.first();
        let added = row.get::<i64>(1).ok().flatten().unwrap_or(0);
        let changed = row.get::<i64>(2).ok().flatten().unwrap_or(0);
        Ok(if o.ch { added + changed } else { added })
    }

    /// A new member is inserted outright; an existing one is locked, and its
    /// new score worked out and checked against the conditions before it is
    /// written — so two at once cannot both read the old score, and a sum
    /// that is not a number is never stored.
    fn zadd_incr(&mut self, key: &[u8], member: &[u8], delta: f64, o: ZaddOpts) -> Out<Incr> {
        let db = self.db;
        if !o.xx {
            let inserted = self
                .client
                .update_kept(
                    &format!(
                        "INSERT INTO redis.zset_{db} (key, member, score) VALUES ($1, $2, $3) \
                         ON CONFLICT (key, member) DO NOTHING RETURNING score"
                    ),
                    &[key.into(), member.into(), delta.into()],
                )
                .map_err(|e| failed("ZADD", e))?;
            if !inserted.is_empty() {
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

    fn zrem(&mut self, key: &[u8], members: &[Vec<u8>]) -> Out<i64> {
        let sql = format!(
            "WITH d AS (DELETE FROM redis.zset_{} WHERE key = $1 AND member = ANY($2::bytea[]) RETURNING 1) \
             SELECT count(*)::bigint FROM d",
            self.db
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
