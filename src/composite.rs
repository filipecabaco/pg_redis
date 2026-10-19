//! Commands built from other commands.
//!
//! Most commands are written once against a single backend's `Store`
//! (`src/store.rs`). The commands here go a level higher: they are written in
//! terms of commands that already exist — `LCS` reads two strings with `GET`,
//! `ZRANGESTORE` is a `ZRANGE` and a `ZADD` — and run those through a
//! [`Runner`] that sends each one to whichever backend serves its database.
//! So a command added here works on both halves, and against any database a
//! `MOVE` or `COPY` names, without knowing which is which.
//!
//! The price is atomicity on the memory half: each sub-command takes its own
//! locks, so another worker can interleave between two of them. On the durable
//! half the whole command runs in one subtransaction and its rows stay locked.
#![forbid(unsafe_code)]

use crate::commands::{Command, Response, Value};

/// Runs one sub-command against a database, on the backend that serves it.
pub type Runner<'a> = dyn FnMut(u8, Command) -> Response + 'a;

#[derive(Debug, Clone)]
pub enum Op {
    Lcs {
        a: Vec<u8>,
        b: Vec<u8>,
        len: bool,
        idx: bool,
        min_match_len: i64,
        with_match_len: bool,
    },
    Bitop {
        op: BitOpKind,
        dest: Vec<u8>,
        keys: Vec<Vec<u8>>,
    },
    BitPos {
        key: Vec<u8>,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        by_bit: bool,
    },
    ZRangeStore {
        dst: Vec<u8>,
        src: Vec<u8>,
        /// Everything after `src`, handed to `ZRANGE` as it came.
        range: Vec<Vec<u8>>,
    },
    MPop {
        zset: bool,
        keys: Vec<Vec<u8>>,
        /// LEFT for lists, MIN for sorted sets.
        front: bool,
        count: i64,
    },
    Move {
        key: Vec<u8>,
        db: u8,
    },
    Sort(Box<SortSpec>),
    CollectionScan {
        kind: ScanKind,
        key: Vec<u8>,
        pattern: Option<Vec<u8>>,
    },
    BitField {
        key: Vec<u8>,
        ops: Vec<BitFieldOp>,
    },
    SwapDb {
        a: u8,
        b: u8,
    },
    PfAdd {
        key: Vec<u8>,
        elements: Vec<Vec<u8>>,
    },
    PfCount {
        keys: Vec<Vec<u8>>,
    },
    PfMerge {
        dest: Vec<u8>,
        sources: Vec<Vec<u8>>,
    },
    PfDebug {
        sub: String,
        key: Vec<u8>,
    },
    PfSelfTest,
    GeoAdd {
        key: Vec<u8>,
        /// NX, XX and CH as given, passed through to ZADD.
        flags: Vec<Vec<u8>>,
        points: Vec<(u64, Vec<u8>)>,
    },
    GeoDist {
        key: Vec<u8>,
        a: Vec<u8>,
        b: Vec<u8>,
        conversion: f64,
    },
    GeoLookup {
        /// GEOHASH when true, GEOPOS otherwise.
        hash: bool,
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    GeoSearch {
        variant: GeoVariant,
        args: Vec<Vec<u8>>,
    },
    /// `ZUNION`, `ZINTER`, `ZDIFF`, their `STORE` forms and `ZINTERCARD`,
    /// over sorted sets and plain sets alike.
    ZAggregate(Box<ZAggregate>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetOp {
    Union,
    Inter,
    Diff,
}

#[derive(Debug, Clone)]
pub struct ZAggregate {
    pub op: SetOp,
    pub dst: Option<Vec<u8>>,
    pub keys: Vec<Vec<u8>>,
    /// The options after the keys, read once the keys' types are known:
    /// Redis reports WRONGTYPE ahead of a bad option.
    pub opts: Vec<Vec<u8>>,
    pub cardinality_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoVariant {
    Radius { by_member: bool, readonly: bool },
    Search { store: bool },
}

#[derive(Debug, Clone, Copy)]
pub struct BitFieldOp {
    kind: BitFieldKind,
    signed: bool,
    bits: u32,
    offset: u64,
    value: i64,
    overflow: Overflow,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BitFieldKind {
    Get,
    Set,
    IncrBy,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BitOpKind {
    And,
    Or,
    Xor,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScanKind {
    Hash,
    Set,
    Zset,
}

#[derive(Debug, Clone)]
pub struct SortSpec {
    key: Vec<u8>,
    by: Option<Vec<u8>>,
    limit: Option<(i64, i64)>,
    get: Vec<Vec<u8>>,
    desc: bool,
    alpha: bool,
    pub(crate) store: Option<Vec<u8>>,
}

const SYNTAX: &str = "ERR syntax error";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn arity(name: &str) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_lowercase()
    )
}

fn int(arg: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| NOT_INT.to_string())
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

/// The keys `numkeys` names, and the arguments after them.
type KeysAndRest<'a> = (Vec<Vec<u8>>, &'a [Vec<u8>]);

/// `numkeys key [key ...]` at the front of `args`, Redis's shared shape for
/// the multi-key commands, with its two errors.
fn numkeys(args: &[Vec<u8>]) -> Result<KeysAndRest<'_>, String> {
    let n = int(&args[0]).map_err(|_| "ERR numkeys should be greater than 0".to_string())?;
    if n <= 0 {
        return Err("ERR numkeys should be greater than 0".to_string());
    }
    let n = n as usize;
    if args.len() < 1 + n {
        return Err("ERR Number of keys can't be greater than number of args".to_string());
    }
    Ok((args[1..1 + n].to_vec(), &args[1 + n..]))
}

impl Op {
    /// `Some` when `name` is a command this module implements.
    pub fn parse(name: &str, args: &[Vec<u8>]) -> Option<Result<Op, String>> {
        let parsed = match name {
            "LCS" => parse_lcs(args),
            "BITOP" => parse_bitop(args),
            "BITPOS" => parse_bitpos(args),
            "ZRANGESTORE" => {
                if args.len() < 4 {
                    return Some(Err(arity(name)));
                }
                Ok(Op::ZRangeStore {
                    dst: args[0].clone(),
                    src: args[1].clone(),
                    range: args[2..].to_vec(),
                })
            }
            "LMPOP" | "ZMPOP" => parse_mpop(name, args),
            "ZUNION" | "ZINTER" | "ZDIFF" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE"
            | "ZINTERCARD" => parse_zaggregate(name, args),
            "MOVE" => parse_move(args),
            "SORT" | "SORT_RO" => parse_sort(name == "SORT_RO", args),
            "HSCAN" | "SSCAN" | "ZSCAN" => parse_collection_scan(name, args),
            "BITFIELD" | "BITFIELD_RO" => parse_bitfield(name == "BITFIELD_RO", args),
            "SWAPDB" => parse_swapdb(args),
            "PFADD" if !args.is_empty() => Ok(Op::PfAdd {
                key: args[0].clone(),
                elements: args[1..].to_vec(),
            }),
            "PFCOUNT" if !args.is_empty() => Ok(Op::PfCount {
                keys: args.to_vec(),
            }),
            "PFMERGE" if !args.is_empty() => Ok(Op::PfMerge {
                dest: args[0].clone(),
                sources: args[1..].to_vec(),
            }),
            "PFDEBUG" if args.len() >= 2 => {
                let sub = String::from_utf8_lossy(&args[0]).to_string();
                if args.len() != 2 {
                    return Some(Err(format!(
                        "ERR Wrong number of arguments for the '{sub}' subcommand"
                    )));
                }
                Ok(Op::PfDebug {
                    sub,
                    key: args[1].clone(),
                })
            }
            "PFSELFTEST" if args.is_empty() => Ok(Op::PfSelfTest),
            "PFADD" | "PFCOUNT" | "PFMERGE" | "PFDEBUG" | "PFSELFTEST" => Err(arity(name)),
            "GEOADD" => parse_geoadd(args),
            "GEODIST" => parse_geodist(args),
            "GEOHASH" | "GEOPOS" if !args.is_empty() => Ok(Op::GeoLookup {
                hash: name == "GEOHASH",
                key: args[0].clone(),
                members: args[1..].to_vec(),
            }),
            "GEOHASH" | "GEOPOS" => Err(arity(name)),
            "GEORADIUS"
            | "GEORADIUS_RO"
            | "GEORADIUSBYMEMBER"
            | "GEORADIUSBYMEMBER_RO"
            | "GEOSEARCH"
            | "GEOSEARCHSTORE" => {
                let (variant, min) = match name {
                    "GEOSEARCH" => (GeoVariant::Search { store: false }, 6),
                    "GEOSEARCHSTORE" => (GeoVariant::Search { store: true }, 7),
                    n => {
                        let by_member = n.starts_with("GEORADIUSBYMEMBER");
                        let readonly = n.ends_with("_RO");
                        (
                            GeoVariant::Radius {
                                by_member,
                                readonly,
                            },
                            if by_member { 4 } else { 5 },
                        )
                    }
                };
                if args.len() < min {
                    return Some(Err(arity(name)));
                }
                Ok(Op::GeoSearch {
                    variant,
                    args: args.to_vec(),
                })
            }
            _ => return None,
        };
        Some(parsed)
    }

    /// The keys this command may write, with the database each lives in, for
    /// WATCH. Empty for the read-only ones.
    pub fn write_targets(&self, db: u8) -> Vec<(u8, &[u8])> {
        match self {
            Op::Bitop { dest, .. } => vec![(db, dest)],
            Op::ZRangeStore { dst, .. } => vec![(db, dst)],
            Op::ZAggregate(z) => z.dst.iter().map(|d| (db, d.as_slice())).collect(),
            Op::MPop { keys, .. } => keys.iter().map(|k| (db, k.as_slice())).collect(),
            Op::Move { key, db: to } => vec![(db, key), (*to, key)],
            Op::Sort(spec) => spec.store.iter().map(|k| (db, k.as_slice())).collect(),
            Op::BitField { key, ops } if ops.iter().any(|o| o.kind != BitFieldKind::Get) => {
                vec![(db, key)]
            }
            Op::PfAdd { key, .. } | Op::PfDebug { key, .. } => vec![(db, key)],
            Op::PfCount { keys } if keys.len() == 1 => vec![(db, &keys[0])],
            Op::PfMerge { dest, .. } => vec![(db, dest)],
            Op::GeoAdd { key, .. } => vec![(db, key)],
            Op::GeoSearch {
                variant: GeoVariant::Search { store: true },
                args,
            } => vec![(db, &args[0])],
            // GEORADIUS ... STORE names its key among the options.
            Op::GeoSearch { args, .. } => args
                .windows(2)
                .filter(|w| is(&w[0], "STORE") || is(&w[0], "STOREDIST"))
                .map(|w| (db, w[1].as_slice()))
                .collect(),
            _ => vec![],
        }
    }

    /// Whether this command rewrites keys it cannot name, so every WATCH must
    /// be invalidated — `SWAPDB` changes what every key in two databases is.
    pub fn touches_everything(&self) -> bool {
        matches!(self, Op::SwapDb { .. })
    }

    pub fn run(&self, db: u8, exec: &mut Runner) -> Response {
        let result = match self {
            Op::Lcs {
                a,
                b,
                len,
                idx,
                min_match_len,
                with_match_len,
            } => run_lcs(
                exec,
                db,
                a,
                b,
                LcsOpts {
                    getlen: *len,
                    getidx: *idx,
                    min_match_len: *min_match_len,
                    with_match_len: *with_match_len,
                },
            ),
            Op::Bitop { op, dest, keys } => run_bitop(exec, db, *op, dest, keys),
            Op::BitPos {
                key,
                bit,
                start,
                end,
                by_bit,
            } => run_bitpos(exec, db, key, *bit, *start, *end, *by_bit),
            Op::ZRangeStore { dst, src, range } => run_zrangestore(exec, db, dst, src, range),
            Op::MPop {
                zset,
                keys,
                front,
                count,
            } => run_mpop(exec, db, *zset, keys, *front, *count),
            Op::Move { key, db: to } => run_move(exec, db, key, *to),
            Op::Sort(spec) => run_sort(exec, db, spec),
            Op::CollectionScan { kind, key, pattern } => {
                run_collection_scan(exec, db, *kind, key, pattern.as_deref())
            }
            Op::BitField { key, ops } => run_bitfield(exec, db, key, ops),
            Op::SwapDb { a, b } => run_swapdb(exec, *a, *b),
            Op::PfAdd { key, elements } => run_pfadd(exec, db, key, elements),
            Op::PfCount { keys } => run_pfcount(exec, db, keys),
            Op::PfMerge { dest, sources } => run_pfmerge(exec, db, dest, sources),
            Op::PfDebug { sub, key } => run_pfdebug(exec, db, sub, key),
            Op::PfSelfTest => crate::hll::self_test(crate::hll::DEFAULT_SPARSE_MAX_BYTES)
                .map(|_| Response::Ok)
                .map_err(Response::Error),
            Op::GeoAdd { key, flags, points } => run_geoadd(exec, db, key, flags, points),
            Op::GeoDist {
                key,
                a,
                b,
                conversion,
            } => run_geodist(exec, db, key, a, b, *conversion),
            Op::GeoLookup { hash, key, members } => run_geolookup(exec, db, *hash, key, members),
            Op::GeoSearch { variant, args } => run_geosearch(exec, db, *variant, args),
            Op::ZAggregate(z) => run_zaggregate(exec, db, z),
        };
        result.unwrap_or_else(|r| r)
    }
}

// ───────────────────────────── sub-command calls ─────────────────────────────

/// Run `argv` as a command, the way a client would have sent it.
pub(crate) fn call(exec: &mut Runner, db: u8, argv: &[&[u8]]) -> Response {
    match Command::parse(argv.iter().map(|a| a.to_vec()).collect::<Vec<_>>()) {
        Ok(cmd) => exec(db, cmd),
        Err(e) => Response::Error(e),
    }
}

/// An error reply is passed straight back to the client, WRONGTYPE included.
fn ok(r: Response) -> Result<Response, Response> {
    match r {
        Response::Error(_) => Err(r),
        r => Ok(r),
    }
}

/// A string key's value: `None` when there is no key, WRONGTYPE otherwise.
fn get_string(exec: &mut Runner, db: u8, key: &[u8]) -> Result<Option<Vec<u8>>, Response> {
    match ok(call(exec, db, &[b"GET", key]))? {
        Response::BulkString(v) => Ok(Some(v)),
        _ => Ok(None),
    }
}

fn key_type(exec: &mut Runner, db: u8, key: &[u8]) -> Result<String, Response> {
    match ok(call(exec, db, &[b"TYPE", key]))? {
        Response::SimpleString(t) => Ok(t),
        _ => Ok("none".to_string()),
    }
}

/// The elements of an array reply, nils as empty.
fn items(r: Response) -> Vec<Vec<u8>> {
    match r {
        Response::Array(v) => v.into_iter().map(Option::unwrap_or_default).collect(),
        _ => vec![],
    }
}

fn int_reply(r: &Response) -> i64 {
    match r {
        Response::Integer(n) => *n,
        _ => 0,
    }
}

// ─────────────────────────────────── LCS ─────────────────────────────────────

fn parse_lcs(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 2 {
        return Err(arity("LCS"));
    }
    let (mut len, mut idx, mut with_match_len, mut min_match_len) = (false, false, false, 0);
    let mut i = 2;
    while i < args.len() {
        let a = &args[i];
        if is(a, "IDX") {
            idx = true;
        } else if is(a, "LEN") {
            len = true;
        } else if is(a, "WITHMATCHLEN") {
            with_match_len = true;
        } else if is(a, "MINMATCHLEN") && i + 1 < args.len() {
            min_match_len = int(&args[i + 1])?.max(0);
            i += 1;
        } else {
            return Err(SYNTAX.to_string());
        }
        i += 1;
    }
    if len && idx {
        return Err(
            "ERR If you want both the length and indexes, please just use IDX.".to_string(),
        );
    }
    Ok(Op::Lcs {
        a: args[0].clone(),
        b: args[1].clone(),
        len,
        idx,
        min_match_len,
        with_match_len,
    })
}

/// `LCS`'s options: `LEN`, `IDX`, `MINMATCHLEN` and `WITHMATCHLEN`.
struct LcsOpts {
    getlen: bool,
    getidx: bool,
    min_match_len: i64,
    with_match_len: bool,
}

fn run_lcs(
    exec: &mut Runner,
    db: u8,
    ka: &[u8],
    kb: &[u8],
    opts: LcsOpts,
) -> Result<Response, Response> {
    let LcsOpts {
        getlen,
        getidx,
        min_match_len,
        with_match_len,
    } = opts;
    let not_strings =
        || Response::Error("ERR The specified keys must contain string values".to_string());
    let a = get_string(exec, db, ka)
        .map_err(|_| not_strings())?
        .unwrap_or_default();
    let b = get_string(exec, db, kb)
        .map_err(|_| not_strings())?
        .unwrap_or_default();
    Ok(lcs(&a, &b, getlen, getidx, min_match_len, with_match_len))
}

/// Redis's `lcsCommand`, ranges and their quirks included: a match that does
/// not extend the current range ends it without starting the next.
fn lcs(
    a: &[u8],
    b: &[u8],
    getlen: bool,
    getidx: bool,
    min_match_len: i64,
    with_match_len: bool,
) -> Response {
    let (alen, blen) = (a.len(), b.len());
    let cells = (alen + 1) as u64 * (blen + 1) as u64;
    if cells * 4 > 512 * 1024 * 1024 {
        return Response::Error(
            "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len"
                .to_string(),
        );
    }
    let w = blen + 1;
    let mut t = vec![0u32; (alen + 1) * w];
    for i in 1..=alen {
        for j in 1..=blen {
            t[i * w + j] = if a[i - 1] == b[j - 1] {
                t[(i - 1) * w + j - 1] + 1
            } else {
                t[(i - 1) * w + j].max(t[i * w + j - 1])
            };
        }
    }
    let total = t[alen * w + blen];
    if getlen {
        return Response::Integer(total as i64);
    }
    let mut result = vec![0u8; total as usize];
    let mut idx = total as usize;
    let mut matches: Vec<Value> = Vec::new();
    let (mut a_start, mut a_end, mut b_start, mut b_end) = (alen, 0usize, 0usize, 0usize);
    let (mut i, mut j) = (alen, blen);
    while i > 0 && j > 0 {
        let mut emit = false;
        if a[i - 1] == b[j - 1] {
            result[idx - 1] = a[i - 1];
            if a_start == alen {
                (a_start, a_end, b_start, b_end) = (i - 1, i - 1, j - 1, j - 1);
            } else if a_start == i && b_start == j {
                a_start -= 1;
                b_start -= 1;
            } else {
                emit = true;
            }
            if a_start == 0 || b_start == 0 {
                emit = true;
            }
            idx -= 1;
            i -= 1;
            j -= 1;
        } else {
            if t[(i - 1) * w + j] > t[i * w + j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
            if a_start != alen {
                emit = true;
            }
        }
        if emit {
            let match_len = (a_end - a_start + 1) as i64;
            if getidx && (min_match_len == 0 || match_len >= min_match_len) {
                let mut m = vec![
                    Value::Array(vec![Value::Int(a_start as i64), Value::Int(a_end as i64)]),
                    Value::Array(vec![Value::Int(b_start as i64), Value::Int(b_end as i64)]),
                ];
                if with_match_len {
                    m.push(Value::Int(match_len));
                }
                matches.push(Value::Array(m));
            }
            a_start = alen;
        }
    }
    if getidx {
        Response::Value(Value::Map(vec![
            (Value::str("matches"), Value::Array(matches)),
            (Value::str("len"), Value::Int(total as i64)),
        ]))
    } else {
        Response::BulkString(result)
    }
}

// ────────────────────────────── BITOP / BITPOS ───────────────────────────────

fn parse_bitop(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 3 {
        return Err(arity("BITOP"));
    }
    let op = match String::from_utf8_lossy(&args[0]).to_lowercase().as_str() {
        "and" => BitOpKind::And,
        "or" => BitOpKind::Or,
        "xor" => BitOpKind::Xor,
        "not" => BitOpKind::Not,
        _ => return Err(SYNTAX.to_string()),
    };
    if op == BitOpKind::Not && args.len() != 3 {
        return Err("ERR BITOP NOT must be called with a single source key.".to_string());
    }
    Ok(Op::Bitop {
        op,
        dest: args[1].clone(),
        keys: args[2..].to_vec(),
    })
}

fn run_bitop(
    exec: &mut Runner,
    db: u8,
    op: BitOpKind,
    dest: &[u8],
    keys: &[Vec<u8>],
) -> Result<Response, Response> {
    let mut srcs = Vec::with_capacity(keys.len());
    for k in keys {
        srcs.push(get_string(exec, db, k)?.unwrap_or_default());
    }
    let out = bitop(op, &srcs);
    if out.is_empty() {
        ok(call(exec, db, &[b"DEL", dest]))?;
        return Ok(Response::Integer(0));
    }
    ok(call(exec, db, &[b"SET", dest, &out]))?;
    Ok(Response::Integer(out.len() as i64))
}

/// Shorter inputs read as zero-padded to the longest, as in Redis.
fn bitop(op: BitOpKind, srcs: &[Vec<u8>]) -> Vec<u8> {
    let maxlen = srcs.iter().map(Vec::len).max().unwrap_or(0);
    (0..maxlen)
        .map(|j| {
            let byte = |s: &Vec<u8>| s.get(j).copied().unwrap_or(0);
            let first = byte(&srcs[0]);
            match op {
                BitOpKind::Not => !first,
                BitOpKind::And => srcs[1..].iter().fold(first, |acc, s| acc & byte(s)),
                BitOpKind::Or => srcs[1..].iter().fold(first, |acc, s| acc | byte(s)),
                BitOpKind::Xor => srcs[1..].iter().fold(first, |acc, s| acc ^ byte(s)),
            }
        })
        .collect()
}

fn parse_bitpos(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 2 {
        return Err(arity("BITPOS"));
    }
    if args.len() > 5 {
        return Err(SYNTAX.to_string());
    }
    let bit = int(&args[1])?;
    if bit != 0 && bit != 1 {
        return Err("ERR The bit argument must be 1 or 0.".to_string());
    }
    let start = args.get(2).map(|a| int(a)).transpose()?;
    let end = args.get(3).map(|a| int(a)).transpose()?;
    let by_bit = match args.get(4) {
        None => false,
        Some(a) if is(a, "BIT") => true,
        Some(a) if is(a, "BYTE") => false,
        Some(_) => return Err(SYNTAX.to_string()),
    };
    Ok(Op::BitPos {
        key: args[0].clone(),
        bit: bit as u8,
        start,
        end,
        by_bit,
    })
}

fn run_bitpos(
    exec: &mut Runner,
    db: u8,
    key: &[u8],
    bit: u8,
    start: Option<i64>,
    end: Option<i64>,
    by_bit: bool,
) -> Result<Response, Response> {
    let Some(v) = get_string(exec, db, key)? else {
        return Ok(Response::Integer(if bit == 1 { -1 } else { 0 }));
    };
    Ok(Response::Integer(bitpos(&v, bit, start, end, by_bit)))
}

/// First position of `bit` in `p[..count]`; for a clear bit that is not there,
/// the first bit past the end, since a string reads as zero-padded.
fn first_bit(p: &[u8], bit: u8) -> i64 {
    for (i, &byte) in p.iter().enumerate() {
        let b = if bit == 1 { byte } else { !byte };
        if b != 0 {
            return i as i64 * 8 + b.leading_zeros() as i64;
        }
    }
    if bit == 1 { -1 } else { p.len() as i64 * 8 }
}

/// Redis's `bitposCommand`, edge masks for BIT ranges included.
fn bitpos(p: &[u8], bit: u8, start: Option<i64>, end: Option<i64>, by_bit: bool) -> i64 {
    let strlen = p.len() as i64;
    let end_given = end.is_some();
    let (mut first_mask, mut last_mask) = (0u8, 0u8);
    let (mut start, end) = match start {
        None => (0, strlen - 1),
        Some(s) => {
            let mut totlen = strlen;
            let mut e = end.unwrap_or(if by_bit {
                (totlen << 3) + 7
            } else {
                totlen - 1
            });
            if by_bit {
                totlen <<= 3;
            }
            let mut s = s;
            if s < 0 {
                s += totlen;
            }
            if e < 0 {
                e += totlen;
            }
            s = s.max(0);
            e = e.max(0);
            if e >= totlen {
                e = totlen - 1;
            }
            if by_bit && s <= e {
                first_mask = (!((1u16 << (8 - (s & 7))) - 1) & 0xFF) as u8;
                last_mask = ((1u16 << (7 - (e & 7))) - 1) as u8;
                s >>= 3;
                e >>= 3;
            }
            (s, e)
        }
    };
    if start > end {
        return -1;
    }
    let mut bytes = end - start + 1;
    let pos = 'found: {
        if first_mask != 0 {
            let mut c = p[start as usize];
            c = if bit == 1 {
                c & !first_mask
            } else {
                c | first_mask
            };
            if last_mask != 0 && bytes == 1 {
                c = if bit == 1 {
                    c & !last_mask
                } else {
                    c | last_mask
                };
            }
            let pos = first_bit(&[c], bit);
            if bytes == 1 || (pos != -1 && pos != 8) {
                break 'found pos;
            }
            start += 1;
            bytes -= 1;
        }
        let cur = bytes - i64::from(last_mask != 0);
        if cur > 0 {
            let pos = first_bit(&p[start as usize..(start + cur) as usize], bit);
            if bytes == cur || (pos != -1 && pos != cur << 3) {
                break 'found pos;
            }
            start += cur;
            bytes -= cur;
        }
        let c = p[end as usize];
        let c = if bit == 1 {
            c & !last_mask
        } else {
            c | last_mask
        };
        first_bit(&[c], bit)
    };
    if end_given && bit == 0 && pos == bytes << 3 {
        return -1;
    }
    if pos != -1 { pos + (start << 3) } else { pos }
}

// ────────────────────────────── ZRANGESTORE ──────────────────────────────────

fn run_zrangestore(
    exec: &mut Runner,
    db: u8,
    dst: &[u8],
    src: &[u8],
    range: &[Vec<u8>],
) -> Result<Response, Response> {
    let mut argv: Vec<&[u8]> = vec![b"ZRANGE", src];
    argv.extend(range.iter().map(Vec::as_slice));
    // WITHSCORES belongs to the reply, which ZRANGESTORE does not have.
    if range.iter().any(|a| is(a, "WITHSCORES")) {
        return Err(Response::Error(SYNTAX.to_string()));
    }
    // The members first, then their scores: BYLEX refuses WITHSCORES.
    let members = items(ok(call(exec, db, &argv))?);
    let scores = if members.is_empty() {
        Vec::new()
    } else {
        let mut zmscore: Vec<&[u8]> = vec![b"ZMSCORE", src];
        zmscore.extend(members.iter().map(Vec::as_slice));
        items(ok(call(exec, db, &zmscore))?)
    };
    ok(call(exec, db, &[b"DEL", dst]))?;
    if members.is_empty() {
        return Ok(Response::Integer(0));
    }
    let mut zadd: Vec<&[u8]> = vec![b"ZADD", dst];
    for (m, s) in members.iter().zip(&scores) {
        zadd.push(s);
        zadd.push(m);
    }
    ok(call(exec, db, &zadd))?;
    Ok(Response::Integer(members.len() as i64))
}

// ─────────────────────── ZUNION / ZINTER / ZDIFF ─────────────────────────────

/// `zunionInterDiffGenericCommand`'s argument checks, up to the keys.
fn parse_zaggregate(name: &str, args: &[Vec<u8>]) -> Result<Op, String> {
    let (op, store, cardinality_only, lower) = match name {
        "ZUNION" => (SetOp::Union, false, false, "zunion"),
        "ZINTER" => (SetOp::Inter, false, false, "zinter"),
        "ZDIFF" => (SetOp::Diff, false, false, "zdiff"),
        "ZUNIONSTORE" => (SetOp::Union, true, false, "zunionstore"),
        "ZINTERSTORE" => (SetOp::Inter, true, false, "zinterstore"),
        "ZDIFFSTORE" => (SetOp::Diff, true, false, "zdiffstore"),
        _ => (SetOp::Inter, false, true, "zintercard"),
    };
    let numkeys_at = usize::from(store);
    if args.len() < numkeys_at + 2 {
        return Err(arity(name));
    }
    let n = int(&args[numkeys_at]).map_err(|_| NOT_INT.to_string())?;
    if n < 1 {
        return Err(format!(
            "ERR at least 1 input key is needed for '{lower}' command"
        ));
    }
    let first = numkeys_at + 1;
    if n as usize > args.len() - first {
        return Err(SYNTAX.to_string());
    }
    let end = first + n as usize;
    Ok(Op::ZAggregate(Box::new(ZAggregate {
        op,
        dst: store.then(|| args[0].clone()),
        keys: args[first..end].to_vec(),
        opts: args[end..].to_vec(),
        cardinality_only,
    })))
}

/// The options after the keys: weights, aggregate, WITHSCORES, LIMIT.
struct ZAggregateOpts {
    weights: Vec<f64>,
    aggregate: crate::commands::Aggregate,
    with_scores: bool,
    limit: usize,
}

fn zaggregate_opts(z: &ZAggregate) -> Result<ZAggregateOpts, Response> {
    use crate::commands::Aggregate;
    let syntax = || Response::Error(SYNTAX.to_string());
    let n = z.keys.len();
    let mut o = ZAggregateOpts {
        weights: vec![1.0; n],
        aggregate: Aggregate::Sum,
        with_scores: false,
        limit: 0,
    };
    let opts = &z.opts;
    let mut j = 0;
    while j < opts.len() {
        let remaining = opts.len() - j;
        let a = &opts[j];
        if z.op != SetOp::Diff && !z.cardinality_only && remaining > n && is(a, "WEIGHTS") {
            for i in 0..n {
                o.weights[i] = crate::commands::parse_score_value(&opts[j + 1 + i])
                    .ok_or_else(|| Response::Error("ERR weight value is not a float".into()))?;
            }
            j += 1 + n;
        } else if z.op != SetOp::Diff && !z.cardinality_only && remaining >= 2 && is(a, "AGGREGATE")
        {
            o.aggregate = match &opts[j + 1] {
                v if is(v, "SUM") => Aggregate::Sum,
                v if is(v, "MIN") => Aggregate::Min,
                v if is(v, "MAX") => Aggregate::Max,
                _ => return Err(syntax()),
            };
            j += 2;
        } else if z.dst.is_none() && !z.cardinality_only && is(a, "WITHSCORES") {
            o.with_scores = true;
            j += 1;
        } else if z.cardinality_only && remaining >= 2 && is(a, "LIMIT") {
            // One message for a bad and a negative limit, as Redis gives.
            let limit = int(&opts[j + 1])
                .ok()
                .filter(|l| *l >= 0)
                .ok_or_else(|| Response::Error("ERR LIMIT can't be negative".into()))?;
            o.limit = limit as usize;
            j += 2;
        } else {
            return Err(syntax());
        }
    }
    Ok(o)
}

/// `zunionInterAggregate`: SUM turns inf + -inf into 0, not NaN.
fn aggregate_into(target: &mut f64, value: f64, how: crate::commands::Aggregate) {
    use crate::commands::Aggregate;
    match how {
        Aggregate::Sum => {
            *target += value;
            if target.is_nan() {
                *target = 0.0;
            }
        }
        Aggregate::Min => *target = target.min(value),
        Aggregate::Max => *target = target.max(value),
    }
}

/// A sorted set's members and scores, or a plain set's members at score 1.
fn zaggregate_source(
    exec: &mut Runner,
    db: u8,
    key: &[u8],
) -> Result<Vec<(Vec<u8>, f64)>, Response> {
    match key_type(exec, db, key)?.as_str() {
        "none" => Ok(Vec::new()),
        "zset" => {
            let flat = items(ok(call(
                exec,
                db,
                &[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"],
            ))?);
            Ok(flat
                .chunks(2)
                .filter_map(|p| {
                    let score = crate::commands::parse_score_value(p.get(1)?)?;
                    Some((p[0].clone(), score))
                })
                .collect())
        }
        "set" => Ok(items(ok(call(exec, db, &[b"SMEMBERS", key]))?)
            .into_iter()
            .map(|m| (m, 1.0))
            .collect()),
        _ => Err(Response::Error(WRONGTYPE.to_string())),
    }
}

fn run_zaggregate(exec: &mut Runner, db: u8, z: &ZAggregate) -> Result<Response, Response> {
    use std::collections::{HashMap, HashSet};
    let sources = z
        .keys
        .iter()
        .map(|k| zaggregate_source(exec, db, k))
        .collect::<Result<Vec<_>, _>>()?;
    let o = zaggregate_opts(z)?;
    let weighted = |score: f64, w: f64| {
        let v = score * w;
        if v.is_nan() { 0.0 } else { v }
    };
    let mut result: Vec<(Vec<u8>, f64)> = match z.op {
        SetOp::Union => {
            let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
            for (src, w) in sources.iter().zip(&o.weights) {
                for (m, s) in src {
                    let v = weighted(*s, *w);
                    match acc.get_mut(m) {
                        Some(t) => aggregate_into(t, v, o.aggregate),
                        None => {
                            acc.insert(m.clone(), v);
                        }
                    }
                }
            }
            acc.into_iter().collect()
        }
        SetOp::Inter => {
            let lookups: Vec<HashMap<&[u8], f64>> = sources
                .iter()
                .map(|src| src.iter().map(|(m, s)| (m.as_slice(), *s)).collect())
                .collect();
            let mut out = Vec::new();
            for (m, s) in &sources[0] {
                let mut score = weighted(*s, o.weights[0]);
                let in_all = (1..sources.len()).all(|i| match lookups[i].get(m.as_slice()) {
                    Some(v) => {
                        aggregate_into(&mut score, v * o.weights[i], o.aggregate);
                        true
                    }
                    None => false,
                });
                if in_all {
                    out.push((m.clone(), score));
                    if z.cardinality_only && o.limit > 0 && out.len() >= o.limit {
                        break;
                    }
                }
            }
            out
        }
        SetOp::Diff => {
            let others: HashSet<&[u8]> = sources[1..]
                .iter()
                .flat_map(|src| src.iter().map(|(m, _)| m.as_slice()))
                .collect();
            sources[0]
                .iter()
                .filter(|(m, _)| !others.contains(m.as_slice()))
                .cloned()
                .collect()
        }
    };
    if z.cardinality_only {
        return Ok(Response::Integer(result.len() as i64));
    }
    result.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    if let Some(dst) = &z.dst {
        ok(call(exec, db, &[b"DEL", dst]))?;
        if result.is_empty() {
            return Ok(Response::Integer(0));
        }
        let scores: Vec<Vec<u8>> = result
            .iter()
            .map(|(_, s)| match *s {
                s if s == f64::INFINITY => b"+inf".to_vec(),
                s if s == f64::NEG_INFINITY => b"-inf".to_vec(),
                s => format!("{s}").into_bytes(),
            })
            .collect();
        let mut zadd: Vec<&[u8]> = vec![b"ZADD", dst];
        for ((m, _), s) in result.iter().zip(&scores) {
            zadd.push(s);
            zadd.push(m);
        }
        ok(call(exec, db, &zadd))?;
        return Ok(Response::Integer(result.len() as i64));
    }
    let mut out = Vec::with_capacity(result.len() * if o.with_scores { 2 } else { 1 });
    for (m, s) in result {
        out.push(Some(m));
        if o.with_scores {
            out.push(Some(crate::commands::format_score(s).into_bytes()));
        }
    }
    Ok(Response::Array(out))
}

// ───────────────────────────── LMPOP / ZMPOP ─────────────────────────────────

fn parse_mpop(name: &str, args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 3 {
        return Err(arity(name));
    }
    let zset = name == "ZMPOP";
    let (keys, rest) = numkeys(args)?;
    let front = match rest.first() {
        Some(a) if is(a, if zset { "MIN" } else { "LEFT" }) => true,
        Some(a) if is(a, if zset { "MAX" } else { "RIGHT" }) => false,
        _ => return Err(SYNTAX.to_string()),
    };
    let count = match &rest[1..] {
        [] => 1,
        [c, n] if is(c, "COUNT") => {
            let n = int(n).map_err(|_| "ERR count should be greater than 0".to_string())?;
            if n <= 0 {
                return Err("ERR count should be greater than 0".to_string());
            }
            n
        }
        _ => return Err(SYNTAX.to_string()),
    };
    Ok(Op::MPop {
        zset,
        keys,
        front,
        count,
    })
}

/// The first non-empty key, popped from: `[key, elements]`, or a nil array.
/// For a sorted set each element is a `[member, score]` pair.
pub fn run_mpop(
    exec: &mut Runner,
    db: u8,
    zset: bool,
    keys: &[Vec<u8>],
    front: bool,
    count: i64,
) -> Result<Response, Response> {
    let (len_cmd, pop_cmd): (&[u8], &[u8]) = match (zset, front) {
        (false, true) => (b"LLEN", b"LPOP"),
        (false, false) => (b"LLEN", b"RPOP"),
        (true, true) => (b"ZCARD", b"ZPOPMIN"),
        (true, false) => (b"ZCARD", b"ZPOPMAX"),
    };
    let n = count.to_string();
    for key in keys {
        if int_reply(&ok(call(exec, db, &[len_cmd, key]))?) == 0 {
            continue;
        }
        let popped = items(ok(call(exec, db, &[pop_cmd, key, n.as_bytes()]))?);
        let elements = if zset {
            popped
                .chunks(2)
                .map(|p| Value::Array(vec![Value::bulk(p[0].clone()), Value::bulk(p[1].clone())]))
                .collect()
        } else {
            popped.into_iter().map(Value::Bulk).collect()
        };
        return Ok(Response::Value(Value::Array(vec![
            Value::bulk(key.clone()),
            Value::Array(elements),
        ])));
    }
    Ok(Response::NullArray)
}

// ─────────────────────────────────── MOVE ────────────────────────────────────

fn parse_move(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() != 2 {
        return Err(arity("MOVE"));
    }
    let db = int(&args[1])?;
    if !(0..crate::commands::NUM_DBS as i64).contains(&db) {
        return Err("ERR DB index is out of range".to_string());
    }
    Ok(Op::Move {
        key: args[0].clone(),
        db: db as u8,
    })
}

/// `COPY` into the other database without REPLACE, then `DEL` here — COPY
/// already moves a whole key of any type, TTL included, across halves.
fn run_move(exec: &mut Runner, db: u8, key: &[u8], to: u8) -> Result<Response, Response> {
    if db == to {
        return Err(Response::Error(
            "ERR source and destination objects are the same".to_string(),
        ));
    }
    let target = to.to_string();
    let copied = ok(call(
        exec,
        db,
        &[b"COPY", key, key, b"DB", target.as_bytes()],
    ))?;
    if int_reply(&copied) == 0 {
        return Ok(Response::Integer(0));
    }
    ok(call(exec, db, &[b"DEL", key]))?;
    Ok(Response::Integer(1))
}

// ─────────────────────────────────── SORT ────────────────────────────────────

fn parse_sort(readonly: bool, args: &[Vec<u8>]) -> Result<Op, String> {
    let name = if readonly { "SORT_RO" } else { "SORT" };
    if args.is_empty() {
        return Err(arity(name));
    }
    let mut spec = SortSpec {
        key: args[0].clone(),
        by: None,
        limit: None,
        get: vec![],
        desc: false,
        alpha: false,
        store: None,
    };
    let mut j = 1;
    while j < args.len() {
        let a = &args[j];
        let left = args.len() - j - 1;
        if is(a, "ASC") {
            spec.desc = false;
        } else if is(a, "DESC") {
            spec.desc = true;
        } else if is(a, "ALPHA") {
            spec.alpha = true;
        } else if is(a, "LIMIT") && left >= 2 {
            spec.limit = Some((int(&args[j + 1])?, int(&args[j + 2])?));
            j += 2;
        } else if !readonly && is(a, "STORE") && left >= 1 {
            spec.store = Some(args[j + 1].clone());
            j += 1;
        } else if is(a, "BY") && left >= 1 {
            spec.by = Some(args[j + 1].clone());
            j += 1;
        } else if is(a, "GET") && left >= 1 {
            spec.get.push(args[j + 1].clone());
            j += 1;
        } else {
            return Err(SYNTAX.to_string());
        }
        j += 1;
    }
    Ok(Op::Sort(Box::new(spec)))
}

/// Redis's `lookupKeyByPattern`: `#` is the element itself; otherwise the
/// first `*` takes the element, and a trailing `->field` reads a hash field.
/// A key of the wrong type, or no `*` at all, is nil — never an error.
fn lookup_by_pattern(exec: &mut Runner, db: u8, pattern: &[u8], elem: &[u8]) -> Option<Vec<u8>> {
    if pattern == b"#" {
        return Some(elem.to_vec());
    }
    let star = pattern.iter().position(|&c| c == b'*')?;
    let after = &pattern[star + 1..];
    let (postfix, field) = match after.windows(2).position(|w| w == b"->") {
        Some(f) if f + 2 < after.len() => (&after[..f], Some(&after[f + 2..])),
        _ => (after, None),
    };
    let mut key = pattern[..star].to_vec();
    key.extend_from_slice(elem);
    key.extend_from_slice(postfix);
    let r = match field {
        Some(f) => call(exec, db, &[b"HGET", &key, f]),
        None => call(exec, db, &[b"GET", &key]),
    };
    match r {
        Response::BulkString(v) => Some(v),
        _ => None,
    }
}

/// `strtod` as SORT applies it: leading space allowed, the whole rest must
/// parse, NaN refused.
fn sort_score(v: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(v).ok()?.trim_start();
    let f = match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        _ => s.parse::<f64>().ok()?,
    };
    (!f.is_nan()).then_some(f)
}

fn run_sort(exec: &mut Runner, db: u8, spec: &SortSpec) -> Result<Response, Response> {
    let kind = key_type(exec, db, &spec.key)?;
    let mut elems: Vec<Vec<u8>> = match kind.as_str() {
        "list" => items(ok(call(exec, db, &[b"LRANGE", &spec.key, b"0", b"-1"]))?),
        "set" => items(ok(call(exec, db, &[b"SMEMBERS", &spec.key]))?),
        "zset" => items(ok(call(exec, db, &[b"ZRANGE", &spec.key, b"0", b"-1"]))?),
        "none" => vec![],
        _ => {
            return Err(Response::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ));
        }
    };

    let mut dontsort = spec.by.as_ref().is_some_and(|b| !b.contains(&b'*'));
    let mut alpha = spec.alpha;
    let mut by = spec.by.clone();
    // A set has no order of its own, so a stored result — or one a script
    // sees — is sorted anyway, to be the same on every replica: Redis forces
    // ALPHA here (`storekey || c->flags & CLIENT_SCRIPT`).
    if dontsort && kind == "set" && (spec.store.is_some() || crate::scripting::in_script_call()) {
        dontsort = false;
        alpha = true;
        by = None;
    }

    let n = elems.len() as i64;
    let (lstart, lcount) = spec.limit.unwrap_or((0, -1));
    let mut start = lstart.max(0).min(n);
    let count = lcount.max(-1).min(n);
    let mut end = if count < 0 { n - 1 } else { start + count - 1 };
    if start >= n {
        start = n - 1;
        end = n - 2;
    }
    if end >= n {
        end = n - 1;
    }

    if dontsort {
        // Native order — list and sorted-set order, set order for a set —
        // reversed by DESC, as Redis iterates from the other end.
        if spec.desc && kind != "set" {
            elems.reverse();
        }
    } else {
        // Each element with what it sorts by: a number, or bytes for ALPHA
        // (`None` when a BY lookup found nothing, which sorts first).
        let mut keyed: Vec<(Vec<u8>, f64, Option<Vec<u8>>)> = Vec::with_capacity(elems.len());
        let mut bad = false;
        for e in elems {
            let byval = match &by {
                Some(p) => lookup_by_pattern(exec, db, p, &e),
                None => Some(e.clone()),
            };
            let (score, cmp) = match (alpha, byval) {
                (true, v) => (0.0, if by.is_some() { v } else { None }),
                (false, None) => (0.0, None),
                (false, Some(v)) => match sort_score(&v) {
                    Some(f) => (f, None),
                    None => {
                        bad = true;
                        (0.0, None)
                    }
                },
            };
            keyed.push((e, score, cmp));
        }
        if bad {
            return Err(Response::Error(
                "ERR One or more scores can't be converted into double".to_string(),
            ));
        }
        let by_pattern = by.is_some();
        keyed.sort_by(|x, y| {
            use std::cmp::Ordering;
            let c = if !alpha {
                x.1.partial_cmp(&y.1)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| x.0.cmp(&y.0))
            } else if by_pattern {
                match (&x.2, &y.2) {
                    (None, None) => Ordering::Equal,
                    (None, _) => Ordering::Less,
                    (_, None) => Ordering::Greater,
                    (Some(a), Some(b)) => a.cmp(b),
                }
            } else {
                x.0.cmp(&y.0)
            };
            if spec.desc { c.reverse() } else { c }
        });
        elems = keyed.into_iter().map(|k| k.0).collect();
    }

    let selected: Vec<Vec<u8>> = if end >= start && start >= 0 {
        elems[start as usize..=end as usize].to_vec()
    } else {
        vec![]
    };
    let mut out: Vec<Option<Vec<u8>>> = Vec::new();
    for e in &selected {
        if spec.get.is_empty() {
            out.push(Some(e.clone()));
        }
        for g in &spec.get {
            out.push(lookup_by_pattern(exec, db, g, e));
        }
    }

    match &spec.store {
        None => Ok(Response::Array(out)),
        Some(dest) => {
            ok(call(exec, db, &[b"DEL", dest]))?;
            if !out.is_empty() {
                let mut argv: Vec<&[u8]> = vec![b"RPUSH", dest];
                argv.extend(out.iter().map(|v| v.as_deref().unwrap_or(b"")));
                ok(call(exec, db, &argv))?;
            }
            Ok(Response::Integer(out.len() as i64))
        }
    }
}

// ───────────────────────────── HSCAN/SSCAN/ZSCAN ─────────────────────────────

fn parse_collection_scan(name: &str, args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 2 {
        return Err(arity(name));
    }
    std::str::from_utf8(&args[1])
        .ok()
        .and_then(|c| c.parse::<u64>().ok())
        .ok_or_else(|| "ERR invalid cursor".to_string())?;
    let mut pattern = None;
    let mut i = 2;
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err(SYNTAX.to_string());
        }
        if is(&args[i], "MATCH") {
            pattern = Some(args[i + 1].clone());
        } else if is(&args[i], "COUNT") {
            if int(&args[i + 1])? < 1 {
                return Err(SYNTAX.to_string());
            }
        } else {
            return Err(SYNTAX.to_string());
        }
        i += 2;
    }
    Ok(Op::CollectionScan {
        kind: match name {
            "HSCAN" => ScanKind::Hash,
            "SSCAN" => ScanKind::Set,
            _ => ScanKind::Zset,
        },
        key: args[0].clone(),
        pattern,
    })
}

/// One pass over the whole collection, cursor 0 — the same guarantee `SCAN`
/// gives here: every element present for the whole scan is returned.
fn run_collection_scan(
    exec: &mut Runner,
    db: u8,
    kind: ScanKind,
    key: &[u8],
    pattern: Option<&[u8]>,
) -> Result<Response, Response> {
    let (flat, stride) = match kind {
        ScanKind::Hash => (items(ok(call(exec, db, &[b"HGETALL", key]))?), 2),
        ScanKind::Set => (items(ok(call(exec, db, &[b"SMEMBERS", key]))?), 1),
        ScanKind::Zset => (
            items(ok(call(
                exec,
                db,
                &[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"],
            ))?),
            2,
        ),
    };
    let mut out = Vec::new();
    for chunk in flat.chunks(stride) {
        if pattern.is_none_or(|p| crate::pubsub::glob_match(p, &chunk[0])) {
            out.extend(chunk.iter().cloned().map(Value::Bulk));
        }
    }
    Ok(Response::Value(Value::Array(vec![
        Value::str("0"),
        Value::Array(out),
    ])))
}

// ──────────────────────────────── BITFIELD ───────────────────────────────────

fn parse_bitfield(readonly: bool, args: &[Vec<u8>]) -> Result<Op, String> {
    let name = if readonly { "BITFIELD_RO" } else { "BITFIELD" };
    if args.is_empty() {
        return Err(arity(name));
    }
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut j = 1;
    while j < args.len() {
        let rem = args.len() - j - 1;
        let sub = &args[j];
        let kind = if is(sub, "GET") && rem >= 2 {
            BitFieldKind::Get
        } else if is(sub, "SET") && rem >= 3 {
            BitFieldKind::Set
        } else if is(sub, "INCRBY") && rem >= 3 {
            BitFieldKind::IncrBy
        } else if is(sub, "OVERFLOW") && rem >= 1 {
            overflow = match String::from_utf8_lossy(&args[j + 1])
                .to_lowercase()
                .as_str()
            {
                "wrap" => Overflow::Wrap,
                "sat" => Overflow::Sat,
                "fail" => Overflow::Fail,
                _ => return Err("ERR Invalid OVERFLOW type specified".to_string()),
            };
            j += 2;
            continue;
        } else {
            return Err(SYNTAX.to_string());
        };
        let (signed, bits) = bitfield_type(&args[j + 1])?;
        let offset = bitfield_offset(&args[j + 2], bits)?;
        let value = if kind == BitFieldKind::Get {
            0
        } else {
            int(&args[j + 3])?
        };
        ops.push(BitFieldOp {
            kind,
            signed,
            bits,
            offset,
            value,
            overflow,
        });
        j += if kind == BitFieldKind::Get { 3 } else { 4 };
    }
    if readonly && ops.iter().any(|o| o.kind != BitFieldKind::Get) {
        return Err("ERR BITFIELD_RO only supports the GET subcommand".to_string());
    }
    Ok(Op::BitField {
        key: args[0].clone(),
        ops,
    })
}

fn bitfield_type(arg: &[u8]) -> Result<(bool, u32), String> {
    let err = || {
        "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
            .to_string()
    };
    let signed = match arg.first() {
        Some(b'i') => true,
        Some(b'u') => false,
        _ => return Err(err()),
    };
    let bits: i64 = std::str::from_utf8(&arg[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(err)?;
    if bits < 1 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(err());
    }
    Ok((signed, bits as u32))
}

/// A bit offset, or `#N` meaning the N-th field of this width.
fn bitfield_offset(arg: &[u8], bits: u32) -> Result<u64, String> {
    let err = || "ERR bit offset is not an integer or out of range".to_string();
    let (hash, digits) = match arg.first() {
        Some(b'#') => (true, &arg[1..]),
        _ => (false, arg),
    };
    let mut off: i64 = std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(err)?;
    if hash {
        off = off.checked_mul(bits as i64).ok_or_else(err)?;
    }
    if off < 0 || (off >> 3) >= 512 * 1024 * 1024 {
        return Err(err());
    }
    Ok(off as u64)
}

fn get_unsigned(p: &[u8], mut offset: u64, bits: u32) -> u64 {
    let mut value = 0u64;
    for _ in 0..bits {
        let byte = (offset >> 3) as usize;
        let bit = 7 - (offset & 7);
        let bitval = p.get(byte).map_or(0, |b| (b >> bit) & 1) as u64;
        value = (value << 1) | bitval;
        offset += 1;
    }
    value
}

fn get_signed(p: &[u8], offset: u64, bits: u32) -> i64 {
    let mut value = get_unsigned(p, offset, bits);
    if bits < 64 && value & (1u64 << (bits - 1)) != 0 {
        value |= u64::MAX << bits;
    }
    value as i64
}

fn set_unsigned(p: &mut [u8], mut offset: u64, bits: u32, value: u64) {
    for j in 0..bits {
        let bitval = (value >> (bits - 1 - j)) & 1;
        let byte = (offset >> 3) as usize;
        let bit = 7 - (offset & 7);
        p[byte] = (p[byte] & !(1 << bit)) | ((bitval as u8) << bit);
        offset += 1;
    }
}

/// Redis's `checkUnsignedBitfieldOverflow`: whether `value + incr` leaves the
/// field, and what the overflow policy makes of it.
fn unsigned_overflow(value: u64, incr: i64, bits: u32, ow: Overflow) -> (bool, u64) {
    let max = if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    let maxincr = max.wrapping_sub(value) as i64;
    let minincr = (value as i64).wrapping_neg();
    let wrap = || value.wrapping_add(incr as u64) & !(u64::MAX.checked_shl(bits).unwrap_or(0));
    if value > max || (incr > 0 && incr > maxincr) {
        return (true, if ow == Overflow::Wrap { wrap() } else { max });
    }
    if incr < 0 && incr < minincr {
        return (true, if ow == Overflow::Wrap { wrap() } else { 0 });
    }
    (false, 0)
}

/// Redis's `checkSignedBitfieldOverflow`.
fn signed_overflow(value: i64, incr: i64, bits: u32, ow: Overflow) -> (bool, i64) {
    let max = if bits == 64 {
        i64::MAX
    } else {
        (1i64 << (bits - 1)) - 1
    };
    let min = -max - 1;
    let maxincr = (max as u64).wrapping_sub(value as u64) as i64;
    let minincr = min.wrapping_sub(value);
    let wrap = || {
        let mut c = (value as u64).wrapping_add(incr as u64);
        if bits < 64 {
            let mask = u64::MAX << bits;
            if c & (1u64 << (bits - 1)) != 0 {
                c |= mask;
            } else {
                c &= !mask;
            }
        }
        c as i64
    };
    if value > max || (bits != 64 && incr > maxincr) || (value >= 0 && incr > 0 && incr > maxincr) {
        return (true, if ow == Overflow::Wrap { wrap() } else { max });
    }
    if value < min || (bits != 64 && incr < minincr) || (value < 0 && incr < 0 && incr < minincr) {
        return (true, if ow == Overflow::Wrap { wrap() } else { min });
    }
    (false, 0)
}

fn run_bitfield(
    exec: &mut Runner,
    db: u8,
    key: &[u8],
    ops: &[BitFieldOp],
) -> Result<Response, Response> {
    let original = get_string(exec, db, key)?;
    let mut buf = original.clone().unwrap_or_default();
    // Every write grows the string to the farthest bit any write reaches,
    // before any of them runs — even one an OVERFLOW FAIL then declines.
    let highest = ops
        .iter()
        .filter(|o| o.kind != BitFieldKind::Get)
        .map(|o| o.offset + o.bits as u64 - 1)
        .max();
    let mut changed = false;
    if let Some(h) = highest {
        let need = (h >> 3) as usize + 1;
        if buf.len() < need {
            buf.resize(need, 0);
            changed = true;
        }
    }
    let mut out = Vec::with_capacity(ops.len());
    for op in ops {
        match (op.kind, op.signed) {
            (BitFieldKind::Get, true) => {
                out.push(Value::Int(get_signed(&buf, op.offset, op.bits)));
            }
            (BitFieldKind::Get, false) => {
                out.push(Value::Int(get_unsigned(&buf, op.offset, op.bits) as i64));
            }
            (kind, true) => {
                let old = get_signed(&buf, op.offset, op.bits);
                let (overflow, new, ret) = if kind == BitFieldKind::IncrBy {
                    let (o, w) = signed_overflow(old, op.value, op.bits, op.overflow);
                    let n = if o { w } else { old.wrapping_add(op.value) };
                    (o, n, n)
                } else {
                    let (o, w) = signed_overflow(op.value, 0, op.bits, op.overflow);
                    (o, if o { w } else { op.value }, old)
                };
                if overflow && op.overflow == Overflow::Fail {
                    out.push(Value::Nil);
                } else {
                    out.push(Value::Int(ret));
                    set_unsigned(&mut buf, op.offset, op.bits, new as u64);
                    changed |= old != new;
                }
            }
            (kind, false) => {
                let old = get_unsigned(&buf, op.offset, op.bits);
                let (overflow, new, ret) = if kind == BitFieldKind::IncrBy {
                    let (o, w) = unsigned_overflow(old, op.value, op.bits, op.overflow);
                    let n = if o {
                        w
                    } else {
                        old.wrapping_add(op.value as u64)
                    };
                    (o, n, n)
                } else {
                    let v = op.value as u64;
                    let (o, w) = unsigned_overflow(v, 0, op.bits, op.overflow);
                    (o, if o { w } else { v }, old)
                };
                if overflow && op.overflow == Overflow::Fail {
                    out.push(Value::Nil);
                } else {
                    out.push(Value::Int(ret as i64));
                    set_unsigned(&mut buf, op.offset, op.bits, new);
                    changed |= old != new;
                }
            }
        }
    }
    if changed {
        ok(call(exec, db, &[b"SET", key, &buf, b"KEEPTTL"]))?;
    }
    Ok(Response::Value(Value::Array(out)))
}

// ───────────────────────────────── SWAPDB ────────────────────────────────────

fn parse_swapdb(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() != 2 {
        return Err(arity("SWAPDB"));
    }
    let a = int(&args[0]).map_err(|_| "ERR invalid first DB index".to_string())?;
    let b = int(&args[1]).map_err(|_| "ERR invalid second DB index".to_string())?;
    let range = 0..crate::commands::NUM_DBS as i64;
    if !range.contains(&a) || !range.contains(&b) {
        return Err("ERR DB index is out of range".to_string());
    }
    Ok(Op::SwapDb {
        a: a as u8,
        b: b as u8,
    })
}

/// `(key, DUMP payload, TTL in ms or 0)` for each key of a database.
type DumpedKeys = Vec<(Vec<u8>, Vec<u8>, i64)>;

/// Every key in a database, as `(key, DUMP payload, TTL in ms or 0)`.
fn dump_db(exec: &mut Runner, db: u8) -> Result<DumpedKeys, Response> {
    let mut out = Vec::new();
    for key in items(ok(call(exec, db, &[b"KEYS", b"*"]))?) {
        let Response::BulkString(payload) = ok(call(exec, db, &[b"DUMP", &key]))? else {
            continue; // expired between KEYS and DUMP
        };
        let ttl = int_reply(&ok(call(exec, db, &[b"PTTL", &key]))?).max(0);
        out.push((key, payload, ttl));
    }
    Ok(out)
}

fn restore_db(exec: &mut Runner, db: u8, keys: &[(Vec<u8>, Vec<u8>, i64)]) -> Result<(), Response> {
    for (key, payload, ttl) in keys {
        let ttl = ttl.to_string();
        ok(call(
            exec,
            db,
            &[b"RESTORE", key, ttl.as_bytes(), payload, b"REPLACE"],
        ))?;
    }
    Ok(())
}

/// Swap two databases by moving every key through `DUMP`/`RESTORE`, which
/// works whichever half each database is on. The memory half's limits can
/// refuse a key the durable half held; if any restore fails, both databases
/// are put back as they were and the refusal is the reply.
fn run_swapdb(exec: &mut Runner, a: u8, b: u8) -> Result<Response, Response> {
    if a == b {
        return Ok(Response::Ok);
    }
    let in_a = dump_db(exec, a)?;
    let in_b = dump_db(exec, b)?;
    ok(call(exec, a, &[b"FLUSHDB"]))?;
    ok(call(exec, b, &[b"FLUSHDB"]))?;
    let swapped = restore_db(exec, a, &in_b).and_then(|_| restore_db(exec, b, &in_a));
    if let Err(refusal) = swapped {
        ok(call(exec, a, &[b"FLUSHDB"]))?;
        ok(call(exec, b, &[b"FLUSHDB"]))?;
        restore_db(exec, a, &in_a)?;
        restore_db(exec, b, &in_b)?;
        return Err(refusal);
    }
    Ok(Response::Ok)
}

// ─────────────────────────────── HyperLogLog ─────────────────────────────────

/// A key's HyperLogLog: `None` when there is no key, an error when it holds
/// another type or a string that is not an HLL.
fn get_hll(exec: &mut Runner, db: u8, key: &[u8]) -> Result<Option<Vec<u8>>, Response> {
    match get_string(exec, db, key)? {
        Some(v) if !crate::hll::is_valid(&v) => {
            Err(Response::Error(crate::hll::NOT_HLL.to_string()))
        }
        other => Ok(other),
    }
}

/// HLL writes keep the key's TTL: in Redis they modify the string in place.
fn put_hll(exec: &mut Runner, db: u8, key: &[u8], v: &[u8]) -> Result<(), Response> {
    ok(call(exec, db, &[b"SET", key, v, b"KEEPTTL"])).map(|_| ())
}

fn corrupt(_: crate::hll::Corrupt) -> Response {
    Response::Error(crate::hll::INVALID.to_string())
}

fn sparse_max() -> usize {
    crate::hll::DEFAULT_SPARSE_MAX_BYTES
}

fn run_pfadd(
    exec: &mut Runner,
    db: u8,
    key: &[u8],
    elements: &[Vec<u8>],
) -> Result<Response, Response> {
    let (mut v, mut updated) = match get_hll(exec, db, key)? {
        Some(v) => (v, false),
        None => (crate::hll::new_sparse(), true),
    };
    for e in elements {
        updated |= crate::hll::add(&mut v, e, sparse_max()).map_err(corrupt)?;
    }
    if updated {
        crate::hll::invalidate_cache(&mut v);
        put_hll(exec, db, key, &v)?;
    }
    Ok(Response::Integer(updated as i64))
}

fn run_pfcount(exec: &mut Runner, db: u8, keys: &[Vec<u8>]) -> Result<Response, Response> {
    if keys.len() > 1 {
        let mut max = [0u8; crate::hll::REGISTERS];
        for k in keys {
            if let Some(v) = get_hll(exec, db, k)? {
                crate::hll::merge_into(&mut max, &v).map_err(corrupt)?;
            }
        }
        return Ok(Response::Integer(crate::hll::count_raw(&max) as i64));
    }
    let Some(mut v) = get_hll(exec, db, &keys[0])? else {
        return Ok(Response::Integer(0));
    };
    if let Some(card) = crate::hll::cached(&v) {
        return Ok(Response::Integer(card as i64));
    }
    // Recompute and store the cache: Redis writes it back into the value.
    let card = crate::hll::count(&v).map_err(corrupt)?;
    crate::hll::set_cache(&mut v, card);
    put_hll(exec, db, &keys[0], &v)?;
    Ok(Response::Integer(card as i64))
}

/// The destination is one of the inputs, as in Redis's `pfmergeCommand`.
fn run_pfmerge(
    exec: &mut Runner,
    db: u8,
    dest: &[u8],
    sources: &[Vec<u8>],
) -> Result<Response, Response> {
    let mut max = [0u8; crate::hll::REGISTERS];
    let mut use_dense = false;
    let mut existing = None;
    for (i, k) in std::iter::once(&dest.to_vec()).chain(sources).enumerate() {
        if let Some(v) = get_hll(exec, db, k)? {
            use_dense |= !crate::hll::is_sparse(&v);
            crate::hll::merge_into(&mut max, &v).map_err(corrupt)?;
            if i == 0 {
                existing = Some(v);
            }
        }
    }
    let mut v = existing.unwrap_or_else(crate::hll::new_sparse);
    crate::hll::apply_registers(&mut v, &max, use_dense, sparse_max()).map_err(corrupt)?;
    put_hll(exec, db, dest, &v)?;
    Ok(Response::Ok)
}

fn run_pfdebug(exec: &mut Runner, db: u8, sub: &str, key: &[u8]) -> Result<Response, Response> {
    let Some(v) = get_hll(exec, db, key)? else {
        return Err(Response::Error(
            "ERR The specified key does not exist".to_string(),
        ));
    };
    match sub.to_lowercase().as_str() {
        "getreg" => {
            // Redis turns the value dense to read it, and keeps it that way.
            if crate::hll::is_sparse(&v) {
                put_hll(
                    exec,
                    db,
                    key,
                    &crate::hll::sparse_to_dense(&v).map_err(corrupt)?,
                )?;
            }
            let regs = crate::hll::registers(&v).map_err(corrupt)?;
            Ok(Response::IntegerArray(
                regs.into_iter().map(i64::from).collect(),
            ))
        }
        "decode" => {
            if !crate::hll::is_sparse(&v) {
                return Err(Response::Error(
                    "ERR HLL encoding is not sparse".to_string(),
                ));
            }
            Ok(Response::BulkString(crate::hll::decode(&v).into_bytes()))
        }
        "encoding" => Ok(Response::SimpleString(
            if crate::hll::is_sparse(&v) {
                "sparse"
            } else {
                "dense"
            }
            .to_string(),
        )),
        "todense" => {
            if !crate::hll::is_sparse(&v) {
                return Ok(Response::Integer(0));
            }
            put_hll(
                exec,
                db,
                key,
                &crate::hll::sparse_to_dense(&v).map_err(corrupt)?,
            )?;
            Ok(Response::Integer(1))
        }
        _ => Err(Response::Error(format!(
            "ERR Unknown PFDEBUG subcommand '{sub}'"
        ))),
    }
}

// ─────────────────────────────────── GEO ─────────────────────────────────────

/// A double as Redis's `getDoubleFromObject` reads one: no leading space, no
/// NaN, the whole string a number.
fn double(arg: &[u8], err: &str) -> Result<f64, String> {
    let s = std::str::from_utf8(arg).map_err(|_| err.to_string())?;
    if s.is_empty() || s.starts_with(|c: char| c.is_ascii_whitespace()) {
        return Err(err.to_string());
    }
    let v = match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        _ => s.parse::<f64>().map_err(|_| err.to_string())?,
    };
    if v.is_nan() {
        return Err(err.to_string());
    }
    Ok(v)
}

const NOT_FLOAT: &str = "ERR value is not a valid float";

fn lon_lat(lon: &[u8], lat: &[u8]) -> Result<[f64; 2], String> {
    let xy = [double(lon, NOT_FLOAT)?, double(lat, NOT_FLOAT)?];
    if !(crate::geo::LONG_MIN..=crate::geo::LONG_MAX).contains(&xy[0])
        || !(crate::geo::LAT_MIN..=crate::geo::LAT_MAX).contains(&xy[1])
    {
        return Err(format!(
            "ERR invalid longitude,latitude pair {:.6},{:.6}",
            xy[0], xy[1]
        ));
    }
    Ok(xy)
}

fn parse_geoadd(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 4 {
        return Err(arity("GEOADD"));
    }
    let mut i = 1;
    let (mut nx, mut xx) = (false, false);
    while i < args.len() {
        let a = &args[i];
        if is(a, "NX") {
            nx = true;
        } else if is(a, "XX") {
            xx = true;
        } else if !is(a, "CH") {
            break;
        }
        i += 1;
    }
    if !(args.len() - i).is_multiple_of(3) || (nx && xx) {
        return Err(SYNTAX.to_string());
    }
    let mut points = Vec::new();
    for t in args[i..].chunks(3) {
        let [lon, lat] = lon_lat(&t[0], &t[1])?;
        let score = crate::geo::score_of(lon, lat).ok_or_else(|| SYNTAX.to_string())?;
        points.push((score, t[2].clone()));
    }
    Ok(Op::GeoAdd {
        key: args[0].clone(),
        flags: args[1..i].to_vec(),
        points,
    })
}

fn run_geoadd(
    exec: &mut Runner,
    db: u8,
    key: &[u8],
    flags: &[Vec<u8>],
    points: &[(u64, Vec<u8>)],
) -> Result<Response, Response> {
    let scores: Vec<String> = points.iter().map(|(s, _)| s.to_string()).collect();
    let mut argv: Vec<&[u8]> = vec![b"ZADD", key];
    argv.extend(flags.iter().map(Vec::as_slice));
    for ((_, member), score) in points.iter().zip(&scores) {
        argv.push(score.as_bytes());
        argv.push(member);
    }
    ok(call(exec, db, &argv))
}

fn parse_geodist(args: &[Vec<u8>]) -> Result<Op, String> {
    if args.len() < 3 {
        return Err(arity("GEODIST"));
    }
    if args.len() > 4 {
        return Err(SYNTAX.to_string());
    }
    let conversion = match args.get(3) {
        Some(u) => crate::geo::unit(u)?,
        None => 1.0,
    };
    Ok(Op::GeoDist {
        key: args[0].clone(),
        a: args[1].clone(),
        b: args[2].clone(),
        conversion,
    })
}

/// A member's score, `None` when it or the key is missing. ZSCORE gives
/// WRONGTYPE for a key of another type, which is what GEO answers too.
fn zscore(exec: &mut Runner, db: u8, key: &[u8], member: &[u8]) -> Result<Option<f64>, Response> {
    match ok(call(exec, db, &[b"ZSCORE", key, member]))? {
        Response::BulkString(v) => Ok(std::str::from_utf8(&v).ok().and_then(|s| s.parse().ok())),
        _ => Ok(None),
    }
}

fn run_geodist(
    exec: &mut Runner,
    db: u8,
    key: &[u8],
    a: &[u8],
    b: &[u8],
    conversion: f64,
) -> Result<Response, Response> {
    let (Some(sa), Some(sb)) = (zscore(exec, db, key, a)?, zscore(exec, db, key, b)?) else {
        return Ok(Response::Null);
    };
    let (pa, pb) = (crate::geo::position_of(sa), crate::geo::position_of(sb));
    let d = crate::geo::distance(pa[0], pa[1], pb[0], pb[1]) / conversion;
    Ok(Response::BulkString(
        crate::geo::fmt_distance(d).into_bytes(),
    ))
}

fn run_geolookup(
    exec: &mut Runner,
    db: u8,
    hash: bool,
    key: &[u8],
    members: &[Vec<u8>],
) -> Result<Response, Response> {
    // The key's type first: a wrong type is an error even with no members.
    let t = key_type(exec, db, key)?;
    if t != "zset" && t != "none" {
        return Err(Response::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        ));
    }
    let mut out = Vec::with_capacity(members.len());
    for m in members {
        out.push(match zscore(exec, db, key, m)? {
            None if hash => Value::Nil,
            None => Value::NilArray,
            Some(s) if hash => Value::str(&crate::geo::geohash_string(s)),
            Some(s) => {
                let [lon, lat] = crate::geo::position_of(s);
                Value::Array(vec![
                    Value::str(&crate::geo::fmt_coord(lon)),
                    Value::str(&crate::geo::fmt_coord(lat)),
                ])
            }
        });
    }
    Ok(Response::Value(Value::Array(out)))
}

struct GeoHit {
    member: Vec<u8>,
    score: u64,
    dist: f64,
    xy: [f64; 2],
}

/// Redis's `georadiusGeneric`, argument order and error order included.
fn run_geosearch(
    exec: &mut Runner,
    db: u8,
    variant: GeoVariant,
    args: &[Vec<u8>],
) -> Result<Response, Response> {
    let e = |m: String| Response::Error(m);
    let (src, mut store) = match variant {
        GeoVariant::Search { store: true } => (&args[1], Some(args[0].clone())),
        _ => (&args[0], None),
    };
    let t = key_type(exec, db, src)?;
    if t != "zset" && t != "none" {
        return Err(e(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        ));
    }
    let exists = t == "zset";

    let mut center = [0.0, 0.0];
    let mut shape: Option<crate::geo::Shape> = None;
    let mut conversion = 1.0;
    let radius = |r: &[u8], u: &[u8]| -> Result<(f64, f64), String> {
        let d = double(r, "ERR need numeric radius")?;
        if d < 0.0 {
            return Err("ERR radius cannot be negative".to_string());
        }
        Ok((d, crate::geo::unit(u)?))
    };
    let base = match variant {
        GeoVariant::Radius {
            by_member: false, ..
        } => {
            center = lon_lat(&args[1], &args[2]).map_err(e)?;
            let (r, c) = radius(&args[3], &args[4]).map_err(e)?;
            (shape, conversion) = (Some(crate::geo::Shape::Radius(r)), c);
            5
        }
        GeoVariant::Radius {
            by_member: true, ..
        } if !exists => 4,
        GeoVariant::Radius {
            by_member: true, ..
        } => {
            let Some(s) = zscore(exec, db, src, &args[1])? else {
                return Err(e("ERR could not decode requested zset member".to_string()));
            };
            center = crate::geo::position_of(s);
            let (r, c) = radius(&args[2], &args[3]).map_err(e)?;
            (shape, conversion) = (Some(crate::geo::Shape::Radius(r)), c);
            4
        }
        GeoVariant::Search { store: s } => {
            if s {
                2
            } else {
                1
            }
        }
    };
    let is_search = matches!(variant, GeoVariant::Search { .. });
    let search_store = variant == GeoVariant::Search { store: true };
    let no_store = matches!(variant, GeoVariant::Radius { readonly: true, .. }) || is_search;

    let (mut withdist, mut withhash, mut withcoord, mut any) = (false, false, false, false);
    let (mut frommember, mut fromloc, mut byradius, mut bybox) = (false, false, false, false);
    let mut sort: Option<bool> = None; // Some(true) = ASC
    let mut storedist = false;
    let mut count: i64 = 0;
    let opts = &args[base..];
    let mut i = 0;
    while i < opts.len() {
        let a = &opts[i];
        let rem = opts.len() - i - 1;
        if is(a, "WITHDIST") {
            withdist = true;
        } else if is(a, "WITHHASH") {
            withhash = true;
        } else if is(a, "WITHCOORD") {
            withcoord = true;
        } else if is(a, "ANY") {
            any = true;
        } else if is(a, "ASC") {
            sort = Some(true);
        } else if is(a, "DESC") {
            sort = Some(false);
        } else if is(a, "COUNT") && rem >= 1 {
            count = int(&opts[i + 1]).map_err(e)?;
            if count <= 0 {
                return Err(e("ERR COUNT must be > 0".to_string()));
            }
            i += 1;
        } else if (is(a, "STORE") || is(a, "STOREDIST")) && rem >= 1 && !no_store {
            store = Some(opts[i + 1].clone());
            storedist = is(a, "STOREDIST");
            i += 1;
        } else if is(a, "STOREDIST") && search_store {
            storedist = true;
        } else if is(a, "FROMMEMBER") && rem >= 1 && is_search && !fromloc {
            if exists {
                let Some(s) = zscore(exec, db, src, &opts[i + 1])? else {
                    return Err(e("ERR could not decode requested zset member".to_string()));
                };
                center = crate::geo::position_of(s);
            }
            frommember = true;
            i += 1;
        } else if is(a, "FROMLONLAT") && rem >= 2 && is_search && !frommember {
            center = lon_lat(&opts[i + 1], &opts[i + 2]).map_err(e)?;
            fromloc = true;
            i += 2;
        } else if is(a, "BYRADIUS") && rem >= 2 && is_search && !bybox {
            let (r, c) = radius(&opts[i + 1], &opts[i + 2]).map_err(e)?;
            (shape, conversion) = (Some(crate::geo::Shape::Radius(r)), c);
            byradius = true;
            i += 2;
        } else if is(a, "BYBOX") && rem >= 3 && is_search && !byradius {
            let w = double(&opts[i + 1], "ERR need numeric width").map_err(e)?;
            let h = double(&opts[i + 2], "ERR need numeric height").map_err(e)?;
            if w < 0.0 || h < 0.0 {
                return Err(e("ERR height or width cannot be negative".to_string()));
            }
            conversion = crate::geo::unit(&opts[i + 3]).map_err(e)?;
            shape = Some(crate::geo::Shape::Box(w, h));
            bybox = true;
            i += 3;
        } else {
            return Err(e(SYNTAX.to_string()));
        }
        i += 1;
    }
    let name = match variant {
        GeoVariant::Search { store: true } => "geosearchstore",
        GeoVariant::Search { store: false } => "geosearch",
        _ => "georadius",
    };
    if store.is_some() && (withdist || withhash || withcoord) {
        return Err(e(format!(
            "ERR {} is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            if search_store {
                "GEOSEARCHSTORE"
            } else {
                "STORE option in GEORADIUS"
            }
        )));
    }
    if is_search && !(frommember || fromloc) {
        return Err(e(format!(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {name}"
        )));
    }
    if is_search && !(byradius || bybox) {
        return Err(e(format!(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for {name}"
        )));
    }
    if any && count == 0 {
        return Err(e("ERR the ANY argument requires COUNT argument".to_string()));
    }
    if !exists {
        return Ok(match store {
            Some(k) => {
                ok(call(exec, db, &[b"DEL", &k]))?;
                Response::Integer(0)
            }
            None => Response::Value(Value::Array(vec![])),
        });
    }
    if count != 0 && sort.is_none() && !any {
        sort = Some(true);
    }

    let search = crate::geo::Search {
        center,
        shape: shape.expect("every variant that reaches here has a shape"),
        conversion,
    };
    let limit = if any { count as usize } else { 0 };
    let mut hits: Vec<GeoHit> = Vec::new();
    'cells: for (min, max) in search.score_ranges() {
        if limit > 0 && hits.len() >= limit {
            break;
        }
        let (lo, hi) = (min.to_string(), format!("({max}"));
        let pairs = items(ok(call(
            exec,
            db,
            &[
                b"ZRANGEBYSCORE",
                src,
                lo.as_bytes(),
                hi.as_bytes(),
                b"WITHSCORES",
            ],
        ))?);
        for pair in pairs.chunks(2) {
            let score: f64 = std::str::from_utf8(&pair[1])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let xy = crate::geo::position_of(score);
            if let Some(dist) = search.within(xy) {
                hits.push(GeoHit {
                    member: pair[0].clone(),
                    score: score as u64,
                    dist,
                    xy,
                });
            }
            if limit > 0 && hits.len() >= limit {
                break 'cells;
            }
        }
    }
    if let Some(asc) = sort {
        hits.sort_by(|a, b| {
            let o = a
                .dist
                .partial_cmp(&b.dist)
                .unwrap_or(std::cmp::Ordering::Equal);
            if asc { o } else { o.reverse() }
        });
    }
    if count > 0 {
        hits.truncate(count as usize);
    }

    if let Some(dest) = store {
        ok(call(exec, db, &[b"DEL", &dest]))?;
        if !hits.is_empty() {
            let scores: Vec<String> = hits
                .iter()
                .map(|h| {
                    if storedist {
                        format!("{}", h.dist / conversion)
                    } else {
                        h.score.to_string()
                    }
                })
                .collect();
            let mut argv: Vec<&[u8]> = vec![b"ZADD", &dest];
            for (h, s) in hits.iter().zip(&scores) {
                argv.push(s.as_bytes());
                argv.push(&h.member);
            }
            ok(call(exec, db, &argv))?;
        }
        return Ok(Response::Integer(hits.len() as i64));
    }

    let with = withdist || withhash || withcoord;
    let out = hits
        .into_iter()
        .map(|h| {
            if !with {
                return Value::Bulk(h.member);
            }
            let mut item = vec![Value::Bulk(h.member)];
            if withdist {
                item.push(Value::str(&crate::geo::fmt_distance(h.dist / conversion)));
            }
            if withhash {
                item.push(Value::Int(h.score as i64));
            }
            if withcoord {
                item.push(Value::Array(vec![
                    Value::str(&crate::geo::fmt_coord(h.xy[0])),
                    Value::str(&crate::geo::fmt_coord(h.xy[1])),
                ]));
            }
            Value::Array(item)
        })
        .collect();
    Ok(Response::Value(Value::Array(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(r: Response) -> Vec<u8> {
        match r {
            Response::BulkString(v) => v,
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    #[test]
    fn lcs_matches_redis() {
        // The examples from Redis's own LCS documentation.
        let (a, b) = (b"ohmytext".as_slice(), b"mynewtext".as_slice());
        assert_eq!(bulk(lcs(a, b, false, false, 0, false)), b"mytext");
        assert!(matches!(
            lcs(a, b, true, false, 0, false),
            Response::Integer(6)
        ));
        let Response::Value(Value::Map(m)) = lcs(a, b, false, true, 0, true) else {
            panic!("expected a map")
        };
        assert_eq!(
            m[0].1,
            Value::Array(vec![
                Value::Array(vec![
                    Value::Array(vec![Value::Int(4), Value::Int(7)]),
                    Value::Array(vec![Value::Int(5), Value::Int(8)]),
                    Value::Int(4),
                ]),
                Value::Array(vec![
                    Value::Array(vec![Value::Int(2), Value::Int(3)]),
                    Value::Array(vec![Value::Int(0), Value::Int(1)]),
                    Value::Int(2),
                ]),
            ])
        );
        assert_eq!(m[1].1, Value::Int(6));
    }

    #[test]
    fn bitop_pads_shorter_inputs_with_zeros() {
        let srcs = vec![vec![0xff, 0x0f], vec![0x0f]];
        assert_eq!(bitop(BitOpKind::And, &srcs), vec![0x0f, 0x00]);
        assert_eq!(bitop(BitOpKind::Or, &srcs), vec![0xff, 0x0f]);
        assert_eq!(bitop(BitOpKind::Xor, &srcs), vec![0xf0, 0x0f]);
        assert_eq!(bitop(BitOpKind::Not, &srcs[1..]), vec![0xf0]);
    }

    #[test]
    fn bitpos_follows_redis() {
        // From Redis's BITPOS documentation.
        let v = [0xff, 0xf0, 0x00];
        assert_eq!(bitpos(&v, 0, None, None, false), 12);
        let v = [0x00, 0xff, 0xf0];
        assert_eq!(bitpos(&v, 1, Some(0), None, false), 8);
        assert_eq!(bitpos(&v, 1, Some(2), None, false), 16);
        assert_eq!(bitpos(&v, 1, Some(2), Some(-1), true), 8);
        assert_eq!(bitpos(&v, 1, Some(7), Some(15), true), 8);
        // All ones: a clear bit is past the end, unless an end was given.
        assert_eq!(bitpos(&[0xff], 0, None, None, false), 8);
        assert_eq!(bitpos(&[0xff], 0, Some(0), Some(-1), false), -1);
        assert_eq!(bitpos(&[0x00], 1, None, None, false), -1);
    }

    #[test]
    fn bitfield_arithmetic_follows_redis() {
        // Signed and unsigned reads of the same bits.
        let p = [0b1000_0001u8];
        assert_eq!(get_unsigned(&p, 0, 8), 129);
        assert_eq!(get_signed(&p, 0, 8), -127);
        let mut q = [0u8; 2];
        set_unsigned(&mut q, 4, 8, 0xff);
        assert_eq!(q, [0x0f, 0xf0]);
        // From Redis's bitfield tests: u2 at 100 + 1 wraps to 0, saturates at 3.
        assert_eq!(unsigned_overflow(3, 1, 2, Overflow::Wrap), (true, 0));
        assert_eq!(unsigned_overflow(3, 1, 2, Overflow::Sat), (true, 3));
        assert_eq!(unsigned_overflow(0, -1, 2, Overflow::Sat), (true, 0));
        assert_eq!(signed_overflow(127, 1, 8, Overflow::Wrap), (true, -128));
        assert_eq!(signed_overflow(127, 1, 8, Overflow::Sat), (true, 127));
        assert_eq!(signed_overflow(-128, -1, 8, Overflow::Sat), (true, -128));
        assert_eq!(signed_overflow(1, 1, 8, Overflow::Sat), (false, 0));
    }

    #[test]
    fn bitfield_parses_like_redis() {
        let a = |v: &[&str]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        let err = |v: &[&str], name: &str| match Op::parse(name, &a(v)) {
            Some(Err(e)) => e,
            other => panic!("expected an error, got {other:?}"),
        };
        assert!(err(&["k", "GET", "u64", "0"], "BITFIELD").contains("u64 is not supported"));
        assert!(err(&["k", "GET", "i65", "0"], "BITFIELD").contains("Invalid bitfield type"));
        assert!(err(&["k", "GET", "u8", "-1"], "BITFIELD").contains("bit offset"));
        assert!(err(&["k", "OVERFLOW", "nope"], "BITFIELD").contains("OVERFLOW"));
        assert!(
            err(&["k", "SET", "u8", "0", "1"], "BITFIELD_RO").contains("only supports the GET")
        );
        let Some(Ok(Op::BitField { ops, .. })) = Op::parse(
            "BITFIELD",
            &a(&["k", "SET", "i8", "#2", "1", "GET", "u4", "0"]),
        ) else {
            panic!("expected a bitfield")
        };
        assert_eq!(ops[0].offset, 16);
    }

    #[test]
    fn sort_parses_like_redis() {
        let a = |v: &[&str]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        assert!(matches!(
            Op::parse(
                "SORT",
                &a(&[
                    "k", "BY", "w_*", "LIMIT", "0", "5", "GET", "#", "DESC", "ALPHA", "STORE", "d"
                ])
            ),
            Some(Ok(Op::Sort(_)))
        ));
        assert!(matches!(
            Op::parse("SORT_RO", &a(&["k", "STORE", "d"])),
            Some(Err(_))
        ));
        assert!(matches!(
            Op::parse("SORT", &a(&["k", "LIMIT", "0"])),
            Some(Err(_))
        ));
    }

    #[test]
    fn mpop_parses_like_redis() {
        let a = |v: &[&str]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        let err = |v: &[&str], name: &str| match Op::parse(name, &a(v)) {
            Some(Err(e)) => e,
            other => panic!("expected an error, got {other:?}"),
        };
        assert!(err(&["0", "k", "LEFT"], "LMPOP").contains("numkeys"));
        assert!(err(&["3", "k", "LEFT"], "LMPOP").contains("Number of keys"));
        assert!(err(&["1", "k", "UP"], "LMPOP").contains("syntax"));
        assert!(err(&["1", "k", "LEFT", "COUNT", "0"], "LMPOP").contains("count"));
        assert!(err(&["1", "k", "LEFT"], "ZMPOP").contains("syntax"));
    }
}
