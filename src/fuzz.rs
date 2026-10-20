//! Fuzz harnesses (https://rust-fuzz.github.io/book/), written with bolero so
//! one harness serves two ways:
//!
//! - `cargo test` runs each one as an ordinary test over generated inputs, on
//!   stable, in CI;
//! - `cargo bolero test fuzz::<name>` runs the same harness under libFuzzer
//!   (nightly), coverage-guided, for as long as it is given.
//!
//! Each states a property, not just "does not crash": the protocol reader
//! round-trips what it is sent, the glob matcher agrees with a port of Redis's own, and
//! the memory engine agrees reply for reply with a model of Redis built on the
//! standard collections.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io::Cursor;

use allocator_api2::alloc::Global;
use bolero::{TypeGenerator, check};

use crate::commands::{Command, Value};
use crate::memdb::Db;
use crate::memdb::dict::Seed;
use crate::resp::RespParser;

// ─────────────────────────────── the protocol ───────────────────────────────

/// Arbitrary bytes: every command read is within the protocol's limits, and
/// the reader stops with an error rather than panicking or looping.
#[test]
fn resp_reader_survives_any_input() {
    check!().for_each(|input: &[u8]| {
        let mut parser = RespParser::from_reader(Cursor::new(input));
        // Each command consumes at least one byte, so this many reads ends it.
        for _ in 0..=input.len() {
            match parser.read_command() {
                Ok(parts) => assert!(parts.len() <= 65_536),
                Err(_) => break,
            }
        }
    });
}

/// Any argument list, encoded as a RESP array, reads back as itself.
#[test]
fn resp_reader_round_trips_arrays() {
    check!().with_type::<Vec<Vec<u8>>>().for_each(|parts| {
        if parts.is_empty() {
            return;
        }
        let mut wire = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            wire.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            wire.extend_from_slice(p);
            wire.extend_from_slice(b"\r\n");
        }
        let mut parser = RespParser::from_reader(Cursor::new(wire));
        assert_eq!(&parser.read_command().expect("a well-formed array"), parts);
    });
}

// ──────────────────────────────── commands ──────────────────────────────────

/// Command names the parser knows, so generated lines get past the name.
const NAMES: &[&str] = &[
    "GET",
    "SET",
    "DEL",
    "INCRBY",
    "INCRBYFLOAT",
    "APPEND",
    "GETRANGE",
    "SETRANGE",
    "LPUSH",
    "LRANGE",
    "LINSERT",
    "LPOS",
    "LREM",
    "LTRIM",
    "SADD",
    "SINTERCARD",
    "SRANDMEMBER",
    "HSET",
    "HRANDFIELD",
    "ZADD",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANGEBYLEX",
    "ZUNIONSTORE",
    "ZINTERCARD",
    "XADD",
    "XRANGE",
    "XREADGROUP",
    "XAUTOCLAIM",
    "XTRIM",
    "EXPIRE",
    "SCAN",
    "SORT",
    "BITFIELD",
    "BITCOUNT",
    "BITPOS",
    "GEOADD",
    "GEOSEARCH",
    "PFADD",
    "LCS",
    "OBJECT",
    "CLIENT",
    "CONFIG",
    "EVAL",
    "FUNCTION",
    "BLMPOP",
];

#[derive(Debug, TypeGenerator)]
struct Line {
    name: u8,
    args: Vec<Vec<u8>>,
}

/// Any arguments after a real command name parse, or are refused with an
/// error; the parser never panics.
#[test]
fn command_parser_survives_any_arguments() {
    check!().with_type::<Line>().for_each(|line| {
        let mut parts = vec![NAMES[line.name as usize % NAMES.len()].as_bytes().to_vec()];
        parts.extend(line.args.iter().take(16).cloned());
        let _ = Command::parse(parts);
    });
}

/// The argument parsers shared across commands, on arbitrary bytes.
#[test]
fn bound_and_timeout_parsers_survive_any_input() {
    check!().for_each(|input: &[u8]| {
        let _ = crate::commands::parse_score_value(input);
        let _ = crate::commands::parse_score_bound(input);
        let _ = crate::commands::parse_lex_bound(input);
        let _ = crate::blocking::parse_timeout(input);
        let _ = crate::blocking::parse_timeout_ms(input);
    });
}

// ─────────────────────────────── glob matching ──────────────────────────────

/// Redis 7.2's `stringmatchlen` (util.c, case-sensitive), ported line for
/// line as the reference: recursive and slow, but Redis's own semantics, quirks
/// included — `]` as a range end, ranges given backwards, a class with no `]`
/// running to the end of the pattern, no wildcard matching an empty name.
fn redis_glob(pattern: &[u8], string: &[u8]) -> bool {
    let (mut p, mut s) = (pattern, string);
    while !p.is_empty() && !s.is_empty() {
        let mut advance = true;
        match p[0] {
            b'*' => {
                while p.len() > 1 && p[1] == b'*' {
                    p = &p[1..];
                }
                if p.len() == 1 {
                    return true;
                }
                return (0..s.len()).any(|i| redis_glob(&p[1..], &s[i..]));
            }
            b'?' => s = &s[1..],
            b'[' => {
                p = &p[1..];
                let not = p.first() == Some(&b'^');
                if not {
                    p = &p[1..];
                }
                let mut matched = false;
                loop {
                    if p.len() >= 2 && p[0] == b'\\' {
                        p = &p[1..];
                        matched |= p[0] == s[0];
                    } else if p.first() == Some(&b']') {
                        break;
                    } else if p.is_empty() {
                        // Redis steps back here so its loop's advance lands
                        // on the end of the pattern.
                        advance = false;
                        break;
                    } else if p.len() >= 3 && p[1] == b'-' {
                        let (lo, hi) = if p[0] <= p[2] {
                            (p[0], p[2])
                        } else {
                            (p[2], p[0])
                        };
                        matched |= (lo..=hi).contains(&s[0]);
                        p = &p[2..];
                    } else {
                        matched |= p[0] == s[0];
                    }
                    p = &p[1..];
                }
                if matched == not {
                    return false;
                }
                s = &s[1..];
            }
            b'\\' if p.len() >= 2 => {
                p = &p[1..];
                if p[0] != s[0] {
                    return false;
                }
                s = &s[1..];
            }
            c => {
                if c != s[0] {
                    return false;
                }
                s = &s[1..];
            }
        }
        if advance {
            p = &p[1..];
        }
        if s.is_empty() {
            while p.first() == Some(&b'*') {
                p = &p[1..];
            }
            break;
        }
    }
    p.is_empty() && s.is_empty()
}

/// A pattern and a string over a small alphabet, where `*`, `?` and classes
/// collide often enough to matter.
#[derive(Debug, TypeGenerator)]
struct GlobCase {
    pattern: Vec<u8>,
    string: Vec<u8>,
}

#[test]
fn glob_match_agrees_with_redis() {
    const P: &[u8] = b"ab*?[]^-\\";
    const S: &[u8] = b"ab-]^";
    check!().with_type::<GlobCase>().for_each(|case| {
        let pattern: Vec<u8> = case
            .pattern
            .iter()
            .take(10)
            .map(|b| P[*b as usize % P.len()])
            .collect();
        let string: Vec<u8> = case
            .string
            .iter()
            .take(10)
            .map(|b| S[*b as usize % S.len()])
            .collect();
        assert_eq!(
            crate::pubsub::glob_match(&pattern, &string),
            redis_glob(&pattern, &string),
            "pattern {:?} on {:?}",
            String::from_utf8_lossy(&pattern),
            String::from_utf8_lossy(&string)
        );
    });
}

// ──────────────────── the memory engine against a model ─────────────────────

const KEYS: &[&[u8]] = &[b"a", b"b", b"c"];
const VALS: &[&[u8]] = &[b"", b"0", b"1", b"-1", b"x", b"9223372036854775807", b"10"];

/// One command, its key and arguments drawn from small sets so that commands
/// meet each other's keys, and each other's types, constantly.
#[derive(Debug, TypeGenerator)]
enum Op {
    Set(u8, u8),
    Get(u8),
    Del(u8),
    Exists(u8),
    Type(u8),
    Incr(u8),
    Append(u8, u8),
    LPush(u8, Vec<u8>),
    RPush(u8, Vec<u8>),
    LPop(u8),
    RPop(u8),
    LLen(u8),
    LRange(u8),
    SAdd(u8, Vec<u8>),
    SRem(u8, u8),
    SCard(u8),
    SIsMember(u8, u8),
    HSet(u8, u8, u8),
    HGet(u8, u8),
    HDel(u8, u8),
    HLen(u8),
}

enum Model {
    Str(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
}

fn key(i: u8) -> Vec<u8> {
    KEYS[i as usize % KEYS.len()].to_vec()
}

fn val(i: u8) -> Vec<u8> {
    VALS[i as usize % VALS.len()].to_vec()
}

/// A reply as the model predicts it. Errors compare by class (`ERR`,
/// `WRONGTYPE`), not by wording.
#[derive(Debug, PartialEq)]
enum Reply {
    Nil,
    Int(i64),
    Bulk(Vec<u8>),
    Simple(String),
    Array(Vec<Option<Vec<u8>>>),
    Error(String),
}

fn reply(v: Value) -> Reply {
    match v {
        Value::Nil | Value::NilArray => Reply::Nil,
        Value::Int(n) => Reply::Int(n),
        Value::Bulk(b) => Reply::Bulk(b),
        Value::Simple(s) => Reply::Simple(s),
        Value::Error(e) | Value::CountedError(e) => {
            Reply::Error(e.split(' ').next().unwrap_or("").to_string())
        }
        Value::Array(items) => Reply::Array(
            items
                .into_iter()
                .map(|i| match i {
                    Value::Bulk(b) => Some(b),
                    _ => None,
                })
                .collect(),
        ),
        other => panic!("a reply these commands never give: {other:?}"),
    }
}

struct Redis(HashMap<Vec<u8>, Model>);

impl Redis {
    /// What Redis answers `op` with, applying it.
    fn apply(&mut self, op: &Op) -> (Vec<Vec<u8>>, Reply) {
        let m = &mut self.0;
        let wrong = || Reply::Error("WRONGTYPE".into());
        let argv = |parts: &[&[u8]]| parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>();
        // A collection left empty is no key at all.
        fn prune(m: &mut HashMap<Vec<u8>, Model>, k: &[u8]) {
            let empty = match m.get(k) {
                Some(Model::List(l)) => l.is_empty(),
                Some(Model::Set(s)) => s.is_empty(),
                Some(Model::Hash(h)) => h.is_empty(),
                _ => false,
            };
            if empty {
                m.remove(k);
            }
        }
        match op {
            Op::Set(k, v) => {
                let (k, v) = (key(*k), val(*v));
                m.insert(k.clone(), Model::Str(v.clone()));
                (argv(&[b"SET", &k, &v]), Reply::Simple("OK".into()))
            }
            Op::Get(k) => {
                let k = key(*k);
                let r = match m.get(&k) {
                    None => Reply::Nil,
                    Some(Model::Str(v)) => Reply::Bulk(v.clone()),
                    Some(_) => wrong(),
                };
                (argv(&[b"GET", &k]), r)
            }
            Op::Del(k) => {
                let k = key(*k);
                (
                    argv(&[b"DEL", &k]),
                    Reply::Int(m.remove(&k).is_some() as i64),
                )
            }
            Op::Exists(k) => {
                let k = key(*k);
                (
                    argv(&[b"EXISTS", &k]),
                    Reply::Int(m.contains_key(&k) as i64),
                )
            }
            Op::Type(k) => {
                let k = key(*k);
                let t = match m.get(&k) {
                    None => "none",
                    Some(Model::Str(_)) => "string",
                    Some(Model::List(_)) => "list",
                    Some(Model::Set(_)) => "set",
                    Some(Model::Hash(_)) => "hash",
                };
                (argv(&[b"TYPE", &k]), Reply::Simple(t.into()))
            }
            Op::Incr(k) => {
                let k = key(*k);
                let r = match m.get(&k) {
                    None => {
                        m.insert(k.clone(), Model::Str(b"1".to_vec()));
                        Reply::Int(1)
                    }
                    Some(Model::Str(v)) => {
                        let parsed = std::str::from_utf8(v)
                            .ok()
                            .filter(|t| {
                                !t.is_empty()
                                    && !t.starts_with('+')
                                    && (*t == "0" || !(t.starts_with('0') || t.starts_with("-0")))
                            })
                            .and_then(|t| t.parse::<i64>().ok());
                        match parsed.map(|n| n.checked_add(1)) {
                            None => Reply::Error("ERR".into()),
                            Some(None) => Reply::Error("ERR".into()),
                            Some(Some(n)) => {
                                m.insert(k.clone(), Model::Str(n.to_string().into_bytes()));
                                Reply::Int(n)
                            }
                        }
                    }
                    Some(_) => wrong(),
                };
                (argv(&[b"INCR", &k]), r)
            }
            Op::Append(k, v) => {
                let (k, v) = (key(*k), val(*v));
                let r = match m.get_mut(&k) {
                    None => {
                        m.insert(k.clone(), Model::Str(v.clone()));
                        Reply::Int(v.len() as i64)
                    }
                    Some(Model::Str(s)) => {
                        s.extend_from_slice(&v);
                        Reply::Int(s.len() as i64)
                    }
                    Some(_) => wrong(),
                };
                (argv(&[b"APPEND", &k, &v]), r)
            }
            Op::LPush(k, vs) | Op::RPush(k, vs) => {
                let left = matches!(op, Op::LPush(..));
                let k = key(*k);
                let vs: Vec<Vec<u8>> = vs.iter().take(4).map(|v| val(*v)).collect();
                let name: &[u8] = if left { b"LPUSH" } else { b"RPUSH" };
                let mut a = vec![name.to_vec(), k.clone()];
                a.extend(vs.iter().cloned());
                if vs.is_empty() {
                    return (a, Reply::Error("ERR".into()));
                }
                let r = match m.entry(k).or_insert_with(|| Model::List(VecDeque::new())) {
                    Model::List(l) => {
                        for v in vs {
                            if left {
                                l.push_front(v)
                            } else {
                                l.push_back(v)
                            }
                        }
                        Reply::Int(l.len() as i64)
                    }
                    _ => wrong(),
                };
                (a, r)
            }
            Op::LPop(k) | Op::RPop(k) => {
                let left = matches!(op, Op::LPop(..));
                let k = key(*k);
                let name: &[u8] = if left { b"LPOP" } else { b"RPOP" };
                let r = match m.get_mut(&k) {
                    None => Reply::Nil,
                    Some(Model::List(l)) => {
                        let v = if left { l.pop_front() } else { l.pop_back() };
                        v.map_or(Reply::Nil, Reply::Bulk)
                    }
                    Some(_) => wrong(),
                };
                prune(m, &k);
                (argv(&[name, &k]), r)
            }
            Op::LLen(k) => {
                let k = key(*k);
                let r = match m.get(&k) {
                    None => Reply::Int(0),
                    Some(Model::List(l)) => Reply::Int(l.len() as i64),
                    Some(_) => wrong(),
                };
                (argv(&[b"LLEN", &k]), r)
            }
            Op::LRange(k) => {
                let k = key(*k);
                let r = match m.get(&k) {
                    None => Reply::Array(vec![]),
                    Some(Model::List(l)) => Reply::Array(l.iter().cloned().map(Some).collect()),
                    Some(_) => wrong(),
                };
                (argv(&[b"LRANGE", &k, b"0", b"-1"]), r)
            }
            Op::SAdd(k, ms) => {
                let k = key(*k);
                let ms: Vec<Vec<u8>> = ms.iter().take(4).map(|v| val(*v)).collect();
                let mut a = vec![b"SADD".to_vec(), k.clone()];
                a.extend(ms.iter().cloned());
                if ms.is_empty() {
                    return (a, Reply::Error("ERR".into()));
                }
                let r = match m.entry(k).or_insert_with(|| Model::Set(BTreeSet::new())) {
                    Model::Set(s) => {
                        Reply::Int(ms.into_iter().filter(|x| s.insert(x.clone())).count() as i64)
                    }
                    _ => wrong(),
                };
                (a, r)
            }
            Op::SRem(k, x) => {
                let (k, x) = (key(*k), val(*x));
                let r = match m.get_mut(&k) {
                    None => Reply::Int(0),
                    Some(Model::Set(s)) => Reply::Int(s.remove(&x) as i64),
                    Some(_) => wrong(),
                };
                prune(m, &k);
                (argv(&[b"SREM", &k, &x]), r)
            }
            Op::SCard(k) => {
                let k = key(*k);
                let r = match m.get(&k) {
                    None => Reply::Int(0),
                    Some(Model::Set(s)) => Reply::Int(s.len() as i64),
                    Some(_) => wrong(),
                };
                (argv(&[b"SCARD", &k]), r)
            }
            Op::SIsMember(k, x) => {
                let (k, x) = (key(*k), val(*x));
                let r = match m.get(&k) {
                    None => Reply::Int(0),
                    Some(Model::Set(s)) => Reply::Int(s.contains(&x) as i64),
                    Some(_) => wrong(),
                };
                (argv(&[b"SISMEMBER", &k, &x]), r)
            }
            Op::HSet(k, f, v) => {
                let (k, f, v) = (key(*k), val(*f), val(*v));
                let r = match m
                    .entry(k.clone())
                    .or_insert_with(|| Model::Hash(BTreeMap::new()))
                {
                    Model::Hash(h) => Reply::Int(h.insert(f.clone(), v.clone()).is_none() as i64),
                    _ => wrong(),
                };
                (argv(&[b"HSET", &k, &f, &v]), r)
            }
            Op::HGet(k, f) => {
                let (k, f) = (key(*k), val(*f));
                let r = match m.get(&k) {
                    None => Reply::Nil,
                    Some(Model::Hash(h)) => h.get(&f).cloned().map_or(Reply::Nil, Reply::Bulk),
                    Some(_) => wrong(),
                };
                (argv(&[b"HGET", &k, &f]), r)
            }
            Op::HDel(k, f) => {
                let (k, f) = (key(*k), val(*f));
                let r = match m.get_mut(&k) {
                    None => Reply::Int(0),
                    Some(Model::Hash(h)) => Reply::Int(h.remove(&f).is_some() as i64),
                    Some(_) => wrong(),
                };
                prune(m, &k);
                (argv(&[b"HDEL", &k, &f]), r)
            }
            Op::HLen(k) => {
                let k = key(*k);
                let r = match m.get(&k) {
                    None => Reply::Int(0),
                    Some(Model::Hash(h)) => Reply::Int(h.len() as i64),
                    Some(_) => wrong(),
                };
                (argv(&[b"HLEN", &k]), r)
            }
        }
    }
}

/// Any sequence of these commands gets the same replies from the memory engine
/// as from the model, step by step.
#[test]
fn memdb_agrees_with_a_model_of_redis() {
    check!().with_type::<Vec<Op>>().for_each(|ops| {
        let mut db = Db::new_in(Seed(7, 11), Global);
        let mut redis = Redis(HashMap::new());
        for (step, op) in ops.iter().take(64).enumerate() {
            let (argv, want) = redis.apply(op);
            let got = match Command::parse(argv.clone()) {
                Ok(cmd) => cmd.run_on_db(&mut db).map(|r| reply(r.into_value())),
                // A refusal to parse goes out as `-ERR …`: the writer adds the
                // code these messages are returned without.
                Err(_) => Some(Reply::Error("ERR".into())),
            };
            let shown: Vec<String> = argv
                .iter()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect();
            assert_eq!(got, Some(want), "step {step}: {}", shown.join(" "));
        }
    });
}
