//! The blocking list and sorted-set commands: `BLPOP`, `BRPOP`, `BLMOVE`,
//! `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`.
//!
//! A blocked client waits on its connection thread, never on the dispatcher.
//! Each attempt is an ordinary, non-blocking command run through the
//! dispatcher; between attempts the client watches the WATCH write counters of
//! its keys, which every worker bumps on every write, and tries again when one
//! moves. Waiters on the same key are served oldest first, through a ticket
//! registry in shared memory — Redis's FIFO, across workers.
//!
//! Inside MULTI a blocking command does not block: it runs once, as Redis
//! does, and answers nil when nothing is there.
#![forbid(unsafe_code)]

use crate::commands::{Response, Value};
use crate::composite::Runner;

#[derive(Debug, Clone)]
pub struct BlockOp {
    pub kind: Kind,
    pub keys: Vec<Vec<u8>>,
    /// Absolute deadline in unix ms; 0 waits for ever.
    pub deadline_ms: i64,
}

#[derive(Debug, Clone)]
pub enum Kind {
    /// BLPOP / BRPOP.
    Pop { left: bool },
    /// BZPOPMIN / BZPOPMAX.
    ZPop { min: bool },
    /// BLMOVE / BRPOPLPUSH: `keys[0]` is the source.
    Move {
        dst: Vec<u8>,
        from_left: bool,
        to_left: bool,
    },
    /// BLMPOP / BZMPOP.
    MPop { zset: bool, front: bool, count: i64 },
}

fn arity(name: &str) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    )
}

/// Redis's `getTimeoutFromObjectOrReply`: seconds as a float — hex included,
/// as `strtold` reads it — turned into an absolute deadline in ms.
pub fn parse_timeout(arg: &[u8]) -> Result<i64, String> {
    let s = std::str::from_utf8(arg).map_err(|_| "ERR timeout is not a float or out of range")?;
    let secs: f64 = if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16)
            .map(|v| v as f64)
            .map_err(|_| "ERR timeout is not a float or out of range")?
    } else {
        s.parse::<f64>()
            .ok()
            .filter(|v| !v.is_nan() && !s.starts_with(char::is_whitespace))
            .ok_or("ERR timeout is not a float or out of range")?
    };
    let ms = secs * 1000.0;
    if ms > i64::MAX as f64 {
        return Err("ERR timeout is out of range".into());
    }
    let ms = ms.ceil() as i64;
    if ms < 0 {
        return Err("ERR timeout is negative".into());
    }
    if ms == 0 {
        return Ok(0);
    }
    let now = crate::server::now_ms();
    if ms > i64::MAX - now {
        return Err("ERR timeout is out of range".into());
    }
    Ok(now + ms)
}

/// The same deadline from an integer count of milliseconds, as XREAD's
/// BLOCK takes it.
pub fn parse_timeout_ms(arg: &[u8]) -> Result<i64, String> {
    let ms: i64 = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or("ERR timeout is not an integer or out of range")?;
    if ms < 0 {
        return Err("ERR timeout is negative".into());
    }
    if ms == 0 {
        return Ok(0);
    }
    let now = crate::server::now_ms();
    if ms > i64::MAX - now {
        return Err("ERR timeout is out of range".into());
    }
    Ok(now + ms)
}

fn side(arg: &[u8]) -> Option<bool> {
    if arg.eq_ignore_ascii_case(b"LEFT") {
        Some(true)
    } else if arg.eq_ignore_ascii_case(b"RIGHT") {
        Some(false)
    } else {
        None
    }
}

impl BlockOp {
    pub fn parse(name: &str, args: &[Vec<u8>]) -> Option<Result<BlockOp, String>> {
        let r = match name {
            "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
                if args.len() < 2 {
                    return Some(Err(arity(name)));
                }
                let deadline_ms = match parse_timeout(&args[args.len() - 1]) {
                    Ok(d) => d,
                    Err(e) => return Some(Err(e)),
                };
                let keys = args[..args.len() - 1].to_vec();
                let kind = match name {
                    "BLPOP" => Kind::Pop { left: true },
                    "BRPOP" => Kind::Pop { left: false },
                    "BZPOPMIN" => Kind::ZPop { min: true },
                    _ => Kind::ZPop { min: false },
                };
                Ok(BlockOp {
                    kind,
                    keys,
                    deadline_ms,
                })
            }
            "BRPOPLPUSH" => {
                if args.len() != 3 {
                    return Some(Err(arity(name)));
                }
                parse_timeout(&args[2]).map(|deadline_ms| BlockOp {
                    kind: Kind::Move {
                        dst: args[1].clone(),
                        from_left: false,
                        to_left: true,
                    },
                    keys: vec![args[0].clone()],
                    deadline_ms,
                })
            }
            "BLMOVE" => {
                if args.len() != 5 {
                    return Some(Err(arity(name)));
                }
                let (Some(from_left), Some(to_left)) = (side(&args[2]), side(&args[3])) else {
                    return Some(Err("ERR syntax error".into()));
                };
                parse_timeout(&args[4]).map(|deadline_ms| BlockOp {
                    kind: Kind::Move {
                        dst: args[1].clone(),
                        from_left,
                        to_left,
                    },
                    keys: vec![args[0].clone()],
                    deadline_ms,
                })
            }
            "BLMPOP" | "BZMPOP" => {
                if args.len() < 4 {
                    return Some(Err(arity(name)));
                }
                let deadline_ms = match parse_timeout(&args[0]) {
                    Ok(d) => d,
                    Err(e) => return Some(Err(e)),
                };
                // The rest is LMPOP's / ZMPOP's own syntax.
                let inner = if name == "BLMPOP" { "LMPOP" } else { "ZMPOP" };
                match crate::composite::Op::parse(inner, &args[1..]) {
                    Some(Ok(crate::composite::Op::MPop {
                        zset,
                        keys,
                        front,
                        count,
                    })) => Ok(BlockOp {
                        kind: Kind::MPop { zset, front, count },
                        keys,
                        deadline_ms,
                    }),
                    Some(Err(e)) => Err(e),
                    _ => Err("ERR syntax error".into()),
                }
            }
            _ => return None,
        };
        Some(r)
    }

    /// What answers when the wait ends with nothing: a nil string for the
    /// move commands, a nil array for the rest.
    pub fn timeout_reply(&self) -> Response {
        match self.kind {
            Kind::Move { .. } => Response::Null,
            _ => Response::NullArray,
        }
    }

    /// The keys a write to which may let this client through, and the type
    /// that does.
    fn source_type(&self) -> &'static str {
        match self.kind {
            Kind::ZPop { .. } | Kind::MPop { zset: true, .. } => "zset",
            _ => "list",
        }
    }

    /// One non-blocking try. `None` when nothing is there yet. `first` is the
    /// attempt made as the command arrives, where a source key of the wrong
    /// type is WRONGTYPE; on a later wake-up it just is not ready, as in Redis
    /// a key turning into a string does not wake a blocked client.
    pub fn attempt(&self, db: u8, exec: &mut Runner, first: bool) -> Option<Response> {
        let want = self.source_type();
        for key in &self.keys {
            let mut call = |argv: &[&[u8]]| crate::composite::call(exec, db, argv);
            let t = match call(&[b"TYPE", key]) {
                Response::SimpleString(t) => t,
                _ => "none".into(),
            };
            if t == "none" {
                continue;
            }
            if t != want {
                if first {
                    return Some(Response::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ));
                }
                continue;
            }
            let reply = match &self.kind {
                Kind::Pop { left } => {
                    let r = call(&[if *left { b"LPOP" } else { b"RPOP" }, key]);
                    match r {
                        Response::BulkString(v) => Response::Value(Value::Array(vec![
                            Value::bulk(key.clone()),
                            Value::Bulk(v),
                        ])),
                        Response::Error(_) => r,
                        _ => continue,
                    }
                }
                Kind::ZPop { min } => {
                    let r = call(&[if *min { b"ZPOPMIN" } else { b"ZPOPMAX" }, key]);
                    match r {
                        Response::Array(v) if v.len() == 2 => {
                            let mut out = vec![Value::bulk(key.clone())];
                            out.extend(v.into_iter().map(|x| Value::Bulk(x.unwrap_or_default())));
                            Response::Value(Value::Array(out))
                        }
                        Response::Error(_) => r,
                        _ => continue,
                    }
                }
                Kind::Move {
                    dst,
                    from_left,
                    to_left,
                } => {
                    let side = |l: bool| -> &'static [u8] { if l { b"LEFT" } else { b"RIGHT" } };
                    match call(&[b"LMOVE", key, dst, side(*from_left), side(*to_left)]) {
                        r @ (Response::BulkString(_) | Response::Error(_)) => r,
                        _ => continue,
                    }
                }
                Kind::MPop { zset, front, count } => {
                    match crate::composite::run_mpop(
                        exec,
                        db,
                        *zset,
                        std::slice::from_ref(key),
                        *front,
                        *count,
                    ) {
                        Ok(Response::NullArray) => continue,
                        Ok(r) => r,
                        Err(e) => e,
                    }
                }
            };
            return Some(reply);
        }
        None
    }

    /// Inside MULTI, or anywhere it may not wait: one try, then nil.
    pub fn run_once(&self, db: u8, exec: &mut Runner) -> Response {
        self.attempt(db, exec, true)
            .unwrap_or_else(|| self.timeout_reply())
    }

    pub fn write_targets(&self, db: u8) -> Vec<(u8, &[u8])> {
        let mut v: Vec<(u8, &[u8])> = self.keys.iter().map(|k| (db, k.as_slice())).collect();
        if let Kind::Move { dst, .. } = &self.kind {
            v.push((db, dst));
        }
        v
    }
}

/// A 64-bit hash of a key in a database, the identity the waiter registry
/// compares — wide enough that two keys sharing one would be a curiosity.
pub fn key_id(db: u8, key: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in std::iter::once(db).chain(key.iter().copied()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash | 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeouts_parse_like_redis() {
        assert_eq!(parse_timeout(b"0"), Ok(0));
        assert!(parse_timeout(b"0.1").unwrap() > 0);
        assert!(parse_timeout(b"-1").unwrap_err().contains("is negative"));
        assert!(
            parse_timeout(b"0x7FFFFFFFFFFFFF")
                .unwrap_err()
                .contains("is out of range")
        );
        assert!(parse_timeout(b"abc").unwrap_err().contains("not a float"));
    }

    #[test]
    fn blocking_commands_parse_like_redis() {
        let a = |v: &[&str]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        assert!(
            matches!(BlockOp::parse("BLPOP", &a(&["k1", "k2", "0"])), Some(Ok(BlockOp { ref keys, .. })) if keys.len() == 2)
        );
        assert!(matches!(
            BlockOp::parse("BLMOVE", &a(&["s", "d", "UP", "LEFT", "0"])),
            Some(Err(_))
        ));
        assert!(matches!(
            BlockOp::parse("BLMPOP", &a(&["0", "2", "a", "b", "LEFT", "COUNT", "3"])),
            Some(Ok(BlockOp {
                kind: Kind::MPop { count: 3, .. },
                ..
            }))
        ));
        assert!(matches!(
            BlockOp::parse("BZMPOP", &a(&["0", "1", "z", "LEFT"])),
            Some(Err(_))
        ));
    }
}
