//! RESP3 replies.
//!
//! RESP3 changes how a reply is framed, not what it holds: `HGETALL`'s flat
//! field/value list becomes a map, `ZSCORE`'s bulk string a double, `SMEMBERS`'
//! array a set, every nil a null. So the commands keep answering in the one
//! shape they always have, and [`shape`] reframes a reply for a client that
//! negotiated protocol 3 — command by command, as Redis 7.2's own reply code
//! does. Anything not named here is the same in both protocols.
#![forbid(unsafe_code)]

use crate::commands::Value;

fn upper(a: &[u8]) -> String {
    String::from_utf8_lossy(a).to_ascii_uppercase()
}

fn has(argv: &[Vec<u8>], word: &str) -> bool {
    argv.iter()
        .skip(1)
        .any(|a| a.eq_ignore_ascii_case(word.as_bytes()))
}

/// A score as RESP3 carries it. A reply already a double, or not a score at
/// all, passes through.
fn double(v: Value) -> Value {
    match v {
        Value::Bulk(b) => match crate::commands::parse_score_value(&b)
            .or_else(|| std::str::from_utf8(&b).ok()?.parse().ok())
        {
            Some(d) => Value::Double(d),
            None => Value::Bulk(b),
        },
        v => v,
    }
}

/// A flat `member, score, member, score` list as `[member, score]` pairs.
fn scored_pairs(v: Value) -> Value {
    match v {
        Value::Array(items) => Value::Array(
            items
                .chunks(2)
                .map(|p| match p {
                    [m, s] => Value::Array(vec![m.clone(), double(s.clone())]),
                    other => Value::Array(other.to_vec()),
                })
                .collect(),
        ),
        v => v,
    }
}

/// A flat `key, value, key, value` list as a map.
fn flat_map(v: Value) -> Value {
    match v {
        Value::Array(items) => Value::Map(
            items
                .chunks(2)
                .map(|p| match p {
                    [k, v] => (k.clone(), v.clone()),
                    [k] => (k.clone(), Value::Nil),
                    _ => unreachable!("chunks(2) yields one or two"),
                })
                .collect(),
        ),
        v => v,
    }
}

fn set(v: Value) -> Value {
    match v {
        Value::Array(items) => Value::Set(items),
        v => v,
    }
}

fn verbatim(v: Value) -> Value {
    match v {
        Value::Bulk(b) => Value::Verbatim(String::from_utf8_lossy(&b).into_owned()),
        v => v,
    }
}

fn map_items(v: Value, f: impl Fn(Value) -> Value) -> Value {
    match v {
        Value::Array(items) => Value::Array(items.into_iter().map(f).collect()),
        v => v,
    }
}

/// `GEORADIUS`/`GEOSEARCH` items: `[member, dist?, hash?, [lon, lat]?]`, in
/// that order, each present when its option was given.
fn geo_items(argv: &[Vec<u8>], v: Value) -> Value {
    let (dist, hash, coord) = (
        has(argv, "WITHDIST"),
        has(argv, "WITHHASH"),
        has(argv, "WITHCOORD"),
    );
    if !(dist || hash || coord) {
        return v;
    }
    map_items(v, |item| match item {
        Value::Array(mut fields) => {
            let mut i = 1;
            if dist && i < fields.len() {
                fields[i] = double(fields[i].clone());
                i += 1;
            }
            if hash {
                i += 1;
            }
            if coord && i < fields.len() {
                fields[i] = map_items(fields[i].clone(), double);
            }
            Value::Array(fields)
        }
        other => other,
    })
}

/// The reply to `argv` as a RESP3 client receives it.
pub fn shape(argv: &[Vec<u8>], v: Value) -> Value {
    let Some(name) = argv.first().map(|a| upper(a)) else {
        return v;
    };
    let sub = argv.get(1).map(|a| upper(a)).unwrap_or_default();
    match name.as_str() {
        "HGETALL" => flat_map(v),
        "HRANDFIELD" if has(argv, "WITHVALUES") => match v {
            Value::Array(items) => {
                Value::Array(items.chunks(2).map(|p| Value::Array(p.to_vec())).collect())
            }
            v => v,
        },
        "CONFIG" if sub == "GET" => flat_map(v),
        "SMEMBERS" | "SINTER" | "SUNION" | "SDIFF" => set(v),
        "SPOP" if argv.len() > 2 => set(v),
        "ZSCORE" | "ZINCRBY" | "GEODIST" => double(v),
        "ZADD" if has(argv, "INCR") => double(v),
        "ZMSCORE" => map_items(v, double),
        "ZRANGE" | "ZRANGEBYSCORE" | "ZREVRANGE" | "ZREVRANGEBYSCORE" | "ZRANDMEMBER"
        | "ZUNION" | "ZINTER" | "ZDIFF"
            if has(argv, "WITHSCORES") =>
        {
            scored_pairs(v)
        }
        // Without COUNT a single `[member, score]`; with it, a list of them.
        "ZPOPMIN" | "ZPOPMAX" if argv.len() > 2 => scored_pairs(v),
        "ZPOPMIN" | "ZPOPMAX" => map_items_at(v, 1, double),
        "BZPOPMIN" | "BZPOPMAX" => map_items_at(v, 2, double),
        "ZRANK" | "ZREVRANK" if has(argv, "WITHSCORE") => map_items_at(v, 1, double),
        // `[key, [[member, score], ...]]`, the pairs already nested in RESP2.
        "ZMPOP" | "BZMPOP" => map_items_at(v, 1, |pairs| {
            map_items(pairs, |p| map_items_at(p, 1, double))
        }),
        "ZSCAN" => map_items_at(v, 1, |flat| match flat {
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .enumerate()
                    .map(|(i, x)| if i % 2 == 1 { double(x) } else { x })
                    .collect(),
            ),
            other => other,
        }),
        "GEOPOS" => map_items(v, |pos| map_items(pos, double)),
        "GEORADIUS"
        | "GEORADIUSBYMEMBER"
        | "GEORADIUS_RO"
        | "GEORADIUSBYMEMBER_RO"
        | "GEOSEARCH" => geo_items(argv, v),
        // `[[key, entries], ...]` becomes a map from key to entries.
        "XREAD" | "XREADGROUP" => match v {
            Value::Array(streams) => Value::Map(
                streams
                    .into_iter()
                    .map(|s| match s {
                        Value::Array(mut kv) if kv.len() == 2 => {
                            let entries = kv.pop().unwrap_or(Value::Nil);
                            (kv.pop().unwrap_or(Value::Nil), entries)
                        }
                        other => (other, Value::Nil),
                    })
                    .collect(),
            ),
            v => v,
        },
        "INFO" | "LOLWUT" => verbatim(v),
        "CLIENT" if sub == "LIST" || sub == "INFO" => verbatim(v),
        "MEMORY" | "LATENCY" if sub == "DOCTOR" => verbatim(v),
        "FUNCTION" if sub == "LIST" => function_flags_as_sets(v),
        _ => v,
    }
}

/// Apply `f` to element `i` of an array reply.
fn map_items_at(v: Value, i: usize, f: impl Fn(Value) -> Value) -> Value {
    match v {
        Value::Array(mut items) if i < items.len() => {
            items[i] = f(items[i].clone());
            Value::Array(items)
        }
        v => v,
    }
}

/// `FUNCTION LIST` reports each function's flags as a set.
fn function_flags_as_sets(v: Value) -> Value {
    map_items(v, |lib| match lib {
        Value::Map(fields) => Value::Map(
            fields
                .into_iter()
                .map(|(k, val)| {
                    let val = if matches!(&k, Value::Bulk(b) if b == b"functions") {
                        map_items(val, |f| match f {
                            Value::Map(ff) => Value::Map(
                                ff.into_iter()
                                    .map(|(fk, fv)| {
                                        let fv = if matches!(&fk, Value::Bulk(b) if b == b"flags") {
                                            set(fv)
                                        } else {
                                            fv
                                        };
                                        (fk, fv)
                                    })
                                    .collect(),
                            ),
                            other => other,
                        })
                    } else {
                        val
                    };
                    (k, val)
                })
                .collect(),
        ),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(a: &[&str]) -> Vec<Vec<u8>> {
        a.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn bulks(a: &[&str]) -> Value {
        Value::Array(a.iter().map(|s| Value::str(s)).collect())
    }

    #[test]
    fn replies_reframe_like_redis() {
        assert_eq!(
            shape(&argv(&["HGETALL", "h"]), bulks(&["f", "v"])),
            Value::Map(vec![(Value::str("f"), Value::str("v"))])
        );
        assert_eq!(
            shape(&argv(&["ZSCORE", "z", "m"]), Value::str("1.5")),
            Value::Double(1.5)
        );
        assert_eq!(shape(&argv(&["ZSCORE", "z", "m"]), Value::Nil), Value::Nil);
        assert_eq!(
            shape(
                &argv(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]),
                bulks(&["a", "1", "b", "inf"])
            ),
            Value::Array(vec![
                Value::Array(vec![Value::str("a"), Value::Double(1.0)]),
                Value::Array(vec![Value::str("b"), Value::Double(f64::INFINITY)]),
            ])
        );
        assert_eq!(
            shape(&argv(&["ZPOPMIN", "z"]), bulks(&["a", "1"])),
            Value::Array(vec![Value::str("a"), Value::Double(1.0)])
        );
        assert_eq!(
            shape(&argv(&["SMEMBERS", "s"]), bulks(&["a"])),
            Value::Set(vec![Value::str("a")])
        );
        assert_eq!(
            shape(&argv(&["GET", "k"]), Value::str("1.5")),
            Value::str("1.5")
        );
        assert_eq!(
            shape(
                &argv(&["XREAD", "STREAMS", "s", "0"]),
                Value::Array(vec![Value::Array(vec![
                    Value::str("s"),
                    Value::Array(vec![])
                ])])
            ),
            Value::Map(vec![(Value::str("s"), Value::Array(vec![]))])
        );
    }
}
