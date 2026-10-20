//! `CONFIG GET` / `CONFIG SET` over Redis 7.2's own parameter table.
//!
//! The table — names, aliases, types, bounds, flags and the rendered default
//! of every parameter — is generated from Redis's sources (`redis_tables.rs`),
//! so `CONFIG GET *` answers what a real Redis answers. A `CONFIG SET` value is
//! validated the way Redis validates it, normalised the way Redis prints it
//! back, and stored in shared memory (`server.rs`) so every worker sees it.
//!
//! Most parameters only describe Redis's own machinery and change nothing
//! here; the ones pg_redis acts on are read through [`get_i64`] and friends.
#![forbid(unsafe_code)]

use crate::redis_tables::{CONFIGS, ConfigKind, ConfigSpec};

pub fn spec(name: &str) -> Option<&'static ConfigSpec> {
    let n = name.to_ascii_lowercase();
    CONFIGS
        .iter()
        .find(|c| c.name == n || (!c.alias.is_empty() && c.alias == n))
}

/// The GUC values connection threads need: what `CONFIG GET` reports in place
/// of Redis's defaults, and the password, connection limit and default
/// database the listener applies. Taken on the worker's main thread by
/// [`snapshot_gucs`], at start and again after every reload: pgrx refuses a
/// GUC read from any other thread, and these are answered on connection
/// threads.
#[derive(Default, Clone)]
pub struct LiveGucs {
    pub port: i32,
    pub bind: Option<String>,
    pub requirepass: Option<String>,
    pub maxclients: i32,
    pub maxmemory_policy: Option<String>,
    pub default_db: u8,
}

impl LiveGucs {
    /// `redis.password` as the bytes a client must send, or `None` when no
    /// password is set.
    pub fn password(&self) -> Option<Vec<u8>> {
        self.requirepass
            .as_deref()
            .filter(|p| !p.is_empty())
            .map(|p| p.as_bytes().to_vec())
    }
}

static LIVE: std::sync::RwLock<Option<LiveGucs>> = std::sync::RwLock::new(None);

/// Take the snapshot, replacing the last one. Call on the background
/// worker's main thread.
pub fn snapshot_gucs() {
    let s = |g: &pgrx::guc::GucSetting<Option<std::ffi::CString>>| {
        g.get().and_then(|c| c.to_str().ok().map(str::to_string))
    };
    let next = LiveGucs {
        port: crate::PORT.get(),
        bind: s(&crate::LISTEN_ADDRESS),
        requirepass: s(&crate::PASSWORD),
        maxclients: crate::MAX_CONNECTIONS.get(),
        maxmemory_policy: Some(crate::MAXMEMORY_POLICY.get().name().to_string()),
        default_db: crate::DEFAULT_DB.get().clamp(0, 15) as u8,
    };
    // A poisoned lock only means a reader panicked holding it; the value is
    // a plain copy and still whole.
    *LIVE.write().unwrap_or_else(|e| e.into_inner()) = Some(next);
    GUC_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Release);
}

/// Bumped by every snapshot, so a cache of settings read through it knows
/// to read them again (`access::settings`).
static GUC_GENERATION: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

pub fn guc_generation() -> u32 {
    GUC_GENERATION.load(std::sync::atomic::Ordering::Acquire)
}

pub fn live_gucs() -> LiveGucs {
    LIVE.read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default()
}

/// What pg_redis itself fixes for a parameter, where Redis's default would
/// be a lie: the port it listens on, the databases it has.
fn live(name: &str) -> Option<String> {
    let g = live_gucs();
    match name {
        "port" => Some(g.port.to_string()),
        "databases" => Some(crate::commands::NUM_DBS.to_string()),
        "bind" => g.bind,
        "requirepass" => g.requirepass,
        "maxclients" => Some(g.maxclients.to_string()),
        "maxmemory-policy" => g.maxmemory_policy,
        "maxmemory" => crate::shm::usage().map(|u| u.limit.to_string()),
        _ => None,
    }
}

/// The current value of a parameter by its canonical name.
pub fn value(spec: &ConfigSpec) -> String {
    crate::server::config_get(spec.name)
        .or_else(|| live(spec.name))
        .unwrap_or_else(|| spec.default.to_string())
}

pub fn get_str(name: &str) -> String {
    spec(name).map(value).unwrap_or_default()
}

pub fn get_i64(name: &str) -> i64 {
    get_str(name).trim_end_matches('%').parse().unwrap_or(0)
}

pub fn get_bool(name: &str) -> bool {
    get_str(name) == "yes"
}

/// `CONFIG GET pattern [pattern ...]`: every matching parameter once, under
/// the name the pattern matched — an alias asked for is answered as the alias.
/// Hidden parameters only answer to their exact name.
pub fn get(patterns: &[Vec<u8>]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for p in patterns {
        let p = p.to_ascii_lowercase();
        let exact = !p.iter().any(|c| matches!(c, b'*' | b'?' | b'['));
        for c in CONFIGS {
            for name in [c.name, c.alias] {
                if name.is_empty() || (c.hidden && !exact) {
                    continue;
                }
                let matched = if exact {
                    name.as_bytes() == p.as_slice()
                } else {
                    crate::pubsub::glob_match(&p, name.as_bytes())
                };
                if matched && seen.insert(name) {
                    out.push((name.to_string(), value(c)));
                }
            }
        }
    }
    out
}

fn failed(arg: &str, why: &str) -> String {
    format!("ERR CONFIG SET failed (possibly related to argument '{arg}') - {why}")
}

/// Redis's `memtoull`: a number with an optional `b`, `k`/`kb`, `m`/`mb`,
/// `g`/`gb` unit — the bare letter in thousands, with `b` in 1024s.
fn memory(v: &str) -> Option<i128> {
    let v = v.to_ascii_lowercase();
    let split = v.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(v.len());
    let (num, unit) = v.split_at(split);
    let mul: i128 = match unit {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    let n: i128 = num.parse().ok()?;
    Some(n * mul)
}

/// The value as Redis would store and print it, or the reason it is refused.
fn normalise(spec: &ConfigSpec, arg: &str, v: &str) -> Result<String, String> {
    match &spec.kind {
        ConfigKind::Bool => match v.to_ascii_lowercase().as_str() {
            "yes" => Ok("yes".into()),
            "no" => Ok("no".into()),
            _ => Err(failed(arg, "argument must be 'yes' or 'no'")),
        },
        ConfigKind::Int {
            min,
            max,
            memory: mem,
            percent,
            octal,
        } => {
            if *percent && v.ends_with('%') {
                let n: i128 = v[..v.len() - 1]
                    .parse()
                    .map_err(|_| failed(arg, "argument must be a memory or percent value"))?;
                if n < 0 {
                    return Err(failed(arg, "argument must be a memory or percent value"));
                }
                // Redis keeps a percentage as its negative, bounded below by
                // the config's lower bound.
                if -n < *min {
                    return Err(failed(
                        arg,
                        &format!("percentage argument must be less or equal to {}", -*min),
                    ));
                }
                return Ok(format!("{n}%"));
            }
            let n = if *mem {
                memory(v).ok_or_else(|| {
                    failed(
                        arg,
                        if *percent {
                            "argument must be a memory or percent value"
                        } else {
                            "argument must be a memory value"
                        },
                    )
                })?
            } else if *octal {
                i128::from_str_radix(v, 8)
                    .map_err(|_| failed(arg, "argument couldn't be parsed into an integer"))?
            } else {
                v.parse()
                    .map_err(|_| failed(arg, "argument couldn't be parsed into an integer"))?
            };
            if n < *min || n > *max {
                return Err(failed(
                    arg,
                    &format!("argument must be between {min} and {max} inclusive"),
                ));
            }
            Ok(if *octal {
                format!("{n:o}")
            } else {
                n.to_string()
            })
        }
        ConfigKind::Enum { choices, multi } => {
            let words: Vec<String> = if *multi {
                v.split_whitespace().map(str::to_ascii_lowercase).collect()
            } else {
                vec![v.to_ascii_lowercase()]
            };
            if words.is_empty() || words.iter().any(|w| !choices.contains(&w.as_str())) {
                return Err(failed(
                    arg,
                    &format!(
                        "argument(s) must be one of the following: {}",
                        choices.join(", ")
                    ),
                ));
            }
            Ok(words.join(" "))
        }
        ConfigKind::Str => Ok(v.to_string()),
        ConfigKind::Special => special(spec.name, arg, v),
    }
}

/// The handful of parameters Redis parses by hand.
fn special(name: &str, arg: &str, v: &str) -> Result<String, String> {
    match name {
        // Pairs of seconds and changes, or empty to disable.
        "save" => {
            let nums: Vec<&str> = v.split_whitespace().collect();
            if !nums.len().is_multiple_of(2) || nums.iter().any(|n| n.parse::<u64>().is_err()) {
                return Err(failed(arg, "Invalid save parameters"));
            }
            Ok(nums.join(" "))
        }
        "notify-keyspace-events" => {
            if v.chars().any(|c| !"KEg$lshzxetmdnA".contains(c)) {
                return Err(failed(
                    arg,
                    "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.",
                ));
            }
            Ok(keyspace_events_canonical(v))
        }
        // Only the classes named change; all three are reported, in bytes,
        // in Redis's order.
        "client-output-buffer-limit" => {
            let parts: Vec<&str> = v.split_whitespace().collect();
            if !parts.len().is_multiple_of(4) {
                return Err(failed(
                    arg,
                    "Wrong number of arguments in buffer limit configuration.",
                ));
            }
            let mut classes = output_limit_classes(&get_str("client-output-buffer-limit"));
            for c in parts.chunks(4) {
                let class = match c[0].to_ascii_lowercase().as_str() {
                    "normal" => 0,
                    "replica" | "slave" => 1,
                    "pubsub" => 2,
                    _ => {
                        return Err(failed(
                            arg,
                            "Invalid client class specified in buffer limit configuration.",
                        ));
                    }
                };
                match (bytes(c[1]), bytes(c[2]), c[3].parse::<u64>()) {
                    (Some(hard), Some(soft), Ok(secs)) => classes[class] = [hard, soft, secs],
                    _ => {
                        return Err(failed(
                            arg,
                            "Error in hard, soft or soft_seconds setting in buffer limit configuration.",
                        ));
                    }
                }
            }
            Ok(["normal", "slave", "pubsub"]
                .iter()
                .zip(classes)
                .map(|(name, [h, s, t])| format!("{name} {h} {s} {t}"))
                .collect::<Vec<_>>()
                .join(" "))
        }
        // Each between 0 and 100, printed back as Redis prints a double.
        "latency-tracking-info-percentiles" => {
            let mut out = Vec::new();
            for p in v.split_whitespace() {
                let Ok(n) = p.parse::<f64>() else {
                    return Err(failed(
                        arg,
                        "Invalid latency-tracking-info-percentiles parameters",
                    ));
                };
                if !(0.0..=100.0).contains(&n) {
                    return Err(failed(
                        arg,
                        "latency-tracking-info-percentiles parameters should be between 0.0 and 100.0",
                    ));
                }
                out.push(crate::commands::format_score(n));
            }
            Ok(out.join(" "))
        }
        "oom-score-adj-values" => {
            let parts: Vec<&str> = v.split_whitespace().collect();
            if parts.len() != 3
                || parts
                    .iter()
                    .any(|p| !p.parse::<i32>().is_ok_and(|n| (-2000..=2000).contains(&n)))
            {
                return Err(failed(arg, "Invalid oom-score-adj-values"));
            }
            Ok(parts.join(" "))
        }
        _ => Ok(v.to_string()),
    }
}

/// A size as Redis writes one, in bytes, for the buffer limits.
fn bytes(v: &str) -> Option<u64> {
    memory(v).and_then(|n| u64::try_from(n).ok())
}

/// The three classes of a `client-output-buffer-limit` value, normal, replica
/// and pubsub, each hard, soft and soft seconds; Redis's defaults for any it
/// does not name.
fn output_limit_classes(value: &str) -> [[u64; 3]; 3] {
    let mut classes = [
        [0, 0, 0],
        [256 << 20, 64 << 20, 60],
        [32 << 20, 8 << 20, 60],
    ];
    let words: Vec<&str> = value.split_whitespace().collect();
    for c in words.chunks(4).filter(|c| c.len() == 4) {
        let i = match c[0] {
            "normal" => 0,
            "replica" | "slave" => 1,
            "pubsub" => 2,
            _ => continue,
        };
        if let (Some(h), Some(s), Ok(t)) = (bytes(c[1]), bytes(c[2]), c[3].parse()) {
            classes[i] = [h, s, t];
        }
    }
    classes
}

/// Redis normalises the flags it was given: `A` expands to its classes and
/// the whole string is re-rendered in a fixed order.
fn keyspace_events_canonical(v: &str) -> String {
    let all = v.contains('A');
    let mut out = String::new();
    for c in "g$lshzxetmdn".chars() {
        let in_a = "g$lshzxetd".contains(c);
        if v.contains(c) || (all && in_a) {
            out.push(c);
        }
    }
    let classes = "g$lshzxetd";
    if classes.chars().all(|c| out.contains(c)) {
        out = classes.chars().fold(out, |acc, c| acc.replacen(c, "", 1));
        out.insert(0, 'A');
    }
    if v.contains('K') {
        out.push('K');
    }
    if v.contains('E') {
        out.push('E');
    }
    out
}

/// `CONFIG SET name value [name value ...]`: all validated first, then all
/// stored together, as Redis applies them.
pub fn set(args: &[Vec<u8>]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    let mut staged: Vec<(&str, String)> = Vec::new();
    for pair in args.chunks(2) {
        let arg = String::from_utf8_lossy(&pair[0]).into_owned();
        let Some(spec) = spec(&arg) else {
            return Err(format!(
                "ERR Unknown option or number of arguments for CONFIG SET - '{arg}'"
            ));
        };
        if !seen.insert(spec.name) {
            return Err(failed(&arg, "duplicate parameter"));
        }
        if spec.immutable {
            return Err(failed(&arg, "can't set immutable config"));
        }
        if spec.protected {
            return Err(failed(&arg, "can't set protected config"));
        }
        let v = String::from_utf8_lossy(&pair[1]);
        let mut value = normalise(spec, &arg, &v)?;
        // Redis takes any hz and clamps it when applying it.
        if spec.name == "hz" {
            value = value.parse::<i64>().unwrap_or(10).clamp(1, 500).to_string();
        }
        // Applied by moving the listener, which Redis tries as it sets it:
        // a port nothing can listen on fails the whole CONFIG SET.
        if spec.name == "port"
            && let Ok(port) = value.parse::<u16>()
            && port != 0
            && i64::from(port) != get_i64("port")
        {
            // A plain bind, without the reuse flags the listeners take: it
            // fails while anything listens on the port, on any address.
            let at = live_gucs().bind.unwrap_or_else(|| "0.0.0.0".into());
            std::net::TcpListener::bind((at.as_str(), port))
                .map_err(|e| failed(&arg, &format!("Unable to listen on this port: {e}")))?;
        }
        staged.push((spec.name, value));
    }
    crate::server::config_set(&staged)?;
    if let Some((_, v)) = staged.iter().find(|(n, _)| *n == "notify-keyspace-events") {
        crate::server::set_notify_flags(crate::notify::parse_flags(v));
    }
    // `updateRequirePass`: the default user's password, or none.
    if let Some((_, v)) = staged.iter().find(|(n, _)| *n == "requirepass") {
        crate::acl::set_default_password(Some(v.as_bytes()));
    }
    if let Some((_, v)) = staged.iter().find(|(n, _)| *n == "maxmemory") {
        crate::shm::set_limit(v.parse().unwrap_or(0));
    }
    use crate::memdb::encoding::LIMIT_NAMES;
    if staged.iter().any(|(n, _)| LIMIT_NAMES.contains(n)) {
        crate::server::set_enc_limits(LIMIT_NAMES.map(get_i64));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_normalise_like_redis() {
        let s = spec("maxmemory").unwrap();
        assert_eq!(normalise(s, "maxmemory", "1gb").unwrap(), "1073741824");
        assert_eq!(normalise(s, "maxmemory", "1k").unwrap(), "1000");
        assert!(
            normalise(s, "maxmemory", "lots")
                .unwrap_err()
                .contains("memory value")
        );
        let o = spec("client-output-buffer-limit").unwrap();
        let set = |v: &str| normalise(o, "client-output-buffer-limit", v);
        assert_eq!(
            set("normal 1mb 2mb 60 replica 3mb 4mb 70 pubsub 5mb 6mb 80").unwrap(),
            "normal 1048576 2097152 60 slave 3145728 4194304 70 pubsub 5242880 6291456 80"
        );
        // A class not named keeps its value.
        assert_eq!(
            set("normal 100000 0 0").unwrap(),
            "normal 100000 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
        );
        assert!(
            set("wrong number")
                .unwrap_err()
                .contains("Wrong number of arguments")
        );
        assert!(
            set("master 10mb 10mb 60")
                .unwrap_err()
                .contains("Invalid client class")
        );
        assert!(
            set("normal 10mbs 10mb 60")
                .unwrap_err()
                .contains("Error in hard")
        );
        assert!(
            set("pubsub 10mb 10mb 60s")
                .unwrap_err()
                .contains("soft_seconds")
        );
        let b = spec("appendonly").unwrap();

        assert_eq!(normalise(b, "appendonly", "YES").unwrap(), "yes");
        assert!(
            normalise(b, "appendonly", "maybe")
                .unwrap_err()
                .contains("'yes' or 'no'")
        );
        let e = spec("maxmemory-policy").unwrap();
        assert!(
            normalise(e, "maxmemory-policy", "random")
                .unwrap_err()
                .contains("one of")
        );
        let i = spec("maxmemory-eviction-tenacity").unwrap();
        assert!(
            normalise(i, "maxmemory-eviction-tenacity", "101")
                .unwrap_err()
                .ends_with("argument must be between 0 and 100 inclusive")
        );
        assert_eq!(
            normalise(spec("save").unwrap(), "save", " 100 10  60 1 ").unwrap(),
            "100 10 60 1"
        );
    }

    #[test]
    fn aliases_and_defaults_come_from_redis() {
        assert_eq!(spec("slave-read-only").unwrap().name, "replica-read-only");
        assert_eq!(
            spec("hash-max-ziplist-entries").unwrap().name,
            "hash-max-listpack-entries"
        );
        assert_eq!(spec("hz").unwrap().default, "10");
        assert!(spec("daemonize").unwrap().immutable);
    }

    #[test]
    fn keyspace_events_render_canonically() {
        assert_eq!(keyspace_events_canonical("KEA"), "AKE");
        assert_eq!(keyspace_events_canonical("Ex"), "xE");
        assert_eq!(keyspace_events_canonical("Kx"), "xK");
        assert_eq!(keyspace_events_canonical("$lshzxet"), "$lshzxet");
        assert_eq!(keyspace_events_canonical("g$lshzxetdKE"), "AKE");
    }
}
