//! Redis 7.2's ACL: users, their rules, the permission checks every command
//! passes, the ACL LOG, and the `ACL` command itself.
//!
//! The rules follow `acl.c` operation for operation: a user is a set of
//! flags, password hashes and selectors; a selector is a command bitmap (one
//! bit per entry of the generated command table, plus the "future commands"
//! bit `+@all` sets), allowed first arguments, key patterns with read/write
//! flags and channel patterns. `ACL GETUSER` and `ACL LIST` describe a user
//! the way Redis does, from the ordered rules it was given.
//!
//! Every worker shares the users: the live set is kept in the shared heap
//! (`pubheap.rs`) as the `ACL LIST` description of each user, with a version
//! counter in [`AclCtl`]. Like Redis, nothing bounds the users or the log
//! but memory and `acllog-max-len`. A process parses the description once per version and every
//! connection thread checks the counter (one atomic load) before each
//! command, so a change made through any worker applies to the next command
//! of every connection. Changes take the region's lock: read, change, write
//! back, bump the version.
#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

use crate::commands::{Response, Value};
use crate::redis_tables::{COMMANDS, CommandSpec};

const WORDS: usize = 8;
/// The bit `+@all` and `allcommands` set and nothing else does: whether the
/// selector also allows commands added later (`ACLSelectorCanExecuteFutureCommands`).
const FUTURE: usize = WORDS * 64 - 1;

/// The `default` user's serial, which never changes.
pub const DEFAULT_SERIAL: u64 = 1;

// ─────────────────────────────── command table ──────────────────────────────

struct Meta {
    /// Category bits per command, `ACL_ORDER` positions.
    cats: Vec<u32>,
    /// A container's subcommands, by index.
    subs: Vec<Vec<usize>>,
    by_name: HashMap<&'static str, usize>,
    no_auth: Vec<bool>,
}

static META: LazyLock<Meta> = LazyLock::new(|| {
    let mut by_name = HashMap::new();
    let mut subs = vec![Vec::new(); COMMANDS.len()];
    for (i, c) in COMMANDS.iter().enumerate() {
        by_name.insert(c.name, i);
    }
    for (i, c) in COMMANDS.iter().enumerate() {
        if !c.container.is_empty()
            && let Some(&p) = by_name.get(c.container)
        {
            subs[p].push(i);
        }
    }
    let cats = COMMANDS
        .iter()
        .map(|c| {
            crate::admin::acl_categories(c)
                .iter()
                .filter_map(|n| category_bit(n.as_bytes()))
                .fold(0, |a, b| a | b)
        })
        .collect();
    let no_auth = COMMANDS
        .iter()
        .map(|c| c.flags.contains(&"no_auth"))
        .collect();
    Meta {
        cats,
        subs,
        by_name,
        no_auth,
    }
});

fn category_bit(name: &[u8]) -> Option<u32> {
    crate::admin::ACL_ORDER
        .iter()
        .position(|c| c.as_bytes().eq_ignore_ascii_case(name))
        .map(|i| 1 << i)
}

fn lower(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_lowercase()
}

fn top_level(name: &str) -> Option<usize> {
    let i = *META.by_name.get(name)?;
    COMMANDS[i].container.is_empty().then_some(i)
}

/// `lookupCommandBySdsLogic`: `name` or `container|sub`, one level only.
fn lookup_name(name: &[u8]) -> Option<usize> {
    let s = lower(name);
    let parts: Vec<&str> = s.split('|').collect();
    match parts.as_slice() {
        [one] => top_level(one),
        [base, sub] => {
            let b = top_level(base)?;
            if META.subs[b].is_empty() {
                return None;
            }
            META.by_name.get(format!("{base}|{sub}").as_str()).copied()
        }
        _ => None,
    }
}

/// `lookupCommand`: the command an argument vector runs, a container's
/// subcommand included.
pub fn lookup_argv(argv: &[Vec<u8>]) -> Option<usize> {
    let base = top_level(&lower(argv.first()?))?;
    if argv.len() == 1 || META.subs[base].is_empty() {
        return Some(base);
    }
    let full = format!("{}|{}", COMMANDS[base].name, lower(&argv[1]));
    META.by_name.get(full.as_str()).copied()
}

fn spec(id: usize) -> &'static CommandSpec {
    &COMMANDS[id]
}

fn has_flag(flags: &[&str], f: &str) -> bool {
    flags.iter().any(|x| x.eq_ignore_ascii_case(f))
}

const KEY_R: u8 = 1;
const KEY_W: u8 = 2;

/// What a key-spec's flags ask of a pattern: read for ACCESS, write for
/// INSERT, DELETE and UPDATE.
fn needed(flags: &[&str]) -> u8 {
    let mut n = 0;
    if has_flag(flags, "ACCESS") {
        n |= KEY_R;
    }
    if has_flag(flags, "INSERT") || has_flag(flags, "DELETE") || has_flag(flags, "UPDATE") {
        n |= KEY_W;
    }
    n
}

fn has_native_getkeys(name: &str) -> bool {
    matches!(
        name,
        "bitfield"
            | "blmpop"
            | "bzmpop"
            | "eval"
            | "eval_ro"
            | "evalsha"
            | "evalsha_ro"
            | "fcall"
            | "fcall_ro"
            | "georadius"
            | "georadiusbymember"
            | "lmpop"
            | "migrate"
            | "set"
            | "sintercard"
            | "sort"
            | "sort_ro"
            | "xread"
            | "xreadgroup"
            | "zdiff"
            | "zdiffstore"
            | "zinter"
            | "zintercard"
            | "zinterstore"
            | "zmpop"
            | "zunion"
            | "zunionstore"
    )
}

/// `doesCommandHaveKeys`.
fn has_keys(c: &CommandSpec) -> bool {
    has_native_getkeys(c.name) || c.key_specs.iter().any(|k| !has_flag(k.flags, "NOT_KEY"))
}

/// `getKeysFromCommandWithSpecs`: each key's position and what it needs.
pub(crate) fn command_keys(id: usize, argv: &[Vec<u8>]) -> Vec<(usize, u8)> {
    let c = spec(id);
    let has_keyspec = c.key_specs.iter().any(|k| !has_flag(k.flags, "NOT_KEY"));
    let varflags = c
        .key_specs
        .iter()
        .any(|k| has_flag(k.flags, "VARIABLE_FLAGS"));
    if has_keyspec
        && !varflags
        && let Some(keys) = crate::admin::keys_from_specs(c, argv)
    {
        return keys.into_iter().map(|(i, f)| (i, needed(&f))).collect();
    }
    native_keys(c.name, argv)
}

/// `genericGetKeys`: keys counted by an argument, flags left empty.
fn generic_keys(store: usize, count_at: usize, first: usize, argv: &[Vec<u8>]) -> Vec<(usize, u8)> {
    let argc = argv.len();
    let num = argv.get(count_at).map(|a| atoi(a)).unwrap_or(0);
    if num < 1 || num as usize > argc.saturating_sub(first) {
        return Vec::new();
    }
    let mut out: Vec<(usize, u8)> = (0..num as usize).map(|i| (first + i, 0)).collect();
    if store > 0 {
        out.push((store, 0));
    }
    out
}

/// C's `atoi`: leading digits, 0 when there are none.
fn atoi(a: &[u8]) -> i64 {
    let s = String::from_utf8_lossy(a);
    let s = s.trim_start();
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    let n: i64 = digits.parse().unwrap_or(0);
    if neg { -n } else { n }
}

fn native_keys(name: &str, argv: &[Vec<u8>]) -> Vec<(usize, u8)> {
    let argc = argv.len();
    let is = |i: usize, w: &str| {
        argv.get(i)
            .is_some_and(|a| a.eq_ignore_ascii_case(w.as_bytes()))
    };
    const RW_ACCESS: u8 = KEY_R | KEY_W;
    match name {
        "sintercard" | "zunion" | "zinter" | "zdiff" | "zintercard" | "lmpop" | "zmpop" => {
            generic_keys(0, 1, 2, argv)
        }
        "zunionstore" | "zinterstore" | "zdiffstore" => generic_keys(1, 2, 3, argv),
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro" | "blmpop"
        | "bzmpop" => generic_keys(0, 2, 3, argv),
        "sort_ro" => vec![(1, KEY_R)],
        "sort" => {
            let mut out = vec![(1, KEY_R)];
            let mut store = None;
            let mut i = 2;
            while i < argc {
                if is(i, "limit") {
                    i += 2;
                } else if is(i, "get") || is(i, "by") {
                    i += 1;
                } else if is(i, "store") && i + 1 < argc {
                    store = Some(i + 1);
                }
                i += 1;
            }
            out.extend(store.map(|p| (p, KEY_W)));
            out
        }
        "migrate" => {
            let (mut first, mut num) = (3usize, 1usize);
            if argc > 6 {
                let mut i = 6;
                while i < argc {
                    if is(i, "keys") {
                        if !argv[3].is_empty() {
                            num = 0;
                        } else {
                            first = i + 1;
                            num = argc - first;
                        }
                        break;
                    }
                    if is(i, "auth") {
                        i += 1;
                    } else if is(i, "auth2") {
                        i += 2;
                    }
                    i += 1;
                }
            }
            (0..num).map(|i| (first + i, RW_ACCESS)).collect()
        }
        "georadius" | "georadiusbymember" => {
            let mut stored = None;
            let mut i = 5;
            while i < argc {
                if (is(i, "store") || is(i, "storedist")) && i + 1 < argc {
                    stored = Some(i + 1);
                    i += 1;
                }
                i += 1;
            }
            let mut out = vec![(1, 0)];
            out.extend(stored.map(|p| (p, 0)));
            out
        }
        "xread" | "xreadgroup" => {
            let mut streams = None;
            let mut i = 1;
            while i < argc {
                if is(i, "block") || is(i, "count") {
                    i += 1;
                } else if is(i, "group") {
                    i += 2;
                } else if is(i, "noack") {
                } else if is(i, "streams") {
                    streams = Some(i);
                    break;
                } else {
                    break;
                }
                i += 1;
            }
            let Some(at) = streams else { return Vec::new() };
            let num = argc - at - 1;
            if num == 0 || !num.is_multiple_of(2) {
                return Vec::new();
            }
            (at + 1..argc - num / 2).map(|i| (i, 0)).collect()
        }
        "set" => {
            let get = (3..argc).any(|i| argv[i].eq_ignore_ascii_case(b"get"));
            vec![(1, if get { RW_ACCESS } else { KEY_W })]
        }
        "bitfield" => {
            let mut readonly = true;
            let mut i = 2;
            while i < argc {
                let rem = argc - i - 1;
                if is(i, "get") && rem >= 2 {
                    i += 2;
                } else if (is(i, "set") || is(i, "incrby")) && rem >= 3 {
                    readonly = false;
                    break;
                } else if is(i, "overflow") && rem >= 1 {
                    i += 1;
                } else {
                    readonly = false;
                    break;
                }
                i += 1;
            }
            vec![(1, if readonly { KEY_R } else { RW_ACCESS })]
        }
        _ => Vec::new(),
    }
}

/// The channels a command names, and whether each is a pattern: Redis's
/// `commands_with_channels`, publish and subscribe kinds only.
fn command_channels(id: usize, argv: &[Vec<u8>]) -> Vec<(usize, bool)> {
    match spec(id).name {
        "subscribe" | "ssubscribe" => (1..argv.len()).map(|i| (i, false)).collect(),
        "psubscribe" => (1..argv.len()).map(|i| (i, true)).collect(),
        "publish" | "spublish" if argv.len() > 1 => vec![(1, false)],
        _ => Vec::new(),
    }
}

// ────────────────────────────────── rules ───────────────────────────────────

/// Why a rule was refused: `ACLSetUserStringError`'s cases.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RuleError {
    NoEnt,
    Inval,
    Exist,
    IsDir,
    NoDev,
    BadMsg,
    Child,
}

impl RuleError {
    pub fn text(self) -> &'static str {
        match self {
            RuleError::NoEnt => "Unknown command or category name in ACL",
            RuleError::Inval => "Syntax error",
            RuleError::Exist => {
                "Adding a pattern after the * pattern (or the 'allkeys' flag) is not valid and \
                 does not have any effect. Try 'resetkeys' to start with an empty list of patterns"
            }
            RuleError::IsDir => {
                "Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid \
                 and does not have any effect. Try 'resetchannels' to start with an empty list \
                 of channels"
            }
            RuleError::NoDev => {
                "The password you are trying to remove from the user does not exist"
            }
            RuleError::BadMsg => {
                "The password hash must be exactly 64 characters and contain only lowercase \
                 hexadecimal characters"
            }
            RuleError::Child => "Allowing first-arg of a subcommand is not supported",
        }
    }
}

fn eq(a: &[u8], b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}

/// `ACLStringHasSpaces`.
fn has_spaces(s: &[u8]) -> bool {
    s.iter()
        .any(|&c| matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | 0))
}

/// One set of permissions: the root one every user has, or one added in
/// parentheses.
#[derive(Clone, Debug, PartialEq)]
pub struct Selector {
    root: bool,
    allkeys: bool,
    allchannels: bool,
    allcommands: bool,
    bits: [u64; WORDS],
    firstargs: BTreeMap<usize, Vec<Vec<u8>>>,
    patterns: Vec<(u8, Vec<u8>)>,
    channels: Vec<Vec<u8>>,
    /// The command and category rules in the order they still apply, as
    /// `+name` / `-@category`: what describes the bitmap back.
    rules: Vec<Vec<u8>>,
}

impl Selector {
    /// `ACLCreateSelector`: no permissions, the channels `acl-pubsub-default`
    /// gives.
    fn new(root: bool, pubsub_all: bool) -> Selector {
        Selector {
            root,
            allkeys: false,
            allchannels: pubsub_all,
            allcommands: false,
            bits: [0; WORDS],
            firstargs: BTreeMap::new(),
            patterns: Vec::new(),
            channels: Vec::new(),
            rules: Vec::new(),
        }
    }

    fn bit(&self, id: usize) -> bool {
        self.bits[id / 64] & (1 << (id % 64)) != 0
    }

    fn set_bit(&mut self, id: usize, on: bool) {
        if on {
            self.bits[id / 64] |= 1 << (id % 64);
        } else {
            self.bits[id / 64] &= !(1 << (id % 64));
            self.allcommands = false;
        }
    }

    /// `ACLChangeSelectorPerm`: a container takes its subcommands with it.
    fn change_perm(&mut self, id: usize, allow: bool) {
        self.set_bit(id, allow);
        self.firstargs.remove(&id);
        for &s in &META.subs[id] {
            self.set_bit(s, allow);
        }
    }

    /// `ACLUpdateCommandRules`: drop the rules this one supersedes — the same
    /// name, or a subcommand of it — and append it.
    fn update_rules(&mut self, body: &[u8], allow: bool) {
        let body = body.to_ascii_lowercase();
        self.rules.retain(|r| {
            let e = &r[1..];
            !(e == body.as_slice()
                || (e.len() > body.len() && e.starts_with(&body) && e[body.len()] == b'|'))
        });
        let mut rule = vec![if allow { b'+' } else { b'-' }];
        rule.extend_from_slice(&body);
        self.rules.push(rule);
    }

    fn set_category(&mut self, cat: &[u8], allow: bool) -> Result<(), RuleError> {
        let flag = category_bit(&cat[1..]).ok_or(RuleError::NoEnt)?;
        self.update_rules(cat, allow);
        for (i, c) in COMMANDS.iter().enumerate() {
            if !c.container.is_empty() {
                continue;
            }
            if META.cats[i] & flag != 0 {
                self.change_perm(i, allow);
            }
            for &s in &META.subs[i] {
                if META.cats[s] & flag != 0 {
                    self.change_perm(s, allow);
                }
            }
        }
        Ok(())
    }

    /// `ACLSetSelector`.
    fn apply(&mut self, op: &[u8]) -> Result<(), RuleError> {
        if op.is_empty() {
            return Err(RuleError::Inval);
        }
        if eq(op, "allkeys") || op == b"~*" {
            self.allkeys = true;
            self.patterns.clear();
        } else if eq(op, "resetkeys") {
            self.allkeys = false;
            self.patterns.clear();
        } else if eq(op, "allchannels") || op == b"&*" {
            self.allchannels = true;
            self.channels.clear();
        } else if eq(op, "resetchannels") {
            self.allchannels = false;
            self.channels.clear();
        } else if eq(op, "allcommands") || eq(op, "+@all") {
            self.bits = [u64::MAX; WORDS];
            self.allcommands = true;
            self.rules.clear();
            self.firstargs.clear();
        } else if eq(op, "nocommands") || eq(op, "-@all") {
            self.bits = [0; WORDS];
            self.allcommands = false;
            self.rules.clear();
            self.firstargs.clear();
        } else if op[0] == b'~' || op[0] == b'%' {
            if self.allkeys {
                return Err(RuleError::Exist);
            }
            let mut flags = 0u8;
            let mut offset = 1;
            if op[0] == b'%' {
                while offset < op.len() {
                    let c = op[offset].to_ascii_uppercase();
                    if c == b'R' && flags & KEY_R == 0 {
                        flags |= KEY_R;
                    } else if c == b'W' && flags & KEY_W == 0 {
                        flags |= KEY_W;
                    } else if op[offset] == b'~' {
                        offset += 1;
                        break;
                    } else {
                        return Err(RuleError::Inval);
                    }
                    offset += 1;
                }
                // Redis accepts a bare `%` and then cannot describe it.
                if flags == 0 {
                    return Err(RuleError::Inval);
                }
            } else {
                flags = KEY_R | KEY_W;
            }
            let pattern = &op[offset.min(op.len())..];
            if has_spaces(pattern) {
                return Err(RuleError::Inval);
            }
            match self.patterns.iter_mut().find(|(_, p)| p == pattern) {
                Some((f, _)) => *f |= flags,
                None => self.patterns.push((flags, pattern.to_vec())),
            }
            self.allkeys = false;
        } else if op[0] == b'&' {
            if self.allchannels {
                return Err(RuleError::IsDir);
            }
            let pattern = &op[1..];
            if has_spaces(pattern) {
                return Err(RuleError::Inval);
            }
            if !self.channels.iter().any(|c| c == pattern) {
                self.channels.push(pattern.to_vec());
            }
            self.allchannels = false;
        } else if op[0] == b'+' && op.get(1) != Some(&b'@') {
            let name = &op[1..];
            match name.iter().rposition(|&c| c == b'|') {
                None => {
                    let id = lookup_name(name).ok_or(RuleError::NoEnt)?;
                    self.change_perm(id, true);
                    self.update_rules(spec(id).name.as_bytes(), true);
                }
                Some(bar) => {
                    let (base, sub) = (&name[..bar], &name[bar + 1..]);
                    let id = lookup_name(base).ok_or(RuleError::NoEnt)?;
                    if !spec(id).container.is_empty() {
                        return Err(RuleError::Child);
                    }
                    if sub.is_empty() {
                        return Err(RuleError::Inval);
                    }
                    if !META.subs[id].is_empty() {
                        let sid = lookup_name(name).ok_or(RuleError::NoEnt)?;
                        self.change_perm(sid, true);
                    } else {
                        // `+select|0`: a first argument of a command with
                        // no subcommands. Spaces would not survive being
                        // described back, so they are refused here.
                        if has_spaces(sub) {
                            return Err(RuleError::Inval);
                        }
                        let list = self.firstargs.entry(id).or_default();
                        if !list.iter().any(|a| a.eq_ignore_ascii_case(sub)) {
                            list.push(sub.to_vec());
                        }
                    }
                    self.update_rules(name, true);
                }
            }
        } else if op[0] == b'-' && op.get(1) != Some(&b'@') {
            let id = lookup_name(&op[1..]).ok_or(RuleError::NoEnt)?;
            self.change_perm(id, false);
            self.update_rules(spec(id).name.as_bytes(), false);
        } else if (op[0] == b'+' || op[0] == b'-') && op.get(1) == Some(&b'@') {
            self.set_category(&op[1..], op[0] == b'+')?;
        } else {
            return Err(RuleError::Inval);
        }
        Ok(())
    }

    /// `ACLDescribeSelectorCommandRules`.
    fn command_rules(&self) -> Vec<u8> {
        let mut out = if self.bit(FUTURE) {
            b"+@all".to_vec()
        } else {
            b"-@all".to_vec()
        };
        for r in &self.rules {
            out.push(b' ');
            out.extend_from_slice(r);
        }
        out
    }

    fn pattern_text(flags: u8, p: &[u8]) -> Vec<u8> {
        let mut out = match flags {
            KEY_R => b"%R~".to_vec(),
            KEY_W => b"%W~".to_vec(),
            _ => b"~".to_vec(),
        };
        out.extend_from_slice(p);
        out
    }

    fn keys_text(&self) -> Vec<u8> {
        if self.allkeys {
            return b"~*".to_vec();
        }
        let parts: Vec<Vec<u8>> = self
            .patterns
            .iter()
            .map(|(f, p)| Self::pattern_text(*f, p))
            .collect();
        parts.join(&b' ')
    }

    fn channels_text(&self) -> Vec<u8> {
        if self.allchannels {
            return b"&*".to_vec();
        }
        let parts: Vec<Vec<u8>> = self
            .channels
            .iter()
            .map(|c| [b"&".as_slice(), c].concat())
            .collect();
        parts.join(&b' ')
    }

    /// `ACLDescribeSelector`.
    fn describe(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if self.allkeys {
            out.extend_from_slice(b"~* ");
        } else {
            for (f, p) in &self.patterns {
                out.extend(Self::pattern_text(*f, p));
                out.push(b' ');
            }
        }
        if self.allchannels {
            out.extend_from_slice(b"&* ");
        } else {
            out.extend_from_slice(b"resetchannels ");
            for c in &self.channels {
                out.push(b'&');
                out.extend_from_slice(c);
                out.push(b' ');
            }
        }
        out.extend(self.command_rules());
        out
    }

    fn check_key(&self, key: &[u8], need: u8) -> bool {
        self.allkeys
            || self
                .patterns
                .iter()
                .any(|(f, p)| f & need == need && crate::pubsub::glob_match(p, key))
    }

    /// `ACLSelectorCheckCmd`.
    fn check(
        &self,
        id: usize,
        argv: &[Vec<u8>],
        keys: &mut Option<Vec<(usize, u8)>>,
    ) -> Result<(), Deny> {
        if !self.allcommands && !META.no_auth[id] && !self.bit(id) {
            let Some(allowed) = self.firstargs.get(&id).filter(|_| argv.len() >= 2) else {
                return Err(Deny::Cmd);
            };
            let idx = if spec(id).container.is_empty() { 1 } else { 2 };
            let arg = argv.get(idx).map(Vec::as_slice).unwrap_or(b"");
            if !allowed.iter().any(|a| a.eq_ignore_ascii_case(arg)) {
                return Err(Deny::Cmd);
            }
        }
        if !self.allkeys && has_keys(spec(id)) {
            let keys = keys.get_or_insert_with(|| command_keys(id, argv));
            for &(i, need) in keys.iter() {
                if !self.check_key(&argv[i], need) {
                    return Err(Deny::Key(i));
                }
            }
        }
        if !self.allchannels {
            for (i, pattern) in command_channels(id, argv) {
                if !channel_in(&self.channels, &argv[i], pattern) {
                    return Err(Deny::Channel(i));
                }
            }
        }
        Ok(())
    }

    /// `ACLSelectorHasUnrestrictedKeyAccess` for read access.
    fn reads_every_key(&self) -> bool {
        self.allkeys
            || self
                .patterns
                .iter()
                .any(|(f, p)| f & KEY_R == KEY_R && p == b"*")
    }
}

/// `ACLCheckChannelAgainstList`: a channel is matched against the patterns,
/// a pattern compared with them literally.
fn channel_in(list: &[Vec<u8>], channel: &[u8], is_pattern: bool) -> bool {
    list.iter().any(|p| {
        if is_pattern {
            p == channel
        } else {
            crate::pubsub::glob_match(p, channel)
        }
    })
}

/// Why a command was refused, with the argument that caused it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Deny {
    Cmd,
    Key(usize),
    Channel(usize),
}

impl Deny {
    fn rank(self) -> (u8, usize) {
        match self {
            Deny::Cmd => (1, 0),
            Deny::Key(i) => (2, i),
            Deny::Channel(i) => (4, i),
        }
    }
}

const F_ON: u8 = 1;
const F_OFF: u8 = 2;
const F_NOPASS: u8 = 4;
const F_SKIP_SANITIZE: u8 = 8;
const F_SANITIZE: u8 = 16;
const FLAG_NAMES: [(u8, &str); 5] = [
    (F_ON, "on"),
    (F_OFF, "off"),
    (F_NOPASS, "nopass"),
    (F_SKIP_SANITIZE, "skip-sanitize-payload"),
    (F_SANITIZE, "sanitize-payload"),
];

#[derive(Clone, Debug, PartialEq)]
pub struct User {
    pub name: Vec<u8>,
    pub serial: u64,
    flags: u8,
    /// SHA-256 of each password, in lowercase hex.
    passwords: Vec<String>,
    /// The root selector first.
    selectors: Vec<Selector>,
}

fn pubsub_default_all() -> bool {
    crate::config::get_str("acl-pubsub-default") == "allchannels"
}

impl User {
    /// `ACLCreateUser`: off, no passwords, no permissions.
    pub fn new(name: &[u8], serial: u64) -> User {
        User {
            name: name.to_vec(),
            serial,
            flags: F_OFF | F_SANITIZE,
            passwords: Vec::new(),
            selectors: vec![Selector::new(true, pubsub_default_all())],
        }
    }

    /// `ACLCreateDefaultUser`.
    pub fn default_user() -> User {
        let mut u = User::new(b"default", DEFAULT_SERIAL);
        for op in ["+@all", "~*", "&*", "on", "nopass"] {
            let _ = u.set(op.as_bytes());
        }
        u
    }

    pub fn enabled(&self) -> bool {
        self.flags & F_OFF == 0
    }

    pub fn nopass(&self) -> bool {
        self.flags & F_NOPASS != 0
    }

    fn set_flag(&mut self, on: u8, off: u8) {
        self.flags = (self.flags | on) & !off;
    }

    /// `ACLSetUser`.
    pub fn set(&mut self, op: &[u8]) -> Result<(), RuleError> {
        if op.is_empty() {
            return Ok(());
        }
        if eq(op, "on") {
            self.set_flag(F_ON, F_OFF);
        } else if eq(op, "off") {
            self.set_flag(F_OFF, F_ON);
        } else if eq(op, "skip-sanitize-payload") {
            self.set_flag(F_SKIP_SANITIZE, F_SANITIZE);
        } else if eq(op, "sanitize-payload") {
            self.set_flag(F_SANITIZE, F_SKIP_SANITIZE);
        } else if eq(op, "nopass") {
            self.flags |= F_NOPASS;
            self.passwords.clear();
        } else if eq(op, "resetpass") {
            self.flags &= !F_NOPASS;
            self.passwords.clear();
        } else if op[0] == b'>' || op[0] == b'#' {
            let hash = if op[0] == b'>' {
                sha256_hex(&op[1..])
            } else {
                valid_hash(&op[1..])?
            };
            if !self.passwords.contains(&hash) {
                self.passwords.push(hash);
            }
            self.flags &= !F_NOPASS;
        } else if op[0] == b'<' || op[0] == b'!' {
            let hash = if op[0] == b'<' {
                sha256_hex(&op[1..])
            } else {
                valid_hash(&op[1..])?
            };
            let at = self
                .passwords
                .iter()
                .position(|p| *p == hash)
                .ok_or(RuleError::NoDev)?;
            self.passwords.remove(at);
        } else if op[0] == b'(' && op[op.len() - 1] == b')' {
            let mut s = Selector::new(false, pubsub_default_all());
            for arg in split_args(&op[1..op.len() - 1]).unwrap_or_default() {
                s.apply(&arg)?;
            }
            self.selectors.push(s);
        } else if eq(op, "clearselectors") {
            self.selectors.truncate(1);
        } else if eq(op, "reset") {
            for o in ["resetpass", "resetkeys", "resetchannels"] {
                self.set(o.as_bytes())?;
            }
            if pubsub_default_all() {
                self.set(b"allchannels")?;
            }
            for o in ["off", "sanitize-payload", "clearselectors", "-@all"] {
                self.set(o.as_bytes())?;
            }
        } else {
            self.selectors[0].apply(op)?;
        }
        Ok(())
    }

    /// Apply a list of rules as `ACL SETUSER` gets them: selectors spread
    /// over several arguments merged first, nothing applied if one fails.
    pub fn set_all(&mut self, args: &[Vec<u8>]) -> Result<(), String> {
        let merged = merge_selector_args(args).map_err(|at| {
            format!(
                "Unmatched parenthesis in acl selector starting at '{}'.",
                String::from_utf8_lossy(&args[at])
            )
        })?;
        let mut tmp = self.clone();
        for op in &merged {
            tmp.set(op).map_err(|e| {
                format!(
                    "Error in ACL SETUSER modifier '{}': {}",
                    String::from_utf8_lossy(op),
                    e.text()
                )
            })?;
        }
        *self = tmp;
        Ok(())
    }

    /// `ACLDescribeUser`: what `ACL LIST` prints after the name.
    pub fn describe(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (f, n) in FLAG_NAMES {
            if self.flags & f != 0 {
                out.extend_from_slice(n.as_bytes());
                out.push(b' ');
            }
        }
        for p in &self.passwords {
            out.push(b'#');
            out.extend_from_slice(p.as_bytes());
            out.push(b' ');
        }
        for s in &self.selectors {
            if s.root {
                out.extend(s.describe());
            } else {
                out.extend_from_slice(b" (");
                out.extend(s.describe());
                out.push(b')');
            }
        }
        out
    }

    /// `ACLCheckUserCredentials`, for an existing user.
    pub fn accepts(&self, password: &[u8]) -> bool {
        if !self.enabled() {
            return false;
        }
        if self.nopass() {
            return true;
        }
        let hashed = sha256_hex(password);
        let mut ok = false;
        for p in &self.passwords {
            ok |= constant_time_eq(hashed.as_bytes(), p.as_bytes());
        }
        ok
    }

    /// Whether every command, key and channel is allowed: the check can be
    /// skipped.
    pub fn unrestricted(&self) -> bool {
        self.selectors
            .iter()
            .any(|s| s.allcommands && s.allkeys && s.allchannels)
    }

    /// `ACLCheckAllUserCommandPerm`.
    pub fn check(&self, id: usize, argv: &[Vec<u8>]) -> Result<(), Deny> {
        let mut keys = None;
        let mut relevant = Deny::Cmd;
        for s in &self.selectors {
            match s.check(id, argv, &mut keys) {
                Ok(()) => return Ok(()),
                Err(d) => {
                    let (r, i) = d.rank();
                    let (br, bi) = relevant.rank();
                    if r > br || (r == br && i > bi) {
                        relevant = d;
                    }
                }
            }
        }
        Err(relevant)
    }

    /// `ACLUserCheckCmdWithUnrestrictedKeyAccess` with read access.
    pub fn reads_every_key(&self, id: usize, argv: &[Vec<u8>]) -> bool {
        let mut keys = None;
        self.selectors
            .iter()
            .any(|s| s.check(id, argv, &mut keys).is_ok() && s.reads_every_key())
    }

    /// Every channel pattern of every selector, or `None` when one allows
    /// all channels.
    fn channel_union(&self) -> Option<Vec<Vec<u8>>> {
        if self.selectors.iter().any(|s| s.allchannels) {
            return None;
        }
        Some(
            self.selectors
                .iter()
                .flat_map(|s| s.channels.iter().cloned())
                .collect(),
        )
    }

    /// `ACLKillPubsubClientsIfNeeded`'s first test: whether `self` keeps
    /// every channel permission `old` had, so no subscriber can lose one.
    fn keeps_channels_of(&self, old: &User) -> bool {
        let Some(upcoming) = self.channel_union() else {
            return true;
        };
        old.selectors
            .iter()
            .all(|s| !s.allchannels && s.channels.iter().all(|c| upcoming.contains(c)))
    }

    /// Whether these subscriptions are all allowed now.
    pub fn allows_subscriptions(&self, channels: &[Vec<u8>], patterns: &[Vec<u8>]) -> bool {
        let Some(list) = self.channel_union() else {
            return true;
        };
        channels.iter().all(|c| channel_in(&list, c, false))
            && patterns.iter().all(|p| channel_in(&list, p, true))
    }

    /// `ACL GETUSER`'s reply.
    fn getuser_value(&self) -> Value {
        let sel = |s: &Selector| {
            vec![
                (Value::str("commands"), Value::Bulk(s.command_rules())),
                (Value::str("keys"), Value::Bulk(s.keys_text())),
                (Value::str("channels"), Value::Bulk(s.channels_text())),
            ]
        };
        let mut m = vec![
            (
                Value::str("flags"),
                Value::Set(
                    FLAG_NAMES
                        .iter()
                        .filter(|(f, _)| self.flags & f != 0)
                        .map(|(_, n)| Value::str(n))
                        .collect(),
                ),
            ),
            (
                Value::str("passwords"),
                Value::Array(self.passwords.iter().map(|p| Value::str(p)).collect()),
            ),
        ];
        m.extend(sel(&self.selectors[0]));
        m.push((
            Value::str("selectors"),
            Value::Array(
                self.selectors[1..]
                    .iter()
                    .map(|s| Value::Map(sel(s)))
                    .collect(),
            ),
        ));
        Value::Map(m)
    }
}

fn valid_hash(h: &[u8]) -> Result<String, RuleError> {
    if h.len() != 64 || !h.iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(RuleError::BadMsg);
    }
    Ok(String::from_utf8_lossy(h).into_owned())
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    let mut equal = ((a.len() ^ b.len()) as u64).ct_eq(&0);
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        equal &= x.ct_eq(&y);
    }
    bool::from(equal)
}

/// `ACLMergeSelectorArguments`: `Err` is the index of the argument whose
/// parenthesis never closes.
fn merge_selector_args(args: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, usize> {
    let mut out = Vec::new();
    let mut open: Option<(usize, Vec<u8>)> = None;
    for (j, op) in args.iter().enumerate() {
        if let Some((_, sel)) = open.as_mut() {
            sel.push(b' ');
            sel.extend_from_slice(op);
            if op.last() == Some(&b')') {
                out.push(open.take().map(|(_, s)| s).unwrap_or_default());
            }
            continue;
        }
        if op.first() == Some(&b'(') && op.last() != Some(&b')') {
            open = Some((j, op.clone()));
            continue;
        }
        out.push(op.clone());
    }
    match open {
        Some((j, _)) => Err(j),
        None => Ok(out),
    }
}

/// `sdssplitargs`: words separated by whitespace, with Redis's double and
/// single quoting. `None` for unbalanced quotes.
pub fn split_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let sp = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    let hex = |c: u8| (c as char).to_digit(16);
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        while i < line.len() && sp(line[i]) {
            i += 1;
        }
        if i >= line.len() {
            return Some(out);
        }
        let mut cur = Vec::new();
        let (mut inq, mut insq) = (false, false);
        loop {
            if inq {
                let c = *line.get(i)?;
                if c == b'\\'
                    && line.get(i + 1) == Some(&b'x')
                    && let (Some(a), Some(b)) = (
                        line.get(i + 2).and_then(|&c| hex(c)),
                        line.get(i + 3).and_then(|&c| hex(c)),
                    )
                {
                    cur.push((a * 16 + b) as u8);
                    i += 3;
                } else if c == b'\\' && i + 1 < line.len() {
                    i += 1;
                    cur.push(match line[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        o => o,
                    });
                } else if c == b'"' {
                    if line.get(i + 1).is_some_and(|&n| !sp(n)) {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    cur.push(c);
                }
            } else if insq {
                let c = *line.get(i)?;
                if c == b'\\' && line.get(i + 1) == Some(&b'\'') {
                    i += 1;
                    cur.push(b'\'');
                } else if c == b'\'' {
                    if line.get(i + 1).is_some_and(|&n| !sp(n)) {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    cur.push(c);
                }
            } else {
                match line.get(i) {
                    None => break,
                    Some(&c) if sp(c) => break,
                    Some(b'"') => inq = true,
                    Some(b'\'') => insq = true,
                    Some(&c) => cur.push(c),
                }
            }
            i += 1;
        }
        out.push(cur);
    }
}

// ─────────────────────────────── SHA-256 ────────────────────────────────────

/// SHA-256 in lowercase hex: how Redis stores a password.
pub fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    let bits = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bits.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[4 * i],
                chunk[4 * i + 1],
                chunk[4 * i + 2],
                chunk[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (x, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *x = x.wrapping_add(v);
        }
    }
    h.iter().map(|x| format!("{x:08x}")).collect()
}

// ───────────────────────────── shared state ─────────────────────────────────

/// A byte blob behind a lock, with a version counter readers check without
/// it. The bytes are blob `B` of the shared heap.
#[repr(C)]
pub struct Region<const B: usize> {
    lock: AtomicU32,
    version: AtomicU64,
}

const USERS: usize = 0;
const LOG: usize = 1;

/// The blobs without a shared heap — no `shared_preload_libraries`, and unit
/// tests that build none: this process's own.
static LOCAL_BLOBS: [std::sync::Mutex<Vec<u8>>; 2] = [
    std::sync::Mutex::new(Vec::new()),
    std::sync::Mutex::new(Vec::new()),
];

fn local_blob(b: usize) -> std::sync::MutexGuard<'static, Vec<u8>> {
    LOCAL_BLOBS[b].lock().unwrap_or_else(|e| e.into_inner())
}

struct Held<'a>(&'a AtomicU32);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

impl<const B: usize> Region<B> {
    fn lock(&self) -> Held<'_> {
        let mut spins = 0u32;
        while self
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            spins += 1;
            if spins < 100 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        Held(&self.lock)
    }

    /// Under the lock.
    fn read(&self) -> Vec<u8> {
        match crate::pubheap::root() {
            Some(root) => root.acl[B].lock().to_vec(),
            None => local_blob(B).clone(),
        }
    }

    /// Under the lock. False when the shared heap has no room for it; the
    /// blob is then as it was.
    fn write(&self, data: &[u8]) -> bool {
        let written = match crate::pubheap::root() {
            Some(root) => crate::pubheap::catching(|| {
                let mut v =
                    crate::pubheap::HVec::with_capacity_in(data.len(), crate::pubheap::PubHeap);
                v.extend_from_slice(data);
                // The old bytes are let go once the blob's lock is.
                let old = std::mem::replace(&mut *root.acl[B].lock(), v);
                drop(old);
            })
            .is_some(),
            None => {
                *local_blob(B) = data.to_vec();
                true
            }
        };
        if written {
            self.version.fetch_add(1, Ordering::AcqRel);
        }
        written
    }
}

/// The ACL state every worker shares.
#[repr(C)]
pub struct AclCtl {
    users: Region<USERS>,
    log: Region<LOG>,
    next_serial: AtomicU64,
    log_next_id: AtomicU64,
    /// 0 before the users are loaded from `redis.acl_users`, 1 while one
    /// worker loads them, 2 after.
    init: AtomicU32,
    /// The `redis.password` last applied to the default user: 0 unset, 1 set
    /// to the password whose hash follows. Under the users' lock.
    pass_set: AtomicU32,
    pass_hash: [AtomicU8; 64],
    /// INFO's `acl_access_denied_*`: auth, cmd, key, channel.
    pub denied: [AtomicU64; 4],
}

pub fn ctl_size() -> usize {
    std::mem::size_of::<AclCtl>()
}

static LOCAL: LazyLock<&'static AclCtl> =
    LazyLock::new(|| Box::leak(crate::shared::zeroed_box::<AclCtl>()));

/// The shared block, or — without `shared_preload_libraries`, and in unit
/// tests — one of this process's own.
fn ctl() -> &'static AclCtl {
    crate::acl_state().unwrap_or(*LOCAL)
}

/// The users as of one version.
pub struct State {
    pub version: u64,
    users: BTreeMap<Vec<u8>, User>,
    serials: HashMap<u64, Vec<u8>>,
}

impl State {
    fn from_users(version: u64, users: BTreeMap<Vec<u8>, User>) -> State {
        let serials = users.values().map(|u| (u.serial, u.name.clone())).collect();
        State {
            version,
            users,
            serials,
        }
    }

    pub fn get(&self, name: &[u8]) -> Option<&User> {
        self.users.get(name)
    }

    pub fn by_serial(&self, serial: u64) -> Option<&User> {
        self.users.get(self.serials.get(&serial)?)
    }

    pub fn default_user(&self) -> &User {
        self.users
            .get(b"default".as_slice())
            .expect("the default user always exists")
    }

    /// Whether a connection that has not authenticated is let in: the
    /// default user is on and needs no password.
    pub fn default_open(&self) -> bool {
        let d = self.default_user();
        d.nopass() && d.enabled()
    }

    /// `authRequired`.
    pub fn auth_required(&self, s: &Session) -> bool {
        !self.default_open() && !s.authenticated
    }
}

fn serialize(users: &BTreeMap<Vec<u8>, User>) -> Vec<u8> {
    let mut out = Vec::new();
    for u in users.values() {
        let d = u.describe();
        out.extend_from_slice(&u.serial.to_le_bytes());
        out.extend_from_slice(&(u.name.len() as u32).to_le_bytes());
        out.extend_from_slice(&u.name);
        out.extend_from_slice(&(d.len() as u32).to_le_bytes());
        out.extend_from_slice(&d);
    }
    out
}

/// A user from its description, as `ACL LIST` prints it.
fn from_description(name: &[u8], serial: u64, rules: &[u8]) -> Result<User, String> {
    let args: Vec<Vec<u8>> = rules.split(|&c| c == b' ').map(<[u8]>::to_vec).collect();
    let mut u = User::new(name, serial);
    u.set_all(&args)?;
    Ok(u)
}

fn deserialize(bytes: &[u8]) -> BTreeMap<Vec<u8>, User> {
    let mut users = BTreeMap::new();
    let mut at = 0;
    let take = |at: &mut usize, n: usize| -> Option<&[u8]> {
        let s = bytes.get(*at..*at + n)?;
        *at += n;
        Some(s)
    };
    let u32_at =
        |at: &mut usize| take(at, 4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize);
    while at < bytes.len() {
        let Some(serial) =
            take(&mut at, 8).map(|b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
        else {
            break;
        };
        let Some(nl) = u32_at(&mut at) else { break };
        let Some(name) = take(&mut at, nl).map(<[u8]>::to_vec) else {
            break;
        };
        let Some(dl) = u32_at(&mut at) else { break };
        let Some(desc) = take(&mut at, dl) else { break };
        match from_description(&name, serial, desc) {
            Ok(u) => {
                users.insert(name, u);
            }
            Err(e) => eprintln!(
                "pg_redis: ACL user '{}' could not be read back: {e}",
                String::from_utf8_lossy(&name)
            ),
        }
    }
    users
        .entry(b"default".to_vec())
        .or_insert_with(User::default_user);
    users
}

static PROC_CACHE: RwLock<Option<Arc<State>>> = RwLock::new(None);

thread_local! {
    static THREAD_CACHE: RefCell<Option<Arc<State>>> = const { RefCell::new(None) };
}

fn remember(state: &Arc<State>) {
    THREAD_CACHE.with(|c| *c.borrow_mut() = Some(state.clone()));
    let mut p = PROC_CACHE.write().unwrap_or_else(|e| e.into_inner());
    if p.as_ref().is_none_or(|s| s.version < state.version) {
        *p = Some(state.clone());
    }
}

fn cached(version: u64) -> Option<Arc<State>> {
    if let Some(s) = THREAD_CACHE.with(|c| c.borrow().clone().filter(|s| s.version == version)) {
        return Some(s);
    }
    let s = PROC_CACHE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .filter(|s| s.version == version)?;
    THREAD_CACHE.with(|c| *c.borrow_mut() = Some(s.clone()));
    Some(s)
}

/// The users now. One atomic load when nothing changed.
pub fn current() -> Arc<State> {
    let ctl = ctl();
    if let Some(s) = cached(ctl.users.version.load(Ordering::Acquire)) {
        return s;
    }
    let _held = ctl.users.lock();
    let version = ctl.users.version.load(Ordering::Acquire);
    if let Some(s) = cached(version) {
        return s;
    }
    let state = Arc::new(State::from_users(version, deserialize(&ctl.users.read())));
    remember(&state);
    state
}

/// Run `f` on a copy of the users and publish the result. Returns the state
/// before and after.
fn mutate<R>(
    f: impl FnOnce(&mut BTreeMap<Vec<u8>, User>) -> Result<R, String>,
) -> Result<(R, Arc<State>, Arc<State>), String> {
    let ctl = ctl();
    let _held = ctl.users.lock();
    let version = ctl.users.version.load(Ordering::Acquire);
    let before = match cached(version) {
        Some(s) => s,
        None => Arc::new(State::from_users(version, deserialize(&ctl.users.read()))),
    };
    let mut users = before.users.clone();
    let r = f(&mut users)?;
    users
        .entry(b"default".to_vec())
        .or_insert_with(User::default_user);
    if !ctl.users.write(&serialize(&users)) {
        return Err("OOM shared memory is full; raise redis.pubsub_memory".to_string());
    }
    let after = Arc::new(State::from_users(
        ctl.users.version.load(Ordering::Acquire),
        users,
    ));
    remember(&after);
    Ok((r, before, after))
}

fn new_serial() -> u64 {
    ctl().next_serial.fetch_add(1, Ordering::AcqRel) + DEFAULT_SERIAL + 1
}

/// The name of the user with this serial, for `CLIENT LIST`.
pub fn name_of(serial: u64) -> String {
    current()
        .by_serial(serial)
        .map(|u| String::from_utf8_lossy(&u.name).into_owned())
        .unwrap_or_else(|| "default".to_string())
}

pub fn serial_of(name: &[u8]) -> Option<u64> {
    current().get(name).map(|u| u.serial)
}

// ───────────────────────────── sessions, auth ───────────────────────────────

/// What a connection knows of its user.
#[derive(Clone, Copy, Debug)]
pub struct Session {
    pub serial: u64,
    pub authenticated: bool,
}

impl Session {
    /// `clientSetDefaultAuth`.
    pub fn new() -> Session {
        Session {
            serial: DEFAULT_SERIAL,
            authenticated: current().default_open(),
        }
    }
}

/// The session of a connected client, by id: what the dispatcher runs a
/// client's queued or scripted commands as.
pub fn session_of(client_id: u64) -> Session {
    let serial = (client_id != 0)
        .then(|| crate::server::slot_of(client_id))
        .flatten()
        .map_or(DEFAULT_SERIAL, |s| s.user_serial.load(Ordering::Relaxed));
    Session {
        serial,
        authenticated: true,
    }
}

impl Default for Session {
    fn default() -> Self {
        Session::new()
    }
}

/// `ACLAuthenticateUser`: the user's serial, or `None` (and an ACL LOG
/// entry) when the pair is wrong or the user is off.
pub fn authenticate(
    name: &[u8],
    password: &[u8],
    argv0: &[u8],
    multi: bool,
    client_id: u64,
) -> Option<u64> {
    let st = current();
    if let Some(u) = st.get(name)
        && u.accepts(password)
    {
        return Some(u.serial);
    }
    record(
        Reason::Auth,
        if multi {
            Context::Multi
        } else {
            Context::Toplevel
        },
        argv0,
        name,
        client_id,
    );
    None
}

pub const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

// ──────────────────────────────── checks ────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Reason {
    Cmd,
    Key,
    Auth,
    Channel,
}

impl Reason {
    fn name(self) -> &'static str {
        match self {
            Reason::Cmd => "command",
            Reason::Key => "key",
            Reason::Auth => "auth",
            Reason::Channel => "channel",
        }
    }

    fn code(self) -> u8 {
        match self {
            Reason::Cmd => 1,
            Reason::Key => 2,
            Reason::Auth => 3,
            Reason::Channel => 4,
        }
    }

    fn from_code(c: u8) -> Reason {
        match c {
            2 => Reason::Key,
            3 => Reason::Auth,
            4 => Reason::Channel,
            _ => Reason::Cmd,
        }
    }

    fn counter(self) -> usize {
        match self {
            Reason::Auth => 0,
            Reason::Cmd => 1,
            Reason::Key => 2,
            Reason::Channel => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Context {
    Toplevel,
    Lua,
    Multi,
}

impl Context {
    fn name(c: u8) -> &'static str {
        match c {
            1 => "lua",
            2 => "multi",
            3 => "module",
            _ => "toplevel",
        }
    }

    fn code(self) -> u8 {
        match self {
            Context::Toplevel => 0,
            Context::Lua => 1,
            Context::Multi => 2,
        }
    }
}

/// A refused command: what the reply and the log entry say.
#[derive(Debug, Clone)]
pub struct Denial {
    pub reason: Reason,
    pub user: Vec<u8>,
    pub command: &'static str,
    /// The key or channel, or the command's full name.
    pub object: Vec<u8>,
}

impl Denial {
    /// `getAclErrorMessage`.
    pub fn message(&self, verbose: bool) -> String {
        let user = String::from_utf8_lossy(&self.user);
        let obj = String::from_utf8_lossy(&self.object);
        match self.reason {
            Reason::Key if verbose => {
                format!("User {user} has no permissions to access the '{obj}' key")
            }
            Reason::Key => "No permissions to access a key".to_string(),
            Reason::Channel if verbose => {
                format!("User {user} has no permissions to access the '{obj}' channel")
            }
            Reason::Channel => "No permissions to access a channel".to_string(),
            _ => format!(
                "User {user} has no permissions to run the '{}' command",
                self.command
            ),
        }
    }

    /// What EXEC answers for a queued command no longer allowed.
    pub fn exec_message(&self) -> String {
        let why = match self.reason {
            Reason::Cmd => "no permission to execute the command or subcommand",
            Reason::Key => "no permission to touch the specified keys",
            Reason::Channel => "no permission to access one of the channels used as arguments",
            Reason::Auth => "no permission",
        };
        format!(
            "NOPERM ACLs rules changed between the moment the transaction was accumulated and \
             the EXEC call. This command is no longer allowed for the following reason: {why}"
        )
    }
}

pub enum Verdict {
    Allowed,
    Denied(Denial),
    /// The connection's user was deleted: it is being closed.
    Gone,
}

/// Whether the user with `serial` may run `argv`. A command not found, or
/// with the wrong number of arguments, is allowed here: its own error is the
/// reply, as in Redis, where those checks come first.
pub fn evaluate(serial: u64, argv: &[Vec<u8>]) -> Verdict {
    let st = current();
    let Some(user) = st.by_serial(serial) else {
        return Verdict::Gone;
    };
    if user.unrestricted() {
        return Verdict::Allowed;
    }
    let Some(id) = lookup_argv(argv) else {
        return Verdict::Allowed;
    };
    if !crate::commands::arity_ok(spec(id).arity, argv.len()) {
        return Verdict::Allowed;
    }
    match user.check(id, argv) {
        Ok(()) => Verdict::Allowed,
        Err(d) => Verdict::Denied(denial(user, id, argv, d)),
    }
}

fn denial(user: &User, id: usize, argv: &[Vec<u8>], d: Deny) -> Denial {
    let (reason, object) = match d {
        Deny::Cmd => (Reason::Cmd, spec(id).name.as_bytes().to_vec()),
        Deny::Key(i) => (Reason::Key, argv[i].clone()),
        Deny::Channel(i) => (Reason::Channel, argv[i].clone()),
    };
    Denial {
        reason,
        user: user.name.clone(),
        command: spec(id).name,
        object,
    }
}

/// SORT's own refusal of BY and GET without read access to every key.
pub fn sort_denied(serial: u64, argv: &[Vec<u8>]) -> Option<&'static str> {
    let name = lower(argv.first()?);
    if name != "sort" && name != "sort_ro" {
        return None;
    }
    let st = current();
    let user = st.by_serial(serial)?;
    if user.unrestricted() {
        return None;
    }
    let id = lookup_argv(argv)?;
    let full = user.reads_every_key(id, argv);
    let mut j = 2;
    while j < argv.len() {
        let left = argv.len() - j - 1;
        let a = &argv[j];
        if eq(a, "asc") || eq(a, "desc") || eq(a, "alpha") {
        } else if eq(a, "limit") && left >= 2 {
            j += 2;
        } else if name == "sort" && eq(a, "store") && left >= 1 {
            j += 1;
        } else if eq(a, "by") && left >= 1 {
            if argv[j + 1].contains(&b'*') && !full {
                return Some("ERR BY option of SORT denied due to insufficient ACL permissions.");
            }
            j += 1;
        } else if eq(a, "get") && left >= 1 {
            if !full {
                return Some("ERR GET option of SORT denied due to insufficient ACL permissions.");
            }
            j += 1;
        } else {
            return None;
        }
        j += 1;
    }
    None
}

/// Whether a subscriber's subscriptions are all still allowed. Shard
/// channels come with their internal prefix, which is stripped here.
pub fn subscriptions_allowed(serial: u64, channels: &[Vec<u8>], patterns: &[Vec<u8>]) -> bool {
    let st = current();
    let Some(user) = st.by_serial(serial) else {
        return false;
    };
    let channels: Vec<Vec<u8>> = channels
        .iter()
        .filter_map(|c| match c.strip_prefix(crate::pubsub::SHARD_PREFIX) {
            Some(s) => Some(s.to_vec()),
            None if crate::pubsub::is_internal(c) => None,
            None => Some(c.clone()),
        })
        .collect();
    user.allows_subscriptions(&channels, patterns)
}

/// The command line of a connected client, for the log.
fn client_info(id: u64) -> String {
    crate::server::clients()
        .into_iter()
        .find(|c| c.id == id)
        .map(|c| c.line())
        .unwrap_or_default()
}

/// Log a refusal and count it, as `addACLLogEntry` does.
pub fn record_denial(d: &Denial, context: Context, client_id: u64) {
    record(d.reason, context, &d.object, &d.user, client_id);
}

// ──────────────────────────────── ACL LOG ───────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct Entry {
    count: u64,
    reason: u8,
    context: u8,
    object: Vec<u8>,
    username: Vec<u8>,
    ctime: i64,
    cinfo: Vec<u8>,
    entry_id: i64,
    created: i64,
}

fn put(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn encode_entries(entries: &[Entry]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        out.extend_from_slice(&e.count.to_le_bytes());
        out.push(e.reason);
        out.push(e.context);
        put(&mut out, &e.object);
        put(&mut out, &e.username);
        out.extend_from_slice(&e.ctime.to_le_bytes());
        put(&mut out, &e.cinfo);
        out.extend_from_slice(&e.entry_id.to_le_bytes());
        out.extend_from_slice(&e.created.to_le_bytes());
    }
    out
}

fn decode_entries(b: &[u8]) -> Vec<Entry> {
    let mut out = Vec::new();
    let mut at = 0usize;
    let mut take = |n: usize| -> Option<&[u8]> {
        let s = b.get(at..at + n)?;
        at += n;
        Some(s)
    };
    let mut next = || -> Option<Entry> {
        let n8 = |s: &[u8]| i64::from_le_bytes(s.try_into().unwrap_or([0; 8]));
        let count = n8(take(8)?) as u64;
        let rc = take(2)?;
        let (reason, context) = (rc[0], rc[1]);
        let mut field = || -> Option<Vec<u8>> {
            let l = take(4)?;
            let l = u32::from_le_bytes([l[0], l[1], l[2], l[3]]) as usize;
            take(l).map(<[u8]>::to_vec)
        };
        let object = field()?;
        let username = field()?;
        let ctime = n8(take(8)?);
        let l = take(4)?;
        let l = u32::from_le_bytes([l[0], l[1], l[2], l[3]]) as usize;
        let cinfo = take(l)?.to_vec();
        let entry_id = n8(take(8)?);
        let created = n8(take(8)?);
        Some(Entry {
            count,
            reason,
            context,
            object,
            username,
            ctime,
            cinfo,
            entry_id,
            created,
        })
    };
    while let Some(e) = next() {
        out.push(e);
    }
    out
}

fn record(reason: Reason, context: Context, object: &[u8], username: &[u8], client_id: u64) {
    let ctl = ctl();
    ctl.denied[reason.counter()].fetch_add(1, Ordering::Relaxed);
    let cinfo = client_info(client_id).into_bytes();
    let max = crate::config::get_i64("acllog-max-len").max(0) as usize;
    let now = crate::server::now_ms();
    let _held = ctl.log.lock();
    let mut entries = decode_entries(&ctl.log.read());
    let found = entries.iter().take(10).position(|e| {
        e.reason == reason.code()
            && e.context == context.code()
            && (e.ctime - now).abs() <= 60_000
            && e.object[..] == *object
            && e.username[..] == *username
    });
    match found {
        Some(i) => {
            let mut e = entries.remove(i);
            e.cinfo = cinfo;
            e.ctime = now;
            e.count += 1;
            entries.insert(0, e);
        }
        None => {
            let id = ctl.log_next_id.fetch_add(1, Ordering::AcqRel) as i64;
            entries.insert(
                0,
                Entry {
                    count: 1,
                    reason: reason.code(),
                    context: context.code(),
                    object: object.to_vec(),
                    username: username.to_vec(),
                    ctime: now,
                    cinfo,
                    entry_id: id,
                    created: now,
                },
            );
            entries.truncate(max);
        }
    }
    ctl.log.write(&encode_entries(&entries));
}

fn log_entries() -> Vec<Entry> {
    let ctl = ctl();
    let _held = ctl.log.lock();
    decode_entries(&ctl.log.read())
}

fn log_reset() {
    let ctl = ctl();
    let _held = ctl.log.lock();
    ctl.log.write(&[]);
}

/// INFO stats' `acl_access_denied_*`, in Redis's order: auth, cmd, key,
/// channel.
pub fn denied_counts() -> [u64; 4] {
    let c = &ctl().denied;
    std::array::from_fn(|i| c[i].load(Ordering::Relaxed))
}

// ───────────────────────── changes and their clients ────────────────────────

/// After a change: close the clients of users that went, and have the
/// subscribers of users who lost channel permissions check theirs. Waits,
/// briefly, for those clients when `wait` (a connection thread; never the
/// dispatcher, which a closing client may be waiting on).
fn after_change(before: &State, after: &State, me: u64, wait: bool) {
    let gone: HashSet<u64> = before
        .serials
        .keys()
        .filter(|s| !after.serials.contains_key(s))
        .copied()
        .collect();
    let recheck: HashSet<u64> = after
        .users
        .values()
        .filter(|u| {
            before
                .by_serial(u.serial)
                .is_some_and(|old| old != *u && !u.keeps_channels_of(old))
        })
        .map(|u| u.serial)
        .collect();
    if gone.is_empty() && recheck.is_empty() {
        return;
    }
    let mut waiting = Vec::new();
    for c in crate::server::clients() {
        if gone.contains(&c.user_serial) {
            c.kill();
            if c.id != me {
                waiting.push(c.id);
            }
        } else if recheck.contains(&c.user_serial) && c.flags & crate::server::flag::PUBSUB != 0 {
            c.request_acl_recheck();
            if c.id != me {
                waiting.push(c.id);
            }
        }
    }
    if !wait || waiting.is_empty() {
        return;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1000);
    while std::time::Instant::now() < deadline
        && crate::server::clients()
            .iter()
            .any(|c| waiting.contains(&c.id) && (c.killed() || c.acl_recheck_pending()))
    {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// `ACLUpdateDefaultUserPassword`: `CONFIG SET requirepass`, and a reload
/// of `redis.password`.
pub fn set_default_password(password: Option<&[u8]>) {
    let _ = mutate(|users| {
        let d = users
            .entry(b"default".to_vec())
            .or_insert_with(User::default_user);
        let _ = d.set(b"resetpass");
        match password.filter(|p| !p.is_empty()) {
            Some(p) => {
                let _ = d.set(&[b">".as_slice(), p].concat());
            }
            None => {
                let _ = d.set(b"nopass");
            }
        }
        Ok(())
    });
}

fn pass_marker(ctl: &AclCtl) -> Option<String> {
    (ctl.pass_set.load(Ordering::Acquire) == 1).then(|| {
        ctl.pass_hash
            .iter()
            .map(|b| b.load(Ordering::Relaxed) as char)
            .collect()
    })
}

fn set_pass_marker(ctl: &AclCtl, guc: Option<&[u8]>) {
    match guc {
        Some(p) => {
            for (a, b) in ctl.pass_hash.iter().zip(sha256_hex(p).bytes()) {
                a.store(b, Ordering::Relaxed);
            }
            ctl.pass_set.store(1, Ordering::Release);
        }
        None => ctl.pass_set.store(0, Ordering::Release),
    }
}

/// After a reload: if `redis.password` changed since it was last applied,
/// it becomes the default user's password (or none). Unchanged, it leaves
/// whatever `ACL SETUSER` or `CONFIG SET requirepass` did since.
pub fn sync_password(guc: Option<&[u8]>) {
    let ctl = ctl();
    let guc = guc.filter(|p| !p.is_empty());
    // Before the users are loaded the loading worker applies it.
    if ctl.init.load(Ordering::Acquire) != 2 && crate::acl_state().is_some() {
        return;
    }
    let want = guc.map(sha256_hex);
    if pass_marker(ctl) == want {
        return;
    }
    set_pass_marker(ctl, guc);
    set_default_password(guc);
}

/// Worker start: the first worker loads `redis.acl_users` (`rows`, from the
/// table, `None` when it is not there) and applies `redis.password`; the
/// rest wait for it.
pub fn startup(rows: impl FnOnce() -> Option<Vec<(Vec<u8>, Vec<u8>)>>, guc: Option<&[u8]>) {
    let ctl = ctl();
    let guc = guc.filter(|p| !p.is_empty());
    if ctl
        .init
        .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        let rows = rows().unwrap_or_default();
        let has_default = rows.iter().any(|(n, _)| n == b"default");
        if !rows.is_empty() {
            match parse_rows(&rows) {
                Ok(users) => {
                    let _ = mutate(|u| {
                        *u = users;
                        Ok(())
                    });
                }
                Err(e) => eprintln!("pg_redis: WARNING redis.acl_users was not loaded: {e}"),
            }
        }
        if !has_default && guc.is_some() {
            set_default_password(guc);
        }
        set_pass_marker(ctl, guc);
        ctl.init.store(2, Ordering::Release);
        return;
    }
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while ctl.init.load(Ordering::Acquire) != 2 && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    sync_password(guc);
}

/// `ACLLoadFromFile`, over the table's rows: every row checked, nothing
/// applied if one is wrong.
fn parse_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> Result<BTreeMap<Vec<u8>, User>, String> {
    let file = "redis.acl_users";
    let mut errors = String::new();
    let mut users: BTreeMap<Vec<u8>, User> = BTreeMap::new();
    for (i, (name, rules)) in rows.iter().enumerate() {
        let line = i + 1;
        let shown = String::from_utf8_lossy(name);
        if has_spaces(name) {
            errors.push_str(&format!(
                "'{file}:{line}: username '{shown}' contains invalid characters. "
            ));
            continue;
        }
        if users.contains_key(name) {
            errors.push_str(&format!(
                "WARNING: Duplicate user '{shown}' found on line {line}. "
            ));
            continue;
        }
        let serial = if name == b"default" {
            DEFAULT_SERIAL
        } else {
            new_serial()
        };
        let mut u = User::new(name, serial);
        let trimmed = String::from_utf8_lossy(rules);
        let args: Vec<Vec<u8>> = trimmed
            .trim_matches(|c| matches!(c, ' ' | '\t' | '\r' | '\n'))
            .as_bytes()
            .split(|&c| c == b' ')
            .map(<[u8]>::to_vec)
            .collect();
        let Ok(merged) = merge_selector_args(&args) else {
            errors.push_str(&format!(
                "{file}:{line}: Unmatched parenthesis in selector definition."
            ));
            continue;
        };
        let mut syntax = false;
        for op in &merged {
            let op: Vec<u8> = String::from_utf8_lossy(op)
                .trim_matches(|c| matches!(c, '\t' | '\r' | '\n'))
                .as_bytes()
                .to_vec();
            if let Err(e) = u.set(&op) {
                if e == RuleError::NoEnt {
                    errors.push_str(&format!(
                        "{file}:{line}: Error in applying operation '{}': {}. ",
                        String::from_utf8_lossy(&op),
                        e.text()
                    ));
                } else if !syntax {
                    errors.push_str(&format!("{file}:{line}: {}. ", e.text()));
                    syntax = true;
                }
            }
        }
        users.insert(name.clone(), u);
    }
    if !errors.is_empty() {
        errors.push_str(
            "WARNING: ACL errors detected, no change to the previously active ACL rules was performed",
        );
        return Err(errors);
    }
    users
        .entry(b"default".to_vec())
        .or_insert_with(User::default_user);
    Ok(users)
}

// ───────────────────────────── the ACL command ──────────────────────────────

/// `ACL SAVE` and `ACL LOAD` read and write `redis.acl_users`, which only
/// the dispatcher can reach: this is what it is sent.
#[derive(Debug, Clone)]
pub enum TableOp {
    Save(Vec<(Vec<u8>, Vec<u8>)>),
    Load,
}

/// Where an `ACL` command runs and who runs it.
pub struct Caller {
    pub session: Session,
    pub client_id: u64,
    /// On a connection thread, which may wait for the clients a change
    /// closes. Not on the dispatcher.
    pub may_wait: bool,
}

fn err(m: impl Into<String>) -> Response {
    Response::Error(m.into())
}

fn syntax_error(sub: &[u8]) -> Response {
    err(format!(
        "ERR unknown subcommand or wrong number of arguments for '{}'. Try ACL HELP.",
        String::from_utf8_lossy(sub)
    ))
}

fn parse_long(a: &[u8]) -> Option<i64> {
    std::str::from_utf8(a).ok()?.parse().ok()
}

const HELP: &[&str] = &[
    "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CAT [<category>]",
    "    List all commands that belong to <category>, or all command categories",
    "    when no category is specified.",
    "DELUSER <username> [<username> ...]",
    "    Delete a list of users.",
    "DRYRUN <username> <command> [<arg> ...]",
    "    Returns whether the user can execute the given command without executing the command.",
    "GETUSER <username>",
    "    Get the user's details.",
    "GENPASS [<bits>]",
    "    Generate a secure 256-bit user password. The optional `bits` argument can",
    "    be used to specify a different size.",
    "LIST",
    "    Show users details in config file format.",
    "LOAD",
    "    Reload users from the ACL file.",
    "LOG [<count> | RESET]",
    "    Show the ACL log entries.",
    "SAVE",
    "    Save the current config to the ACL file.",
    "SETUSER <username> <attribute> [<attribute> ...]",
    "    Create or modify a user with the specified attributes.",
    "USERS",
    "    List all the registered usernames.",
    "WHOAMI",
    "    Return the current connection username.",
    "HELP",
    "    Print this help.",
];

fn random_hex(chars: usize) -> String {
    use std::io::Read;
    let mut bytes = vec![0u8; chars.div_ceil(2)];
    let ok = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .is_ok();
    if !ok {
        for b in bytes.iter_mut() {
            *b = crate::memdb::fast_random() as u8;
        }
    }
    let mut s: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    s.truncate(chars);
    s
}

/// `ACL <subcommand> ...`. `args` starts at the subcommand.
pub fn command(args: &[Vec<u8>], caller: &Caller, data: &mut crate::admin::Data) -> Response {
    let Some(sub_raw) = args.first() else {
        return err("ERR wrong number of arguments for 'acl' command");
    };
    let sub = lower(sub_raw);
    let argc = args.len() + 1;
    match sub.as_str() {
        "setuser" if argc >= 3 => {
            let name = &args[1];
            if has_spaces(name) {
                return err("ERR Usernames can't contain spaces or null characters");
            }
            let pubsub = mutate(|users| {
                let mut u = match users.get(name) {
                    Some(u) => u.clone(),
                    None => User::new(name, 0),
                };
                u.set_all(&args[2..]).map_err(|e| format!("ERR {e}"))?;
                if u.serial == 0 {
                    u.serial = if name == b"default" {
                        DEFAULT_SERIAL
                    } else {
                        new_serial()
                    };
                }
                users.insert(name.clone(), u);
                Ok(())
            });
            match pubsub {
                Ok(((), before, after)) => {
                    after_change(&before, &after, caller.client_id, caller.may_wait);
                    Response::Ok
                }
                Err(e) => err(e),
            }
        }
        "deluser" if argc >= 3 => {
            if args[1..].iter().any(|n| n == b"default") {
                return err("ERR The 'default' user cannot be removed");
            }
            match mutate(|users| {
                Ok(args[1..]
                    .iter()
                    .filter(|n| users.remove(n.as_slice()).is_some())
                    .count())
            }) {
                Ok((n, before, after)) => {
                    after_change(&before, &after, caller.client_id, caller.may_wait);
                    Response::Integer(n as i64)
                }
                Err(e) => err(e),
            }
        }
        "getuser" if argc == 3 => match current().get(&args[1]) {
            None => Response::Null,
            Some(u) => Response::Value(u.getuser_value()),
        },
        "list" | "users" if argc == 2 => {
            let st = current();
            Response::Value(Value::Array(
                st.users
                    .values()
                    .map(|u| {
                        if sub == "users" {
                            Value::Bulk(u.name.clone())
                        } else {
                            let mut line = b"user ".to_vec();
                            line.extend_from_slice(&u.name);
                            line.push(b' ');
                            line.extend(u.describe());
                            Value::Bulk(line)
                        }
                    })
                    .collect(),
            ))
        }
        "whoami" if argc == 2 => match current().by_serial(caller.session.serial) {
            Some(u) => Response::BulkString(u.name.clone()),
            None => Response::Null,
        },
        "save" if argc == 2 => {
            let rows: Vec<(Vec<u8>, Vec<u8>)> = current()
                .users
                .values()
                .map(|u| (u.name.clone(), u.describe()))
                .collect();
            match data(crate::commands::Command::AclTable(TableOp::Save(rows)), 0) {
                Response::Error(_) => err(
                    "ERR There was an error trying to save the ACLs. Please check the server logs for more information",
                ),
                _ => Response::Ok,
            }
        }
        "load" if argc == 2 => {
            let rows = match data(crate::commands::Command::AclTable(TableOp::Load), 0) {
                Response::Value(Value::Array(rows)) => rows
                    .into_iter()
                    .filter_map(|r| match r {
                        Value::Array(mut p) if p.len() == 2 => match (p.remove(0), p.remove(0)) {
                            (Value::Bulk(n), Value::Bulk(r)) => Some((n, r)),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                Response::Error(e) => return err(e),
                _ => return err("ERR Error loading ACLs from redis.acl_users"),
            };
            let users = match parse_rows(&rows) {
                Ok(u) => u,
                Err(e) => return err(format!("ERR {e}")),
            };
            let has_default = rows.iter().any(|(n, _)| n == b"default");
            match mutate(|u| {
                *u = users;
                Ok(())
            }) {
                Ok(((), before, after)) => {
                    if !has_default {
                        let g = crate::config::live_gucs();
                        if let Some(p) = g.password() {
                            set_default_password(Some(&p));
                        }
                    }
                    after_change(&before, &after, caller.client_id, caller.may_wait);
                    Response::Ok
                }
                Err(e) => err(e),
            }
        }
        "cat" if argc == 2 => Response::Value(Value::Array(
            crate::admin::ACL_ORDER
                .iter()
                .map(|c| Value::str(c))
                .collect(),
        )),
        "cat" if argc == 3 => {
            let Some(flag) = category_bit(&args[1]) else {
                let shown: String = String::from_utf8_lossy(&args[1])
                    .chars()
                    .take(128)
                    .collect();
                return err(format!("ERR Unknown category '{shown}'"));
            };
            Response::Value(Value::Array(
                crate::admin::implemented()
                    .filter(|c| {
                        META.by_name
                            .get(c.name)
                            .is_some_and(|&i| META.cats[i] & flag != 0)
                    })
                    .map(|c| Value::str(c.name))
                    .collect(),
            ))
        }
        "genpass" if argc == 2 || argc == 3 => {
            let bits = match args.get(1) {
                None => 256,
                Some(a) => match parse_long(a) {
                    Some(n) => n,
                    None => return err("ERR value is not an integer or out of range"),
                },
            };
            if bits <= 0 || bits > 4096 {
                return err(
                    "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
                );
            }
            Response::BulkString(random_hex(((bits + 3) / 4) as usize).into_bytes())
        }
        "log" if argc == 2 || argc == 3 => {
            let mut count = 10i64;
            if let Some(a) = args.get(1) {
                if eq(a, "reset") {
                    log_reset();
                    return Response::Ok;
                }
                match parse_long(a) {
                    Some(n) => count = n.max(0),
                    None => return err("ERR value is not an integer or out of range"),
                }
            }
            let now = crate::server::now_ms();
            Response::Value(Value::Array(
                log_entries()
                    .into_iter()
                    .take(count as usize)
                    .map(|e| {
                        Value::Map(vec![
                            (Value::str("count"), Value::Int(e.count as i64)),
                            (
                                Value::str("reason"),
                                Value::str(Reason::from_code(e.reason).name()),
                            ),
                            (Value::str("context"), Value::str(Context::name(e.context))),
                            (Value::str("object"), Value::Bulk(e.object)),
                            (Value::str("username"), Value::Bulk(e.username)),
                            (
                                Value::str("age-seconds"),
                                Value::Double((now - e.ctime) as f64 / 1000.0),
                            ),
                            (Value::str("client-info"), Value::Bulk(e.cinfo)),
                            (Value::str("entry-id"), Value::Int(e.entry_id)),
                            (Value::str("timestamp-created"), Value::Int(e.created)),
                            (Value::str("timestamp-last-updated"), Value::Int(e.ctime)),
                        ])
                    })
                    .collect(),
            ))
        }
        "dryrun" if argc >= 4 => {
            let st = current();
            let Some(user) = st.get(&args[1]) else {
                return err(format!(
                    "ERR User '{}' not found",
                    String::from_utf8_lossy(&args[1])
                ));
            };
            let argv = &args[2..];
            let Some(id) = lookup_argv(argv) else {
                return err(format!(
                    "ERR Command '{}' not found",
                    String::from_utf8_lossy(&argv[0])
                ));
            };
            if !crate::commands::arity_ok(spec(id).arity, argv.len()) {
                return err(format!(
                    "ERR wrong number of arguments for '{}' command",
                    spec(id).name
                ));
            }
            match user.check(id, argv) {
                Ok(()) => Response::Ok,
                Err(d) => {
                    Response::BulkString(denial(user, id, argv, d).message(true).into_bytes())
                }
            }
        }
        "help" if argc == 2 => Response::Value(Value::Array(
            HELP.iter()
                .map(|l| Value::Simple((*l).to_string()))
                .collect(),
        )),
        _ => syntax_error(sub_raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<Vec<u8>> {
        args.iter().map(|a| a.as_bytes().to_vec()).collect()
    }

    fn user(rules: &[&str]) -> User {
        let mut u = User::new(b"u", 9);
        u.set_all(&v(rules)).unwrap();
        u
    }

    fn cmds(u: &User) -> String {
        String::from_utf8(u.selectors[0].command_rules()).unwrap()
    }

    fn id(name: &str) -> usize {
        lookup_name(name.as_bytes()).unwrap()
    }

    fn allowed(u: &User, argv: &[&str]) -> Result<(), Deny> {
        let argv = v(argv);
        u.check(lookup_argv(&argv).unwrap(), &argv)
    }

    #[test]
    fn the_table_fits_the_bitmap() {
        assert!(COMMANDS.len() < FUTURE);
    }

    #[test]
    fn sha256_matches_redis() {
        // `ACL SETUSER x >pw` on Redis 7.2 stores this hash.
        assert_eq!(
            sha256_hex(b"pw"),
            "30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn default_user_is_described_as_redis_does() {
        let d = User::default_user();
        assert_eq!(d.describe(), b"on nopass sanitize-payload ~* &* +@all");
        assert!(d.unrestricted());
    }

    #[test]
    fn a_new_user_is_off_with_nothing() {
        let u = User::new(b"x", 2);
        assert_eq!(u.describe(), b"off sanitize-payload resetchannels -@all");
    }

    #[test]
    fn description_matches_redis_for_every_rule_kind() {
        // The same rules on Redis 7.2.
        let u = user(&[
            "on",
            ">pw",
            "~foo:*",
            "%R~r:*",
            "&ch:*",
            "+get",
            "+set",
            "-@dangerous",
            "+select|0",
            "(+hget ~h:*)",
        ]);
        assert_eq!(
            String::from_utf8(u.describe()).unwrap(),
            "on sanitize-payload #30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4 \
             ~foo:* %R~r:* resetchannels &ch:* -@all +get +set -@dangerous +select|0 \
             (~h:* resetchannels -@all +hget)"
        );
    }

    #[test]
    fn rules_compact_as_redis_does() {
        let cases: &[(&[&str], &str)] = &[
            (
                &["+@all", "-@hash", "-@slow", "+hget"],
                "+@all -@hash -@slow +hget",
            ),
            (
                &["+@all", "-@hash", "-@slow", "+hget", "-@hash"],
                "+@all -@slow +hget -@hash",
            ),
            (&["-@all", "+@HASH", "+@hash", "+@HaSh"], "-@all +@hash"),
            (&["-@all", "+HGET", "+hget", "+hGeT"], "-@all +hget"),
            (
                &["-@all", "+hget", "-hset", "+hset", "-hget"],
                "-@all +hset -hget",
            ),
            (
                &[
                    "-@all",
                    "+client|list",
                    "+client|list",
                    "+config|get",
                    "+config",
                    "+acl|list",
                    "-acl",
                ],
                "-@all +client|list +config -acl",
            ),
            (
                &[
                    "-@all",
                    "+select|0",
                    "+select|0",
                    "+debug|segfault",
                    "+debug",
                ],
                "-@all +select|0 +debug",
            ),
            (&["-@all", "-@dangerous"], "-@all -@dangerous"),
            (
                &["-@all", "+config", "+config|get", "-config|set", "+config"],
                "-@all +config",
            ),
            (&["+@all", "-client|kill"], "+@all -client|kill"),
            (&["-@all", "-@string", "-@slow", "+@all"], "+@all"),
        ];
        for (rules, want) in cases {
            assert_eq!(cmds(&user(rules)), *want, "{rules:?}");
        }
    }

    #[test]
    fn errors_match_redis() {
        let mut u = User::new(b"x", 2);
        let e = |u: &mut User, r: &[&str]| u.set_all(&v(r)).unwrap_err();
        assert_eq!(
            e(&mut u, &["+nonexist"]),
            "Error in ACL SETUSER modifier '+nonexist': Unknown command or category name in ACL"
        );
        assert!(
            e(&mut u, &["+config|get|x"])
                .ends_with("Allowing first-arg of a subcommand is not supported")
        );
        assert!(e(&mut u, &["+config|asdf"]).ends_with("Unknown command or category name in ACL"));
        assert!(e(&mut u, &["-get|a|b"]).ends_with("Unknown command or category name in ACL"));
        assert!(e(&mut u, &["+debug|"]).ends_with("Syntax error"));
        assert!(e(&mut u, &["badop"]).ends_with("Syntax error"));
        assert!(e(&mut u, &["<nope"]).ends_with("does not exist"));
        assert!(e(&mut u, &["#abc"]).ends_with("lowercase hexadecimal characters"));
        assert_eq!(
            e(&mut u, &["(+PING", "(+SELECT", "(+DEL"]),
            "Unmatched parenthesis in acl selector starting at '(+PING'."
        );
        assert!(e(&mut u, &["(this-is-invalid)"]).contains("'(this-is-invalid)': Syntax error"));
        assert!(e(&mut u, &["(&* &fail)"]).contains("Adding a pattern after the"));
        assert!(
            e(&mut u, &["(+PING", "(+SELECT", "(+DEL", ")", ")", ")"])
                .starts_with("Error in ACL SETUSER modifier")
        );
        assert!(e(&mut u, &[" () "]).ends_with("Syntax error"));
        assert!(e(&mut u, &["("]).starts_with("Unmatched parenthesis"));
        u.set_all(&v(&["~*"])).unwrap();
        assert!(e(&mut u, &["~a"]).contains("'allkeys'"));
        // A failed SETUSER changes nothing.
        let before = u.clone();
        let _ = u.set_all(&v(&["on", "+get", "nonsense"]));
        assert_eq!(u, before);
    }

    #[test]
    fn passwords_hash_and_remove() {
        let mut u = user(&["on", ">a", ">b"]);
        assert!(u.accepts(b"a") && u.accepts(b"b") && !u.accepts(b"c"));
        u.set_all(&v(&["<a"])).unwrap();
        assert!(!u.accepts(b"a"));
        u.set_all(&v(&[&format!("!{}", sha256_hex(b"b"))])).unwrap();
        assert!(!u.accepts(b"b"));
        u.set_all(&v(&["nopass"])).unwrap();
        assert!(u.accepts(b"anything"));
        u.set_all(&v(&["off"])).unwrap();
        assert!(!u.accepts(b"anything"));
    }

    #[test]
    fn commands_categories_and_subcommands() {
        let u = user(&["+@all", "-client", "+client|id"]);
        assert!(allowed(&u, &["client", "id"]).is_ok());
        assert_eq!(
            allowed(&u, &["client", "kill", "type", "master"]),
            Err(Deny::Cmd)
        );
        let u = user(&["-@all", "+@set"]);
        assert!(allowed(&u, &["sadd", "k", "m"]).is_err()); // no keys
        let u = user(&["-@all", "+@set", "~*"]);
        assert!(allowed(&u, &["sadd", "k", "m"]).is_ok());
        assert_eq!(allowed(&u, &["set", "k", "v"]), Err(Deny::Cmd));
        let u = user(&["-@all", "+select|0"]);
        assert!(allowed(&u, &["select", "0"]).is_ok());
        assert_eq!(allowed(&u, &["select", "1"]), Err(Deny::Cmd));
        // AUTH, HELLO, QUIT and RESET are always allowed.
        let u = user(&["-@all"]);
        assert!(allowed(&u, &["auth", "a", "b"]).is_ok());
        assert!(allowed(&u, &["hello"]).is_ok());
    }

    #[test]
    fn keys_follow_the_key_specs() {
        let u = user(&["+@all", "%R~read*", "%W~write*", "%RW~rw*"]);
        let ok = |a: &[&str]| allowed(&u, a).is_ok();
        assert!(ok(&["get", "read"]));
        assert!(!ok(&["get", "write"]));
        assert!(ok(&["lpush", "write", "1"]));
        assert!(!ok(&["lpush", "read", "1"]));
        // SET is overwrite-only unless GET.
        assert!(ok(&["set", "write", "v"]));
        assert!(!ok(&["set", "write", "v", "get"]));
        assert!(ok(&["set", "rw", "v", "get"]));
        assert!(ok(&["copy", "read", "write"]));
        assert!(!ok(&["copy", "write", "read"]));
        // Existence tests need either.
        assert!(ok(&["exists", "read"]) && ok(&["exists", "write"]) && !ok(&["exists", "nothing"]));
        assert!(ok(&["hexists", "write", "f"]));
        assert!(!ok(&["zcount", "write", "0", "1"]));
        // Native key finders.
        assert!(ok(&["eval", "", "1", "rw1"]));
        assert!(!ok(&["eval", "", "1", "read"]));
        assert!(ok(&["eval_ro", "", "1", "read"]));
        assert!(ok(&["eval", "", "0", "read"]));
        assert!(ok(&["eval", "", "3", "rw", "read"]));
        assert!(ok(&["sort", "read", "store", "write"]));
        assert!(!ok(&["sort", "read", "store", "read"]));
        assert!(ok(&["migrate", "h", "p", "rw", "0", "500"]));
        assert!(!ok(&["migrate", "h", "p", "read", "0", "500"]));
        assert!(ok(&[
            "migrate", "h", "p", "", "0", "5000", "AUTH2", "KEYS", "123", "KEYS", "rw"
        ]));
        assert_eq!(
            allowed(
                &u,
                &[
                    "migrate", "h", "p", "", "0", "5000", "AUTH", "KEYS", "KEYS", "write"
                ]
            ),
            Err(Deny::Key(9))
        );
        assert!(ok(&[
            "georadius",
            "read",
            "1",
            "2",
            "3",
            "M",
            "STOREDIST",
            "write"
        ]));
        assert!(!ok(&[
            "georadius",
            "write1",
            "1",
            "2",
            "3",
            "M",
            "STORE",
            "write2"
        ]));
        assert!(ok(&["bitfield", "read", "get", "u4", "0"]));
        assert!(!ok(&["bitfield", "read", "set", "u4", "0", "1"]));
    }

    #[test]
    fn channels_match_patterns_and_patterns_match_literally() {
        let u = user(&["+@all", "resetchannels", "&foo:1", "&bar:*"]);
        assert!(allowed(&u, &["publish", "bar:2", "m"]).is_ok());
        assert_eq!(allowed(&u, &["publish", "zap", "m"]), Err(Deny::Channel(1)));
        assert!(allowed(&u, &["subscribe", "foo:1", "bar:x"]).is_ok());
        assert!(allowed(&u, &["psubscribe", "bar:*"]).is_ok());
        assert_eq!(
            allowed(&u, &["psubscribe", "bar:baz"]),
            Err(Deny::Channel(1))
        );
        assert!(allowed(&u, &["unsubscribe", "zap"]).is_ok());
    }

    #[test]
    fn selectors_are_tried_in_turn_and_the_last_key_is_reported() {
        let u = user(&["+mget", "~key", "(+mget ~key ~otherkey)"]);
        assert_eq!(allowed(&u, &["get", "key"]), Err(Deny::Cmd));
        assert_eq!(
            allowed(&u, &["mget", "otherkey", "someotherkey"]),
            Err(Deny::Key(2))
        );
        assert_eq!(
            allowed(&u, &["mget", "key", "otherkey", "someotherkey"]),
            Err(Deny::Key(3))
        );
        let u = user(&["(+@write ~write::*)", "(+@read ~read::*)"]);
        assert!(allowed(&u, &["set", "write::foo", "bar"]).is_ok());
        assert_eq!(allowed(&u, &["get", "write::foo"]), Err(Deny::Key(1)));
        // Read on one selector and write on another do not combine.
        let u = user(&["(%R~read* +@all)", "(%W~write* +@all)"]);
        assert!(allowed(&u, &["copy", "read", "write"]).is_err());
    }

    #[test]
    fn getuser_splits_root_and_selectors() {
        let u = user(&[
            "-@all",
            "+get",
            "resetchannels",
            "&channel1",
            "%R~foo1",
            "%W~bar1",
            "~baz1",
            "(-@all +set resetchannels &channel2 %R~foo2 %W~bar2 ~baz2)",
        ]);
        let s = &u.selectors;
        assert_eq!(s[0].keys_text(), b"%R~foo1 %W~bar1 ~baz1");
        assert_eq!(s[0].channels_text(), b"&channel1");
        assert_eq!(s[1].command_rules(), b"-@all +set");
        assert_eq!(s[1].keys_text(), b"%R~foo2 %W~bar2 ~baz2");
        let u = user(&["%R~foo", "%W~foo", "%RW~bar"]);
        assert_eq!(u.selectors[0].keys_text(), b"~foo ~bar");
        let u = user(&["reset", "()"]);
        assert_eq!(u.selectors[1].command_rules(), b"-@all");
        assert_eq!(u.selectors[1].keys_text(), b"");
    }

    #[test]
    fn descriptions_read_back_to_the_same_user() {
        for rules in [
            &[
                "on",
                ">pw",
                "~a*",
                "%R~b",
                "&c",
                "+@all",
                "-@dangerous",
                "+select|0",
                "(~x +get &y)",
            ][..],
            &["reset"],
            &[
                "on",
                "nopass",
                "skip-sanitize-payload",
                "allkeys",
                "allchannels",
                "-@all",
                "+config|get",
            ],
        ] {
            let u = user(rules);
            let back = from_description(b"u", 9, &u.describe()).unwrap();
            assert_eq!(back, u, "{rules:?}");
        }
        let mut users = BTreeMap::new();
        users.insert(b"default".to_vec(), User::default_user());
        users.insert(b"u".to_vec(), user(&["on", "+get"]));
        assert_eq!(deserialize(&serialize(&users)), users);
    }

    #[test]
    fn channel_revocation_is_detected() {
        let old = user(&["resetchannels", "&foo:1", "&bar:*"]);
        let wider = user(&["resetchannels", "&foo:1", "&bar:*", "&zoo:*"]);
        let narrower = user(&["resetchannels", "&foo:1"]);
        assert!(wider.keeps_channels_of(&old));
        assert!(!narrower.keeps_channels_of(&old));
        assert!(user(&["allchannels"]).keeps_channels_of(&old));
        assert!(!old.keeps_channels_of(&user(&["allchannels"])));
        assert!(narrower.allows_subscriptions(&v(&["foo:1"]), &[]));
        assert!(!narrower.allows_subscriptions(&[], &v(&["bar:*"])));
    }

    #[test]
    fn split_args_handles_quotes() {
        assert_eq!(
            split_args(b" a  \"b c\" 'd' ").unwrap(),
            v(&["a", "b c", "d"])
        );
        assert_eq!(split_args(b"\"\\x41\\n\"").unwrap(), vec![b"A\n".to_vec()]);
        assert!(split_args(b"\"open").is_none());
        assert!(split_args(b"\"a\"b").is_none());
    }

    #[test]
    fn rows_load_like_an_acl_file() {
        let rows = vec![
            (b"alice".to_vec(), b"on >alice ~* &* +@all".to_vec()),
            (
                b"bob".to_vec(),
                b"on -@all +@set +acl ~set* &* >bob".to_vec(),
            ),
        ];
        let users = parse_rows(&rows).unwrap();
        assert!(users["alice".as_bytes()].accepts(b"alice"));
        assert!(users.contains_key(b"default".as_slice()));
        let bad = vec![(b"x".to_vec(), b"on +nosuch".to_vec())];
        assert!(parse_rows(&bad).unwrap_err().contains(
            "redis.acl_users:1: Error in applying operation '+nosuch': Unknown command or category name in ACL. WARNING: ACL errors detected"
        ));
        let _ = id("get");
    }

    #[test]
    fn log_entries_round_trip() {
        let e = Entry {
            count: 3,
            reason: 2,
            context: 1,
            object: b"k".to_vec(),
            username: b"u".to_vec(),
            ctime: 5,
            cinfo: b"id=1".to_vec(),
            entry_id: 7,
            created: 4,
        };
        assert_eq!(
            decode_entries(&encode_entries(&[e.clone(), e.clone()])),
            vec![e.clone(), e]
        );
    }
}
