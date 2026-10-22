//! Lua scripting: `EVAL`, `EVALSHA`, `SCRIPT`, and the function libraries of
//! `FUNCTION` and `FCALL`.
//!
//! A script runs whole on the dispatcher, in a Lua 5.1 state that thread
//! owns, so it is atomic against every other command that dispatcher serves.
//! Its `redis.call` builds a [`Command`] and sends it to whichever backend
//! serves the script's current database, as a composite command does — one
//! script reads and writes both halves.
//!
//! What a script sees is Redis's environment, rebuilt: the same libraries
//! (`cjson`, `cmsgpack`, `struct` and `bit` are Redis's own C sources, see
//! `build.rs`), the same `redis` API, the same conversions between replies
//! and Lua values, the same error texts. Redis's Lua is patched to make
//! tables read-only; here every protected table is an empty proxy whose
//! metatable forwards reads and refuses writes, which gives scripts the same
//! "Attempt to modify a readonly table" without patching Lua.
//!
//! Each worker has its own Lua states, so what has to be shared lives in SQL:
//! the script cache in `redis.scripts`, the function libraries in
//! `redis.functions`. A worker that changes either bumps a generation counter
//! in shared memory once its transaction commits, and every worker rebuilds
//! its copy when it sees the counter move.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use mlua::{
    Function, HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Table, Value as Lv, VmState,
};

use crate::commands::{Command, Response, Value};
use mlua::chunk::ChunkMode;

// ───────────────────────────────── flags ────────────────────────────────────

pub const FLAG_NO_WRITES: u32 = 1;
pub const FLAG_ALLOW_OOM: u32 = 2;
pub const FLAG_ALLOW_STALE: u32 = 4;
pub const FLAG_NO_CLUSTER: u32 = 8;
pub const FLAG_ALLOW_CROSS_SLOT: u32 = 16;
/// A script with no `#!lua` line: the pre-7.0 rules, flags ignored.
const FLAG_EVAL_COMPAT: u32 = 32;

/// `scripts_flags_def`, in Redis's order — `FUNCTION LIST` reports them so.
const SCRIPT_FLAGS: [(&str, u32); 5] = [
    ("no-writes", FLAG_NO_WRITES),
    ("allow-oom", FLAG_ALLOW_OOM),
    ("allow-stale", FLAG_ALLOW_STALE),
    ("no-cluster", FLAG_NO_CLUSTER),
    ("allow-cross-slot-keys", FLAG_ALLOW_CROSS_SLOT),
];

/// The chunk name of the Lua this module writes itself. The error handler
/// skips its frames, so an error raised through `redis.call` is reported at
/// the user's line, not at the wrapper's.
const INTERNAL: &str = "=[pg_redis]";
/// How long a library's top level may run in `FUNCTION LOAD`.
const LOAD_TIMEOUT: Duration = Duration::from_millis(500);
/// Nesting past which a returned table is cut off, as Redis's Lua stack
/// limit cuts off a table that contains itself.
const MAX_REPLY_DEPTH: usize = 1000;
const KILLED: &str = "ERR Script killed by user with SCRIPT KILL...";
const LOAD_TIMED_OUT: &str = "ERR FUNCTION LOAD timeout";
const NOSCRIPT: &str = "NOSCRIPT No matching script. Please use EVAL.";
const RO_WITH_WRITE_FLAG: &str = "ERR Can not execute a script with write flag using *_ro command.";

// ──────────────────────────────── commands ──────────────────────────────────

/// The keys-and-arguments tail every script invocation shares.
#[derive(Debug, Clone)]
pub struct Invocation {
    /// The whole command as sent, for `FUNCTION STATS`.
    pub argv: Vec<Vec<u8>>,
    pub numkeys: Vec<u8>,
    pub rest: Vec<Vec<u8>>,
    /// `EVAL_RO`, `EVALSHA_RO`, `FCALL_RO`.
    pub ro: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Policy {
    Append,
    Replace,
    Flush,
}

#[derive(Debug, Clone)]
pub enum ScriptCmd {
    Eval {
        body: Vec<u8>,
        inv: Invocation,
    },
    EvalSha {
        sha: String,
        inv: Invocation,
    },
    Fcall {
        name: Vec<u8>,
        inv: Invocation,
    },
    Load(Vec<u8>),
    Exists(Vec<Vec<u8>>),
    Flush,
    FunctionLoad {
        replace: bool,
        code: Vec<u8>,
    },
    FunctionDelete(Vec<u8>),
    FunctionFlush,
    FunctionList {
        pattern: Option<Vec<u8>>,
        with_code: bool,
    },
    FunctionDump,
    FunctionRestore {
        payload: Vec<u8>,
        policy: Policy,
    },
    /// Internal, never parsed: the flags of a cached script (`EVALSHA`) or a
    /// function (`FCALL`), for deciding whether a write pause holds it.
    /// Answers the flag bits, or -1 when there is no such script.
    Flags {
        sha: Option<String>,
        function: Option<Vec<u8>>,
    },
}

/// Whether a script with these flags is a write as `CLIENT PAUSE WRITE` sees
/// it: anything not declared `no-writes`, the flagless pre-7.0 form included.
pub fn flags_write(flags: i64) -> bool {
    flags < 0 || flags as u32 & FLAG_EVAL_COMPAT != 0 || flags as u32 & FLAG_NO_WRITES == 0
}

pub const NOREPLICAS: &str = "NOREPLICAS Not enough good replicas to write.";

/// `scriptPrepareForRun`'s refusals, for a script declared with flags (the
/// flagless pre-7.0 form is checked command by command instead): one that
/// may write, with too few replicas, or past `maxmemory` without
/// `allow-oom`.
fn refused_before_run(flags: u32, compat: bool, host: &mut dyn Host) -> Option<Response> {
    if compat || flags & FLAG_NO_WRITES != 0 {
        return None;
    }
    if crate::server::too_few_replicas() {
        return Some(Response::Error(NOREPLICAS.into()));
    }
    if flags & FLAG_ALLOW_OOM == 0 && !host.room_to_write() {
        return Some(crate::commands::mem_out_of_memory());
    }
    None
}

/// Whether the commands a script runs may grow memory past `maxmemory`:
/// `allow-oom`, which `no-writes` implies.
fn allows_oom(flags: u32, compat: bool) -> bool {
    !compat && flags & (FLAG_ALLOW_OOM | FLAG_NO_WRITES) != 0
}

/// The flags an `EVAL` body declares, read straight from its shebang.
pub fn body_flags(body: &[u8]) -> i64 {
    shebang_flags(body).map_or(-1, |(f, _)| f as i64)
}

fn upper(a: &[u8]) -> String {
    String::from_utf8_lossy(a).to_ascii_uppercase()
}

fn arity(name: &str) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_ascii_lowercase()
    )
}

/// The scripting commands. `None` for any other name. `SCRIPT KILL`,
/// `FUNCTION KILL`, `FUNCTION STATS` and the help texts are answered on the
/// connection's thread — they must work while a script holds the dispatcher —
/// so they come back as admin commands.
/// `raw_name` is the command name as the client spelled it, which
/// `FUNCTION STATS` reports back.
pub fn parse(name: &str, raw_name: &[u8], args: &[Vec<u8>]) -> Option<Result<Command, String>> {
    let invocation = |ro: bool| {
        if args.len() < 2 {
            return Err(arity(name));
        }
        Ok(Invocation {
            argv: std::iter::once(raw_name.to_vec())
                .chain(args.iter().cloned())
                .collect(),
            numkeys: args[1].clone(),
            rest: args[2..].to_vec(),
            ro,
        })
    };
    let cmd = match name {
        "EVAL" | "EVAL_RO" => invocation(name == "EVAL_RO").map(|inv| ScriptCmd::Eval {
            body: args[0].clone(),
            inv,
        }),
        "EVALSHA" | "EVALSHA_RO" => {
            invocation(name == "EVALSHA_RO").map(|inv| ScriptCmd::EvalSha {
                sha: String::from_utf8_lossy(&args[0]).to_ascii_lowercase(),
                inv,
            })
        }
        "FCALL" | "FCALL_RO" => invocation(name == "FCALL_RO").map(|inv| ScriptCmd::Fcall {
            name: args[0].clone(),
            inv,
        }),
        "SCRIPT" => return Some(parse_script(args)),
        "FUNCTION" => return Some(parse_function(args)),
        _ => return None,
    };
    Some(cmd.map(Command::Script))
}

fn admin(name: &str, args: &[Vec<u8>]) -> Result<Command, String> {
    Ok(Command::Admin(crate::admin::Admin {
        name: name.to_string(),
        args: args.to_vec(),
        saved: false,
    }))
}

fn parse_script(args: &[Vec<u8>]) -> Result<Command, String> {
    let Some(sub) = args.first().map(|a| upper(a)) else {
        return Err(arity("SCRIPT"));
    };
    let cmd = match sub.as_str() {
        "LOAD" if args.len() == 2 => ScriptCmd::Load(args[1].clone()),
        "EXISTS" if args.len() >= 2 => ScriptCmd::Exists(args[1..].to_vec()),
        "FLUSH" => match args.get(1).map(|a| upper(a)).as_deref() {
            None | Some("SYNC") | Some("ASYNC") if args.len() <= 2 => ScriptCmd::Flush,
            _ => return Err("ERR SCRIPT FLUSH only support SYNC|ASYNC option".into()),
        },
        "KILL" | "HELP" | "DEBUG" => return admin("SCRIPT", args),
        _ => {
            return Err(format!(
                "ERR unknown subcommand or wrong number of arguments for '{}'. Try SCRIPT HELP.",
                String::from_utf8_lossy(&args[0])
            ));
        }
    };
    Ok(Command::Script(cmd))
}

fn parse_function(args: &[Vec<u8>]) -> Result<Command, String> {
    let Some(sub) = args.first().map(|a| upper(a)) else {
        return Err(arity("FUNCTION"));
    };
    let syntax = || {
        format!(
            "ERR unknown subcommand or wrong number of arguments for '{}'. Try FUNCTION HELP.",
            String::from_utf8_lossy(&args[0])
        )
    };
    let cmd = match sub.as_str() {
        "LOAD" if args.len() >= 2 => {
            let mut replace = false;
            for opt in &args[1..args.len() - 1] {
                if opt.eq_ignore_ascii_case(b"REPLACE") {
                    replace = true;
                } else {
                    return Err(format!(
                        "ERR Unknown option given: {}",
                        String::from_utf8_lossy(opt)
                    ));
                }
            }
            ScriptCmd::FunctionLoad {
                replace,
                code: args[args.len() - 1].clone(),
            }
        }
        "DELETE" if args.len() == 2 => ScriptCmd::FunctionDelete(args[1].clone()),
        "FLUSH" if args.len() <= 2 => match args.get(1).map(|a| upper(a)).as_deref() {
            None | Some("SYNC") | Some("ASYNC") => ScriptCmd::FunctionFlush,
            _ => return Err("ERR FUNCTION FLUSH only supports SYNC|ASYNC option".into()),
        },
        "LIST" => {
            let (mut pattern, mut with_code) = (None, false);
            let mut i = 1;
            while i < args.len() {
                let a = &args[i];
                if !with_code && a.eq_ignore_ascii_case(b"WITHCODE") {
                    with_code = true;
                } else if pattern.is_none() && a.eq_ignore_ascii_case(b"LIBRARYNAME") {
                    if i + 1 >= args.len() {
                        return Err("ERR library name argument was not given".into());
                    }
                    i += 1;
                    pattern = Some(args[i].clone());
                } else {
                    return Err(format!(
                        "ERR Unknown argument {}",
                        String::from_utf8_lossy(a)
                    ));
                }
                i += 1;
            }
            ScriptCmd::FunctionList { pattern, with_code }
        }
        "DUMP" if args.len() == 1 => ScriptCmd::FunctionDump,
        "RESTORE" if args.len() == 2 || args.len() == 3 => {
            let policy = match args.get(2).map(|a| upper(a)).as_deref() {
                None | Some("APPEND") => Policy::Append,
                Some("REPLACE") => Policy::Replace,
                Some("FLUSH") => Policy::Flush,
                Some(_) => {
                    return Err("ERR Wrong restore policy given, value should be either \
                                FLUSH, APPEND or REPLACE."
                        .into());
                }
            };
            ScriptCmd::FunctionRestore {
                payload: args[1].clone(),
                policy,
            }
        }
        "KILL" | "STATS" | "HELP" if args.len() == 1 => return admin("FUNCTION", args),
        _ => return Err(syntax()),
    };
    Ok(Command::Script(cmd))
}

/// The scripting commands answered on the connection's thread.
pub fn admin_execute(a: &crate::admin::Admin) -> Response {
    let sub = a.args.first().map(|s| upper(s)).unwrap_or_default();
    let result = match (a.name.as_str(), sub.as_str()) {
        ("SCRIPT", "KILL") => crate::server::kill_script(true).map(|_| Response::Ok),
        ("FUNCTION", "KILL") => crate::server::kill_script(false).map(|_| Response::Ok),
        ("FUNCTION", "STATS") => Ok(function_stats()),
        ("SCRIPT", "DEBUG") => match a.args.get(1).map(|s| upper(s)).as_deref() {
            Some("NO") if a.args.len() == 2 => Ok(Response::Ok),
            Some("YES") | Some("SYNC") if a.args.len() == 2 => {
                Err("ERR SCRIPT DEBUG is not supported by pg_redis".into())
            }
            _ => Err("ERR Use SCRIPT DEBUG YES/SYNC/NO".into()),
        },
        ("SCRIPT", "HELP") => Ok(help(SCRIPT_HELP)),
        ("FUNCTION", "HELP") => Ok(help(FUNCTION_HELP)),
        _ => Err("ERR syntax error".into()),
    };
    result.unwrap_or_else(Response::Error)
}

fn help(lines: &[&str]) -> Response {
    Response::Value(Value::Array(
        lines
            .iter()
            .map(|l| Value::Simple((*l).to_string()))
            .collect(),
    ))
}

const SCRIPT_HELP: &[&str] = &[
    "SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "DEBUG (YES|SYNC|NO)",
    "    Set the debug mode for subsequent scripts executed.",
    "EXISTS <sha1> [<sha1> ...]",
    "    Return information about the existence of the scripts in the script cache.",
    "FLUSH [ASYNC|SYNC]",
    "    Flush the Lua scripts cache. Very dangerous on replicas.",
    "    When called without the optional mode argument, the behavior is determined by the",
    "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
    "    * ASYNC: Asynchronously flush the scripts cache.",
    "    * SYNC: Synchronously flush the scripts cache.",
    "KILL",
    "    Kill the currently executing Lua script.",
    "LOAD <script>",
    "    Load a script into the scripts cache without executing it.",
    "HELP",
    "    Print this help.",
];

const FUNCTION_HELP: &[&str] = &[
    "FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "LOAD [REPLACE] <FUNCTION CODE>",
    "    Create a new library with the given library name and code.",
    "DELETE <LIBRARY NAME>",
    "    Delete the given library.",
    "LIST [LIBRARYNAME PATTERN] [WITHCODE]",
    "    Return general information on all the libraries:",
    "    * Library name",
    "    * The engine used to run the Library",
    "    * Library description",
    "    * Functions list",
    "    * Library code (if WITHCODE is given)",
    "    It also possible to get only function that matches a pattern using LIBRARYNAME argument.",
    "STATS",
    "    Return information about the current function running:",
    "    * Function name",
    "    * Command used to run the function",
    "    * Duration in MS that the function is running",
    "    If no function is running, return nil",
    "    In addition, returns a list of available engines.",
    "KILL",
    "    Kill the current running function.",
    "FLUSH [ASYNC|SYNC]",
    "    Delete all the libraries.",
    "    When called without the optional mode argument, the behavior is determined by the",
    "    lazyfree-lazy-user-flush configuration directive. Valid modes are:",
    "    * ASYNC: Asynchronously flush the libraries.",
    "    * SYNC: Synchronously flush the libraries.",
    "DUMP",
    "    Return a serialized payload representing the current libraries, can be restored using FUNCTION RESTORE command",
    "RESTORE <PAYLOAD> [FLUSH|APPEND|REPLACE]",
    "    Restore the libraries represented by the given payload, it is possible to give a restore policy to",
    "    control how to handle existing libraries (default APPEND):",
    "    * FLUSH: delete all existing libraries.",
    "    * APPEND: appends the restored libraries to the existing libraries. On collision, abort.",
    "    * REPLACE: appends the restored libraries to the existing libraries, On collision, replace the old",
    "      libraries with the new libraries (notice that even on this option there is a chance of failure",
    "      in case of functions name collision with another library).",
    "HELP",
    "    Print this help.",
];

fn function_stats() -> Response {
    let running = crate::server::running_script();
    if running.as_ref().is_some_and(|r| r.eval) {
        return Response::Error(
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."
                .into(),
        );
    }
    let running = match running {
        None => Value::Nil,
        Some(r) => Value::Map(vec![
            (Value::str("name"), Value::Bulk(r.name)),
            (
                Value::str("command"),
                Value::Array(r.argv.into_iter().map(Value::Bulk).collect()),
            ),
            (Value::str("duration_ms"), Value::Int(r.duration_ms)),
        ]),
    };
    let (libs, funcs) = crate::server::function_counts();
    Response::Value(Value::Map(vec![
        (Value::str("running_script"), running),
        (
            Value::str("engines"),
            Value::Map(vec![(
                Value::str("LUA"),
                Value::Map(vec![
                    (Value::str("libraries_count"), Value::Int(libs)),
                    (Value::str("functions_count"), Value::Int(funcs)),
                ]),
            )]),
        ),
    ]))
}

// ───────────────────────────────── host ─────────────────────────────────────

/// What a script needs from the dispatcher: a way to run a command against a
/// database, the SQL tables that hold the script cache and the libraries,
/// and a hook on the transaction's commit — the generation counters move
/// only once a change is visible to every other worker.
pub trait Host {
    fn exec(&mut self, db: u8, cmd: Command) -> Response;
    fn script_body(&mut self, sha: &str) -> Option<Vec<u8>>;
    fn save_script(&mut self, sha: &str, body: &[u8]);
    fn scripts_exist(&mut self, shas: &[String]) -> Vec<bool>;
    fn flush_scripts(&mut self);
    /// Every library's code, in name order.
    fn libraries(&mut self) -> Vec<Vec<u8>>;
    fn save_library(&mut self, name: &str, code: &[u8]);
    fn delete_library(&mut self, name: &str);
    fn flush_libraries(&mut self);
    fn on_commit(&mut self, f: fn());
    /// Whether a write may grow memory now: false past `maxmemory` with
    /// nothing the policy may evict.
    fn room_to_write(&mut self) -> bool {
        true
    }
    fn on_abort(&mut self, f: fn());
}

// ──────────────────────────── thread-local state ────────────────────────────

/// What the instruction hook checks.
enum Interrupt {
    None,
    Run(crate::server::KillProbe),
    Load(Instant),
}

thread_local! {
    static ENGINE: RefCell<Engine> = RefCell::new(Engine::default());
    static INTERRUPT: RefCell<Interrupt> = const { RefCell::new(Interrupt::None) };
    /// The error a kill or a load timeout raised. While set, `pcall` cannot
    /// swallow it: every protected call re-raises it on the way out.
    static PENDING: RefCell<Option<&'static str>> = const { RefCell::new(None) };
    /// `redis.setresp`: how replies convert to Lua for this run.
    static RESP3: Cell<bool> = const { Cell::new(false) };
    /// Set while a script's `redis.call` runs its command — Redis's
    /// `CLIENT_SCRIPT`, which a few commands answer differently under.
    static IN_CALL: Cell<bool> = const { Cell::new(false) };
    /// The ACL user of the client running the script, 0 for none: what
    /// `redis.call` checks a command against (`scriptVerifyACL`).
    static CALLER: Cell<u64> = const { Cell::new(0) };
    /// The `rand48` state behind `math.random`.
    static RAND48: Cell<u64> = const { Cell::new(0) };
    /// The functions a library's top level registers during `FUNCTION LOAD`.
    static LOADING: RefCell<Option<Vec<FuncInfo>>> = const { RefCell::new(None) };
    /// Keys a script's sub-commands wrote, for the WATCH counters the
    /// dispatcher bumps once the batch commits. `None` in the list means
    /// every key.
    static WRITES: RefCell<ScriptWrites> = const { RefCell::new(Vec::new()) };
    /// Set when a transaction that changed a library rolled back: the copy
    /// this worker holds may have it, the table does not.
    static FUNCTIONS_STALE: Cell<bool> = const { Cell::new(false) };
    static SCRIPTS_STALE: Cell<bool> = const { Cell::new(false) };
}

/// The keys scripts wrote, as `(origin, (db, key))`; `None` for every key.
pub type ScriptWrites = Vec<(u64, Option<(u8, Vec<u8>)>)>;

/// The writes scripts made since the last call, for the WATCH bump.
pub fn take_writes() -> ScriptWrites {
    WRITES.with(|w| std::mem::take(&mut *w.borrow_mut()))
}

fn record_writes(targets: Option<Vec<(u8, Vec<u8>)>>) {
    let origin = crate::tracking::origin();
    WRITES.with(|w| {
        let mut w = w.borrow_mut();
        match targets {
            None => w.push((origin, None)),
            Some(keys) => w.extend(keys.into_iter().map(|k| (origin, Some(k)))),
        }
    });
}

fn mark_functions_stale() {
    FUNCTIONS_STALE.with(|s| s.set(true));
}

fn mark_scripts_stale() {
    SCRIPTS_STALE.with(|s| s.set(true));
}

fn pending() -> Option<&'static str> {
    PENDING.with(|p| *p.borrow())
}

fn check_interrupt() -> mlua::Result<VmState> {
    let hit = INTERRUPT.with(|i| match &*i.borrow() {
        Interrupt::None => None,
        Interrupt::Run(probe) => probe.killed().then_some(KILLED),
        Interrupt::Load(deadline) => (Instant::now() > *deadline).then_some(LOAD_TIMED_OUT),
    });
    match hit.or_else(pending) {
        Some(msg) => {
            PENDING.with(|p| *p.borrow_mut() = Some(msg));
            Err(mlua::Error::runtime(msg))
        }
        None => Ok(VmState::Continue),
    }
}

/// Clears the interrupt state however the run ends.
struct InterruptGuard;

impl InterruptGuard {
    fn set(i: Interrupt) -> InterruptGuard {
        INTERRUPT.with(|s| *s.borrow_mut() = i);
        PENDING.with(|p| *p.borrow_mut() = None);
        InterruptGuard
    }
}

impl Drop for InterruptGuard {
    fn drop(&mut self) {
        INTERRUPT.with(|s| *s.borrow_mut() = Interrupt::None);
        PENDING.with(|p| *p.borrow_mut() = None);
    }
}

// ─────────────────────────────── sandbox ────────────────────────────────────

unsafe extern "C-unwind" {
    fn luaopen_cjson(l: *mut mlua::ffi::lua_State) -> std::ffi::c_int;
    fn luaopen_struct(l: *mut mlua::ffi::lua_State) -> std::ffi::c_int;
    fn luaopen_cmsgpack(l: *mut mlua::ffi::lua_State) -> std::ffi::c_int;
    fn luaopen_bit(l: *mut mlua::ffi::lua_State) -> std::ffi::c_int;
}

/// The Lua that builds a script's environment, run once per state with the
/// real globals still in place. It returns the read-only proxy that becomes
/// the globals, the function that runs a script under the error handler,
/// the proxy's metatable, and — for the function engine — the API a library
/// sees while it loads.
const SANDBOX: &str = r#"
local G, native, functions = ...
local error, type, tostring, rawget, rawset, setmetatable, next, select, pcall, xpcall, setfenv, loadstring, concat, floor =
      error, type, tostring, rawget, rawset, setmetatable, next, select, pcall, xpcall, setfenv, loadstring, table.concat, math.floor
local pending, handler, as_error = native.pending, native.handler, native.as_error
local string_lib = string

local readonly = setmetatable({}, {__mode = 'k'})
local function deny() error('Attempt to modify a readonly table', 2) end
local function raise(msg) error({err = msg}, 0) end

local function guard_of()
  local g = {}
  readonly[g] = true
  setmetatable(g, {__newindex = deny, __metatable = g})
  return g
end

local memo = {}
local function protect(t)
  if memo[t] then return memo[t] end
  local p = {}
  memo[t] = p
  readonly[p] = true
  for k, v in next, t do
    if type(v) == 'table' then rawset(t, k, protect(v)) end
  end
  local meta = {__index = t, __newindex = deny, __metatable = guard_of()}
  setmetatable(p, meta)
  return p, meta
end

local function nonexistent(_, k)
  error("Script attempted to access nonexistent global variable '" .. tostring(k) .. "'", 2)
end

-- pcall that a kill or a load timeout cannot be caught by, and that hands
-- back the message of a Redis error table rather than the table.
local function after_pcall(ok, ...)
  local p = pending()
  if p then raise(p) end
  if ok then return true, ... end
  local e = as_error((...))
  if type(e) == 'table' then
    local m = e.err
    if type(m) == 'string' then return false, m end
  end
  return false, e
end
local function after_xpcall(ok, ...)
  local p = pending()
  if p then raise(p) end
  return ok, ...
end

-- redis.call and redis.pcall are set for each run, from Rust.
local redis = {}
redis.sha1hex = function(...)
  if select('#', ...) ~= 1 then raise('ERR wrong number of arguments') end
  return native.sha1hex((...))
end
redis.error_reply = function(...)
  if select('#', ...) ~= 1 or type((...)) ~= 'string' then
    return {err = 'ERR wrong number or type of arguments'}
  end
  local e = ...
  if e:sub(1, 1) ~= '-' then e = '-' .. e end
  return {err = native.error_text(e)}
end
redis.status_reply = function(...)
  if select('#', ...) ~= 1 or type((...)) ~= 'string' then
    return {err = 'ERR wrong number or type of arguments'}
  end
  return {ok = (...)}
end
local function log(...)
  local e = native.log(select('#', ...), ...)
  if e then raise(e) end
end
redis.log = log
redis.setresp = function(...)
  local e = native.setresp(select('#', ...), ...)
  if e then raise(e) end
end
redis.set_repl = function(...)
  if select('#', ...) ~= 1 then raise('ERR redis.set_repl() requires one argument.') end
  local f = tonumber((...)) or 0
  if f ~= floor(f) or f < 0 or f > 3 then
    raise('ERR Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.')
  end
end
redis.acl_check_cmd = function(...)
  local ok, e = native.acl_check_cmd(...)
  if e then raise(e) end
  return ok
end
redis.REPL_NONE, redis.REPL_AOF, redis.REPL_SLAVE, redis.REPL_REPLICA, redis.REPL_ALL = 0, 1, 2, 2, 3
redis.LOG_DEBUG, redis.LOG_VERBOSE, redis.LOG_NOTICE, redis.LOG_WARNING = 0, 1, 2, 3
redis.REDIS_VERSION, redis.REDIS_VERSION_NUM = native.version, native.version_num
if not functions then
  redis.replicate_commands = function() return true end
  redis.breakpoint = function() return false end
  redis.debug = function() end
end
G.redis = redis

G.pcall = function(f, ...) return after_pcall(pcall(f, ...)) end
G.xpcall = function(f, h)
  return after_xpcall(xpcall(f, function(e)
    if pending() then return e end
    return h(as_error(e))
  end))
end
G.setmetatable = function(t, m)
  if readonly[t] then deny() end
  return setmetatable(t, m)
end
G.rawset = function(t, k, v)
  if readonly[t] then deny() end
  return rawset(t, k, v)
end
-- Redis's Lua refuses precompiled chunks.
G.loadstring = function(s, name)
  if type(s) == 'string' and s:byte(1) == 27 then
    return nil, 'attempt to load a binary chunk'
  end
  return loadstring(s, name)
end
G.load = function(f, name)
  local parts = {}
  while true do
    local piece = f()
    if piece == nil or piece == '' then break end
    parts[#parts + 1] = piece
  end
  return G.loadstring(concat(parts), name)
end
G.math.random = native.random
G.math.randomseed = native.randomseed
G.dofile, G.loadfile, G.print, G.debug, G.module, G.require, G.package = nil, nil, nil, nil, nil, nil, nil
if not functions then G.__redis__err__handler = handler end

local lib_api
if functions then
  local lib_redis = {
    register_function = function(...)
      local e = native.register(...)
      if e then raise(e) end
    end,
    log = log,
    LOG_DEBUG = 0, LOG_VERBOSE = 1, LOG_NOTICE = 2, LOG_WARNING = 3,
    REDIS_VERSION = native.version, REDIS_VERSION_NUM = native.version_num,
  }
  setmetatable(lib_redis, {__index = nonexistent})
  lib_api = setmetatable({redis = lib_redis}, {__index = nonexistent})
end

setmetatable(G, {__index = nonexistent})
local env, meta = protect(G)
if lib_api then lib_api = protect(lib_api) end

-- The string metatable leads to the string library: guard it the same way.
local smt = getmetatable('')
smt.__index = memo[string_lib]
smt.__metatable = guard_of()

-- A function's environment would otherwise lead back to the real globals.
for t in next, memo do
  for _, v in next, t do
    if type(v) == 'function' then pcall(setfenv, v, env) end
  end
end

local function run(f, keys, args, eval)
  if eval then return xpcall(f, handler) end
  return xpcall(function() return f(keys, args) end, handler)
end

return env, run, meta, lib_api, redis
"#;

struct Sandbox {
    lua: Lua,
    /// The real globals: `KEYS` and `ARGV` are set here, behind the proxy.
    globals: Table,
    /// The proxy's metatable. During `FUNCTION LOAD` its `__index` is the
    /// library API instead of the globals.
    proxy_meta: Table,
    lib_api: Option<Table>,
    run: Function,
    /// The real `redis` table behind its proxy: each run sets its `call`
    /// and `pcall`.
    redis: Table,
}

impl Sandbox {
    fn new(functions: bool) -> mlua::Result<Sandbox> {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )?;
        let globals = lua.globals();
        let libs: [(&str, mlua::ffi::lua_CFunction); 4] = [
            ("cjson", luaopen_cjson),
            ("struct", luaopen_struct),
            ("cmsgpack", luaopen_cmsgpack),
            ("bit", luaopen_bit),
        ];
        for (name, open) in libs {
            // SAFETY: each is a `luaopen_*` entry point of a C library built
            // by build.rs against this same Lua; called with the library name
            // as `require` would, it only pushes and returns its table.
            let open = unsafe { lua.create_c_function(open)? };
            let table: Table = open.call(name)?;
            globals.raw_set(name, table)?;
        }

        let native = lua.create_table()?;
        native.raw_set(
            "as_error",
            lua.create_function(|lua, v: Lv| match &v {
                Lv::Error(e) => match redis_error(e) {
                    Some(r) => Ok(r.table(lua)),
                    None => Ok(v),
                },
                _ => Ok(v),
            })?,
        )?;
        native.raw_set("pending", lua.create_function(|_, ()| Ok(pending()))?)?;
        native.raw_set("handler", lua.create_function(error_handler)?)?;
        native.raw_set(
            "sha1hex",
            lua.create_function(|lua, v: Lv| {
                let bytes = match v {
                    Lv::String(s) => s.as_bytes().to_vec(),
                    Lv::Number(n) => lua_number_text(n).into_bytes(),
                    Lv::Integer(n) => n.to_string().into_bytes(),
                    _ => Vec::new(),
                };
                lua.create_string(sha1_hex(&bytes))
            })?,
        )?;
        native.raw_set(
            "error_text",
            lua.create_function(|_, s: mlua::LuaString| Ok(error_text(&s.as_bytes())))?,
        )?;
        native.raw_set("log", lua.create_function(redis_log)?)?;
        native.raw_set(
            "setresp",
            lua.create_function(|_, (n, v): (usize, Option<f64>)| {
                if n != 1 {
                    return Ok(Some("ERR redis.setresp() requires one argument."));
                }
                match v.map(|v| v as i64) {
                    Some(2) => RESP3.with(|r| r.set(false)),
                    Some(3) => RESP3.with(|r| r.set(true)),
                    _ => return Ok(Some("ERR RESP version must be 2 or 3.")),
                }
                Ok(None)
            })?,
        )?;
        native.raw_set("random", lua.create_function(math_random)?)?;
        native.raw_set(
            "randomseed",
            lua.create_function(|_, seed: f64| {
                srand48(seed as i32);
                Ok(())
            })?,
        )?;
        native.raw_set(
            "acl_check_cmd",
            lua.create_function(|_, args: MultiValue| {
                let argv = match lua_args_to_argv(args) {
                    Ok(a) => a,
                    Err(e) => return Ok((None, Some(format!("ERR {e}")))),
                };
                match command_spec(&argv) {
                    Some(_) => {
                        let caller = CALLER.get();
                        let ok = caller == 0
                            || !matches!(
                                crate::acl::evaluate(caller, &argv),
                                crate::acl::Verdict::Denied(_)
                            );
                        Ok((Some(ok), None))
                    }
                    None => Ok((
                        None,
                        Some("ERR Invalid command passed to redis.acl_check_cmd()".into()),
                    )),
                }
            })?,
        )?;
        native.raw_set("version", crate::commands::REDIS_VERSION)?;
        native.raw_set("version_num", version_num())?;
        if functions {
            native.raw_set("register", lua.create_function(register_function)?)?;
        }

        let (env, run, proxy_meta, lib_api, redis): (Table, Function, Table, Option<Table>, Table) =
            lua.load(SANDBOX)
                .set_name(INTERNAL)
                .call((globals.clone(), native, functions))?;
        lua.set_globals(env)?;
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(100_000),
            |_, _| check_interrupt(),
        )?;
        if RAND48.with(|r| r.get()) == 0 {
            srand48(crate::memdb::fast_random() as i32);
        }
        Ok(Sandbox {
            lua,
            globals,
            proxy_meta,
            lib_api,
            run,
            redis,
        })
    }

    /// Run `f` — an `EVAL` body with `KEYS`/`ARGV` already set, or a
    /// function taking them as arguments — with `redis.call` connected to
    /// `ctx`, and turn what it returns into a reply.
    fn call(
        &self,
        f: &Function,
        keys: Option<Table>,
        args: Option<Table>,
        funcname: &str,
        ctx: &mut CallCtx<'_>,
    ) -> Response {
        RESP3.with(|r| r.set(false));
        let eval = keys.is_none();
        // Both are Rust functions, as Redis's are C ones: a script's
        // `return redis.call(...)` is a tail call, and only a call into C
        // keeps the script's own frame — the line its error is reported at.
        let ctx = RefCell::new(ctx);
        let outcome = self.lua.scope(|scope| {
            let call =
                scope.create_function(|lua, args: MultiValue| {
                    match dispatch_from_script(lua, args, &mut ctx.borrow_mut()) {
                        (true, Lv::Table(t)) => Err(mlua::Error::external(RedisError {
                            msg: t.raw_get::<Option<String>>("err")?.unwrap_or_default(),
                            counted: counted(&t),
                        })),
                        (_, v) => Ok(v),
                    }
                })?;
            let pcall = scope.create_function(|lua, args: MultiValue| {
                Ok(dispatch_from_script(lua, args, &mut ctx.borrow_mut()).1)
            })?;
            self.redis.raw_set("call", call)?;
            self.redis.raw_set("pcall", pcall)?;
            let r = self.run.call::<(bool, Lv)>((f.clone(), keys, args, eval));
            self.redis.raw_set("call", Lv::Nil)?;
            self.redis.raw_set("pcall", Lv::Nil)?;
            r
        });
        match outcome {
            Ok((true, v)) => Response::Value(lua_to_value(v, 0)),
            Ok((false, Lv::Table(t))) if counted(&t) => {
                Response::Value(Value::CountedError(script_error(&t, funcname)))
            }
            Ok((false, Lv::Table(t))) => Response::Value(Value::Error(script_error(&t, funcname))),
            Ok((false, other)) => Response::Error(format!(
                "ERR Error running script {funcname}, {:.100}",
                describe(&other)
            )),
            Err(e) => Response::Error(format!("ERR Error running script {funcname}, {e}")),
        }
    }
}

/// A failed `redis.call`, carried through Lua as an mlua error and turned
/// back into Redis's `{err=...}` table by the error handler and `pcall`.
#[derive(Debug, Clone)]
struct RedisError {
    msg: String,
    /// Already counted in `errorstats` where the command failed.
    counted: bool,
}

impl RedisError {
    fn table(&self, lua: &Lua) -> Lv {
        let t = err_table(lua, &self.msg);
        if self.counted
            && let Lv::Table(t) = &t
        {
            let _ = t.raw_set(COUNTED, true);
        }
        t
    }
}

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for RedisError {}

fn redis_error(e: &mlua::Error) -> Option<RedisError> {
    match e {
        mlua::Error::CallbackError { cause, .. } => redis_error(cause),
        mlua::Error::ExternalError(x) => x.downcast_ref::<RedisError>().cloned(),
        _ => None,
    }
}

/// The field Redis sets on the error table of a command a script called,
/// whose error `call()` has already counted: a script reply carrying it is
/// not counted again.
const COUNTED: &str = "ignore_error_stats_update";

fn counted(t: &Table) -> bool {
    matches!(t.raw_get::<Lv>(COUNTED), Ok(Lv::Boolean(true)))
}

/// `luaCallFunction`'s error reply: the message, then where it was raised.
fn script_error(t: &Table, funcname: &str) -> String {
    let msg = match t.raw_get::<Lv>("err") {
        Ok(Lv::String(s)) => s.to_string_lossy(),
        _ => String::new(),
    };
    let source = t
        .raw_get::<Option<mlua::LuaString>>("source")
        .ok()
        .flatten();
    let line = t.raw_get::<Option<i64>>("line").ok().flatten();
    match (source, line) {
        (Some(s), Some(l)) => format!("{msg} script: {funcname}, on {}:{l}.", s.to_string_lossy()),
        _ => msg,
    }
}

/// Lua's `tostring`, for the values an error can be.
fn describe(v: &Lv) -> String {
    match v {
        Lv::Nil => "nil".into(),
        Lv::Boolean(b) => b.to_string(),
        Lv::Integer(n) => n.to_string(),
        Lv::Number(n) => lua_number_text(*n),
        Lv::String(s) => s.to_string_lossy(),
        Lv::Error(e) => e.to_string(),
        other => format!("{}: {:p}", other.type_name(), other.to_pointer()),
    }
}

/// The message handler every script runs under: Redis's
/// `__redis__err__handler`, which turns any error into a Redis error table
/// and notes where it was raised.
fn error_handler(lua: &Lua, err: Lv) -> mlua::Result<Table> {
    let t = match (pending(), err) {
        (Some(msg), _) => {
            let t = lua.create_table()?;
            t.raw_set("err", msg)?;
            t
        }
        (None, Lv::Table(t)) => t,
        (None, Lv::Error(e)) if let Some(r) = redis_error(&e) => match r.table(lua) {
            Lv::Table(t) => t,
            _ => lua.create_table()?,
        },
        (None, other) => {
            let t = lua.create_table()?;
            t.raw_set("err", format!("ERR {}", describe(&other)))?;
            t
        }
    };
    // Level 0 is this handler. The first frame that is neither C nor this
    // module's own Lua is where the user's code raised it.
    let mut level = 1;
    while let Some(frame) = lua.inspect_stack(level, |d| {
        let s = d.source();
        let internal = s.what == "C" || s.source.as_deref().is_some_and(|s| s == INTERNAL);
        (internal, s.source.map(|s| s.into_owned()), d.current_line())
    }) {
        let (internal, source, line) = frame;
        if !internal {
            if let Some(s) = source {
                t.raw_set("source", s)?;
            }
            if let Some(l) = line {
                t.raw_set("line", l)?;
            }
            break;
        }
        level += 1;
    }
    Ok(t)
}

/// `luaPushErrorBuff`: `-CODE message` becomes `CODE message`; a bare
/// `-message` gets the generic `ERR`.
fn error_text(s: &[u8]) -> String {
    let body = String::from_utf8_lossy(s.strip_prefix(b"-").unwrap_or(s)).into_owned();
    let (code, msg) = match body.split_once(' ') {
        Some((code, msg)) => (code.to_string(), msg.to_string()),
        None => ("ERR".to_string(), body),
    };
    format!("{code} {}", msg.trim_matches(['\r', '\n']))
}

/// An error a sub-command replied, as the `err` field a script sees.
fn command_error_text(e: &str) -> String {
    if crate::resp::has_error_code(e) {
        e.to_string()
    } else {
        format!("ERR {e}")
    }
}

fn redis_log(_: &Lua, args: MultiValue) -> mlua::Result<Option<&'static str>> {
    let mut args = args.into_iter();
    let n = match args.next() {
        Some(Lv::Integer(n)) => n as usize,
        Some(Lv::Number(n)) => n as usize,
        _ => 0,
    };
    if n < 2 {
        return Ok(Some("ERR redis.log() requires two arguments or more."));
    }
    let level = match args.next() {
        Some(Lv::Integer(l)) => l as f64,
        Some(Lv::Number(l)) => l,
        Some(Lv::String(s)) => match s.to_str().ok().and_then(|s| s.trim().parse().ok()) {
            Some(l) => l,
            None => return Ok(Some("ERR First argument must be a number (log level).")),
        },
        _ => return Ok(Some("ERR First argument must be a number (log level).")),
    };
    if !(0.0..=3.0).contains(&level) {
        return Ok(Some("ERR Invalid debug level."));
    }
    // Redis's default verbosity is notice: debug and verbose lines are dropped.
    if level >= 2.0 {
        let line = args
            .filter_map(|v| match v {
                Lv::String(s) => Some(s.to_string_lossy()),
                Lv::Number(n) => Some(lua_number_text(n)),
                Lv::Integer(n) => Some(n.to_string()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!("pg_redis: script: {line}");
    }
    Ok(None)
}

fn version_num() -> i64 {
    let mut parts = crate::commands::REDIS_VERSION
        .split('.')
        .map(|p| p.parse::<i64>().unwrap_or(0));
    let (a, b, c) = (
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
    );
    (a << 16) | (b << 8) | c
}

// ──────────────────────────────── random ────────────────────────────────────

/// Redis's `rand48`: the 48-bit linear congruential generator of `lrand48`,
/// so a script that seeds `math.random` gets Redis's numbers.
fn srand48(seed: i32) {
    let seed = seed as u32 as u64;
    RAND48.with(|r| r.set((seed << 16) | 0x330E));
}

fn lrand48() -> i32 {
    RAND48.with(|r| {
        let next = (r.get().wrapping_mul(0x5_DEEC_E66D).wrapping_add(0xB)) & ((1 << 48) - 1);
        r.set(next);
        (next >> 17) as i32
    })
}

fn math_random(_: &Lua, args: MultiValue) -> mlua::Result<f64> {
    const MAX: i32 = i32::MAX;
    let r = (lrand48() % MAX) as f64 / MAX as f64;
    let int = |v: &Lv| -> mlua::Result<i64> {
        match v {
            Lv::Integer(n) => Ok(*n),
            Lv::Number(n) => Ok(*n as i64),
            Lv::String(s) => s
                .to_str()
                .ok()
                .and_then(|s| s.trim().parse::<f64>().ok())
                .map(|n| n as i64)
                .ok_or_else(|| mlua::Error::runtime("bad argument to 'random' (number expected)")),
            _ => Err(mlua::Error::runtime(
                "bad argument to 'random' (number expected)",
            )),
        }
    };
    let args: Vec<Lv> = args.into_iter().collect();
    match args.as_slice() {
        [] => Ok(r),
        [u] => {
            let u = int(u)?;
            if u < 1 {
                return Err(mlua::Error::runtime(
                    "bad argument #1 to 'random' (interval is empty)",
                ));
            }
            Ok((r * u as f64).floor() + 1.0)
        }
        [l, u] => {
            let (l, u) = (int(l)?, int(u)?);
            if l > u {
                return Err(mlua::Error::runtime(
                    "bad argument #2 to 'random' (interval is empty)",
                ));
            }
            Ok((r * (u - l + 1) as f64).floor() + l as f64)
        }
        _ => Err(mlua::Error::runtime("wrong number of arguments")),
    }
}

// ───────────────────────────── conversions ──────────────────────────────────

/// How Lua prints a number: an integral value without a fraction.
fn lua_number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

/// The text a number argument to `redis.call` becomes: an integer when it is
/// one, otherwise the shortest decimal that reads back the same.
fn argument_number_text(n: f64) -> String {
    // `double2ll`, then fpconv, as `luaArgsToRedisArgv` does.
    let half = (i64::MAX / 2) as f64;
    if (-half..=half).contains(&n) && n == n.trunc() {
        format!("{}", n as i64)
    } else if n.is_nan() {
        "nan".into()
    } else if n.is_infinite() {
        if n > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        crate::commands::fpconv_dtoa(n)
    }
}

/// `luaArgsToRedisArgv`.
fn lua_args_to_argv(args: MultiValue) -> Result<Vec<Vec<u8>>, &'static str> {
    if args.is_empty() {
        return Err("Please specify at least one argument for this redis lib call");
    }
    args.into_iter()
        .map(|v| match v {
            Lv::String(s) => Ok(s.as_bytes().to_vec()),
            Lv::Integer(n) => Ok(n.to_string().into_bytes()),
            Lv::Number(n) => Ok(argument_number_text(n).into_bytes()),
            _ => Err("Lua redis lib command arguments must be strings or integers"),
        })
        .collect()
}

fn err_table(lua: &Lua, msg: &str) -> Lv {
    match lua.create_table() {
        Ok(t) => {
            let _ = t.raw_set("err", msg);
            Lv::Table(t)
        }
        Err(_) => Lv::Nil,
    }
}

/// A reply as a script sees it: Redis's own error codes kept, a bare
/// message given the generic `ERR`.
fn response_value(r: Response) -> Value {
    match r.into_value() {
        Value::Error(e) => Value::Error(command_error_text(&e)),
        v => v,
    }
}

/// `redisProtocolToLuaType`.
fn value_to_lua(lua: &Lua, v: Value) -> mlua::Result<Lv> {
    let resp3 = RESP3.with(|r| r.get());
    Ok(match v {
        Value::Nil | Value::NilArray => {
            if resp3 {
                Lv::Nil
            } else {
                Lv::Boolean(false)
            }
        }
        Value::Int(n) => Lv::Number(n as f64),
        Value::Bulk(b) => Lv::String(lua.create_string(&b)?),
        Value::Simple(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", s)?;
            Lv::Table(t)
        }
        Value::Error(e) => {
            let t = lua.create_table()?;
            t.raw_set("err", command_error_text(&e))?;
            Lv::Table(t)
        }
        Value::CountedError(e) => RedisError {
            msg: command_error_text(&e),
            counted: true,
        }
        .table(lua),
        Value::Array(items) => {
            let t = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.into_iter().enumerate() {
                t.raw_set(i + 1, value_to_lua(lua, item)?)?;
            }
            Lv::Table(t)
        }
        Value::Map(pairs) if resp3 => {
            let map = lua.create_table()?;
            for (k, v) in pairs {
                map.raw_set(value_to_lua(lua, k)?, value_to_lua(lua, v)?)?;
            }
            let t = lua.create_table()?;
            t.raw_set("map", map)?;
            Lv::Table(t)
        }
        Value::Map(pairs) => {
            let t = lua.create_table_with_capacity(pairs.len() * 2, 0)?;
            for (i, (k, v)) in pairs.into_iter().enumerate() {
                t.raw_set(2 * i + 1, value_to_lua(lua, k)?)?;
                t.raw_set(2 * i + 2, value_to_lua(lua, v)?)?;
            }
            Lv::Table(t)
        }
        // `redisProtocolToLuaType` for the RESP3 types, which a script only
        // meets after `redis.setresp(3)`.
        Value::Set(items) if resp3 => {
            let set = lua.create_table()?;
            for item in items {
                set.raw_set(value_to_lua(lua, item)?, true)?;
            }
            let t = lua.create_table()?;
            t.raw_set("set", set)?;
            Lv::Table(t)
        }
        Value::Set(items) => value_to_lua(lua, Value::Array(items))?,
        Value::Double(d) if resp3 => {
            let t = lua.create_table()?;
            t.raw_set("double", d)?;
            Lv::Table(t)
        }
        Value::Double(d) => Lv::String(lua.create_string(crate::commands::format_score(d))?),
        Value::Bool(b) if resp3 => Lv::Boolean(b),
        Value::Bool(b) => Lv::Number(if b { 1.0 } else { 0.0 }),
        Value::Verbatim(text) if resp3 => {
            let v = lua.create_table()?;
            v.raw_set("format", "txt")?;
            v.raw_set("string", text)?;
            let t = lua.create_table()?;
            t.raw_set("verbatim_string", v)?;
            Lv::Table(t)
        }
        Value::Verbatim(text) => Lv::String(lua.create_string(text)?),
        Value::BigNum(n) if resp3 => {
            let t = lua.create_table()?;
            t.raw_set("big_number", n)?;
            Lv::Table(t)
        }
        Value::BigNum(n) => Lv::String(lua.create_string(n)?),
        // `redisProtocolToLuaType_Attribute`: read and dropped.
        Value::Attributed(_, v) | Value::Pushed(_, v) => value_to_lua(lua, *v)?,
    })
}

/// `luaReplyToRedisReply`. Raw access throughout, as Redis's: a metatable
/// on a returned table is never consulted.
fn lua_to_value(v: Lv, depth: usize) -> Value {
    if depth >= MAX_REPLY_DEPTH {
        return Value::Error("ERR reached lua stack limit".into());
    }
    match v {
        Lv::String(s) => Value::Bulk(s.as_bytes().to_vec()),
        // A script that switched to RESP3 returns real booleans.
        Lv::Boolean(b) if RESP3.with(|r| r.get()) => Value::Bool(b),
        Lv::Boolean(true) => Value::Int(1),
        Lv::Boolean(false) => Value::Nil,
        Lv::Integer(n) => Value::Int(n),
        Lv::Number(n) => Value::Int(n as i64),
        Lv::Table(t) => {
            if let Ok(Lv::String(e)) = t.raw_get::<Lv>("err") {
                return if counted(&t) {
                    Value::CountedError(e.to_string_lossy())
                } else {
                    Value::Error(e.to_string_lossy())
                };
            }
            if let Ok(Lv::String(ok)) = t.raw_get::<Lv>("ok") {
                return Value::Simple(ok.to_string_lossy().replace(['\r', '\n'], " "));
            }
            if let Ok(Lv::Number(d)) = t.raw_get::<Lv>("double") {
                return Value::Double(d);
            }
            if let Ok(Lv::Integer(d)) = t.raw_get::<Lv>("double") {
                return Value::Double(d as f64);
            }
            if let Ok(Lv::String(b)) = t.raw_get::<Lv>("big_number") {
                // `addReplyBigNum`: a line break would end the frame.
                return Value::BigNum(b.to_string_lossy().replace(['\r', '\n'], " "));
            }
            if let Ok(Lv::Table(v)) = t.raw_get::<Lv>("verbatim_string")
                && let (Ok(Lv::String(_)), Ok(Lv::String(s))) =
                    (v.raw_get::<Lv>("format"), v.raw_get::<Lv>("string"))
            {
                return Value::Verbatim(s.to_string_lossy());
            }
            if let Ok(Lv::Table(map)) = t.raw_get::<Lv>("map") {
                return Value::Map(
                    map.pairs::<Lv, Lv>()
                        .flatten()
                        .map(|(k, v)| (lua_to_value(k, depth + 1), lua_to_value(v, depth + 1)))
                        .collect(),
                );
            }
            if let Ok(Lv::Table(set)) = t.raw_get::<Lv>("set") {
                return Value::Set(
                    set.pairs::<Lv, Lv>()
                        .flatten()
                        .map(|(k, _)| lua_to_value(k, depth + 1))
                        .collect(),
                );
            }
            let mut items = Vec::new();
            for i in 1.. {
                match t.raw_get::<Lv>(i) {
                    Ok(Lv::Nil) | Err(_) => break,
                    Ok(v) => items.push(lua_to_value(v, depth + 1)),
                }
            }
            Value::Array(items)
        }
        _ => Value::Nil,
    }
}

// ─────────────────────────── redis.call backend ─────────────────────────────

/// What one run's `redis.call` needs: where to send commands, which
/// database is selected, and what the script may do.
struct CallCtx<'h> {
    host: &'h mut dyn Host,
    db: u8,
    read_only: bool,
    run: &'h crate::server::ScriptRunHandle,
}

/// The command `argv` names, as Redis's `lookupCommand` finds it — a
/// container's subcommand included. `None` for what pg_redis does not serve.
fn command_spec(argv: &[Vec<u8>]) -> Option<&'static crate::redis_tables::CommandSpec> {
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    if !crate::commands::IMPLEMENTED.contains(name.to_ascii_uppercase().as_str()) {
        return None;
    }
    if crate::commands::CONTAINERS.contains(name.as_str()) && argv.len() > 1 {
        let sub = String::from_utf8_lossy(&argv[1]).to_ascii_lowercase();
        return crate::commands::COMMAND_SPECS
            .get(format!("{name}|{sub}").as_str())
            .copied();
    }
    crate::commands::COMMAND_SPECS.get(name.as_str()).copied()
}

/// Whether `argv` may run while a script holds the server: Redis's
/// `allow-busy` flag. A command pg_redis does not know is refused like any.
pub fn allowed_while_busy(argv: &[Vec<u8>]) -> bool {
    flagged(argv, "allow_busy")
}

/// Whether the command `argv` names carries one of Redis's command flags.
pub fn flagged(argv: &[Vec<u8>], flag: &str) -> bool {
    !argv.is_empty() && command_spec(argv).is_some_and(|s| s.flags.contains(&flag))
}

/// Like [`command_spec`], over every command Redis 7.2 has.
fn redis_command_spec(argv: &[Vec<u8>]) -> Option<&'static crate::redis_tables::CommandSpec> {
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    if crate::commands::CONTAINERS.contains(name.as_str()) && argv.len() > 1 {
        let sub = String::from_utf8_lossy(&argv[1]).to_ascii_lowercase();
        return crate::commands::COMMAND_SPECS
            .get(format!("{name}|{sub}").as_str())
            .copied();
    }
    crate::commands::COMMAND_SPECS.get(name.as_str()).copied()
}

/// One `redis.call`: `(true, error table)` when it failed, so the Lua side
/// raises it, `(false, reply)` otherwise.
fn dispatch_from_script(lua: &Lua, args: MultiValue, ctx: &mut &mut CallCtx<'_>) -> (bool, Lv) {
    let argv = match lua_args_to_argv(args) {
        Ok(a) => a,
        Err(e) => return (true, err_table(lua, &format!("ERR {e}"))),
    };
    let started = std::time::Instant::now();
    // Counted as Redis's `scriptCall` and `call()` count a script's
    // commands: a refusal (`scriptCall`'s checks, OOM, ACL) as a rejected
    // call, a command's error as a failed one, and either in `errorstats`
    // here, once, whether `pcall` catches it or the script returns it.
    let ran = match run_from_script(&argv, ctx) {
        Err(e) => Err((format!("ERR {e}"), true)),
        Ok(Response::Error(e)) => {
            let e = command_error_text(&e);
            let rejected = crate::resp::is_rejection(&e);
            Err((e, rejected))
        }
        Ok(r) => Ok(r),
    };
    if let Some(i) = crate::worker::stat_index(&crate::worker::stat_name(&argv)) {
        let usec = started.elapsed().as_micros() as u64;
        let rejected = ran.as_ref().is_err_and(|e| e.1);
        crate::server::count_command(i, usec, ran.is_err(), rejected);
    }
    match ran {
        Err((msg, _)) => {
            crate::server::count_error(crate::resp::error_code(&msg));
            (true, RedisError { msg, counted: true }.table(lua))
        }
        Ok(r) => match value_to_lua(lua, {
            // After `redis.setresp(3)` a script sees replies as a RESP3
            // client would.
            let v = response_value(r);
            if RESP3.with(|r| r.get()) {
                crate::resp3::shape(&argv, v)
            } else {
                v
            }
        }) {
            Ok(v) => (false, v),
            Err(e) => (true, err_table(lua, &format!("ERR {e}"))),
        },
    }
}

/// Whether the command running now was called from a script.
pub(crate) fn in_script_call() -> bool {
    IN_CALL.get()
}

/// Sets [`CALLER`] for one run, from the client that sent it.
struct CallerGuard;

impl CallerGuard {
    fn set() -> CallerGuard {
        let origin = crate::tracking::origin();
        let serial = if origin == 0 {
            0
        } else {
            crate::acl::session_of(origin).serial
        };
        CALLER.set(serial);
        CallerGuard
    }
}

impl Drop for CallerGuard {
    fn drop(&mut self) {
        CALLER.set(0);
    }
}

struct InCall(bool);

impl Drop for InCall {
    fn drop(&mut self) {
        IN_CALL.set(self.0);
    }
}

/// `scriptCall`: the checks a command passes before a script may run it,
/// then the command. `Err` is a scripting error, `Ok` the command's reply.
fn run_from_script(argv: &[Vec<u8>], ctx: &mut CallCtx<'_>) -> Result<Response, String> {
    let _in_call = InCall(IN_CALL.replace(true));
    // Redis's own table, not just what pg_redis serves: a command Redis has
    // and forbids in scripts (CLUSTER) is "not allowed", not "unknown". One it
    // allows but pg_redis lacks fails below, as the unknown command it is.
    let spec = redis_command_spec(argv).ok_or("Unknown Redis command called from script")?;
    if !crate::commands::arity_ok(spec.arity, argv.len()) {
        return Err("Wrong number of args calling Redis command from script".into());
    }
    let has = |f: &str| spec.flags.contains(&f);
    // `DEBUG SET-DISABLE-DENY-SCRIPTS 1` lets a script run them.
    if has("noscript") && !crate::server::deny_scripts_disabled() {
        return Err("This Redis command is not allowed from script".into());
    }
    // `scriptCall`'s refusal of a write with too few replicas: the command's
    // error, raised in the script.
    // (A command that may grow memory meets `maxmemory` where it runs, as
    // every command does.)
    if has("write") && crate::server::too_few_replicas() {
        return Ok(Response::Error(NOREPLICAS.into()));
    }
    let caller = CALLER.get();
    if caller != 0 {
        if let crate::acl::Verdict::Denied(d) = crate::acl::evaluate(caller, argv) {
            crate::acl::record_denial(&d, crate::acl::Context::Lua, crate::tracking::origin());
            return Err(format!("ACL failure in script: {}", d.message(false)));
        }
        if let Some(e) = crate::acl::sort_denied(caller, argv) {
            return Ok(Response::Error(e.to_string()));
        }
    }
    if ctx.read_only && (has("write") || has("may_replicate")) {
        return Err("Write commands are not allowed from read-only scripts.".into());
    }
    if has("write") {
        ctx.run.mark_dirty();
    }
    if has("readonly")
        && crate::server::tracking_active()
        && let Ok(keys) = crate::admin::get_keys(argv)
    {
        crate::tracking::after_read(
            crate::tracking::origin(),
            keys.into_iter().map(|(k, _)| k).collect(),
        );
    }
    let name = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    if (name == "xread" || name == "xreadgroup")
        && argv[1..]
            .iter()
            .take_while(|a| !a.eq_ignore_ascii_case(b"STREAMS"))
            .any(|a| a.eq_ignore_ascii_case(b"BLOCK"))
    {
        return Ok(Response::Error(format!(
            "ERR {name} command is not allowed with BLOCK option from scripts"
        )));
    }
    let cmd = match Command::parse(argv) {
        Ok(c) => c,
        Err(e) => return Ok(Response::Error(e)),
    };
    let db = ctx.db;
    crate::worker::feed_monitors_from_script(argv, db);
    // Every database the command touches must be held, exclusive, by the
    // run (`src/dblock.rs`). A database the script was not started with is
    // only tried: waiting here, inside the transaction, could deadlock.
    let reached = match &cmd {
        Command::Select { db } => crate::dblock::bit(*db),
        cmd => crate::dblock::dbs_of(cmd, db),
    };
    if let Err(d) = crate::dblock::reach(reached) {
        return Ok(Response::Error(reach_error(d)));
    }
    Ok(match cmd {
        Command::Select { db } => {
            ctx.db = db;
            Response::Ok
        }
        cmd @ (Command::Publish { .. }
        | Command::PubSubChannels { .. }
        | Command::PubSubNumSub { .. }
        | Command::PubSubNumPat) => {
            let (reply, route) = crate::pubsub::shared::run(cmd).expect("a pub/sub command");
            if let Some(insert) = route {
                ctx.host.exec(db, insert);
            }
            reply
        }
        Command::Admin(a) => {
            let client = crate::server::ClientHandle::detached(0);
            let mut conn = crate::admin::Conn {
                client: &client,
                db,
                reply: crate::admin::ReplyMode::On,
                session: crate::acl::session_of(crate::tracking::origin()),
                may_wait: false,
            };
            let host = &mut *ctx.host;
            crate::admin::execute(&a, &mut conn, &mut |c, d| host.exec(d, c))
        }
        cmd => {
            match cmd {
                Command::FlushDb { .. } => crate::server::bump_flush_epoch(db),
                Command::FlushAll { .. } => {
                    (0..crate::commands::NUM_DBS as u8).for_each(crate::server::bump_flush_epoch)
                }
                _ => {}
            }
            let targets = crate::worker::watch_targets(&cmd, db);
            let block = matches!(cmd, Command::Block(_) | Command::BlockTry { .. });
            // A command a script runs raises its keyspace events like any
            // other. The host takes the command, so the events work from a
            // second parse of it — only made when they are switched on.
            let host = &mut *ctx.host;
            let mut exists = |d: u8, k: &[u8]| {
                matches!(
                    host.exec(d, Command::Exists { keys: vec![k.to_vec()] }),
                    Response::Integer(n) if n > 0
                )
            };
            let notified = (crate::notify::flags() != 0)
                .then(|| Command::parse(argv).ok())
                .flatten();
            let before = notified
                .as_ref()
                .and_then(|c| crate::notify::before(c, db, &mut exists));
            let lazy = crate::lazyfree::before(&cmd, db, &mut |d, c| host.exec(d, c));
            let r = host.exec(db, cmd);
            crate::lazyfree::after(lazy, &r);
            // The command's read lookups count as they do outside a script.
            if let Some(l) = crate::keystats::lookups(argv) {
                let mut exists = |d: u8, k: &[u8]| {
                    matches!(
                        host.exec(d, Command::Exists { keys: vec![k.to_vec()] }),
                        Response::Integer(n) if n > 0
                    )
                };
                crate::keystats::tally(db, &l, &r, &mut exists).apply();
            }
            crate::access::after_command(
                db,
                argv,
                crate::access::client_no_touch(crate::tracking::origin()),
            );
            if let Some(c) = &notified {
                let mut exists = |d: u8, k: &[u8]| {
                    matches!(
                        host.exec(d, Command::Exists { keys: vec![k.to_vec()] }),
                        Response::Integer(n) if n > 0
                    )
                };
                crate::notify::after(before, c, db, &r, &mut exists);
            }
            let untouched = crate::notify::take_untouched();
            let targets = targets.map(|t| {
                t.into_iter()
                    .filter(|(d, k)| !(*d == db && untouched.contains(k)))
                    .collect()
            });
            let found_nothing =
                matches!(r, Response::NullArray | Response::Null | Response::Error(_));
            if !(block && found_nothing) {
                record_writes(targets);
            }
            r
        }
    })
}

// ──────────────────────────────── engines ───────────────────────────────────

#[derive(Default)]
struct Engine {
    eval: Option<EvalEngine>,
    func: Option<FuncEngine>,
}

struct EvalEngine {
    sb: Sandbox,
    /// Compiled scripts by SHA1, with their shebang flags.
    scripts: HashMap<String, (Function, u32)>,
    /// Whether each compiled script names a command that reaches another
    /// database ([`names_other_db`]).
    reach: HashMap<String, bool>,
    generation: u64,
}

struct FuncInfo {
    name: String,
    desc: Option<Vec<u8>>,
    flags: u32,
    func: Function,
}

struct Library {
    name: String,
    code: Vec<u8>,
    functions: Vec<FuncInfo>,
    /// Whether the code names a command that reaches another database
    /// ([`names_other_db`]).
    reaches: bool,
}

struct FuncEngine {
    sb: Sandbox,
    /// In name order.
    libs: Vec<Library>,
    generation: u64,
}

fn sandbox_failed(e: mlua::Error) -> Response {
    Response::Error(format!("ERR could not start the Lua engine: {e}"))
}

impl Engine {
    fn eval_engine(&mut self) -> Result<&mut EvalEngine, Response> {
        let generation = crate::server::scripts_gen();
        let stale = SCRIPTS_STALE.with(|s| s.replace(false));
        if stale
            || self
                .eval
                .as_ref()
                .is_none_or(|e| e.generation != generation)
        {
            self.eval = Some(EvalEngine {
                sb: Sandbox::new(false).map_err(sandbox_failed)?,
                scripts: HashMap::new(),
                reach: HashMap::new(),
                generation,
            });
        }
        Ok(self.eval.as_mut().unwrap())
    }

    fn func_engine(&mut self, host: &mut dyn Host) -> Result<&mut FuncEngine, Response> {
        let generation = crate::server::functions_gen();
        let stale = FUNCTIONS_STALE.with(|s| s.replace(false));
        if stale
            || self
                .func
                .as_ref()
                .is_none_or(|e| e.generation != generation)
        {
            let sb = Sandbox::new(true).map_err(sandbox_failed)?;
            let mut libs: Vec<Library> = Vec::new();
            for code in host.libraries() {
                match compile_library(&sb, &code) {
                    Ok(lib) if !libs.iter().any(|l| l.name == lib.name) => libs.push(lib),
                    Ok(lib) => eprintln!("pg_redis: library {} stored twice", lib.name),
                    Err(e) => eprintln!("pg_redis: could not load a stored library: {e}"),
                }
            }
            libs.sort_by(|a, b| a.name.cmp(&b.name));
            publish_counts(&libs);
            self.func = Some(FuncEngine {
                sb,
                libs,
                generation,
            });
        }
        Ok(self.func.as_mut().unwrap())
    }
}

fn publish_counts(libs: &[Library]) {
    let funcs = libs.iter().map(|l| l.functions.len()).sum::<usize>();
    crate::server::set_function_counts(libs.len() as i64, funcs as i64);
}

/// `evalExtractShebangFlags`: the flags a `#!lua` line names, and where the
/// code after it starts (the line feed is kept, so line numbers hold).
fn shebang_flags(body: &[u8]) -> Result<(u32, usize), String> {
    if !body.starts_with(b"#!") {
        return Ok((FLAG_EVAL_COMPAT, 0));
    }
    let end = body
        .iter()
        .position(|&b| b == b'\n')
        .ok_or("ERR Invalid script shebang")?;
    let parts = crate::resp::split_inline(&body[..end])
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or("ERR Invalid engine in script shebang")?;
    if parts[0] != b"#!lua" {
        return Err(format!(
            "ERR Unexpected engine in script shebang: {}",
            String::from_utf8_lossy(&parts[0])
        ));
    }
    let mut flags = 0;
    for part in &parts[1..] {
        let Some(list) = part.strip_prefix(b"flags=") else {
            return Err(format!(
                "ERR Unknown lua shebang option: {}",
                String::from_utf8_lossy(part)
            ));
        };
        for flag in list.split(|&b| b == b',').filter(|f| !f.is_empty()) {
            match SCRIPT_FLAGS
                .iter()
                .find(|(name, _)| name.as_bytes() == flag)
            {
                Some((_, bit)) => flags |= bit,
                None => {
                    return Err(format!(
                        "ERR Unexpected flag in script shebang: {}",
                        String::from_utf8_lossy(flag)
                    ));
                }
            }
        }
    }
    Ok((flags, end))
}

/// mlua's syntax error, as the bare message Lua produced.
fn compile_message(e: mlua::Error) -> String {
    match e {
        mlua::Error::SyntaxError { message, .. } => message,
        e => e.to_string(),
    }
}

fn compile_script(sb: &Sandbox, body: &[u8]) -> Result<(Function, u32), String> {
    let (flags, skip) = shebang_flags(body)?;
    let f = sb
        .lua
        .load(&body[skip..])
        .set_name("@user_script")
        .set_mode(ChunkMode::Text)
        .into_function()
        .map_err(|e| {
            format!(
                "ERR Error compiling script (new function): {}",
                compile_message(e)
            )
        })?;
    Ok((f, flags))
}

/// `functionsVerifyName`.
fn valid_name(name: &[u8]) -> bool {
    !name.is_empty() && name.iter().all(|&c| c.is_ascii_alphanumeric() || c == b'_')
}

const BAD_NAME: &str = "Library names can only contain letters, numbers, or underscores(_) \
                        and must be at least one character long";

/// `functionExtractLibMetaData`: the library's name, and its code from the
/// line feed that ends the `#!` line.
fn library_metadata(code: &[u8]) -> Result<(String, usize), String> {
    if !code.starts_with(b"#!") {
        return Err("ERR Missing library metadata".into());
    }
    let end = code
        .iter()
        .position(|&b| b == b'\n')
        .ok_or("ERR Invalid library metadata")?;
    let parts = crate::resp::split_inline(&code[..end])
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or("ERR Invalid library metadata")?;
    let engine = String::from_utf8_lossy(&parts[0][2..]).into_owned();
    let mut name: Option<Vec<u8>> = None;
    for part in &parts[1..] {
        if part.len() >= 5 && part[..5].eq_ignore_ascii_case(b"name=") {
            if name.is_some() {
                return Err(
                    "ERR Invalid metadata value, name argument was given multiple times".into(),
                );
            }
            name = Some(part[5..].to_vec());
            continue;
        }
        return Err(format!(
            "ERR Invalid metadata value given: {}",
            String::from_utf8_lossy(part)
        ));
    }
    let name = name.ok_or("ERR Library name was not given")?;
    if !valid_name(&name) {
        return Err(format!("ERR {BAD_NAME}"));
    }
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Engine '{engine}' not found"));
    }
    Ok((String::from_utf8_lossy(&name).into_owned(), end))
}

/// Compile a library and run its top level, which registers its functions.
fn compile_library(sb: &Sandbox, code: &[u8]) -> Result<Library, String> {
    let (name, skip) = library_metadata(code)?;
    let lib_api = sb
        .lib_api
        .as_ref()
        .expect("the function engine has a library API");
    let f = sb
        .lua
        .load(&code[skip..])
        .set_name("@user_function")
        .set_mode(ChunkMode::Text)
        .into_function()
        .map_err(|e| format!("ERR Error compiling function: {}", compile_message(e)))?;
    sb.proxy_meta
        .raw_set("__index", lib_api)
        .map_err(|e| e.to_string())?;
    LOADING.with(|l| *l.borrow_mut() = Some(Vec::new()));
    let outcome = {
        let _interrupt = InterruptGuard::set(Interrupt::Load(Instant::now() + LOAD_TIMEOUT));
        sb.run.call::<(bool, Lv)>((f, Lv::Nil, Lv::Nil, true))
    };
    let functions = LOADING.with(|l| l.borrow_mut().take()).unwrap_or_default();
    sb.proxy_meta
        .raw_set("__index", &sb.globals)
        .map_err(|e| e.to_string())?;
    match outcome {
        Ok((true, _)) => {}
        Ok((false, Lv::Table(t))) => {
            let msg = match t.raw_get::<Lv>("err") {
                Ok(Lv::String(s)) => s.to_string_lossy(),
                _ => String::new(),
            };
            return Err(format!("ERR Error registering functions: {msg}"));
        }
        Ok((false, other)) => {
            return Err(format!(
                "ERR Error registering functions: ERR {}",
                describe(&other)
            ));
        }
        Err(e) => return Err(format!("ERR Error registering functions: ERR {e}")),
    }
    Ok(Library {
        name,
        code: code.to_vec(),
        functions,
        reaches: names_other_db(code),
    })
}

/// One `redis.register_function` call: the name, the description, the
/// function and its flags.
type Registration = (Vec<u8>, Option<Vec<u8>>, Function, u32);

/// `redis.register_function`, positional or named. `Some(message)` is an
/// error the Lua side raises.
fn register_function(lua: &Lua, args: MultiValue) -> mlua::Result<Option<String>> {
    if LOADING.with(|l| l.borrow().is_none()) {
        return Ok(Some(
            "ERR redis.register_function can only be called on FUNCTION LOAD command".into(),
        ));
    }
    let string_arg = |v: &Lv| -> Option<Vec<u8>> {
        match v {
            Lv::String(s) => Some(s.as_bytes().to_vec()),
            Lv::Number(n) => Some(lua_number_text(*n).into_bytes()),
            Lv::Integer(n) => Some(n.to_string().into_bytes()),
            _ => None,
        }
    };
    let args: Vec<Lv> = args.into_iter().collect();
    let parsed: Result<Registration, &str> = match args.as_slice() {
        [Lv::Table(t)] => (|| {
            let (mut name, mut desc, mut callback, mut flags) = (None, None, None, 0);
            for pair in t.pairs::<Lv, Lv>() {
                let (k, v) =
                    pair.map_err(|_| "unknown argument given to redis.register_function")?;
                let key = match &k {
                    Lv::String(s) => s.to_string_lossy().to_ascii_lowercase(),
                    Lv::Number(_) | Lv::Integer(_) => describe(&k),
                    _ => {
                        return Err(
                            "named argument key given to redis.register_function is not a string",
                        );
                    }
                };
                match key.as_str() {
                    "function_name" => {
                        name = Some(string_arg(&v).ok_or(
                            "function_name argument given to redis.register_function must be a string",
                        )?);
                    }
                    "description" => {
                        desc = Some(string_arg(&v).ok_or(
                            "description argument given to redis.register_function must be a string",
                        )?);
                    }
                    "callback" => match v {
                        Lv::Function(f) => callback = Some(f),
                        _ => {
                            return Err(
                                "callback argument given to redis.register_function must be a function",
                            );
                        }
                    },
                    "flags" => {
                        let Lv::Table(list) = v else {
                            return Err(
                                "flags argument to redis.register_function must be a table representing function flags",
                            );
                        };
                        for i in 1.. {
                            match list.raw_get::<Lv>(i) {
                                Ok(Lv::Nil) | Err(_) => break,
                                Ok(f) => {
                                    let f = string_arg(&f).ok_or("unknown flag given")?;
                                    let bit = SCRIPT_FLAGS
                                        .iter()
                                        .find(|(n, _)| n.as_bytes().eq_ignore_ascii_case(&f))
                                        .ok_or("unknown flag given")?
                                        .1;
                                    flags |= bit;
                                }
                            }
                        }
                    }
                    _ => return Err("unknown argument given to redis.register_function"),
                }
            }
            let name = name.ok_or("redis.register_function must get a function name argument")?;
            let callback =
                callback.ok_or("redis.register_function must get a callback argument")?;
            Ok((name, desc, callback, flags))
        })(),
        [Lv::Table(_), ..] | [] => Err("wrong number of arguments to redis.register_function"),
        [_] => Err(
            "calling redis.register_function with a single argument is only applicable to Lua table (representing named arguments).",
        ),
        [name, callback] => match (string_arg(name), callback) {
            (None, _) => Err("first argument to redis.register_function must be a string"),
            (Some(n), Lv::Function(f)) => Ok((n, None, f.clone(), 0)),
            (Some(_), _) => Err("second argument to redis.register_function must be a function"),
        },
        _ => Err("wrong number of arguments to redis.register_function"),
    };
    let _ = lua;
    let (name, desc, func, flags) = match parsed {
        Ok(p) => p,
        Err(e) => return Ok(Some(format!("ERR {e}"))),
    };
    if !valid_name(&name) {
        return Ok(Some(format!("ERR {BAD_NAME}")));
    }
    let name = String::from_utf8_lossy(&name).into_owned();
    LOADING.with(|l| {
        let mut l = l.borrow_mut();
        let list = l.as_mut().expect("checked above");
        if list.iter().any(|f| f.name.eq_ignore_ascii_case(&name)) {
            return Ok(Some("ERR Function already exists in the library".into()));
        }
        list.push(FuncInfo {
            name,
            desc,
            flags,
            func,
        });
        Ok(None)
    })
}

/// An invocation's keys, and its arguments after them.
type KeysAndArgs<'a> = (&'a [Vec<u8>], &'a [Vec<u8>]);

/// The keys and arguments of an invocation, split at `numkeys`.
fn split_numkeys(inv: &Invocation, fcall: bool) -> Result<KeysAndArgs<'_>, String> {
    let n: i64 = std::str::from_utf8(&inv.numkeys)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            if fcall {
                "ERR Bad number of keys provided".to_string()
            } else {
                "ERR value is not an integer or out of range".to_string()
            }
        })?;
    if n > inv.rest.len() as i64 {
        return Err("ERR Number of keys can't be greater than number of args".into());
    }
    if n < 0 {
        return Err("ERR Number of keys can't be negative".into());
    }
    Ok(inv.rest.split_at(n as usize))
}

fn lua_array(lua: &Lua, items: &[Vec<u8>]) -> mlua::Result<Table> {
    let t = lua.create_table_with_capacity(items.len(), 0)?;
    for (i, item) in items.iter().enumerate() {
        t.raw_set(i + 1, lua.create_string(item)?)?;
    }
    Ok(t)
}

/// Whether the command runs Lua against the data: `EVAL`, `EVALSHA`, `FCALL`
/// and their `_RO` forms. The rest manage the cache and the libraries.
pub fn runs_code(cmd: &ScriptCmd) -> bool {
    matches!(
        cmd,
        ScriptCmd::Eval { .. } | ScriptCmd::EvalSha { .. } | ScriptCmd::Fcall { .. }
    )
}

/// Whether a script may reach a database other than the one it starts on, so
/// its dispatcher locks every database before it runs (`src/dblock.rs`). True
/// when its source names `SELECT`, `MOVE`, `COPY`, `SWAPDB` or `FLUSHALL`, and
/// when this worker has not compiled it yet and cannot tell.
pub fn reaches_other_dbs(cmd: &ScriptCmd) -> bool {
    match cmd {
        ScriptCmd::Eval { body, .. } => names_other_db(body),
        // A SHA names one body, so what any compile of it found holds.
        ScriptCmd::EvalSha { sha, .. } => ENGINE.with(|e| {
            e.try_borrow()
                .ok()
                .and_then(|e| e.eval.as_ref().and_then(|ev| ev.reach.get(sha).copied()))
                .unwrap_or(true)
        }),
        // A library can be replaced from another worker: only an engine at
        // the current generation knows what the function runs.
        ScriptCmd::Fcall { name, .. } => ENGINE.with(|e| {
            let name = String::from_utf8_lossy(name);
            e.try_borrow()
                .ok()
                .and_then(|e| {
                    let func = e.func.as_ref()?;
                    if func.generation != crate::server::functions_gen() {
                        return None;
                    }
                    func.libs
                        .iter()
                        .find(|l| {
                            l.functions
                                .iter()
                                .any(|f| f.name.eq_ignore_ascii_case(&name))
                        })
                        .map(|l| l.reaches)
                })
                .unwrap_or(true)
        }),
        _ => false,
    }
}

/// Whether Lua source may reach a database other than the one it starts on:
/// it names, as a whole word in any case, a command that does, or it runs a
/// command whose name it does not spell out. Comments and strings count too:
/// a false match only locks more than needed.
fn names_other_db(src: &[u8]) -> bool {
    const WORDS: [&[u8]; 5] = [b"select", b"move", b"copy", b"swapdb", b"flushall"];
    let word = |b: &u8| b.is_ascii_alphanumeric() || *b == b'_';
    src.split(|b| !word(b))
        .any(|w| WORDS.iter().any(|x| w.eq_ignore_ascii_case(x)))
        || names_commands_at_run_time(src)
}

/// Whether some `redis.call` or `redis.pcall` (or `server.…`) takes a
/// command name that is not a plain string literal: one built at run time,
/// read from `ARGV`, or called through another name. Such a script can run
/// any command, so it holds every database, as every script does in Redis.
fn names_commands_at_run_time(src: &[u8]) -> bool {
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let skip_ws = |mut j: usize| {
        while src.get(j).is_some_and(u8::is_ascii_whitespace) {
            j += 1;
        }
        j
    };
    let mut i = 0;
    while i < src.len() {
        let len = [b"pcall".as_slice(), b"call"]
            .iter()
            .find(|w| src[i..].starts_with(w))
            .map_or(0, |w| w.len());
        let whole = len > 0
            && (i == 0 || !word(src[i - 1]))
            && !src.get(i + len).copied().is_some_and(word);
        let after_dot = src[..i].iter().rev().find(|b| !b.is_ascii_whitespace()) == Some(&b'.');
        if !whole || !after_dot {
            i += 1;
            continue;
        }
        let mut j = skip_ws(i + len);
        let paren = src.get(j) == Some(&b'(');
        if paren {
            j = skip_ws(j + 1);
        }
        let Some(end) = lua_string_end(src, j) else {
            return true;
        };
        // In parentheses the name is the first argument, alone: `'sel' ..
        // 'ect'` builds one.
        if paren && !matches!(src.get(skip_ws(end)), Some(b',' | b')')) {
            return true;
        }
        i = end;
    }
    false
}

/// Where the Lua string literal starting at `j` ends: quoted, or a long
/// bracket `[[ … ]]` / `[==[ … ]==]`. `None` when no literal starts there.
fn lua_string_end(src: &[u8], j: usize) -> Option<usize> {
    match *src.get(j)? {
        q @ (b'\'' | b'"') => {
            let mut k = j + 1;
            while k < src.len() {
                match src[k] {
                    b'\\' => k += 2,
                    c if c == q => return Some(k + 1),
                    _ => k += 1,
                }
            }
            None
        }
        b'[' => {
            let eqs = src[j + 1..].iter().take_while(|&&b| b == b'=').count();
            if src.get(j + 1 + eqs) != Some(&b'[') {
                return None;
            }
            let close: Vec<u8> = std::iter::once(b']')
                .chain(std::iter::repeat_n(b'=', eqs))
                .chain(std::iter::once(b']'))
                .collect();
            let body = j + 2 + eqs;
            src[body..]
                .windows(close.len())
                .position(|w| w == close.as_slice())
                .map(|p| body + p + close.len())
        }
        _ => None,
    }
}

/// The error a script gets when it reaches a database another client holds.
fn reach_error(db: u8) -> String {
    format!(
        "ERR this script reached database {db}, which another client is using. \
         A script that names SELECT, MOVE, COPY, SWAPDB or FLUSHALL in its source, \
         or runs a command it does not name with a string literal, locks every \
         database before it starts; this one reached it in a way that could not \
         be seen ahead"
    )
}

/// Run one scripting command on the dispatcher.
pub fn execute(cmd: &ScriptCmd, db: u8, host: &mut dyn Host) -> Response {
    let _frozen = crate::server::freeze_time();
    ENGINE.with(|e| {
        let mut engine = e.borrow_mut();
        engine.execute(cmd, db, host).unwrap_or_else(|r| r)
    })
}

impl Engine {
    fn execute(
        &mut self,
        cmd: &ScriptCmd,
        db: u8,
        host: &mut dyn Host,
    ) -> Result<Response, Response> {
        let err = |e: String| Response::Error(e);
        match cmd {
            ScriptCmd::Eval { body, inv } => {
                let (keys, args) = split_numkeys(inv, false).map_err(err)?;
                let sha = sha1_hex(body);
                let engine = self.eval_engine()?;
                if !engine.scripts.contains_key(&sha) {
                    let compiled = compile_script(&engine.sb, body).map_err(err)?;
                    engine.reach.insert(sha.clone(), names_other_db(body));
                    engine.scripts.insert(sha.clone(), compiled);
                    host.save_script(&sha, body);
                }
                Ok(run_eval(engine, &sha, keys, args, inv, db, host))
            }
            ScriptCmd::EvalSha { sha, inv } => {
                let (keys, args) = split_numkeys(inv, false).map_err(err)?;
                let engine = self.eval_engine()?;
                if !engine.scripts.contains_key(sha) {
                    let body = (sha.len() == 40)
                        .then(|| host.script_body(sha))
                        .flatten()
                        .ok_or_else(|| err(NOSCRIPT.into()))?;
                    let compiled = compile_script(&engine.sb, &body).map_err(err)?;
                    engine.reach.insert(sha.clone(), names_other_db(&body));
                    engine.scripts.insert(sha.clone(), compiled);
                }
                Ok(run_eval(engine, sha, keys, args, inv, db, host))
            }
            ScriptCmd::Load(body) => {
                let sha = sha1_hex(body);
                let engine = self.eval_engine()?;
                if !engine.scripts.contains_key(&sha) {
                    let compiled = compile_script(&engine.sb, body).map_err(err)?;
                    engine.reach.insert(sha.clone(), names_other_db(body));
                    engine.scripts.insert(sha.clone(), compiled);
                }
                host.save_script(&sha, body);
                Ok(Response::BulkString(sha.into_bytes()))
            }
            ScriptCmd::Exists(shas) => {
                let shas: Vec<String> = shas
                    .iter()
                    .map(|s| String::from_utf8_lossy(s).to_ascii_lowercase())
                    .collect();
                let found = host.scripts_exist(&shas);
                Ok(Response::IntegerArray(
                    found.into_iter().map(i64::from).collect(),
                ))
            }
            ScriptCmd::Flush => {
                host.flush_scripts();
                self.eval = None;
                host.on_commit(crate::server::bump_scripts_gen);
                host.on_abort(mark_scripts_stale);
                Ok(Response::Ok)
            }
            ScriptCmd::Fcall { name, inv } => {
                let engine = self.func_engine(host)?;
                let name = String::from_utf8_lossy(name);
                let f = engine
                    .libs
                    .iter()
                    .flat_map(|l| &l.functions)
                    .find(|f| f.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| err("ERR Function not found".into()))?;
                let (keys, args) = split_numkeys(inv, true).map_err(err)?;
                if f.flags & FLAG_NO_WRITES == 0 && inv.ro {
                    return Err(err(RO_WITH_WRITE_FLAG.into()));
                }
                if let Some(refused) = refused_before_run(f.flags, false, host) {
                    return Err(refused);
                }
                let read_only = inv.ro || f.flags & FLAG_NO_WRITES != 0;
                let lua = &engine.sb.lua;
                let (k, a) = (
                    lua_array(lua, keys).map_err(|e| err(e.to_string()))?,
                    lua_array(lua, args).map_err(|e| err(e.to_string()))?,
                );
                let _caller = CallerGuard::set();
                let run =
                    crate::server::ScriptRunHandle::start(false, f.name.as_bytes(), &inv.argv);
                let _interrupt = InterruptGuard::set(Interrupt::Run(run.probe()));
                let mut ctx = CallCtx {
                    host,
                    db,
                    read_only,
                    run: &run,
                };
                let allow = allows_oom(f.flags, false);
                Ok(crate::evict::allowing_oom(allow, || {
                    engine.sb.call(&f.func, Some(k), Some(a), &f.name, &mut ctx)
                }))
            }
            ScriptCmd::FunctionLoad { replace, code } => {
                let engine = self.func_engine(host)?;
                let (name, _) = library_metadata(code).map_err(err)?;
                let existing = engine.libs.iter().position(|l| l.name == name);
                if existing.is_some() && !replace {
                    return Err(err(format!("ERR Library '{name}' already exists")));
                }
                let lib = compile_library(&engine.sb, code).map_err(err)?;
                if lib.functions.is_empty() {
                    return Err(err("ERR No functions registered".into()));
                }
                for f in &lib.functions {
                    let taken = engine.libs.iter().filter(|l| l.name != name).any(|l| {
                        l.functions
                            .iter()
                            .any(|g| g.name.eq_ignore_ascii_case(&f.name))
                    });
                    if taken {
                        return Err(err(format!("ERR Function {} already exists", f.name)));
                    }
                }
                if let Some(i) = existing {
                    engine.libs.remove(i);
                }
                let at = engine.libs.partition_point(|l| l.name < name);
                engine.libs.insert(at, lib);
                publish_counts(&engine.libs);
                host.save_library(&name, code);
                host.on_commit(crate::server::bump_functions_gen);
                host.on_abort(mark_functions_stale);
                Ok(Response::BulkString(name.into_bytes()))
            }
            ScriptCmd::FunctionDelete(name) => {
                let engine = self.func_engine(host)?;
                let name = String::from_utf8_lossy(name);
                let i = engine
                    .libs
                    .iter()
                    .position(|l| l.name == name)
                    .ok_or_else(|| err("ERR Library not found".into()))?;
                engine.libs.remove(i);
                publish_counts(&engine.libs);
                host.delete_library(&name);
                host.on_commit(crate::server::bump_functions_gen);
                host.on_abort(mark_functions_stale);
                Ok(Response::Ok)
            }
            ScriptCmd::FunctionFlush => {
                host.flush_libraries();
                self.func = None;
                crate::server::set_function_counts(0, 0);
                host.on_commit(crate::server::bump_functions_gen);
                host.on_abort(mark_functions_stale);
                Ok(Response::Ok)
            }
            ScriptCmd::FunctionList { pattern, with_code } => {
                let engine = self.func_engine(host)?;
                let libs = engine.libs.iter().filter(|l| {
                    pattern
                        .as_ref()
                        .is_none_or(|p| crate::pubsub::glob_match(p, l.name.as_bytes()))
                });
                let list = libs
                    .map(|l| {
                        // In registration order. Redis's order is its hash
                        // table's, which is not defined, so no fixed order
                        // matches it in general.
                        let functions = l
                            .functions
                            .iter()
                            .map(|f| {
                                Value::Map(vec![
                                    (Value::str("name"), Value::str(&f.name)),
                                    (
                                        Value::str("description"),
                                        f.desc.clone().map_or(Value::Nil, Value::Bulk),
                                    ),
                                    (
                                        Value::str("flags"),
                                        Value::Array(
                                            SCRIPT_FLAGS
                                                .iter()
                                                .filter(|(_, bit)| f.flags & bit != 0)
                                                .map(|(n, _)| Value::Simple((*n).to_string()))
                                                .collect(),
                                        ),
                                    ),
                                ])
                            })
                            .collect();
                        let mut fields = vec![
                            (Value::str("library_name"), Value::str(&l.name)),
                            (Value::str("engine"), Value::str("LUA")),
                            (Value::str("functions"), Value::Array(functions)),
                        ];
                        if *with_code {
                            fields.push((Value::str("library_code"), Value::Bulk(l.code.clone())));
                        }
                        Value::Map(fields)
                    })
                    .collect();
                Ok(Response::Value(Value::Array(list)))
            }
            ScriptCmd::Flags { sha, function } => {
                if let Some(sha) = sha {
                    let engine = self.eval_engine()?;
                    if let Some((_, flags)) = engine.scripts.get(sha) {
                        return Ok(Response::Integer(*flags as i64));
                    }
                    let flags = host.script_body(sha).map_or(-1, |body| body_flags(&body));
                    return Ok(Response::Integer(flags));
                }
                let name = String::from_utf8_lossy(function.as_deref().unwrap_or_default());
                let engine = self.func_engine(host)?;
                let flags = engine
                    .libs
                    .iter()
                    .flat_map(|l| &l.functions)
                    .find(|f| f.name.eq_ignore_ascii_case(&name))
                    .map_or(-1, |f| f.flags as i64);
                Ok(Response::Integer(flags))
            }
            ScriptCmd::FunctionDump => {
                let engine = self.func_engine(host)?;
                let codes: Vec<Vec<u8>> = engine.libs.iter().map(|l| l.code.clone()).collect();
                Ok(Response::BulkString(crate::rdb::functions_dump(&codes)))
            }
            ScriptCmd::FunctionRestore { payload, policy } => {
                let codes = crate::rdb::functions_restore(payload).map_err(err)?;
                // Compiled apart from the live libraries, which stay as they
                // are if anything below refuses.
                let scratch = Sandbox::new(true).map_err(sandbox_failed)?;
                let mut incoming: Vec<Library> = Vec::new();
                for code in &codes {
                    let lib = compile_library(&scratch, code).map_err(err)?;
                    if lib.functions.is_empty() {
                        return Err(err("ERR No functions registered".into()));
                    }
                    if incoming.iter().any(|l| l.name == lib.name) {
                        return Err(err(format!("ERR Library '{}' already exists", lib.name)));
                    }
                    for f in &lib.functions {
                        if incoming.iter().any(|l| {
                            l.functions
                                .iter()
                                .any(|g| g.name.eq_ignore_ascii_case(&f.name))
                        }) {
                            return Err(err(format!("ERR Function {} already exists", f.name)));
                        }
                    }
                    incoming.push(lib);
                }
                let engine = self.func_engine(host)?;
                let mut kept: Vec<(&str, Vec<&str>)> = Vec::new();
                if *policy != Policy::Flush {
                    for l in &engine.libs {
                        if incoming.iter().any(|n| n.name == l.name) {
                            if *policy == Policy::Append {
                                return Err(err(format!("ERR Library {} already exists", l.name)));
                            }
                            continue;
                        }
                        kept.push((
                            &l.name,
                            l.functions.iter().map(|f| f.name.as_str()).collect(),
                        ));
                    }
                    for lib in &incoming {
                        for f in &lib.functions {
                            if kept
                                .iter()
                                .any(|(_, fs)| fs.iter().any(|g| g.eq_ignore_ascii_case(&f.name)))
                            {
                                return Err(err(format!("ERR Function {} already exists", f.name)));
                            }
                        }
                    }
                }
                let kept: Vec<String> = kept.into_iter().map(|(n, _)| n.to_string()).collect();
                let mut all: Vec<(String, Vec<u8>)> = engine
                    .libs
                    .iter()
                    .filter(|l| kept.contains(&l.name))
                    .map(|l| (l.name.clone(), l.code.clone()))
                    .collect();
                all.extend(incoming.into_iter().map(|l| (l.name, l.code)));
                host.flush_libraries();
                for (name, code) in &all {
                    host.save_library(name, code);
                }
                // Rebuilt from the table, which now holds what was restored.
                self.func = None;
                host.on_commit(crate::server::bump_functions_gen);
                host.on_abort(mark_functions_stale);
                Ok(Response::Ok)
            }
        }
    }
}

fn run_eval(
    engine: &mut EvalEngine,
    sha: &str,
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
    inv: &Invocation,
    db: u8,
    host: &mut dyn Host,
) -> Response {
    let (f, flags) = engine.scripts[sha].clone();
    let compat = flags & FLAG_EVAL_COMPAT != 0;
    if !compat && flags & FLAG_NO_WRITES == 0 && inv.ro {
        return Response::Error(RO_WITH_WRITE_FLAG.into());
    }
    if let Some(refused) = refused_before_run(flags, compat, host) {
        return refused;
    }
    let read_only = inv.ro || (!compat && flags & FLAG_NO_WRITES != 0);
    let lua = &engine.sb.lua;
    let set = (|| {
        engine.sb.globals.raw_set("KEYS", lua_array(lua, keys)?)?;
        engine.sb.globals.raw_set("ARGV", lua_array(lua, args)?)
    })();
    if let Err(e) = set {
        return Response::Error(format!("ERR {e}"));
    }
    let _caller = CallerGuard::set();
    let run = crate::server::ScriptRunHandle::start(true, sha.as_bytes(), &inv.argv);
    let _interrupt = InterruptGuard::set(Interrupt::Run(run.probe()));
    let mut ctx = CallCtx {
        host,
        db,
        read_only,
        run: &run,
    };
    crate::evict::allowing_oom(allows_oom(flags, compat), || {
        engine.sb.call(&f, None, None, sha, &mut ctx)
    })
}

// ───────────────────────────────── SHA-1 ────────────────────────────────────

/// SHA-1 in lowercase hex: a script's name in the cache.
pub fn sha1_hex(data: &[u8]) -> String {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut msg = data.to_vec();
    let bits = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bits.to_be_bytes());
    for block in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (i, word) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (x, y) in h.iter_mut().zip([a, b, c, d, e]) {
            *x = x.wrapping_add(y);
        }
    }
    h.iter().map(|x| format!("{x:08x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A host with no backend: commands answer from a script of replies,
    /// tables live in memory, and commit hooks run at once.
    #[derive(Default)]
    struct MockHost {
        kv: HashMap<Vec<u8>, Vec<u8>>,
        scripts: HashMap<String, Vec<u8>>,
        libs: std::collections::BTreeMap<String, Vec<u8>>,
        calls: Vec<(u8, String)>,
    }

    impl Host for MockHost {
        fn exec(&mut self, db: u8, cmd: Command) -> Response {
            self.calls.push((db, format!("{cmd:?}")));
            match cmd {
                Command::Set { key, value, .. } => {
                    self.kv.insert(key, value);
                    Response::Ok
                }
                Command::Get { key } => self
                    .kv
                    .get(&key)
                    .cloned()
                    .map_or(Response::Null, Response::BulkString),
                Command::Incr { key } => match self.kv.get(&key) {
                    Some(v) => match std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(n) => {
                            self.kv.insert(key, (n + 1).to_string().into_bytes());
                            Response::Integer(n + 1)
                        }
                        None => {
                            Response::Error("ERR value is not an integer or out of range".into())
                        }
                    },
                    None => {
                        self.kv.insert(key, b"1".to_vec());
                        Response::Integer(1)
                    }
                },
                Command::Ping { msg } => Response::Pong(msg),
                _ => Response::Null,
            }
        }
        fn script_body(&mut self, sha: &str) -> Option<Vec<u8>> {
            self.scripts.get(sha).cloned()
        }
        fn save_script(&mut self, sha: &str, body: &[u8]) {
            self.scripts.insert(sha.into(), body.to_vec());
        }
        fn scripts_exist(&mut self, shas: &[String]) -> Vec<bool> {
            shas.iter().map(|s| self.scripts.contains_key(s)).collect()
        }
        fn flush_scripts(&mut self) {
            self.scripts.clear();
        }
        fn libraries(&mut self) -> Vec<Vec<u8>> {
            self.libs.values().cloned().collect()
        }
        fn save_library(&mut self, name: &str, code: &[u8]) {
            self.libs.insert(name.into(), code.to_vec());
        }
        fn delete_library(&mut self, name: &str) {
            self.libs.remove(name);
        }
        fn flush_libraries(&mut self) {
            self.libs.clear();
        }
        fn on_commit(&mut self, f: fn()) {
            f();
        }
        fn on_abort(&mut self, _: fn()) {}
    }

    fn run(host: &mut MockHost, argv: &[&str]) -> Response {
        let argv: Vec<Vec<u8>> = argv.iter().map(|a| a.as_bytes().to_vec()).collect();
        let name = String::from_utf8_lossy(&argv[0]).to_ascii_uppercase();
        match parse(&name, &argv[0], &argv[1..]) {
            Some(Ok(Command::Script(c))) => execute(&c, 0, host),
            Some(Ok(Command::Admin(a))) => admin_execute(&a),
            Some(Err(e)) => Response::Error(e),
            other => panic!("not a scripting command: {other:?}"),
        }
    }

    fn eval(host: &mut MockHost, body: &str, rest: &[&str]) -> Response {
        let mut argv = vec!["EVAL", body];
        argv.extend_from_slice(rest);
        run(host, &argv)
    }

    fn error_of(r: Response) -> String {
        match r {
            Response::Error(e)
            | Response::Value(Value::Error(e))
            | Response::Value(Value::CountedError(e)) => e,
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn a_source_that_names_another_database_is_found() {
        for src in [
            "redis.call('SELECT', 1)",
            "redis.call(\"select\",ARGV[1])",
            "return redis.call('move', KEYS[1], 2)",
            "redis.pcall('COPY', 'a', 'b', 'DB', '3')",
            "redis.call('swapdb', 0, 1)",
            "redis.call('FlushAll')",
            "-- select\nreturn 1",
            "return redis.call(ARGV[1], KEYS[1])",
            "local c = redis.call return c('get', 'k')",
            "redis.pcall('sel' .. 'ect', 1)",
            "redis.call(unpack(ARGV))",
            "server.call(cmd)",
            "redis.call('get', 'k') redis.call(name)",
        ] {
            assert!(names_other_db(src.as_bytes()), "{src}");
        }
        for src in [
            "return redis.call('get', KEYS[1])",
            "redis.call('LMOVE', 'a', 'b', 'LEFT', 'LEFT')",
            "redis.call('SMOVE', 'a', 'b', 'm')",
            "local remove = 1 local selected = 2 return remove + selected",
            "redis.call('FLUSHDB')",
            "redis.call( \"incr\" , KEYS[1])",
            "redis.call([[get]], 'k')",
            "redis.call([==[get]==], 'k')",
            "redis.call 'ping'",
            "local ok = pcall(function() return 1 end) return redis.pcall('get', 'x')",
            "redis.call('set', 'k', 'it\\'s')",
        ] {
            assert!(!names_other_db(src.as_bytes()), "{src}");
        }
    }

    #[test]
    fn sha1_matches_redis() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            sha1_hex(b"Pizza & Mandolino"),
            "74822d82031af7493c20eefa13bd07ec4fada82f"
        );
        assert_eq!(
            sha1_hex(b"return redis.call('get',KEYS[1])"),
            "fd758d1589d044dd850a6f05d52f2eefd27f033f"
        );
    }

    #[test]
    fn lua_values_convert_like_redis() {
        let mut h = MockHost::default();
        assert!(
            matches!(eval(&mut h, "return 'hello'", &["0"]), Response::Value(Value::Bulk(b)) if b == b"hello")
        );
        assert!(matches!(
            eval(&mut h, "return 100.5", &["0"]),
            Response::Value(Value::Int(100))
        ));
        assert!(matches!(
            eval(&mut h, "return true", &["0"]),
            Response::Value(Value::Int(1))
        ));
        assert!(matches!(
            eval(&mut h, "return false", &["0"]),
            Response::Value(Value::Nil)
        ));
        assert!(
            matches!(eval(&mut h, "return {ok='fine'}", &["0"]), Response::Value(Value::Simple(s)) if s == "fine")
        );
        assert_eq!(
            error_of(eval(&mut h, "return {err='ERR this is an error'}", &["0"])),
            "ERR this is an error"
        );
        assert!(
            matches!(eval(&mut h, "return _G", &["0"]), Response::Value(Value::Array(v)) if v.is_empty())
        );
        assert_eq!(
            eval(&mut h, "return {1,2,3,'ciao',{1,2}}", &["0"]).clone_value(),
            Value::Array(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::str("ciao"),
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
            ])
        );
        assert_eq!(
            eval(
                &mut h,
                "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
                &["2", "a", "b", "c", "d"]
            )
            .clone_value(),
            Value::Array(["a", "b", "c", "d"].iter().map(|s| Value::str(s)).collect())
        );
    }

    #[test]
    fn redis_call_reaches_the_host_and_errors_carry_the_line() {
        let mut h = MockHost::default();
        assert!(matches!(
            eval(&mut h, "redis.call('set', KEYS[1], 'v'); return redis.call('get', KEYS[1])", &["1", "k"]),
            Response::Value(Value::Bulk(b)) if b == b"v"
        ));
        let e = error_of(eval(
            &mut h,
            "return redis.call('incr', KEYS[1])",
            &["1", "k"],
        ));
        assert!(
            e.starts_with("ERR value is not an integer or out of range script: "),
            "{e}"
        );
        assert!(e.ends_with("on @user_script:1."), "{e}");
        assert!(matches!(
            eval(&mut h, "local t = redis.pcall('incr', KEYS[1]); return t['err']", &["1", "k"]),
            Response::Value(Value::Bulk(b)) if b == b"ERR value is not an integer or out of range"
        ));
        assert!(error_of(eval(&mut h, "return redis.call()", &["0"])).contains("one argument"));
        assert!(
            error_of(eval(&mut h, "redis.call('nosuchcommand')", &["0"])).contains("Unknown Redis")
        );
        assert!(
            error_of(eval(&mut h, "redis.call('get','a','b','c')", &["0"]))
                .contains("number of args")
        );
        assert!(
            error_of(eval(&mut h, "return redis.call('ping', {})", &["0"]))
                .starts_with("ERR Lua redis lib command arguments must be strings or integers")
        );
    }

    #[test]
    fn globals_are_protected() {
        let mut h = MockHost::default();
        assert!(
            error_of(eval(&mut h, "return a", &["0"]))
                .contains("Script attempted to access nonexistent global variable 'a'")
        );
        for body in [
            "a = 10",
            "setmetatable(_G, {})",
            "local g = getmetatable(_G); g.__index = {}",
            "redis = function() return 1 end",
            "_G = {}",
            "redis.call = function() return 1 end",
            "cjson.encode = function() return 1 end",
            "cmsgpack.pack = function() return 1 end",
            "bit.lshift = function() return 1 end",
            "rawset(_G, 'x', 1)",
            "getmetatable('').__index = {}",
        ] {
            let e = error_of(eval(&mut h, body, &["0"]));
            assert!(
                e.contains("Attempt to modify a readonly table"),
                "{body}: {e}"
            );
        }
        for f in ["loadfile", "dofile", "print"] {
            let e = error_of(eval(&mut h, &format!("{f}('x')"), &["0"]));
            assert!(
                e.contains(&format!("nonexistent global variable '{f}'")),
                "{e}"
            );
        }
        let e = error_of(eval(
            &mut h,
            "return loadstring(string.dump(function() return 1 end))()",
            &["0"],
        ));
        assert!(e.contains("attempt to call a nil value"), "{e}");
        // A pcall'd global miss hands back the message, not the table.
        assert!(matches!(
            eval(&mut h, "local s, r = pcall(function() return foo end); return tostring(s) .. ' ' .. r", &["0"]),
            Response::Value(Value::Bulk(b)) if String::from_utf8_lossy(&b).starts_with("false ") && String::from_utf8_lossy(&b).ends_with("variable 'foo'")
        ));
    }

    #[test]
    fn libraries_answer_like_redis() {
        let mut h = MockHost::default();
        let hex = |h: &mut MockHost, body: &str| match eval(h, body, &["0"]) {
            Response::Value(Value::Bulk(b)) => String::from_utf8(b).unwrap(),
            other => panic!("{other:?}"),
        };
        let pack_hex = "local e = cmsgpack.pack(ARG) local h = '' for i = 1, #e do h = h .. string.format('%02x', string.byte(e, i)) end return h";
        assert_eq!(
            hex(&mut h, &pack_hex.replace("ARG", "0.1")),
            "cb3fb999999999999a"
        );
        assert_eq!(
            hex(&mut h, &pack_hex.replace("ARG", "-1099511627776")),
            "d3ffffff0000000000"
        );
        assert_eq!(
            hex(
                &mut h,
                "return table.concat(cjson.decode('[0.0, -5e3, -1, 0.3e-3, 1023.2, 0e10]'), ' ')"
            ),
            "0 -5000 -1 0.0003 1023.2 0"
        );
        assert!(matches!(
            eval(&mut h, "return bit.band(0xff, 0x0f)", &["0"]),
            Response::Value(Value::Int(15))
        ));
        assert!(matches!(
            eval(
                &mut h,
                "return struct.unpack('>I2', struct.pack('>I2', 258))",
                &["0"]
            ),
            Response::Value(Value::Int(258))
        ));
        assert_eq!(
            hex(&mut h, "return redis.sha1hex('Pizza & Mandolino')"),
            "74822d82031af7493c20eefa13bd07ec4fada82f"
        );
    }

    #[test]
    fn prng_seeds_like_redis() {
        let mut h = MockHost::default();
        let roll = |h: &mut MockHost, seed: &str| {
            eval(
                h,
                "math.randomseed(ARGV[1]); return tostring(math.random())",
                &["0", seed],
            )
        };
        assert_eq!(
            roll(&mut h, "10").clone_value(),
            roll(&mut h, "10").clone_value()
        );
        assert_ne!(
            roll(&mut h, "10").clone_value(),
            roll(&mut h, "20").clone_value()
        );
    }

    #[test]
    fn script_cache_and_evalsha() {
        let mut h = MockHost::default();
        let sha = match run(&mut h, &["SCRIPT", "LOAD", "return 'x'"]) {
            Response::BulkString(s) => String::from_utf8(s).unwrap(),
            other => panic!("{other:?}"),
        };
        assert!(
            matches!(run(&mut h, &["EVALSHA", &sha.to_uppercase(), "0"]), Response::Value(Value::Bulk(b)) if b == b"x")
        );
        assert!(
            matches!(run(&mut h, &["SCRIPT", "EXISTS", &sha, "ffffffffffffffffffffffffffffffffffffffff"]), Response::IntegerArray(v) if v == vec![1, 0])
        );
        assert!(error_of(run(&mut h, &["EVALSHA", "NotValidShaSUM", "0"])).starts_with("NOSCRIPT"));
        assert!(
            error_of(eval(&mut h, "return 1", &["3", "a"])).contains("greater than number of args")
        );
        assert!(error_of(eval(&mut h, "return 1", &["-1"])).contains("can't be negative"));
        assert!(
            error_of(eval(&mut h, "return (", &["0"]))
                .starts_with("ERR Error compiling script (new function): user_script:1:")
        );
    }

    #[test]
    fn read_only_scripts_refuse_writes() {
        let mut h = MockHost::default();
        let e = error_of(run(
            &mut h,
            &["EVAL_RO", "return redis.call('set','x','y')", "1", "x"],
        ));
        assert!(
            e.starts_with("ERR Write commands are not allowed from read-only scripts."),
            "{e}"
        );
        let e = error_of(run(&mut h, &["EVAL_RO", "#!lua\nreturn 1", "0"]));
        assert_eq!(e, RO_WITH_WRITE_FLAG);
        assert!(matches!(
            run(&mut h, &["EVAL_RO", "#!lua flags=no-writes\nreturn 1", "0"]),
            Response::Value(Value::Int(1))
        ));
        assert!(
            error_of(eval(&mut h, "#!lua flags=bogus\nreturn 1", &["0"]))
                .contains("Unexpected flag in script shebang: bogus")
        );
    }

    #[test]
    fn functions_load_call_list_and_restore() {
        let mut h = MockHost::default();
        let lib = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return args[1] end)\nredis.register_function{function_name='ro', callback=function() return 1 end, flags={'no-writes'}}";
        assert!(
            matches!(run(&mut h, &["FUNCTION", "LOAD", lib]), Response::BulkString(b) if b == b"mylib")
        );
        assert!(
            error_of(run(&mut h, &["FUNCTION", "LOAD", lib]))
                .contains("Library 'mylib' already exists")
        );
        assert!(
            matches!(run(&mut h, &["FCALL", "myfunc", "0", "hi"]), Response::Value(Value::Bulk(b)) if b == b"hi")
        );
        assert!(error_of(run(&mut h, &["FCALL_RO", "myfunc", "0"])).contains("write flag"));
        assert!(matches!(
            run(&mut h, &["FCALL_RO", "ro", "0"]),
            Response::Value(Value::Int(1))
        ));
        assert_eq!(
            error_of(run(&mut h, &["FCALL", "nope", "0"])),
            "ERR Function not found"
        );
        assert!(
            error_of(run(
                &mut h,
                &[
                    "FUNCTION",
                    "LOAD",
                    "#!lua name=other\nredis.register_function('myfunc', function() end)"
                ]
            ))
            .contains("Function myfunc already exists")
        );
        assert!(
            error_of(run(
                &mut h,
                &[
                    "FUNCTION",
                    "LOAD",
                    "#!lua name=bad\nlocal x = string.len('a')"
                ]
            ))
            .contains("nonexistent global variable 'string'")
        );
        assert!(
            error_of(run(
                &mut h,
                &["FUNCTION", "LOAD", "#!lua name=empty\nlocal x = 1"]
            ))
            .contains("No functions registered")
        );
        assert!(
            error_of(run(
                &mut h,
                &["FUNCTION", "LOAD", "#!lua name=slow\nwhile true do end"]
            ))
            .contains("FUNCTION LOAD timeout")
        );

        let dump = match run(&mut h, &["FUNCTION", "DUMP"]) {
            Response::BulkString(b) => b,
            other => panic!("{other:?}"),
        };
        assert!(matches!(run(&mut h, &["FUNCTION", "FLUSH"]), Response::Ok));
        assert_eq!(
            error_of(run(&mut h, &["FCALL", "myfunc", "0"])),
            "ERR Function not found"
        );
        let mut argv = vec![b"RESTORE".to_vec(), dump.clone()];
        let restore =
            |h: &mut MockHost, argv: &[Vec<u8>]| match parse("FUNCTION", b"FUNCTION", argv) {
                Some(Ok(Command::Script(c))) => execute(&c, 0, h),
                other => panic!("{other:?}"),
            };
        assert!(matches!(restore(&mut h, &argv), Response::Ok));
        assert!(error_of(restore(&mut h, &argv)).contains("Library mylib already exists"));
        argv.push(b"REPLACE".to_vec());
        assert!(matches!(restore(&mut h, &argv), Response::Ok));
        assert!(
            matches!(run(&mut h, &["FCALL", "myfunc", "0", "back"]), Response::Value(Value::Bulk(b)) if b == b"back")
        );
        match run(&mut h, &["FUNCTION", "LIST"]) {
            Response::Value(Value::Array(libs)) => assert_eq!(libs.len(), 1),
            other => panic!("{other:?}"),
        }
    }

    impl Response {
        fn clone_value(&self) -> Value {
            match self {
                Response::Value(v) => v.clone(),
                other => panic!("{other:?}"),
            }
        }
    }
}
