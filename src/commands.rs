use pgrx::datum::DatumWithOid;
use pgrx::spi::SpiClient;

pub const NUM_DBS: usize = 16;

/// The databases are split into two contiguous halves rather than interleaved
/// by parity: `0..DURABLE_FROM` are ephemeral, `DURABLE_FROM..NUM_DBS` are
/// WAL-logged. Halves are what clients can hold in their head — "anything under
/// 8 is a cache" survives a code review in a way "even numbers are a cache"
/// does not.
pub const DURABLE_FROM: u8 = 8;

/// Lowest ephemeral database — what `SELECT cache` resolves to.
pub const CACHE_DB: u8 = 0;

/// Lowest durable database — what `SELECT durable` resolves to.
pub const DURABLE_DB: u8 = DURABLE_FROM;

/// Whether `db` belongs to the ephemeral half.
pub const fn is_ephemeral(db: u8) -> bool {
    db < DURABLE_FROM
}

pub mod sql {
    use super::NUM_DBS;
    use std::sync::LazyLock;

    fn arr<F: Fn(usize) -> String>(f: F) -> [String; NUM_DBS] {
        std::array::from_fn(f)
    }

    pub static GET: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT value FROM redis.kv_{db} WHERE key=$1 \
         AND (expires_at IS NULL OR expires_at > now())"
            )
        })
    });

    pub static MGET: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT key, value FROM redis.kv_{db} \
         WHERE key=ANY($1::bytea[]) AND (expires_at IS NULL OR expires_at > now())"
            )
        })
    });

    pub static STRLEN: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT coalesce(length(value), 0)::bigint FROM redis.kv_{db} \
         WHERE key=$1 AND (expires_at IS NULL OR expires_at > now())"
            )
        })
    });

    pub static TTL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT CASE \
           WHEN r.key IS NULL THEN -2::bigint \
           WHEN r.expires_at IS NULL THEN -1::bigint \
           ELSE GREATEST(-1, EXTRACT(EPOCH FROM (r.expires_at - now()))::bigint) \
         END \
         FROM (VALUES ($1::bytea)) AS dummy(k) \
         LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
            )
        })
    });

    pub static PTTL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT CASE \
           WHEN r.key IS NULL THEN -2::bigint \
           WHEN r.expires_at IS NULL THEN -1::bigint \
           ELSE GREATEST(-1, (EXTRACT(EPOCH FROM (r.expires_at - now())) * 1000)::bigint) \
         END \
         FROM (VALUES ($1::bytea)) AS dummy(k) \
         LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
            )
        })
    });

    pub static EXPIRETIME: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT CASE \
           WHEN r.key IS NULL THEN -2::bigint \
           WHEN r.expires_at IS NULL THEN -1::bigint \
           ELSE EXTRACT(EPOCH FROM r.expires_at)::bigint \
         END \
         FROM (VALUES ($1::bytea)) AS dummy(k) \
         LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
            )
        })
    });

    pub static PEXPIRETIME: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT CASE \
           WHEN r.key IS NULL THEN -2::bigint \
           WHEN r.expires_at IS NULL THEN -1::bigint \
           ELSE (EXTRACT(EPOCH FROM r.expires_at) * 1000)::bigint \
         END \
         FROM (VALUES ($1::bytea)) AS dummy(k) \
         LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
            )
        })
    });

    pub static INCR: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to('1', 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((CAST(encode(redis.kv_{db}.value, 'escape') AS bigint) + 1)::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^-?[0-9]+$' \
         RETURNING CAST(encode(value, 'escape') AS bigint)"
            )
        })
    });

    pub static DECR: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to('-1', 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((CAST(encode(redis.kv_{db}.value, 'escape') AS bigint) - 1)::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^-?[0-9]+$' \
         RETURNING CAST(encode(value, 'escape') AS bigint)"
            )
        })
    });

    pub static INCRBY: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to($2::text, 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((CAST(encode(redis.kv_{db}.value, 'escape') AS bigint) + $3)::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^-?[0-9]+$' \
         RETURNING CAST(encode(value, 'escape') AS bigint)"
            )
        })
    });

    pub static DECRBY: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to((-$2::bigint)::text, 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((CAST(encode(redis.kv_{db}.value, 'escape') AS bigint) - $3)::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^-?[0-9]+$' \
         RETURNING CAST(encode(value, 'escape') AS bigint)"
            )
        })
    });

    pub static INCRBYFLOAT: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to($2::text, 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((CAST(encode(redis.kv_{db}.value, 'escape') AS float8) + $3)::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^-?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?$' \
         RETURNING value"
            )
        })
    });

    pub static PERSIST: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "UPDATE redis.kv_{db} SET expires_at = NULL \
         WHERE key=$1 AND expires_at IS NOT NULL"
            )
        })
    });

    pub static GETDEL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "DELETE FROM redis.kv_{db} WHERE key=$1 \
         AND (expires_at IS NULL OR expires_at > now()) \
         RETURNING value"
            )
        })
    });

    pub static HGET: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT value FROM redis.hash_{db} WHERE key=$1 AND field=$2"))
    });

    pub static HGETALL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT field, value FROM redis.hash_{db} WHERE key=$1 ORDER BY field"))
    });

    pub static HKEYS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT field FROM redis.hash_{db} WHERE key=$1 ORDER BY field"))
    });

    pub static HVALS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT value FROM redis.hash_{db} WHERE key=$1 ORDER BY field"))
    });

    pub static HLEN: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT count(*)::bigint FROM redis.hash_{db} WHERE key=$1"))
    });

    pub static HEXISTS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT 1 FROM redis.hash_{db} WHERE key=$1 AND field=$2"))
    });

    pub static SCARD: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT count(*)::bigint FROM redis.set_{db} WHERE key=$1"))
    });

    pub static SMEMBERS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT member FROM redis.set_{db} WHERE key=$1 ORDER BY member"))
    });

    pub static SISMEMBER: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT 1 FROM redis.set_{db} WHERE key=$1 AND member=$2"))
    });

    pub static ZCARD: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT count(*)::bigint FROM redis.zset_{db} WHERE key=$1"))
    });

    pub static ZSCORE: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT score FROM redis.zset_{db} WHERE key=$1 AND member=$2"))
    });

    pub static LLEN: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| format!("SELECT count(*)::bigint FROM redis.list_{db} WHERE key=$1"))
    });
}

/// Score bound for ZRANGEBYSCORE / ZCOUNT. `exclusive` encodes the Redis
/// `(` prefix; `value` may be +/- infinity.
#[derive(Debug, Clone, Copy)]
pub struct ScoreBound {
    pub value: f64,
    pub exclusive: bool,
}

/// Lex bound for ZRANGEBYLEX / ZLEXCOUNT. Redis uses `-` and `+` as
/// the lowest/highest string sentinels; `[m` is inclusive and `(m` is exclusive.
#[derive(Debug, Clone)]
pub enum LexBound {
    NegInf,
    PosInf,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregate {
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeBy {
    Index,
    Score,
    Lex,
}

#[derive(Debug)]
pub enum Command {
    Ping {
        msg: Option<Vec<u8>>,
    },
    Echo {
        msg: Vec<u8>,
    },
    Select {
        db: u8,
    },
    Auth {
        password: String,
    },
    Info,
    Hello {
        proto: Option<u8>,
        auth: Option<String>,
        _setname: Option<String>,
    },
    Reset,
    Quit,
    // CLIENT subcommands
    ClientId,
    ClientGetname,
    ClientSetname {
        _name: String,
    },
    ClientSetinfo,
    ClientList,
    ClientInfo,
    ClientNoEvict,
    ClientNoTouch,
    ClientOther,
    // COMMAND subcommands
    CmdCount,
    CmdInfo,
    CmdDocs,
    CmdList,
    CmdOther,
    // CONFIG subcommands
    ConfigGet {
        _pattern: String,
    },
    ConfigSet,
    ConfigOther,
    // Key-value commands
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ex_ms: Option<i64>,
        nx: bool,
        xx: bool,
        get: bool,
        keepttl: bool,
    },
    SetEx {
        key: Vec<u8>,
        value: Vec<u8>,
        ex_secs: i64,
    },
    PSetEx {
        key: Vec<u8>,
        value: Vec<u8>,
        ex_ms: i64,
    },
    MGet {
        keys: Vec<Vec<u8>>,
    },
    MSet {
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Del {
        keys: Vec<Vec<u8>>,
    },
    Exists {
        keys: Vec<Vec<u8>>,
    },
    Expire {
        key: Vec<u8>,
        secs: i64,
    },
    PExpire {
        key: Vec<u8>,
        ms: i64,
    },
    ExpireAt {
        key: Vec<u8>,
        unix_secs: i64,
    },
    PExpireAt {
        key: Vec<u8>,
        unix_ms: i64,
    },
    Ttl {
        key: Vec<u8>,
    },
    PTtl {
        key: Vec<u8>,
    },
    Persist {
        key: Vec<u8>,
    },
    ExpireTime {
        key: Vec<u8>,
    },
    PExpireTime {
        key: Vec<u8>,
    },
    // String commands
    Incr {
        key: Vec<u8>,
    },
    Decr {
        key: Vec<u8>,
    },
    IncrBy {
        key: Vec<u8>,
        delta: i64,
    },
    DecrBy {
        key: Vec<u8>,
        delta: i64,
    },
    IncrByFloat {
        key: Vec<u8>,
        delta: f64,
    },
    Append {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Strlen {
        key: Vec<u8>,
    },
    GetDel {
        key: Vec<u8>,
    },
    GetSet {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    SetNx {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    MSetNx {
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    // Key inspection commands
    Type {
        key: Vec<u8>,
    },
    Keys {
        pattern: Vec<u8>,
    },
    DbSize,
    Unlink {
        keys: Vec<Vec<u8>>,
    },
    Rename {
        key: Vec<u8>,
        newkey: Vec<u8>,
    },
    RandomKey,
    Scan {
        _cursor: u64,
        pattern: Option<Vec<u8>>,
        _count: Option<i64>,
    },
    // Hash commands
    HGet {
        key: Vec<u8>,
        field: Vec<u8>,
    },
    HSet {
        key: Vec<u8>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    HDel {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    HGetAll {
        key: Vec<u8>,
    },
    HMGet {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    HMSet {
        key: Vec<u8>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    HKeys {
        key: Vec<u8>,
    },
    HVals {
        key: Vec<u8>,
    },
    HExists {
        key: Vec<u8>,
        field: Vec<u8>,
    },
    HLen {
        key: Vec<u8>,
    },
    HIncrBy {
        key: Vec<u8>,
        field: Vec<u8>,
        delta: i64,
    },
    HSetNx {
        key: Vec<u8>,
        field: Vec<u8>,
        value: Vec<u8>,
    },
    // List commands
    LPush {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    RPush {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    LPushX {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    RPushX {
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    LPop {
        key: Vec<u8>,
        count: Option<i64>,
    },
    RPop {
        key: Vec<u8>,
        count: Option<i64>,
    },
    LLen {
        key: Vec<u8>,
    },
    LRange {
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    LIndex {
        key: Vec<u8>,
        index: i64,
    },
    LSet {
        key: Vec<u8>,
        index: i64,
        value: Vec<u8>,
    },
    LInsert {
        key: Vec<u8>,
        before: bool,
        pivot: Vec<u8>,
        value: Vec<u8>,
    },
    LRem {
        key: Vec<u8>,
        count: i64,
        value: Vec<u8>,
    },
    LMove {
        src: Vec<u8>,
        dst: Vec<u8>,
        src_left: bool,
        dst_left: bool,
    },
    LPos {
        key: Vec<u8>,
        element: Vec<u8>,
        rank: Option<i64>,
        count: Option<i64>,
    },
    LTrim {
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    // Set commands
    SAdd {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SRem {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SMembers {
        key: Vec<u8>,
    },
    SCard {
        key: Vec<u8>,
    },
    SIsMember {
        key: Vec<u8>,
        member: Vec<u8>,
    },
    SMisMember {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SPop {
        key: Vec<u8>,
        count: Option<i64>,
    },
    SRandMember {
        key: Vec<u8>,
        count: Option<i64>,
    },
    SUnion {
        keys: Vec<Vec<u8>>,
    },
    SInter {
        keys: Vec<Vec<u8>>,
    },
    SDiff {
        keys: Vec<Vec<u8>>,
    },
    SUnionStore {
        dst: Vec<u8>,
        keys: Vec<Vec<u8>>,
    },
    SInterStore {
        dst: Vec<u8>,
        keys: Vec<Vec<u8>>,
    },
    SDiffStore {
        dst: Vec<u8>,
        keys: Vec<Vec<u8>>,
    },
    SMove {
        src: Vec<u8>,
        dst: Vec<u8>,
        member: Vec<u8>,
    },
    // Sorted set commands
    ZAdd {
        key: Vec<u8>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
        pairs: Vec<(f64, Vec<u8>)>,
    },
    ZRem {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    ZScore {
        key: Vec<u8>,
        member: Vec<u8>,
    },
    ZMScore {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    ZIncrBy {
        key: Vec<u8>,
        increment: f64,
        member: Vec<u8>,
    },
    ZCard {
        key: Vec<u8>,
    },
    ZCount {
        key: Vec<u8>,
        min: ScoreBound,
        max: ScoreBound,
    },
    ZLexCount {
        key: Vec<u8>,
        min: LexBound,
        max: LexBound,
    },
    ZRank {
        key: Vec<u8>,
        member: Vec<u8>,
        rev: bool,
        with_score: bool,
    },
    ZRange {
        key: Vec<u8>,
        start: Vec<u8>,
        stop: Vec<u8>,
        by: RangeBy,
        rev: bool,
        limit: Option<(i64, i64)>,
        with_scores: bool,
    },
    ZRangeByScore {
        key: Vec<u8>,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        with_scores: bool,
        limit: Option<(i64, i64)>,
    },
    ZRangeByLex {
        key: Vec<u8>,
        min: LexBound,
        max: LexBound,
        rev: bool,
        limit: Option<(i64, i64)>,
    },
    ZPopMin {
        key: Vec<u8>,
        count: Option<i64>,
    },
    ZPopMax {
        key: Vec<u8>,
        count: Option<i64>,
    },
    ZRandMember {
        key: Vec<u8>,
        count: Option<i64>,
        with_scores: bool,
    },
    ZRemRangeByRank {
        key: Vec<u8>,
        start: i64,
        stop: i64,
    },
    ZRemRangeByScore {
        key: Vec<u8>,
        min: ScoreBound,
        max: ScoreBound,
    },
    ZRemRangeByLex {
        key: Vec<u8>,
        min: LexBound,
        max: LexBound,
    },
    ZUnion {
        keys: Vec<Vec<u8>>,
        weights: Option<Vec<f64>>,
        aggregate: Aggregate,
        with_scores: bool,
    },
    ZInter {
        keys: Vec<Vec<u8>>,
        weights: Option<Vec<f64>>,
        aggregate: Aggregate,
        with_scores: bool,
    },
    ZDiff {
        keys: Vec<Vec<u8>>,
        with_scores: bool,
    },
    ZUnionStore {
        dst: Vec<u8>,
        keys: Vec<Vec<u8>>,
        weights: Option<Vec<f64>>,
        aggregate: Aggregate,
    },
    ZInterStore {
        dst: Vec<u8>,
        keys: Vec<Vec<u8>>,
        weights: Option<Vec<f64>>,
        aggregate: Aggregate,
    },
    ZDiffStore {
        dst: Vec<u8>,
        keys: Vec<Vec<u8>>,
    },
    Subscribe {
        channels: Vec<Vec<u8>>,
    },
    Unsubscribe,
    PSubscribe {
        patterns: Vec<Vec<u8>>,
    },
    PUnsubscribe,
    Publish {
        channel: Vec<u8>,
        message: Vec<u8>,
    },
    PubSubChannels {
        pattern: Option<Vec<u8>>,
    },
    PubSubNumSub {
        channels: Vec<Vec<u8>>,
    },
    PubSubNumPat,
    PubSubHelp,
    // Internal: dispatched fire-and-forget when a PUBLISH channel has a table route.
    // Never parsed from the wire protocol.
    TablePublish {
        schema: Vec<u8>,
        table: Vec<u8>,
        channel: Vec<u8>,
        payload: Vec<u8>,
    },

    Multi,
    Exec,
    Discard,
    Watch {
        keys: Vec<Vec<u8>>,
    },
    Unwatch,
}

pub enum Response {
    Pong(Option<Vec<u8>>),
    Null,
    Ok,
    Integer(i64),
    BulkString(Vec<u8>),
    SimpleString(String),
    Array(Vec<Option<Vec<u8>>>),
    IntegerArray(Vec<i64>),
    ScanResult { keys: Vec<Option<Vec<u8>>> },
    Error(String),
}

impl Command {
    pub fn parse(parts: Vec<Vec<u8>>) -> Result<Command, String> {
        if parts.is_empty() {
            return Err("empty command".to_string());
        }
        let cmd = String::from_utf8_lossy(&parts[0]).to_uppercase();
        let args = &parts[1..];

        match cmd.as_str() {
            "PING" => Ok(Command::Ping {
                msg: args.first().cloned(),
            }),
            "ECHO" => {
                let msg = args.first().cloned().ok_or("ECHO requires argument")?;
                Ok(Command::Echo { msg })
            }
            "SELECT" => {
                let arg = str_arg(args, 0, "SELECT")?;
                // Names for the two halves so callers do not have to memorise
                // which numbers are durable. Redis rejects these, but it also
                // has no such split to name.
                let db = match arg.to_ascii_lowercase().as_str() {
                    "cache" => CACHE_DB,
                    "durable" => DURABLE_DB,
                    n => n
                        .parse::<u8>()
                        .map_err(|_| "SELECT requires integer db".to_string())?,
                };
                if db as usize >= NUM_DBS {
                    return Err("ERR DB index is out of range".to_string());
                }
                Ok(Command::Select { db })
            }
            "AUTH" => Ok(Command::Auth {
                // AUTH <password> or AUTH <username> <password> — password is always last.
                password: str_arg(args, args.len().saturating_sub(1), "AUTH").unwrap_or_default(),
            }),
            "INFO" => Ok(Command::Info),
            "HELLO" => {
                let proto = if args.is_empty() {
                    None
                } else {
                    let v: u8 = str_arg(args, 0, "HELLO")?.parse().map_err(|_| {
                        "Protocol version is not an integer or out of range".to_string()
                    })?;
                    if v != 2 && v != 3 {
                        return Err("NOPROTO unsupported protocol version".to_string());
                    }
                    Some(v)
                };
                let mut auth: Option<String> = None;
                let mut setname: Option<String> = None;
                let mut i = 1;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "AUTH" => {
                            let password = str_arg(args, i + 2, "HELLO AUTH")
                                .or_else(|_| str_arg(args, i + 1, "HELLO AUTH"))?;
                            auth = Some(password);
                            i += if i + 2 < args.len() { 3 } else { 2 };
                        }
                        "SETNAME" => {
                            setname = Some(str_arg(args, i + 1, "HELLO SETNAME")?);
                            i += 2;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
                Ok(Command::Hello {
                    proto,
                    auth,
                    _setname: setname,
                })
            }
            "RESET" => Ok(Command::Reset),
            "QUIT" => Ok(Command::Quit),
            "COMMAND" => {
                let sub = args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("COUNT") => Ok(Command::CmdCount),
                    Some("INFO") => Ok(Command::CmdInfo),
                    Some("DOCS") => Ok(Command::CmdDocs),
                    Some("LIST") => Ok(Command::CmdList),
                    _ => Ok(Command::CmdOther),
                }
            }
            "CLIENT" => {
                let sub = args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("ID") => Ok(Command::ClientId),
                    Some("GETNAME") => Ok(Command::ClientGetname),
                    Some("SETNAME") => Ok(Command::ClientSetname {
                        _name: str_arg(args, 1, "CLIENT SETNAME")?,
                    }),
                    Some("SETINFO") => Ok(Command::ClientSetinfo),
                    Some("LIST") => Ok(Command::ClientList),
                    Some("INFO") => Ok(Command::ClientInfo),
                    Some("NO-EVICT") => Ok(Command::ClientNoEvict),
                    Some("NO-TOUCH") => Ok(Command::ClientNoTouch),
                    _ => Ok(Command::ClientOther),
                }
            }
            "CONFIG" => {
                let sub = args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("GET") => Ok(Command::ConfigGet {
                        _pattern: str_arg(args, 1, "CONFIG GET")?,
                    }),
                    Some("SET") => Ok(Command::ConfigSet),
                    _ => Ok(Command::ConfigOther),
                }
            }
            "GET" => Ok(Command::Get {
                key: bytes_arg(args, 0, "GET")?,
            }),
            "SET" => {
                let key = bytes_arg(args, 0, "SET")?;
                let value = bytes_arg(args, 1, "SET")?;
                let mut ex_ms: Option<i64> = None;
                let mut nx = false;
                let mut xx = false;
                let mut get = false;
                let mut keepttl = false;
                let mut ttl_set = false;
                let mut i = 2;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "EX" => {
                            let secs: i64 = str_arg(args, i + 1, "SET EX")?
                                .parse()
                                .map_err(|_| "EX requires integer".to_string())?;
                            ex_ms = Some(secs * 1000);
                            ttl_set = true;
                            i += 2;
                        }
                        "PX" => {
                            let ms: i64 = str_arg(args, i + 1, "SET PX")?
                                .parse()
                                .map_err(|_| "PX requires integer".to_string())?;
                            ex_ms = Some(ms);
                            ttl_set = true;
                            i += 2;
                        }
                        "EXAT" => {
                            let secs: i64 = str_arg(args, i + 1, "SET EXAT")?
                                .parse()
                                .map_err(|_| "EXAT requires integer".to_string())?;
                            ex_ms = Some(secs.saturating_mul(1000) - now_ms());
                            ttl_set = true;
                            i += 2;
                        }
                        "PXAT" => {
                            let ms: i64 = str_arg(args, i + 1, "SET PXAT")?
                                .parse()
                                .map_err(|_| "PXAT requires integer".to_string())?;
                            ex_ms = Some(ms - now_ms());
                            ttl_set = true;
                            i += 2;
                        }
                        "NX" => {
                            nx = true;
                            i += 1;
                        }
                        "XX" => {
                            xx = true;
                            i += 1;
                        }
                        "GET" => {
                            get = true;
                            i += 1;
                        }
                        "KEEPTTL" => {
                            keepttl = true;
                            i += 1;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
                if nx && xx {
                    return Err("syntax error".to_string());
                }
                if keepttl && ttl_set {
                    return Err("syntax error".to_string());
                }
                Ok(Command::Set {
                    key,
                    value,
                    ex_ms,
                    nx,
                    xx,
                    get,
                    keepttl,
                })
            }
            "SETEX" => Ok(Command::SetEx {
                key: bytes_arg(args, 0, "SETEX")?,
                ex_secs: str_arg(args, 1, "SETEX")?
                    .parse()
                    .map_err(|_| "SETEX requires integer seconds".to_string())?,
                value: bytes_arg(args, 2, "SETEX")?,
            }),
            "PSETEX" => Ok(Command::PSetEx {
                key: bytes_arg(args, 0, "PSETEX")?,
                ex_ms: str_arg(args, 1, "PSETEX")?
                    .parse()
                    .map_err(|_| "PSETEX requires integer ms".to_string())?,
                value: bytes_arg(args, 2, "PSETEX")?,
            }),
            "MGET" => {
                if args.is_empty() {
                    return Err("MGET requires at least one key".to_string());
                }
                Ok(Command::MGet {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "MSET" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return Err("MSET requires pairs of key value".to_string());
                }
                let pairs = args
                    .chunks(2)
                    .map(|c| (c[0].to_vec(), c[1].to_vec()))
                    .collect();
                Ok(Command::MSet { pairs })
            }
            "DEL" => {
                if args.is_empty() {
                    return Err("DEL requires at least one key".to_string());
                }
                Ok(Command::Del {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "EXISTS" => {
                if args.is_empty() {
                    return Err("EXISTS requires at least one key".to_string());
                }
                Ok(Command::Exists {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "EXPIRE" => Ok(Command::Expire {
                key: bytes_arg(args, 0, "EXPIRE")?,
                secs: str_arg(args, 1, "EXPIRE")?
                    .parse()
                    .map_err(|_| "EXPIRE requires integer".to_string())?,
            }),
            "PEXPIRE" => Ok(Command::PExpire {
                key: bytes_arg(args, 0, "PEXPIRE")?,
                ms: str_arg(args, 1, "PEXPIRE")?
                    .parse()
                    .map_err(|_| "PEXPIRE requires integer".to_string())?,
            }),
            "EXPIREAT" => Ok(Command::ExpireAt {
                key: bytes_arg(args, 0, "EXPIREAT")?,
                unix_secs: str_arg(args, 1, "EXPIREAT")?
                    .parse()
                    .map_err(|_| "EXPIREAT requires integer".to_string())?,
            }),
            "PEXPIREAT" => Ok(Command::PExpireAt {
                key: bytes_arg(args, 0, "PEXPIREAT")?,
                unix_ms: str_arg(args, 1, "PEXPIREAT")?
                    .parse()
                    .map_err(|_| "PEXPIREAT requires integer".to_string())?,
            }),
            "TTL" => Ok(Command::Ttl {
                key: bytes_arg(args, 0, "TTL")?,
            }),
            "PTTL" => Ok(Command::PTtl {
                key: bytes_arg(args, 0, "PTTL")?,
            }),
            "PERSIST" => Ok(Command::Persist {
                key: bytes_arg(args, 0, "PERSIST")?,
            }),
            "EXPIRETIME" => Ok(Command::ExpireTime {
                key: bytes_arg(args, 0, "EXPIRETIME")?,
            }),
            "PEXPIRETIME" => Ok(Command::PExpireTime {
                key: bytes_arg(args, 0, "PEXPIRETIME")?,
            }),
            "INCR" => Ok(Command::Incr {
                key: bytes_arg(args, 0, "INCR")?,
            }),
            "DECR" => Ok(Command::Decr {
                key: bytes_arg(args, 0, "DECR")?,
            }),
            "INCRBY" => Ok(Command::IncrBy {
                key: bytes_arg(args, 0, "INCRBY")?,
                delta: str_arg(args, 1, "INCRBY")?
                    .parse()
                    .map_err(|_| "INCRBY requires integer".to_string())?,
            }),
            "DECRBY" => Ok(Command::DecrBy {
                key: bytes_arg(args, 0, "DECRBY")?,
                delta: str_arg(args, 1, "DECRBY")?
                    .parse()
                    .map_err(|_| "DECRBY requires integer".to_string())?,
            }),
            "INCRBYFLOAT" => Ok(Command::IncrByFloat {
                key: bytes_arg(args, 0, "INCRBYFLOAT")?,
                delta: str_arg(args, 1, "INCRBYFLOAT")?
                    .parse()
                    .map_err(|_| "INCRBYFLOAT requires float".to_string())?,
            }),
            "APPEND" => Ok(Command::Append {
                key: bytes_arg(args, 0, "APPEND")?,
                value: bytes_arg(args, 1, "APPEND")?,
            }),
            "STRLEN" => Ok(Command::Strlen {
                key: bytes_arg(args, 0, "STRLEN")?,
            }),
            "GETDEL" => Ok(Command::GetDel {
                key: bytes_arg(args, 0, "GETDEL")?,
            }),
            "GETSET" => Ok(Command::GetSet {
                key: bytes_arg(args, 0, "GETSET")?,
                value: bytes_arg(args, 1, "GETSET")?,
            }),
            "SETNX" => Ok(Command::SetNx {
                key: bytes_arg(args, 0, "SETNX")?,
                value: bytes_arg(args, 1, "SETNX")?,
            }),
            "MSETNX" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return Err("MSETNX requires pairs of key value".to_string());
                }
                let pairs = args
                    .chunks(2)
                    .map(|c| (c[0].to_vec(), c[1].to_vec()))
                    .collect();
                Ok(Command::MSetNx { pairs })
            }
            "TYPE" => Ok(Command::Type {
                key: bytes_arg(args, 0, "TYPE")?,
            }),
            "KEYS" => Ok(Command::Keys {
                pattern: bytes_arg(args, 0, "KEYS")?,
            }),
            "DBSIZE" => Ok(Command::DbSize),
            "UNLINK" => {
                if args.is_empty() {
                    return Err("UNLINK requires at least one key".to_string());
                }
                Ok(Command::Unlink {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "RENAME" => Ok(Command::Rename {
                key: bytes_arg(args, 0, "RENAME")?,
                newkey: bytes_arg(args, 1, "RENAME")?,
            }),
            "RANDOMKEY" => Ok(Command::RandomKey),
            "SCAN" => {
                let cursor: u64 = str_arg(args, 0, "SCAN")?
                    .parse()
                    .map_err(|_| "SCAN requires integer cursor".to_string())?;
                let mut pattern: Option<Vec<u8>> = None;
                let mut count: Option<i64> = None;
                let mut i = 1;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "MATCH" => {
                            pattern = Some(bytes_arg(args, i + 1, "SCAN MATCH")?);
                            i += 2;
                        }
                        "COUNT" => {
                            count = Some(
                                str_arg(args, i + 1, "SCAN COUNT")?
                                    .parse()
                                    .map_err(|_| "SCAN COUNT requires integer".to_string())?,
                            );
                            i += 2;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
                Ok(Command::Scan {
                    _cursor: cursor,
                    pattern,
                    _count: count,
                })
            }
            "HGET" => Ok(Command::HGet {
                key: bytes_arg(args, 0, "HGET")?,
                field: bytes_arg(args, 1, "HGET")?,
            }),
            "HSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return Err("HSET requires key field value [field value ...]".to_string());
                }
                let key = bytes_arg(args, 0, "HSET")?;
                let pairs = args[1..]
                    .chunks(2)
                    .map(|c| (c[0].to_vec(), c[1].to_vec()))
                    .collect();
                Ok(Command::HSet { key, pairs })
            }
            "HDEL" => {
                if args.len() < 2 {
                    return Err("HDEL requires key and at least one field".to_string());
                }
                Ok(Command::HDel {
                    key: bytes_arg(args, 0, "HDEL")?,
                    fields: args[1..].iter().map(|a| a.to_vec()).collect(),
                })
            }
            "HGETALL" => Ok(Command::HGetAll {
                key: bytes_arg(args, 0, "HGETALL")?,
            }),
            "HMGET" => {
                if args.len() < 2 {
                    return Err("HMGET requires key and at least one field".to_string());
                }
                Ok(Command::HMGet {
                    key: bytes_arg(args, 0, "HMGET")?,
                    fields: args[1..].iter().map(|a| a.to_vec()).collect(),
                })
            }
            "HMSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return Err("HMSET requires key field value [field value ...]".to_string());
                }
                let key = bytes_arg(args, 0, "HMSET")?;
                let pairs = args[1..]
                    .chunks(2)
                    .map(|c| (c[0].to_vec(), c[1].to_vec()))
                    .collect();
                Ok(Command::HMSet { key, pairs })
            }
            "HKEYS" => Ok(Command::HKeys {
                key: bytes_arg(args, 0, "HKEYS")?,
            }),
            "HVALS" => Ok(Command::HVals {
                key: bytes_arg(args, 0, "HVALS")?,
            }),
            "HEXISTS" => Ok(Command::HExists {
                key: bytes_arg(args, 0, "HEXISTS")?,
                field: bytes_arg(args, 1, "HEXISTS")?,
            }),
            "HLEN" => Ok(Command::HLen {
                key: bytes_arg(args, 0, "HLEN")?,
            }),
            "HINCRBY" => Ok(Command::HIncrBy {
                key: bytes_arg(args, 0, "HINCRBY")?,
                field: bytes_arg(args, 1, "HINCRBY")?,
                delta: str_arg(args, 2, "HINCRBY")?
                    .parse()
                    .map_err(|_| "HINCRBY requires integer".to_string())?,
            }),
            "HSETNX" => Ok(Command::HSetNx {
                key: bytes_arg(args, 0, "HSETNX")?,
                field: bytes_arg(args, 1, "HSETNX")?,
                value: bytes_arg(args, 2, "HSETNX")?,
            }),

            "LPUSH" => {
                if args.len() < 2 {
                    return Err("LPUSH requires key value [value ...]".to_string());
                }
                let key = bytes_arg(args, 0, "LPUSH")?;
                let values = (1..args.len())
                    .map(|i| bytes_arg(args, i, "LPUSH"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::LPush { key, values })
            }
            "RPUSH" => {
                if args.len() < 2 {
                    return Err("RPUSH requires key value [value ...]".to_string());
                }
                let key = bytes_arg(args, 0, "RPUSH")?;
                let values = (1..args.len())
                    .map(|i| bytes_arg(args, i, "RPUSH"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::RPush { key, values })
            }
            "LPUSHX" => {
                if args.len() < 2 {
                    return Err("LPUSHX requires key value [value ...]".to_string());
                }
                let key = bytes_arg(args, 0, "LPUSHX")?;
                let values = (1..args.len())
                    .map(|i| bytes_arg(args, i, "LPUSHX"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::LPushX { key, values })
            }
            "RPUSHX" => {
                if args.len() < 2 {
                    return Err("RPUSHX requires key value [value ...]".to_string());
                }
                let key = bytes_arg(args, 0, "RPUSHX")?;
                let values = (1..args.len())
                    .map(|i| bytes_arg(args, i, "RPUSHX"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::RPushX { key, values })
            }
            "LPOP" => {
                let key = bytes_arg(args, 0, "LPOP")?;
                let count = if args.len() >= 2 {
                    Some(
                        str_arg(args, 1, "LPOP")?
                            .parse::<i64>()
                            .map_err(|_| "LPOP count must be integer".to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::LPop { key, count })
            }
            "RPOP" => {
                let key = bytes_arg(args, 0, "RPOP")?;
                let count = if args.len() >= 2 {
                    Some(
                        str_arg(args, 1, "RPOP")?
                            .parse::<i64>()
                            .map_err(|_| "RPOP count must be integer".to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::RPop { key, count })
            }
            "LLEN" => Ok(Command::LLen {
                key: bytes_arg(args, 0, "LLEN")?,
            }),
            "LRANGE" => {
                let key = bytes_arg(args, 0, "LRANGE")?;
                let start: i64 = str_arg(args, 1, "LRANGE")?
                    .parse()
                    .map_err(|_| "LRANGE start must be integer".to_string())?;
                let stop: i64 = str_arg(args, 2, "LRANGE")?
                    .parse()
                    .map_err(|_| "LRANGE stop must be integer".to_string())?;
                Ok(Command::LRange { key, start, stop })
            }
            "LINDEX" => {
                let key = bytes_arg(args, 0, "LINDEX")?;
                let index: i64 = str_arg(args, 1, "LINDEX")?
                    .parse()
                    .map_err(|_| "LINDEX index must be integer".to_string())?;
                Ok(Command::LIndex { key, index })
            }
            "LSET" => {
                let key = bytes_arg(args, 0, "LSET")?;
                let index: i64 = str_arg(args, 1, "LSET")?
                    .parse()
                    .map_err(|_| "LSET index must be integer".to_string())?;
                let value = bytes_arg(args, 2, "LSET")?;
                Ok(Command::LSet { key, index, value })
            }
            "LINSERT" => {
                let key = bytes_arg(args, 0, "LINSERT")?;
                let dir = str_arg(args, 1, "LINSERT")?.to_uppercase();
                let before = match dir.as_str() {
                    "BEFORE" => true,
                    "AFTER" => false,
                    _ => return Err("LINSERT direction must be BEFORE or AFTER".to_string()),
                };
                let pivot = bytes_arg(args, 2, "LINSERT")?;
                let value = bytes_arg(args, 3, "LINSERT")?;
                Ok(Command::LInsert {
                    key,
                    before,
                    pivot,
                    value,
                })
            }
            "LREM" => {
                let key = bytes_arg(args, 0, "LREM")?;
                let count: i64 = str_arg(args, 1, "LREM")?
                    .parse()
                    .map_err(|_| "LREM count must be integer".to_string())?;
                let value = bytes_arg(args, 2, "LREM")?;
                Ok(Command::LRem { key, count, value })
            }
            "LMOVE" => {
                let src = bytes_arg(args, 0, "LMOVE")?;
                let dst = bytes_arg(args, 1, "LMOVE")?;
                let src_left = match str_arg(args, 2, "LMOVE")?.to_uppercase().as_str() {
                    "LEFT" => true,
                    "RIGHT" => false,
                    _ => return Err("LMOVE src direction must be LEFT or RIGHT".to_string()),
                };
                let dst_left = match str_arg(args, 3, "LMOVE")?.to_uppercase().as_str() {
                    "LEFT" => true,
                    "RIGHT" => false,
                    _ => return Err("LMOVE dst direction must be LEFT or RIGHT".to_string()),
                };
                Ok(Command::LMove {
                    src,
                    dst,
                    src_left,
                    dst_left,
                })
            }
            "LPOS" => {
                let key = bytes_arg(args, 0, "LPOS")?;
                let element = bytes_arg(args, 1, "LPOS")?;
                let mut rank: Option<i64> = None;
                let mut count: Option<i64> = None;
                let mut i = 2;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "RANK" => {
                            rank = Some(
                                str_arg(args, i + 1, "LPOS RANK")?
                                    .parse()
                                    .map_err(|_| "LPOS RANK must be integer".to_string())?,
                            );
                            i += 2;
                        }
                        "COUNT" => {
                            count = Some(
                                str_arg(args, i + 1, "LPOS COUNT")?
                                    .parse()
                                    .map_err(|_| "LPOS COUNT must be integer".to_string())?,
                            );
                            i += 2;
                        }
                        _ => i += 1,
                    }
                }
                if matches!(rank, Some(0)) {
                    return Err("LPOS RANK can't be zero".to_string());
                }
                Ok(Command::LPos {
                    key,
                    element,
                    rank,
                    count,
                })
            }
            "LTRIM" => {
                let key = bytes_arg(args, 0, "LTRIM")?;
                let start: i64 = str_arg(args, 1, "LTRIM")?
                    .parse()
                    .map_err(|_| "LTRIM start must be integer".to_string())?;
                let stop: i64 = str_arg(args, 2, "LTRIM")?
                    .parse()
                    .map_err(|_| "LTRIM stop must be integer".to_string())?;
                Ok(Command::LTrim { key, start, stop })
            }

            "SADD" => {
                if args.len() < 2 {
                    return Err("SADD requires key member [member ...]".to_string());
                }
                let key = bytes_arg(args, 0, "SADD")?;
                let members = (1..args.len())
                    .map(|i| bytes_arg(args, i, "SADD"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SAdd { key, members })
            }
            "SREM" => {
                if args.len() < 2 {
                    return Err("SREM requires key member [member ...]".to_string());
                }
                let key = bytes_arg(args, 0, "SREM")?;
                let members = (1..args.len())
                    .map(|i| bytes_arg(args, i, "SREM"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SRem { key, members })
            }
            "SMEMBERS" => Ok(Command::SMembers {
                key: bytes_arg(args, 0, "SMEMBERS")?,
            }),
            "SCARD" => Ok(Command::SCard {
                key: bytes_arg(args, 0, "SCARD")?,
            }),
            "SISMEMBER" => Ok(Command::SIsMember {
                key: bytes_arg(args, 0, "SISMEMBER")?,
                member: bytes_arg(args, 1, "SISMEMBER")?,
            }),
            "SMISMEMBER" => {
                if args.len() < 2 {
                    return Err("SMISMEMBER requires key member [member ...]".to_string());
                }
                let key = bytes_arg(args, 0, "SMISMEMBER")?;
                let members = (1..args.len())
                    .map(|i| bytes_arg(args, i, "SMISMEMBER"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SMisMember { key, members })
            }
            "SPOP" => {
                let key = bytes_arg(args, 0, "SPOP")?;
                let count = if args.len() >= 2 {
                    Some(
                        str_arg(args, 1, "SPOP")?
                            .parse::<i64>()
                            .map_err(|_| "SPOP count must be integer".to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::SPop { key, count })
            }
            "SRANDMEMBER" => {
                let key = bytes_arg(args, 0, "SRANDMEMBER")?;
                let count = if args.len() >= 2 {
                    Some(
                        str_arg(args, 1, "SRANDMEMBER")?
                            .parse::<i64>()
                            .map_err(|_| "SRANDMEMBER count must be integer".to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::SRandMember { key, count })
            }
            "SUNION" => {
                if args.is_empty() {
                    return Err("SUNION requires at least one key".to_string());
                }
                let keys = (0..args.len())
                    .map(|i| bytes_arg(args, i, "SUNION"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SUnion { keys })
            }
            "SINTER" => {
                if args.is_empty() {
                    return Err("SINTER requires at least one key".to_string());
                }
                let keys = (0..args.len())
                    .map(|i| bytes_arg(args, i, "SINTER"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SInter { keys })
            }
            "SDIFF" => {
                if args.is_empty() {
                    return Err("SDIFF requires at least one key".to_string());
                }
                let keys = (0..args.len())
                    .map(|i| bytes_arg(args, i, "SDIFF"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SDiff { keys })
            }
            "SUNIONSTORE" => {
                if args.len() < 2 {
                    return Err("SUNIONSTORE requires dst and at least one source key".to_string());
                }
                let dst = bytes_arg(args, 0, "SUNIONSTORE")?;
                let keys = (1..args.len())
                    .map(|i| bytes_arg(args, i, "SUNIONSTORE"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SUnionStore { dst, keys })
            }
            "SINTERSTORE" => {
                if args.len() < 2 {
                    return Err("SINTERSTORE requires dst and at least one source key".to_string());
                }
                let dst = bytes_arg(args, 0, "SINTERSTORE")?;
                let keys = (1..args.len())
                    .map(|i| bytes_arg(args, i, "SINTERSTORE"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SInterStore { dst, keys })
            }
            "SDIFFSTORE" => {
                if args.len() < 2 {
                    return Err("SDIFFSTORE requires dst and at least one source key".to_string());
                }
                let dst = bytes_arg(args, 0, "SDIFFSTORE")?;
                let keys = (1..args.len())
                    .map(|i| bytes_arg(args, i, "SDIFFSTORE"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::SDiffStore { dst, keys })
            }
            "SMOVE" => Ok(Command::SMove {
                src: bytes_arg(args, 0, "SMOVE")?,
                dst: bytes_arg(args, 1, "SMOVE")?,
                member: bytes_arg(args, 2, "SMOVE")?,
            }),

            "ZADD" => parse_zadd(args),
            "ZREM" => {
                if args.len() < 2 {
                    return Err("ZREM requires key member [member ...]".to_string());
                }
                let key = bytes_arg(args, 0, "ZREM")?;
                let members = (1..args.len())
                    .map(|i| bytes_arg(args, i, "ZREM"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::ZRem { key, members })
            }
            "ZSCORE" => Ok(Command::ZScore {
                key: bytes_arg(args, 0, "ZSCORE")?,
                member: bytes_arg(args, 1, "ZSCORE")?,
            }),
            "ZMSCORE" => {
                if args.len() < 2 {
                    return Err("ZMSCORE requires key member [member ...]".to_string());
                }
                let key = bytes_arg(args, 0, "ZMSCORE")?;
                let members = (1..args.len())
                    .map(|i| bytes_arg(args, i, "ZMSCORE"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::ZMScore { key, members })
            }
            "ZINCRBY" => {
                let key = bytes_arg(args, 0, "ZINCRBY")?;
                let increment = parse_score_value(&bytes_arg(args, 1, "ZINCRBY")?)
                    .ok_or_else(|| "ZINCRBY increment is not a valid float".to_string())?;
                let member = bytes_arg(args, 2, "ZINCRBY")?;
                Ok(Command::ZIncrBy {
                    key,
                    increment,
                    member,
                })
            }
            "ZCARD" => Ok(Command::ZCard {
                key: bytes_arg(args, 0, "ZCARD")?,
            }),
            "ZCOUNT" => {
                let key = bytes_arg(args, 0, "ZCOUNT")?;
                let min = parse_score_bound(&bytes_arg(args, 1, "ZCOUNT")?)
                    .ok_or_else(|| "ZCOUNT min is not a valid float".to_string())?;
                let max = parse_score_bound(&bytes_arg(args, 2, "ZCOUNT")?)
                    .ok_or_else(|| "ZCOUNT max is not a valid float".to_string())?;
                Ok(Command::ZCount { key, min, max })
            }
            "ZLEXCOUNT" => {
                let key = bytes_arg(args, 0, "ZLEXCOUNT")?;
                let min = parse_lex_bound(&bytes_arg(args, 1, "ZLEXCOUNT")?)
                    .ok_or_else(|| "ZLEXCOUNT min is invalid".to_string())?;
                let max = parse_lex_bound(&bytes_arg(args, 2, "ZLEXCOUNT")?)
                    .ok_or_else(|| "ZLEXCOUNT max is invalid".to_string())?;
                Ok(Command::ZLexCount { key, min, max })
            }
            "ZRANK" => parse_zrank(args, false),
            "ZREVRANK" => parse_zrank(args, true),
            "ZRANGE" => parse_zrange(args, false),
            "ZREVRANGE" => {
                let mut cmd = parse_zrange(args, true)?;
                if let Command::ZRange { rev, .. } = &mut cmd {
                    *rev = true;
                }
                Ok(cmd)
            }
            "ZRANGEBYSCORE" => parse_zrangebyscore(args, false),
            "ZREVRANGEBYSCORE" => parse_zrangebyscore(args, true),
            "ZRANGEBYLEX" => parse_zrangebylex(args, false),
            "ZREVRANGEBYLEX" => parse_zrangebylex(args, true),
            "ZPOPMIN" => {
                let key = bytes_arg(args, 0, "ZPOPMIN")?;
                let count = if args.len() >= 2 {
                    Some(
                        str_arg(args, 1, "ZPOPMIN")?
                            .parse::<i64>()
                            .map_err(|_| "ZPOPMIN count must be integer".to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::ZPopMin { key, count })
            }
            "ZPOPMAX" => {
                let key = bytes_arg(args, 0, "ZPOPMAX")?;
                let count = if args.len() >= 2 {
                    Some(
                        str_arg(args, 1, "ZPOPMAX")?
                            .parse::<i64>()
                            .map_err(|_| "ZPOPMAX count must be integer".to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::ZPopMax { key, count })
            }
            "ZRANDMEMBER" => {
                let key = bytes_arg(args, 0, "ZRANDMEMBER")?;
                let (count, with_scores) = if args.len() >= 2 {
                    let n = str_arg(args, 1, "ZRANDMEMBER")?
                        .parse::<i64>()
                        .map_err(|_| "ZRANDMEMBER count must be integer".to_string())?;
                    let ws = args
                        .get(2)
                        .map(|a| String::from_utf8_lossy(a).to_uppercase() == "WITHSCORES")
                        .unwrap_or(false);
                    (Some(n), ws)
                } else {
                    (None, false)
                };
                Ok(Command::ZRandMember {
                    key,
                    count,
                    with_scores,
                })
            }
            "ZREMRANGEBYRANK" => {
                let key = bytes_arg(args, 0, "ZREMRANGEBYRANK")?;
                let start: i64 = str_arg(args, 1, "ZREMRANGEBYRANK")?
                    .parse()
                    .map_err(|_| "ZREMRANGEBYRANK start must be integer".to_string())?;
                let stop: i64 = str_arg(args, 2, "ZREMRANGEBYRANK")?
                    .parse()
                    .map_err(|_| "ZREMRANGEBYRANK stop must be integer".to_string())?;
                Ok(Command::ZRemRangeByRank { key, start, stop })
            }
            "ZREMRANGEBYSCORE" => {
                let key = bytes_arg(args, 0, "ZREMRANGEBYSCORE")?;
                let min = parse_score_bound(&bytes_arg(args, 1, "ZREMRANGEBYSCORE")?)
                    .ok_or_else(|| "ZREMRANGEBYSCORE min is not a valid float".to_string())?;
                let max = parse_score_bound(&bytes_arg(args, 2, "ZREMRANGEBYSCORE")?)
                    .ok_or_else(|| "ZREMRANGEBYSCORE max is not a valid float".to_string())?;
                Ok(Command::ZRemRangeByScore { key, min, max })
            }
            "ZREMRANGEBYLEX" => {
                let key = bytes_arg(args, 0, "ZREMRANGEBYLEX")?;
                let min = parse_lex_bound(&bytes_arg(args, 1, "ZREMRANGEBYLEX")?)
                    .ok_or_else(|| "ZREMRANGEBYLEX min is invalid".to_string())?;
                let max = parse_lex_bound(&bytes_arg(args, 2, "ZREMRANGEBYLEX")?)
                    .ok_or_else(|| "ZREMRANGEBYLEX max is invalid".to_string())?;
                Ok(Command::ZRemRangeByLex { key, min, max })
            }
            "ZUNION" => parse_zaggregate(args, false, false),
            "ZINTER" => parse_zaggregate(args, true, false),
            "ZDIFF" => parse_zdiff(args, false),
            "ZUNIONSTORE" => parse_zaggregate_store(args, false),
            "ZINTERSTORE" => parse_zaggregate_store(args, true),
            "ZDIFFSTORE" => {
                if args.len() < 3 {
                    return Err("ZDIFFSTORE requires destination numkeys key [key ...]".to_string());
                }
                let dst = bytes_arg(args, 0, "ZDIFFSTORE")?;
                let numkeys: usize = str_arg(args, 1, "ZDIFFSTORE")?
                    .parse()
                    .map_err(|_| "ZDIFFSTORE numkeys must be integer".to_string())?;
                if numkeys == 0 {
                    return Err("ZDIFFSTORE numkeys must be positive".to_string());
                }
                if args.len() < 2 + numkeys {
                    return Err("ZDIFFSTORE numkeys exceeds provided keys".to_string());
                }
                let keys = (0..numkeys)
                    .map(|i| bytes_arg(args, 2 + i, "ZDIFFSTORE"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Command::ZDiffStore { dst, keys })
            }

            "SUBSCRIBE" => {
                if args.is_empty() {
                    return Err("SUBSCRIBE requires at least one channel".to_string());
                }
                Ok(Command::Subscribe {
                    channels: args.to_vec(),
                })
            }
            "UNSUBSCRIBE" => Ok(Command::Unsubscribe),
            "PSUBSCRIBE" => {
                if args.is_empty() {
                    return Err("PSUBSCRIBE requires at least one pattern".to_string());
                }
                Ok(Command::PSubscribe {
                    patterns: args.to_vec(),
                })
            }
            "PUNSUBSCRIBE" => Ok(Command::PUnsubscribe),
            "PUBLISH" => {
                if args.len() != 2 {
                    return Err("PUBLISH requires channel and message".to_string());
                }
                Ok(Command::Publish {
                    channel: args[0].clone(),
                    message: args[1].clone(),
                })
            }
            "PUBSUB" => {
                let sub = args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("CHANNELS") => Ok(Command::PubSubChannels {
                        pattern: args.get(1).cloned(),
                    }),
                    Some("NUMSUB") => Ok(Command::PubSubNumSub {
                        channels: args[1..].to_vec(),
                    }),
                    Some("NUMPAT") => Ok(Command::PubSubNumPat),
                    Some("HELP") | None => Ok(Command::PubSubHelp),
                    Some(s) => Err(format!("PUBSUB subcommand '{}' not supported", s)),
                }
            }

            "MULTI" => Ok(Command::Multi),
            "EXEC" => Ok(Command::Exec),
            "DISCARD" => Ok(Command::Discard),
            "WATCH" => {
                if args.is_empty() {
                    return Err("WATCH requires at least one key".to_string());
                }
                Ok(Command::Watch {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "UNWATCH" => Ok(Command::Unwatch),

            _ => Err(format!("unknown command '{}'", cmd)),
        }
    }

    pub fn execute(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
        match self {
            Command::Ping { msg } => Response::Pong(msg.clone()),
            Command::Echo { msg } => Response::BulkString(msg.clone()),
            Command::Select { .. } => Response::Ok,
            Command::Auth { .. } => Response::Ok,
            Command::Info => Response::BulkString(
                format!(
                    "# Server\r\nredis_version:7.0.0\r\nmode:standalone\r\nos:PostgreSQL\r\ntable_mode:{}\r\n",
                    if is_ephemeral(db) { "unlogged" } else { "logged" }
                )
                .into_bytes(),
            ),
            Command::Hello { .. } => Response::Ok,
            Command::Reset => Response::Ok,
            Command::Quit => Response::Ok,

            // CLIENT commands
            Command::ClientId => Response::Integer(0),
            Command::ClientGetname => Response::Null,
            Command::ClientSetname { .. } => Response::Ok,
            Command::ClientSetinfo => Response::Ok,
            Command::ClientList => Response::BulkString(
                b"id=0 addr=127.0.0.1:0 name= db=0 cmd=client\n".to_vec(),
            ),
            Command::ClientInfo => Response::BulkString(
                b"id=0 addr=127.0.0.1:0 name= db=0 cmd=client\n".to_vec(),
            ),
            Command::ClientNoEvict => Response::Ok,
            Command::ClientNoTouch => Response::Ok,
            Command::ClientOther => Response::Ok,

            // COMMAND commands
            Command::CmdCount => Response::Integer(100),
            Command::CmdInfo => Response::Array(vec![]),
            Command::CmdDocs => Response::Array(vec![]),
            Command::CmdList => Response::Array(vec![]),
            Command::CmdOther => Response::Array(vec![]),

            // CONFIG commands
            Command::ConfigGet { .. } => Response::Array(vec![]),
            Command::ConfigSet => Response::Ok,
            Command::ConfigOther => Response::Ok,

            Command::Get { key } => {
                match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: GET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Set {
                key,
                value,
                ex_ms,
                nx,
                xx,
                get,
                keepttl,
            } => {
                // Expiry expression for the inserted/updated row.
                // $3 is ex_ms (Option<i64>); when NULL, no expiry.
                let new_expires =
                    "CASE WHEN $3::bigint IS NULL THEN NULL \
                     ELSE now() + ($3::bigint * interval '1 millisecond') END";
                // CASE for ON CONFLICT update: keep existing expires_at when KEEPTTL.
                let conflict_expires = if *keepttl {
                    format!("redis.kv_{db}.expires_at")
                } else {
                    "EXCLUDED.expires_at".to_string()
                };

                // Build write CTE depending on NX/XX semantics.
                let write_cte = if *xx {
                    // Update only when key exists and is not expired.
                    format!(
                        "wrote AS (UPDATE redis.kv_{db} \
                         SET value = $2, \
                             expires_at = CASE WHEN $4::bool THEN redis.kv_{db}.expires_at \
                                          ELSE {new_expires} END \
                         WHERE key = $1 \
                           AND (expires_at IS NULL OR expires_at > now()) \
                         RETURNING 1)"
                    )
                } else if *nx {
                    // Insert only when no live key exists. Treat expired rows as absent
                    // by deleting them inline first via a sub-CTE.
                    format!(
                        "purge AS (DELETE FROM redis.kv_{db} \
                                   WHERE key = $1 AND expires_at IS NOT NULL \
                                     AND expires_at <= now() RETURNING 1), \
                         wrote AS (INSERT INTO redis.kv_{db} (key, value, expires_at) \
                                   SELECT $1, $2, {new_expires} \
                                   WHERE NOT EXISTS (SELECT 1 FROM redis.kv_{db} \
                                                     WHERE key = $1) \
                                   RETURNING 1)"
                    )
                } else {
                    // Default: unconditional UPSERT (KEEPTTL respected).
                    format!(
                        "wrote AS (INSERT INTO redis.kv_{db} (key, value, expires_at) \
                                   VALUES ($1, $2, {new_expires}) \
                                   ON CONFLICT (key) DO UPDATE \
                                   SET value = EXCLUDED.value, \
                                       expires_at = {conflict_expires} \
                                   RETURNING 1)"
                    )
                };

                let (old_cte, old_select) = if *get {
                    (
                        format!(
                            "old AS ( \
                                 SELECT value FROM redis.kv_{db} \
                                 WHERE key = $1 \
                                   AND (expires_at IS NULL OR expires_at > now()) \
                             ), "
                        ),
                        "(SELECT value FROM old) AS old_value,".to_string(),
                    )
                } else {
                    (String::new(), "NULL AS old_value,".to_string())
                };
                let sql = format!(
                    "WITH {old_cte}{write_cte} \
                     SELECT {old_select} \
                            EXISTS (SELECT 1 FROM wrote) AS wrote"
                );

                let args: &[DatumWithOid] = &[
                    key.as_slice().into(),
                    value.as_slice().into(),
                    (*ex_ms).into(),
                    (*keepttl).into(),
                ];

                match client.update(&sql, None, args) {
                    Ok(tbl) => {
                        let row = tbl.first();
                        let old_value = row.get::<Vec<u8>>(1).ok().flatten();
                        let wrote = row.get::<bool>(2).ok().flatten().unwrap_or(false);
                        if *get {
                            match old_value {
                                Some(v) => Response::BulkString(v),
                                None => Response::Null,
                            }
                        } else if *nx || *xx {
                            if wrote {
                                Response::Ok
                            } else {
                                Response::Null
                            }
                        } else {
                            Response::Ok
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SetEx { key, value, ex_secs } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     VALUES ($1, $2, now() + ($3::bigint * interval '1 second')) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let args: &[DatumWithOid] =
                    &[key.as_slice().into(), value.as_slice().into(), (*ex_secs).into()];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: SETEX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::PSetEx { key, value, ex_ms } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     VALUES ($1, $2, now() + ($3::bigint * interval '1 millisecond')) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let args: &[DatumWithOid] =
                    &[key.as_slice().into(), value.as_slice().into(), (*ex_ms).into()];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: PSETEX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::MGet { keys } => {
                let keys_vec: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.select(&sql::MGET[db as usize], None, &[keys_vec.into()]) {
                    Ok(tbl) => {
                        let mut map = std::collections::HashMap::new();
                        for row in tbl {
                            if let (Ok(Some(k)), Ok(Some(v))) =
                                (row.get::<Vec<u8>>(1), row.get::<Vec<u8>>(2))
                            {
                                map.insert(k, v);
                            }
                        }
                        let result = keys
                            .iter()
                            .map(|k| map.remove(k))
                            .collect();
                        Response::Array(result)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: MGET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::MSet { pairs } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     SELECT unnest($1::bytea[]), unnest($2::bytea[]), NULL \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let keys: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(k, _)| Some(k.clone())).collect();
                let vals: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(&sql, None, &[keys.into(), vals.into()]) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: MSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Del { keys } => {
                let sql = format!(
                    "WITH d1 AS (DELETE FROM redis.kv_{db}   WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d2 AS (DELETE FROM redis.hash_{db} WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d3 AS (DELETE FROM redis.list_{db} WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d4 AS (DELETE FROM redis.set_{db}  WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d5 AS (DELETE FROM redis.zset_{db} WHERE key = ANY($1::bytea[]) RETURNING key) \
                     SELECT count(DISTINCT key)::bigint FROM ( \
                         SELECT key FROM d1 UNION ALL \
                         SELECT key FROM d2 UNION ALL \
                         SELECT key FROM d3 UNION ALL \
                         SELECT key FROM d4 UNION ALL \
                         SELECT key FROM d5 \
                     ) u"
                );
                let keys_vec: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.update(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: DEL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Exists { keys } => {
                let sql = format!(
                    "SELECT count(DISTINCT key)::bigint FROM ( \
                         SELECT key FROM redis.kv_{db} \
                         WHERE key = ANY($1::bytea[]) \
                           AND (expires_at IS NULL OR expires_at > now()) \
                         UNION ALL \
                         SELECT key FROM redis.hash_{db} WHERE key = ANY($1::bytea[]) \
                         UNION ALL \
                         SELECT key FROM redis.list_{db} WHERE key = ANY($1::bytea[]) \
                         UNION ALL \
                         SELECT key FROM redis.set_{db}  WHERE key = ANY($1::bytea[]) \
                         UNION ALL \
                         SELECT key FROM redis.zset_{db} WHERE key = ANY($1::bytea[]) \
                     ) u"
                );
                let keys_vec: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.select(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: EXISTS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Expire { key, secs } => update_expiry(
                client,
                &format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = now() + ($2::bigint * interval '1 second') \
                     WHERE key = $1"
                ),
                key,
                *secs,
            ),

            Command::PExpire { key, ms } => update_expiry(
                client,
                &format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = now() + ($2::bigint * interval '1 millisecond') \
                     WHERE key = $1"
                ),
                key,
                *ms,
            ),

            Command::ExpireAt { key, unix_secs } => update_expiry(
                client,
                &format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = to_timestamp($2::bigint) WHERE key = $1"
                ),
                key,
                *unix_secs,
            ),

            Command::PExpireAt { key, unix_ms } => {
                let sql = format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = to_timestamp($2::float8 / 1000.0) WHERE key = $1"
                );
                let ms_f = *unix_ms as f64;
                match client.update(&sql, None, &[key.as_slice().into(), ms_f.into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: PEXPIREAT error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            // TTL variants use a single LEFT JOIN instead of three correlated subqueries,
            // reducing index scans from 3 to 1 per call.
            Command::Ttl { key } => get_ttl(
                client,
                &sql::TTL[db as usize],
                key,
            ),

            Command::PTtl { key } => get_ttl(
                client,
                &sql::PTTL[db as usize],
                key,
            ),

            Command::Persist { key } => {
                match client.update(&sql::PERSIST[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: PERSIST error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::ExpireTime { key } => get_ttl(
                client,
                &sql::EXPIRETIME[db as usize],
                key,
            ),

            Command::PExpireTime { key } => get_ttl(
                client,
                &sql::PEXPIRETIME[db as usize],
                key,
            ),

            Command::Incr { key } => {
                match client.update(&sql::INCR[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: INCR error: {}", e);
                        Response::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }

            Command::Decr { key } => {
                match client.update(&sql::DECR[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: DECR error: {}", e);
                        Response::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }

            Command::IncrBy { key, delta } => {
                match client.update(&sql::INCRBY[db as usize], None, &[key.as_slice().into(), (*delta).into(), (*delta).into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: INCRBY error: {}", e);
                        Response::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }

            Command::DecrBy { key, delta } => {
                match client.update(&sql::DECRBY[db as usize], None, &[key.as_slice().into(), (*delta).into(), (*delta).into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: DECRBY error: {}", e);
                        Response::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }

            Command::IncrByFloat { key, delta } => {
                match client.update(&sql::INCRBYFLOAT[db as usize], None, &[key.as_slice().into(), (*delta).into(), (*delta).into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Error("ERR value is not a valid float".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: INCRBYFLOAT error: {}", e);
                        Response::Error("ERR value is not a valid float".to_string())
                    }
                }
            }

            Command::Append { key, value } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = redis.kv_{db}.value || $2 \
                     RETURNING length(redis.kv_{db}.value)"
                );
                match client.update(&sql, None, &[key.as_slice().into(), value.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(value.len() as i64),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: APPEND error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Strlen { key } => {
                match client.select(&sql::STRLEN[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: STRLEN error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::GetDel { key } => {
                match client.update(&sql::GETDEL[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: GETDEL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::GetSet { key, value } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value \
                     RETURNING (SELECT value FROM redis.kv_{db} WHERE key = $1)"
                );
                match client.update(&sql, None, &[key.as_slice().into(), value.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: GETSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SetNx { key, value } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO NOTHING"
                );
                match client.update(&sql, None, &[key.as_slice().into(), value.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: SETNX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::MSetNx { pairs } => {
                let keys: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(k, _)| Some(k.clone())).collect();
                let check_sql = format!(
                    "SELECT count(*) FROM redis.kv_{db} \
                     WHERE key = ANY($1::bytea[]) \
                     AND (expires_at IS NULL OR expires_at > now())"
                );
                let existing = client.select(&check_sql, None, &[keys.clone().into()]);
                let any_exist = match existing {
                    Ok(tbl) => matches!(tbl.first().get::<i64>(1), Ok(Some(n)) if n > 0),
                    Err(_) => false,
                };
                if any_exist {
                    return Response::Integer(0);
                }
                let vals: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                let insert_sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     SELECT unnest($1::bytea[]), unnest($2::bytea[]), NULL \
                     ON CONFLICT (key) DO NOTHING"
                );
                match client.update(&insert_sql, None, &[keys.into(), vals.into()]) {
                    Ok(_) => Response::Integer(1),
                    Err(e) => {
                        eprintln!("pg_redis: MSETNX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Type { key } => {
                let sql = format!(
                    "SELECT 'string' WHERE EXISTS ( \
                         SELECT 1 FROM redis.kv_{db} \
                         WHERE key = $1 AND (expires_at IS NULL OR expires_at > now()) \
                     ) \
                     UNION ALL SELECT 'list'   WHERE EXISTS (SELECT 1 FROM redis.list_{db} WHERE key = $1) \
                     UNION ALL SELECT 'set'    WHERE EXISTS (SELECT 1 FROM redis.set_{db}  WHERE key = $1) \
                     UNION ALL SELECT 'hash'   WHERE EXISTS (SELECT 1 FROM redis.hash_{db} WHERE key = $1) \
                     UNION ALL SELECT 'zset'   WHERE EXISTS (SELECT 1 FROM redis.zset_{db} WHERE key = $1) \
                     LIMIT 1"
                );
                // The column is a SQL text literal ('string', 'list', ...), not
                // a stored key or value, so it stayed TEXT through the bytea
                // migration. Reading it as Vec<u8> fails and every type reports
                // as "none".
                match client.select(&sql, None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(t)) => Response::SimpleString(t),
                        _ => Response::SimpleString("none".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: TYPE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Keys { pattern } => {
                Response::Array(matching_keys(client, db, Some(pattern)))
            }

            Command::DbSize => {
                let sql = format!(
                    "SELECT count(DISTINCT key)::bigint FROM ( \
                         SELECT key FROM redis.kv_{db} \
                         WHERE (expires_at IS NULL OR expires_at > now()) \
                         UNION ALL \
                         SELECT DISTINCT key FROM redis.hash_{db} \
                         UNION ALL \
                         SELECT DISTINCT key FROM redis.list_{db} \
                         UNION ALL \
                         SELECT DISTINCT key FROM redis.set_{db} \
                         UNION ALL \
                         SELECT DISTINCT key FROM redis.zset_{db} \
                     ) u"
                );
                match client.select(&sql, None, &[]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: DBSIZE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Unlink { keys } => {
                let sql = format!(
                    "WITH d1 AS (DELETE FROM redis.kv_{db}   WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d2 AS (DELETE FROM redis.hash_{db} WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d3 AS (DELETE FROM redis.list_{db} WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d4 AS (DELETE FROM redis.set_{db}  WHERE key = ANY($1::bytea[]) RETURNING key), \
                          d5 AS (DELETE FROM redis.zset_{db} WHERE key = ANY($1::bytea[]) RETURNING key) \
                     SELECT count(DISTINCT key)::bigint FROM ( \
                         SELECT key FROM d1 UNION ALL \
                         SELECT key FROM d2 UNION ALL \
                         SELECT key FROM d3 UNION ALL \
                         SELECT key FROM d4 UNION ALL \
                         SELECT key FROM d5 \
                     ) u"
                );
                let keys_vec: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.update(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: UNLINK error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Rename { key, newkey } => {
                let sql = format!(
                    "WITH r AS ( \
                         UPDATE redis.kv_{db} SET key = $2 WHERE key = $1 RETURNING 1 \
                     ) \
                     SELECT (SELECT count(*) FROM r) > 0 AS renamed"
                );
                match client.update(&sql, None, &[key.as_slice().into(), newkey.as_slice().into()]) {
                    Ok(tbl) => {
                        let renamed = tbl.first().get::<bool>(1).ok().flatten().unwrap_or(false);
                        if renamed {
                            Response::Ok
                        } else {
                            Response::Error("ERR no such key".to_string())
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: RENAME error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::RandomKey => {
                let sql = format!(
                    "SELECT key FROM redis.kv_{db} \
                     WHERE (expires_at IS NULL OR expires_at > now()) \
                     ORDER BY random() LIMIT 1"
                );
                match client.select(&sql, None, &[]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(k)) => Response::BulkString(k),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: RANDOMKEY error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Scan { pattern, .. } => Response::ScanResult {
                keys: matching_keys(client, db, pattern.as_deref()),
            },

            Command::HGet { key, field } => {
                match client.select(&sql::HGET[db as usize], None, &[key.as_slice().into(), field.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: HGET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HSet { key, pairs } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     SELECT $1, unnest($2::bytea[]), unnest($3::bytea[]) \
                     ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
                );
                let fields: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(f, _)| Some(f.clone())).collect();
                let vals: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), fields.into(), vals.into()],
                ) {
                    Ok(tbl) => Response::Integer(tbl.len() as i64),
                    Err(e) => {
                        eprintln!("pg_redis: HSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HDel { key, fields } => {
                let sql = format!(
                    "DELETE FROM redis.hash_{db} \
                     WHERE key = $1 AND field = ANY($2::bytea[])"
                );
                let fields_vec: Vec<Option<Vec<u8>>> =
                    fields.iter().map(|f| Some(f.clone())).collect();
                match client.update(&sql, None, &[key.as_slice().into(), fields_vec.into()]) {
                    Ok(tbl) => Response::Integer(tbl.len() as i64),
                    Err(e) => {
                        eprintln!("pg_redis: HDEL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HGetAll { key } => {
                match client.select(&sql::HGETALL[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let mut items: Vec<Option<Vec<u8>>> = Vec::new();
                        for row in tbl {
                            if let (Ok(Some(f)), Ok(Some(v))) =
                                (row.get::<Vec<u8>>(1), row.get::<Vec<u8>>(2))
                            {
                                items.push(Some(f));
                                items.push(Some(v));
                            }
                        }
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HGETALL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HMGet { key, fields } => {
                let fields_vec: Vec<Option<Vec<u8>>> =
                    fields.iter().map(|f| Some(f.clone())).collect();
                let sql = format!(
                    "SELECT field, value FROM redis.hash_{db} \
                     WHERE key = $1 AND field = ANY($2::bytea[])"
                );
                match client.select(&sql, None, &[key.as_slice().into(), fields_vec.into()]) {
                    Ok(tbl) => {
                        let mut map = std::collections::HashMap::new();
                        for row in tbl {
                            if let (Ok(Some(f)), Ok(Some(v))) =
                                (row.get::<Vec<u8>>(1), row.get::<Vec<u8>>(2))
                            {
                                map.insert(f, v);
                            }
                        }
                        let result = fields
                            .iter()
                            .map(|f| map.remove(f))
                            .collect();
                        Response::Array(result)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HMGET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HMSet { key, pairs } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     SELECT $1, unnest($2::bytea[]), unnest($3::bytea[]) \
                     ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
                );
                let fields: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(f, _)| Some(f.clone())).collect();
                let vals: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), fields.into(), vals.into()],
                ) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: HMSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HKeys { key } => {
                match client.select(&sql::HKEYS[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .map(Some)
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HKEYS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HVals { key } => {
                match client.select(&sql::HVALS[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .map(Some)
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HVALS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HExists { key, field } => {
                match client.select(&sql::HEXISTS[db as usize], None, &[key.as_slice().into(), field.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: HEXISTS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HLen { key } => {
                match client.select(&sql::HLEN[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: HLEN error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HIncrBy { key, field, delta } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) VALUES ($1, $2, $3::text) \
                     ON CONFLICT (key, field) DO UPDATE \
                     SET value = (CAST(redis.hash_{db}.value AS bigint) + $4)::text \
                     WHERE redis.hash_{db}.value ~ '^-?[0-9]+$' \
                     RETURNING CAST(encode(value, 'escape') AS bigint)"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), field.as_slice().into(), (*delta).into(), (*delta).into()],
                ) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Error("ERR value is not an integer or out of range".to_string()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: HINCRBY error: {}", e);
                        Response::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }

            Command::HSetNx { key, field, value } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) VALUES ($1, $2, $3) \
                     ON CONFLICT (key, field) DO NOTHING"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), field.as_slice().into(), value.as_slice().into()],
                ) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: HSETNX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LPush { key, values } => list_push(client, db, key, values, true, false),
            Command::RPush { key, values } => list_push(client, db, key, values, false, false),
            Command::LPushX { key, values } => list_push(client, db, key, values, true, true),
            Command::RPushX { key, values } => list_push(client, db, key, values, false, true),

            Command::LPop { key, count } => list_pop(client, db, key, *count, true),
            Command::RPop { key, count } => list_pop(client, db, key, *count, false),

            Command::LLen { key } => {
                match client.select(&sql::LLEN[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: LLEN error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LRange { key, start, stop } => {
                let sql = format!(
                    "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.list_{db} WHERE key = $1), \
                     nrm AS ( \
                         SELECT \
                             CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                             LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
                         FROM cnt \
                     ) \
                     SELECT l.value \
                     FROM redis.list_{db} l, nrm \
                     WHERE l.key = $1 \
                       AND (SELECT n FROM cnt) > 0 \
                       AND (SELECT s FROM nrm) <= (SELECT e FROM nrm) \
                     ORDER BY l.pos ASC \
                     OFFSET (SELECT s FROM nrm) \
                     LIMIT (SELECT GREATEST(0, e - s + 1) FROM nrm)"
                );
                match client.select(
                    &sql,
                    None,
                    &[key.as_slice().into(), (*start).into(), (*stop).into()],
                ) {
                    Ok(tbl) => {
                        let mut out: Vec<Option<Vec<u8>>> = Vec::new();
                        for row in tbl {
                            if let Ok(Some(v)) = row.get::<Vec<u8>>(1) {
                                out.push(Some(v));
                            }
                        }
                        Response::Array(out)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: LRANGE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LIndex { key, index } => {
                let sql = format!(
                    "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.list_{db} WHERE key = $1), \
                     idx AS ( \
                         SELECT CASE WHEN $2::bigint < 0 THEN n + $2::bigint ELSE $2::bigint END AS i \
                         FROM cnt \
                     ) \
                     SELECT l.value \
                     FROM redis.list_{db} l, idx \
                     WHERE l.key = $1 \
                       AND (SELECT i FROM idx) >= 0 \
                       AND (SELECT i FROM idx) < (SELECT n FROM cnt) \
                     ORDER BY l.pos ASC \
                     OFFSET (SELECT i FROM idx) \
                     LIMIT 1"
                );
                match client.select(&sql, None, &[key.as_slice().into(), (*index).into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: LINDEX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LSet { key, index, value } => {
                let sql = format!(
                    "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.list_{db} WHERE key = $1), \
                     chk AS ( \
                         SELECT n, CASE WHEN $2::bigint < 0 THEN n + $2::bigint ELSE $2::bigint END AS idx \
                         FROM cnt \
                     ), \
                     target AS ( \
                         SELECT pos FROM redis.list_{db} \
                         WHERE key = $1 \
                         ORDER BY pos ASC \
                         OFFSET GREATEST(0, (SELECT idx FROM chk)) \
                         LIMIT 1 \
                     ), \
                     upd AS ( \
                         UPDATE redis.list_{db} SET value = $3 \
                         WHERE key = $1 \
                           AND pos = (SELECT pos FROM target) \
                           AND (SELECT idx FROM chk) >= 0 \
                           AND (SELECT idx FROM chk) < (SELECT n FROM cnt) \
                         RETURNING 1 \
                     ) \
                     SELECT n, EXISTS(SELECT 1 FROM upd) AS updated FROM chk"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), (*index).into(), value.as_slice().into()],
                ) {
                    Ok(tbl) => {
                        let row = tbl.first();
                        let n = row.get::<i64>(1).ok().flatten().unwrap_or(0);
                        let updated = row.get::<bool>(2).ok().flatten().unwrap_or(false);
                        if n == 0 {
                            Response::Error("ERR no such key".to_string())
                        } else if !updated {
                            Response::Error("ERR index out of range".to_string())
                        } else {
                            Response::Ok
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: LSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LInsert { key, before, pivot, value } => {
                list_insert(client, db, key, *before, pivot, value)
            }

            Command::LRem { key, count, value } => {
                let limit_clause = if *count == 0 {
                    String::new()
                } else {
                    format!("LIMIT {}", count.unsigned_abs())
                };
                let order_dir = if *count < 0 { "DESC" } else { "ASC" };
                let sql = format!(
                    "WITH d AS ( \
                         SELECT pos FROM redis.list_{db} \
                         WHERE key = $1 AND value = $2 \
                         ORDER BY pos {order_dir} {limit_clause} \
                     ), \
                     del AS ( \
                         DELETE FROM redis.list_{db} t USING d \
                         WHERE t.key = $1 AND t.pos = d.pos \
                         RETURNING 1 \
                     ) \
                     SELECT count(*)::bigint FROM del"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), value.as_slice().into()],
                ) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: LREM error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LMove { src, dst, src_left, dst_left } => {
                let pop_order = if *src_left { "ASC" } else { "DESC" };
                let edge_agg = if *dst_left { "MIN" } else { "MAX" };
                let edge_op = if *dst_left { "-" } else { "+" };
                let sql = format!(
                    "WITH popped AS ( \
                         DELETE FROM redis.list_{db} t \
                         USING ( \
                             SELECT key, pos FROM redis.list_{db} \
                             WHERE key = $1 \
                             ORDER BY pos {pop_order} LIMIT 1 \
                             FOR UPDATE SKIP LOCKED \
                         ) p \
                         WHERE t.key = p.key AND t.pos = p.pos \
                         RETURNING t.value \
                     ), \
                     edge AS ( \
                         SELECT COALESCE({edge_agg}(pos), 0) AS p \
                         FROM redis.list_{db} WHERE key = $2 \
                     ), \
                     ins AS ( \
                         INSERT INTO redis.list_{db} (key, pos, value) \
                         SELECT $2, edge.p {edge_op} 1, popped.value \
                         FROM popped, edge \
                         RETURNING value \
                     ) \
                     SELECT value FROM ins"
                );
                match client.update(
                    &sql,
                    None,
                    &[src.as_slice().into(), dst.as_slice().into()],
                ) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: LMOVE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LPos { key, element, rank, count } => {
                let r = rank.unwrap_or(1);
                let order_dir = if r < 0 { "DESC" } else { "ASC" };
                let skip = (r.unsigned_abs() as i64) - 1;
                let limit_clause = match count {
                    None => "LIMIT 1".to_string(),
                    Some(0) => String::new(),
                    Some(n) => format!("LIMIT {}", n),
                };
                let sql = format!(
                    "WITH ordered AS ( \
                         SELECT value, ROW_NUMBER() OVER (ORDER BY pos ASC) - 1 AS idx \
                         FROM redis.list_{db} WHERE key = $1 \
                     ) \
                     SELECT idx FROM ordered WHERE value = $2 \
                     ORDER BY idx {order_dir} OFFSET $3 {limit_clause}"
                );
                match client.select(
                    &sql,
                    None,
                    &[
                        key.as_slice().into(),
                        element.as_slice().into(),
                        skip.into(),
                    ],
                ) {
                    Ok(tbl) => {
                        if count.is_none() {
                            match tbl.first().get::<i64>(1) {
                                Ok(Some(n)) => Response::Integer(n),
                                _ => Response::Null,
                            }
                        } else {
                            let mut out: Vec<i64> = Vec::new();
                            for row in tbl {
                                if let Ok(Some(n)) = row.get::<i64>(1) {
                                    out.push(n);
                                }
                            }
                            Response::IntegerArray(out)
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: LPOS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::LTrim { key, start, stop } => {
                let sql = format!(
                    "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.list_{db} WHERE key = $1), \
                     nrm AS ( \
                         SELECT \
                             CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                             LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
                         FROM cnt \
                     ), \
                     bounds AS ( \
                         SELECT s, e, \
                             (SELECT pos FROM redis.list_{db} WHERE key = $1 ORDER BY pos ASC OFFSET GREATEST(0, s) LIMIT 1) AS lo, \
                             (SELECT pos FROM redis.list_{db} WHERE key = $1 ORDER BY pos ASC OFFSET GREATEST(0, e) LIMIT 1) AS hi \
                         FROM nrm \
                     ) \
                     DELETE FROM redis.list_{db} USING bounds b \
                     WHERE key = $1 \
                       AND (b.s > b.e OR pos < b.lo OR pos > b.hi)"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), (*start).into(), (*stop).into()],
                ) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: LTRIM error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SAdd { key, members } => {
                let sql = format!(
                    "WITH ins AS ( \
                         INSERT INTO redis.set_{db} (key, member) \
                         SELECT $1, unnest($2::bytea[]) \
                         ON CONFLICT DO NOTHING RETURNING 1 \
                     ) \
                     SELECT count(*)::bigint FROM ins"
                );
                let members_vec: Vec<Option<Vec<u8>>> =
                    members.iter().map(|m| Some(m.clone())).collect();
                match client.update(&sql, None, &[key.as_slice().into(), members_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SADD error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SRem { key, members } => {
                let sql = format!(
                    "WITH d AS ( \
                         DELETE FROM redis.set_{db} \
                         WHERE key = $1 AND member = ANY($2::bytea[]) \
                         RETURNING 1 \
                     ) \
                     SELECT count(*)::bigint FROM d"
                );
                let members_vec: Vec<Option<Vec<u8>>> =
                    members.iter().map(|m| Some(m.clone())).collect();
                match client.update(&sql, None, &[key.as_slice().into(), members_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SREM error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SMembers { key } => {
                match client.select(&sql::SMEMBERS[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .map(Some)
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SMEMBERS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SCard { key } => {
                match client.select(&sql::SCARD[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SCARD error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SIsMember { key, member } => {
                match client.select(&sql::SISMEMBER[db as usize], None, &[key.as_slice().into(), member.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: SISMEMBER error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SMisMember { key, members } => {
                let sql = format!(
                    "SELECT m.member, (s.member IS NOT NULL)::int AS present \
                     FROM unnest($2::bytea[]) WITH ORDINALITY AS m(member, ord) \
                     LEFT JOIN redis.set_{db} s \
                         ON s.key = $1 AND s.member = m.member \
                     ORDER BY m.ord"
                );
                let members_vec: Vec<Option<Vec<u8>>> =
                    members.iter().map(|m| Some(m.clone())).collect();
                match client.select(&sql, None, &[key.as_slice().into(), members_vec.into()]) {
                    Ok(tbl) => {
                        let mut out: Vec<i64> = Vec::with_capacity(members.len());
                        for row in tbl {
                            let v = row.get::<i32>(2).ok().flatten().unwrap_or(0);
                            out.push(v as i64);
                        }
                        Response::IntegerArray(out)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SMISMEMBER error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SPop { key, count } => {
                let (want_array, limit) = match count {
                    None => (false, 1i64),
                    Some(n) => (true, (*n).max(0)),
                };
                if want_array && limit == 0 {
                    return Response::Array(vec![]);
                }
                let sql = format!(
                    "WITH picked AS ( \
                         SELECT ctid FROM redis.set_{db} \
                         WHERE key = $1 \
                         ORDER BY random() LIMIT $2 \
                     ) \
                     DELETE FROM redis.set_{db} t USING picked \
                     WHERE t.ctid = picked.ctid \
                     RETURNING t.member"
                );
                match client.update(&sql, None, &[key.as_slice().into(), limit.into()]) {
                    Ok(tbl) => {
                        let members: Vec<Vec<u8>> = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .collect();
                        if want_array {
                            Response::Array(members.into_iter().map(Some).collect())
                        } else {
                            match members.into_iter().next() {
                                Some(m) => Response::BulkString(m),
                                None => Response::Null,
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SPOP error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SRandMember { key, count } => {
                let (want_array, allow_dup, limit) = match count {
                    None => (false, false, 1i64),
                    Some(n) if *n >= 0 => (true, false, *n),
                    Some(n) => (true, true, -*n),
                };
                if want_array && limit == 0 {
                    return Response::Array(vec![]);
                }
                // With negative count, Redis allows duplicates. LATERAL re-
                // evaluates random() per outer row so each pick is independent.
                let sql = if allow_dup {
                    format!(
                        "WITH cnt AS ( \
                             SELECT count(*)::int AS c FROM redis.set_{db} WHERE key = $1 \
                         ) \
                         SELECT p.member \
                         FROM cnt, generate_series(1, $2::int) g, \
                              LATERAL ( \
                                  SELECT member FROM redis.set_{db} \
                                  WHERE key = $1 \
                                  OFFSET floor(random() * cnt.c)::int \
                                  LIMIT 1 \
                              ) p \
                         WHERE cnt.c > 0"
                    )
                } else {
                    format!(
                        "SELECT member FROM redis.set_{db} WHERE key = $1 \
                         ORDER BY random() LIMIT $2"
                    )
                };
                match client.select(&sql, None, &[key.as_slice().into(), limit.into()]) {
                    Ok(tbl) => {
                        let members: Vec<Vec<u8>> = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .collect();
                        if want_array {
                            Response::Array(members.into_iter().map(Some).collect())
                        } else {
                            match members.into_iter().next() {
                                Some(m) => Response::BulkString(m),
                                None => Response::Null,
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SRANDMEMBER error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SUnion { keys } => {
                let keys_vec: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                let sql = format!(
                    "SELECT DISTINCT member FROM redis.set_{db} WHERE key = ANY($1::bytea[])"
                );
                match client.select(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .map(Some)
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SUNION error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SInter { keys } => {
                if keys.is_empty() {
                    return Response::Array(vec![]);
                }
                let sql = build_inter_sql(db, keys.len(), 1);
                let args: Vec<DatumWithOid> =
                    keys.iter().map(|k| k.as_slice().into()).collect();
                match client.select(&sql, None, &args) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .map(Some)
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: SINTER error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SDiff { keys } => {
                if keys.is_empty() {
                    return Response::Array(vec![]);
                }
                let first = &keys[0];
                if keys.len() == 1 {
                    let sql = format!(
                        "SELECT DISTINCT member FROM redis.set_{db} WHERE key = $1"
                    );
                    return match client.select(&sql, None, &[first.as_slice().into()]) {
                        Ok(tbl) => Response::Array(
                            tbl.into_iter()
                                .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                                .map(Some)
                                .collect(),
                        ),
                        Err(e) => {
                            eprintln!("pg_redis: SDIFF error: {}", e);
                            Response::Error("internal error".to_string())
                        }
                    };
                }
                let rest: Vec<Option<Vec<u8>>> =
                    keys[1..].iter().map(|k| Some(k.clone())).collect();
                let sql = format!(
                    "SELECT member FROM redis.set_{db} WHERE key = $1 \
                     EXCEPT \
                     SELECT member FROM redis.set_{db} WHERE key = ANY($2::bytea[])"
                );
                match client.select(&sql, None, &[first.as_slice().into(), rest.into()]) {
                    Ok(tbl) => Response::Array(
                        tbl.into_iter()
                            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
                            .map(Some)
                            .collect(),
                    ),
                    Err(e) => {
                        eprintln!("pg_redis: SDIFF error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SUnionStore { dst, keys } => {
                let keys_vec: Vec<Option<Vec<u8>>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                let sql = format!(
                    "WITH new_data AS ( \
                         SELECT DISTINCT member FROM redis.set_{db} \
                         WHERE key = ANY($2::bytea[]) \
                     ), \
                     del AS (DELETE FROM redis.set_{db} WHERE key = $1 RETURNING 1), \
                     ins AS ( \
                         INSERT INTO redis.set_{db} (key, member) \
                         SELECT $1, member FROM new_data \
                         ON CONFLICT DO NOTHING \
                         RETURNING 1 \
                     ) \
                     SELECT count(*)::bigint FROM ins"
                );
                match client.update(&sql, None, &[dst.as_slice().into(), keys_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SUNIONSTORE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SInterStore { dst, keys } => {
                if keys.is_empty() {
                    return Response::Integer(0);
                }
                let inter_body = build_inter_sql(db, keys.len(), 2);
                let sql = format!(
                    "WITH new_data AS ({inter_body}), \
                          del AS (DELETE FROM redis.set_{db} WHERE key = $1 RETURNING 1), \
                          ins AS ( \
                              INSERT INTO redis.set_{db} (key, member) \
                              SELECT $1, member FROM new_data \
                              ON CONFLICT DO NOTHING \
                              RETURNING 1 \
                          ) \
                     SELECT count(*)::bigint FROM ins"
                );
                let mut args: Vec<DatumWithOid> = Vec::with_capacity(1 + keys.len());
                args.push(dst.as_slice().into());
                for k in keys {
                    args.push(k.as_slice().into());
                }
                match client.update(&sql, None, &args) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SINTERSTORE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SDiffStore { dst, keys } => {
                if keys.is_empty() {
                    return Response::Integer(0);
                }
                let first = &keys[0];
                let (body, rest_arg) = if keys.len() == 1 {
                    (
                        format!(
                            "SELECT DISTINCT member FROM redis.set_{db} WHERE key = $2"
                        ),
                        None,
                    )
                } else {
                    let rest: Vec<Option<Vec<u8>>> =
                        keys[1..].iter().map(|k| Some(k.clone())).collect();
                    (
                        format!(
                            "SELECT member FROM redis.set_{db} WHERE key = $2 \
                             EXCEPT \
                             SELECT member FROM redis.set_{db} WHERE key = ANY($3::bytea[])"
                        ),
                        Some(rest),
                    )
                };
                let sql = format!(
                    "WITH new_data AS ({body}), \
                          del AS (DELETE FROM redis.set_{db} WHERE key = $1 RETURNING 1), \
                          ins AS ( \
                              INSERT INTO redis.set_{db} (key, member) \
                              SELECT $1, member FROM new_data \
                              ON CONFLICT DO NOTHING \
                              RETURNING 1 \
                          ) \
                     SELECT count(*)::bigint FROM ins"
                );
                let result = match rest_arg {
                    None => client.update(
                        &sql,
                        None,
                        &[dst.as_slice().into(), first.as_slice().into()],
                    ),
                    Some(rest) => client.update(
                        &sql,
                        None,
                        &[dst.as_slice().into(), first.as_slice().into(), rest.into()],
                    ),
                };
                match result {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SDIFFSTORE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SMove { src, dst, member } => {
                let sql = format!(
                    "WITH d AS ( \
                         DELETE FROM redis.set_{db} \
                         WHERE key = $1 AND member = $3 \
                         RETURNING member \
                     ), \
                     ins AS ( \
                         INSERT INTO redis.set_{db} (key, member) \
                         SELECT $2, d.member FROM d \
                         ON CONFLICT DO NOTHING \
                         RETURNING 1 \
                     ) \
                     SELECT (SELECT count(*)::bigint FROM d)"
                );
                match client.update(
                    &sql,
                    None,
                    &[
                        src.as_slice().into(),
                        dst.as_slice().into(),
                        member.as_slice().into(),
                    ],
                ) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: SMOVE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::ZAdd { key, nx, xx, gt, lt, ch, incr, pairs } => {
                zadd_execute(client, db, key, ZAddFlags { nx: *nx, xx: *xx, gt: *gt, lt: *lt, ch: *ch, incr: *incr }, pairs)
            }
            Command::ZRem { key, members } => {
                let sql = format!(
                    "WITH d AS (DELETE FROM redis.zset_{db} \
                                WHERE key = $1 AND member = ANY($2::bytea[]) \
                                RETURNING 1) \
                     SELECT count(*)::bigint FROM d"
                );
                let members_vec: Vec<Option<Vec<u8>>> =
                    members.iter().map(|m| Some(m.clone())).collect();
                run_count(client, &sql, &[key.as_slice().into(), members_vec.into()], "ZREM")
            }
            Command::ZScore { key, member } => {
                match client.select(&sql::ZSCORE[db as usize], None, &[key.as_slice().into(), member.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<f64>(1) {
                        Ok(Some(s)) => Response::BulkString(format_score(s).into_bytes()),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: ZSCORE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }
            Command::ZMScore { key, members } => {
                let sql = format!(
                    "SELECT z.score \
                     FROM unnest($2::bytea[]) WITH ORDINALITY AS m(member, ord) \
                     LEFT JOIN redis.zset_{db} z ON z.key = $1 AND z.member = m.member \
                     ORDER BY m.ord"
                );
                let members_vec: Vec<Option<Vec<u8>>> =
                    members.iter().map(|m| Some(m.clone())).collect();
                match client.select(&sql, None, &[key.as_slice().into(), members_vec.into()]) {
                    Ok(tbl) => {
                        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(members.len());
                        for row in tbl {
                            match row.get::<f64>(1).ok().flatten() {
                                Some(s) => out.push(Some(format_score(s).into_bytes())),
                                None => out.push(None),
                            }
                        }
                        Response::Array(out)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: ZMSCORE error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }
            Command::ZIncrBy { key, increment, member } => {
                let sql = format!(
                    "INSERT INTO redis.zset_{db} (key, member, score) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (key, member) \
                         DO UPDATE SET score = redis.zset_{db}.score + EXCLUDED.score \
                     RETURNING score"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), member.as_slice().into(), (*increment).into()],
                ) {
                    Ok(tbl) => match tbl.first().get::<f64>(1) {
                        Ok(Some(s)) => {
                            if s.is_nan() {
                                Response::Error(
                                    "ERR resulting score is not a number (NaN)".to_string(),
                                )
                            } else {
                                Response::BulkString(format_score(s).into_bytes())
                            }
                        }
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: ZINCRBY error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }
            Command::ZCard { key } => {
                run_count(client, &sql::ZCARD[db as usize], &[key.as_slice().into()], "ZCARD")
            }
            Command::ZCount { key, min, max } => {
                let (min_op, max_op) = (score_ge_op(min), score_le_op(max));
                let sql = format!(
                    "SELECT count(*)::bigint FROM redis.zset_{db} \
                     WHERE key = $1 AND score {min_op} $2 AND score {max_op} $3"
                );
                run_count(
                    client,
                    &sql,
                    &[key.as_slice().into(), min.value.into(), max.value.into()],
                    "ZCOUNT",
                )
            }
            Command::ZLexCount { key, min, max } => {
                let (where_clause, extra_args) = lex_where(min, max, 2);
                let sql = format!(
                    "SELECT count(*)::bigint FROM redis.zset_{db} \
                     WHERE key = $1{where_clause}"
                );
                let mut args: Vec<DatumWithOid> = Vec::new();
                args.push(key.as_slice().into());
                for a in extra_args.iter() {
                    args.push(a.as_slice().into());
                }
                run_count(client, &sql, &args, "ZLEXCOUNT")
            }
            Command::ZRank { key, member, rev, with_score } => {
                zrank_execute(client, db, key, member, *rev, *with_score)
            }
            Command::ZRange { key, start, stop, by, rev, limit, with_scores } => {
                zrange_execute(client, db, key, start, stop, ZRangeOptions { by: *by, rev: *rev, limit: *limit, with_scores: *with_scores })
            }
            Command::ZRangeByScore { key, min, max, rev, with_scores, limit } => {
                zrange_by_score_execute(client, db, key, *min, *max, ZRangeByScoreOptions { rev: *rev, with_scores: *with_scores, limit: *limit })
            }
            Command::ZRangeByLex { key, min, max, rev, limit } => {
                zrange_by_lex_execute(client, db, key, min, max, *rev, *limit)
            }
            Command::ZPopMin { key, count } => zpop_execute(client, db, key, *count, true),
            Command::ZPopMax { key, count } => zpop_execute(client, db, key, *count, false),
            Command::ZRandMember { key, count, with_scores } => {
                zrandmember_execute(client, db, key, *count, *with_scores)
            }
            Command::ZRemRangeByRank { key, start, stop } => {
                let sql = format!(
                    "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.zset_{db} WHERE key = $1), \
                     nrm AS ( \
                         SELECT \
                             CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                             LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
                         FROM cnt \
                     ), \
                     victims AS ( \
                         SELECT key, member FROM redis.zset_{db}, nrm \
                         WHERE key = $1 \
                           AND (SELECT n FROM cnt) > 0 \
                           AND (SELECT s FROM nrm) <= (SELECT e FROM nrm) \
                         ORDER BY score ASC, member ASC \
                         OFFSET (SELECT s FROM nrm) \
                         LIMIT (SELECT GREATEST(0, e - s + 1) FROM nrm) \
                     ), \
                     d AS ( \
                         DELETE FROM redis.zset_{db} z USING victims v \
                         WHERE z.key = v.key AND z.member = v.member \
                         RETURNING 1 \
                     ) \
                     SELECT count(*)::bigint FROM d"
                );
                run_count(
                    client,
                    &sql,
                    &[key.as_slice().into(), (*start).into(), (*stop).into()],
                    "ZREMRANGEBYRANK",
                )
            }
            Command::ZRemRangeByScore { key, min, max } => {
                let (min_op, max_op) = (score_ge_op(min), score_le_op(max));
                let sql = format!(
                    "WITH d AS (DELETE FROM redis.zset_{db} \
                                WHERE key = $1 AND score {min_op} $2 AND score {max_op} $3 \
                                RETURNING 1) \
                     SELECT count(*)::bigint FROM d"
                );
                run_count(
                    client,
                    &sql,
                    &[key.as_slice().into(), min.value.into(), max.value.into()],
                    "ZREMRANGEBYSCORE",
                )
            }
            Command::ZRemRangeByLex { key, min, max } => {
                let (where_clause, extra_args) = lex_where(min, max, 2);
                let sql = format!(
                    "WITH d AS (DELETE FROM redis.zset_{db} \
                                WHERE key = $1{where_clause} \
                                RETURNING 1) \
                     SELECT count(*)::bigint FROM d"
                );
                let mut args: Vec<DatumWithOid> = Vec::new();
                args.push(key.as_slice().into());
                for a in extra_args.iter() {
                    args.push(a.as_slice().into());
                }
                run_count(client, &sql, &args, "ZREMRANGEBYLEX")
            }
            Command::ZUnion { keys, weights, aggregate, with_scores } => {
                zaggregate_execute(client, db, keys, weights.as_deref(), ZAggregateOptions { aggregate: *aggregate, with_scores: *with_scores, op: AggOp::Union, store_into: None })
            }
            Command::ZInter { keys, weights, aggregate, with_scores } => {
                zaggregate_execute(client, db, keys, weights.as_deref(), ZAggregateOptions { aggregate: *aggregate, with_scores: *with_scores, op: AggOp::Inter, store_into: None })
            }
            Command::ZDiff { keys, with_scores } => {
                zaggregate_execute(client, db, keys, None, ZAggregateOptions { aggregate: Aggregate::Sum, with_scores: *with_scores, op: AggOp::Diff, store_into: None })
            }
            Command::ZUnionStore { dst, keys, weights, aggregate } => {
                zaggregate_execute(client, db, keys, weights.as_deref(), ZAggregateOptions { aggregate: *aggregate, with_scores: false, op: AggOp::Union, store_into: Some(dst.as_slice()) })
            }
            Command::ZInterStore { dst, keys, weights, aggregate } => {
                zaggregate_execute(client, db, keys, weights.as_deref(), ZAggregateOptions { aggregate: *aggregate, with_scores: false, op: AggOp::Inter, store_into: Some(dst.as_slice()) })
            }
            Command::ZDiffStore { dst, keys } => {
                zaggregate_execute(client, db, keys, None, ZAggregateOptions { aggregate: Aggregate::Sum, with_scores: false, op: AggOp::Diff, store_into: Some(dst.as_slice()) })
            }
            Command::TablePublish { schema, table, channel, payload } => {
                let schema_str = String::from_utf8_lossy(schema);
                let table_str = String::from_utf8_lossy(table);
                let sql = format!(
                    "INSERT INTO {}.{} (channel, payload) VALUES ($1, $2)",
                    pg_quote_ident(&schema_str),
                    pg_quote_ident(&table_str),
                );
                // Bound as bytes, not as a lossy String: `create_pubsub_table`
                // makes both columns BYTEA, so binding text raises "column is
                // of type bytea but expression is of type text" — and a routed
                // PUBLISH is fire-and-forget, so nobody would see the reply.
                match client.update(
                    &sql,
                    None,
                    &[channel.as_slice().into(), payload.as_slice().into()],
                ) {
                    Ok(_) => Response::Null,
                    Err(e) => {
                        pgrx::warning!("pg_redis: table publish failed: {e}");
                        Response::Null
                    }
                }
            }
            Command::Multi | Command::Exec | Command::Discard | Command::Watch { .. }
            | Command::Unwatch
            | Command::Subscribe { .. }
            | Command::Unsubscribe
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe
            | Command::Publish { .. }
            | Command::PubSubChannels { .. }
            | Command::PubSubNumSub { .. }
            | Command::PubSubNumPat
            | Command::PubSubHelp => {
                Response::Error("ERR command not allowed in this context".to_string())
            }
        }
    }

    /// Execute command via shared-memory path (no SPI, no transaction).
    /// Only valid for the ephemeral half (`db < DURABLE_FROM`) when storage_mode = 'memory'.
    /// Hash/list/set/zset commands fall back to the SPI path.
    pub fn execute_mem(&self, db: u8) -> Response {
        // The ephemeral half maps one-to-one onto the shared-memory databases.
        debug_assert!(is_ephemeral(db), "execute_mem called for durable db {db}");

        // Before anything is written, so a refusal cannot leave a partial write.
        if let Some(err) = self.mem_too_long_error() {
            return err;
        }

        // Cleared up front so a flag raised by an earlier command cannot leak
        // into this reply.
        //
        // Unlike the length check above this cannot run before execution —
        // whether there is room is only knowable at the insert. For a multi-key
        // write that fills the table part-way through (MSET, SADD with many
        // members) the earlier keys are already stored when the error is
        // reported, where Redis would have rejected the command whole.
        crate::mem::take_oom();
        let response = self.execute_mem_inner(db as usize);
        if crate::mem::take_oom() {
            return mem_out_of_memory();
        }
        response
    }

    fn execute_mem_inner(&self, db_idx: usize) -> Response {
        use crate::mem;

        /// Borrow owned byte arguments as slices for the mem_* API.
        fn strs(v: &[Vec<u8>]) -> Vec<&[u8]> {
            v.iter().map(Vec::as_slice).collect()
        }

        fn mem_ttl_response(db_idx: usize, key: &[u8], divisor: i64, relative: bool) -> Response {
            let (exists, exp_us) = unsafe { crate::mem::mem_ttl_raw(db_idx, key) };
            if !exists {
                return Response::Integer(-2);
            }
            if exp_us == 0 {
                return Response::Integer(-1);
            }
            let value = if relative {
                ((exp_us - now_micros()).max(-divisor) / divisor).max(-1)
            } else {
                exp_us / divisor
            };
            Response::Integer(value)
        }

        match self {
            Command::Get { key } => {
                let v = unsafe { mem::mem_get(db_idx, key) };
                match v {
                    Some(s) => Response::BulkString(s),
                    None => Response::Null,
                }
            }

            Command::Set {
                key,
                value,
                ex_ms,
                nx,
                xx,
                get,
                keepttl,
            } => {
                let expires_at_us = ex_ms.map(|ms| now_micros() + ms * 1000).unwrap_or(0);

                unsafe {
                    // GET reports the value the key held, including on the paths
                    // where NX or XX declines to write it. Returning nil there
                    // would say "there was nothing here" about a key whose
                    // existence is the very reason the write was refused.
                    let declined = |db_idx: usize, key: &[u8]| match *get {
                        true => match mem::mem_get(db_idx, key) {
                            Some(s) => Response::BulkString(s),
                            None => Response::Null,
                        },
                        false => Response::Null,
                    };
                    if *nx {
                        let exists = mem::mem_exists(db_idx, &[key.as_slice()]) > 0;
                        if exists {
                            return declined(db_idx, key);
                        }
                    }
                    if *xx {
                        let exists = mem::mem_exists(db_idx, &[key.as_slice()]) > 0;
                        if !exists {
                            return declined(db_idx, key);
                        }
                    }

                    if *keepttl {
                        let (exists, exp) = mem::mem_ttl_raw(db_idx, key);
                        let ttl = if exists { exp } else { 0 };
                        return match mem::mem_set(db_idx, key, value, ttl) {
                            true => Response::Ok,
                            false => mem_value_too_large(),
                        };
                    }

                    let old = if *get {
                        mem::mem_get(db_idx, key)
                    } else {
                        None
                    };

                    if !mem::mem_set(db_idx, key, value, expires_at_us) {
                        return mem_value_too_large();
                    }

                    if *get {
                        match old {
                            Some(s) => Response::BulkString(s),
                            None => Response::Null,
                        }
                    } else {
                        Response::Ok
                    }
                }
            }

            Command::SetEx {
                key,
                value,
                ex_secs,
            } => {
                let expires_at_us = now_micros() + ex_secs * 1_000_000;
                if unsafe { mem::mem_set(db_idx, key, value, expires_at_us) } {
                    Response::Ok
                } else {
                    mem_value_too_large()
                }
            }

            Command::PSetEx { key, value, ex_ms } => {
                let expires_at_us = now_micros() + ex_ms * 1000;
                if unsafe { mem::mem_set(db_idx, key, value, expires_at_us) } {
                    Response::Ok
                } else {
                    mem_value_too_large()
                }
            }

            Command::SetNx { key, value } => {
                let exists = unsafe { mem::mem_exists(db_idx, &[key.as_slice()]) > 0 };
                if !exists {
                    if unsafe { mem::mem_set(db_idx, key, value, 0) } {
                        Response::Integer(1)
                    } else {
                        mem_value_too_large()
                    }
                } else {
                    Response::Integer(0)
                }
            }

            Command::MSetNx { pairs } => {
                let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
                let any_exists = unsafe { mem::mem_exists(db_idx, &keys) > 0 };
                if any_exists {
                    Response::Integer(0)
                } else {
                    let p: Vec<(&[u8], &[u8])> = pairs
                        .iter()
                        .map(|(k, v)| (k.as_slice(), v.as_slice()))
                        .collect();
                    unsafe { mem::mem_mset(db_idx, &p) };
                    Response::Integer(1)
                }
            }

            Command::Del { keys } | Command::Unlink { keys } => {
                let mut count = 0i64;
                for k in keys {
                    let kv = unsafe { mem::mem_del(db_idx, &[k.as_slice()]) };
                    let sub = unsafe { mem::mem_del_all_types(db_idx, k) };
                    if kv > 0 || sub > 0 {
                        count += 1;
                    }
                }
                Response::Integer(count)
            }

            Command::Exists { keys } => {
                Response::Integer(unsafe { mem::mem_exists(db_idx, &strs(keys)) })
            }

            Command::Incr { key } => match unsafe { mem::mem_incr(db_idx, key, 1) } {
                Ok(n) => Response::Integer(n),
                Err(e) => Response::Error(e),
            },

            Command::Decr { key } => match unsafe { mem::mem_incr(db_idx, key, -1) } {
                Ok(n) => Response::Integer(n),
                Err(e) => Response::Error(e),
            },

            Command::IncrBy { key, delta } => match unsafe { mem::mem_incr(db_idx, key, *delta) } {
                Ok(n) => Response::Integer(n),
                Err(e) => Response::Error(e),
            },

            Command::DecrBy { key, delta } => match unsafe { mem::mem_incr(db_idx, key, -delta) } {
                Ok(n) => Response::Integer(n),
                Err(e) => Response::Error(e),
            },

            Command::IncrByFloat { key, delta } => {
                match unsafe { mem::mem_incr_float(db_idx, key, *delta) } {
                    Ok(s) => Response::BulkString(s.into_bytes()),
                    Err(e) => Response::Error(e),
                }
            }

            Command::Append { key, value } => {
                Response::Integer(unsafe { mem::mem_append(db_idx, key, value) })
            }

            Command::Strlen { key } => Response::Integer(unsafe { mem::mem_strlen(db_idx, key) }),

            Command::GetDel { key } => match unsafe { mem::mem_getdel(db_idx, key) } {
                Some(v) => Response::BulkString(v),
                None => Response::Null,
            },

            Command::GetSet { key, value } => {
                let old = unsafe { mem::mem_getset(db_idx, key, value) };
                match old {
                    Some(v) => Response::BulkString(v),
                    None => Response::Null,
                }
            }

            Command::Ttl { key } => mem_ttl_response(db_idx, key, 1_000_000, true),
            Command::PTtl { key } => mem_ttl_response(db_idx, key, 1_000, true),
            Command::ExpireTime { key } => mem_ttl_response(db_idx, key, 1_000_000, false),
            Command::PExpireTime { key } => mem_ttl_response(db_idx, key, 1_000, false),

            Command::Expire { key, secs } => {
                let exp_us = now_micros() + secs * 1_000_000;
                Response::Integer(unsafe { mem::mem_set_expiry(db_idx, key, exp_us) } as i64)
            }

            Command::PExpire { key, ms } => {
                let exp_us = now_micros() + ms * 1000;
                Response::Integer(unsafe { mem::mem_set_expiry(db_idx, key, exp_us) } as i64)
            }

            Command::ExpireAt { key, unix_secs } => {
                let exp_us = unix_secs * 1_000_000;
                Response::Integer(unsafe { mem::mem_set_expiry(db_idx, key, exp_us) } as i64)
            }

            Command::PExpireAt { key, unix_ms } => {
                let exp_us = unix_ms * 1000;
                Response::Integer(unsafe { mem::mem_set_expiry(db_idx, key, exp_us) } as i64)
            }

            Command::Persist { key } => {
                Response::Integer(unsafe { mem::mem_persist(db_idx, key) } as i64)
            }

            Command::MGet { keys } => {
                let results = unsafe { mem::mem_mget(db_idx, keys) };
                Response::Array(results.into_iter().collect())
            }

            Command::MSet { pairs } => {
                let p: Vec<(&[u8], &[u8])> = pairs
                    .iter()
                    .map(|(k, v)| (k.as_slice(), v.as_slice()))
                    .collect();
                unsafe { mem::mem_mset(db_idx, &p) };
                Response::Ok
            }

            Command::Keys { pattern } => {
                let keys = unsafe { mem::mem_scan(db_idx, pattern) };
                Response::Array(keys.into_iter().map(Some).collect())
            }

            Command::Scan { pattern, .. } => {
                let pat: &[u8] = pattern.as_deref().unwrap_or(b"*");
                let keys = unsafe { mem::mem_scan(db_idx, pat) };
                Response::ScanResult {
                    keys: keys.into_iter().map(Some).collect(),
                }
            }

            Command::DbSize => Response::Integer(unsafe { mem::mem_dbsize(db_idx) }),

            Command::Type { key } => {
                let t = unsafe { mem::mem_type(db_idx, key) };
                Response::SimpleString(t.to_string())
            }

            Command::Rename { key, newkey } => {
                let val = unsafe { mem::mem_getdel(db_idx, key) };
                match val {
                    Some(v) => {
                        unsafe { mem::mem_set(db_idx, newkey, &v, 0) };
                        Response::Ok
                    }
                    None => Response::Error("ERR no such key".to_string()),
                }
            }

            Command::RandomKey => match unsafe { mem::mem_random_key(db_idx) } {
                Some(k) => Response::BulkString(k),
                None => Response::Null,
            },

            // ── Hash commands ──────────────────────────────────────────────
            Command::HSet { key, pairs } => {
                let mut new_count = 0i64;
                for (f, v) in pairs {
                    if unsafe { mem::mem_hset(db_idx, key, f, v) } {
                        new_count += 1;
                    }
                }
                Response::Integer(new_count)
            }
            Command::HGet { key, field } => match unsafe { mem::mem_hget(db_idx, key, field) } {
                Some(v) => Response::BulkString(v),
                None => Response::Null,
            },
            Command::HGetAll { key } => {
                let pairs = unsafe { mem::mem_hgetall(db_idx, key) };
                let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(pairs.len() * 2);
                for (f, v) in pairs {
                    out.push(Some(f));
                    out.push(Some(v));
                }
                Response::Array(out)
            }
            Command::HDel { key, fields } => {
                Response::Integer(unsafe { mem::mem_hdel(db_idx, key, &strs(fields)) })
            }
            Command::HExists { key, field } => {
                Response::Integer(unsafe { mem::mem_hexists(db_idx, key, field) } as i64)
            }
            Command::HKeys { key } => {
                let keys = unsafe { mem::mem_hkeys(db_idx, key) };
                Response::Array(keys.into_iter().map(Some).collect())
            }
            Command::HVals { key } => {
                let vals = unsafe { mem::mem_hvals(db_idx, key) };
                Response::Array(vals.into_iter().map(Some).collect())
            }
            Command::HLen { key } => Response::Integer(unsafe { mem::mem_hlen(db_idx, key) }),
            Command::HMGet { key, fields } => {
                let results = unsafe { mem::mem_hmget(db_idx, key, &strs(fields)) };
                Response::Array(results.into_iter().collect())
            }
            Command::HMSet { key, pairs } => {
                for (f, v) in pairs {
                    unsafe { mem::mem_hset(db_idx, key, f, v) };
                }
                Response::Ok
            }
            Command::HIncrBy { key, field, delta } => {
                match unsafe { mem::mem_hincrby(db_idx, key, field, *delta) } {
                    Ok(n) => Response::Integer(n),
                    Err(e) => Response::Error(e),
                }
            }
            Command::HSetNx { key, field, value } => {
                Response::Integer(unsafe { mem::mem_hsetnx(db_idx, key, field, value) } as i64)
            }

            // ── Set commands ───────────────────────────────────────────────
            Command::SAdd { key, members } => {
                Response::Integer(unsafe { mem::mem_sadd(db_idx, key, &strs(members)) })
            }
            Command::SRem { key, members } => {
                Response::Integer(unsafe { mem::mem_srem(db_idx, key, &strs(members)) })
            }
            Command::SIsMember { key, member } => {
                Response::Integer(unsafe { mem::mem_sismember(db_idx, key, member) } as i64)
            }
            Command::SMisMember { key, members } => {
                let results = unsafe { mem::mem_smismember(db_idx, key, &strs(members)) };
                Response::IntegerArray(results.into_iter().map(i64::from).collect())
            }
            Command::SMembers { key } => {
                let members = unsafe { mem::mem_smembers(db_idx, key) };
                Response::Array(members.into_iter().map(Some).collect())
            }
            Command::SCard { key } => Response::Integer(unsafe { mem::mem_scard(db_idx, key) }),
            Command::SPop { key, count } => {
                let n = count.unwrap_or(1).max(0);
                let popped = unsafe { mem::mem_spop(db_idx, key, n) };
                if count.is_none() {
                    match popped.into_iter().next() {
                        Some(m) => Response::BulkString(m),
                        None => Response::Null,
                    }
                } else {
                    Response::Array(popped.into_iter().map(Some).collect())
                }
            }
            Command::SRandMember { key, count } => {
                let n = count.unwrap_or(1);
                let members = unsafe { mem::mem_srandmember(db_idx, key, n) };
                if count.is_none() {
                    match members.into_iter().next() {
                        Some(m) => Response::BulkString(m),
                        None => Response::Null,
                    }
                } else {
                    Response::Array(members.into_iter().map(Some).collect())
                }
            }
            Command::SMove { src, dst, member } => {
                Response::Integer(unsafe { mem::mem_smove(db_idx, src, dst, member) } as i64)
            }
            Command::SUnion { keys } => {
                let members = unsafe { mem::mem_sunion(db_idx, &strs(keys)) };
                Response::Array(members.into_iter().map(Some).collect())
            }
            Command::SInter { keys } => {
                let members = unsafe { mem::mem_sinter(db_idx, &strs(keys)) };
                Response::Array(members.into_iter().map(Some).collect())
            }
            Command::SDiff { keys } => {
                let members = unsafe { mem::mem_sdiff(db_idx, &strs(keys)) };
                Response::Array(members.into_iter().map(Some).collect())
            }
            Command::SUnionStore { dst, keys } => {
                Response::Integer(unsafe { mem::mem_sunionstore(db_idx, dst, &strs(keys)) })
            }
            Command::SInterStore { dst, keys } => {
                Response::Integer(unsafe { mem::mem_sinterstore(db_idx, dst, &strs(keys)) })
            }
            Command::SDiffStore { dst, keys } => {
                Response::Integer(unsafe { mem::mem_sdiffstore(db_idx, dst, &strs(keys)) })
            }

            // ── Sorted set commands ────────────────────────────────────────
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
                if *incr {
                    let (delta, member) = &pairs[0];
                    match unsafe {
                        mem::mem_zadd_incr(
                            db_idx,
                            key,
                            *delta,
                            member.as_slice(),
                            *nx,
                            *xx,
                            *gt,
                            *lt,
                        )
                    } {
                        Some(s) => Response::BulkString(format_score(s).into_bytes()),
                        None => Response::Null,
                    }
                } else {
                    let ps: Vec<(f64, &[u8])> =
                        pairs.iter().map(|(s, m)| (*s, m.as_slice())).collect();
                    Response::Integer(unsafe {
                        mem::mem_zadd(db_idx, key, &ps, *nx, *xx, *gt, *lt, *ch)
                    })
                }
            }
            Command::ZRem { key, members } => {
                Response::Integer(unsafe { mem::mem_zrem(db_idx, key, &strs(members)) })
            }
            Command::ZScore { key, member } => {
                match unsafe { mem::mem_zscore(db_idx, key, member) } {
                    Some(s) => Response::BulkString(format_score(s).into_bytes()),
                    None => Response::Null,
                }
            }
            Command::ZMScore { key, members } => {
                let scores = unsafe { mem::mem_zmsmembers(db_idx, key, &strs(members)) };
                Response::Array(
                    scores
                        .into_iter()
                        .map(|s| s.map(|v| format_score(v).into_bytes()))
                        .collect(),
                )
            }
            Command::ZIncrBy {
                key,
                increment,
                member,
            } => {
                let s = unsafe { mem::mem_zincrby(db_idx, key, *increment, member) };
                Response::BulkString(format_score(s).into_bytes())
            }
            Command::ZCard { key } => Response::Integer(unsafe { mem::mem_zcard(db_idx, key) }),
            Command::ZCount { key, min, max } => Response::Integer(unsafe {
                mem::mem_zcount(
                    db_idx,
                    key,
                    min.value,
                    max.value,
                    min.exclusive,
                    max.exclusive,
                )
            }),
            Command::ZLexCount { key, min, max } => {
                Response::Integer(unsafe { mem::mem_zlexcount(db_idx, key, min, max) })
            }
            Command::ZRank {
                key,
                member,
                rev,
                with_score,
            } => match unsafe { mem::mem_zrank(db_idx, key, member, *rev) } {
                Some((rank, score)) => {
                    if *with_score {
                        let mut out: Vec<Option<Vec<u8>>> =
                            vec![Some(rank.to_string().into_bytes())];
                        if let Some(s) = score {
                            out.push(Some(format_score(s).into_bytes()));
                        }
                        Response::Array(out)
                    } else {
                        Response::Integer(rank)
                    }
                }
                None => Response::Null,
            },
            Command::ZRange {
                key,
                start,
                stop,
                by,
                rev,
                limit,
                with_scores,
            } => {
                use RangeBy;
                match by {
                    RangeBy::Index => {
                        let s: i64 = int_from_bytes(start).unwrap_or(0);
                        let e: i64 = int_from_bytes(stop).unwrap_or(-1);
                        let results = unsafe {
                            mem::mem_zrange_by_index(db_idx, key, s, e, *rev, *with_scores)
                        };
                        mem_zrange_response(results)
                    }
                    RangeBy::Score => {
                        let (min, max, ex_min, ex_max) = parse_score_range(start, stop);
                        let results = unsafe {
                            mem::mem_zrange_by_score(
                                db_idx, key, min, max, ex_min, ex_max, *rev, *limit,
                            )
                        };
                        mem_zrange_response(results)
                    }
                    RangeBy::Lex => {
                        let min_b = parse_lex_bound_infallible(start);
                        let max_b = parse_lex_bound_infallible(stop);
                        let results = unsafe {
                            mem::mem_zrangebylex(db_idx, key, &min_b, &max_b, *rev, *limit)
                        };
                        Response::Array(results.into_iter().map(Some).collect())
                    }
                }
            }
            Command::ZRangeByScore {
                key,
                min,
                max,
                rev,
                with_scores,
                limit,
            } => {
                let results = unsafe {
                    mem::mem_zrange_by_score(
                        db_idx,
                        key,
                        min.value,
                        max.value,
                        min.exclusive,
                        max.exclusive,
                        *rev,
                        *limit,
                    )
                };
                if *with_scores {
                    let mut out = Vec::new();
                    for (m, s) in results {
                        out.push(Some(m));
                        if let Some(sc) = s {
                            out.push(Some(format_score(sc).into_bytes()));
                        }
                    }
                    Response::Array(out)
                } else {
                    Response::Array(results.into_iter().map(|(m, _)| Some(m)).collect())
                }
            }
            Command::ZRangeByLex {
                key,
                min,
                max,
                rev,
                limit,
            } => {
                let results = unsafe { mem::mem_zrangebylex(db_idx, key, min, max, *rev, *limit) };
                Response::Array(results.into_iter().map(Some).collect())
            }
            Command::ZPopMin { key, count } => {
                let n = count.unwrap_or(1).max(0);
                let results = unsafe { mem::mem_zpopmin(db_idx, key, n) };
                if count.is_none() && results.is_empty() {
                    return Response::Array(vec![]);
                }
                let mut out = Vec::new();
                for (m, s) in results {
                    out.push(Some(m));
                    out.push(Some(format_score(s).into_bytes()));
                }
                Response::Array(out)
            }
            Command::ZPopMax { key, count } => {
                let n = count.unwrap_or(1).max(0);
                let results = unsafe { mem::mem_zpopmax(db_idx, key, n) };
                if count.is_none() && results.is_empty() {
                    return Response::Array(vec![]);
                }
                let mut out = Vec::new();
                for (m, s) in results {
                    out.push(Some(m));
                    out.push(Some(format_score(s).into_bytes()));
                }
                Response::Array(out)
            }
            Command::ZRandMember {
                key,
                count,
                with_scores,
            } => {
                let n = count.unwrap_or(1);
                let results = unsafe { mem::mem_zrandmember(db_idx, key, n, *with_scores) };
                if count.is_none() {
                    match results.into_iter().next() {
                        Some((m, _)) => Response::BulkString(m),
                        None => Response::Null,
                    }
                } else if *with_scores {
                    let mut out = Vec::new();
                    for (m, s) in results {
                        out.push(Some(m));
                        if let Some(sc) = s {
                            out.push(Some(format_score(sc).into_bytes()));
                        }
                    }
                    Response::Array(out)
                } else {
                    Response::Array(results.into_iter().map(|(m, _)| Some(m)).collect())
                }
            }
            Command::ZRemRangeByRank { key, start, stop } => {
                Response::Integer(unsafe { mem::mem_zremrangebyrank(db_idx, key, *start, *stop) })
            }
            Command::ZRemRangeByScore { key, min, max } => Response::Integer(unsafe {
                mem::mem_zremrangebyscore(
                    db_idx,
                    key,
                    min.value,
                    max.value,
                    min.exclusive,
                    max.exclusive,
                )
            }),
            Command::ZRemRangeByLex { key, min, max } => {
                Response::Integer(unsafe { mem::mem_zremrangebylex(db_idx, key, min, max) })
            }
            Command::ZUnionStore {
                dst,
                keys,
                weights,
                aggregate,
            } => {
                let ws: Vec<f64> = weights.as_deref().unwrap_or(&[]).to_vec();
                Response::Integer(unsafe {
                    mem::mem_zunionstore(db_idx, dst, &strs(keys), &ws, *aggregate)
                })
            }
            Command::ZInterStore {
                dst,
                keys,
                weights,
                aggregate,
            } => {
                let ws: Vec<f64> = weights.as_deref().unwrap_or(&[]).to_vec();
                Response::Integer(unsafe {
                    mem::mem_zinterstore(db_idx, dst, &strs(keys), &ws, *aggregate)
                })
            }
            Command::ZDiffStore { dst, keys } => {
                Response::Integer(unsafe { mem::mem_zdiffstore(db_idx, dst, &strs(keys)) })
            }
            Command::ZUnion {
                keys,
                weights,
                aggregate,
                with_scores,
            } => {
                let ws: Vec<f64> = weights.as_deref().unwrap_or(&[]).to_vec();
                let tmp_dst: &[u8] = b"__mem_zunion_tmp__";
                unsafe { mem::mem_zunionstore(db_idx, tmp_dst, &strs(keys), &ws, *aggregate) };
                let htab_results = unsafe { mem::mem_zset_collect_all(db_idx, tmp_dst) };
                unsafe { mem::mem_del_zset_key(db_idx, tmp_dst) };
                if *with_scores {
                    let mut out = Vec::new();
                    for (m, s) in htab_results {
                        out.push(Some(m));
                        out.push(Some(format_score(s).into_bytes()));
                    }
                    Response::Array(out)
                } else {
                    Response::Array(htab_results.into_iter().map(|(m, _)| Some(m)).collect())
                }
            }
            Command::ZInter {
                keys,
                weights,
                aggregate,
                with_scores,
            } => {
                let ws: Vec<f64> = weights.as_deref().unwrap_or(&[]).to_vec();
                let tmp_dst: &[u8] = b"__mem_zinter_tmp__";
                unsafe { mem::mem_zinterstore(db_idx, tmp_dst, &strs(keys), &ws, *aggregate) };
                let htab_results = unsafe { mem::mem_zset_collect_all(db_idx, tmp_dst) };
                unsafe { mem::mem_del_zset_key(db_idx, tmp_dst) };
                if *with_scores {
                    let mut out = Vec::new();
                    for (m, s) in htab_results {
                        out.push(Some(m));
                        out.push(Some(format_score(s).into_bytes()));
                    }
                    Response::Array(out)
                } else {
                    Response::Array(htab_results.into_iter().map(|(m, _)| Some(m)).collect())
                }
            }
            Command::ZDiff { keys, with_scores } => {
                let tmp_dst: &[u8] = b"__mem_zdiff_tmp__";
                unsafe { mem::mem_zdiffstore(db_idx, tmp_dst, &strs(keys)) };
                let htab_results = unsafe { mem::mem_zset_collect_all(db_idx, tmp_dst) };
                unsafe { mem::mem_del_zset_key(db_idx, tmp_dst) };
                if *with_scores {
                    let mut out = Vec::new();
                    for (m, s) in htab_results {
                        out.push(Some(m));
                        out.push(Some(format_score(s).into_bytes()));
                    }
                    Response::Array(out)
                } else {
                    Response::Array(htab_results.into_iter().map(|(m, _)| Some(m)).collect())
                }
            }

            // ── List commands ──────────────────────────────────────────────
            Command::LPush { key, values } => {
                Response::Integer(unsafe { mem::mem_lpush(db_idx, key, &strs(values)) })
            }
            Command::RPush { key, values } => {
                Response::Integer(unsafe { mem::mem_rpush(db_idx, key, &strs(values)) })
            }
            Command::LPushX { key, values } => {
                Response::Integer(unsafe { mem::mem_lpushx(db_idx, key, &strs(values)) })
            }
            Command::RPushX { key, values } => {
                Response::Integer(unsafe { mem::mem_rpushx(db_idx, key, &strs(values)) })
            }
            Command::LPop { key, count } => {
                let popped = unsafe { mem::mem_lpop(db_idx, key, *count) };
                if count.is_none() {
                    match popped.into_iter().next() {
                        Some(v) => Response::BulkString(v),
                        None => Response::Null,
                    }
                } else if popped.is_empty() {
                    Response::Null
                } else {
                    Response::Array(popped.into_iter().map(Some).collect())
                }
            }
            Command::RPop { key, count } => {
                let popped = unsafe { mem::mem_rpop(db_idx, key, *count) };
                if count.is_none() {
                    match popped.into_iter().next() {
                        Some(v) => Response::BulkString(v),
                        None => Response::Null,
                    }
                } else if popped.is_empty() {
                    Response::Null
                } else {
                    Response::Array(popped.into_iter().map(Some).collect())
                }
            }
            Command::LLen { key } => Response::Integer(unsafe { mem::mem_llen(db_idx, key) }),
            Command::LRange { key, start, stop } => {
                let items = unsafe { mem::mem_lrange(db_idx, key, *start, *stop) };
                Response::Array(items.into_iter().map(Some).collect())
            }
            Command::LIndex { key, index } => match unsafe { mem::mem_lindex(db_idx, key, *index) }
            {
                Some(v) => Response::BulkString(v),
                None => Response::Null,
            },
            Command::LSet { key, index, value } => {
                if unsafe { mem::mem_lset(db_idx, key, *index, value) } {
                    Response::Ok
                } else {
                    Response::Error("ERR index out of range".to_string())
                }
            }
            Command::LRem { key, count, value } => {
                Response::Integer(unsafe { mem::mem_lrem(db_idx, key, *count, value) })
            }
            Command::LTrim { key, start, stop } => {
                unsafe { mem::mem_ltrim(db_idx, key, *start, *stop) };
                Response::Ok
            }
            Command::LMove {
                src,
                dst,
                src_left,
                dst_left,
            } => match unsafe { mem::mem_lmove(db_idx, src, dst, *src_left, *dst_left) } {
                Some(v) => Response::BulkString(v),
                None => Response::Null,
            },
            Command::LPos {
                key,
                element,
                rank,
                count,
            } => {
                let r = rank.unwrap_or(1);
                let positions = unsafe { mem::mem_lpos(db_idx, key, element, r, *count) };
                if count.is_none() {
                    match positions.into_iter().next() {
                        Some(p) => Response::Integer(p),
                        None => Response::Null,
                    }
                } else {
                    Response::IntegerArray(positions)
                }
            }

            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch
            | Command::Subscribe { .. }
            | Command::Unsubscribe
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe
            | Command::Publish { .. }
            | Command::PubSubChannels { .. }
            | Command::PubSubNumSub { .. }
            | Command::PubSubNumPat
            | Command::PubSubHelp => {
                Response::Error("ERR command not allowed in this context".to_string())
            }

            Command::LInsert {
                key,
                before,
                pivot,
                value,
            } => Response::Integer(unsafe { mem::mem_linsert(db_idx, key, *before, pivot, value) }),

            // TablePublish falls back: it always needs SPI regardless of storage
            // mode. So do the connection-level commands, which touch no data.
            _ => {
                use pgrx::bgworkers::BackgroundWorker;
                use pgrx::prelude::*;
                BackgroundWorker::transaction(|| {
                    Spi::connect_mut(|client| self.execute(client, db_idx as u8))
                })
            }
        }
    }

    /// Refuse anything the shared-memory backend would have to truncate to
    /// store.
    ///
    /// A key lives in a fixed-size array, and the old behaviour was to copy as
    /// much as fit. That is the one failure mode worse than an error: two
    /// distinct keys sharing a 511-byte prefix collapse onto the same entry, so
    /// a `GET` returns another key's value and a `DEL` removes it. Redis has no
    /// length limit at all, so the closest we can get in a fixed-size region is
    /// to refuse and name the database that has no limit.
    ///
    /// Values and members are bounded for a different reason: they spill into a
    /// chunk pool rather than a fixed slot, so the limit is what one of them may
    /// claim of that pool — see `mem::max_total_val_len` and
    /// `mem::max_member_len`, which agree at 64 KiB by default. Over it is still
    /// a refusal, never a truncation.
    ///
    /// Runs before the command executes, so a refusal never leaves a partial
    /// write behind. Allocation-free — the arms borrow and the iterators are
    /// lazy, because this sits on the hot path.
    fn mem_too_long_error(&self) -> Option<Response> {
        fn slices(v: &[Vec<u8>]) -> impl Iterator<Item = &[u8]> {
            v.iter().map(Vec::as_slice)
        }
        // Commands carrying a field or member get both checks; everything else
        // falls through to the key-only arm below.
        let over = match self {
            Command::HGet { key, field }
            | Command::HExists { key, field }
            | Command::HIncrBy { key, field, .. } => {
                over_key(key).or_else(|| over_members([field.as_slice()]))
            }
            Command::HSetNx { key, field, value } => over_key(key)
                .or_else(|| over_members([field.as_slice()]))
                .or_else(|| over_values([value.as_slice()])),
            Command::HSet { key, pairs } | Command::HMSet { key, pairs } => over_key(key)
                .or_else(|| over_members(pairs.iter().map(|(f, _)| f.as_slice())))
                .or_else(|| over_values(pairs.iter().map(|(_, v)| v.as_slice()))),
            Command::HDel { key, fields } | Command::HMGet { key, fields } => {
                over_key(key).or_else(|| over_members(slices(fields)))
            }
            Command::SIsMember { key, member }
            | Command::ZScore { key, member }
            | Command::ZIncrBy { key, member, .. }
            | Command::ZRank { key, member, .. } => {
                over_key(key).or_else(|| over_members([member.as_slice()]))
            }
            Command::SAdd { key, members }
            | Command::SRem { key, members }
            | Command::SMisMember { key, members }
            | Command::ZRem { key, members }
            | Command::ZMScore { key, members } => {
                over_key(key).or_else(|| over_members(slices(members)))
            }
            Command::ZAdd { key, pairs, .. } => {
                over_key(key).or_else(|| over_members(pairs.iter().map(|(_, m)| m.as_slice())))
            }
            Command::SMove { src, dst, member } => over_keys([src.as_slice(), dst.as_slice()])
                .or_else(|| over_members([member.as_slice()])),

            // Commands whose value is stored verbatim. `list_write_full_value`
            // truncates to fit exactly as the key helpers did, so an unchecked
            // LPUSH loses the tail of its element and still replies with the
            // new length.
            Command::LPush { key, values }
            | Command::RPush { key, values }
            | Command::LPushX { key, values }
            | Command::RPushX { key, values } => {
                over_key(key).or_else(|| over_values(slices(values)))
            }
            Command::LSet { key, value, .. } | Command::LInsert { key, value, .. } => {
                over_key(key).or_else(|| over_values([value.as_slice()]))
            }
            Command::Set { key, value, .. }
            | Command::SetEx { key, value, .. }
            | Command::PSetEx { key, value, .. }
            | Command::SetNx { key, value }
            | Command::GetSet { key, value }
            | Command::Append { key, value } => {
                over_key(key).or_else(|| over_values([value.as_slice()]))
            }
            Command::MSet { pairs } | Command::MSetNx { pairs } => {
                over_keys(pairs.iter().map(|(k, _)| k.as_slice()))
                    .or_else(|| over_values(pairs.iter().map(|(_, v)| v.as_slice())))
            }

            // Multi-key commands.
            Command::MGet { keys }
            | Command::Del { keys }
            | Command::Unlink { keys }
            | Command::Exists { keys }
            | Command::SUnion { keys }
            | Command::SInter { keys }
            | Command::SDiff { keys }
            | Command::ZUnion { keys, .. }
            | Command::ZInter { keys, .. }
            | Command::ZDiff { keys, .. } => over_keys(slices(keys)),
            Command::Rename { key, newkey } => over_keys([key.as_slice(), newkey.as_slice()]),
            Command::LMove { src, dst, .. } => over_keys([src.as_slice(), dst.as_slice()]),
            Command::SUnionStore { dst, keys }
            | Command::SInterStore { dst, keys }
            | Command::SDiffStore { dst, keys }
            | Command::ZDiffStore { dst, keys }
            | Command::ZUnionStore { dst, keys, .. }
            | Command::ZInterStore { dst, keys, .. } => {
                over_key(dst).or_else(|| over_keys(slices(keys)))
            }

            // Everything reaching the shared-memory backend with a single key.
            Command::Get { key }
            | Command::GetDel { key }
            | Command::Strlen { key }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. }
            | Command::IncrByFloat { key, .. }
            | Command::Expire { key, .. }
            | Command::PExpire { key, .. }
            | Command::ExpireAt { key, .. }
            | Command::PExpireAt { key, .. }
            | Command::Ttl { key }
            | Command::PTtl { key }
            | Command::ExpireTime { key }
            | Command::PExpireTime { key }
            | Command::Persist { key }
            | Command::Type { key }
            | Command::HGetAll { key }
            | Command::HKeys { key }
            | Command::HVals { key }
            | Command::HLen { key }
            | Command::LPop { key, .. }
            | Command::RPop { key, .. }
            | Command::LLen { key }
            | Command::LRange { key, .. }
            | Command::LIndex { key, .. }
            | Command::LRem { key, .. }
            | Command::LPos { key, .. }
            | Command::LTrim { key, .. }
            | Command::SMembers { key }
            | Command::SCard { key }
            | Command::SPop { key, .. }
            | Command::SRandMember { key, .. }
            | Command::ZCard { key }
            | Command::ZCount { key, .. }
            | Command::ZLexCount { key, .. }
            | Command::ZRange { key, .. }
            | Command::ZRangeByScore { key, .. }
            | Command::ZRangeByLex { key, .. }
            | Command::ZPopMin { key, .. }
            | Command::ZPopMax { key, .. }
            | Command::ZRandMember { key, .. }
            | Command::ZRemRangeByRank { key, .. }
            | Command::ZRemRangeByScore { key, .. }
            | Command::ZRemRangeByLex { key, .. } => over_key(key),

            // No key or member reaches a fixed-size slot: KEYS and SCAN match
            // against stored keys rather than storing their pattern, and WATCH
            // hashes into a separate table with no length limit.
            _ => None,
        };
        over.map(mem_too_long)
    }

    pub fn write_keys(&self) -> Vec<&[u8]> {
        match self {
            Command::Set { key, .. }
            | Command::SetEx { key, .. }
            | Command::PSetEx { key, .. }
            | Command::SetNx { key, .. }
            | Command::GetSet { key, .. }
            | Command::GetDel { key, .. }
            | Command::Append { key, .. }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. }
            | Command::IncrByFloat { key, .. }
            | Command::Expire { key, .. }
            | Command::PExpire { key, .. }
            | Command::ExpireAt { key, .. }
            | Command::PExpireAt { key, .. }
            | Command::Persist { key }
            | Command::HSet { key, .. }
            | Command::HDel { key, .. }
            | Command::HMSet { key, .. }
            | Command::HIncrBy { key, .. }
            | Command::HSetNx { key, .. }
            | Command::LPush { key, .. }
            | Command::RPush { key, .. }
            | Command::LPushX { key, .. }
            | Command::RPushX { key, .. }
            | Command::LPop { key, .. }
            | Command::RPop { key, .. }
            | Command::LInsert { key, .. }
            | Command::LSet { key, .. }
            | Command::LRem { key, .. }
            | Command::LTrim { key, .. }
            | Command::SAdd { key, .. }
            | Command::SRem { key, .. }
            | Command::SPop { key, .. }
            | Command::ZAdd { key, .. }
            | Command::ZRem { key, .. }
            | Command::ZIncrBy { key, .. }
            | Command::ZPopMin { key, .. }
            | Command::ZPopMax { key, .. }
            | Command::ZRemRangeByRank { key, .. }
            | Command::ZRemRangeByScore { key, .. }
            | Command::ZRemRangeByLex { key, .. } => vec![key.as_slice()],

            Command::Del { keys } | Command::Unlink { keys } => {
                keys.iter().map(Vec::as_slice).collect()
            }
            Command::MSet { pairs } | Command::MSetNx { pairs } => {
                pairs.iter().map(|(k, _)| k.as_slice()).collect()
            }
            Command::Rename { key, newkey } => vec![key.as_slice(), newkey.as_slice()],
            Command::LMove { src, dst, .. } => vec![src.as_slice(), dst.as_slice()],
            Command::SMove { src, dst, .. } => vec![src.as_slice(), dst.as_slice()],
            Command::SUnionStore { dst, .. }
            | Command::SInterStore { dst, .. }
            | Command::SDiffStore { dst, .. }
            | Command::ZUnionStore { dst, .. }
            | Command::ZInterStore { dst, .. }
            | Command::ZDiffStore { dst, .. } => vec![dst.as_slice()],

            _ => vec![],
        }
    }
}

/// The value limit reached at runtime rather than at the pre-flight check —
/// APPEND only knows the final length once it has read the existing value.
fn mem_value_too_large() -> Response {
    mem_too_long(Over::Value)
}

/// Redis's own wording for a write refused because the store is full, so a
/// client's existing OOM handling recognises it.
fn mem_out_of_memory() -> Response {
    Response::Error(
        "OOM command not allowed when used memory > 'maxmemory'. Raise \
         redis.mem_max_entries, set redis.maxmemory_policy, or use SELECT durable."
            .to_string(),
    )
}

/// Longest key the shared-memory backend stores intact. `make_key` reserves the
/// final byte of the slot for the NUL terminator.
pub const MEM_MAX_KEY: usize = crate::mem::MAX_KEY_LEN - 1;
/// Longest hash field or set/zset member the shared-memory backend stores
/// intact. Like a value, and unlike a key, this is a function of the chunk pool
/// the member spills into rather than the size of a fixed array — see
/// `mem::max_member_len`.
pub fn mem_max_member() -> usize {
    crate::mem::max_member_len()
}

/// What exceeded a shared-memory limit, if anything.
enum Over {
    Key,
    Member,
    Value,
}

fn over_key(key: &[u8]) -> Option<Over> {
    (key.len() > MEM_MAX_KEY).then_some(Over::Key)
}

fn over_keys<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Option<Over> {
    keys.into_iter().find_map(over_key)
}

fn over_members<'a>(members: impl IntoIterator<Item = &'a [u8]>) -> Option<Over> {
    members
        .into_iter()
        .find_map(|m| (m.len() > mem_max_member()).then_some(Over::Member))
}

fn over_values<'a>(values: impl IntoIterator<Item = &'a [u8]>) -> Option<Over> {
    values
        .into_iter()
        .find_map(|v| (v.len() > crate::mem::max_total_val_len()).then_some(Over::Value))
}

fn mem_too_long(over: Over) -> Response {
    let (what, limit) = match over {
        Over::Key => ("key", MEM_MAX_KEY),
        Over::Member => ("hash field or set member", mem_max_member()),
        Over::Value => ("value", crate::mem::max_total_val_len()),
    };
    Response::Error(format!(
        "ERR {what} exceeds redis.storage_mode='memory' limit of {limit} bytes \
         (use SELECT durable for unbounded values)"
    ))
}

fn mem_zrange_response(results: Vec<(Vec<u8>, Option<f64>)>) -> Response {
    let has_scores = results.first().map(|(_, s)| s.is_some()).unwrap_or(false);
    if has_scores {
        let mut out = Vec::new();
        for (m, s) in results {
            out.push(Some(m));
            if let Some(sc) = s {
                out.push(Some(format_score(sc).into_bytes()));
            }
        }
        Response::Array(out)
    } else {
        Response::Array(results.into_iter().map(|(m, _)| Some(m)).collect())
    }
}

fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn parse_score_range(start: &[u8], stop: &[u8]) -> (f64, f64, bool, bool) {
    // Score bounds are ASCII numerals on the wire; anything else is not a score.
    fn parse_one(b: &[u8]) -> (f64, bool) {
        let s = std::str::from_utf8(b).unwrap_or("");
        if let Some(rest) = s.strip_prefix('(') {
            (rest.parse().unwrap_or(f64::NEG_INFINITY), true)
        } else if s == "+inf" || s == "+Inf" {
            (f64::INFINITY, false)
        } else if s == "-inf" || s == "-Inf" {
            (f64::NEG_INFINITY, false)
        } else {
            (s.parse().unwrap_or(0.0), false)
        }
    }
    let (min, ex_min) = parse_one(start);
    let (max, ex_max) = parse_one(stop);
    (min, max, ex_min, ex_max)
}

fn parse_lex_bound_infallible(s: &[u8]) -> LexBound {
    match s {
        b"-" => LexBound::NegInf,
        b"+" => LexBound::PosInf,
        [b'[', rest @ ..] => LexBound::Inclusive(rest.to_vec()),
        [b'(', rest @ ..] => LexBound::Exclusive(rest.to_vec()),
        _ => LexBound::Inclusive(s.to_vec()),
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn build_inter_sql(db: u8, n_keys: usize, first_param: usize) -> String {
    let mut sql = String::new();
    for i in 0..n_keys {
        if i > 0 {
            sql.push_str(" INTERSECT ");
        }
        sql.push_str(&format!(
            "SELECT member FROM redis.set_{db} WHERE key = ${}",
            first_param + i
        ));
    }
    sql
}

fn list_key_lock(client: &mut SpiClient<'_>, key: &[u8]) {
    // Serialize concurrent writers on the same list key to prevent duplicate-pos conflicts.
    // Namespace 42 is reserved for pg_redis list operations.
    client
        .update(
            "SELECT pg_advisory_xact_lock(42, hashtext(encode($1, 'escape')))",
            None,
            &[key.into()],
        )
        .ok();
}

fn list_push(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    values: &[Vec<u8>],
    left: bool,
    require_exists: bool,
) -> Response {
    if values.is_empty() {
        return Response::Error("ERR wrong number of arguments".to_string());
    }
    list_key_lock(client, key);
    let edge_agg = if left { "MIN" } else { "MAX" };
    let pos_op = if left { "-" } else { "+" };
    let values_vec: Vec<Option<Vec<u8>>> = values.iter().map(|v| Some(v.clone())).collect();

    if require_exists {
        let sql = format!(
            "WITH base AS ( \
                 SELECT COALESCE({edge_agg}(pos), 0) AS p, count(*)::bigint AS n \
                 FROM redis.list_{db} WHERE key = $1 \
             ), \
             ins AS ( \
                 INSERT INTO redis.list_{db} (key, pos, value) \
                 SELECT $1, base.p {pos_op} u.ord, u.v \
                 FROM base, unnest($2::bytea[]) WITH ORDINALITY AS u(v, ord) \
                 WHERE base.n > 0 \
                 RETURNING 1 \
             ) \
             SELECT (SELECT n FROM base) \
                  + (SELECT count(*)::bigint FROM ins)"
        );
        match client.update(&sql, None, &[key.into(), values_vec.into()]) {
            Ok(tbl) => match tbl.first().get::<i64>(1) {
                Ok(Some(n)) => Response::Integer(n),
                _ => Response::Integer(0),
            },
            Err(e) => {
                eprintln!("pg_redis: list_push error: {}", e);
                Response::Error("internal error".to_string())
            }
        }
    } else {
        let sql = format!(
            "WITH base AS ( \
                 SELECT COALESCE({edge_agg}(pos), 0) AS p FROM redis.list_{db} WHERE key = $1 \
             ), \
             ins AS ( \
                 INSERT INTO redis.list_{db} (key, pos, value) \
                 SELECT $1, base.p {pos_op} u.ord, u.v \
                 FROM base, unnest($2::bytea[]) WITH ORDINALITY AS u(v, ord) \
                 RETURNING 1 \
             ) \
             SELECT (SELECT count(*)::bigint FROM redis.list_{db} WHERE key = $1) \
                  + (SELECT count(*)::bigint FROM ins)"
        );
        match client.update(&sql, None, &[key.into(), values_vec.into()]) {
            Ok(tbl) => match tbl.first().get::<i64>(1) {
                Ok(Some(n)) => Response::Integer(n),
                _ => Response::Integer(0),
            },
            Err(e) => {
                eprintln!("pg_redis: list_push error: {}", e);
                Response::Error("internal error".to_string())
            }
        }
    }
}

fn list_pop(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    count: Option<i64>,
    left: bool,
) -> Response {
    let order = if left { "ASC" } else { "DESC" };
    let limit = count.unwrap_or(1).max(0);
    if limit == 0 {
        return Response::Array(vec![]);
    }
    let sql = format!(
        "WITH d AS ( \
             SELECT key, pos FROM redis.list_{db} WHERE key = $1 \
             ORDER BY pos {order} LIMIT $2 \
             FOR UPDATE SKIP LOCKED \
         ) \
         DELETE FROM redis.list_{db} t USING d \
         WHERE t.key = d.key AND t.pos = d.pos \
         RETURNING t.value, t.pos"
    );
    match client.update(&sql, None, &[key.into(), limit.into()]) {
        Ok(tbl) => {
            let mut rows: Vec<(i64, Vec<u8>)> = Vec::new();
            for row in tbl {
                let v = row.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default();
                let p = row.get::<i64>(2).ok().flatten().unwrap_or(0);
                rows.push((p, v));
            }
            if left {
                rows.sort_by_key(|(p, _)| *p);
            } else {
                rows.sort_by_key(|b| std::cmp::Reverse(b.0));
            }
            if count.is_none() {
                match rows.into_iter().next() {
                    Some((_, v)) => Response::BulkString(v),
                    None => Response::Null,
                }
            } else if rows.is_empty() {
                Response::Null
            } else {
                Response::Array(rows.into_iter().map(|(_, v)| Some(v)).collect())
            }
        }
        Err(e) => {
            eprintln!("pg_redis: POP error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn list_insert(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    before: bool,
    pivot: &[u8],
    value: &[u8],
) -> Response {
    list_key_lock(client, key);
    let pivot_sql = format!(
        "SELECT pos FROM redis.list_{db} WHERE key = $1 AND value = $2 \
         ORDER BY pos ASC LIMIT 1"
    );
    let pivot_pos = match client.select(&pivot_sql, None, &[key.into(), pivot.into()]) {
        Ok(tbl) => match tbl.first().get::<i64>(1) {
            Ok(Some(p)) => p,
            _ => {
                let exists_sql =
                    format!("SELECT count(*)::bigint FROM redis.list_{db} WHERE key = $1");
                let n = client
                    .select(&exists_sql, None, &[key.into()])
                    .ok()
                    .and_then(|t| t.first().get::<i64>(1).ok().flatten())
                    .unwrap_or(0);
                return Response::Integer(if n == 0 { 0 } else { -1 });
            }
        },
        Err(e) => {
            eprintln!("pg_redis: LINSERT pivot lookup: {}", e);
            return Response::Error("internal error".to_string());
        }
    };

    let neighbour_sql = if before {
        format!(
            "SELECT pos FROM redis.list_{db} WHERE key = $1 AND pos < $2 \
             ORDER BY pos DESC LIMIT 1"
        )
    } else {
        format!(
            "SELECT pos FROM redis.list_{db} WHERE key = $1 AND pos > $2 \
             ORDER BY pos ASC LIMIT 1"
        )
    };
    let neighbour: Option<i64> = client
        .select(&neighbour_sql, None, &[key.into(), pivot_pos.into()])
        .ok()
        .and_then(|t| t.first().get::<i64>(1).ok().flatten());

    let new_pos = match neighbour {
        None => {
            if before {
                pivot_pos - 1024
            } else {
                pivot_pos + 1024
            }
        }
        Some(n) => {
            let lo = pivot_pos.min(n);
            let hi = pivot_pos.max(n);
            if hi - lo >= 2 {
                lo + (hi - lo) / 2
            } else {
                if let Err(e) = renumber(client, db, key) {
                    eprintln!("pg_redis: LINSERT renumber: {}", e);
                    return Response::Error("internal error".to_string());
                }
                return list_insert(client, db, key, before, pivot, value);
            }
        }
    };

    let ins_sql = format!("INSERT INTO redis.list_{db} (key, pos, value) VALUES ($1, $2, $3)");
    if let Err(e) = client.update(&ins_sql, None, &[key.into(), new_pos.into(), value.into()]) {
        eprintln!("pg_redis: LINSERT insert: {}", e);
        return Response::Error("internal error".to_string());
    }

    let len_sql = format!("SELECT count(*)::bigint FROM redis.list_{db} WHERE key = $1");
    match client.select(&len_sql, None, &[key.into()]) {
        Ok(tbl) => match tbl.first().get::<i64>(1) {
            Ok(Some(n)) => Response::Integer(n),
            _ => Response::Integer(0),
        },
        Err(_) => Response::Integer(0),
    }
}

fn renumber(client: &mut SpiClient<'_>, db: u8, key: &[u8]) -> Result<(), String> {
    let sql = format!(
        "UPDATE redis.list_{db} t SET pos = sub.new_pos \
         FROM ( \
             SELECT ctid, ROW_NUMBER() OVER (ORDER BY pos) * 1024 AS new_pos \
             FROM redis.list_{db} WHERE key = $1 \
         ) sub \
         WHERE t.ctid = sub.ctid"
    );
    client
        .update(&sql, None, &[key.into()])
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Fetch an argument that carries Redis data. Redis keys, values, fields and
/// members are arbitrary byte strings, so these must never round-trip through
/// `String`: that replaces invalid UTF-8 with U+FFFD and cannot represent NUL.
/// Parse an integer that arrived as raw bytes, rejecting non-ASCII input.
fn int_from_bytes(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn bytes_arg(args: &[Vec<u8>], idx: usize, cmd: &str) -> Result<Vec<u8>, String> {
    args.get(idx)
        .map(|a| a.to_vec())
        .ok_or_else(|| format!("{} missing argument at position {}", cmd, idx))
}

/// Fetch an argument that is genuinely textual: a number, a flag, a config name.
fn str_arg(args: &[Vec<u8>], idx: usize, cmd: &str) -> Result<String, String> {
    args.get(idx)
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .ok_or_else(|| format!("{} missing argument at position {}", cmd, idx))
}

fn update_expiry(client: &mut SpiClient<'_>, sql: &str, key: &[u8], n: i64) -> Response {
    match client.update(sql, None, &[key.into(), n.into()]) {
        Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
        Err(e) => {
            eprintln!("pg_redis: expiry update error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn get_ttl(client: &mut SpiClient<'_>, sql: &str, key: &[u8]) -> Response {
    match client.select(sql, None, &[key.into()]) {
        Ok(tbl) => match tbl.first().get::<i64>(1) {
            Ok(Some(n)) => Response::Integer(n),
            _ => Response::Integer(-2),
        },
        Err(e) => {
            eprintln!("pg_redis: TTL error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

/// Every live key in `db`, optionally filtered by a Redis glob.
///
/// The glob is applied here rather than as a SQL `LIKE`: bytea has no `LIKE`
/// operator, and the pattern is itself an arbitrary byte string. Reusing
/// `glob_match` also means KEYS, SCAN and PSUBSCRIBE agree on what a pattern
/// means, which the previous SQL translation could not guarantee.
fn matching_keys(
    client: &mut SpiClient<'_>,
    db: u8,
    pattern: Option<&[u8]>,
) -> Vec<Option<Vec<u8>>> {
    let sql = format!(
        "SELECT key FROM redis.kv_{db} \
           WHERE (expires_at IS NULL OR expires_at > now()) \
         UNION SELECT DISTINCT key FROM redis.hash_{db} \
         UNION SELECT DISTINCT key FROM redis.list_{db} \
         UNION SELECT DISTINCT key FROM redis.set_{db} \
         UNION SELECT DISTINCT key FROM redis.zset_{db}"
    );
    let mut keys: Vec<Option<Vec<u8>>> = Vec::new();
    match client.select(&sql, None, &[]) {
        Ok(tbl) => {
            for row in tbl {
                if let Ok(Some(k)) = row.get::<Vec<u8>>(1)
                    && pattern.is_none_or(|p| crate::pubsub::glob_match(p, &k))
                {
                    keys.push(Some(k));
                }
            }
        }
        Err(e) => eprintln!("pg_redis: KEYS error: {e}"),
    }
    keys
}

/// Parse a Redis score literal — accepts case-insensitive `inf`/`+inf`/`-inf`
/// as well as regular floats. Rejects NaN which Redis also rejects.
fn parse_score_value(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?;
    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => {
            let v: f64 = s.parse().ok()?;
            if v.is_nan() { None } else { Some(v) }
        }
    }
}

/// Parse a ZRANGEBYSCORE bound. `(5` = exclusive 5, `5` = inclusive 5,
/// `-inf` / `+inf` = unbounded.
fn parse_score_bound(s: &[u8]) -> Option<ScoreBound> {
    if let [b'(', rest @ ..] = s {
        parse_score_value(rest).map(|value| ScoreBound {
            value,
            exclusive: true,
        })
    } else {
        parse_score_value(s).map(|value| ScoreBound {
            value,
            exclusive: false,
        })
    }
}

/// Parse a ZRANGEBYLEX bound. `-` / `+` are sentinels; `[foo` and `(foo`
/// are inclusive/exclusive string bounds.
fn parse_lex_bound(s: &[u8]) -> Option<LexBound> {
    match s {
        b"-" => Some(LexBound::NegInf),
        b"+" => Some(LexBound::PosInf),
        [b'[', rest @ ..] => Some(LexBound::Inclusive(rest.to_vec())),
        [b'(', rest @ ..] => Some(LexBound::Exclusive(rest.to_vec())),
        _ => None,
    }
}

fn parse_zadd(args: &[Vec<u8>]) -> Result<Command, String> {
    if args.len() < 3 {
        return Err("ZADD requires key [options] score member [score member ...]".to_string());
    }
    let key = bytes_arg(args, 0, "ZADD")?;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;
    let mut i = 1;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GT" => {
                gt = true;
                i += 1;
            }
            "LT" => {
                lt = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            "INCR" => {
                incr = true;
                i += 1;
            }
            _ => break,
        }
    }
    if nx && xx {
        return Err("ZADD XX and NX options at the same time are not compatible".to_string());
    }
    if gt && lt {
        return Err(
            "ZADD GT, LT, and/or NX options at the same time are not compatible".to_string(),
        );
    }
    if nx && (gt || lt) {
        return Err(
            "ZADD GT, LT, and/or NX options at the same time are not compatible".to_string(),
        );
    }
    let remaining = &args[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Err("ZADD requires score/member pairs".to_string());
    }
    let mut pairs = Vec::with_capacity(remaining.len() / 2);
    for chunk in remaining.chunks(2) {
        let score = parse_score_value(&chunk[0])
            .ok_or_else(|| "ZADD score is not a valid float".to_string())?;
        let member = chunk[1].to_vec();
        pairs.push((score, member));
    }
    if incr && pairs.len() != 1 {
        return Err("INCR option supports a single increment-element pair".to_string());
    }
    Ok(Command::ZAdd {
        key,
        nx,
        xx,
        gt,
        lt,
        ch,
        incr,
        pairs,
    })
}

fn parse_zrank(args: &[Vec<u8>], rev: bool) -> Result<Command, String> {
    let cmd_name = if rev { "ZREVRANK" } else { "ZRANK" };
    let key = bytes_arg(args, 0, cmd_name)?;
    let member = bytes_arg(args, 1, cmd_name)?;
    let with_score = args
        .get(2)
        .map(|a| String::from_utf8_lossy(a).to_uppercase() == "WITHSCORE")
        .unwrap_or(false);
    Ok(Command::ZRank {
        key,
        member,
        rev,
        with_score,
    })
}

fn parse_zrange(args: &[Vec<u8>], _rev_default: bool) -> Result<Command, String> {
    if args.len() < 3 {
        return Err("ZRANGE requires key start stop".to_string());
    }
    let key = bytes_arg(args, 0, "ZRANGE")?;
    let start = bytes_arg(args, 1, "ZRANGE")?;
    let stop = bytes_arg(args, 2, "ZRANGE")?;
    let mut by = RangeBy::Index;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut with_scores = false;
    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "BYSCORE" => {
                by = RangeBy::Score;
                i += 1;
            }
            "BYLEX" => {
                by = RangeBy::Lex;
                i += 1;
            }
            "REV" => {
                rev = true;
                i += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                let off: i64 = str_arg(args, i + 1, "ZRANGE LIMIT")?
                    .parse()
                    .map_err(|_| "ZRANGE LIMIT offset must be integer".to_string())?;
                let cnt: i64 = str_arg(args, i + 2, "ZRANGE LIMIT")?
                    .parse()
                    .map_err(|_| "ZRANGE LIMIT count must be integer".to_string())?;
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return Err(format!("ZRANGE unknown option: {}", opt)),
        }
    }
    if by == RangeBy::Lex && with_scores {
        return Err("ZRANGE BYLEX does not support WITHSCORES".to_string());
    }
    Ok(Command::ZRange {
        key,
        start,
        stop,
        by,
        rev,
        limit,
        with_scores,
    })
}

fn parse_zrangebyscore(args: &[Vec<u8>], rev: bool) -> Result<Command, String> {
    let cmd_name = if rev {
        "ZREVRANGEBYSCORE"
    } else {
        "ZRANGEBYSCORE"
    };
    let key = bytes_arg(args, 0, cmd_name)?;
    let (min_raw, max_raw) = if rev {
        (bytes_arg(args, 2, cmd_name)?, bytes_arg(args, 1, cmd_name)?)
    } else {
        (bytes_arg(args, 1, cmd_name)?, bytes_arg(args, 2, cmd_name)?)
    };
    let min = parse_score_bound(&min_raw)
        .ok_or_else(|| format!("{} min is not a valid float", cmd_name))?;
    let max = parse_score_bound(&max_raw)
        .ok_or_else(|| format!("{} max is not a valid float", cmd_name))?;
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                let off: i64 = str_arg(args, i + 1, cmd_name)?
                    .parse()
                    .map_err(|_| format!("{} LIMIT offset must be integer", cmd_name))?;
                let cnt: i64 = str_arg(args, i + 2, cmd_name)?
                    .parse()
                    .map_err(|_| format!("{} LIMIT count must be integer", cmd_name))?;
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return Err(format!("{} unknown option: {}", cmd_name, opt)),
        }
    }
    Ok(Command::ZRangeByScore {
        key,
        min,
        max,
        rev,
        with_scores,
        limit,
    })
}

fn parse_zrangebylex(args: &[Vec<u8>], rev: bool) -> Result<Command, String> {
    let cmd_name = if rev { "ZREVRANGEBYLEX" } else { "ZRANGEBYLEX" };
    let key = bytes_arg(args, 0, cmd_name)?;
    let (min_raw, max_raw) = if rev {
        (bytes_arg(args, 2, cmd_name)?, bytes_arg(args, 1, cmd_name)?)
    } else {
        (bytes_arg(args, 1, cmd_name)?, bytes_arg(args, 2, cmd_name)?)
    };
    let min = parse_lex_bound(&min_raw).ok_or_else(|| format!("{} min is invalid", cmd_name))?;
    let max = parse_lex_bound(&max_raw).ok_or_else(|| format!("{} max is invalid", cmd_name))?;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "LIMIT" => {
                let off: i64 = str_arg(args, i + 1, cmd_name)?
                    .parse()
                    .map_err(|_| format!("{} LIMIT offset must be integer", cmd_name))?;
                let cnt: i64 = str_arg(args, i + 2, cmd_name)?
                    .parse()
                    .map_err(|_| format!("{} LIMIT count must be integer", cmd_name))?;
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return Err(format!("{} unknown option: {}", cmd_name, opt)),
        }
    }
    Ok(Command::ZRangeByLex {
        key,
        min,
        max,
        rev,
        limit,
    })
}

fn parse_zaggregate(args: &[Vec<u8>], is_inter: bool, _is_store: bool) -> Result<Command, String> {
    let cmd_name = if is_inter { "ZINTER" } else { "ZUNION" };
    if args.is_empty() {
        return Err(format!("{} requires numkeys", cmd_name));
    }
    let numkeys: usize = str_arg(args, 0, cmd_name)?
        .parse()
        .map_err(|_| format!("{} numkeys must be integer", cmd_name))?;
    if numkeys == 0 {
        return Err(format!("{} numkeys must be positive", cmd_name));
    }
    if args.len() < 1 + numkeys {
        return Err(format!("{} numkeys exceeds provided keys", cmd_name));
    }
    let keys = (0..numkeys)
        .map(|i| bytes_arg(args, 1 + i, cmd_name))
        .collect::<Result<Vec<_>, _>>()?;
    let (weights, aggregate, with_scores) =
        parse_aggregate_opts(args, 1 + numkeys, numkeys, cmd_name)?;
    if is_inter {
        Ok(Command::ZInter {
            keys,
            weights,
            aggregate,
            with_scores,
        })
    } else {
        Ok(Command::ZUnion {
            keys,
            weights,
            aggregate,
            with_scores,
        })
    }
}

fn parse_zdiff(args: &[Vec<u8>], _is_store: bool) -> Result<Command, String> {
    if args.is_empty() {
        return Err("ZDIFF requires numkeys".to_string());
    }
    let numkeys: usize = str_arg(args, 0, "ZDIFF")?
        .parse()
        .map_err(|_| "ZDIFF numkeys must be integer".to_string())?;
    if numkeys == 0 {
        return Err("ZDIFF numkeys must be positive".to_string());
    }
    if args.len() < 1 + numkeys {
        return Err("ZDIFF numkeys exceeds provided keys".to_string());
    }
    let keys = (0..numkeys)
        .map(|i| bytes_arg(args, 1 + i, "ZDIFF"))
        .collect::<Result<Vec<_>, _>>()?;
    let with_scores = args
        .get(1 + numkeys)
        .map(|a| String::from_utf8_lossy(a).to_uppercase() == "WITHSCORES")
        .unwrap_or(false);
    Ok(Command::ZDiff { keys, with_scores })
}

fn parse_zaggregate_store(args: &[Vec<u8>], is_inter: bool) -> Result<Command, String> {
    let cmd_name = if is_inter {
        "ZINTERSTORE"
    } else {
        "ZUNIONSTORE"
    };
    if args.len() < 3 {
        return Err(format!(
            "{} requires destination numkeys key [key ...]",
            cmd_name
        ));
    }
    let dst = bytes_arg(args, 0, cmd_name)?;
    let numkeys: usize = str_arg(args, 1, cmd_name)?
        .parse()
        .map_err(|_| format!("{} numkeys must be integer", cmd_name))?;
    if numkeys == 0 {
        return Err(format!("{} numkeys must be positive", cmd_name));
    }
    if args.len() < 2 + numkeys {
        return Err(format!("{} numkeys exceeds provided keys", cmd_name));
    }
    let keys = (0..numkeys)
        .map(|i| bytes_arg(args, 2 + i, cmd_name))
        .collect::<Result<Vec<_>, _>>()?;
    let (weights, aggregate, _ws) = parse_aggregate_opts(args, 2 + numkeys, numkeys, cmd_name)?;
    if is_inter {
        Ok(Command::ZInterStore {
            dst,
            keys,
            weights,
            aggregate,
        })
    } else {
        Ok(Command::ZUnionStore {
            dst,
            keys,
            weights,
            aggregate,
        })
    }
}

fn parse_aggregate_opts(
    args: &[Vec<u8>],
    start: usize,
    numkeys: usize,
    cmd_name: &str,
) -> Result<(Option<Vec<f64>>, Aggregate, bool), String> {
    let mut weights: Option<Vec<f64>> = None;
    let mut aggregate = Aggregate::Sum;
    let mut with_scores = false;
    let mut i = start;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= args.len() {
                    return Err(format!("{} WEIGHTS requires numkeys values", cmd_name));
                }
                let mut w = Vec::with_capacity(numkeys);
                for j in 0..numkeys {
                    let v = parse_score_value(&args[i + 1 + j]).ok_or_else(|| {
                        format!("{} WEIGHTS value is not a valid float", cmd_name)
                    })?;
                    w.push(v);
                }
                weights = Some(w);
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                let ag = str_arg(args, i + 1, cmd_name)?.to_uppercase();
                aggregate = match ag.as_str() {
                    "SUM" => Aggregate::Sum,
                    "MIN" => Aggregate::Min,
                    "MAX" => Aggregate::Max,
                    _ => return Err(format!("{} AGGREGATE must be SUM|MIN|MAX", cmd_name)),
                };
                i += 2;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            _ => return Err(format!("{} unknown option: {}", cmd_name, opt)),
        }
    }
    Ok((weights, aggregate, with_scores))
}

/// Format a score the way Redis does: integer-valued doubles without a
/// decimal point, special values as `inf` / `-inf`, otherwise round-trippable
/// shortest representation.
fn format_score(s: f64) -> String {
    if s.is_nan() {
        return "nan".to_string();
    }
    if s.is_infinite() {
        return if s > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    if s == s.trunc() && s.abs() < 1e16 {
        return format!("{}", s as i64);
    }
    format!("{}", s)
}

fn score_ge_op(b: &ScoreBound) -> &'static str {
    if b.exclusive { ">" } else { ">=" }
}
fn score_le_op(b: &ScoreBound) -> &'static str {
    if b.exclusive { "<" } else { "<=" }
}

/// Build the lex `AND member ...` where-clause fragment plus the string
/// arguments that must be bound, starting at `$next`. `-` / `+` sentinels
/// do not need a bind value.
fn lex_where(min: &LexBound, max: &LexBound, mut next: usize) -> (String, Vec<Vec<u8>>) {
    let mut clause = String::new();
    let mut args = Vec::new();
    match min {
        LexBound::NegInf => {}
        LexBound::PosInf => clause.push_str(" AND false"),
        LexBound::Inclusive(s) => {
            clause.push_str(&format!(" AND member >= ${}", next));
            args.push(s.clone());
            next += 1;
        }
        LexBound::Exclusive(s) => {
            clause.push_str(&format!(" AND member > ${}", next));
            args.push(s.clone());
            next += 1;
        }
    }
    match max {
        LexBound::PosInf => {}
        LexBound::NegInf => clause.push_str(" AND false"),
        LexBound::Inclusive(s) => {
            clause.push_str(&format!(" AND member <= ${}", next));
            args.push(s.clone());
        }
        LexBound::Exclusive(s) => {
            clause.push_str(&format!(" AND member < ${}", next));
            args.push(s.clone());
        }
    }
    (clause, args)
}

fn run_count(client: &mut SpiClient<'_>, sql: &str, args: &[DatumWithOid], cmd: &str) -> Response {
    match client.update(sql, None, args) {
        Ok(tbl) => match tbl.first().get::<i64>(1) {
            Ok(Some(n)) => Response::Integer(n),
            _ => Response::Integer(0),
        },
        Err(e) => {
            eprintln!("pg_redis: {} error: {}", cmd, e);
            Response::Error("internal error".to_string())
        }
    }
}

struct ZAddFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
    incr: bool,
}

fn zadd_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    flags: ZAddFlags,
    pairs: &[(f64, Vec<u8>)],
) -> Response {
    let ZAddFlags {
        nx,
        xx,
        gt,
        lt,
        ch,
        incr,
    } = flags;
    if incr {
        let (inc, member) = &pairs[0];
        return zadd_incr(
            client,
            db,
            key,
            *inc,
            member,
            ZAddIncrFlags { nx, xx, gt, lt },
        );
    }
    let members: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, m)| Some(m.clone())).collect();
    let scores: Vec<Option<f64>> = pairs.iter().map(|(s, _)| Some(*s)).collect();
    let sql = zadd_sql(
        db,
        &ZAddFlags {
            nx,
            xx,
            gt,
            lt,
            ch,
            incr,
        },
    );
    match client.update(&sql, None, &[key.into(), members.into(), scores.into()]) {
        Ok(tbl) => {
            let row = tbl.first();
            let added = row.get::<i64>(1).ok().flatten().unwrap_or(0);
            let changed = row.get::<i64>(2).ok().flatten().unwrap_or(0);
            if ch {
                Response::Integer(added + changed)
            } else {
                Response::Integer(added)
            }
        }
        Err(e) => {
            eprintln!("pg_redis: ZADD error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn zadd_sql(db: u8, flags: &ZAddFlags) -> String {
    let ZAddFlags {
        nx, xx, gt, lt, ch, ..
    } = *flags;
    let cmp = if gt {
        Some(">")
    } else if lt {
        Some("<")
    } else {
        None
    };
    if nx {
        format!(
            "WITH input AS (SELECT u.m AS member, u.s AS score FROM unnest($2::bytea[], $3::float8[]) AS u(m, s)), \
                  ins AS ( \
                    INSERT INTO redis.zset_{db} (key, member, score) \
                    SELECT $1, member, score FROM input \
                    ON CONFLICT (key, member) DO NOTHING \
                    RETURNING 1 \
                  ) \
             SELECT count(*)::bigint AS added, 0::bigint AS changed FROM ins"
        )
    } else if xx {
        let where_cmp = cmp
            .map(|c| format!(" AND i.score {} z.score", c))
            .unwrap_or_default();
        format!(
            "WITH input AS (SELECT u.m AS member, u.s AS score FROM unnest($2::bytea[], $3::float8[]) AS u(m, s)), \
                  existing AS (SELECT member, score FROM redis.zset_{db} \
                               WHERE key = $1 AND member = ANY($2::bytea[])), \
                  upd AS ( \
                    UPDATE redis.zset_{db} z SET score = i.score \
                    FROM input i \
                    WHERE z.key = $1 AND z.member = i.member{where_cmp} \
                    RETURNING z.member, i.score AS new_score \
                  ) \
             SELECT 0::bigint AS added, \
                    (SELECT count(*) FILTER (WHERE e.score IS DISTINCT FROM u.new_score)::bigint \
                     FROM upd u LEFT JOIN existing e ON e.member = u.member) AS changed"
        )
    } else if !ch && !gt && !lt {
        format!(
            "WITH input AS (SELECT u.m AS member, u.s AS score FROM unnest($2::bytea[], $3::float8[]) AS u(m, s)), \
                  ups AS ( \
                    INSERT INTO redis.zset_{db} (key, member, score) \
                    SELECT $1, member, score FROM input \
                    ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score \
                    RETURNING (xmax = 0)::int AS inserted \
                  ) \
             SELECT count(*) FILTER (WHERE inserted = 1)::bigint AS added, 0::bigint AS changed FROM ups"
        )
    } else {
        let where_cmp = cmp
            .map(|c| format!(" WHERE EXCLUDED.score {} redis.zset_{db}.score", c))
            .unwrap_or_default();
        format!(
            "WITH input AS (SELECT u.m AS member, u.s AS score FROM unnest($2::bytea[], $3::float8[]) AS u(m, s)), \
                  existing AS (SELECT member, score FROM redis.zset_{db} \
                               WHERE key = $1 AND member = ANY($2::bytea[])), \
                  ups AS ( \
                    INSERT INTO redis.zset_{db} (key, member, score) \
                    SELECT $1, member, score FROM input \
                    ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score{where_cmp} \
                    RETURNING member, score \
                  ) \
             SELECT (SELECT count(*) FILTER (WHERE e.member IS NULL)::bigint \
                     FROM ups u LEFT JOIN existing e ON e.member = u.member) AS added, \
                    (SELECT count(*) FILTER (WHERE e.score IS DISTINCT FROM u.score AND e.member IS NOT NULL)::bigint \
                     FROM ups u LEFT JOIN existing e ON e.member = u.member) AS changed"
        )
    }
}

struct ZAddIncrFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

fn zadd_incr(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    increment: f64,
    member: &[u8],
    flags: ZAddIncrFlags,
) -> Response {
    let ZAddIncrFlags { nx, xx, gt, lt } = flags;
    let existing_sql = format!("SELECT score FROM redis.zset_{db} WHERE key = $1 AND member = $2");
    let existing = match client.select(&existing_sql, None, &[key.into(), member.into()]) {
        Ok(tbl) => tbl.first().get::<f64>(1).ok().flatten(),
        Err(e) => {
            eprintln!("pg_redis: ZADD INCR lookup error: {}", e);
            return Response::Error("internal error".to_string());
        }
    };
    match (existing, nx, xx) {
        (Some(_), true, _) => return Response::Null,
        (None, _, true) => return Response::Null,
        _ => {}
    }
    let new_score = existing.unwrap_or(0.0) + increment;
    if new_score.is_nan() {
        return Response::Error("ERR resulting score is not a number (NaN)".to_string());
    }
    if let Some(old) = existing {
        if gt && new_score <= old {
            return Response::Null;
        }
        if lt && new_score >= old {
            return Response::Null;
        }
    }
    let sql = format!(
        "INSERT INTO redis.zset_{db} (key, member, score) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score \
         RETURNING score"
    );
    match client.update(&sql, None, &[key.into(), member.into(), new_score.into()]) {
        Ok(tbl) => match tbl.first().get::<f64>(1) {
            Ok(Some(s)) => Response::BulkString(format_score(s).into_bytes()),
            _ => Response::Null,
        },
        Err(e) => {
            eprintln!("pg_redis: ZADD INCR error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn zrank_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    member: &[u8],
    rev: bool,
    with_score: bool,
) -> Response {
    let rank_cond = if rev {
        "score > t.score OR (score = t.score AND member > $2)"
    } else {
        "score < t.score OR (score = t.score AND member < $2)"
    };
    let sql = format!(
        "WITH t AS (SELECT score FROM redis.zset_{db} WHERE key = $1 AND member = $2) \
         SELECT t.score, \
                (SELECT count(*)::bigint FROM redis.zset_{db} \
                 WHERE key = $1 AND ({rank_cond})) AS rank \
         FROM t"
    );
    match client.select(&sql, None, &[key.into(), member.into()]) {
        Ok(tbl) => {
            let row = tbl.first();
            let score = match row.get::<f64>(1) {
                Ok(Some(s)) => s,
                _ => return Response::Null,
            };
            let rank = row.get::<i64>(2).ok().flatten().unwrap_or(0);
            if with_score {
                Response::Array(vec![
                    Some(rank.to_string().into_bytes()),
                    Some(format_score(score).into_bytes()),
                ])
            } else {
                Response::Integer(rank)
            }
        }
        Err(e) => {
            eprintln!("pg_redis: ZRANK error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

struct ZRangeOptions {
    by: RangeBy,
    rev: bool,
    limit: Option<(i64, i64)>,
    with_scores: bool,
}

fn zrange_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    start: &[u8],
    stop: &[u8],
    opts: ZRangeOptions,
) -> Response {
    let ZRangeOptions {
        by,
        rev,
        limit,
        with_scores,
    } = opts;
    match by {
        RangeBy::Index => {
            let s: i64 = match int_from_bytes(start) {
                Some(v) => v,
                None => return Response::Error("ZRANGE start must be integer".to_string()),
            };
            let e: i64 = match int_from_bytes(stop) {
                Some(v) => v,
                None => return Response::Error("ZRANGE stop must be integer".to_string()),
            };
            zrange_by_index(client, db, key, s, e, rev, with_scores)
        }
        RangeBy::Score => {
            let (min_raw, max_raw) = if rev { (stop, start) } else { (start, stop) };
            let min = match parse_score_bound(min_raw) {
                Some(b) => b,
                None => return Response::Error("min is not a valid float".to_string()),
            };
            let max = match parse_score_bound(max_raw) {
                Some(b) => b,
                None => return Response::Error("max is not a valid float".to_string()),
            };
            zrange_by_score_execute(
                client,
                db,
                key,
                min,
                max,
                ZRangeByScoreOptions {
                    rev,
                    with_scores,
                    limit,
                },
            )
        }
        RangeBy::Lex => {
            let (min_raw, max_raw) = if rev { (stop, start) } else { (start, stop) };
            let min = match parse_lex_bound(min_raw) {
                Some(b) => b,
                None => return Response::Error("min is invalid".to_string()),
            };
            let max = match parse_lex_bound(max_raw) {
                Some(b) => b,
                None => return Response::Error("max is invalid".to_string()),
            };
            zrange_by_lex_execute(client, db, key, &min, &max, rev, limit)
        }
    }
}

fn zrange_by_index(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
) -> Response {
    let order = if rev { "DESC" } else { "ASC" };
    let sql = format!(
        "WITH cnt AS (SELECT count(*)::bigint AS n FROM redis.zset_{db} WHERE key = $1), \
         nrm AS ( \
             SELECT \
                 CASE WHEN $2::bigint < 0 THEN GREATEST(0, n + $2::bigint) ELSE LEAST($2::bigint, n) END AS s, \
                 LEAST(CASE WHEN $3::bigint < 0 THEN n + $3::bigint ELSE $3::bigint END, GREATEST(n - 1, 0)) AS e \
             FROM cnt \
         ) \
         SELECT member, score FROM redis.zset_{db}, nrm \
         WHERE key = $1 \
           AND (SELECT n FROM cnt) > 0 \
           AND (SELECT s FROM nrm) <= (SELECT e FROM nrm) \
         ORDER BY score {order}, member {order} \
         OFFSET (SELECT s FROM nrm) \
         LIMIT (SELECT GREATEST(0, e - s + 1) FROM nrm)"
    );
    zrange_collect(
        client,
        &sql,
        &[key.into(), start.into(), stop.into()],
        with_scores,
        "ZRANGE",
    )
}

struct ZRangeByScoreOptions {
    rev: bool,
    with_scores: bool,
    limit: Option<(i64, i64)>,
}

fn zrange_by_score_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    min: ScoreBound,
    max: ScoreBound,
    opts: ZRangeByScoreOptions,
) -> Response {
    let ZRangeByScoreOptions {
        rev,
        with_scores,
        limit,
    } = opts;
    let (min_op, max_op) = (score_ge_op(&min), score_le_op(&max));
    let order = if rev { "DESC" } else { "ASC" };
    let (offset, lim) = limit.unwrap_or((0, -1));
    let lim_sql = if lim < 0 {
        "ALL".to_string()
    } else {
        "$5".to_string()
    };
    let sql = format!(
        "SELECT member, score FROM redis.zset_{db} \
         WHERE key = $1 AND score {min_op} $2 AND score {max_op} $3 \
         ORDER BY score {order}, member {order} \
         OFFSET $4 LIMIT {lim_sql}"
    );
    let result = if lim < 0 {
        client.select(
            &sql,
            None,
            &[
                key.into(),
                min.value.into(),
                max.value.into(),
                offset.into(),
            ],
        )
    } else {
        client.select(
            &sql,
            None,
            &[
                key.into(),
                min.value.into(),
                max.value.into(),
                offset.into(),
                lim.into(),
            ],
        )
    };
    match result {
        Ok(tbl) => collect_member_score(tbl, with_scores),
        Err(e) => {
            eprintln!("pg_redis: ZRANGEBYSCORE error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn zrange_by_lex_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    min: &LexBound,
    max: &LexBound,
    rev: bool,
    limit: Option<(i64, i64)>,
) -> Response {
    let (where_clause, extra_args) = lex_where(min, max, 2);
    let order = if rev { "DESC" } else { "ASC" };
    let (offset, lim) = limit.unwrap_or((0, -1));
    let mut args: Vec<DatumWithOid> = Vec::new();
    args.push(key.into());
    for a in extra_args.iter() {
        args.push(a.as_slice().into());
    }
    let offset_param = format!("${}", args.len() + 1);
    args.push(offset.into());
    let lim_sql = if lim < 0 {
        "ALL".to_string()
    } else {
        let p = format!("${}", args.len() + 1);
        args.push(lim.into());
        p
    };
    let sql = format!(
        "SELECT member, score FROM redis.zset_{db} \
         WHERE key = $1{where_clause} \
         ORDER BY member {order} \
         OFFSET {offset_param} LIMIT {lim_sql}"
    );
    match client.select(&sql, None, &args) {
        Ok(tbl) => collect_member_score(tbl, false),
        Err(e) => {
            eprintln!("pg_redis: ZRANGEBYLEX error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn zrange_collect(
    client: &mut SpiClient<'_>,
    sql: &str,
    args: &[DatumWithOid],
    with_scores: bool,
    cmd: &str,
) -> Response {
    match client.select(sql, None, args) {
        Ok(tbl) => collect_member_score(tbl, with_scores),
        Err(e) => {
            eprintln!("pg_redis: {} error: {}", cmd, e);
            Response::Error("internal error".to_string())
        }
    }
}

fn collect_member_score(tbl: pgrx::spi::SpiTupleTable<'_>, with_scores: bool) -> Response {
    let mut out: Vec<Option<Vec<u8>>> = Vec::new();
    for row in tbl {
        let member: Vec<u8> = row.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default();
        out.push(Some(member));
        if with_scores {
            let score = row.get::<f64>(2).ok().flatten().unwrap_or(0.0);
            out.push(Some(format_score(score).into_bytes()));
        }
    }
    Response::Array(out)
}

fn zpop_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    count: Option<i64>,
    min: bool,
) -> Response {
    let limit = count.unwrap_or(1).max(0);
    if limit == 0 {
        return Response::Array(vec![]);
    }
    let order = if min { "ASC" } else { "DESC" };
    let sql = format!(
        "WITH popped AS ( \
             SELECT key, member FROM redis.zset_{db} WHERE key = $1 \
             ORDER BY score {order}, member {order} \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED \
         ) \
         DELETE FROM redis.zset_{db} z USING popped p \
         WHERE z.key = p.key AND z.member = p.member \
         RETURNING z.member, z.score"
    );
    match client.update(&sql, None, &[key.into(), limit.into()]) {
        Ok(tbl) => {
            let mut rows: Vec<(f64, Vec<u8>)> = Vec::new();
            for row in tbl {
                let m = row.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default();
                let s = row.get::<f64>(2).ok().flatten().unwrap_or(0.0);
                rows.push((s, m));
            }
            if min {
                rows.sort_by(|a, b| {
                    a.0.partial_cmp(&b.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.1.cmp(&b.1))
                });
            } else {
                rows.sort_by(|a, b| {
                    b.0.partial_cmp(&a.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.1.cmp(&a.1))
                });
            }
            let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(rows.len() * 2);
            for (s, m) in rows {
                out.push(Some(m));
                out.push(Some(format_score(s).into_bytes()));
            }
            Response::Array(out)
        }
        Err(e) => {
            eprintln!("pg_redis: ZPOP error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn zrandmember_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    key: &[u8],
    count: Option<i64>,
    with_scores: bool,
) -> Response {
    let (want_array, allow_dup, limit) = match count {
        None => (false, false, 1i64),
        Some(n) if n >= 0 => (true, false, n),
        Some(n) => (true, true, -n),
    };
    if want_array && limit == 0 {
        return Response::Array(vec![]);
    }
    let sql = if allow_dup {
        format!(
            "WITH cnt AS (SELECT count(*)::int AS c FROM redis.zset_{db} WHERE key = $1) \
             SELECT p.member, p.score \
             FROM cnt, generate_series(1, $2::int) g, \
                  LATERAL (SELECT member, score FROM redis.zset_{db} \
                           WHERE key = $1 \
                           OFFSET floor(random() * cnt.c)::int LIMIT 1) p \
             WHERE cnt.c > 0"
        )
    } else {
        format!(
            "SELECT member, score FROM redis.zset_{db} WHERE key = $1 \
             ORDER BY random() LIMIT $2"
        )
    };
    match client.select(&sql, None, &[key.into(), limit.into()]) {
        Ok(tbl) => {
            let mut out: Vec<Option<Vec<u8>>> = Vec::new();
            let mut first_member: Option<Vec<u8>> = None;
            for row in tbl {
                let m = row.get::<Vec<u8>>(1).ok().flatten().unwrap_or_default();
                if first_member.is_none() {
                    first_member = Some(m.clone());
                }
                out.push(Some(m));
                if with_scores {
                    let s = row.get::<f64>(2).ok().flatten().unwrap_or(0.0);
                    out.push(Some(format_score(s).into_bytes()));
                }
            }
            if want_array {
                Response::Array(out)
            } else {
                match first_member {
                    Some(m) => Response::BulkString(m),
                    None => Response::Null,
                }
            }
        }
        Err(e) => {
            eprintln!("pg_redis: ZRANDMEMBER error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

#[derive(Clone, Copy)]
enum AggOp {
    Union,
    Inter,
    Diff,
}

struct ZAggregateOptions<'a> {
    aggregate: Aggregate,
    with_scores: bool,
    op: AggOp,
    store_into: Option<&'a [u8]>,
}

fn zaggregate_execute(
    client: &mut SpiClient<'_>,
    db: u8,
    keys: &[Vec<u8>],
    weights: Option<&[f64]>,
    opts: ZAggregateOptions<'_>,
) -> Response {
    let ZAggregateOptions {
        aggregate,
        with_scores,
        op,
        store_into,
    } = opts;
    if keys.is_empty() {
        return match store_into {
            Some(_) => Response::Integer(0),
            None => Response::Array(vec![]),
        };
    }
    let weights_vec: Vec<f64> = weights
        .map(|w| w.to_vec())
        .unwrap_or_else(|| vec![1.0; keys.len()]);
    if weights_vec.len() != keys.len() {
        return Response::Error("ERR WEIGHTS count does not match numkeys".to_string());
    }
    let agg_fn = match aggregate {
        Aggregate::Sum => "SUM(w_score)",
        Aggregate::Min => "MIN(w_score)",
        Aggregate::Max => "MAX(w_score)",
    };
    let body = match op {
        AggOp::Union => format!(
            "SELECT member, {agg_fn} AS score FROM ( \
                 SELECT z.member, z.score * kw.w AS w_score \
                 FROM unnest($1::bytea[], $2::float8[]) WITH ORDINALITY AS kw(k, w, ord) \
                 JOIN redis.zset_{db} z ON z.key = kw.k \
             ) t \
             GROUP BY member"
        ),
        AggOp::Inter => format!(
            "SELECT member, {agg_fn} AS score FROM ( \
                 SELECT z.member, z.score * kw.w AS w_score \
                 FROM unnest($1::bytea[], $2::float8[]) WITH ORDINALITY AS kw(k, w, ord) \
                 JOIN redis.zset_{db} z ON z.key = kw.k \
             ) t \
             GROUP BY member \
             HAVING count(*) = array_length($1::bytea[], 1)"
        ),
        AggOp::Diff => format!(
            "SELECT z.member, z.score FROM redis.zset_{db} z \
             WHERE z.key = ($1::bytea[])[1] \
               AND NOT EXISTS ( \
                   SELECT 1 FROM redis.zset_{db} z2 \
                   WHERE z2.member = z.member \
                     AND z2.key = ANY(($1::bytea[])[2:array_length($1::bytea[], 1)]) \
               )"
        ),
    };

    let keys_vec: Vec<Option<Vec<u8>>> = keys.iter().map(|k| Some(k.clone())).collect();
    let weights_opt: Vec<Option<f64>> = weights_vec.iter().map(|w| Some(*w)).collect();

    if let Some(dst) = store_into {
        // DELETE targets old-only members (not in new_data); INSERT covers the
        // new ones. Disjoint target sets eliminate CTE ordering hazards; the
        // ON CONFLICT clause handles dst already containing members from the
        // new result (e.g. when dst is also an input key). Data-modifying
        // CTEs always execute to completion, so del and ins fire even though
        // only ins is read back for the card count.
        let sql = format!(
            "WITH new_data AS ({body}), \
                  del AS ( \
                    DELETE FROM redis.zset_{db} \
                    WHERE key = $3 AND member NOT IN (SELECT member FROM new_data) \
                    RETURNING 1 \
                  ), \
                  ins AS ( \
                    INSERT INTO redis.zset_{db} (key, member, score) \
                    SELECT $3, member, score FROM new_data \
                    ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score \
                    RETURNING 1 \
                  ) \
             SELECT count(*)::bigint FROM ins"
        );
        run_count(
            client,
            &sql,
            &[keys_vec.into(), weights_opt.into(), dst.into()],
            "ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE",
        )
    } else {
        let sql = format!(
            "SELECT member, score FROM ({body}) agg \
             ORDER BY score ASC, member ASC"
        );
        match client.select(&sql, None, &[keys_vec.into(), weights_opt.into()]) {
            Ok(tbl) => collect_member_score(tbl, with_scores),
            Err(e) => {
                eprintln!("pg_redis: ZUNION/ZINTER/ZDIFF error: {}", e);
                Response::Error("internal error".to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(input: &[&str]) -> Vec<Vec<u8>> {
        input.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn is_ok(cmd: Result<Command, String>) -> Command {
        cmd.unwrap_or_else(|e| panic!("expected Ok, got Err: {e}"))
    }

    fn is_err(cmd: Result<Command, String>) -> String {
        assert!(cmd.is_err(), "expected Err but got Ok");
        cmd.unwrap_err()
    }

    /// `input => expected pattern`, checked with `matches!`. Command-name
    /// dispatch and arity are what these cases pin down; anything that also
    /// needs field values gets its own test further down.
    macro_rules! parses_to {
        ($($input:expr => $expected:pat $(if $guard:expr)?),* $(,)?) => {
            $(
                let cmd = is_ok(Command::parse(parts($input)));
                // Rendered up front: guard patterns bind by move.
                let actual = format!("{cmd:?}");
                assert!(
                    matches!(cmd, $expected $(if $guard)?),
                    "{:?} parsed as {}, expected {}",
                    $input,
                    actual,
                    stringify!($expected $(if $guard)?)
                );
            )*
        };
    }

    /// Every command name the server accepts, and the variant it maps to.
    #[test]
    fn command_names_map_to_their_variants() {
        parses_to! {
        // Connection commands
        &["PING"] => Command::Ping { msg: None },
        &["PING", "hello"] => Command::Ping { msg: Some(_) },
        &["ECHO", "world"] => Command::Echo { .. },
        &["SELECT", "0"] => Command::Select { db: 0 },
        &["SELECT", "1"] => Command::Select { db: 1 },
        &["SELECT", "15"] => Command::Select { db: 15 },
        // Half aliases, case-insensitive like every other keyword.
        &["SELECT", "cache"] => Command::Select { db: CACHE_DB },
        &["SELECT", "DURABLE"] => Command::Select { db: DURABLE_DB },
        &["AUTH", "secret"] => Command::Auth { password } if password == "secret",
        &["AUTH", "user", "mypass"] => Command::Auth { password } if password == "mypass",
        &["INFO"] => Command::Info,
        // CLIENT subcommand parsing
        &["CLIENT", "ID"] => Command::ClientId,
        &["CLIENT", "GETNAME"] => Command::ClientGetname,
        &["CLIENT", "SETNAME", "myconn"] => Command::ClientSetname { _name } if _name == "myconn",
        &[ "CLIENT", "SETINFO", "lib-name", "redis-rs", ] => Command::ClientSetinfo,
        &["CLIENT", "LIST"] => Command::ClientList,
        &["CLIENT", "INFO"] => Command::ClientInfo,
        &["CLIENT", "NO-EVICT", "on"] => Command::ClientNoEvict,
        &["CLIENT", "NO-TOUCH", "on"] => Command::ClientNoTouch,
        &["CLIENT", "UNPAUSE"] => Command::ClientOther,
        &["CLIENT"] => Command::ClientOther,
        // COMMAND subcommand parsing
        &["COMMAND"] => Command::CmdOther,
        &["COMMAND", "COUNT"] => Command::CmdCount,
        &["COMMAND", "INFO", "get"] => Command::CmdInfo,
        &["COMMAND", "DOCS"] => Command::CmdDocs,
        &["COMMAND", "LIST"] => Command::CmdList,
        // CONFIG subcommand parsing
        &["CONFIG", "GET", "maxmemory"] => Command::ConfigGet { _pattern } if _pattern == "maxmemory",
        &[ "CONFIG", "SET", "maxmemory", "100mb", ] => Command::ConfigSet,
        &["CONFIG", "RESETSTAT"] => Command::ConfigOther,
        // Key-value commands
        &["GET", "mykey"] => Command::Get { key } if key == b"mykey",
        &["SET", "k", "v", "GET"] => Command::Set { get: true, .. },
        &[ "SET", "k", "v", "EXAT", "9999999999", ] => Command::Set { ex_ms: Some(_), .. },
        &[ "SET", "k", "v", "PXAT", "9999999999000", ] => Command::Set { ex_ms: Some(_), .. },
        &["SETEX", "k", "30", "v"] => Command::SetEx { ex_secs: 30, .. },
        &["PSETEX", "k", "300", "v"] => Command::PSetEx { ex_ms: 300, .. },
        &["MGET", "k1", "k2", "k3"] => Command::MGet { keys } if keys.len() == 3,
        &["MSET", "k1", "v1", "k2", "v2"] => Command::MSet { pairs } if pairs.len() == 2,
        &["DEL", "k"] => Command::Del { keys } if keys.len() == 1,
        &["DEL", "k1", "k2", "k3"] => Command::Del { keys } if keys.len() == 3,
        &["EXISTS", "k1", "k2"] => Command::Exists { keys } if keys.len() == 2,
        // String increment commands
        &["INCR", "counter"] => Command::Incr { key } if key == b"counter",
        &["DECR", "counter"] => Command::Decr { key } if key == b"counter",
        &["INCRBY", "counter", "5"] => Command::IncrBy { key, delta } if key == b"counter" && delta == 5,
        &["DECRBY", "counter", "3"] => Command::DecrBy { key, delta } if key == b"counter" && delta == 3,
        &["INCRBYFLOAT", "f", "1.5"] => Command::IncrByFloat { key, delta } if key == b"f" && delta == 1.5,
        &["APPEND", "k", "suffix"] => Command::Append { key, value } if key == b"k" && value == b"suffix",
        &["STRLEN", "k"] => Command::Strlen { key } if key == b"k",
        &["GETDEL", "k"] => Command::GetDel { key } if key == b"k",
        &["GETSET", "k", "newval"] => Command::GetSet { key, value } if key == b"k" && value == b"newval",
        &["SETNX", "k", "v"] => Command::SetNx { key, value } if key == b"k" && value == b"v",
        &["MSETNX", "k1", "v1", "k2", "v2"] => Command::MSetNx { pairs } if pairs.len() == 2,
        // Expiry commands
        &["EXPIRE", "k", "60"] => Command::Expire { secs: 60, .. },
        &["PEXPIRE", "k", "5000"] => Command::PExpire { ms: 5000, .. },
        &["TTL", "k"] => Command::Ttl { .. },
        &["PTTL", "k"] => Command::PTtl { .. },
        &["PERSIST", "k"] => Command::Persist { .. },
        &["EXPIRETIME", "k"] => Command::ExpireTime { .. },
        &["PEXPIRETIME", "k"] => Command::PExpireTime { .. },
        // Key inspection commands
        &["TYPE", "k"] => Command::Type { key } if key == b"k",
        &["KEYS", "*"] => Command::Keys { pattern } if pattern == b"*",
        &["KEYS", "user:*"] => Command::Keys { pattern } if pattern == b"user:*",
        &["DBSIZE"] => Command::DbSize,
        &["UNLINK", "k1", "k2"] => Command::Unlink { keys } if keys.len() == 2,
        &["RENAME", "old", "new"] => Command::Rename { key, newkey } if key == b"old" && newkey == b"new",
        &["RANDOMKEY"] => Command::RandomKey,
        // Hash commands
        &["HGET", "h", "f"] => Command::HGet { key, field } if key == b"h" && field == b"f",
        &["HSET", "h", "f", "v"] => Command::HSet { pairs, .. } if pairs.len() == 1,
        &[ "HSET", "h", "f1", "v1", "f2", "v2", ] => Command::HSet { pairs, .. } if pairs.len() == 2,
        &["HDEL", "h", "f1", "f2"] => Command::HDel { fields, .. } if fields.len() == 2,
        &["HGETALL", "h"] => Command::HGetAll { key } if key == b"h",
        &["HMGET", "h", "f1", "f2", "f3"] => Command::HMGet { fields, .. } if fields.len() == 3,
        &[ "HMSET", "h", "f1", "v1", "f2", "v2", ] => Command::HMSet { pairs, .. } if pairs.len() == 2,
        &["HKEYS", "h"] => Command::HKeys { key } if key == b"h",
        &["HVALS", "h"] => Command::HVals { key } if key == b"h",
        &["HEXISTS", "h", "field"] => Command::HExists { key, field } if key == b"h" && field == b"field",
        &["HLEN", "h"] => Command::HLen { key } if key == b"h",
        // Command names are matched case-insensitively.
        &["get", "k"] => Command::Get { .. },
        &["Get", "k"] => Command::Get { .. },
        &["GeT", "k"] => Command::Get { .. },
        // glob_to_sql_like helper
        &["LPUSH", "k", "v"] => Command::LPush { key, values } if key == b"k" && values == vec![b"v".to_vec()],
        &["LPUSH", "k", "a", "b", "c"] => Command::LPush { values, .. } if values.len() == 3,
        &["RPUSH", "k", "v"] => Command::RPush { values, .. } if values == vec![b"v".to_vec()],
        &["LPUSHX", "k", "v"] => Command::LPushX { values, .. } if values == vec![b"v".to_vec()],
        &["RPUSHX", "k", "v"] => Command::RPushX { values, .. } if values == vec![b"v".to_vec()],
        &["LPOP", "k"] => Command::LPop { count: None, .. },
        &["LPOP", "k", "5"] => Command::LPop { count: Some(5), .. },
        &["RPOP", "k", "3"] => Command::RPop { count: Some(3), .. },
        &["LLEN", "k"] => Command::LLen { key } if key == b"k",
        &["LINDEX", "k", "-2"] => Command::LIndex { index: -2, .. },
        &["LSET", "k", "1", "v"] => Command::LSet { index: 1, .. },
        &["LINSERT", "k", "BEFORE", "p", "v"] => Command::LInsert { before: true, .. },
        &["LINSERT", "k", "AFTER", "p", "v"] => Command::LInsert { before: false, .. },
        &["LREM", "k", "-2", "v"] => Command::LRem { count: -2, .. },
        // Set commands
        &["SADD", "s", "a", "b", "c"] => Command::SAdd { members, .. } if members.len() == 3,
        &["SREM", "s", "a", "b"] => Command::SRem { members, .. } if members.len() == 2,
        &["SMEMBERS", "s"] => Command::SMembers { key } if key == b"s",
        &["SCARD", "s"] => Command::SCard { key } if key == b"s",
        &["SISMEMBER", "s", "m"] => Command::SIsMember { key, member } if key == b"s" && member == b"m",
        &["SMISMEMBER", "s", "a", "b", "c"] => Command::SMisMember { members, .. } if members.len() == 3,
        &["SPOP", "s"] => Command::SPop { count: None, .. },
        &["SPOP", "s", "3"] => Command::SPop { count: Some(3), .. },
        &["SRANDMEMBER", "s"] => Command::SRandMember { count: None, .. },
        &["SUNION", "a", "b", "c"] => Command::SUnion { keys } if keys.len() == 3,
        &["SINTER", "a", "b"] => Command::SInter { keys } if keys.len() == 2,
        &["SDIFF", "a", "b"] => Command::SDiff { keys } if keys.len() == 2,
        &["SUNIONSTORE", "d", "a", "b"] => Command::SUnionStore { dst, keys } if dst == b"d" && keys.len() == 2,
        &["SINTERSTORE", "d", "a", "b"] => Command::SInterStore { dst, keys } if dst == b"d" && keys.len() == 2,
        &["SDIFFSTORE", "d", "a", "b"] => Command::SDiffStore { dst, keys } if dst == b"d" && keys.len() == 2,
        // aggregate / store parsing
        &["ZPOPMIN", "z", "3"] => Command::ZPopMin { count: Some(3), .. },
        &["ZPOPMIN", "z"] => Command::ZPopMin { count: None, .. },
        }
    }

    /// Every command listed in the coverage doc must parse, and every command
    /// that parses must be listed.
    ///
    /// The doc had drifted badly enough to claim LPUSH, SADD, INCR and STRLEN
    /// were unimplemented. Checking it against the parser is the only way it
    /// stays true, so the page is data for this test rather than prose nobody
    /// re-reads.
    #[test]
    fn the_coverage_doc_lists_exactly_what_parses() {
        const DOC: &str = include_str!("../docs/command-coverage.md");

        let listed: Vec<&str> = DOC
            .lines()
            .filter_map(|l| l.strip_prefix("| `"))
            .filter_map(|l| l.split('`').next())
            .filter(|c| {
                c.chars()
                    .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
            })
            .collect();
        assert!(
            listed.len() > 100,
            "only {} commands parsed out of the doc",
            listed.len()
        );

        for cmd in &listed {
            let parsed = Command::parse(vec![cmd.as_bytes().to_vec()]);
            // Missing-argument errors are fine; "unknown command" is not.
            if let Err(e) = &parsed {
                assert!(
                    !e.contains("unknown command"),
                    "{cmd} is documented but the parser rejects it as unknown"
                );
            }
        }

        // ...and nothing the parser knows is missing from the page. COMMAND is
        // spelled differently in the dispatch, so compare on the doc side.
        for cmd in [
            "GET",
            "SET",
            "APPEND",
            "STRLEN",
            "GETSET",
            "INCR",
            "LPUSH",
            "LPOS",
            "LINSERT",
            "SADD",
            "SMISMEMBER",
            "ZADD",
            "ZRANDMEMBER",
            "ZDIFFSTORE",
            "HSETNX",
            "UNLINK",
            "SCAN",
            "RANDOMKEY",
            "PUBSUB",
            "WATCH",
        ] {
            assert!(
                listed.contains(&cmd),
                "{cmd} parses but is missing from docs/command-coverage.md"
            );
        }
    }

    /// The two halves must stay contiguous and cover every database, and the
    /// shared-memory tables must line up one-per-ephemeral-db — `execute_mem`
    /// indexes them with the raw db number.
    #[test]
    fn the_database_halves_partition_the_range() {
        assert!((0..NUM_DBS).all(|db| is_ephemeral(db as u8) == (db < DURABLE_FROM as usize)));
        assert!(is_ephemeral(CACHE_DB) && !is_ephemeral(DURABLE_DB));
        assert_eq!(crate::mem::NUM_MEM_DBS, DURABLE_FROM as usize);
    }

    /// Memory mode used to copy as much of an over-long key, field or value as
    /// fit into its fixed-size slot. Every command that reaches the
    /// shared-memory backend must now refuse instead, before it writes
    /// anything — a truncated key collides with whatever shares its prefix.
    #[test]
    fn oversized_keys_fields_and_values_are_refused_by_memory_mode() {
        let key = "k".repeat(MEM_MAX_KEY + 1);
        let member = "m".repeat(mem_max_member() + 1);
        let value = "v".repeat(crate::mem::max_total_val_len() + 1);
        let ok = "x";

        // (command, which limit the reply must name)
        let cases: &[(&[&str], &str)] = &[
            (&["GET", &key], "key"),
            (&["SET", &key, ok], "key"),
            (&["SET", ok, &value], "value"),
            (&["MSET", ok, ok, &key, ok], "key"),
            (&["MSET", ok, &value], "value"),
            (&["DEL", ok, &key], "key"),
            (&["RENAME", ok, &key], "key"),
            (&["HGET", ok, &member], "hash field"),
            (&["HSET", ok, &member, ok], "hash field"),
            (&["HSET", ok, ok, &value], "value"),
            (&["HDEL", ok, &member], "hash field"),
            (&["SADD", ok, &member], "hash field"),
            (&["SISMEMBER", ok, &member], "hash field"),
            (&["SMOVE", ok, ok, &member], "hash field"),
            (&["ZADD", ok, "1", &member], "hash field"),
            (&["ZSCORE", ok, &member], "hash field"),
            (&["LPUSH", ok, &value], "value"),
            (&["LSET", ok, "0", &value], "value"),
            (&["SUNIONSTORE", &key, ok], "key"),
        ];

        for (input, limit) in cases {
            let cmd = is_ok(Command::parse(parts(input)));
            match cmd.mem_too_long_error() {
                Some(Response::Error(e)) => assert!(
                    e.contains(limit) && e.contains("SELECT durable"),
                    "{input:?} refused with {e:?}, expected it to name {limit:?}"
                ),
                _ => panic!("{input:?} was not refused"),
            }
        }

        // ...and a command sized exactly at the limits still goes through.
        let at_limit: &[&str] = &[
            "HSET",
            &"k".repeat(MEM_MAX_KEY),
            &"m".repeat(mem_max_member()),
            &"v".repeat(crate::mem::max_total_val_len()),
        ];
        let cmd = is_ok(Command::parse(parts(at_limit)));
        assert!(cmd.mem_too_long_error().is_none());
    }

    /// Inputs a client can send that must be rejected at parse time rather
    /// than reaching the dispatcher.
    #[test]
    fn malformed_commands_are_rejected() {
        let cases: &[&[&str]] = &[
            // Connection commands
            &["ECHO"],
            &["SELECT", "abc"],
            &["SELECT", "16"],
            &["SELECT", "255"],
            // CLIENT subcommand parsing
            &["CLIENT", "SETNAME"],
            // CONFIG subcommand parsing
            &["CONFIG", "GET"],
            // Key-value commands
            &["GET"],
            &["SET", "k", "v", "NX", "XX"],
            &["SET", "k", "v", "EX", "10", "KEEPTTL"],
            &["MGET"],
            &["MSET", "k1", "v1", "k2"],
            &["DEL"],
            // String increment commands
            &["INCR"],
            &["INCRBY", "counter", "abc"],
            &["INCRBYFLOAT", "f", "notafloat"],
            &["MSETNX", "k1"],
            // Key inspection commands
            &["UNLINK"],
            &["SCAN", "notanumber"],
            // Hash commands
            &["HSET", "h", "f"],
            &["HDEL", "h"],
            &["HMGET", "h"],
            &["HINCRBY", "h", "f", "notanumber"],
            // Edge cases
            &["UNKNOWN"],
            // glob_to_sql_like helper
            &["LPUSH", "k"],
            &["LRANGE", "k", "x", "1"],
            &["LINSERT", "k", "MIDDLE", "p", "v"],
            &["LMOVE", "s", "d", "UP", "DOWN"],
            &["LPOS", "k", "v", "RANK", "0"],
            // Set commands
            &["SADD", "s"],
            &["SREM", "s"],
            &["SMISMEMBER", "s"],
            &["SUNION"],
            &["SUNIONSTORE", "d"],
            &["SMOVE", "s", "d"],
            // Range / count parsing
            &["ZRANGE", "z", "-", "+", "BYLEX", "WITHSCORES"],
            // aggregate / store parsing
            &["ZDIFFSTORE", "d", "3", "a", "b"],
        ];
        for input in cases {
            is_err(Command::parse(parts(input)));
        }
    }

    // ─────────────────────────── Key-value commands ───────────────────────────

    #[test]
    fn parse_set_no_expiry() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v"])));
        assert!(
            matches!(cmd, Command::Set { key, value, ex_ms: None, .. } if key == b"k" && value == b"v")
        );
    }

    #[test]
    fn parse_set_with_ex() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "EX", "10"])));
        assert!(matches!(
            cmd,
            Command::Set {
                ex_ms: Some(10000),
                ..
            }
        ));
    }

    #[test]
    fn parse_set_with_px() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "PX", "500"])));
        assert!(matches!(
            cmd,
            Command::Set {
                ex_ms: Some(500),
                ..
            }
        ));
    }

    #[test]
    fn parse_set_case_insensitive_ex() {
        let cmd = is_ok(Command::parse(parts(&["set", "k", "v", "ex", "5"])));
        assert!(matches!(
            cmd,
            Command::Set {
                ex_ms: Some(5000),
                ..
            }
        ));
    }

    #[test]
    fn parse_set_nx() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "NX"])));
        assert!(matches!(
            cmd,
            Command::Set {
                nx: true,
                xx: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_set_xx() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "XX"])));
        assert!(matches!(
            cmd,
            Command::Set {
                nx: false,
                xx: true,
                ..
            }
        ));
    }

    #[test]
    fn parse_set_keepttl() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "KEEPTTL"])));
        assert!(matches!(
            cmd,
            Command::Set {
                keepttl: true,
                ex_ms: None,
                ..
            }
        ));
    }

    #[test]
    fn parse_set_nx_get_combo() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "NX", "GET"])));
        assert!(matches!(
            cmd,
            Command::Set {
                nx: true,
                get: true,
                ..
            }
        ));
    }

    // ──────────────────────────── Expiry commands ────────────────────────────

    #[test]
    fn parse_expireat() {
        let cmd = is_ok(Command::parse(parts(&["EXPIREAT", "k", "1700000000"])));
        assert!(matches!(
            cmd,
            Command::ExpireAt {
                unix_secs: 1700000000,
                ..
            }
        ));
    }

    #[test]
    fn parse_pexpireat() {
        let cmd = is_ok(Command::parse(parts(&["PEXPIREAT", "k", "1700000000000"])));
        assert!(matches!(
            cmd,
            Command::PExpireAt {
                unix_ms: 1700000000000,
                ..
            }
        ));
    }

    // ──────────────────────── Key inspection commands ────────────────────────

    #[test]
    fn parse_scan_zero_cursor() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0"])));
        assert!(matches!(
            cmd,
            Command::Scan {
                _cursor: 0,
                pattern: None,
                _count: None
            }
        ));
    }

    #[test]
    fn parse_scan_with_match() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "MATCH", "user:*"])));
        assert!(
            matches!(cmd, Command::Scan { _cursor: 0, pattern: Some(p), _count: None } if p == b"user:*")
        );
    }

    #[test]
    fn parse_scan_with_count() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "COUNT", "100"])));
        assert!(matches!(
            cmd,
            Command::Scan {
                _cursor: 0,
                pattern: None,
                _count: Some(100)
            }
        ));
    }

    #[test]
    fn parse_scan_with_match_and_count() {
        let cmd = is_ok(Command::parse(parts(&[
            "SCAN", "0", "MATCH", "*", "COUNT", "10",
        ])));
        assert!(matches!(
            cmd,
            Command::Scan {
                _cursor: 0,
                pattern: Some(_),
                _count: Some(10)
            }
        ));
    }

    // ───────────────────────────── Hash commands ─────────────────────────────

    #[test]
    fn parse_hincrby() {
        let cmd = is_ok(Command::parse(parts(&["HINCRBY", "h", "f", "10"])));
        assert!(
            matches!(cmd, Command::HIncrBy { key, field, delta } if key == b"h" && field == b"f" && delta == 10)
        );
    }

    #[test]
    fn parse_hsetnx() {
        let cmd = is_ok(Command::parse(parts(&["HSETNX", "h", "f", "v"])));
        assert!(
            matches!(cmd, Command::HSetNx { key, field, value } if key == b"h" && field == b"f" && value == b"v")
        );
    }

    // ─────────────────────────────── Edge cases ───────────────────────────────

    #[test]
    fn parse_empty_parts() {
        is_err(Command::parse(vec![]));
    }

    // ──────────────────────── glob_to_sql_like helper ────────────────────────

    #[test]
    fn parse_lrange() {
        let cmd = is_ok(Command::parse(parts(&["LRANGE", "k", "0", "-1"])));
        assert!(matches!(
            cmd,
            Command::LRange {
                start: 0,
                stop: -1,
                ..
            }
        ));
    }

    #[test]
    fn parse_lmove() {
        let cmd = is_ok(Command::parse(parts(&["LMOVE", "s", "d", "LEFT", "RIGHT"])));
        assert!(matches!(
            cmd,
            Command::LMove {
                src_left: true,
                dst_left: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_lpos_simple() {
        let cmd = is_ok(Command::parse(parts(&["LPOS", "k", "v"])));
        assert!(matches!(
            cmd,
            Command::LPos {
                rank: None,
                count: None,
                ..
            }
        ));
    }

    #[test]
    fn parse_lpos_with_rank_count() {
        let cmd = is_ok(Command::parse(parts(&[
            "LPOS", "k", "v", "RANK", "-1", "COUNT", "0",
        ])));
        assert!(matches!(
            cmd,
            Command::LPos {
                rank: Some(-1),
                count: Some(0),
                ..
            }
        ));
    }

    #[test]
    fn parse_ltrim() {
        let cmd = is_ok(Command::parse(parts(&["LTRIM", "k", "1", "-1"])));
        assert!(matches!(
            cmd,
            Command::LTrim {
                start: 1,
                stop: -1,
                ..
            }
        ));
    }

    // ────────────────────────────── Set commands ──────────────────────────────

    #[test]
    fn parse_sadd_single() {
        let cmd = is_ok(Command::parse(parts(&["SADD", "s", "m"])));
        assert!(
            matches!(cmd, Command::SAdd { key, members } if key == b"s" && members == vec![b"m".to_vec()])
        );
    }

    #[test]
    fn parse_srandmember_negative_count() {
        let cmd = is_ok(Command::parse(parts(&["SRANDMEMBER", "s", "-4"])));
        assert!(matches!(
            cmd,
            Command::SRandMember {
                count: Some(-4),
                ..
            }
        ));
    }

    #[test]
    fn parse_smove() {
        let cmd = is_ok(Command::parse(parts(&["SMOVE", "s", "d", "m"])));
        assert!(
            matches!(cmd, Command::SMove { src, dst, member } if src == b"s" && dst == b"d" && member == b"m")
        );
    }

    // ──────────────────────────── build_inter_sql ────────────────────────────

    #[test]
    fn build_inter_sql_chains_intersect() {
        let sql = build_inter_sql(0, 3, 1);
        assert_eq!(
            sql,
            "SELECT member FROM redis.set_0 WHERE key = $1 \
             INTERSECT SELECT member FROM redis.set_0 WHERE key = $2 \
             INTERSECT SELECT member FROM redis.set_0 WHERE key = $3"
        );
    }

    #[test]
    fn build_inter_sql_offsets_parameters() {
        let sql = build_inter_sql(5, 2, 3);
        assert_eq!(
            sql,
            "SELECT member FROM redis.set_5 WHERE key = $3 \
             INTERSECT SELECT member FROM redis.set_5 WHERE key = $4"
        );
    }

    // ─────────────────────────── Score / lex bounds ───────────────────────────

    #[test]
    fn parse_score_value_finite() {
        assert_eq!(parse_score_value(b"1"), Some(1.0));
        assert_eq!(parse_score_value(b"-3.5"), Some(-3.5));
    }

    #[test]
    fn parse_score_value_infinity_aliases() {
        assert_eq!(parse_score_value(b"inf"), Some(f64::INFINITY));
        assert_eq!(parse_score_value(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_score_value(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_score_value(b"+Infinity"), Some(f64::INFINITY));
    }

    #[test]
    fn parse_score_value_rejects_nan_and_garbage() {
        assert_eq!(parse_score_value(b"nan"), None);
        assert_eq!(parse_score_value(b"abc"), None);
    }

    #[test]
    fn parse_score_bound_inclusive_vs_exclusive() {
        let inc = parse_score_bound(b"5").unwrap();
        assert!(!inc.exclusive && inc.value == 5.0);
        let exc = parse_score_bound(b"(2.5").unwrap();
        assert!(exc.exclusive && exc.value == 2.5);
        let neg = parse_score_bound(b"-inf").unwrap();
        assert!(!neg.exclusive && neg.value == f64::NEG_INFINITY);
    }

    #[test]
    fn parse_lex_bound_sentinels_and_strings() {
        assert!(matches!(parse_lex_bound(b"-"), Some(LexBound::NegInf)));
        assert!(matches!(parse_lex_bound(b"+"), Some(LexBound::PosInf)));
        assert!(matches!(parse_lex_bound(b"[foo"), Some(LexBound::Inclusive(s)) if s == b"foo"));
        assert!(matches!(parse_lex_bound(b"(bar"), Some(LexBound::Exclusive(s)) if s == b"bar"));
        assert!(parse_lex_bound(b"foo").is_none());
    }

    #[test]
    fn format_score_rounds_integers_without_decimal() {
        assert_eq!(format_score(1.0), "1");
        assert_eq!(format_score(-42.0), "-42");
        assert_eq!(format_score(f64::INFINITY), "inf");
        assert_eq!(format_score(f64::NEG_INFINITY), "-inf");
    }

    // ────────────────────────────── ZADD parsing ──────────────────────────────

    #[test]
    fn parse_zadd_simple_pairs() {
        let cmd = is_ok(Command::parse(parts(&["ZADD", "z", "1", "a", "2", "b"])));
        assert!(matches!(
            cmd,
            Command::ZAdd { pairs, nx: false, xx: false, gt: false, lt: false, ch: false, incr: false, .. }
            if pairs.len() == 2
        ));
    }

    #[test]
    fn parse_zadd_with_flags() {
        let cmd = is_ok(Command::parse(parts(&["ZADD", "z", "NX", "CH", "1", "a"])));
        assert!(matches!(
            cmd,
            Command::ZAdd { nx: true, ch: true, pairs, .. } if pairs.len() == 1
        ));
    }

    #[test]
    fn parse_zadd_rejects_conflicting_flags() {
        is_err(Command::parse(parts(&["ZADD", "z", "NX", "XX", "1", "a"])));
        is_err(Command::parse(parts(&["ZADD", "z", "GT", "LT", "1", "a"])));
        is_err(Command::parse(parts(&["ZADD", "z", "NX", "GT", "1", "a"])));
    }

    #[test]
    fn parse_zadd_incr_requires_single_pair() {
        let ok = is_ok(Command::parse(parts(&["ZADD", "z", "INCR", "1", "a"])));
        assert!(matches!(ok, Command::ZAdd { incr: true, pairs, .. } if pairs.len() == 1));
        is_err(Command::parse(parts(&[
            "ZADD", "z", "INCR", "1", "a", "2", "b",
        ])));
    }

    #[test]
    fn parse_zadd_accepts_infinity_score() {
        let cmd = is_ok(Command::parse(parts(&["ZADD", "z", "+inf", "a"])));
        assert!(matches!(
            cmd,
            Command::ZAdd { pairs, .. } if pairs[0].0.is_infinite() && pairs[0].0 > 0.0
        ));
    }

    // ───────────────────────── Range / count parsing ─────────────────────────

    #[test]
    fn parse_zrange_default_index() {
        let cmd = is_ok(Command::parse(parts(&["ZRANGE", "z", "0", "-1"])));
        assert!(matches!(
            cmd,
            Command::ZRange {
                by: RangeBy::Index,
                rev: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_zrange_byscore_withscores_limit() {
        let cmd = is_ok(Command::parse(parts(&[
            "ZRANGE",
            "z",
            "0",
            "10",
            "BYSCORE",
            "LIMIT",
            "5",
            "2",
            "WITHSCORES",
        ])));
        assert!(matches!(
            cmd,
            Command::ZRange {
                by: RangeBy::Score,
                limit: Some((5, 2)),
                with_scores: true,
                rev: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_zrangebyscore_exclusive_bound() {
        let cmd = is_ok(Command::parse(parts(&["ZRANGEBYSCORE", "z", "(1", "+inf"])));
        assert!(matches!(
            cmd,
            Command::ZRangeByScore { min, max, rev: false, .. }
            if min.exclusive && min.value == 1.0 && !max.exclusive && max.value.is_infinite()
        ));
    }

    #[test]
    fn parse_zrevrangebyscore_swaps_bounds() {
        let cmd = is_ok(Command::parse(parts(&["ZREVRANGEBYSCORE", "z", "10", "1"])));
        assert!(matches!(
            cmd,
            Command::ZRangeByScore { min, max, rev: true, .. }
            if min.value == 1.0 && max.value == 10.0
        ));
    }

    #[test]
    fn parse_zrangebylex_sentinels() {
        let cmd = is_ok(Command::parse(parts(&["ZRANGEBYLEX", "z", "-", "+"])));
        assert!(matches!(
            cmd,
            Command::ZRangeByLex {
                min: LexBound::NegInf,
                max: LexBound::PosInf,
                ..
            }
        ));
    }

    // ─────────────────────── aggregate / store parsing ───────────────────────

    #[test]
    fn parse_zunionstore_weights_and_aggregate() {
        let cmd = is_ok(Command::parse(parts(&[
            "ZUNIONSTORE",
            "d",
            "2",
            "a",
            "b",
            "WEIGHTS",
            "2",
            "3",
            "AGGREGATE",
            "MAX",
        ])));
        assert!(matches!(
            cmd,
            Command::ZUnionStore { dst, keys, weights: Some(w), aggregate: Aggregate::Max }
            if dst == b"d" && keys == [b"a".to_vec(), b"b".to_vec()] && w == [2.0, 3.0]
        ));
    }

    #[test]
    fn parse_zinterstore_default_aggregate_is_sum() {
        let cmd = is_ok(Command::parse(parts(&["ZINTERSTORE", "d", "2", "a", "b"])));
        assert!(matches!(
            cmd,
            Command::ZInterStore {
                aggregate: Aggregate::Sum,
                weights: None,
                ..
            }
        ));
    }

    #[test]
    fn parse_zrank_with_score() {
        let cmd = is_ok(Command::parse(parts(&["ZRANK", "z", "m", "WITHSCORE"])));
        assert!(matches!(
            cmd,
            Command::ZRank {
                with_score: true,
                rev: false,
                ..
            }
        ));
        let rev = is_ok(Command::parse(parts(&["ZREVRANK", "z", "m"])));
        assert!(matches!(
            rev,
            Command::ZRank {
                rev: true,
                with_score: false,
                ..
            }
        ));
    }

    #[test]
    fn parse_zrandmember_withscores() {
        let cmd = is_ok(Command::parse(parts(&[
            "ZRANDMEMBER",
            "z",
            "2",
            "WITHSCORES",
        ])));
        assert!(matches!(
            cmd,
            Command::ZRandMember {
                count: Some(2),
                with_scores: true,
                ..
            }
        ));
    }

    #[test]
    fn lrange_parses_with_negative_indices() {
        let cmd = Command::parse(vec![
            b"LRANGE".to_vec(),
            b"k".to_vec(),
            b"-1".to_vec(),
            b"-1".to_vec(),
        ])
        .unwrap();
        assert!(matches!(
            cmd,
            Command::LRange {
                start: -1,
                stop: -1,
                ..
            }
        ));
    }

    #[test]
    fn lset_parses_with_negative_index() {
        let cmd = Command::parse(vec![
            b"LSET".to_vec(),
            b"k".to_vec(),
            b"-1".to_vec(),
            b"v".to_vec(),
        ])
        .unwrap();
        assert!(matches!(cmd, Command::LSet { index: -1, .. }));
    }

    #[test]
    fn ltrim_parses_with_out_of_bounds_stop() {
        let cmd = Command::parse(vec![
            b"LTRIM".to_vec(),
            b"k".to_vec(),
            b"0".to_vec(),
            b"-100".to_vec(),
        ])
        .unwrap();
        assert!(matches!(
            cmd,
            Command::LTrim {
                start: 0,
                stop: -100,
                ..
            }
        ));
    }

    #[test]
    fn multi_parses() {
        let cmd = Command::parse(vec![b"MULTI".to_vec()]).unwrap();
        assert!(matches!(cmd, Command::Multi));
    }

    #[test]
    fn exec_parses() {
        let cmd = Command::parse(vec![b"EXEC".to_vec()]).unwrap();
        assert!(matches!(cmd, Command::Exec));
    }

    #[test]
    fn discard_parses() {
        let cmd = Command::parse(vec![b"DISCARD".to_vec()]).unwrap();
        assert!(matches!(cmd, Command::Discard));
    }

    #[test]
    fn watch_parses_keys() {
        let cmd = Command::parse(vec![b"WATCH".to_vec(), b"k1".to_vec(), b"k2".to_vec()]).unwrap();
        assert!(
            matches!(cmd, Command::Watch { ref keys } if keys == &vec![b"k1".to_vec(), b"k2".to_vec()])
        );
    }

    #[test]
    fn watch_requires_keys() {
        let err = Command::parse(vec![b"WATCH".to_vec()]).unwrap_err();
        assert!(err.contains("WATCH requires"));
    }

    #[test]
    fn unwatch_parses() {
        let cmd = Command::parse(vec![b"UNWATCH".to_vec()]).unwrap();
        assert!(matches!(cmd, Command::Unwatch));
    }
}
