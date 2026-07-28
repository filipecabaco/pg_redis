use pgrx::datum::DatumWithOid;
use pgrx::spi::SpiClient;

pub const NUM_DBS: usize = 16;

/// Two contiguous halves rather than interleaved by parity: `0..DURABLE_FROM`
/// are ephemeral, the rest WAL-logged. "Anything under 8 is a cache" is a rule
/// a client can hold in its head.
pub const DURABLE_FROM: u8 = 8;

/// Lowest ephemeral database — what `SELECT cache` resolves to.
const CACHE_DB: u8 = 0;

/// Lowest durable database — what `SELECT durable` resolves to.
const DURABLE_DB: u8 = DURABLE_FROM;

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

    pub static REAP_EXPIRED: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| arr(reap_expired_keys));

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

    /// Whether any collection table holds the key named by the `k` CTE. The
    /// four are keyed independently, so "does this key exist" is the same
    /// four-way question wherever it is asked.
    pub fn collection_exists(db: usize) -> String {
        ["list", "set", "hash", "zset"]
            .iter()
            .map(|t| format!("EXISTS (SELECT 1 FROM redis.{t}_{db} c, k WHERE c.key = k.key)"))
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    /// `TTL`, `PTTL`, `EXPIRETIME` and `PEXPIRETIME` differ only in how they
    /// render the deadline. Everything ahead of that — is the key there, and
    /// does it carry an expiry — is one question for all five types: a string
    /// keeps its expiry in its own row, a collection in `redis.expiry_N`.
    fn ttl_like(db: usize, render: &str) -> String {
        let collection = collection_exists(db);
        format!(
            "WITH k AS (SELECT $1::bytea AS key), \
                  s AS (SELECT t.expires_at FROM redis.kv_{db} t, k \
                         WHERE t.key = k.key \
                           AND (t.expires_at IS NULL OR t.expires_at > now())), \
                  c AS (SELECT e.expires_at FROM k \
                        LEFT JOIN redis.expiry_{db} e ON e.key = k.key \
                         WHERE ({collection}) \
                           AND (e.expires_at IS NULL OR e.expires_at > now())), \
                  live AS (SELECT expires_at FROM s UNION ALL SELECT expires_at FROM c) \
             SELECT CASE \
                      WHEN NOT EXISTS (SELECT 1 FROM live) THEN -2::bigint \
                      WHEN (SELECT expires_at FROM live LIMIT 1) IS NULL THEN -1::bigint \
                      ELSE {render} \
                    END"
        )
    }

    /// The deadline as `ttl_like` will have bound it.
    const DEADLINE: &str = "(SELECT expires_at FROM live LIMIT 1)";

    pub static TTL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            ttl_like(
                db,
                &format!("GREATEST(-1, EXTRACT(EPOCH FROM ({DEADLINE} - now()))::bigint)"),
            )
        })
    });

    pub static PTTL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            ttl_like(
                db,
                &format!("GREATEST(-1, (EXTRACT(EPOCH FROM ({DEADLINE} - now())) * 1000)::bigint)"),
            )
        })
    });

    pub static EXPIRETIME: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| ttl_like(db, &format!("EXTRACT(EPOCH FROM {DEADLINE})::bigint")))
    });

    pub static PEXPIRETIME: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            ttl_like(
                db,
                &format!("(EXTRACT(EPOCH FROM {DEADLINE}) * 1000)::bigint"),
            )
        })
    });

    /// The four counters differ in two places: what they store for a key that
    /// is not there yet, and how they change one that is. Everything else is
    /// the guard, and the guard is the part that must not drift — it is what
    /// keeps a value that is not an integer, or an increment that would leave
    /// the `bigint` range, from being written at all.
    fn counter_like(db: usize, seed: &str, delta: &str) -> String {
        format!(
            "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to({seed}, 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((CAST(encode(redis.kv_{db}.value, 'escape') AS bigint) {delta})::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^(0|-?[1-9][0-9]*)$' \
           AND encode(redis.kv_{db}.value, 'escape')::numeric \
               BETWEEN -9223372036854775808 AND 9223372036854775807 \
         RETURNING CAST(encode(value, 'escape') AS bigint)"
        )
    }

    pub static INCR: LazyLock<[String; NUM_DBS]> =
        LazyLock::new(|| arr(|db| counter_like(db, "'1'", "+ 1")));

    pub static DECR: LazyLock<[String; NUM_DBS]> =
        LazyLock::new(|| arr(|db| counter_like(db, "'-1'", "- 1")));

    pub static INCRBY: LazyLock<[String; NUM_DBS]> =
        LazyLock::new(|| arr(|db| counter_like(db, "$2::text", "+ $3")));

    pub static DECRBY: LazyLock<[String; NUM_DBS]> =
        LazyLock::new(|| arr(|db| counter_like(db, "(-$2::bigint)::text", "- $3")));

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
                "WITH k AS (SELECT $1::bytea AS key), \
                      s AS (UPDATE redis.kv_{db} t SET expires_at = NULL FROM k \
                             WHERE t.key = k.key AND t.expires_at IS NOT NULL \
                               AND t.expires_at > now() RETURNING 1), \
                      c AS (DELETE FROM redis.expiry_{db} e USING k \
                             WHERE e.key = k.key AND e.expires_at > now() \
                               AND NOT EXISTS (SELECT 1 FROM s) RETURNING 1) \
                 SELECT 1 WHERE EXISTS (SELECT 1 FROM s) OR EXISTS (SELECT 1 FROM c)"
            )
        })
    });

    /// CTEs that clear whatever else held `$1`, for a statement that is about to
    /// write a string there. Redis lets `SET` land on a key that held a list;
    /// the list goes, and so does any expiry, since replacing a value discards
    /// its TTL. Inline rather than a lookup first: on the common path there is
    /// nothing to clear, and asking cost more than the four index probes.
    pub fn clear_for_string(db: usize) -> String {
        let mut ctes: Vec<String> = ["hash", "list", "set", "zset"]
            .iter()
            .enumerate()
            .map(|(i, t)| format!("clr{i} AS (DELETE FROM redis.{t}_{db} WHERE key = $1)"))
            .collect();
        ctes.push(format!(
            "clr4 AS (DELETE FROM redis.expiry_{db} WHERE key = $1)"
        ));
        format!("{}, ", ctes.join(", "))
    }

    /// Empty every table of the named databases. `TRUNCATE` rather than
    /// `DELETE`: it does not build a dead tuple per row, and these tables are
    /// being emptied whole.
    pub fn flush_dbs(dbs: &[usize]) -> String {
        let tables: Vec<String> = dbs
            .iter()
            .flat_map(|db| {
                ["kv", "hash", "list", "set", "zset", "expiry"]
                    .iter()
                    .map(move |t| format!("redis.{t}_{db}"))
            })
            .collect();
        format!("TRUNCATE {}", tables.join(", "))
    }

    /// One statement per database for the background sweep: the strings whose
    /// own column has passed, and every table's rows for a collection whose
    /// `redis.expiry_N` row has. A collection's data lives in four tables and
    /// its expiry in a fifth, so the sweep has to name all of them.
    pub fn sweep_expired(db: usize) -> String {
        let drops = ["hash", "list", "set", "zset"]
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!(
                    "d{i} AS (DELETE FROM redis.{t}_{db} t \
                              WHERE t.key IN (SELECT key FROM dead))"
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "WITH dead AS (DELETE FROM redis.expiry_{db} \
                            WHERE expires_at <= now() RETURNING key), \
                  {drops}, \
                  s AS (DELETE FROM redis.kv_{db} \
                         WHERE expires_at IS NOT NULL AND expires_at <= now()) \
             SELECT 1"
        )
    }

    /// `sweep_expired` for a named set of keys: reap only these, and only if
    /// their deadline has passed.
    ///
    /// This runs before a collection read, so it is on the hot path and is
    /// shaped for the case where nothing has expired — which is almost always.
    /// That case is one indexed lookup on `redis.expiry_N`, a table holding
    /// only the keys that carry a TTL, and four deletes that match no rows.
    ///
    /// Deliberately *not* `sql_key_kinds`, which is the other thing that would
    /// answer the question: that runs five `EXISTS` across five tables to work
    /// out a kind this caller does not need, and using it here cost two thirds
    /// of the throughput of every `HGET`, `SCARD`, `ZCARD` and `LLEN`.
    pub fn reap_expired_keys(db: usize) -> String {
        let drops = ["hash", "list", "set", "zset"]
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!(
                    "d{i} AS (DELETE FROM redis.{t}_{db} t \
                              WHERE t.key IN (SELECT key FROM dead))"
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "WITH dead AS (DELETE FROM redis.expiry_{db} \
                            WHERE key = ANY($1::bytea[]) AND expires_at <= now() \
                            RETURNING key), \
                  {drops} \
             SELECT 1"
        )
    }

    /// `EXPIRE` and its three siblings differ only in how they spell the
    /// deadline. A string's goes in its own row; a collection's has nowhere to
    /// go but `redis.expiry_N`, and only when the key is actually there.
    pub fn set_expiry(db: usize, deadline: &str) -> String {
        let collection = collection_exists(db);
        format!(
            "WITH k AS (SELECT $1::bytea AS key, {deadline} AS at), \
                  s AS (UPDATE redis.kv_{db} t SET expires_at = k.at FROM k \
                         WHERE t.key = k.key \
                           AND (t.expires_at IS NULL OR t.expires_at > now()) RETURNING 1), \
                  c AS (INSERT INTO redis.expiry_{db} (key, expires_at) \
                        SELECT k.key, k.at FROM k \
                         WHERE NOT EXISTS (SELECT 1 FROM s) AND ({collection}) \
                        ON CONFLICT (key) DO UPDATE SET expires_at = EXCLUDED.expires_at \
                        RETURNING 1) \
             SELECT 1 WHERE EXISTS (SELECT 1 FROM s) OR EXISTS (SELECT 1 FROM c)"
        )
    }

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
    /// `FLUSHDB [ASYNC|SYNC]` — empty the current database.
    FlushDb,
    /// `FLUSHALL [ASYNC|SYNC]` — empty all sixteen, the durable half included.
    FlushAll,
    SetRange {
        key: Vec<u8>,
        offset: i64,
        value: Vec<u8>,
    },
    GetRange {
        key: Vec<u8>,
        start: i64,
        end: i64,
    },
    HIncrByFloat {
        key: Vec<u8>,
        field: Vec<u8>,
        delta: f64,
    },
    RenameNx {
        key: Vec<u8>,
        newkey: Vec<u8>,
    },
    /// `GETEX key [EX s|PX ms|EXAT ts|PXAT ts|PERSIST]` — read the value and,
    /// optionally, change what happens to it next.
    GetEx {
        key: Vec<u8>,
        expiry: GetExExpiry,
    },
    /// `COPY src dst [DB n] [REPLACE]`. `db` may name the other storage half,
    /// which is how a cache entry is promoted to a durable one.
    Copy {
        src: Vec<u8>,
        dst: Vec<u8>,
        db: Option<u8>,
        replace: bool,
    },
    HRandField {
        key: Vec<u8>,
        count: Option<i64>,
        withvalues: bool,
    },
    SInterCard {
        keys: Vec<Vec<u8>>,
        limit: Option<i64>,
    },
    ObjectEncoding {
        key: Vec<u8>,
    },
    /// Any other `OBJECT` subcommand, which is answered rather than refused.
    ObjectOther,
    SetBit {
        key: Vec<u8>,
        offset: i64,
        value: bool,
    },
    GetBit {
        key: Vec<u8>,
        offset: i64,
    },
    BitCount {
        key: Vec<u8>,
        range: Option<(i64, i64, bool)>,
    },
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
    /// `*-1`, which a count-taking command answers for a key that is not there.
    /// Distinct from `Null`: a client asking for a list back gets a nil list,
    /// not a nil string.
    NullArray,
    Ok,
    Integer(i64),
    BulkString(Vec<u8>),
    SimpleString(String),
    Array(Vec<Option<Vec<u8>>>),
    IntegerArray(Vec<i64>),
    ScanResult {
        keys: Vec<Option<Vec<u8>>>,
    },
    Error(String),
}

/// One row of a collection as `dump_key` reads it back: the member or field,
/// a score if the type has one, and a value if the type has one.
type DumpRow = (Vec<u8>, Option<f64>, Option<Vec<u8>>);

/// Everything one key holds, in a form neither backend stores — see `dump_key`.
enum KeyDump {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Zset(Vec<(Vec<u8>, f64)>),
}

/// Whether this database is served from shared memory rather than SQL. Both
/// halves are reachable from one command only for `COPY`, which is the only one
/// that has to ask.
fn uses_shared_memory(db: u8) -> bool {
    crate::storage_mode() == crate::StorageMode::Memory && is_ephemeral(db)
}

/// What `GETEX` does to the key's expiry once it has read the value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GetExExpiry {
    /// No option given: the expiry is left exactly as it was.
    Keep,
    /// `PERSIST`.
    Clear,
    /// `EX`/`PX`/`EXAT`/`PXAT`, resolved to microseconds since the epoch.
    At(i64),
}

impl Response {
    /// Whether this reply is the one a command gives for a key that is not
    /// there — which is also what it gives for a key holding another type, and
    /// so the only case where a read has to ask which of the two happened.
    fn found_nothing(&self) -> bool {
        match self {
            Response::Null | Response::NullArray | Response::Integer(0) => true,
            Response::Array(v) => v.is_empty(),
            Response::IntegerArray(v) => v.is_empty(),
            Response::BulkString(v) => v.is_empty(),
            _ => false,
        }
    }
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
                let msg = args.first().cloned().ok_or_else(|| wrong_arity("ECHO"))?;
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
                        .parse::<i64>()
                        .map_err(|_| NOT_AN_INTEGER.to_string())
                        .and_then(|n| {
                            u8::try_from(n).map_err(|_| "DB index is out of range".to_string())
                        })?,
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
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            ex_ms = Some(check_expire(secs, 1000, "SET")?);
                            ttl_set = true;
                            i += 2;
                        }
                        "PX" => {
                            let ms: i64 = str_arg(args, i + 1, "SET PX")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            ex_ms = Some(check_expire(ms, 1, "SET")?);
                            ttl_set = true;
                            i += 2;
                        }
                        "EXAT" => {
                            let secs: i64 = str_arg(args, i + 1, "SET EXAT")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            ex_ms = Some(secs.saturating_mul(1000) - now_ms());
                            ttl_set = true;
                            i += 2;
                        }
                        "PXAT" => {
                            let ms: i64 = str_arg(args, i + 1, "SET PXAT")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                ex_secs: check_expire(
                    str_arg(args, 1, "SETEX")?
                        .parse()
                        .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    1,
                    "SETEX",
                )?,
                value: bytes_arg(args, 2, "SETEX")?,
            }),
            "PSETEX" => Ok(Command::PSetEx {
                key: bytes_arg(args, 0, "PSETEX")?,
                ex_ms: check_expire(
                    str_arg(args, 1, "PSETEX")?
                        .parse()
                        .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    1,
                    "PSETEX",
                )?,
                value: bytes_arg(args, 2, "PSETEX")?,
            }),
            "MGET" => {
                if args.is_empty() {
                    return Err(wrong_arity("MGET"));
                }
                Ok(Command::MGet {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "MSET" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return Err(wrong_arity("MSET"));
                }
                let pairs = args
                    .chunks(2)
                    .map(|c| (c[0].to_vec(), c[1].to_vec()))
                    .collect();
                Ok(Command::MSet { pairs })
            }
            "DEL" => {
                if args.is_empty() {
                    return Err(wrong_arity("DEL"));
                }
                Ok(Command::Del {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "EXISTS" => {
                if args.is_empty() {
                    return Err(wrong_arity("EXISTS"));
                }
                Ok(Command::Exists {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "EXPIRE" => Ok(Command::Expire {
                key: bytes_arg(args, 0, "EXPIRE")?,
                secs: {
                    let secs: i64 = str_arg(args, 1, "EXPIRE")?
                        .parse()
                        .map_err(|_| NOT_AN_INTEGER.to_string())?;
                    check_expire_deadline(secs, 1000, "EXPIRE")?;
                    secs
                },
            }),
            "PEXPIRE" => Ok(Command::PExpire {
                key: bytes_arg(args, 0, "PEXPIRE")?,
                ms: {
                    let ms: i64 = str_arg(args, 1, "PEXPIRE")?
                        .parse()
                        .map_err(|_| NOT_AN_INTEGER.to_string())?;
                    check_expire_deadline(ms, 1, "PEXPIRE")?;
                    ms
                },
            }),
            "EXPIREAT" => Ok(Command::ExpireAt {
                key: bytes_arg(args, 0, "EXPIREAT")?,
                unix_secs: str_arg(args, 1, "EXPIREAT")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
            }),
            "PEXPIREAT" => Ok(Command::PExpireAt {
                key: bytes_arg(args, 0, "PEXPIREAT")?,
                unix_ms: str_arg(args, 1, "PEXPIREAT")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
            }),
            "DECRBY" => Ok(Command::DecrBy {
                key: bytes_arg(args, 0, "DECRBY")?,
                delta: str_arg(args, 1, "DECRBY")?
                    .parse::<i64>()
                    .map_err(|_| NOT_AN_INTEGER.to_string())
                    .and_then(|d| {
                        // The command negates its argument, and `i64::MIN` has
                        // no positive counterpart.
                        d.checked_neg()
                            .map(|_| d)
                            .ok_or_else(|| "decrement would overflow".to_string())
                    })?,
            }),
            "INCRBYFLOAT" => Ok(Command::IncrByFloat {
                key: bytes_arg(args, 0, "INCRBYFLOAT")?,
                // NaN is refused as an argument; an infinite increment parses
                // and fails on the result, which is where Redis draws the line.
                delta: str_arg(args, 1, "INCRBYFLOAT")?
                    .parse::<f64>()
                    .ok()
                    .filter(|d| !d.is_nan())
                    .ok_or_else(|| NOT_A_FLOAT.to_string())?,
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
                    return Err(wrong_arity("MSETNX"));
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
            // ASYNC and SYNC are accepted and ignored: nothing here defers the
            // work, so both spellings describe what already happens.
            "FLUSHDB" | "FLUSHALL" => {
                if let Some(mode) = args.first() {
                    let mode = String::from_utf8_lossy(mode).to_uppercase();
                    if mode != "ASYNC" && mode != "SYNC" {
                        return Err("ERR syntax error".to_string());
                    }
                }
                if args.len() > 1 {
                    return Err(wrong_arity(&cmd));
                }
                Ok(match cmd.as_str() {
                    "FLUSHDB" => Command::FlushDb,
                    _ => Command::FlushAll,
                })
            }
            "UNLINK" => {
                if args.is_empty() {
                    return Err(wrong_arity("UNLINK"));
                }
                Ok(Command::Unlink {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "RENAME" => Ok(Command::Rename {
                key: bytes_arg(args, 0, "RENAME")?,
                newkey: bytes_arg(args, 1, "RENAME")?,
            }),
            "RENAMENX" => Ok(Command::RenameNx {
                key: bytes_arg(args, 0, "RENAMENX")?,
                newkey: bytes_arg(args, 1, "RENAMENX")?,
            }),
            "SETRANGE" => {
                let key = bytes_arg(args, 0, "SETRANGE")?;
                let offset: i64 = str_arg(args, 1, "SETRANGE")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                if offset < 0 {
                    return Err("offset is out of range".to_string());
                }
                Ok(Command::SetRange {
                    key,
                    offset,
                    value: bytes_arg(args, 2, "SETRANGE")?,
                })
            }
            "GETRANGE" | "SUBSTR" => Ok(Command::GetRange {
                key: bytes_arg(args, 0, "GETRANGE")?,
                start: str_arg(args, 1, "GETRANGE")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
                end: str_arg(args, 2, "GETRANGE")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
            }),
            "HINCRBYFLOAT" => {
                let key = bytes_arg(args, 0, "HINCRBYFLOAT")?;
                let field = bytes_arg(args, 1, "HINCRBYFLOAT")?;
                let delta: f64 = str_arg(args, 2, "HINCRBYFLOAT")?
                    .parse()
                    .map_err(|_| NOT_A_FLOAT.to_string())?;
                // As `INCRBYFLOAT`: Redis takes an infinite increment happily
                // and draws the line at the result, but never parses a NaN.
                if delta.is_nan() {
                    return Err(NOT_A_FLOAT.to_string());
                }
                Ok(Command::HIncrByFloat { key, field, delta })
            }
            // `LMOVE src dst RIGHT LEFT`, under the name it had before LMOVE.
            "RPOPLPUSH" => Ok(Command::LMove {
                src: bytes_arg(args, 0, "RPOPLPUSH")?,
                dst: bytes_arg(args, 1, "RPOPLPUSH")?,
                src_left: false,
                dst_left: true,
            }),
            "GETEX" => {
                let key = bytes_arg(args, 0, "GETEX")?;
                let expiry = match args.get(1) {
                    None => GetExExpiry::Keep,
                    Some(opt) => {
                        let opt = String::from_utf8_lossy(opt).to_uppercase();
                        if opt == "PERSIST" {
                            if args.len() > 2 {
                                return Err("ERR syntax error".to_string());
                            }
                            GetExExpiry::Clear
                        } else {
                            let n: i64 = str_arg(args, 2, "GETEX")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            if args.len() > 3 {
                                return Err("ERR syntax error".to_string());
                            }
                            let now_ms = now_micros() / 1000;
                            let ms = match opt.as_str() {
                                "EX" => check_expire(n, 1000, "getex")? + now_ms,
                                "PX" => check_expire(n, 1, "getex")? + now_ms,
                                "EXAT" => check_expire_deadline(n, 1000, "getex")?,
                                "PXAT" => check_expire_deadline(n, 1, "getex")?,
                                _ => return Err("ERR syntax error".to_string()),
                            };
                            GetExExpiry::At(ms * 1000)
                        }
                    }
                };
                Ok(Command::GetEx { key, expiry })
            }
            "COPY" => {
                let src = bytes_arg(args, 0, "COPY")?;
                let dst = bytes_arg(args, 1, "COPY")?;
                let (mut db, mut replace) = (None, false);
                let mut i = 2;
                while i < args.len() {
                    match String::from_utf8_lossy(&args[i]).to_uppercase().as_str() {
                        "REPLACE" => {
                            replace = true;
                            i += 1;
                        }
                        "DB" => {
                            let n: i64 = str_arg(args, i + 1, "COPY")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            if !(0..NUM_DBS as i64).contains(&n) {
                                return Err("ERR DB index is out of range".to_string());
                            }
                            db = Some(n as u8);
                            i += 2;
                        }
                        _ => return Err("ERR syntax error".to_string()),
                    }
                }
                Ok(Command::Copy {
                    src,
                    dst,
                    db,
                    replace,
                })
            }
            "HRANDFIELD" => {
                let key = bytes_arg(args, 0, "HRANDFIELD")?;
                let count = match args.get(1) {
                    None => None,
                    Some(_) => Some(
                        str_arg(args, 1, "HRANDFIELD")?
                            .parse::<i64>()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    ),
                };
                let withvalues = match args.get(2) {
                    None => false,
                    Some(a) if a.eq_ignore_ascii_case(b"WITHVALUES") => true,
                    Some(_) => return Err("ERR syntax error".to_string()),
                };
                if args.len() > 3 {
                    return Err("ERR syntax error".to_string());
                }
                Ok(Command::HRandField {
                    key,
                    count,
                    withvalues,
                })
            }
            "SINTERCARD" => {
                let numkeys: i64 = str_arg(args, 0, "SINTERCARD")?
                    .parse()
                    .map_err(|_| "ERR numkeys should be greater than 0".to_string())?;
                if numkeys <= 0 {
                    return Err("ERR numkeys should be greater than 0".to_string());
                }
                let numkeys = numkeys as usize;
                if args.len() < 1 + numkeys {
                    return Err(
                        "ERR Number of keys can't be greater than number of args".to_string()
                    );
                }
                let keys: Vec<Vec<u8>> = args[1..1 + numkeys].to_vec();
                let limit = match args.get(1 + numkeys) {
                    None => None,
                    Some(a) if a.eq_ignore_ascii_case(b"LIMIT") => {
                        let n: i64 = str_arg(args, 2 + numkeys, "SINTERCARD")?
                            .parse()
                            .map_err(|_| "ERR LIMIT can't be negative".to_string())?;
                        if n < 0 {
                            return Err("ERR LIMIT can't be negative".to_string());
                        }
                        // Redis reads 0 as "no limit".
                        (n > 0).then_some(n)
                    }
                    Some(_) => return Err("ERR syntax error".to_string()),
                };
                Ok(Command::SInterCard { keys, limit })
            }
            "OBJECT" => match str_arg(args, 0, "OBJECT")?.to_uppercase().as_str() {
                "ENCODING" => Ok(Command::ObjectEncoding {
                    key: bytes_arg(args, 1, "OBJECT")?,
                }),
                _ => Ok(Command::ObjectOther),
            },
            "SETBIT" => {
                let key = bytes_arg(args, 0, "SETBIT")?;
                let offset = bit_offset(args, 1)?;
                let value = match str_arg(args, 2, "SETBIT")?.as_str() {
                    "0" => false,
                    "1" => true,
                    _ => return Err("ERR bit is not an integer or out of range".to_string()),
                };
                Ok(Command::SetBit { key, offset, value })
            }
            "GETBIT" => Ok(Command::GetBit {
                key: bytes_arg(args, 0, "GETBIT")?,
                offset: bit_offset(args, 1)?,
            }),
            "BITCOUNT" => {
                let key = bytes_arg(args, 0, "BITCOUNT")?;
                let range = match args.len() {
                    1 => None,
                    3 | 4 => {
                        let start: i64 = str_arg(args, 1, "BITCOUNT")?
                            .parse()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?;
                        let end: i64 = str_arg(args, 2, "BITCOUNT")?
                            .parse()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?;
                        let by_bit = match args.get(3) {
                            None => false,
                            Some(a) if a.eq_ignore_ascii_case(b"BIT") => true,
                            Some(a) if a.eq_ignore_ascii_case(b"BYTE") => false,
                            Some(_) => return Err("ERR syntax error".to_string()),
                        };
                        Some((start, end, by_bit))
                    }
                    _ => return Err("ERR syntax error".to_string()),
                };
                Ok(Command::BitCount { key, range })
            }
            "RANDOMKEY" => Ok(Command::RandomKey),
            "SCAN" => {
                let cursor: u64 = str_arg(args, 0, "SCAN")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
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
                    return Err(wrong_arity("HSET"));
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
                    return Err(wrong_arity("HDEL"));
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
                    return Err(wrong_arity("HMGET"));
                }
                Ok(Command::HMGet {
                    key: bytes_arg(args, 0, "HMGET")?,
                    fields: args[1..].iter().map(|a| a.to_vec()).collect(),
                })
            }
            "HMSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return Err(wrong_arity("HMSET"));
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
            }),
            "HSETNX" => Ok(Command::HSetNx {
                key: bytes_arg(args, 0, "HSETNX")?,
                field: bytes_arg(args, 1, "HSETNX")?,
                value: bytes_arg(args, 2, "HSETNX")?,
            }),

            "LPUSH" => {
                if args.len() < 2 {
                    return Err(wrong_arity("LPUSH"));
                }
                let key = bytes_arg(args, 0, "LPUSH")?;
                let values = key_args(args, 1, "LPUSH")?;
                Ok(Command::LPush { key, values })
            }
            "RPUSH" => {
                if args.len() < 2 {
                    return Err(wrong_arity("RPUSH"));
                }
                let key = bytes_arg(args, 0, "RPUSH")?;
                let values = key_args(args, 1, "RPUSH")?;
                Ok(Command::RPush { key, values })
            }
            "LPUSHX" => {
                if args.len() < 2 {
                    return Err(wrong_arity("LPUSHX"));
                }
                let key = bytes_arg(args, 0, "LPUSHX")?;
                let values = key_args(args, 1, "LPUSHX")?;
                Ok(Command::LPushX { key, values })
            }
            "RPUSHX" => {
                if args.len() < 2 {
                    return Err(wrong_arity("RPUSHX"));
                }
                let key = bytes_arg(args, 0, "RPUSHX")?;
                let values = key_args(args, 1, "RPUSHX")?;
                Ok(Command::RPushX { key, values })
            }
            "LPOP" => {
                let key = bytes_arg(args, 0, "LPOP")?;
                let count = if args.len() >= 2 {
                    Some(check_count(
                        str_arg(args, 1, "LPOP")?
                            .parse::<i64>()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    )?)
                } else {
                    None
                };
                Ok(Command::LPop { key, count })
            }
            "RPOP" => {
                let key = bytes_arg(args, 0, "RPOP")?;
                let count = if args.len() >= 2 {
                    Some(check_count(
                        str_arg(args, 1, "RPOP")?
                            .parse::<i64>()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    )?)
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let stop: i64 = str_arg(args, 2, "LRANGE")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                Ok(Command::LRange { key, start, stop })
            }
            "LINDEX" => {
                let key = bytes_arg(args, 0, "LINDEX")?;
                let index: i64 = str_arg(args, 1, "LINDEX")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                Ok(Command::LIndex { key, index })
            }
            "LSET" => {
                let key = bytes_arg(args, 0, "LSET")?;
                let index: i64 = str_arg(args, 1, "LSET")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
                            );
                            i += 2;
                        }
                        "COUNT" => {
                            count = Some(
                                str_arg(args, i + 1, "LPOS COUNT")?
                                    .parse()
                                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
                            );
                            i += 2;
                        }
                        _ => i += 1,
                    }
                }
                if matches!(rank, Some(0)) {
                    return Err("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list".to_string());
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let stop: i64 = str_arg(args, 2, "LTRIM")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                Ok(Command::LTrim { key, start, stop })
            }

            "SADD" => {
                if args.len() < 2 {
                    return Err(wrong_arity("SADD"));
                }
                let key = bytes_arg(args, 0, "SADD")?;
                let members = key_args(args, 1, "SADD")?;
                Ok(Command::SAdd { key, members })
            }
            "SREM" => {
                if args.len() < 2 {
                    return Err(wrong_arity("SREM"));
                }
                let key = bytes_arg(args, 0, "SREM")?;
                let members = key_args(args, 1, "SREM")?;
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
                    return Err(wrong_arity("SMISMEMBER"));
                }
                let key = bytes_arg(args, 0, "SMISMEMBER")?;
                let members = key_args(args, 1, "SMISMEMBER")?;
                Ok(Command::SMisMember { key, members })
            }
            "SPOP" => {
                let key = bytes_arg(args, 0, "SPOP")?;
                let count = if args.len() >= 2 {
                    Some(check_count(
                        str_arg(args, 1, "SPOP")?
                            .parse::<i64>()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    )?)
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
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    )
                } else {
                    None
                };
                Ok(Command::SRandMember { key, count })
            }
            "SUNION" => {
                if args.is_empty() {
                    return Err(wrong_arity("SUNION"));
                }
                let keys = key_args(args, 0, "SUNION")?;
                Ok(Command::SUnion { keys })
            }
            "SINTER" => {
                if args.is_empty() {
                    return Err(wrong_arity("SINTER"));
                }
                let keys = key_args(args, 0, "SINTER")?;
                Ok(Command::SInter { keys })
            }
            "SDIFF" => {
                if args.is_empty() {
                    return Err(wrong_arity("SDIFF"));
                }
                let keys = key_args(args, 0, "SDIFF")?;
                Ok(Command::SDiff { keys })
            }
            "SUNIONSTORE" => {
                if args.len() < 2 {
                    return Err(wrong_arity("SUNIONSTORE"));
                }
                let dst = bytes_arg(args, 0, "SUNIONSTORE")?;
                let keys = key_args(args, 1, "SUNIONSTORE")?;
                Ok(Command::SUnionStore { dst, keys })
            }
            "SINTERSTORE" => {
                if args.len() < 2 {
                    return Err(wrong_arity("SINTERSTORE"));
                }
                let dst = bytes_arg(args, 0, "SINTERSTORE")?;
                let keys = key_args(args, 1, "SINTERSTORE")?;
                Ok(Command::SInterStore { dst, keys })
            }
            "SDIFFSTORE" => {
                if args.len() < 2 {
                    return Err(wrong_arity("SDIFFSTORE"));
                }
                let dst = bytes_arg(args, 0, "SDIFFSTORE")?;
                let keys = key_args(args, 1, "SDIFFSTORE")?;
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
                    return Err(wrong_arity("ZREM"));
                }
                let key = bytes_arg(args, 0, "ZREM")?;
                let members = key_args(args, 1, "ZREM")?;
                Ok(Command::ZRem { key, members })
            }
            "ZSCORE" => Ok(Command::ZScore {
                key: bytes_arg(args, 0, "ZSCORE")?,
                member: bytes_arg(args, 1, "ZSCORE")?,
            }),
            "ZMSCORE" => {
                if args.len() < 2 {
                    return Err(wrong_arity("ZMSCORE"));
                }
                let key = bytes_arg(args, 0, "ZMSCORE")?;
                let members = key_args(args, 1, "ZMSCORE")?;
                Ok(Command::ZMScore { key, members })
            }
            "ZINCRBY" => {
                let key = bytes_arg(args, 0, "ZINCRBY")?;
                let increment = parse_score_value(&bytes_arg(args, 1, "ZINCRBY")?)
                    .ok_or_else(|| NOT_A_FLOAT.to_string())?;
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
                    .ok_or_else(|| "min or max is not a float".to_string())?;
                let max = parse_score_bound(&bytes_arg(args, 2, "ZCOUNT")?)
                    .ok_or_else(|| "min or max is not a float".to_string())?;
                Ok(Command::ZCount { key, min, max })
            }
            "ZLEXCOUNT" => {
                let key = bytes_arg(args, 0, "ZLEXCOUNT")?;
                let min = parse_lex_bound(&bytes_arg(args, 1, "ZLEXCOUNT")?)
                    .ok_or_else(|| "min or max not valid string range item".to_string())?;
                let max = parse_lex_bound(&bytes_arg(args, 2, "ZLEXCOUNT")?)
                    .ok_or_else(|| "min or max not valid string range item".to_string())?;
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
                    Some(check_count(
                        str_arg(args, 1, "ZPOPMIN")?
                            .parse::<i64>()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    )?)
                } else {
                    None
                };
                Ok(Command::ZPopMin { key, count })
            }
            "ZPOPMAX" => {
                let key = bytes_arg(args, 0, "ZPOPMAX")?;
                let count = if args.len() >= 2 {
                    Some(check_count(
                        str_arg(args, 1, "ZPOPMAX")?
                            .parse::<i64>()
                            .map_err(|_| NOT_AN_INTEGER.to_string())?,
                    )?)
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
                        .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let stop: i64 = str_arg(args, 2, "ZREMRANGEBYRANK")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                Ok(Command::ZRemRangeByRank { key, start, stop })
            }
            "ZREMRANGEBYSCORE" => {
                let key = bytes_arg(args, 0, "ZREMRANGEBYSCORE")?;
                let min = parse_score_bound(&bytes_arg(args, 1, "ZREMRANGEBYSCORE")?)
                    .ok_or_else(|| "min or max is not a float".to_string())?;
                let max = parse_score_bound(&bytes_arg(args, 2, "ZREMRANGEBYSCORE")?)
                    .ok_or_else(|| "min or max is not a float".to_string())?;
                Ok(Command::ZRemRangeByScore { key, min, max })
            }
            "ZREMRANGEBYLEX" => {
                let key = bytes_arg(args, 0, "ZREMRANGEBYLEX")?;
                let min = parse_lex_bound(&bytes_arg(args, 1, "ZREMRANGEBYLEX")?)
                    .ok_or_else(|| "min or max not valid string range item".to_string())?;
                let max = parse_lex_bound(&bytes_arg(args, 2, "ZREMRANGEBYLEX")?)
                    .ok_or_else(|| "min or max not valid string range item".to_string())?;
                Ok(Command::ZRemRangeByLex { key, min, max })
            }
            "ZUNION" => parse_zaggregate(args, false, false),
            "ZINTER" => parse_zaggregate(args, true, false),
            "ZDIFF" => parse_zdiff(args, false),
            "ZUNIONSTORE" => parse_zaggregate_store(args, false),
            "ZINTERSTORE" => parse_zaggregate_store(args, true),
            "ZDIFFSTORE" => {
                if args.len() < 3 {
                    return Err(wrong_arity("ZDIFFSTORE"));
                }
                let dst = bytes_arg(args, 0, "ZDIFFSTORE")?;
                let numkeys: usize = str_arg(args, 1, "ZDIFFSTORE")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                    return Err(wrong_arity("SUBSCRIBE"));
                }
                Ok(Command::Subscribe {
                    channels: args.to_vec(),
                })
            }
            "UNSUBSCRIBE" => Ok(Command::Unsubscribe),
            "PSUBSCRIBE" => {
                if args.is_empty() {
                    return Err(wrong_arity("PSUBSCRIBE"));
                }
                Ok(Command::PSubscribe {
                    patterns: args.to_vec(),
                })
            }
            "PUNSUBSCRIBE" => Ok(Command::PUnsubscribe),
            "PUBLISH" => {
                if args.len() != 2 {
                    return Err(wrong_arity("PUBLISH"));
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
                    return Err(wrong_arity("WATCH"));
                }
                Ok(Command::Watch {
                    keys: args.iter().map(|a| a.to_vec()).collect(),
                })
            }
            "UNWATCH" => Ok(Command::Unwatch),

            // Redis names the command and then quotes each argument, one
            // space apart, with a trailing space — the whole of it, that
            // space included, is what a client's error handling matches on.
            _ => Err(format!(
                "unknown command '{cmd}', with args beginning with: {}",
                args.iter()
                    .map(|a| format!("'{}' ", String::from_utf8_lossy(a)))
                    .collect::<String>()
            )),
        }
    }

    pub fn execute(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
        // A write has to be refused before it lands, so its keys are checked up
        // front. A read cannot damage anything, and a read of a key holding
        // another type finds nothing — so it runs first and only asks what the
        // key holds when it came back empty. That keeps the extra round trip
        // off every successful `GET` and `GETRANGE`.
        //
        // A read of a *collection* is the exception, and has to be. Its rows
        // outlive the key's expiry until the sweep runs, so coming back with
        // data is not evidence the key is live — which is how `LLEN` answered
        // 3 for a key `TYPE` had already called gone. Asking first is what
        // reaps it. Strings are excluded because they never had the problem:
        // the expiry is a column of `kv_N` and every statement reading one
        // tests it, which is why the ordering above could be an optimisation
        // in the first place.
        let writes = !self.write_keys().is_empty();
        if writes && let Some(err) = self.sql_type_error(client, db) {
            return err;
        }
        if !writes && self.reads_a_collection() {
            self.sql_reap_expired(client, db);
        }
        let response = self.execute_inner(client, db);
        if !writes
            && response.found_nothing()
            && let Some(err) = self.sql_type_error(client, db)
        {
            return err;
        }
        response
    }

    /// Everything one key holds, whatever type holds it. `COPY` is the only
    /// command that has to move a whole key between two backends, so it is the
    /// only one that needs the value in a form neither of them stores.
    fn dump_key(client: &mut SpiClient<'_>, db: u8, key: &[u8]) -> Option<(KeyDump, i64)> {
        use crate::mem::KeyKind;
        let mem = uses_shared_memory(db);
        let kind = match mem {
            true => unsafe { crate::mem::mem_key_kind(db as usize, key) }?,
            false => Self::sql_key_kinds(client, db, &[key])
                .first()
                .and_then(|(_, k)| *k)?,
        };
        let idx = db as usize;
        let dump = match (kind, mem) {
            (KeyKind::String, true) => KeyDump::String(unsafe { crate::mem::mem_get(idx, key) }?),
            (KeyKind::List, true) => {
                KeyDump::List(unsafe { crate::mem::mem_lrange(idx, key, 0, -1) })
            }
            (KeyKind::Set, true) => KeyDump::Set(unsafe { crate::mem::mem_smembers(idx, key) }),
            (KeyKind::Hash, true) => KeyDump::Hash(unsafe { crate::mem::mem_hgetall(idx, key) }),
            (KeyKind::Zset, true) => {
                KeyDump::Zset(unsafe { crate::mem::mem_zset_collect_all(idx, key) })
            }
            (KeyKind::String, false) => KeyDump::String(
                client
                    .select(&sql::GET[idx], None, &[key.into()])
                    .ok()?
                    .first()
                    .get(1)
                    .ok()
                    .flatten()?,
            ),
            (kind, false) => {
                let sql = match kind {
                    KeyKind::List => format!(
                        "SELECT value, NULL::float8 FROM redis.list_{db} \
                         WHERE key = $1 ORDER BY pos"
                    ),
                    KeyKind::Set => {
                        format!("SELECT member, NULL::float8 FROM redis.set_{db} WHERE key = $1")
                    }
                    KeyKind::Zset => {
                        format!("SELECT member, score FROM redis.zset_{db} WHERE key = $1")
                    }
                    _ => format!(
                        "SELECT field, NULL::float8, value FROM redis.hash_{db} WHERE key = $1"
                    ),
                };
                let tbl = client.select(&sql, None, &[key.into()]).ok()?;
                let mut items: Vec<DumpRow> = Vec::new();
                for row in tbl {
                    let Ok(Some(a)) = row.get::<Vec<u8>>(1) else {
                        continue;
                    };
                    let score = row.get::<f64>(2).ok().flatten();
                    let value = row.get::<Vec<u8>>(3).ok().flatten();
                    items.push((a, score, value));
                }
                match kind {
                    KeyKind::List => KeyDump::List(items.into_iter().map(|(a, _, _)| a).collect()),
                    KeyKind::Set => KeyDump::Set(items.into_iter().map(|(a, _, _)| a).collect()),
                    KeyKind::Zset => KeyDump::Zset(
                        items
                            .into_iter()
                            .map(|(a, s, _)| (a, s.unwrap_or(0.0)))
                            .collect(),
                    ),
                    _ => KeyDump::Hash(
                        items
                            .into_iter()
                            .map(|(a, _, v)| (a, v.unwrap_or_default()))
                            .collect(),
                    ),
                }
            }
        };
        let (_, expires_at) = match mem {
            true => unsafe { crate::mem::mem_ttl_raw(idx, key) },
            false => (true, 0),
        };
        Some((dump, expires_at))
    }

    /// Write back what `dump_key` read, into whichever backend `db` names.
    fn restore_key(client: &mut SpiClient<'_>, db: u8, key: &[u8], dump: &KeyDump) {
        let idx = db as usize;
        if uses_shared_memory(db) {
            unsafe {
                match dump {
                    KeyDump::String(v) => {
                        crate::mem::mem_set(idx, key, v, 0);
                    }
                    KeyDump::List(items) => {
                        let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
                        crate::mem::mem_rpush(idx, key, &refs);
                    }
                    KeyDump::Set(items) => {
                        let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
                        crate::mem::mem_sadd(idx, key, &refs);
                    }
                    KeyDump::Hash(pairs) => {
                        for (f, v) in pairs {
                            crate::mem::mem_hset(idx, key, f, v);
                        }
                    }
                    KeyDump::Zset(pairs) => {
                        let scored: Vec<(f64, &[u8])> =
                            pairs.iter().map(|(m, s)| (*s, m.as_slice())).collect();
                        crate::mem::mem_zadd(idx, key, &scored, false, false, false, false, false);
                    }
                }
            }
            return;
        }
        match dump {
            KeyDump::String(v) => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
                );
                let _ = client.update(&sql, None, &[key.into(), v.clone().into()]);
            }
            KeyDump::List(items) => {
                let sql = format!(
                    "INSERT INTO redis.list_{db} (key, pos, value) \
                     SELECT $1, i - 1, v FROM unnest($2::bytea[]) WITH ORDINALITY AS t(v, i)"
                );
                let vals: Vec<Option<Vec<u8>>> = items.iter().map(|v| Some(v.clone())).collect();
                let _ = client.update(&sql, None, &[key.into(), vals.into()]);
            }
            KeyDump::Set(items) => {
                let sql = format!(
                    "INSERT INTO redis.set_{db} (key, member) \
                     SELECT $1, m FROM unnest($2::bytea[]) AS t(m) \
                     ON CONFLICT DO NOTHING"
                );
                let vals: Vec<Option<Vec<u8>>> = items.iter().map(|v| Some(v.clone())).collect();
                let _ = client.update(&sql, None, &[key.into(), vals.into()]);
            }
            KeyDump::Hash(pairs) => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     SELECT $1, f, v FROM unnest($2::bytea[], $3::bytea[]) AS t(f, v) \
                     ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
                );
                let fs: Vec<Option<Vec<u8>>> = pairs.iter().map(|(f, _)| Some(f.clone())).collect();
                let vs: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                let _ = client.update(&sql, None, &[key.into(), fs.into(), vs.into()]);
            }
            KeyDump::Zset(pairs) => {
                let sql = format!(
                    "INSERT INTO redis.zset_{db} (key, member, score) \
                     SELECT $1, m, s FROM unnest($2::bytea[], $3::float8[]) AS t(m, s) \
                     ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score"
                );
                let ms: Vec<Option<Vec<u8>>> = pairs.iter().map(|(m, _)| Some(m.clone())).collect();
                let ss: Vec<Option<f64>> = pairs.iter().map(|(_, s)| Some(*s)).collect();
                let _ = client.update(&sql, None, &[key.into(), ms.into(), ss.into()]);
            }
        }
    }

    /// `rename_to`, for the shared-memory half.
    fn mem_rename(&self, db_idx: usize, key: &[u8], newkey: &[u8]) -> Response {
        use crate::mem;
        // The expiry travels with the key: read it before the value is taken,
        // since the entry is gone afterwards.
        let (_, expires_at) = unsafe { mem::mem_ttl_raw(db_idx, key) };
        match unsafe { mem::mem_getdel(db_idx, key) } {
            Some(v) => {
                // Only now that the source is known to be there: a rename that
                // fails must leave the destination alone.
                mem_clear_other_type(db_idx, newkey, mem::KeyKind::String);
                unsafe { mem::mem_set(db_idx, newkey, &v, expires_at) };
                Response::Ok
            }
            None => Response::Error("ERR no such key".to_string()),
        }
    }

    /// Move a string from `key` to `newkey`, clearing the destination first.
    /// `RENAME` answers `+OK`; `RENAMENX` turns that into `1` once it has
    /// established the destination was free.
    fn rename_to(client: &mut SpiClient<'_>, db: u8, key: &[u8], newkey: &[u8]) -> Response {
        // The destination goes first, in its own statement: renaming
        // onto an existing key is a primary-key violation otherwise,
        // and a CTE that deletes it is not visible to the update in the
        // same statement. Renaming a key to itself must not delete it.
        if key != newkey {
            // Only when the source is there: a RENAME that answers "no
            // such key" must leave the destination alone.
            let del = format!(
                "DELETE FROM redis.kv_{db} WHERE key = $1 \
                     AND EXISTS (SELECT 1 FROM redis.kv_{db} s WHERE s.key = $2)"
            );
            if let Err(e) = client.update(&del, None, &[newkey.into(), key.into()]) {
                pgrx::warning!("pg_redis: RENAME could not clear the destination: {e}");
                return Response::Error("ERR internal error".to_string());
            }
        }
        let sql = format!(
            "WITH r AS ( \
                     UPDATE redis.kv_{db} SET key = $2 WHERE key = $1 RETURNING 1 \
                 ) \
                 SELECT (SELECT count(*) FROM r) > 0 AS renamed"
        );
        match client.update(&sql, None, &[key.into(), newkey.into()]) {
            Ok(tbl) => {
                let renamed = tbl.first().get::<bool>(1).ok().flatten().unwrap_or(false);
                if renamed {
                    Response::Ok
                } else {
                    Response::Error("ERR no such key".to_string())
                }
            }
            Err(e) => spi_failed("RENAME", e),
        }
    }

    /// What each of `keys` holds, in one round trip. The five tables are keyed
    /// independently, so this is the durable half's key directory: derived on
    /// demand rather than stored, which is a query per typed command and no
    /// second copy of the truth to keep in step.
    /// What each key holds, with `None` for one whose expiry has passed.
    ///
    /// Absent keys are simply missing from the result; a key that is present
    /// but past its deadline comes back as `None`, because the caller has to
    /// tell the two apart. It is the same distinction `dir_lookup_raw` keeps on
    /// the shared-memory half, and for the same reason: a collection's rows
    /// outlive its expiry until the sweep runs, and every collection read goes
    /// to the rows rather than to `redis.expiry_N`.
    fn sql_key_kinds(
        client: &mut SpiClient<'_>,
        db: u8,
        keys: &[&[u8]],
    ) -> Vec<(Vec<u8>, Option<crate::mem::KeyKind>)> {
        use crate::mem::KeyKind;
        let sql = format!(
            "SELECT a.key, \
                    CASE \
                      WHEN EXISTS (SELECT 1 FROM redis.kv_{db} t WHERE t.key = a.key \
                                     AND (t.expires_at IS NULL OR t.expires_at > now())) THEN 'string' \
                      WHEN EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = a.key \
                                     AND e.expires_at <= now()) THEN 'expired' \
                      WHEN EXISTS (SELECT 1 FROM redis.list_{db} t WHERE t.key = a.key) THEN 'list' \
                      WHEN EXISTS (SELECT 1 FROM redis.set_{db}  t WHERE t.key = a.key) THEN 'set' \
                      WHEN EXISTS (SELECT 1 FROM redis.hash_{db} t WHERE t.key = a.key) THEN 'hash' \
                      WHEN EXISTS (SELECT 1 FROM redis.zset_{db} t WHERE t.key = a.key) THEN 'zset' \
                    END AS kind \
             FROM unnest($1::bytea[]) AS a(key)"
        );
        let args: Vec<Option<Vec<u8>>> = keys.iter().map(|k| Some(k.to_vec())).collect();
        let Ok(tbl) = client.select(&sql, None, &[args.into()]) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for row in tbl {
            let (Ok(Some(key)), Ok(Some(kind))) = (row.get::<Vec<u8>>(1), row.get::<String>(2))
            else {
                continue;
            };
            let kind = match kind.as_str() {
                "string" => Some(KeyKind::String),
                "list" => Some(KeyKind::List),
                "set" => Some(KeyKind::Set),
                "hash" => Some(KeyKind::Hash),
                "zset" => Some(KeyKind::Zset),
                "expired" => None,
                _ => continue,
            };
            out.push((key, kind));
        }
        out
    }

    /// Delete everything every table holds for `key`. What `SET` does to a key
    /// that was a list: Redis replaces the value rather than refusing.
    fn sql_drop_key(client: &mut SpiClient<'_>, db: u8, key: &[u8]) {
        let sql = format!(
            "WITH d1 AS (DELETE FROM redis.kv_{db}     WHERE key = $1), \
                  d2 AS (DELETE FROM redis.hash_{db}   WHERE key = $1), \
                  d3 AS (DELETE FROM redis.list_{db}   WHERE key = $1), \
                  d4 AS (DELETE FROM redis.set_{db}    WHERE key = $1), \
                  d5 AS (DELETE FROM redis.zset_{db}   WHERE key = $1), \
                  d6 AS (DELETE FROM redis.expiry_{db} WHERE key = $1) \
             SELECT 1"
        );
        let _ = client.update(&sql, None, &[key.into()]);
    }

    /// The durable half's `mem_type_error`: refuse a command whose key holds
    /// another type, and clear a key the command is about to replace.
    fn sql_type_error(&self, client: &mut SpiClient<'_>, db: u8) -> Option<Response> {
        let typed = self.typed_keys();
        if typed.is_empty() {
            // Nothing to refuse, so nothing to look up. The commands that only
            // replace a key — `SET` and its kin — clear what was there from
            // inside their own statement rather than paying a round trip to
            // find out there was nothing to clear, which is the common case and
            // halved their throughput when they did.
            return None;
        }
        let overwrites = self.overwritten_keys();
        let lookup: Vec<&[u8]> = typed
            .iter()
            .map(|(k, _)| *k)
            .chain(overwrites.iter().map(|(k, _)| *k))
            .collect();
        let kinds = Self::sql_key_kinds(client, db, &lookup);

        // Reap before deciding anything. A key past its expiry is one the
        // sweep has not reached yet: `TYPE` and `EXISTS` already call it gone,
        // and its rows are still in the four collection tables, which is where
        // `LLEN`, `SMEMBERS`, `HGETALL` and the rest read from. Dropping it
        // here is what stops the command being answered out of them.
        for (key, kind) in &kinds {
            if kind.is_none() {
                Self::sql_drop_key(client, db, key);
            }
        }

        // Reaped keys are gone now, so they read as absent from here on.
        let kind_of = |key: &[u8]| {
            kinds
                .iter()
                .find(|(k, _)| k.as_slice() == key)
                .and_then(|(_, kind)| *kind)
        };
        for (key, accepted) in &typed {
            if let Some(kind) = kind_of(key)
                && !accepted.contains(&kind)
            {
                return Some(wrong_type());
            }
        }
        for (key, becomes) in &overwrites {
            match kind_of(key) {
                Some(kind) if kind != *becomes => Self::sql_drop_key(client, db, key),
                // Replacing a collection with one of its own type keeps the
                // rows the command is about to rewrite, but not the expiry:
                // Redis discards a key's TTL whenever its value is replaced.
                Some(_) => {
                    let sql = format!("DELETE FROM redis.expiry_{db} WHERE key = $1");
                    let _ = client.update(&sql, None, &[(*key).into()]);
                }
                None => {}
            }
        }
        None
    }

    fn execute_inner(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
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
                bulk_query(client, Spi::Reads, "GET", &sql::GET[db as usize], &[key.as_slice().into()])
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
                // The NX and XX branches below test `redis.kv_N` alone, which
                // cannot see a list or a set holding the key. Settle that here,
                // where the whole key directory is in reach: `NX` declines, and
                // `XX` — whose condition the collection satisfies — replaces it
                // and writes unconditionally.
                let (nx, xx) = {
                    let held = (*nx || *xx)
                        .then(|| Self::sql_key_kinds(client, db, &[key.as_slice()]))
                        .and_then(|k| k.first().and_then(|(_, kind)| *kind))
                        .filter(|k| *k != crate::mem::KeyKind::String);
                    match held {
                        Some(_) if *nx => return Response::Null,
                        Some(_) => {
                            Self::sql_drop_key(client, db, key);
                            (false, false)
                        }
                        None => (*nx, *xx),
                    }
                };
                let (nx, xx) = (&nx, &xx);

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
                // An unconditional write replaces whatever held the key. The
                // conditional forms have already settled that above, and `GET`
                // demands a string, so neither needs the clearing CTEs.
                let clear = if *nx || *xx || *get {
                    String::new()
                } else {
                    sql::clear_for_string(db as usize)
                };
                let sql = format!(
                    "WITH {clear}{old_cte}{write_cte} \
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
                    Err(e) => spi_failed("SET", e),
                }
            }

            Command::SetEx { key, value, ex_secs } => {
                let clear = sql::clear_for_string(db as usize);
                let sql = format!(
                    "WITH {clear}w AS ( \
                         INSERT INTO redis.kv_{db} (key, value, expires_at) \
                         VALUES ($1, $2, now() + ($3::bigint * interval '1 second')) \
                         ON CONFLICT (key) DO UPDATE \
                         SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at) \
                     SELECT 1"
                );
                let args: &[DatumWithOid] =
                    &[key.as_slice().into(), value.as_slice().into(), (*ex_secs).into()];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => spi_failed("SETEX", e),
                }
            }

            Command::PSetEx { key, value, ex_ms } => {
                let clear = sql::clear_for_string(db as usize);
                let sql = format!(
                    "WITH {clear}w AS ( \
                         INSERT INTO redis.kv_{db} (key, value, expires_at) \
                         VALUES ($1, $2, now() + ($3::bigint * interval '1 millisecond')) \
                         ON CONFLICT (key) DO UPDATE \
                         SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at) \
                     SELECT 1"
                );
                let args: &[DatumWithOid] =
                    &[key.as_slice().into(), value.as_slice().into(), (*ex_ms).into()];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => spi_failed("PSETEX", e),
                }
            }

            Command::MGet { keys } => {
                let keys_vec = key_array(keys);
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
                    Err(e) => spi_failed("MGET", e),
                }
            }

            Command::MSet { pairs } => {
                // As `SET`, for every key at once.
                let clear = ["hash", "list", "set", "zset", "expiry"]
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        format!("clr{i} AS (DELETE FROM redis.{t}_{db} WHERE key = ANY($1::bytea[]))")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "WITH {clear}, w AS ( \
                         INSERT INTO redis.kv_{db} (key, value, expires_at) \
                         SELECT unnest($1::bytea[]), unnest($2::bytea[]), NULL \
                         ON CONFLICT (key) DO UPDATE \
                         SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at) \
                     SELECT 1"
                );
                let deduped = dedup_last(pairs, |(k, _)| k.as_slice());
                let keys: Vec<Option<Vec<u8>>> =
                    deduped.iter().map(|(k, _)| Some(k.clone())).collect();
                let vals: Vec<Option<Vec<u8>>> =
                    deduped.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(&sql, None, &[keys.into(), vals.into()]) {
                    Ok(_) => Response::Ok,
                    Err(e) => spi_failed("MSET", e),
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
                let keys_vec = key_array(keys);
                integer_query(client, Spi::Writes, "DEL", &sql, &[keys_vec.into()], 0)
            }

            Command::Exists { keys } => {
                // Once per argument, duplicates included: `EXISTS k k` is 2 in
                // Redis when k is there.
                let sql = format!(
                    "SELECT count(*)::bigint FROM unnest($1::bytea[]) AS a(key) \
                     WHERE EXISTS (SELECT 1 FROM redis.kv_{db} t WHERE t.key = a.key \
                                     AND (t.expires_at IS NULL OR t.expires_at > now())) \
                        OR (NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e \
                                          WHERE e.key = a.key AND e.expires_at <= now()) \
                            AND (EXISTS (SELECT 1 FROM redis.hash_{db} t WHERE t.key = a.key) \
                              OR EXISTS (SELECT 1 FROM redis.list_{db} t WHERE t.key = a.key) \
                              OR EXISTS (SELECT 1 FROM redis.set_{db}  t WHERE t.key = a.key) \
                              OR EXISTS (SELECT 1 FROM redis.zset_{db} t WHERE t.key = a.key)))"
                );
                let keys_vec = key_array(keys);
                integer_query(client, Spi::Reads, "EXISTS", &sql, &[keys_vec.into()], 0)
            }

            Command::Expire { key, secs } => update_expiry(
                client,
                &sql::set_expiry(db as usize, "now() + ($2::bigint * interval '1 second')"),
                key,
                *secs,
            ),

            Command::PExpire { key, ms } => update_expiry(
                client,
                &sql::set_expiry(
                    db as usize,
                    "now() + ($2::bigint * interval '1 millisecond')",
                ),
                key,
                *ms,
            ),

            Command::ExpireAt { key, unix_secs } => update_expiry(
                client,
                &sql::set_expiry(db as usize, "to_timestamp($2::bigint)"),
                key,
                *unix_secs,
            ),

            Command::PExpireAt { key, unix_ms } => {
                let sql = sql::set_expiry(db as usize, "to_timestamp($2::float8 / 1000.0)");
                let ms_f = *unix_ms as f64;
                match client.update(&sql, None, &[key.as_slice().into(), ms_f.into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => spi_failed("PEXPIREAT", e),
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
                    Err(e) => spi_failed("PERSIST", e),
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
                        pgrx::warning!("pg_redis: INCR failed: {e}");
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
                        pgrx::warning!("pg_redis: DECR failed: {e}");
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
                        pgrx::warning!("pg_redis: INCRBY failed: {e}");
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
                        pgrx::warning!("pg_redis: DECRBY failed: {e}");
                        Response::Error("ERR value is not an integer or out of range".to_string())
                    }
                }
            }

            Command::IncrByFloat { key, delta } => {
                match client.update(&sql::INCRBYFLOAT[db as usize], None, &[key.as_slice().into(), (*delta).into(), (*delta).into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        // An error rolls the write back, so a result that is
                        // not a number is refused rather than stored.
                        Ok(Some(v)) if is_finite_number(&v) => Response::BulkString(v),
                        Ok(Some(_)) => Response::Error(
                            "ERR increment would produce NaN or Infinity".to_string(),
                        ),
                        _ => Response::Error("ERR value is not a valid float".to_string()),
                    },
                    Err(e) => {
                        pgrx::warning!("pg_redis: INCRBYFLOAT failed: {e}");
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
                    Err(e) => spi_failed("APPEND", e),
                }
            }

            Command::Strlen { key } => {
                integer_query(client, Spi::Reads, "STRLEN", &sql::STRLEN[db as usize], &[key.as_slice().into()], 0)
            }

            Command::GetDel { key } => {
                match client.update(&sql::GETDEL[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => spi_failed("GETDEL", e),
                }
            }

            Command::GetSet { key, value } => {
                // The expiry goes with the value it replaces, as SET does.
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) VALUES ($1, $2, NULL) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = NULL \
                     RETURNING (SELECT value FROM redis.kv_{db} WHERE key = $1)"
                );
                match client.update(&sql, None, &[key.as_slice().into(), value.as_slice().into()]) {
                    Ok(tbl) => match tbl.first().get::<Vec<u8>>(1) {
                        Ok(Some(v)) => Response::BulkString(v),
                        _ => Response::Null,
                    },
                    Err(e) => spi_failed("GETSET", e),
                }
            }

            Command::SetNx { key, value } => {
                // "The key does not exist" is a question about every table, not
                // just the KV one: `SETNX` over a list is 0, not a second value.
                if !Self::sql_key_kinds(client, db, &[key.as_slice()]).is_empty() {
                    return Response::Integer(0);
                }
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO NOTHING"
                );
                match client.update(&sql, None, &[key.as_slice().into(), value.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => spi_failed("SETNX", e),
                }
            }

            Command::MSetNx { pairs } => {
                let keys: Vec<Option<Vec<u8>>> =
                    pairs.iter().map(|(k, _)| Some(k.clone())).collect();
                let named: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
                if !Self::sql_key_kinds(client, db, &named).is_empty() {
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
                    Err(e) => spi_failed("MSETNX", e),
                }
            }

            Command::Type { key } => {
                let sql = format!(
                    "SELECT 'string' WHERE EXISTS ( \
                         SELECT 1 FROM redis.kv_{db} \
                         WHERE key = $1 AND (expires_at IS NULL OR expires_at > now()) \
                     ) \
                     UNION ALL SELECT v.t FROM (VALUES ('list'), ('set'), ('hash'), ('zset')) v(t) \
                      WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} \
                                         WHERE key = $1 AND expires_at <= now()) \
                        AND CASE v.t \
                              WHEN 'list' THEN EXISTS (SELECT 1 FROM redis.list_{db} WHERE key = $1) \
                              WHEN 'set'  THEN EXISTS (SELECT 1 FROM redis.set_{db}  WHERE key = $1) \
                              WHEN 'hash' THEN EXISTS (SELECT 1 FROM redis.hash_{db} WHERE key = $1) \
                              ELSE             EXISTS (SELECT 1 FROM redis.zset_{db} WHERE key = $1) \
                            END \
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
                    Err(e) => spi_failed("TYPE", e),
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
                         SELECT c.key FROM ( \
                             SELECT DISTINCT key FROM redis.hash_{db} \
                             UNION SELECT DISTINCT key FROM redis.list_{db} \
                             UNION SELECT DISTINCT key FROM redis.set_{db} \
                             UNION SELECT DISTINCT key FROM redis.zset_{db} \
                         ) c \
                          WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e \
                                             WHERE e.key = c.key AND e.expires_at <= now()) \
                     ) u"
                );
                integer_query(client, Spi::Reads, "DBSIZE", &sql, &[], 0)
            }

            // Redis pads the gap with NUL bytes, so an offset past the end is
            // not an error but a hole. `overlay` cannot do that on its own.
            Command::SetRange { key, offset, value } => {
                if value.is_empty() {
                    // Redis writes nothing and reports the length as it stands.
                    return match client.select(&sql::STRLEN[db as usize], None, &[key.as_slice().into()]) {
                        Ok(tbl) => Response::Integer(
                            tbl.first().get::<i64>(1).ok().flatten().unwrap_or(0),
                        ),
                        Err(e) => spi_failed("SETRANGE", e),
                    };
                }
                // `chr(0)` is not a legal text character in PostgreSQL, so the
                // gap is built as hex rather than repeated text.
                let sql = format!(
                    "WITH old AS (SELECT value FROM redis.kv_{db} \
                                   WHERE key = $1 \
                                     AND (expires_at IS NULL OR expires_at > now())), \
                          padded AS ( \
                              SELECT coalesce((SELECT value FROM old), '') \
                                     || decode(repeat('00', GREATEST(0, $2::int \
                                          - coalesce(length((SELECT value FROM old)), 0))), \
                                        'hex') AS v) \
                     INSERT INTO redis.kv_{db} (key, value) \
                     SELECT $1, \
                            substring(v from 1 for $2::int) || $3 \
                            || coalesce(substring(v from $2::int + length($3) + 1), '') \
                     FROM padded \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value \
                     RETURNING length(value)::bigint"
                );
                let args: &[DatumWithOid] =
                    &[key.as_slice().into(), (*offset as i32).into(), value.as_slice().into()];
                match client.update(&sql, None, args) {
                    Ok(tbl) => {
                        Response::Integer(tbl.first().get::<i64>(1).ok().flatten().unwrap_or(0))
                    }
                    Err(e) => spi_failed("SETRANGE", e),
                }
            }

            Command::GetRange { key, start, end } => {
                let sql = format!(
                    "SELECT substring(value from $2::int for $3::int) \
                     FROM redis.kv_{db} \
                     WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())"
                );
                match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let _ = sql;
                        let v: Vec<u8> = tbl.first().get::<Vec<u8>>(1).ok().flatten().unwrap_or_default();
                        Response::BulkString(byte_range(&v, *start, *end))
                    }
                    Err(e) => spi_failed("GETRANGE", e),
                }
            }

            Command::HIncrByFloat { key, field, delta } => {
                let sql = format!(
                    "SELECT encode(value, 'escape') FROM redis.hash_{db} \
                     WHERE key = $1 AND field = $2"
                );
                let cur = match client.select(&sql, None, &[key.as_slice().into(), field.as_slice().into()]) {
                    Ok(tbl) => tbl.first().get::<String>(1).ok().flatten(),
                    Err(e) => return spi_failed("HINCRBYFLOAT", e),
                };
                let base = match cur {
                    None => 0.0,
                    Some(text) => match text.trim().parse::<f64>() {
                        Ok(v) if v.is_finite() => v,
                        _ => return Response::Error(format!("ERR {NOT_A_FLOAT}")),
                    },
                };
                let sum = base + delta;
                if !sum.is_finite() {
                    return Response::Error(
                        "ERR increment would produce NaN or Infinity".to_string(),
                    );
                }
                let text = crate::mem::format_float(sum);
                let write = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
                );
                let args: &[DatumWithOid] =
                    &[key.as_slice().into(), field.as_slice().into(), text.as_bytes().into()];
                match client.update(&write, None, args) {
                    Ok(_) => Response::BulkString(text.into_bytes()),
                    Err(e) => spi_failed("HINCRBYFLOAT", e),
                }
            }

            Command::RenameNx { key, newkey } => {
                if !Self::sql_key_kinds(client, db, &[newkey.as_slice()]).is_empty() {
                    // The source still has to exist, or Redis says so first.
                    return match Self::sql_key_kinds(client, db, &[key.as_slice()]).is_empty() {
                        true => Response::Error("ERR no such key".to_string()),
                        false => Response::Integer(0),
                    };
                }
                match Self::rename_to(client, db, key, newkey) {
                    Response::Ok => Response::Integer(1),
                    other => other,
                }
            }

            Command::GetEx { key, expiry } => {
                let value = match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => tbl.first().get::<Vec<u8>>(1).ok().flatten(),
                    Err(e) => return spi_failed("GETEX", e),
                };
                let Some(value) = value else {
                    return Response::Null;
                };
                match expiry {
                    GetExExpiry::Keep => {}
                    GetExExpiry::Clear => {
                        let _ = client.update(&sql::PERSIST[db as usize], None, &[key.as_slice().into()]);
                    }
                    GetExExpiry::At(us) => {
                        let sql = sql::set_expiry(db as usize, "to_timestamp($2::float8 / 1000000.0)");
                        let _ = client.update(&sql, None, &[key.as_slice().into(), (*us as f64).into()]);
                    }
                }
                Response::BulkString(value)
            }

            // Routed here by `run_dispatch_batch` whatever the storage mode:
            // the destination may be on the other half, which only this path
            // can reach.
            Command::Copy {
                src,
                dst,
                db: dst_db,
                replace,
            } => {
                let dst_db = dst_db.unwrap_or(db);
                if src == dst && dst_db == db {
                    return Response::Error(
                        "ERR source and destination objects are the same".to_string(),
                    );
                }
                let exists = match uses_shared_memory(dst_db) {
                    true => unsafe { crate::mem::mem_key_kind(dst_db as usize, dst) }.is_some(),
                    false => !Self::sql_key_kinds(client, dst_db, &[dst.as_slice()]).is_empty(),
                };
                if exists && !*replace {
                    return Response::Integer(0);
                }
                let Some((dump, expires_at)) = Self::dump_key(client, db, src) else {
                    return Response::Integer(0);
                };
                if exists {
                    match uses_shared_memory(dst_db) {
                        true => unsafe {
                            if let Some(kind) = crate::mem::mem_key_kind(dst_db as usize, dst) {
                                crate::mem::mem_drop_key_of(dst_db as usize, kind, dst);
                            }
                        },
                        false => Self::sql_drop_key(client, dst_db, dst),
                    }
                }
                Self::restore_key(client, dst_db, dst, &dump);
                if expires_at != 0 && uses_shared_memory(dst_db) {
                    unsafe { crate::mem::mem_set_expiry(dst_db as usize, dst, expires_at) };
                }
                Response::Integer(1)
            }

            Command::GetBit { key, offset } => {
                match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let v: Vec<u8> = tbl.first().get(1).ok().flatten().unwrap_or_default();
                        Response::Integer(bit_at(&v, *offset) as i64)
                    }
                    Err(e) => spi_failed("GETBIT", e),
                }
            }

            Command::BitCount { key, range } => {
                match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let v: Vec<u8> = tbl.first().get(1).ok().flatten().unwrap_or_default();
                        Response::Integer(count_bits(&v, *range))
                    }
                    Err(e) => spi_failed("BITCOUNT", e),
                }
            }

            // Read, edit and write back. Both statements are in the batch's
            // transaction, so nothing else sees the value between them.
            Command::SetBit { key, offset, value } => {
                let mut v: Vec<u8> =
                    match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                        Ok(tbl) => tbl.first().get(1).ok().flatten().unwrap_or_default(),
                        Err(e) => return spi_failed("SETBIT", e),
                    };
                let was = set_bit_at(&mut v, *offset, *value);
                let write = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
                );
                match client.update(&write, None, &[key.as_slice().into(), v.into()]) {
                    Ok(_) => Response::Integer(was as i64),
                    Err(e) => spi_failed("SETBIT", e),
                }
            }

            Command::HRandField {
                key,
                count,
                withvalues,
            } => {
                let sql = format!(
                    "SELECT field, value FROM redis.hash_{db} WHERE key = $1 ORDER BY field"
                );
                match client.select(&sql, None, &[key.as_slice().into()]) {
                    Ok(tbl) => {
                        let pairs: Vec<(Vec<u8>, Vec<u8>)> = tbl
                            .filter_map(|row| {
                                match (row.get::<Vec<u8>>(1), row.get::<Vec<u8>>(2)) {
                                    (Ok(Some(f)), Ok(Some(v))) => Some((f, v)),
                                    _ => None,
                                }
                            })
                            .collect();
                        random_fields(&pairs, *count, *withvalues)
                    }
                    Err(e) => spi_failed("HRANDFIELD", e),
                }
            }

            Command::SInterCard { keys, limit } => {
                let sql = format!(
                    "SELECT count(*)::bigint FROM ( \
                         SELECT member FROM redis.set_{db} WHERE key = $1 \
                          AND member IN (SELECT member FROM redis.set_{db} \
                                          WHERE key = ANY($2::bytea[]) \
                                       GROUP BY member \
                                         HAVING count(DISTINCT key) = $3::bigint) \
                          LIMIT $4::bigint \
                     ) c"
                );
                let rest = key_array(keys);
                let want = keys.len() as i64;
                let cap = limit.unwrap_or(i64::MAX);
                let args: &[DatumWithOid] = &[
                    keys[0].as_slice().into(),
                    rest.into(),
                    want.into(),
                    cap.into(),
                ];
                match client.select(&sql, None, args) {
                    Ok(tbl) => Response::Integer(tbl.first().get(1).ok().flatten().unwrap_or(0)),
                    Err(e) => spi_failed("SINTERCARD", e),
                }
            }

            Command::ObjectEncoding { key } => {
                // As the shared-memory arm: nil, not an error.
                let Some(kind) = Self::sql_key_kinds(client, db, &[key.as_slice()])
                    .first()
                    .and_then(|(_, k)| *k)
                else {
                    return Response::Null;
                };
                let name = match kind {
                    crate::mem::KeyKind::String => {
                        match client.select(&sql::GET[db as usize], None, &[key.as_slice().into()]) {
                            Ok(tbl) => {
                                let v: Vec<u8> =
                                    tbl.first().get(1).ok().flatten().unwrap_or_default();
                                string_encoding(&v)
                            }
                            Err(e) => return spi_failed("OBJECT", e),
                        }
                    }
                    other => {
                        let sql = match other {
                            crate::mem::KeyKind::List => {
                                format!("SELECT value FROM redis.list_{db} WHERE key = $1")
                            }
                            crate::mem::KeyKind::Set => {
                                format!("SELECT member FROM redis.set_{db} WHERE key = $1")
                            }
                            crate::mem::KeyKind::Zset => {
                                format!("SELECT member FROM redis.zset_{db} WHERE key = $1")
                            }
                            // Fields and values both, which is what Redis measures.
                            _ => format!(
                                "SELECT field FROM redis.hash_{db} WHERE key = $1 \
                                 UNION ALL SELECT value FROM redis.hash_{db} WHERE key = $1"
                            ),
                        };
                        match column_query(
                            client,
                            Spi::Reads,
                            "OBJECT",
                            &sql,
                            &[key.as_slice().into()],
                        ) {
                            Ok(items) => collection_encoding(other, &items),
                            Err(e) => return e,
                        }
                    }
                };
                Response::SimpleString(name.to_string())
            }

            Command::ObjectOther => Response::Null,

            Command::FlushDb => {
                match client.update(&sql::flush_dbs(&[db as usize]), None, &[]) {
                    Ok(_) => Response::Ok,
                    Err(e) => spi_failed("FLUSHDB", e),
                }
            }

            // Every database, the durable half included — Redis's own reach,
            // and the reason the Redis port should not be exposed past
            // loopback without a password.
            Command::FlushAll => {
                if crate::storage_mode() == crate::StorageMode::Memory {
                    for db_idx in 0..crate::mem::NUM_MEM_DBS {
                        unsafe { crate::mem::mem_flush_db(db_idx) };
                    }
                }
                let all: Vec<usize> = (0..NUM_DBS).collect();
                match client.update(&sql::flush_dbs(&all), None, &[]) {
                    Ok(_) => Response::Ok,
                    Err(e) => spi_failed("FLUSHALL", e),
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
                let keys_vec = key_array(keys);
                integer_query(client, Spi::Writes, "UNLINK", &sql, &[keys_vec.into()], 0)
            }

            Command::Rename { key, newkey } => Self::rename_to(client, db, key, newkey),

            Command::RandomKey => {
                let sql = format!(
                    "SELECT key FROM redis.kv_{db} \
                     WHERE (expires_at IS NULL OR expires_at > now()) \
                     ORDER BY random() LIMIT 1"
                );
                bulk_query(client, Spi::Reads, "RANDOMKEY", &sql, &[])
            }

            Command::Scan { pattern, .. } => Response::ScanResult {
                keys: matching_keys(client, db, pattern.as_deref()),
            },

            Command::HGet { key, field } => {
                bulk_query(client, Spi::Reads, "HGET", &sql::HGET[db as usize], &[key.as_slice().into(), field.as_slice().into()])
            }

            Command::HSet { key, pairs } => {
                // `xmax = 0` marks a row this statement inserted rather than
                // updated, which is the count Redis returns.
                let sql = format!(
                    "WITH ins AS ( \
                         INSERT INTO redis.hash_{db} (key, field, value) \
                         SELECT $1, unnest($2::bytea[]), unnest($3::bytea[]) \
                         ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value \
                         RETURNING (xmax = 0) AS added \
                     ) \
                     SELECT count(*) FILTER (WHERE added)::bigint FROM ins"
                );
                let deduped = dedup_last(pairs, |(f, _)| f.as_slice());
                let fields: Vec<Option<Vec<u8>>> =
                    deduped.iter().map(|(f, _)| Some(f.clone())).collect();
                let vals: Vec<Option<Vec<u8>>> =
                    deduped.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), fields.into(), vals.into()],
                ) {
                    Ok(tbl) => Response::Integer(
                        tbl.first().get::<i64>(1).ok().flatten().unwrap_or(0),
                    ),
                    Err(e) => spi_failed("HSET", e),
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
                    Err(e) => spi_failed("HDEL", e),
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
                    Err(e) => spi_failed("HGETALL", e),
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
                    Err(e) => spi_failed("HMGET", e),
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
                    Err(e) => spi_failed("HMSET", e),
                }
            }

            Command::HKeys { key } => {
                array_query(client, Spi::Reads, "HKEYS", &sql::HKEYS[db as usize], &[key.as_slice().into()])
            }

            Command::HVals { key } => {
                array_query(client, Spi::Reads, "HVALS", &sql::HVALS[db as usize], &[key.as_slice().into()])
            }

            Command::HExists { key, field } => {
                match client.select(&sql::HEXISTS[db as usize], None, &[key.as_slice().into(), field.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => spi_failed("HEXISTS", e),
                }
            }

            Command::HLen { key } => {
                integer_query(client, Spi::Reads, "HLEN", &sql::HLEN[db as usize], &[key.as_slice().into()], 0)
            }

            Command::HIncrBy { key, field, delta } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     VALUES ($1, $2, convert_to($3::text, 'UTF8')) \
                     ON CONFLICT (key, field) DO UPDATE \
                     SET value = convert_to( \
                         (CAST(encode(redis.hash_{db}.value, 'escape') AS bigint) + $4)::text, \
                         'UTF8') \
                     WHERE encode(redis.hash_{db}.value, 'escape') ~ '^(0|-?[1-9][0-9]*)$' \
           AND encode(redis.hash_{db}.value, 'escape')::numeric \
               BETWEEN -9223372036854775808 AND 9223372036854775807 \
                     RETURNING CAST(encode(value, 'escape') AS bigint)"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_slice().into(), field.as_slice().into(), (*delta).into(), (*delta).into()],
                ) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Error("ERR hash value is not an integer".to_string()),
                    },
                    Err(e) => {
                        pgrx::warning!("pg_redis: HINCRBY failed: {e}");
                        Response::Error("ERR hash value is not an integer".to_string())
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
                    Err(e) => spi_failed("HSETNX", e),
                }
            }

            Command::LPush { key, values } => list_push(client, db, key, values, true, false),
            Command::RPush { key, values } => list_push(client, db, key, values, false, false),
            Command::LPushX { key, values } => list_push(client, db, key, values, true, true),
            Command::RPushX { key, values } => list_push(client, db, key, values, false, true),

            Command::LPop { key, count } => list_pop(client, db, key, *count, true),
            Command::RPop { key, count } => list_pop(client, db, key, *count, false),

            Command::LLen { key } => {
                integer_query(client, Spi::Reads, "LLEN", &sql::LLEN[db as usize], &[key.as_slice().into()], 0)
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
                    Err(e) => spi_failed("LRANGE", e),
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
                     OFFSET GREATEST((SELECT i FROM idx), 0) \
                     LIMIT 1"
                );
                bulk_query(client, Spi::Reads, "LINDEX", &sql, &[key.as_slice().into(), (*index).into()])
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
                    Err(e) => spi_failed("LSET", e),
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
                    Err(e) => spi_failed("LREM", e),
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
                    Err(e) => spi_failed("LMOVE", e),
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
                    Err(e) => spi_failed("LPOS", e),
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
                    Err(e) => spi_failed("LTRIM", e),
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
                integer_query(client, Spi::Writes, "SADD", &sql, &[key.as_slice().into(), members_vec.into()], 0)
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
                integer_query(client, Spi::Writes, "SREM", &sql, &[key.as_slice().into(), members_vec.into()], 0)
            }

            Command::SMembers { key } => {
                array_query(client, Spi::Reads, "SMEMBERS", &sql::SMEMBERS[db as usize], &[key.as_slice().into()])
            }

            Command::SCard { key } => {
                integer_query(client, Spi::Reads, "SCARD", &sql::SCARD[db as usize], &[key.as_slice().into()], 0)
            }

            Command::SIsMember { key, member } => {
                match client.select(&sql::SISMEMBER[db as usize], None, &[key.as_slice().into(), member.as_slice().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => spi_failed("SISMEMBER", e),
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
                    Err(e) => spi_failed("SMISMEMBER", e),
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
                match column_query(
                    client,
                    Spi::Writes,
                    "SPOP",
                    &sql,
                    &[key.as_slice().into(), limit.into()],
                ) {
                    Ok(members) => {
                        if want_array {
                            Response::Array(members.into_iter().map(Some).collect())
                        } else {
                            match members.into_iter().next() {
                                Some(m) => Response::BulkString(m),
                                None => Response::Null,
                            }
                        }
                    }
                    Err(e) => e,
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
                match column_query(
                    client,
                    Spi::Reads,
                    "SRANDMEMBER",
                    &sql,
                    &[key.as_slice().into(), limit.into()],
                ) {
                    Ok(members) => {
                        if want_array {
                            Response::Array(members.into_iter().map(Some).collect())
                        } else {
                            match members.into_iter().next() {
                                Some(m) => Response::BulkString(m),
                                None => Response::Null,
                            }
                        }
                    }
                    Err(e) => e,
                }
            }

            Command::SUnion { keys } => {
                let keys_vec = key_array(keys);
                let sql = format!(
                    "SELECT DISTINCT member FROM redis.set_{db} WHERE key = ANY($1::bytea[])"
                );
                array_query(client, Spi::Reads, "SUNION", &sql, &[keys_vec.into()])
            }

            Command::SInter { keys } => {
                if keys.is_empty() {
                    return Response::Array(vec![]);
                }
                let sql = build_inter_sql(db, keys.len(), 1);
                let args: Vec<DatumWithOid> =
                    keys.iter().map(|k| k.as_slice().into()).collect();
                array_query(client, Spi::Reads, "SINTER", &sql, &args)
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
                    return array_query(client, Spi::Reads, "SDIFF", &sql, &[first.as_slice().into()]);
                }
                let rest: Vec<Option<Vec<u8>>> =
                    keys[1..].iter().map(|k| Some(k.clone())).collect();
                let sql = format!(
                    "SELECT member FROM redis.set_{db} WHERE key = $1 \
                     EXCEPT \
                     SELECT member FROM redis.set_{db} WHERE key = ANY($2::bytea[])"
                );
                array_query(client, Spi::Reads, "SDIFF", &sql, &[first.as_slice().into(), rest.into()])
            }

            Command::SUnionStore { dst, keys } => {
                let keys_vec = key_array(keys);
                let sql = set_store_sql(
                    db,
                    &format!(
                        "SELECT DISTINCT member FROM redis.set_{db} \
                         WHERE key = ANY($2::bytea[])"
                    ),
                );
                integer_query(client, Spi::Writes, "SUNIONSTORE", &sql, &[dst.as_slice().into(), keys_vec.into()], 0)
            }

            Command::SInterStore { dst, keys } => {
                if keys.is_empty() {
                    return Response::Integer(0);
                }
                let inter_body = build_inter_sql(db, keys.len(), 2);
                let sql = set_store_sql(db, &inter_body);
                let mut args: Vec<DatumWithOid> = Vec::with_capacity(1 + keys.len());
                args.push(dst.as_slice().into());
                for k in keys {
                    args.push(k.as_slice().into());
                }
                integer_query(client, Spi::Writes, "SINTERSTORE", &sql, &args, 0)
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
                let sql = set_store_sql(db, &body);
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
                    Err(e) => spi_failed("SDIFFSTORE", e),
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
                    Err(e) => spi_failed("SMOVE", e),
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
                    Err(e) => spi_failed("ZSCORE", e),
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
                    Err(e) => spi_failed("ZMSCORE", e),
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
                    Err(e) => spi_failed("ZINCRBY", e),
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

        // Before the early returns below, so a flag raised by an earlier
        // command cannot leak into this reply.
        crate::mem::take_refusal();

        // Before anything is written, so a refusal cannot leave a partial write.
        if let Some(err) = self.mem_too_long_error() {
            return err;
        }

        // Priced whole before any of it runs. A single-key write has nothing
        // to be partial about and is left to the insert.
        if let Some(err) = self.mem_full_error(db as usize) {
            return err;
        }

        // After the room check and before any write: a refused type must leave
        // the key exactly as it was.
        if let Some(err) = self.mem_type_error(db as usize) {
            return err;
        }

        let response = self.execute_mem_inner(db as usize);
        match crate::mem::take_refusal() {
            crate::mem::Refusal::None => response,
            crate::mem::Refusal::OutOfMemory => mem_out_of_memory(),
            crate::mem::Refusal::KeyCollision => mem_key_collision(),
        }
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

                    mem_clear_other_type(db_idx, key, mem::KeyKind::String);

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

            // Negating i64::MIN overflows: it wraps in a release build and
            // panics in a debug one.
            Command::DecrBy { key, delta } => match delta.checked_neg() {
                None => Response::Error("ERR decrement would overflow".to_string()),
                Some(d) => match unsafe { mem::mem_incr(db_idx, key, d) } {
                    Ok(n) => Response::Integer(n),
                    Err(e) => Response::Error(e),
                },
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

            Command::Rename { key, newkey } => self.mem_rename(db_idx, key, newkey),

            Command::SetRange { key, offset, value } => {
                if value.is_empty() {
                    // Redis writes nothing and reports the length as it stands.
                    return Response::Integer(unsafe { mem::mem_strlen(db_idx, key) });
                }
                match unsafe { mem::mem_setrange(db_idx, key, *offset as usize, value) } {
                    Some(len) => Response::Integer(len),
                    None => mem_value_too_large(),
                }
            }

            Command::GetRange { key, start, end } => {
                let v = unsafe { mem::mem_get(db_idx, key) }.unwrap_or_default();
                Response::BulkString(byte_range(&v, *start, *end))
            }

            Command::HIncrByFloat { key, field, delta } => {
                match unsafe { mem::mem_hincrbyfloat(db_idx, key, field, *delta) } {
                    Ok(text) => Response::BulkString(text.into_bytes()),
                    Err(e) => Response::Error(e),
                }
            }

            Command::RenameNx { key, newkey } => {
                if unsafe { mem::mem_key_kind(db_idx, newkey) }.is_some() {
                    // The source still has to exist, or Redis says so first.
                    return match unsafe { mem::mem_key_kind(db_idx, key) } {
                        None => Response::Error("ERR no such key".to_string()),
                        Some(_) => Response::Integer(0),
                    };
                }
                match self.mem_rename(db_idx, key, newkey) {
                    Response::Ok => Response::Integer(1),
                    other => other,
                }
            }

            Command::GetEx { key, expiry } => {
                let Some(value) = (unsafe { mem::mem_get(db_idx, key) }) else {
                    return Response::Null;
                };
                match expiry {
                    GetExExpiry::Keep => {}
                    GetExExpiry::Clear => {
                        unsafe { mem::mem_persist(db_idx, key) };
                    }
                    GetExExpiry::At(us) => {
                        unsafe { mem::mem_set_expiry(db_idx, key, *us) };
                    }
                }
                Response::BulkString(value)
            }

            Command::GetBit { key, offset } => {
                let v = unsafe { mem::mem_get(db_idx, key) }.unwrap_or_default();
                Response::Integer(bit_at(&v, *offset) as i64)
            }

            Command::SetBit { key, offset, value } => {
                let on = *value;
                match unsafe {
                    mem::mem_setbit(db_idx, key, *offset, |buf| set_bit_at(buf, *offset, on))
                } {
                    Some(was) => Response::Integer(was as i64),
                    None => mem_value_too_large(),
                }
            }

            Command::BitCount { key, range } => {
                let v = unsafe { mem::mem_get(db_idx, key) }.unwrap_or_default();
                Response::Integer(count_bits(&v, *range))
            }

            Command::HRandField {
                key,
                count,
                withvalues,
            } => {
                let pairs = unsafe { mem::mem_hgetall(db_idx, key) };
                random_fields(&pairs, *count, *withvalues)
            }

            Command::SInterCard { keys, limit } => {
                let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                let n = unsafe { mem::mem_sinter(db_idx, &refs) }.len() as i64;
                Response::Integer(limit.map_or(n, |l| n.min(l)))
            }

            Command::ObjectEncoding { key } => {
                // Redis answers nil rather than erroring here, unlike every
                // other command that names a key it cannot find.
                let Some(kind) = (unsafe { mem::mem_key_kind(db_idx, key) }) else {
                    return Response::Null;
                };
                let name = match kind {
                    mem::KeyKind::String => {
                        string_encoding(&unsafe { mem::mem_get(db_idx, key) }.unwrap_or_default())
                    }
                    mem::KeyKind::List => {
                        let items = unsafe { mem::mem_lrange(db_idx, key, 0, -1) };
                        collection_encoding(kind, &items)
                    }
                    mem::KeyKind::Set => {
                        let items = unsafe { mem::mem_smembers(db_idx, key) };
                        collection_encoding(kind, &items)
                    }
                    mem::KeyKind::Hash => {
                        let items: Vec<Vec<u8>> = unsafe { mem::mem_hgetall(db_idx, key) }
                            .into_iter()
                            .flat_map(|(f, v)| [f, v])
                            .collect();
                        collection_encoding(kind, &items)
                    }
                    mem::KeyKind::Zset => {
                        let items: Vec<Vec<u8>> = unsafe { mem::mem_zset_collect_all(db_idx, key) }
                            .into_iter()
                            .map(|(m, _)| m)
                            .collect();
                        collection_encoding(kind, &items)
                    }
                };
                Response::SimpleString(name.to_string())
            }

            Command::ObjectOther => Response::Null,

            Command::FlushDb => {
                unsafe { mem::mem_flush_db(db_idx) };
                Response::Ok
            }

            // Routed to the SQL path by `run_dispatch_batch`, which is where it
            // can reach both halves. Unreachable here, and answered rather than
            // ignored in case that routing ever changes.
            Command::FlushAll => {
                for idx in 0..mem::NUM_MEM_DBS {
                    unsafe { mem::mem_flush_db(idx) };
                }
                Response::Ok
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
                if s.is_nan() {
                    return Response::Error(NAN_SCORE.to_string());
                }
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
                } else if popped.is_empty() && unsafe { mem::mem_llen(db_idx, key) } == 0 {
                    Response::NullArray
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
                } else if popped.is_empty() && unsafe { mem::mem_llen(db_idx, key) } == 0 {
                    Response::NullArray
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
                } else if unsafe { mem::mem_llen(db_idx, key) } == 0 {
                    Response::Error("ERR no such key".to_string())
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

    /// Refuse anything the shared-memory backend would have to truncate.
    ///
    /// Every limit is a share of a chunk pool. Runs before the command, so a
    /// refusal leaves no partial write, and allocation-free because it sits on
    /// the hot path.
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

    /// Refuse a multi-key write the shared-memory backend cannot take whole.
    /// Single-key commands are not checked — they are already all or nothing,
    /// and this costs a lookup per key.
    fn mem_full_error(&self, db_idx: usize) -> Option<Response> {
        fn lens(vs: &[Vec<u8>]) -> Vec<usize> {
            vs.iter().map(Vec::len).collect()
        }
        fn pairs(ps: &[(Vec<u8>, Vec<u8>)]) -> Vec<(&[u8], usize)> {
            ps.iter().map(|(a, b)| (a.as_slice(), b.len())).collect()
        }
        fn slices(vs: &[Vec<u8>]) -> Vec<&[u8]> {
            vs.iter().map(Vec::as_slice).collect()
        }
        let room = match self {
            Command::MSet { pairs: ps } | Command::MSetNx { pairs: ps } if ps.len() > 1 => unsafe {
                crate::mem::mem_room_for_kv(db_idx, &pairs(ps))
            },
            Command::HSet { key, pairs: ps } | Command::HMSet { key, pairs: ps }
                if ps.len() > 1 =>
            unsafe { crate::mem::mem_room_for_hash(db_idx, key, &pairs(ps)) },
            Command::SAdd { key, members } if members.len() > 1 => unsafe {
                crate::mem::mem_room_for_members(db_idx, false, key, &slices(members))
            },
            Command::ZAdd { key, pairs: ps, .. } if ps.len() > 1 => unsafe {
                let members: Vec<&[u8]> = ps.iter().map(|(_, m)| m.as_slice()).collect();
                crate::mem::mem_room_for_members(db_idx, true, key, &members)
            },
            Command::LPush { values, .. }
            | Command::RPush { values, .. }
            | Command::LPushX { values, .. }
            | Command::RPushX { values, .. }
                if values.len() > 1 =>
            unsafe { crate::mem::mem_room_for_list(db_idx, &lens(values)) },
            _ => true,
        };
        (!room).then(mem_out_of_memory)
    }

    /// The keys this command touches, each with the types it will accept. A key
    /// holding anything else is a `WRONGTYPE` — which is the whole of Redis's
    /// type discipline, since a key holds exactly one thing.
    ///
    /// Commands that replace a key whatever it held are not here but in
    /// `overwritten_keys`, and the ones that answer nil rather than erroring on
    /// the wrong type — `MGET`, `SETNX`, `MSETNX` — are in neither.
    fn typed_keys(&self) -> Vec<(&[u8], &'static [crate::mem::KeyKind])> {
        use crate::mem::KeyKind::{Hash, List, Set, String as Str, Zset};
        const STRING: &[crate::mem::KeyKind] = &[Str];
        const HASH: &[crate::mem::KeyKind] = &[Hash];
        const LIST: &[crate::mem::KeyKind] = &[List];
        const SET: &[crate::mem::KeyKind] = &[Set];
        const ZSET: &[crate::mem::KeyKind] = &[Zset];
        /// `ZUNIONSTORE` and friends read a plain set as a sorted set whose
        /// every score is 1, so a set source is not an error.
        const ZSET_OR_SET: &[crate::mem::KeyKind] = &[Zset, Set];

        fn one<'a>(
            key: &'a [u8],
            kinds: &'static [crate::mem::KeyKind],
        ) -> Vec<(&'a [u8], &'static [crate::mem::KeyKind])> {
            vec![(key, kinds)]
        }
        fn many<'a>(
            keys: &'a [Vec<u8>],
            kinds: &'static [crate::mem::KeyKind],
        ) -> Vec<(&'a [u8], &'static [crate::mem::KeyKind])> {
            keys.iter().map(|k| (k.as_slice(), kinds)).collect()
        }

        match self {
            Command::Set { key, get: true, .. }
            | Command::SetRange { key, .. }
            | Command::GetRange { key, .. }
            | Command::GetEx { key, .. }
            | Command::SetBit { key, .. }
            | Command::GetBit { key, .. }
            | Command::BitCount { key, .. }
            | Command::Get { key }
            | Command::GetDel { key }
            | Command::GetSet { key, .. }
            | Command::Append { key, .. }
            | Command::Strlen { key }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. }
            | Command::IncrByFloat { key, .. } => one(key, STRING),

            Command::HGet { key, .. }
            | Command::HSet { key, .. }
            | Command::HDel { key, .. }
            | Command::HGetAll { key }
            | Command::HMGet { key, .. }
            | Command::HMSet { key, .. }
            | Command::HKeys { key }
            | Command::HVals { key }
            | Command::HExists { key, .. }
            | Command::HLen { key }
            | Command::HIncrBy { key, .. }
            | Command::HIncrByFloat { key, .. }
            | Command::HRandField { key, .. }
            | Command::HSetNx { key, .. } => one(key, HASH),

            Command::LPush { key, .. }
            | Command::RPush { key, .. }
            | Command::LPushX { key, .. }
            | Command::RPushX { key, .. }
            | Command::LPop { key, .. }
            | Command::RPop { key, .. }
            | Command::LLen { key }
            | Command::LRange { key, .. }
            | Command::LIndex { key, .. }
            | Command::LSet { key, .. }
            | Command::LInsert { key, .. }
            | Command::LRem { key, .. }
            | Command::LPos { key, .. }
            | Command::LTrim { key, .. } => one(key, LIST),
            Command::LMove { src, dst, .. } => vec![(src, LIST), (dst, LIST)],

            Command::SAdd { key, .. }
            | Command::SRem { key, .. }
            | Command::SMembers { key }
            | Command::SCard { key }
            | Command::SIsMember { key, .. }
            | Command::SMisMember { key, .. }
            | Command::SPop { key, .. }
            | Command::SRandMember { key, .. } => one(key, SET),
            Command::SUnion { keys }
            | Command::SInter { keys }
            | Command::SDiff { keys }
            | Command::SInterCard { keys, .. } => many(keys, SET),
            Command::SUnionStore { keys, .. }
            | Command::SInterStore { keys, .. }
            | Command::SDiffStore { keys, .. } => many(keys, SET),
            Command::SMove { src, dst, .. } => vec![(src, SET), (dst, SET)],

            Command::ZAdd { key, .. }
            | Command::ZRem { key, .. }
            | Command::ZScore { key, .. }
            | Command::ZMScore { key, .. }
            | Command::ZIncrBy { key, .. }
            | Command::ZCard { key }
            | Command::ZCount { key, .. }
            | Command::ZLexCount { key, .. }
            | Command::ZRank { key, .. }
            | Command::ZRange { key, .. }
            | Command::ZRangeByScore { key, .. }
            | Command::ZRangeByLex { key, .. }
            | Command::ZPopMin { key, .. }
            | Command::ZPopMax { key, .. }
            | Command::ZRandMember { key, .. }
            | Command::ZRemRangeByRank { key, .. }
            | Command::ZRemRangeByScore { key, .. }
            | Command::ZRemRangeByLex { key, .. } => one(key, ZSET),
            Command::ZUnion { keys, .. }
            | Command::ZInter { keys, .. }
            | Command::ZDiff { keys, .. }
            | Command::ZUnionStore { keys, .. }
            | Command::ZInterStore { keys, .. }
            | Command::ZDiffStore { keys, .. } => many(keys, ZSET_OR_SET),

            _ => vec![],
        }
    }

    /// Drop this command's keys if their deadline has passed, before it reads
    /// them. One statement, and in the ordinary case — nothing expired — one
    /// indexed lookup on a table holding only the keys that carry a TTL.
    fn sql_reap_expired(&self, client: &mut SpiClient<'_>, db: u8) {
        let keys: Vec<Option<Vec<u8>>> = self
            .typed_keys()
            .iter()
            .map(|(k, _)| Some(k.to_vec()))
            .collect();
        if keys.is_empty() {
            return;
        }
        let _ = client.update(&sql::REAP_EXPIRED[db as usize], None, &[keys.into()]);
    }

    /// Whether this command reads a key that could hold a collection.
    ///
    /// The four collection tables carry no expiry of their own — a key's
    /// deadline lives in `redis.expiry_N` — so a statement reading them cannot
    /// tell a live key from one the sweep has not reached. There are sixty-odd
    /// such statements and guarding each one is sixty-odd chances to miss one,
    /// so the question is asked here instead, once.
    fn reads_a_collection(&self) -> bool {
        self.typed_keys()
            .iter()
            .any(|(_, kinds)| kinds.iter().any(|k| *k != crate::mem::KeyKind::String))
    }

    /// The keys this command replaces outright, with the type each becomes.
    /// Redis lets `SET` land on a key that held a list; the list goes, rather
    /// than the write being refused or the two sitting side by side.
    fn overwritten_keys(&self) -> Vec<(&[u8], crate::mem::KeyKind)> {
        match self {
            // `SET k v GET` has to hand back the old string, so it is a typed
            // read rather than a blind overwrite and lives in `typed_keys`. `NX`
            // and `XX` decide on whether the key already exists, so clearing it
            // here would answer their question for them; they clear it
            // themselves, once the decision is made. So does `RENAME`, which
            // must leave its destination alone when the source is not there.
            Command::Set {
                key,
                get: false,
                nx: false,
                xx: false,
                ..
            }
            | Command::SetEx { key, .. }
            | Command::PSetEx { key, .. } => vec![(key.as_slice(), crate::mem::KeyKind::String)],
            Command::MSet { pairs } => pairs
                .iter()
                .map(|(k, _)| (k.as_slice(), crate::mem::KeyKind::String))
                .collect(),
            Command::SUnionStore { dst, .. }
            | Command::SInterStore { dst, .. }
            | Command::SDiffStore { dst, .. } => vec![(dst.as_slice(), crate::mem::KeyKind::Set)],
            Command::ZUnionStore { dst, .. }
            | Command::ZInterStore { dst, .. }
            | Command::ZDiffStore { dst, .. } => vec![(dst.as_slice(), crate::mem::KeyKind::Zset)],
            _ => vec![],
        }
    }

    /// Refuse a command whose key holds another type, and clear a key the
    /// command is about to replace. Both walk the key directory, which is the
    /// only structure that knows what a key holds.
    fn mem_type_error(&self, db_idx: usize) -> Option<Response> {
        for (key, kinds) in self.typed_keys() {
            // Reaping, not looking up: a collection whose expiry has passed is
            // reported gone by the directory while its rows are still in the
            // type's tables, and a collection read goes to the rows. Every
            // command that touches a typed key comes through here, so this is
            // the one place that has to make the two agree.
            if let Some(kind) = unsafe { crate::mem::mem_key_kind_reaping(db_idx, key) }
                && !kinds.contains(&kind)
            {
                return Some(wrong_type());
            }
        }
        for (key, becomes) in self.overwritten_keys() {
            mem_clear_other_type(db_idx, key, becomes);
        }
        None
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
            | Command::HIncrByFloat { key, .. }
            | Command::HSetNx { key, .. }
            | Command::SetRange { key, .. }
            | Command::GetEx { key, .. }
            | Command::SetBit { key, .. }
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
            Command::Rename { key, newkey } | Command::RenameNx { key, newkey } => {
                vec![key.as_slice(), newkey.as_slice()]
            }
            Command::Copy { dst, .. } => vec![dst.as_slice()],
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
/// Two keys, or two members of one collection, whose 128-bit keyed hashes
/// agree. Unreachable in practice; the alternative is a silent overwrite.
fn mem_key_collision() -> Response {
    Response::Error(
        "ERR key or member hash collides with a different one already stored; \
         rename it, or use SELECT durable."
            .to_string(),
    )
}

/// Clear whatever else held this key, so a write that replaces a value of
/// another type does not leave the old one stranded beside the new one.
fn mem_clear_other_type(db_idx: usize, key: &[u8], becomes: crate::mem::KeyKind) {
    if let Some(kind) = unsafe { crate::mem::mem_key_kind(db_idx, key) }
        && kind != becomes
    {
        unsafe { crate::mem::mem_drop_key_of(db_idx, kind, key) };
    }
}

/// Redis's own wording, so a client's existing type handling recognises it.
fn wrong_type() -> Response {
    Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

fn mem_out_of_memory() -> Response {
    Response::Error(
        "OOM command not allowed when used memory > 'maxmemory'. Raise \
         redis.mem_max_entries, set redis.maxmemory_policy, or use SELECT durable."
            .to_string(),
    )
}

/// Longest key the shared-memory backend stores intact.
///
/// The whole of `MAX_KEY_LEN`: keys are length-prefixed, so none of it goes to
/// a terminator.
const MEM_MAX_KEY: usize = crate::mem::MAX_KEY_LEN;
/// Longest hash field or set/zset member the shared-memory backend stores intact.
const MEM_MAX_MEMBER: usize = crate::mem::MAX_MEMBER_LEN;

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
        .find_map(|m| (m.len() > MEM_MAX_MEMBER).then_some(Over::Member))
}

fn over_values<'a>(values: impl IntoIterator<Item = &'a [u8]>) -> Option<Over> {
    values
        .into_iter()
        .find_map(|v| (v.len() > crate::mem::max_total_val_len()).then_some(Over::Value))
}

fn mem_too_long(over: Over) -> Response {
    let (what, limit) = match over {
        Over::Key => ("key", MEM_MAX_KEY),
        Over::Member => ("hash field or set member", MEM_MAX_MEMBER),
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
        integer_query(
            client,
            Spi::Writes,
            "list_push",
            &sql,
            &[key.into(), values_vec.into()],
            0,
        )
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
        integer_query(
            client,
            Spi::Writes,
            "list_push",
            &sql,
            &[key.into(), values_vec.into()],
            0,
        )
    }
}

/// Whether the list has any elements. Only asked on the empty-reply path, to
/// tell "nothing popped" from "no such key" — Redis answers those differently.
fn list_exists(client: &mut SpiClient<'_>, db: u8, key: &[u8]) -> bool {
    let sql = format!("SELECT 1 FROM redis.list_{db} WHERE key = $1 LIMIT 1");
    client
        .select(&sql, Some(1), &[key.into()])
        .map(|t| !t.is_empty())
        .unwrap_or(false)
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
        return if list_exists(client, db, key) {
            Response::Array(vec![])
        } else {
            Response::NullArray
        };
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
            } else if rows.is_empty() && !list_exists(client, db, key) {
                Response::NullArray
            } else {
                Response::Array(rows.into_iter().map(|(_, v)| Some(v)).collect())
            }
        }
        Err(e) => spi_failed("POP", e),
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
            pgrx::warning!("pg_redis: LINSERT pivot lookup: {e}");
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
                    pgrx::warning!("pg_redis: LINSERT renumber: {e}");
                    return Response::Error("internal error".to_string());
                }
                return list_insert(client, db, key, before, pivot, value);
            }
        }
    };

    let ins_sql = format!("INSERT INTO redis.list_{db} (key, pos, value) VALUES ($1, $2, $3)");
    if let Err(e) = client.update(&ins_sql, None, &[key.into(), new_pos.into(), value.into()]) {
        pgrx::warning!("pg_redis: LINSERT insert: {e}");
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

/// Redis's wording for an argument-count error. Clients match on the code, but
/// the text is what a person sees, and matching it costs a format string.
/// Redis refuses a non-positive expiry outright, and one large enough that the
/// deadline would overflow. `SET k v EX 0` is an error, not a delete.
fn check_expire(value: i64, unit_ms: i64, cmd: &str) -> Result<i64, String> {
    let ms = value
        .checked_mul(unit_ms)
        .filter(|ms| ms.checked_add(now_micros() / 1000).is_some())
        .ok_or_else(|| invalid_expire(cmd))?;
    if value <= 0 {
        return Err(invalid_expire(cmd));
    }
    Ok(ms)
}

/// The same rule for the commands that only set a deadline, where a past one is
/// legal — `EXPIRE k -1` deletes the key — but an unrepresentable one is not.
fn check_expire_deadline(value: i64, unit_ms: i64, cmd: &str) -> Result<i64, String> {
    value
        .checked_mul(unit_ms)
        .filter(|ms| ms.checked_add(now_micros() / 1000).is_some())
        .ok_or_else(|| invalid_expire(cmd))
}

/// A count argument Redis requires to be non-negative — `SPOP`, `LPOP`, `RPOP`
/// and the `ZPOP*` pair. `SRANDMEMBER` and `ZRANDMEMBER` are not among them: a
/// negative count there means "with repeats".
fn check_count(n: i64) -> Result<i64, String> {
    if n < 0 {
        return Err("value is out of range, must be positive".to_string());
    }
    Ok(n)
}

fn invalid_expire(cmd: &str) -> String {
    format!("invalid expire time in '{}' command", cmd.to_lowercase())
}

/// The bytes `GETRANGE` selects. Redis clamps rather than erroring: a negative
/// index counts from the end, a start past the end is empty, and an end past
/// the end is the end.
fn byte_range(value: &[u8], start: i64, end: i64) -> Vec<u8> {
    let len = value.len() as i64;
    let s = if start < 0 { start + len } else { start }.max(0);
    let e = if end < 0 { end + len } else { end }.min(len - 1);
    if len == 0 || e < 0 || s > e {
        return Vec::new();
    }
    value[s as usize..=(e as usize)].to_vec()
}

/// Redis's name for how it would store this string. pg_redis stores none of
/// these representations — it has an inline slot and a chunk pool — but the
/// name is what a client comparing against a stock server expects, and the
/// thresholds are Redis's own defaults.
fn string_encoding(value: &[u8]) -> &'static str {
    /// `OBJ_ENCODING_EMBSTR_SIZE_LIMIT`.
    const EMBSTR_LIMIT: usize = 44;
    let is_int = std::str::from_utf8(value)
        .ok()
        .is_some_and(|t| crate::mem::parse_stored_int(t.as_bytes()).is_some());
    match () {
        _ if is_int => "int",
        _ if value.len() <= EMBSTR_LIMIT => "embstr",
        _ => "raw",
    }
}

/// The same, for a collection: Redis packs a small one into a flat listpack and
/// promotes it once it outgrows either threshold. `items` is every element —
/// for a hash, fields and values alike, which is what Redis measures.
fn collection_encoding(kind: crate::mem::KeyKind, items: &[Vec<u8>]) -> &'static str {
    use crate::mem::KeyKind;
    /// `*-max-listpack-entries`, which is 128 for every type that has one.
    const MAX_ENTRIES: usize = 128;
    /// `*-max-listpack-value`.
    const MAX_VALUE: usize = 64;
    /// `set-max-intset-entries`.
    const MAX_INTSET: usize = 512;

    let count = match kind {
        // A hash's items arrive as field, value, field, value.
        KeyKind::Hash => items.len() / 2,
        _ => items.len(),
    };
    let all_ints = items.iter().all(|v| {
        std::str::from_utf8(v)
            .ok()
            .is_some_and(|t| crate::mem::parse_stored_int(t.as_bytes()).is_some())
    });
    let packed = count <= MAX_ENTRIES && items.iter().all(|v| v.len() <= MAX_VALUE);
    match kind {
        KeyKind::Set if all_ints && count <= MAX_INTSET => "intset",
        KeyKind::Set => {
            if packed {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KeyKind::Hash => {
            if packed {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KeyKind::Zset => {
            if packed {
                "listpack"
            } else {
                "skiplist"
            }
        }
        KeyKind::List => {
            if packed {
                "listpack"
            } else {
                "quicklist"
            }
        }
        KeyKind::String => "raw",
    }
}

/// `HRANDFIELD`'s reply, from the hash's fields. No count is one field as a
/// bulk string; a positive count is that many distinct fields; a negative one
/// allows repeats, which is the only way to ask for more than the hash holds.
fn random_fields(pairs: &[(Vec<u8>, Vec<u8>)], count: Option<i64>, withvalues: bool) -> Response {
    let Some(count) = count else {
        return match pairs.first() {
            None => Response::Null,
            Some((f, _)) => Response::BulkString(f.clone()),
        };
    };
    if pairs.is_empty() {
        return Response::Array(vec![]);
    }
    let push = |out: &mut Vec<Option<Vec<u8>>>, i: usize| {
        out.push(Some(pairs[i].0.clone()));
        if withvalues {
            out.push(Some(pairs[i].1.clone()));
        }
    };
    let mut out = Vec::new();
    if count >= 0 {
        for i in 0..(count as usize).min(pairs.len()) {
            push(&mut out, i);
        }
    } else {
        for i in 0..(-count) as usize {
            push(&mut out, i % pairs.len());
        }
    }
    Response::Array(out)
}

/// The bit `GETBIT` reads. Redis numbers bits from the *most* significant of
/// byte 0, so bit 0 is `0x80` of the first byte — the opposite of PostgreSQL's
/// `get_bit`, which counts from the least significant.
fn bit_at(value: &[u8], offset: i64) -> bool {
    let byte = (offset / 8) as usize;
    match value.get(byte) {
        None => false,
        Some(b) => b & (0x80 >> (offset % 8)) != 0,
    }
}

/// Set or clear a bit, growing the value with NUL bytes to reach it. Returns
/// the bit as it was, which is what `SETBIT` answers with.
fn set_bit_at(value: &mut Vec<u8>, offset: i64, on: bool) -> bool {
    let byte = (offset / 8) as usize;
    if value.len() <= byte {
        value.resize(byte + 1, 0);
    }
    let mask = 0x80u8 >> (offset % 8);
    let was = value[byte] & mask != 0;
    if on {
        value[byte] |= mask;
    } else {
        value[byte] &= !mask;
    }
    was
}

/// Bits set in `value`, or in the range `BITCOUNT` named. `by_bit` counts the
/// range in bits rather than bytes; either way the indices clamp as `GETRANGE`
/// does rather than erroring.
fn count_bits(value: &[u8], range: Option<(i64, i64, bool)>) -> i64 {
    let ones = |bytes: &[u8]| bytes.iter().map(|b| b.count_ones() as i64).sum::<i64>();
    let Some((start, end, by_bit)) = range else {
        return ones(value);
    };
    if !by_bit {
        return ones(&byte_range(value, start, end));
    }
    let len = value.len() as i64 * 8;
    let s = if start < 0 { start + len } else { start }.max(0);
    let e = if end < 0 { end + len } else { end }.min(len - 1);
    if len == 0 || e < 0 || s > e {
        return 0;
    }
    (s..=e).filter(|i| bit_at(value, *i)).count() as i64
}

/// Whether stored bytes spell a finite number. `float8` prints `Infinity` and
/// `NaN`, which are values Redis will not keep.
/// The reply for an SPI call that failed. Logged through PostgreSQL rather than
/// stderr, where an operator will find it.
/// The statement all three set-store commands run; only `new_data` differs.
/// `$1` is the destination, which loses only what the result does not contain:
/// the insert's uniqueness check still sees rows the same statement deleted, so
/// deleting everything first would drop the overlap.
fn set_store_sql(db: u8, new_data: &str) -> String {
    format!(
        "WITH new_data AS ({new_data}), \
              del AS ( \
                  DELETE FROM redis.set_{db} \
                  WHERE key = $1 AND member NOT IN (SELECT member FROM new_data) \
                  RETURNING 1 \
              ), \
              ins AS ( \
                  INSERT INTO redis.set_{db} (key, member) \
                  SELECT $1, member FROM new_data \
                  ON CONFLICT DO NOTHING \
                  RETURNING 1 \
              ) \
         SELECT count(*)::bigint FROM new_data"
    )
}

/// Whether a statement writes. `SpiClient::select` runs read-only and refuses a
/// data-modifying statement outright, so a `DEL` built as
/// `WITH d AS (DELETE …) SELECT count(…)` has to go through `update`. The
/// distinction is not cosmetic — naming it keeps each call site honest about
/// which it is.
#[derive(Clone, Copy, PartialEq)]
enum Spi {
    Reads,
    Writes,
}

fn spi_run<'a>(
    client: &'a mut SpiClient<'_>,
    how: Spi,
    sql: &str,
    args: &[DatumWithOid],
) -> Result<pgrx::spi::SpiTupleTable<'a>, pgrx::spi::Error> {
    match how {
        Spi::Reads => client.select(sql, None, args),
        Spi::Writes => client.update(sql, None, args),
    }
}

/// A statement whose reply is one integer — `EXISTS`, `STRLEN`, `DEL` and a
/// dozen more. `absent` is what Redis answers when it returns nothing.
fn integer_query(
    client: &mut SpiClient<'_>,
    how: Spi,
    cmd: &str,
    sql: &str,
    args: &[DatumWithOid],
    absent: i64,
) -> Response {
    match spi_run(client, how, sql, args) {
        Ok(tbl) => Response::Integer(tbl.first().get::<i64>(1).ok().flatten().unwrap_or(absent)),
        Err(e) => spi_failed(cmd, e),
    }
}

/// A statement whose reply is one value, or nil when the key is not there.
fn bulk_query(
    client: &mut SpiClient<'_>,
    how: Spi,
    cmd: &str,
    sql: &str,
    args: &[DatumWithOid],
) -> Response {
    match spi_run(client, how, sql, args) {
        Ok(tbl) => match tbl.first().get::<Vec<u8>>(1).ok().flatten() {
            Some(v) => Response::BulkString(v),
            None => Response::Null,
        },
        Err(e) => spi_failed(cmd, e),
    }
}

/// The first column of every row a statement returns, skipping any that is
/// NULL or of another type — which is what each caller did by hand.
fn column_query(
    client: &mut SpiClient<'_>,
    how: Spi,
    cmd: &str,
    sql: &str,
    args: &[DatumWithOid],
) -> Result<Vec<Vec<u8>>, Response> {
    match spi_run(client, how, sql, args) {
        Ok(tbl) => Ok(tbl
            .filter_map(|row| row.get::<Vec<u8>>(1).ok().flatten())
            .collect()),
        Err(e) => Err(spi_failed(cmd, e)),
    }
}

/// The same, as the array reply it almost always becomes — `SMEMBERS`,
/// `HKEYS`, `HVALS`, `SUNION` and the rest.
fn array_query(
    client: &mut SpiClient<'_>,
    how: Spi,
    cmd: &str,
    sql: &str,
    args: &[DatumWithOid],
) -> Response {
    match column_query(client, how, cmd, sql, args) {
        Ok(items) => Response::Array(items.into_iter().map(Some).collect()),
        Err(e) => e,
    }
}

/// Redis keys as a `bytea[]` parameter. Every multi-key statement binds one,
/// and each was building it by hand.
fn key_array(keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
    keys.iter().map(|k| Some(k.clone())).collect()
}

fn spi_failed(cmd: &str, e: impl std::fmt::Display) -> Response {
    pgrx::warning!("pg_redis: {cmd} failed: {e}");
    Response::Error("ERR internal error executing the command".to_string())
}

fn is_finite_number(v: &[u8]) -> bool {
    std::str::from_utf8(v)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .is_some_and(f64::is_finite)
}

fn wrong_arity(cmd: &str) -> String {
    format!(
        "wrong number of arguments for '{}' command",
        cmd.to_lowercase()
    )
}

/// The two parse failures Redis names the same way everywhere.
const NOT_AN_INTEGER: &str = "value is not an integer or out of range";
const NOT_A_FLOAT: &str = "value is not a valid float";
/// Adding infinities of opposite sign. Redis refuses rather than storing NaN,
/// which would order unpredictably against every other score.
const NAN_SCORE: &str = "ERR resulting score is not a number (NaN)";

/// A `SETBIT`/`GETBIT` offset. Redis caps it at 4 GiB of bits and gives that
/// ceiling its own message, distinct from a value that is not a number at all.
fn bit_offset(args: &[Vec<u8>], idx: usize) -> Result<i64, String> {
    const MAX_BIT_OFFSET: i64 = 4 * 1024 * 1024 * 1024 - 1;
    let n: i64 = str_arg(args, idx, "SETBIT")?
        .parse()
        .map_err(|_| "ERR bit offset is not an integer or out of range".to_string())?;
    if !(0..=MAX_BIT_OFFSET).contains(&n) {
        return Err("ERR bit offset is not an integer or out of range".to_string());
    }
    Ok(n)
}

/// Every argument from `from` onward, as keys. Fifteen commands take a bare
/// list of them — `SUNION` from the first, the `*STORE` pair from the second —
/// and each was spelling out the same range-map-collect.
fn key_args(args: &[Vec<u8>], from: usize, cmd: &str) -> Result<Vec<Vec<u8>>, String> {
    (from..args.len())
        .map(|i| bytes_arg(args, i, cmd))
        .collect()
}

fn bytes_arg(args: &[Vec<u8>], idx: usize, cmd: &str) -> Result<Vec<u8>, String> {
    args.get(idx)
        .map(|a| a.to_vec())
        .ok_or_else(|| wrong_arity(cmd))
}

/// Fetch an argument that is genuinely textual: a number, a flag, a config name.
fn str_arg(args: &[Vec<u8>], idx: usize, cmd: &str) -> Result<String, String> {
    args.get(idx)
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .ok_or_else(|| wrong_arity(cmd))
}

fn update_expiry(client: &mut SpiClient<'_>, sql: &str, key: &[u8], n: i64) -> Response {
    match client.update(sql, None, &[key.into(), n.into()]) {
        Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
        Err(e) => spi_failed("expiry update", e),
    }
}

fn get_ttl(client: &mut SpiClient<'_>, sql: &str, key: &[u8]) -> Response {
    integer_query(client, Spi::Reads, "TTL", sql, &[key.into()], -2)
}

/// Every live key in `db`, optionally filtered by a Redis glob. Matched here
/// rather than as SQL `LIKE` — bytea has no such operator — which also keeps
/// KEYS, SCAN and PSUBSCRIBE agreeing on what a pattern means.
fn matching_keys(
    client: &mut SpiClient<'_>,
    db: u8,
    pattern: Option<&[u8]>,
) -> Vec<Option<Vec<u8>>> {
    let sql = format!(
        "SELECT key FROM redis.kv_{db} \
           WHERE (expires_at IS NULL OR expires_at > now()) \
         UNION \
         SELECT c.key FROM ( \
             SELECT DISTINCT key FROM redis.hash_{db} \
             UNION SELECT DISTINCT key FROM redis.list_{db} \
             UNION SELECT DISTINCT key FROM redis.set_{db} \
             UNION SELECT DISTINCT key FROM redis.zset_{db} \
         ) c \
          WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e \
                             WHERE e.key = c.key AND e.expires_at <= now())"
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
        Err(e) => pgrx::warning!("pg_redis: KEYS failed: {e}"),
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
            // A literal that overflows to infinity is out of range, not
            // infinite: only the spellings above mean infinity.
            if v.is_finite() { Some(v) } else { None }
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
        return Err(wrong_arity("ZADD"));
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
        return Err("XX and NX options at the same time are not compatible".to_string());
    }
    if gt && lt {
        return Err("GT, LT, and/or NX options at the same time are not compatible".to_string());
    }
    if nx && (gt || lt) {
        return Err("GT, LT, and/or NX options at the same time are not compatible".to_string());
    }
    let remaining = &args[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Err(wrong_arity("ZADD"));
    }
    let mut pairs = Vec::with_capacity(remaining.len() / 2);
    for chunk in remaining.chunks(2) {
        let score = parse_score_value(&chunk[0]).ok_or_else(|| NOT_A_FLOAT.to_string())?;
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
        return Err(wrong_arity("ZRANGE"));
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let cnt: i64 = str_arg(args, i + 2, "ZRANGE LIMIT")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
    let min = parse_score_bound(&min_raw).ok_or_else(|| "min or max is not a float".to_string())?;
    let max = parse_score_bound(&max_raw).ok_or_else(|| "min or max is not a float".to_string())?;
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
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let cnt: i64 = str_arg(args, i + 2, cmd_name)?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
    let min = parse_lex_bound(&min_raw)
        .ok_or_else(|| "min or max not valid string range item".to_string())?;
    let max = parse_lex_bound(&max_raw)
        .ok_or_else(|| "min or max not valid string range item".to_string())?;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "LIMIT" => {
                let off: i64 = str_arg(args, i + 1, cmd_name)?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let cnt: i64 = str_arg(args, i + 2, cmd_name)?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
        return Err(wrong_arity(cmd_name));
    }
    let numkeys: usize = str_arg(args, 0, cmd_name)?
        .parse()
        .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
        return Err(wrong_arity("ZDIFF"));
    }
    let numkeys: usize = str_arg(args, 0, "ZDIFF")?
        .parse()
        .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
        return Err(wrong_arity(cmd_name));
    }
    let dst = bytes_arg(args, 0, cmd_name)?;
    let numkeys: usize = str_arg(args, 1, cmd_name)?
        .parse()
        .map_err(|_| NOT_AN_INTEGER.to_string())?;
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
                    return Err(wrong_arity(cmd_name));
                }
                let mut w = Vec::with_capacity(numkeys);
                for j in 0..numkeys {
                    let v = parse_score_value(&args[i + 1 + j])
                        .ok_or_else(|| NOT_A_FLOAT.to_string())?;
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
            pgrx::warning!("pg_redis: {cmd} failed: {e}");
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

/// The last occurrence of each key, in the order they first appear. `ON
/// CONFLICT DO UPDATE` cannot touch a row twice in one statement, and Redis
/// takes the last.
fn dedup_last<T: Clone>(items: &[T], key: impl Fn(&T) -> &[u8]) -> Vec<T> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<T> = items
        .iter()
        .rev()
        .filter(|it| seen.insert(key(it).to_vec()))
        .cloned()
        .collect();
    out.reverse();
    out
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
    let deduped = dedup_last(pairs, |(_, m)| m.as_slice());
    let members: Vec<Option<Vec<u8>>> = deduped.iter().map(|(_, m)| Some(m.clone())).collect();
    let scores: Vec<Option<f64>> = deduped.iter().map(|(s, _)| Some(*s)).collect();
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
        Err(e) => spi_failed("ZADD", e),
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
            pgrx::warning!("pg_redis: ZADD INCR lookup error: {e}");
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
        Err(e) => spi_failed("ZADD INCR", e),
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
        Err(e) => spi_failed("ZRANK", e),
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
                None => return Response::Error(NOT_AN_INTEGER.to_string()),
            };
            let e: i64 = match int_from_bytes(stop) {
                Some(v) => v,
                None => return Response::Error(NOT_AN_INTEGER.to_string()),
            };
            zrange_by_index(client, db, key, s, e, rev, with_scores)
        }
        RangeBy::Score => {
            let (min_raw, max_raw) = if rev { (stop, start) } else { (start, stop) };
            let min = match parse_score_bound(min_raw) {
                Some(b) => b,
                None => return Response::Error("ERR min or max is not a float".to_string()),
            };
            let max = match parse_score_bound(max_raw) {
                Some(b) => b,
                None => return Response::Error("ERR min or max is not a float".to_string()),
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
                None => {
                    return Response::Error(
                        "ERR min or max not valid string range item".to_string(),
                    );
                }
            };
            let max = match parse_lex_bound(max_raw) {
                Some(b) => b,
                None => {
                    return Response::Error(
                        "ERR min or max not valid string range item".to_string(),
                    );
                }
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
        Err(e) => spi_failed("ZRANGEBYSCORE", e),
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
        Err(e) => spi_failed("ZRANGEBYLEX", e),
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
            pgrx::warning!("pg_redis: {cmd} failed: {e}");
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
        Err(e) => spi_failed("ZPOP", e),
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
        Err(e) => spi_failed("ZRANDMEMBER", e),
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

    let keys_vec = key_array(keys);
    let weights_opt: Vec<Option<f64>> = weights_vec.iter().map(|w| Some(*w)).collect();

    if let Some(dst) = store_into {
        // Disjoint targets: the DELETE takes only old-only members, the INSERT
        // only new ones, so the two CTEs cannot race over a row. Both run to
        // completion even though only `ins` is read back.
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
            Err(e) => spi_failed("ZUNION/ZINTER/ZDIFF", e),
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

    /// Redis numbers bits from the most significant of byte 0, which is the
    /// opposite of PostgreSQL's `get_bit`. Getting it backwards passes every
    /// symmetric test, so these pin the asymmetric ones.
    #[test]
    fn bits_are_numbered_from_the_most_significant() {
        assert!(bit_at(&[0x80], 0));
        assert!(!bit_at(&[0x80], 7));
        assert!(bit_at(&[0x01], 7));
        assert!(!bit_at(&[0x01], 0));
        assert!(bit_at(&[0x00, 0x80], 8));
        // Past the end reads zero rather than erroring.
        assert!(!bit_at(&[0x80], 100));
        assert!(!bit_at(&[], 0));

        // Setting bit 0 of an empty value gives 0x80, as Redis does.
        let mut v = Vec::new();
        assert!(!set_bit_at(&mut v, 0, true));
        assert_eq!(v, vec![0x80]);
        // And setting it again reports the bit as it was.
        assert!(set_bit_at(&mut v, 0, false));
        assert_eq!(v, vec![0x00]);
        // A far offset grows the value with NUL bytes.
        let mut v = Vec::new();
        set_bit_at(&mut v, 15, true);
        assert_eq!(v, vec![0x00, 0x01]);
    }

    /// Redis's own documented `BITCOUNT` examples, which pin the byte and bit
    /// ranges against a source that is not this implementation.
    #[test]
    fn bitcount_matches_the_documented_examples() {
        let v = b"foobar";
        assert_eq!(count_bits(v, None), 26);
        assert_eq!(count_bits(v, Some((0, 0, false))), 4);
        assert_eq!(count_bits(v, Some((1, 1, false))), 6);
        assert_eq!(count_bits(v, Some((0, -5, false))), 10);
        assert_eq!(count_bits(v, Some((5, 30, true))), 17);
        assert_eq!(count_bits(v, Some((0, -5, true))), 25);
        // An empty value has nothing to count, whatever range is named.
        assert_eq!(count_bits(b"", None), 0);
        assert_eq!(count_bits(b"", Some((0, -1, false))), 0);
        assert_eq!(count_bits(b"", Some((0, -1, true))), 0);
    }

    /// `OBJECT ENCODING` reports Redis's representation, not ours. The names
    /// and the thresholds are Redis's defaults; what pg_redis actually stores
    /// is an inline slot and a chunk pool.
    #[test]
    fn object_encoding_names_redis_representations() {
        use crate::mem::KeyKind::{Hash, List, Set, Zset};
        assert_eq!(string_encoding(b"12345"), "int");
        assert_eq!(string_encoding(b"-1"), "int");
        // Not canonical, so not an int — `007` is a string to Redis.
        assert_eq!(string_encoding(b"007"), "embstr");
        assert_eq!(string_encoding(b"hello"), "embstr");
        assert_eq!(string_encoding(&[b'x'; 44]), "embstr");
        assert_eq!(string_encoding(&[b'x'; 45]), "raw");

        let small: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec()];
        assert_eq!(collection_encoding(List, &small), "listpack");
        assert_eq!(collection_encoding(Zset, &small), "listpack");
        assert_eq!(collection_encoding(Hash, &small), "listpack");
        // All-integer members are an intset whatever their count, up to 512.
        let ints: Vec<Vec<u8>> = vec![b"1".to_vec(), b"2".to_vec()];
        assert_eq!(collection_encoding(Set, &ints), "intset");
        assert_eq!(collection_encoding(Set, &small), "listpack");

        // Either threshold promotes it: too many elements, or one too long.
        let many: Vec<Vec<u8>> = (0..129).map(|i| format!("m{i}").into_bytes()).collect();
        assert_eq!(collection_encoding(List, &many), "quicklist");
        assert_eq!(collection_encoding(Zset, &many), "skiplist");
        // A hash's items are field and value both, so 129 fields is 258 items —
        // and 129 items is 64 fields, which still packs.
        assert_eq!(collection_encoding(Hash, &many), "listpack");
        let many_fields: Vec<Vec<u8>> = (0..258).map(|i| format!("m{i}").into_bytes()).collect();
        assert_eq!(collection_encoding(Hash, &many_fields), "hashtable");
        let long: Vec<Vec<u8>> = vec![vec![b'x'; 65]];
        assert_eq!(collection_encoding(List, &long), "quicklist");
        assert_eq!(collection_encoding(Set, &long), "hashtable");
    }

    /// `HRANDFIELD`'s three shapes. A negative count is the only way to ask for
    /// more fields than the hash holds.
    #[test]
    fn hrandfield_takes_distinct_fields_unless_the_count_is_negative() {
        let pairs = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let len = |r: &Response| match r {
            Response::Array(v) => v.len(),
            _ => panic!("expected an array reply"),
        };
        // No count is one field as a bulk string, not a one-element array.
        assert!(matches!(
            random_fields(&pairs, None, false),
            Response::BulkString(ref f) if f == b"a"
        ));
        assert!(matches!(random_fields(&[], None, false), Response::Null));

        assert_eq!(len(&random_fields(&pairs, Some(1), false)), 1);
        // A positive count is capped at what the hash holds.
        assert_eq!(len(&random_fields(&pairs, Some(5), false)), 2);
        // A negative one repeats to reach the count asked for.
        assert_eq!(len(&random_fields(&pairs, Some(-5), false)), 5);
        // WITHVALUES interleaves, so every count doubles.
        assert_eq!(len(&random_fields(&pairs, Some(5), true)), 4);
        assert_eq!(len(&random_fields(&pairs, Some(-3), true)), 6);
        // An empty hash is an empty array whatever the count.
        assert_eq!(len(&random_fields(&[], Some(3), false)), 0);
    }

    /// `GETRANGE`'s clamping, which Redis does rather than erroring. Every case
    /// here reads a value it does not have to bounds-check first.
    #[test]
    fn a_byte_range_clamps_rather_than_erroring() {
        let v = b"Hello";
        assert_eq!(byte_range(v, 0, -1), b"Hello");
        assert_eq!(byte_range(v, 0, 4), b"Hello");
        assert_eq!(byte_range(v, -3, -1), b"llo");
        assert_eq!(byte_range(v, 1, 3), b"ell");
        // Past either end, in both directions.
        assert_eq!(byte_range(v, 0, 100), b"Hello");
        assert_eq!(byte_range(v, -100, -1), b"Hello");
        assert_eq!(byte_range(v, 10, 20), b"");
        assert_eq!(byte_range(v, 3, 1), b"");
        assert_eq!(byte_range(v, -1, -3), b"");
        // An empty value has no range at all, whatever is asked for.
        assert_eq!(byte_range(b"", 0, -1), b"");
        assert_eq!(byte_range(b"", -5, 5), b"");
        // Bytes, not characters: a NUL is a byte like any other.
        assert_eq!(byte_range(b"a\0b", 1, 1), b"\0");
    }

    /// `RPOPLPUSH` is `LMOVE` under its older name, so it has to parse to the
    /// same command rather than to a variant of its own.
    #[test]
    fn rpoplpush_is_lmove_right_to_left() {
        let cmd = is_ok(Command::parse(parts(&["RPOPLPUSH", "a", "b"])));
        assert!(matches!(
            cmd,
            Command::LMove {
                ref src,
                ref dst,
                src_left: false,
                dst_left: true,
            } if src == b"a" && dst == b"b"
        ));
    }

    /// The options `GETEX` takes, and the ones it refuses. Redis reads the
    /// value either way; the option only decides what happens to the expiry.
    #[test]
    fn getex_resolves_every_expiry_option() {
        use GetExExpiry::*;
        let expiry = |argv: &[&str]| match is_ok(Command::parse(parts(argv))) {
            Command::GetEx { expiry, .. } => expiry,
            other => panic!("parsed as {other:?}"),
        };
        assert_eq!(expiry(&["GETEX", "k"]), Keep);
        assert_eq!(expiry(&["GETEX", "k", "PERSIST"]), Clear);
        assert!(matches!(expiry(&["GETEX", "k", "EX", "100"]), At(_)));
        assert!(matches!(expiry(&["GETEX", "k", "PX", "100"]), At(_)));
        assert!(matches!(expiry(&["GETEX", "k", "EXAT", "100"]), At(_)));
        assert!(matches!(expiry(&["GETEX", "k", "PXAT", "100"]), At(_)));

        assert_eq!(
            is_err(Command::parse(parts(&["GETEX", "k", "NOPE", "1"]))),
            "ERR syntax error"
        );
        assert_eq!(
            is_err(Command::parse(parts(&["GETEX", "k", "PERSIST", "1"]))),
            "ERR syntax error"
        );
        assert_eq!(
            is_err(Command::parse(parts(&["GETEX", "k", "EX", "100", "x"]))),
            "ERR syntax error"
        );
        // A non-positive expiry is refused, as it is for SET and EXPIRE.
        assert_eq!(
            is_err(Command::parse(parts(&["GETEX", "k", "EX", "0"]))),
            invalid_expire("getex")
        );
    }

    /// Redis quotes each argument after the command name, and the trailing
    /// space is part of what a client matches on.
    #[test]
    fn an_unknown_command_names_the_arguments_it_was_given() {
        assert_eq!(
            is_err(Command::parse(parts(&["NOSUCHCOMMAND"]))),
            "unknown command 'NOSUCHCOMMAND', with args beginning with: "
        );
        assert_eq!(
            is_err(Command::parse(parts(&["NOSUCHCOMMAND", "arg"]))),
            "unknown command 'NOSUCHCOMMAND', with args beginning with: 'arg' "
        );
        assert_eq!(
            is_err(Command::parse(parts(&["NOSUCHCOMMAND", "a", "b"]))),
            "unknown command 'NOSUCHCOMMAND', with args beginning with: 'a' 'b' "
        );
    }

    /// A command missing from `typed_keys` is a silent `WRONGTYPE` hole: it
    /// would happily read a hash as a list and answer empty. Every command that
    /// names a key of a known type is listed here, against the type it demands.
    #[test]
    fn every_typed_command_declares_the_type_its_key_must_hold() {
        use crate::mem::KeyKind::{Hash, List, Set, String as Str, Zset};

        let cases: &[(&[&str], &[crate::mem::KeyKind])] = &[
            (&["GET", "k"], &[Str]),
            (&["GETDEL", "k"], &[Str]),
            (&["GETSET", "k", "v"], &[Str]),
            (&["APPEND", "k", "v"], &[Str]),
            (&["STRLEN", "k"], &[Str]),
            (&["INCR", "k"], &[Str]),
            (&["DECR", "k"], &[Str]),
            (&["INCRBY", "k", "1"], &[Str]),
            (&["DECRBY", "k", "1"], &[Str]),
            (&["INCRBYFLOAT", "k", "1.5"], &[Str]),
            (&["SET", "k", "v", "GET"], &[Str]),
            (&["HGET", "k", "f"], &[Hash]),
            (&["HSET", "k", "f", "v"], &[Hash]),
            (&["HDEL", "k", "f"], &[Hash]),
            (&["HGETALL", "k"], &[Hash]),
            (&["HMGET", "k", "f"], &[Hash]),
            (&["HMSET", "k", "f", "v"], &[Hash]),
            (&["HKEYS", "k"], &[Hash]),
            (&["HVALS", "k"], &[Hash]),
            (&["HEXISTS", "k", "f"], &[Hash]),
            (&["HLEN", "k"], &[Hash]),
            (&["HINCRBY", "k", "f", "1"], &[Hash]),
            (&["HSETNX", "k", "f", "v"], &[Hash]),
            (&["LPUSH", "k", "v"], &[List]),
            (&["RPUSH", "k", "v"], &[List]),
            (&["LPUSHX", "k", "v"], &[List]),
            (&["RPUSHX", "k", "v"], &[List]),
            (&["LPOP", "k"], &[List]),
            (&["RPOP", "k"], &[List]),
            (&["LLEN", "k"], &[List]),
            (&["LRANGE", "k", "0", "-1"], &[List]),
            (&["LINDEX", "k", "0"], &[List]),
            (&["LSET", "k", "0", "v"], &[List]),
            (&["LINSERT", "k", "BEFORE", "a", "b"], &[List]),
            (&["LREM", "k", "0", "v"], &[List]),
            (&["LPOS", "k", "v"], &[List]),
            (&["LTRIM", "k", "0", "-1"], &[List]),
            (&["SADD", "k", "m"], &[Set]),
            (&["SREM", "k", "m"], &[Set]),
            (&["SMEMBERS", "k"], &[Set]),
            (&["SCARD", "k"], &[Set]),
            (&["SISMEMBER", "k", "m"], &[Set]),
            (&["SMISMEMBER", "k", "m"], &[Set]),
            (&["SPOP", "k"], &[Set]),
            (&["SRANDMEMBER", "k"], &[Set]),
            (&["SUNION", "k"], &[Set]),
            (&["SINTER", "k"], &[Set]),
            (&["SDIFF", "k"], &[Set]),
            (&["ZADD", "k", "1", "m"], &[Zset]),
            (&["ZREM", "k", "m"], &[Zset]),
            (&["ZSCORE", "k", "m"], &[Zset]),
            (&["ZMSCORE", "k", "m"], &[Zset]),
            (&["ZINCRBY", "k", "1", "m"], &[Zset]),
            (&["ZCARD", "k"], &[Zset]),
            (&["ZCOUNT", "k", "0", "1"], &[Zset]),
            (&["ZLEXCOUNT", "k", "-", "+"], &[Zset]),
            (&["ZRANK", "k", "m"], &[Zset]),
            (&["ZRANGE", "k", "0", "-1"], &[Zset]),
            (&["ZRANGEBYSCORE", "k", "0", "1"], &[Zset]),
            (&["ZRANGEBYLEX", "k", "-", "+"], &[Zset]),
            (&["ZPOPMIN", "k"], &[Zset]),
            (&["ZPOPMAX", "k"], &[Zset]),
            (&["ZRANDMEMBER", "k"], &[Zset]),
            (&["ZREMRANGEBYRANK", "k", "0", "1"], &[Zset]),
            (&["ZREMRANGEBYSCORE", "k", "0", "1"], &[Zset]),
            (&["ZREMRANGEBYLEX", "k", "-", "+"], &[Zset]),
            // A plain set reads as a sorted set whose scores are all 1.
            (&["ZUNION", "1", "k"], &[Zset, Set]),
            (&["ZINTER", "1", "k"], &[Zset, Set]),
            (&["ZDIFF", "1", "k"], &[Zset, Set]),
        ];

        for (input, expected) in cases {
            let cmd = is_ok(Command::parse(parts(input)));
            let typed = cmd.typed_keys();
            assert_eq!(
                typed.len(),
                1,
                "{input:?} named {} typed keys, expected one",
                typed.len()
            );
            assert_eq!(typed[0].0, b"k", "{input:?} named the wrong key");
            assert_eq!(typed[0].1, *expected, "{input:?} demands the wrong type");
        }
    }

    /// `(argv, expected typed keys)` — the shape both two-key tests below take.
    type TypedCase<'a> = (&'a [&'a str], &'a [(&'a [u8], &'a [crate::mem::KeyKind])]);
    /// `(argv, expected replaced keys)`.
    type OverwriteCase<'a> = (&'a [&'a str], &'a [(&'a [u8], crate::mem::KeyKind)]);

    /// The two-key commands name both sides, and the store commands type their
    /// sources — the destination is replaced whatever it held, so it is not one
    /// of these.
    #[test]
    fn two_key_commands_type_both_sides() {
        use crate::mem::KeyKind::{List, Set, Zset};

        let cases: &[TypedCase] = &[
            (
                &["LMOVE", "a", "b", "LEFT", "RIGHT"],
                &[(b"a", &[List]), (b"b", &[List])],
            ),
            (&["SMOVE", "a", "b", "m"], &[(b"a", &[Set]), (b"b", &[Set])]),
            (
                &["SUNIONSTORE", "d", "a", "b"],
                &[(b"a", &[Set]), (b"b", &[Set])],
            ),
            (
                &["SINTERSTORE", "d", "a", "b"],
                &[(b"a", &[Set]), (b"b", &[Set])],
            ),
            (
                &["SDIFFSTORE", "d", "a", "b"],
                &[(b"a", &[Set]), (b"b", &[Set])],
            ),
            (
                &["ZUNIONSTORE", "d", "2", "a", "b"],
                &[(b"a", &[Zset, Set]), (b"b", &[Zset, Set])],
            ),
        ];

        for (input, expected) in cases {
            let cmd = is_ok(Command::parse(parts(input)));
            let typed = cmd.typed_keys();
            assert_eq!(typed.len(), expected.len(), "{input:?}");
            for (got, want) in typed.iter().zip(expected.iter()) {
                assert_eq!((got.0, got.1), (want.0, want.1), "{input:?}");
            }
        }
    }

    /// Redis lets these land on a key of any type, replacing what was there.
    /// A command in both lists would be checked and then cleared, which is a
    /// contradiction: one of the two is wrong.
    #[test]
    fn the_commands_that_replace_a_key_are_not_also_type_checked() {
        use crate::mem::KeyKind::{Set, String as Str, Zset};

        let cases: &[OverwriteCase] = &[
            (&["SET", "k", "v"], &[(b"k", Str)]),
            (&["SETEX", "k", "10", "v"], &[(b"k", Str)]),
            (&["PSETEX", "k", "10", "v"], &[(b"k", Str)]),
            (&["MSET", "a", "1", "b", "2"], &[(b"a", Str), (b"b", Str)]),
            (&["SUNIONSTORE", "d", "a"], &[(b"d", Set)]),
            (&["ZUNIONSTORE", "d", "1", "a"], &[(b"d", Zset)]),
            // Conditional writes decide on whether the key is there, so they
            // clear it themselves rather than up front.
            (&["SET", "k", "v", "NX"], &[]),
            (&["SET", "k", "v", "XX"], &[]),
            (&["SET", "k", "v", "GET"], &[]),
            (&["RENAME", "a", "b"], &[]),
        ];

        for (input, expected) in cases {
            let cmd = is_ok(Command::parse(parts(input)));
            let over = cmd.overwritten_keys();
            assert_eq!(over.len(), expected.len(), "{input:?} -> {over:?}");
            for (got, want) in over.iter().zip(expected.iter()) {
                assert_eq!((got.0, got.1), (want.0, want.1), "{input:?}");
            }
            let typed: Vec<&[u8]> = cmd.typed_keys().into_iter().map(|(k, _)| k).collect();
            for (key, _) in expected.iter() {
                assert!(
                    !typed.contains(key),
                    "{input:?} both type-checks and replaces {:?}",
                    std::str::from_utf8(key)
                );
            }
        }
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

    /// Every command in the coverage doc must parse, and every command that
    /// parses must be listed. The page is data for this test, because prose
    /// nobody re-reads drifts — it once claimed LPUSH was unimplemented.
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

        // Every command the page calls absent must actually be absent. This
        // is the direction that drifts: a command gets implemented and the
        // paragraph saying it is not keeps its word for another release.
        let absent = DOC
            .split("## Not implemented")
            .nth(1)
            .expect("the page must have a Not implemented section");
        let named: Vec<&str> = absent
            .split('`')
            .skip(1)
            .step_by(2)
            .filter(|c| {
                !c.is_empty()
                    && c.chars()
                        .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
            })
            .collect();
        assert!(named.len() > 8, "only {} absences named", named.len());
        for cmd in &named {
            let err = Command::parse(vec![cmd.as_bytes().to_vec()])
                .err()
                .unwrap_or_default();
            assert!(
                err.contains("unknown command"),
                "{cmd} is listed as not implemented but the parser accepts it"
            );
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
            "SETRANGE",
            "GETRANGE",
            "SETBIT",
            "GETBIT",
            "BITCOUNT",
            "GETEX",
            "RENAMENX",
            "COPY",
            "OBJECT",
            "HRANDFIELD",
            "HINCRBYFLOAT",
            "SINTERCARD",
            "RPOPLPUSH",
            "FLUSHDB",
            "FLUSHALL",
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

    /// Every command that reaches the shared-memory backend must refuse an
    /// over-long key, field or value before it writes anything. Copying as much
    /// as fits is the alternative, and a truncated member collides with
    /// whatever shares its prefix.
    #[test]
    fn oversized_keys_fields_and_values_are_refused_by_memory_mode() {
        let key = "k".repeat(MEM_MAX_KEY + 1);
        let member = "m".repeat(MEM_MAX_MEMBER + 1);
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
            &"m".repeat(MEM_MAX_MEMBER),
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

    /// Redis's wording, in the shapes clients and people actually see.
    #[test]
    fn error_strings_match_redis() {
        assert_eq!(
            wrong_arity("GET"),
            "wrong number of arguments for 'get' command"
        );
        assert_eq!(
            invalid_expire("SETEX"),
            "invalid expire time in 'setex' command"
        );
    }

    /// A non-positive expiry is refused outright, and one whose deadline could
    /// not be represented is refused rather than stored as something else.
    #[test]
    fn an_expiry_must_be_positive_and_representable() {
        assert!(check_expire(1, 1000, "SET").is_ok());
        assert!(check_expire(0, 1000, "SET").is_err());
        assert!(check_expire(-1, 1000, "SET").is_err());
        assert!(check_expire(i64::MAX, 1000, "SET").is_err());
        // A deadline in the past is legal — `EXPIRE k -1` deletes the key —
        // but an unrepresentable one is not.
        assert!(check_expire_deadline(-1, 1000, "EXPIRE").is_ok());
        assert!(check_expire_deadline(9_999_999_999_999_999, 1000, "EXPIRE").is_err());
    }

    /// `SPOP`, the pops and `ZPOP*` take no negative count; `SRANDMEMBER` and
    /// `ZRANDMEMBER` do, where it means "with repeats", and never reach this.
    #[test]
    fn a_pop_count_may_not_be_negative() {
        assert_eq!(check_count(0), Ok(0));
        assert_eq!(check_count(5), Ok(5));
        assert!(check_count(-1).is_err());
    }

    /// `ON CONFLICT DO UPDATE` cannot touch a row twice in one statement, and
    /// Redis keeps the last of a repeated key.
    #[test]
    fn duplicate_arguments_collapse_to_the_last() {
        let pairs = vec![(b"a".to_vec(), 1), (b"b".to_vec(), 2), (b"a".to_vec(), 3)];
        let deduped = dedup_last(&pairs, |(k, _)| k.as_slice());
        assert_eq!(deduped, vec![(b"b".to_vec(), 2), (b"a".to_vec(), 3)]);
        assert_eq!(
            dedup_last::<(Vec<u8>, i32)>(&[], |(k, _)| k.as_slice()),
            vec![]
        );
    }

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
        // A literal too large to represent is out of range, not infinity —
        // only the spelled-out forms mean that.
        assert_eq!(parse_score_value(b"1e400"), None);
        assert_eq!(parse_score_value(b"-1e400"), None);
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
        assert!(err.contains("wrong number of arguments"));
    }

    #[test]
    fn unwatch_parses() {
        let cmd = Command::parse(vec![b"UNWATCH".to_vec()]).unwrap();
        assert!(matches!(cmd, Command::Unwatch));
    }
}
