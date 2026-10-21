#![forbid(unsafe_code)]
use pgrx::spi::SpiClient;

use crate::memdb::encoding::Enc;
use crate::plans::Kept;

pub const NUM_DBS: usize = 16;

/// The Redis version pg_redis answers as: the one whose behaviour, command
/// table and test suite it follows.
pub const REDIS_VERSION: &str = "7.2.5";

/// Two contiguous halves rather than interleaved by parity: `0..DURABLE_FROM`
/// are ephemeral, the rest WAL-logged. "Anything under 8 is a cache" is a rule
/// a client can hold in its head.
pub const DURABLE_FROM: u8 = 8;

/// Lowest ephemeral database — what `SELECT cache` resolves to.
const CACHE_DB: u8 = 0;

/// Lowest durable database — what `SELECT durable` resolves to.
const DURABLE_DB: u8 = DURABLE_FROM;

/// Whether `db` belongs to the ephemeral half of the default policies: in
/// memory below `DURABLE_FROM`, logged from it. What serves a database now is
/// its policy (`storage.rs`).
#[cfg(any(test, feature = "pg_test"))]
pub const fn is_ephemeral(db: u8) -> bool {
    db < DURABLE_FROM
}

pub mod sql {
    /// The tables that hold a key's data apart from `kv_N`, whose rows carry
    /// no expiry of their own — it lives in `expiry_N`. A stream keeps its whole
    /// value in one row of `stream_N`, and expires the same way.
    pub const COLLECTION_TABLES: [&str; 5] = ["hash", "list", "set", "zset", "stream"];

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

    /// What the key `key` (an SQL expression) holds: `string`, `list`,
    /// `set`, `hash`, `zset` or `stream`; `expired` for a collection past its
    /// deadline, and for a string too when `expired_strings`; NULL for none.
    pub fn kind_case(db: usize, key: &str, expired_strings: bool) -> String {
        let expired_string = if expired_strings {
            format!(
                "WHEN EXISTS (SELECT 1 FROM redis.kv_{db} t WHERE t.key = {key}) THEN 'expired' "
            )
        } else {
            String::new()
        };
        format!(
            "CASE \
               WHEN EXISTS (SELECT 1 FROM redis.kv_{db} t WHERE t.key = {key} \
                              AND (t.expires_at IS NULL OR t.expires_at > now())) THEN 'string' \
               {expired_string}\
               WHEN EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = {key} \
                              AND e.expires_at <= now()) THEN 'expired' \
               WHEN EXISTS (SELECT 1 FROM redis.list_{db} t WHERE t.key = {key}) THEN 'list' \
               WHEN EXISTS (SELECT 1 FROM redis.set_{db}  t WHERE t.key = {key}) THEN 'set' \
               WHEN EXISTS (SELECT 1 FROM redis.hash_{db} t WHERE t.key = {key}) THEN 'hash' \
               WHEN EXISTS (SELECT 1 FROM redis.zset_{db} t WHERE t.key = {key}) THEN 'zset' \
               WHEN EXISTS (SELECT 1 FROM redis.stream_{db} t WHERE t.key = {key}) THEN 'stream' \
             END"
        )
    }

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
        COLLECTION_TABLES
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

    /// The deadline in microseconds since the epoch — the memory half's unit
    /// — for a key to carry it to another key: COPY, MOVE.
    pub static EXPIRE_US: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            ttl_like(
                db,
                &format!("(EXTRACT(EPOCH FROM {DEADLINE}) * 1000000)::bigint"),
            )
        })
    });

    /// The four counters differ in two places: what they store for a key that
    /// is not there yet, and how they change one that is. Everything else is
    /// the guard, and the guard is the part that must not drift — it is what
    /// keeps a value that is not an integer, or an increment that would leave
    /// the `bigint` range, from being written at all.
    /// The sum is taken as `numeric` and must land in range too, so an
    /// overflow updates nothing rather than raising: the caller tells it
    /// apart from a value that is not an integer (`counter_refused`).
    fn counter_like(db: usize, seed: &str, delta: &str) -> String {
        format!(
            "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, convert_to({seed}, 'UTF8')) \
         ON CONFLICT (key) DO UPDATE \
         SET value = convert_to((encode(redis.kv_{db}.value, 'escape')::numeric {delta})::text, 'UTF8') \
         WHERE encode(redis.kv_{db}.value, 'escape') ~ '^(0|-?[1-9][0-9]*)$' \
           AND encode(redis.kv_{db}.value, 'escape')::numeric \
               BETWEEN -9223372036854775808 AND 9223372036854775807 \
           AND encode(redis.kv_{db}.value, 'escape')::numeric {delta} \
               BETWEEN -9223372036854775808 AND 9223372036854775807 \
         RETURNING CAST(encode(value, 'escape') AS bigint)"
        )
    }

    pub static INCRBY: LazyLock<[String; NUM_DBS]> =
        LazyLock::new(|| arr(|db| counter_like(db, "$2::text", "+ $3")));

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
        let mut ctes: Vec<String> = COLLECTION_TABLES
            .iter()
            .enumerate()
            .map(|(i, t)| format!("clr{i} AS (DELETE FROM redis.{t}_{db} WHERE key = $1)"))
            .collect();
        ctes.push(format!(
            "clrx AS (DELETE FROM redis.expiry_{db} WHERE key = $1)"
        ));
        ctes.push(format!(
            "clre AS (DELETE FROM redis.encoding_{db} WHERE key = $1)"
        ));
        format!("{}, ", ctes.join(", "))
    }

    /// Every table a database has, in the order a statement that locks
    /// several of them takes them: `expiry_N` first, as the sweep does.
    pub const DB_TABLES: [&str; 8] = [
        "expiry", "kv", "hash", "list", "set", "zset", "stream", "encoding",
    ];

    /// Every table of `dbs`, in ascending database order and `DB_TABLES`
    /// order within each.
    fn tables_of(dbs: &[usize]) -> String {
        let mut dbs = dbs.to_vec();
        dbs.sort_unstable();
        dbs.iter()
            .flat_map(|db| DB_TABLES.iter().map(move |t| format!("redis.{t}_{db}")))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Empty every table of the named databases at once, leaving no dead
    /// rows for vacuum. `TRUNCATE` takes each table's ACCESS EXCLUSIVE lock,
    /// so whoever runs it must hold the databases exclusive (`dblock`): then
    /// no worker holds a lock on their tables, and those it takes come in
    /// `tables_of`'s order.
    pub fn truncate_dbs(dbs: &[usize]) -> String {
        format!("TRUNCATE {}", tables_of(dbs))
    }

    /// `SWAPDB` of two databases in tables: their tables trade names, and
    /// each index takes its table's, in one transaction that moves no row.
    /// The views are bound to tables, not names, so they are made again.
    pub fn swap_tables(a: usize, b: usize) -> String {
        let tables = DB_TABLES.map(|t| format!("'{t}'")).join(", ");
        format!(
            "LOCK TABLE {locked} IN ACCESS EXCLUSIVE MODE; \
             DO $swap$ \
             DECLARE t text; r record; \
             BEGIN \
               FOREACH t IN ARRAY ARRAY[{tables}] LOOP \
                 EXECUTE format('ALTER TABLE redis.%I RENAME TO %I', t || '_{a}', t || '_swapping'); \
                 EXECUTE format('ALTER TABLE redis.%I RENAME TO %I', t || '_{b}', t || '_{a}'); \
                 EXECUTE format('ALTER TABLE redis.%I RENAME TO %I', t || '_swapping', t || '_{b}'); \
               END LOOP; \
               FOR r IN SELECT i.oid, i.relname AS idx, \
                               substring(i.relname FROM '^[a-z]+_[0-9]+(_.+)$') AS rest \
                          FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid \
                          JOIN pg_class c ON c.oid = x.indrelid \
                          JOIN pg_namespace n ON n.oid = c.relnamespace \
                         WHERE n.nspname = 'redis' AND c.relname ~ '^[a-z]+_({a}|{b})$' LOOP \
                 EXECUTE format('ALTER INDEX redis.%I RENAME TO %I', r.idx, 'swapping_' || r.oid || r.rest); \
               END LOOP; \
               FOR r IN SELECT i.relname AS idx, c.relname AS tbl, \
                               substring(i.relname FROM '^swapping_[0-9]+(_.+)$') AS rest \
                          FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid \
                          JOIN pg_class c ON c.oid = x.indrelid \
                          JOIN pg_namespace n ON n.oid = c.relnamespace \
                         WHERE n.nspname = 'redis' AND i.relname LIKE 'swapping\\_%' LOOP \
                 EXECUTE format('ALTER INDEX redis.%I RENAME TO %I', r.idx, r.tbl || r.rest); \
               END LOOP; \
             END $swap$; \
             {views}",
            locked = tables_of(&[a, b]),
            views = include_str!("../sql/views.sql"),
        )
    }

    /// Empty every table of the named databases, in one statement.
    ///
    /// `DELETE`, not `TRUNCATE`. `TRUNCATE` takes each table's ACCESS
    /// EXCLUSIVE lock in turn, so it deadlocked with any transaction already
    /// writing one of those tables and about to write another: the expiry
    /// sweep, every second, which starts at `expiry_N`. A `DELETE`'s lock
    /// conflicts with no other writer's. The expiry rows go first (the main
    /// statement runs before the CTEs), in the sweep's order.
    pub fn flush_dbs(dbs: &[usize]) -> String {
        let Some((first, rest)) = dbs.split_first() else {
            return "SELECT 1".to_string();
        };
        let ctes: Vec<String> = dbs
            .iter()
            .flat_map(|db| {
                ["kv", "hash", "list", "set", "zset", "stream", "encoding"]
                    .iter()
                    .map(move |t| format!("{t}{db} AS (DELETE FROM redis.{t}_{db})"))
            })
            .chain(
                rest.iter()
                    .map(|db| format!("expiry{db} AS (DELETE FROM redis.expiry_{db})")),
            )
            .collect();
        format!("WITH {} DELETE FROM redis.expiry_{first}", ctes.join(", "))
    }

    /// One statement per database for the background sweep: the strings whose
    /// own column has passed, and every table's rows for a collection whose
    /// `redis.expiry_N` row has. A collection's data lives in four tables and
    /// its expiry in a fifth, so the sweep has to name all of them.
    pub fn sweep_expired(db: usize) -> String {
        let drops = COLLECTION_TABLES
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
        // The keys it removed, for INFO's expired_keys and the `expired`
        // keyspace event.
        format!(
            "WITH dead AS (DELETE FROM redis.expiry_{db} \
                            WHERE expires_at <= now() RETURNING key), \
                  {drops}, \
                  denc AS (DELETE FROM redis.encoding_{db} t \
                            WHERE t.key IN (SELECT key FROM dead)), \
                  s AS (DELETE FROM redis.kv_{db} \
                         WHERE expires_at IS NOT NULL AND expires_at <= now() RETURNING key) \
             SELECT key FROM dead UNION ALL SELECT key FROM s"
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
        let drops = COLLECTION_TABLES
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
        // The keys it removed, for the `expired` event and INFO's
        // expired_keys, as Redis's lazy expiry reports them.
        format!(
            "WITH dead AS (DELETE FROM redis.expiry_{db} \
                            WHERE key = ANY($1::bytea[]) AND expires_at <= now() \
                            RETURNING key), \
                  {drops}, \
                  denc AS (DELETE FROM redis.encoding_{db} t \
                            WHERE t.key IN (SELECT key FROM dead)), \
                  s AS (DELETE FROM redis.kv_{db} \
                         WHERE key = ANY($1::bytea[]) AND expires_at IS NOT NULL \
                           AND expires_at <= now() RETURNING key) \
             SELECT key FROM dead UNION ALL SELECT key FROM s"
        )
    }

    /// `EXPIRE` and its three siblings differ only in how they spell the
    /// deadline. A string's goes in its own row; a collection's has nowhere to
    /// go but `redis.expiry_N`, and only when the key is actually there.
    pub fn set_expiry(db: usize, deadline: &str, cond: super::ExpireCond) -> String {
        let collection = collection_exists(db);
        if cond != super::ExpireCond::Always {
            return set_expiry_if(db, deadline, cond, &collection);
        }
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

    /// `set_expiry` under `NX`/`XX`/`GT`/`LT`: the condition is tested against
    /// the deadline the key holds now, NULL meaning none. A collection with no
    /// `expiry_N` row has no deadline, so only the conditions that accept
    /// "none" may insert one.
    fn set_expiry_if(
        db: usize,
        deadline: &str,
        cond: super::ExpireCond,
        collection: &str,
    ) -> String {
        let kv_ok = cond.sql("t.expires_at", "k.at");
        let row_ok = cond.sql("e.expires_at", "k.at");
        let none_ok = cond.allows_none();
        format!(
            "WITH k AS (SELECT $1::bytea AS key, {deadline} AS at), \
                  s0 AS (SELECT t.key, t.expires_at FROM redis.kv_{db} t, k \
                          WHERE t.key = k.key \
                            AND (t.expires_at IS NULL OR t.expires_at > now())), \
                  s AS (UPDATE redis.kv_{db} t SET expires_at = k.at FROM k \
                         WHERE t.key = k.key AND EXISTS (SELECT 1 FROM s0) \
                           AND {kv_ok} RETURNING 1), \
                  e0 AS (SELECT e.key FROM redis.expiry_{db} e, k \
                          WHERE e.key = k.key AND e.expires_at > now()), \
                  u AS (UPDATE redis.expiry_{db} e SET expires_at = k.at FROM k \
                         WHERE e.key = k.key AND NOT EXISTS (SELECT 1 FROM s0) \
                           AND EXISTS (SELECT 1 FROM e0) AND ({collection}) \
                           AND {row_ok} RETURNING 1), \
                  c AS (INSERT INTO redis.expiry_{db} (key, expires_at) \
                        SELECT k.key, k.at FROM k \
                         WHERE {none_ok} AND NOT EXISTS (SELECT 1 FROM s0) \
                           AND NOT EXISTS (SELECT 1 FROM e0) AND ({collection}) \
                        ON CONFLICT (key) DO UPDATE SET expires_at = EXCLUDED.expires_at \
                        RETURNING 1) \
             SELECT 1 WHERE EXISTS (SELECT 1 FROM s) OR EXISTS (SELECT 1 FROM u) \
                         OR EXISTS (SELECT 1 FROM c)"
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

    /// Refuses a key whose deadline has passed. A collection's rows carry no
    /// expiry of their own — the deadline is a row in `redis.expiry_N` — so a
    /// statement reading them has to ask that table or it serves a key the
    /// sweep has not reached. Mentions `$1` and nothing from the outer query,
    /// so the planner runs it once per statement as an InitPlan rather than
    /// once per row: the liveness question rides inside the round trip the
    /// read was already making, which a separate reap statement was measured
    /// to cost two thirds of collection-read throughput.
    pub fn live(db: usize) -> String {
        format!(
            "NOT EXISTS (SELECT 1 FROM redis.expiry_{db} \
                          WHERE key = $1 AND expires_at <= now())"
        )
    }

    // The thirteen statements below are the cached collection reads, and
    // each carries `live` in its own predicate. That is what lets
    // `Command::collection_read_guarded` route their commands past the
    // up-front reap — the two lists name the same thirteen commands and must
    // move together. A statement that lost its guard is a stale read nothing
    // in-process would notice; the expired-key e2e tests cover every one of
    // the thirteen on both halves, and
    // `every_cached_collection_read_carries_the_liveness_guard` pins the
    // guard's presence here.

    pub static HGET: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT value FROM redis.hash_{db} WHERE key=$1 AND field=$2 AND {}",
                live(db)
            )
        })
    });

    pub static HGETALL: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT field, value FROM redis.hash_{db} WHERE key=$1 AND {} ORDER BY seq",
                live(db)
            )
        })
    });

    pub static HKEYS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT field FROM redis.hash_{db} WHERE key=$1 AND {} ORDER BY seq",
                live(db)
            )
        })
    });

    pub static HVALS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT value FROM redis.hash_{db} WHERE key=$1 AND {} ORDER BY seq",
                live(db)
            )
        })
    });

    pub static HLEN: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT count(*)::bigint FROM redis.hash_{db} WHERE key=$1 AND {}",
                live(db)
            )
        })
    });

    pub static HEXISTS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT 1 FROM redis.hash_{db} WHERE key=$1 AND field=$2 AND {}",
                live(db)
            )
        })
    });

    pub static HSTRLEN: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT length(value)::bigint FROM redis.hash_{db} \
                 WHERE key=$1 AND field=$2 AND {}",
                live(db)
            )
        })
    });

    pub static SCARD: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT count(*)::bigint FROM redis.set_{db} WHERE key=$1 AND {}",
                live(db)
            )
        })
    });

    pub static SMEMBERS: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                // The encoding row rides along: a set of integers with none
                // is an intset, which comes back sorted (`store::set_order`).
                "SELECT member, (SELECT encoding FROM redis.encoding_{db} WHERE key = $1) \
                 FROM redis.set_{db} WHERE key=$1 AND {} ORDER BY seq",
                live(db)
            )
        })
    });

    pub static SISMEMBER: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT 1 FROM redis.set_{db} WHERE key=$1 AND member=$2 AND {}",
                live(db)
            )
        })
    });

    pub static ZCARD: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT count(*)::bigint FROM redis.zset_{db} WHERE key=$1 AND {}",
                live(db)
            )
        })
    });

    pub static ZSCORE: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                "SELECT score FROM redis.zset_{db} WHERE key=$1 AND member=$2 AND {}",
                live(db)
            )
        })
    });

    pub static LLEN: LazyLock<[String; NUM_DBS]> = LazyLock::new(|| {
        arr(|db| {
            format!(
                // Positions are contiguous (`store::list_ends`): the length is
                // the distance between the two ends, two index probes.
                "SELECT CASE WHEN {} THEN COALESCE( \
                    (SELECT pos FROM redis.list_{db} WHERE key = $1 ORDER BY pos DESC LIMIT 1) \
                  - (SELECT pos FROM redis.list_{db} WHERE key = $1 ORDER BY pos ASC LIMIT 1) + 1, 0) \
                  ELSE 0 END::bigint",
                live(db)
            )
        })
    });
}

/// What `sql_reap_and_kinds` found: the type of each live key, and the keys it
/// reaped.
type ReapedKinds = (Vec<(Vec<u8>, crate::memdb::KeyKind)>, Vec<Vec<u8>>);

/// Every command named by a row of the coverage page.
///
/// The page is data, not prose: `the_coverage_doc_lists_exactly_what_parses`
/// asserts it against the parser in both directions, and `COMMAND COUNT`
/// answers with the length of this list. Chained that way, the reply, the
/// page and the parser cannot drift apart in pairs — a command added without
/// a row fails the test, and a row added without a command does too.
pub fn documented_commands() -> impl Iterator<Item = &'static str> {
    include_str!("../docs/command-coverage.md")
        .lines()
        .filter_map(|l| l.strip_prefix("| `"))
        .filter_map(|l| l.split('`').next())
        .filter(|c| {
            !c.is_empty()
                && c.chars()
                    .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
        })
}

/// What `COMMAND COUNT` reports, counted the way Redis counts its own
/// table: every accepted name is a command, aliases included — `SUBSTR` has
/// its own entry even though it parses to the same variant as `GETRANGE`.
pub static COMMAND_COUNT: std::sync::LazyLock<i64> =
    std::sync::LazyLock::new(|| documented_commands().count() as i64);

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

/// What `OBJECT` reports of a key's object beyond its encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectField {
    Refcount,
    Idletime,
    Freq,
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
    /// `AUTH [username] password`; no username means `default`.
    Auth {
        user: Option<Vec<u8>>,
        password: Vec<u8>,
    },
    Time,
    /// `HELLO [protover [AUTH username password] [SETNAME name]]`: the
    /// last AUTH and SETNAME given win.
    Hello {
        proto: Option<u8>,
        auth: Option<(Vec<u8>, Vec<u8>)>,
        setname: Option<Vec<u8>>,
    },
    Reset,
    Quit,
    /// A server-administration command — see `admin.rs`.
    Admin(crate::admin::Admin),
    /// MONITOR: the connection turns into a feed of every command run.
    Monitor,
    /// A stream command — see `stream.rs`.
    Stream(crate::stream::StreamCmd),
    /// One try of a blocking XREAD / XREADGROUP, with the IDs `$` resolved to
    /// on the first try. Answers `StreamWait` when nothing is there. Internal.
    StreamRead {
        cmd: crate::stream::StreamCmd,
        resolved: Option<Vec<Vec<u8>>>,
    },
    // Key-value commands
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ex_ms: Option<i64>,
        /// `EXAT`/`PXAT`: `ex_ms` is a Unix time in milliseconds, kept as
        /// one so it lands exactly, rather than as a TTL measured against a
        /// clock other than the one that adds it back.
        ex_abs: bool,
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
    Touch {
        keys: Vec<Vec<u8>>,
    },
    Expire {
        key: Vec<u8>,
        secs: i64,
        cond: ExpireCond,
    },
    PExpire {
        key: Vec<u8>,
        ms: i64,
        cond: ExpireCond,
    },
    ExpireAt {
        key: Vec<u8>,
        unix_secs: i64,
        cond: ExpireCond,
    },
    PExpireAt {
        key: Vec<u8>,
        unix_ms: i64,
        cond: ExpireCond,
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
        /// `None` when the increment does not parse. Redis checks the key's
        /// type and the stored value before the increment, so the refusal
        /// waits until the command runs.
        delta: Option<crate::longdouble::LongDouble>,
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
    /// `FLUSHDB [ASYNC|SYNC]` — empty the current database. `lazy` when it
    /// runs `ASYNC`, as `lazyfree.rs` counts it; the work is the same.
    FlushDb {
        lazy: bool,
    },
    /// `FLUSHALL [ASYNC|SYNC]` — empty all sixteen, the durable half included.
    FlushAll {
        lazy: bool,
    },
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
        delta: crate::longdouble::LongDouble,
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
    Dump {
        key: Vec<u8>,
    },
    MemoryUsage {
        key: Vec<u8>,
    },
    /// A command built from other commands — see `composite.rs`.
    Composite(crate::composite::Op),
    /// `EVAL`, `FCALL`, `SCRIPT` and `FUNCTION`: run on the dispatcher, see
    /// `scripting.rs`.
    Script(crate::scripting::ScriptCmd),
    /// A blocking command — see `blocking.rs`. Dispatched only inside MULTI,
    /// where it runs once; elsewhere the connection waits and sends tries.
    Block(crate::blocking::BlockOp),
    /// One non-blocking try of a blocking command. Answers a nil array when
    /// nothing is ready, which no successful try ever answers. Internal.
    BlockTry {
        op: crate::blocking::BlockOp,
        first: bool,
    },
    /// `INFO keyspace`'s figures for one database: live keys, and how many of
    /// them carry an expiry. Internal — no client spelling parses to it.
    Keyspace,
    Restore {
        key: Vec<u8>,
        /// Milliseconds, relative unless `absttl`; 0 means no expiry.
        ttl_ms: i64,
        absttl: bool,
        replace: bool,
        payload: Vec<u8>,
        /// `IDLETIME seconds`, applied under an LRU-kept policy.
        idletime: Option<i64>,
        /// `FREQ frequency`, applied under an LFU policy.
        freq: Option<u8>,
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
    /// No command of its own: `SORT` converting the sorted set it reads to a
    /// skiplist, as Redis's does.
    ZsetToSkiplist {
        key: Vec<u8>,
    },
    /// `OBJECT REFCOUNT | IDLETIME | FREQ`.
    ObjectAccess {
        key: Vec<u8>,
        field: ObjectField,
    },
    /// Any other `OBJECT` subcommand, which is answered rather than refused.
    ObjectOther(String),
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
        cursor: u64,
        pattern: Option<Vec<u8>>,
        count: Option<i64>,
        /// `TYPE`: `Some(None)` names no type pg_redis has, so nothing matches.
        kind: Option<Option<crate::memdb::KeyKind>>,
    },
    /// `HSCAN`, `SSCAN` or `ZSCAN`, by the type it iterates: `Hash`, `Set`
    /// or `Zset`.
    CollectionScan {
        kind: crate::memdb::KeyKind,
        key: Vec<u8>,
        cursor: u64,
        count: Option<i64>,
        pattern: Option<Vec<u8>>,
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
    HStrLen {
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
        /// Compare at most this many elements; 0 is no limit.
        maxlen: i64,
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
    Subscribe {
        channels: Vec<Vec<u8>>,
    },
    Unsubscribe {
        names: Vec<Vec<u8>>,
    },
    PSubscribe {
        patterns: Vec<Vec<u8>>,
    },
    PUnsubscribe {
        names: Vec<Vec<u8>>,
    },
    /// Sharded pub/sub: its own namespace, kept under
    /// `pubsub::SHARD_PREFIX`. The names here are as the client sent them.
    SSubscribe {
        channels: Vec<Vec<u8>>,
    },
    SUnsubscribe {
        names: Vec<Vec<u8>>,
    },
    /// `PUBLISH`, and `SPUBLISH` with the channel already prefixed.
    Publish {
        channel: Vec<u8>,
        message: Vec<u8>,
    },
    /// `PUBSUB CHANNELS`, or `SHARDCHANNELS` when `shard`.
    PubSubChannels {
        pattern: Option<Vec<u8>>,
        shard: bool,
    },
    /// `PUBSUB NUMSUB`, or `SHARDNUMSUB` when `shard`.
    PubSubNumSub {
        channels: Vec<Vec<u8>>,
        shard: bool,
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

#[derive(Debug)]
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
    /// `ZRANK ... WITHSCORE`: the rank as an integer, the score as a bulk
    /// string — one array of two types, which `Array` cannot spell.
    RankWithScore(i64, Vec<u8>),
    /// A blocking XREAD found nothing yet; the IDs to retry from. Internal —
    /// the connection loop consumes it, no client ever sees it.
    StreamWait(Vec<Vec<u8>>),
    /// Any reply shape, nesting included — what the variants above cannot
    /// spell: `LMPOP`'s key-and-list, `LCS IDX`, `XRANGE`, `SLOWLOG GET`.
    Value(Value),
    /// A `SCAN`-family reply: the cursor to continue from, in decimal, and
    /// the elements found.
    ScanResult {
        cursor: u64,
        keys: Vec<Option<Vec<u8>>>,
    },
    Error(String),
}

/// A reply of any shape. Written as RESP2: a map is a flat array of
/// alternating keys and values, a double a bulk string, as Redis does for a
/// RESP2 client.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Nil,
    NilArray,
    Int(i64),
    Bulk(Vec<u8>),
    Simple(String),
    /// An error passed through as given — a script's own error reply.
    Error(String),
    /// The same, already counted in `errorstats` where a command a script
    /// called failed, so writing it counts nothing more.
    CountedError(String),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
    /// The RESP3 types. A RESP2 client gets what Redis gives it: a double as
    /// a bulk string, a set as an array, a boolean as 1 or 0, verbatim text
    /// as a bulk string.
    Double(f64),
    Set(Vec<Value>),
    Bool(bool),
    Verbatim(String),
    /// A RESP3 big number; a bulk string to RESP2.
    BigNum(String),
    /// A RESP3 attribute ahead of the reply it describes. RESP2, and a
    /// script, get only the reply.
    Attributed(Vec<(Value, Value)>, Box<Value>),
    /// An out-of-band RESP3 push ahead of the reply.
    Pushed(Vec<Value>, Box<Value>),
}

impl Response {
    /// The reply in the one shape every reply has, which RESP3 framing and
    /// script conversions work from.
    pub fn into_value(self) -> Value {
        match self {
            Response::Pong(None) => Value::Simple("PONG".into()),
            Response::Pong(Some(m)) => Value::Bulk(m),
            Response::Null => Value::Nil,
            Response::NullArray | Response::StreamWait(_) => Value::NilArray,
            Response::Ok => Value::Simple("OK".into()),
            Response::Integer(n) => Value::Int(n),
            Response::BulkString(b) => Value::Bulk(b),
            Response::SimpleString(s) => Value::Simple(s),
            Response::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|i| i.map_or(Value::Nil, Value::Bulk))
                    .collect(),
            ),
            Response::IntegerArray(items) => {
                Value::Array(items.into_iter().map(Value::Int).collect())
            }
            Response::RankWithScore(rank, score) => {
                Value::Array(vec![Value::Int(rank), Value::Bulk(score)])
            }
            Response::Value(v) => v,
            Response::ScanResult { cursor, keys } => Value::Array(vec![
                Value::Bulk(cursor.to_string().into_bytes()),
                Value::Array(
                    keys.into_iter()
                        .map(|k| k.map_or(Value::Nil, Value::Bulk))
                        .collect(),
                ),
            ]),
            Response::Error(e) => Value::Error(if crate::resp::has_error_code(&e) {
                e
            } else {
                format!("ERR {e}")
            }),
        }
    }
}

impl Value {
    pub fn bulk(b: impl Into<Vec<u8>>) -> Self {
        Value::Bulk(b.into())
    }

    pub fn str(s: &str) -> Self {
        Value::Bulk(s.as_bytes().to_vec())
    }
}

/// One row of a collection as `dump_key` reads it back: the member or field,
/// a score if the type has one, and a value if the type has one.
type DumpRow = (Vec<u8>, Option<f64>, Option<Vec<u8>>);

/// Everything one key holds, in a form neither backend stores — see `dump_key`.
/// A set's members and a hash's fields are in their order, and with the
/// encoding Redis would have given them, so a copy is the same collection.
pub(crate) enum KeyDump {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>, Enc),
    Hash(Vec<(Vec<u8>, Vec<u8>)>, Enc),
    Zset(Vec<(Vec<u8>, f64)>, Enc),
    /// A stream's serialised value — see `stream.rs`.
    Stream(Vec<u8>),
}

/// Whether this database is served from shared memory rather than SQL. Both
/// halves are reachable from one command only for `COPY`, which is the only one
/// that has to ask.
fn uses_shared_memory(db: u8) -> bool {
    crate::storage::slot(db).is_some()
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
    /// Parse a command line. Borrows it: the connection keeps the arguments
    /// for the slow log, which a parse that took them would make it copy.
    pub fn parse(parts: impl AsRef<[Vec<u8>]>) -> Result<Command, String> {
        let parts = parts.as_ref();
        if parts.is_empty() {
            return Err("empty command".to_string());
        }
        let cmd = String::from_utf8_lossy(&parts[0]).to_uppercase();
        let args = &parts[1..];
        if let Some(e) = arity_error(&cmd, parts) {
            return Err(e);
        }

        match cmd.as_str() {
            "PING" if args.len() > 1 => Err(wrong_arity("PING")),
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
            "AUTH" => {
                if args.len() > 2 {
                    return Err("ERR syntax error".to_string());
                }
                Ok(Command::Auth {
                    user: (args.len() == 2).then(|| args[0].clone()),
                    password: args[args.len() - 1].clone(),
                })
            }
            "HELLO" => {
                let proto = if args.is_empty() {
                    None
                } else {
                    let v: i64 = std::str::from_utf8(&args[0])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| {
                            "ERR Protocol version is not an integer or out of range".to_string()
                        })?;
                    if v != 2 && v != 3 {
                        return Err("NOPROTO unsupported protocol version".to_string());
                    }
                    Some(v as u8)
                };
                let mut auth = None;
                let mut setname = None;
                let mut i = 1;
                while i < args.len() {
                    let more = args.len() - 1 - i;
                    if args[i].eq_ignore_ascii_case(b"AUTH") && more >= 2 {
                        auth = Some((args[i + 1].clone(), args[i + 2].clone()));
                        i += 3;
                    } else if args[i].eq_ignore_ascii_case(b"SETNAME") && more >= 1 {
                        let name = &args[i + 1];
                        if name.iter().any(|&c| c <= b' ' || c > b'~') {
                            return Err("ERR Client names cannot contain spaces, newlines or special characters."
                                .to_string());
                        }
                        setname = Some(name.clone());
                        i += 2;
                    } else {
                        return Err(format!(
                            "ERR Syntax error in HELLO option '{}'",
                            String::from_utf8_lossy(&args[i])
                        ));
                    }
                }
                Ok(Command::Hello {
                    proto,
                    auth,
                    setname,
                })
            }
            "RESET" => Ok(Command::Reset),
            "MONITOR" => Ok(Command::Monitor),
            "QUIT" => Ok(Command::Quit),
            "GET" => Ok(Command::Get {
                key: bytes_arg(args, 0, "GET")?,
            }),
            "SET" => {
                let key = bytes_arg(args, 0, "SET")?;
                let value = bytes_arg(args, 1, "SET")?;
                let mut ex_ms: Option<i64> = None;
                let mut ex_abs = false;
                let mut nx = false;
                let mut xx = false;
                let mut get = false;
                let mut keepttl = false;
                let mut ttl_set = false;
                let mut i = 2;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    // One expiry option at most, and each one needs its value.
                    if matches!(opt.as_str(), "EX" | "PX" | "EXAT" | "PXAT")
                        && (ttl_set || i + 1 >= args.len())
                    {
                        return Err("syntax error".to_string());
                    }
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
                            ex_ms = Some(check_expire(secs, 1000, "SET")?);
                            ex_abs = true;
                            ttl_set = true;
                            i += 2;
                        }
                        "PXAT" => {
                            let ms: i64 = str_arg(args, i + 1, "SET PXAT")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            ex_ms = Some(check_expire(ms, 1, "SET")?);
                            ex_abs = true;
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
                        _ => return Err("syntax error".to_string()),
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
                    ex_abs,
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
                    1000,
                    "SETEX",
                )? / 1000,
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
            "TIME" => {
                if !args.is_empty() {
                    return Err(wrong_arity("TIME"));
                }
                Ok(Command::Time)
            }
            "TOUCH" => {
                if args.is_empty() {
                    return Err(wrong_arity("TOUCH"));
                }
                Ok(Command::Touch {
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
                cond: ExpireCond::parse(&args[2..])?,
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
                cond: ExpireCond::parse(&args[2..])?,
            }),
            "EXPIREAT" => Ok(Command::ExpireAt {
                key: bytes_arg(args, 0, "EXPIREAT")?,
                unix_secs: str_arg(args, 1, "EXPIREAT")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
                cond: ExpireCond::parse(&args[2..])?,
            }),
            "PEXPIREAT" => Ok(Command::PExpireAt {
                key: bytes_arg(args, 0, "PEXPIREAT")?,
                unix_ms: str_arg(args, 1, "PEXPIREAT")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?,
                cond: ExpireCond::parse(&args[2..])?,
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
                delta: crate::longdouble::LongDouble::parse(&bytes_arg(args, 1, "INCRBYFLOAT")?),
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
                // A key named twice is set twice in Redis, so the last value
                // wins; keep one pair per key, holding that value.
                let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(args.len() / 2);
                for c in args.chunks(2) {
                    match pairs.iter_mut().find(|(k, _)| *k == c[0]) {
                        Some(pair) => pair.1 = c[1].to_vec(),
                        None => pairs.push((c[0].to_vec(), c[1].to_vec())),
                    }
                }
                Ok(Command::MSetNx { pairs })
            }
            "TYPE" => Ok(Command::Type {
                key: bytes_arg(args, 0, "TYPE")?,
            }),
            "KEYS" => Ok(Command::Keys {
                pattern: bytes_arg(args, 0, "KEYS")?,
            }),
            "DBSIZE" => Ok(Command::DbSize),
            // Nothing here defers the work: ASYNC only decides what
            // `lazyfreed_objects` counts. Without either, as Redis,
            // `lazyfree-lazy-user-flush` decides.
            "FLUSHDB" | "FLUSHALL" => {
                let lazy = match args.first() {
                    None => crate::config::get_bool("lazyfree-lazy-user-flush"),
                    Some(mode) => match String::from_utf8_lossy(mode).to_uppercase().as_str() {
                        "ASYNC" => true,
                        "SYNC" => false,
                        _ => return Err("ERR syntax error".to_string()),
                    },
                };
                if args.len() > 1 {
                    return Err(wrong_arity(&cmd));
                }
                Ok(match cmd.as_str() {
                    "FLUSHDB" => Command::FlushDb { lazy },
                    _ => Command::FlushAll { lazy },
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
                let value = bytes_arg(args, 2, "SETRANGE")?;
                // An empty value writes nothing and so is never too large.
                if !value.is_empty()
                    && offset
                        .checked_add(value.len() as i64)
                        .is_none_or(|end| end > MAX_STRING_LEN)
                {
                    return Err(
                        "string exceeds maximum allowed size (proto-max-bulk-len)".to_string()
                    );
                }
                Ok(Command::SetRange { key, offset, value })
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
                // Redis checks the increment before anything else, and
                // refuses an infinite one up front: unlike `INCRBYFLOAT`,
                // which only checks the result.
                let delta = crate::longdouble::LongDouble::parse(
                    bytes_arg(args, 2, "HINCRBYFLOAT")?.as_slice(),
                )
                .ok_or_else(|| NOT_A_FLOAT.to_string())?;
                if !delta.is_finite() {
                    return Err("ERR value is NaN or Infinity".to_string());
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
                        } else if !matches!(opt.as_str(), "EX" | "PX" | "EXAT" | "PXAT")
                            || args.len() != 3
                        {
                            return Err("ERR syntax error".to_string());
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
                            GetExExpiry::At(deadline_us(0, ms, 1000))
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
            "MEMORY" => match str_arg(args, 0, "MEMORY")?.to_uppercase().as_str() {
                "USAGE" => {
                    let key = bytes_arg(args, 1, "MEMORY|USAGE")?;
                    // SAMPLES is accepted and ignored: the estimate below
                    // already reads the whole key.
                    match &args[2..] {
                        [] => {}
                        [opt, n] if opt.eq_ignore_ascii_case(b"SAMPLES") => {
                            std::str::from_utf8(n)
                                .ok()
                                .and_then(|n| n.parse::<i64>().ok())
                                .ok_or_else(|| NOT_AN_INTEGER.to_string())?;
                        }
                        _ => return Err("ERR syntax error".to_string()),
                    }
                    Ok(Command::MemoryUsage { key })
                }
                _ => Ok(Command::Admin(crate::admin::Admin {
                    name: "MEMORY".to_string(),
                    args: args.to_vec(),
                    saved: false,
                })),
            },
            "DUMP" => {
                if args.len() != 1 {
                    return Err(wrong_arity("DUMP"));
                }
                Ok(Command::Dump {
                    key: bytes_arg(args, 0, "DUMP")?,
                })
            }
            "RESTORE" => {
                let key = bytes_arg(args, 0, "RESTORE")?;
                // Options first, as `restoreCommand` reads them: IDLETIME and
                // FREQ exclude each other, and each needs its value.
                let (mut replace, mut absttl) = (false, false);
                let (mut idletime, mut freq): (Option<i64>, Option<u8>) = (None, None);
                let mut i = 3;
                while i < args.len() {
                    let more = i + 1 < args.len();
                    let int = |j: usize| -> Result<i64, String> {
                        String::from_utf8_lossy(&args[j])
                            .parse()
                            .map_err(|_| NOT_AN_INTEGER.to_string())
                    };
                    match String::from_utf8_lossy(&args[i]).to_uppercase().as_str() {
                        "REPLACE" => replace = true,
                        "ABSTTL" => absttl = true,
                        "IDLETIME" if more && freq.is_none() => {
                            let n = int(i + 1)?;
                            if n < 0 {
                                return Err("ERR Invalid IDLETIME value, must be >= 0".to_string());
                            }
                            idletime = Some(n);
                            i += 1;
                        }
                        "FREQ" if more && idletime.is_none() => {
                            let n = int(i + 1)?;
                            if !(0..=255).contains(&n) {
                                return Err(
                                    "ERR Invalid FREQ value, must be >= 0 and <= 255".to_string()
                                );
                            }
                            freq = Some(n as u8);
                            i += 1;
                        }
                        _ => return Err("ERR syntax error".to_string()),
                    }
                    i += 1;
                }
                let ttl_ms: i64 = str_arg(args, 1, "RESTORE")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                if ttl_ms < 0 {
                    return Err("ERR Invalid TTL value, must be >= 0".to_string());
                }
                let payload = bytes_arg(args, 2, "RESTORE")?;
                Ok(Command::Restore {
                    key,
                    ttl_ms,
                    absttl,
                    replace,
                    payload,
                    idletime,
                    freq,
                })
            }
            "HRANDFIELD" => {
                let key = bytes_arg(args, 0, "HRANDFIELD")?;
                let withvalues = match args.get(2) {
                    None => false,
                    Some(a) if a.eq_ignore_ascii_case(b"WITHVALUES") => true,
                    Some(_) => return Err("ERR syntax error".to_string()),
                };
                if args.len() > 3 {
                    return Err("ERR syntax error".to_string());
                }
                let count = match args.get(1) {
                    None => None,
                    Some(_) => Some(random_count(&str_arg(args, 1, "HRANDFIELD")?, withvalues)?),
                };
                Ok(Command::HRandField {
                    key,
                    count,
                    withvalues,
                })
            }
            "SINTERCARD" => {
                let (keys, limit) = intercard_args(args, "SINTERCARD")?;
                Ok(Command::SInterCard { keys, limit })
            }
            "OBJECT" => match str_arg(args, 0, "OBJECT")?.to_uppercase().as_str() {
                "ENCODING" => Ok(Command::ObjectEncoding {
                    key: bytes_arg(args, 1, "OBJECT")?,
                }),
                sub @ ("REFCOUNT" | "IDLETIME" | "FREQ") => Ok(Command::ObjectAccess {
                    key: bytes_arg(args, 1, "OBJECT")?,
                    field: match sub {
                        "REFCOUNT" => ObjectField::Refcount,
                        "IDLETIME" => ObjectField::Idletime,
                        _ => ObjectField::Freq,
                    },
                }),
                other => Ok(Command::ObjectOther(other.to_string())),
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
            "HSCAN" | "SSCAN" | "ZSCAN" => parse_collection_scan(&cmd, args),
            "SCAN" => {
                let cursor: u64 = str_arg(args, 0, "SCAN")?
                    .parse()
                    .map_err(|_| NOT_AN_INTEGER.to_string())?;
                let mut pattern: Option<Vec<u8>> = None;
                let mut count: Option<i64> = None;
                let mut kind: Option<Option<crate::memdb::KeyKind>> = None;
                let mut i = 1;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    if i + 1 >= args.len() {
                        return Err("ERR syntax error".to_string());
                    }
                    match opt.as_str() {
                        "MATCH" => {
                            pattern = Some(bytes_arg(args, i + 1, "SCAN MATCH")?);
                        }
                        "COUNT" => {
                            let n: i64 = str_arg(args, i + 1, "SCAN COUNT")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            if n < 1 {
                                return Err("ERR syntax error".to_string());
                            }
                            count = Some(n);
                        }
                        "TYPE" => {
                            use crate::memdb::KeyKind;
                            kind = Some(
                                match str_arg(args, i + 1, "SCAN TYPE")?.to_lowercase().as_str() {
                                    "string" => Some(KeyKind::String),
                                    "list" => Some(KeyKind::List),
                                    "set" => Some(KeyKind::Set),
                                    "hash" => Some(KeyKind::Hash),
                                    "zset" => Some(KeyKind::Zset),
                                    "stream" => Some(KeyKind::Stream),
                                    _ => None,
                                },
                            );
                        }
                        _ => return Err("ERR syntax error".to_string()),
                    }
                    i += 2;
                }
                Ok(Command::Scan {
                    cursor,
                    pattern,
                    count,
                    kind,
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
            "HSTRLEN" => Ok(Command::HStrLen {
                key: bytes_arg(args, 0, "HSTRLEN")?,
                field: bytes_arg(args, 1, "HSTRLEN")?,
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
                if args.len() > 2 {
                    return Err(wrong_arity("LPOP"));
                }
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
                if args.len() > 2 {
                    return Err(wrong_arity("RPOP"));
                }
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
                    _ => return Err("ERR syntax error".to_string()),
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
                let mut maxlen: i64 = 0;
                let mut i = 2;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    if i + 1 >= args.len() {
                        return Err("ERR syntax error".to_string());
                    }
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
                            if count.is_some_and(|c| c < 0) {
                                return Err("ERR COUNT can't be negative".to_string());
                            }
                            i += 2;
                        }
                        "MAXLEN" => {
                            maxlen = str_arg(args, i + 1, "LPOS MAXLEN")?
                                .parse()
                                .map_err(|_| NOT_AN_INTEGER.to_string())?;
                            if maxlen < 0 {
                                return Err("ERR MAXLEN can't be negative".to_string());
                            }
                            i += 2;
                        }
                        _ => return Err("ERR syntax error".to_string()),
                    }
                }
                // Redis refuses LONG_MIN: its absolute value is not a rank.
                if rank == Some(i64::MIN) {
                    return Err(
                        "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"
                            .to_string(),
                    );
                }
                if matches!(rank, Some(0)) {
                    return Err("RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list".to_string());
                }
                Ok(Command::LPos {
                    key,
                    element,
                    rank,
                    count,
                    maxlen,
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
                if args.len() > 2 {
                    return Err("ERR syntax error".to_string());
                }
                let count = match args.get(1) {
                    None => None,
                    Some(_) => Some(random_count(&str_arg(args, 1, "SRANDMEMBER")?, false)?),
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
                // A fixed rank range: WITHSCORES is its only option.
                if args
                    .iter()
                    .skip(3)
                    .any(|a| !a.eq_ignore_ascii_case(b"WITHSCORES"))
                {
                    return Err("ERR syntax error".to_string());
                }
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
                let with_scores = match args.get(2) {
                    None => false,
                    Some(a) if a.eq_ignore_ascii_case(b"WITHSCORES") => true,
                    Some(_) => return Err("ERR syntax error".to_string()),
                };
                if args.len() > 3 {
                    return Err("ERR syntax error".to_string());
                }
                let count = match args.get(1) {
                    None => None,
                    Some(_) => Some(random_count(
                        &str_arg(args, 1, "ZRANDMEMBER")?,
                        with_scores,
                    )?),
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

            "SUBSCRIBE" => {
                if args.is_empty() {
                    return Err(wrong_arity("SUBSCRIBE"));
                }
                Ok(Command::Subscribe {
                    channels: args.to_vec(),
                })
            }
            "UNSUBSCRIBE" => Ok(Command::Unsubscribe {
                names: args.to_vec(),
            }),
            "PSUBSCRIBE" => {
                if args.is_empty() {
                    return Err(wrong_arity("PSUBSCRIBE"));
                }
                Ok(Command::PSubscribe {
                    patterns: args.to_vec(),
                })
            }
            "PUNSUBSCRIBE" => Ok(Command::PUnsubscribe {
                names: args.to_vec(),
            }),
            "PUBLISH" => {
                if args.len() != 2 {
                    return Err(wrong_arity("PUBLISH"));
                }
                Ok(Command::Publish {
                    channel: args[0].clone(),
                    message: args[1].clone(),
                })
            }
            "SPUBLISH" => {
                if args.len() != 2 {
                    return Err(wrong_arity("SPUBLISH"));
                }
                Ok(Command::Publish {
                    channel: crate::pubsub::shard_name(&args[0]),
                    message: args[1].clone(),
                })
            }
            "SSUBSCRIBE" => {
                if args.is_empty() {
                    return Err(wrong_arity("SSUBSCRIBE"));
                }
                Ok(Command::SSubscribe {
                    channels: args.to_vec(),
                })
            }
            "SUNSUBSCRIBE" => Ok(Command::SUnsubscribe {
                names: args.to_vec(),
            }),
            "PUBSUB" => {
                let sub = args
                    .first()
                    .map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("CHANNELS") => Ok(Command::PubSubChannels {
                        pattern: args.get(1).cloned(),
                        shard: false,
                    }),
                    Some("SHARDCHANNELS") => Ok(Command::PubSubChannels {
                        pattern: args.get(1).cloned(),
                        shard: true,
                    }),
                    Some("NUMSUB") => Ok(Command::PubSubNumSub {
                        channels: args[1..].to_vec(),
                        shard: false,
                    }),
                    Some("SHARDNUMSUB") => Ok(Command::PubSubNumSub {
                        channels: args[1..].to_vec(),
                        shard: true,
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

            // Commands built from other commands, then Redis's unknown-command
            // error, which names the command and quotes each argument, one
            // space apart, with a trailing space — the whole of it, that
            // space included, is what a client's error handling matches on.
            _ => match crate::composite::Op::parse(&cmd, args) {
                Some(op) => op.map(Command::Composite),
                None if let Some(s) = crate::scripting::parse(&cmd, &parts[0], args) => s,
                None if let Some(sc) = crate::stream::StreamCmd::parse(&cmd, args) => {
                    sc.map(Command::Stream)
                }
                None if let Some(b) = crate::blocking::BlockOp::parse(&cmd, args) => {
                    b.map(Command::Block)
                }
                None if let Some(a) = crate::admin::Admin::parse(&cmd, args) => {
                    Ok(Command::Admin(a))
                }
                None => Err(format!(
                    "unknown command '{cmd}', with args beginning with: {}",
                    args.iter()
                        .map(|a| format!("'{}' ", String::from_utf8_lossy(a)))
                        .collect::<String>()
                )),
            },
        }
    }

    /// Run on the SQL path. What it reaches of the memory engine — `COPY`,
    /// `DUMP`, `RESTORE` — looks keys up as the command would.
    pub fn execute(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
        // `maxmemory` is the server's, as in Redis: past it a write that can
        // grow memory is refused on a logged database too.
        if self.may_grow() && !crate::evict::make_room() {
            return mem_out_of_memory();
        }
        let access = crate::access::mode(self, crate::tracking::origin());
        crate::access::with_mode(access, || self.execute_scoped(client, db))
    }

    fn execute_scoped(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
        // A write has to be refused before it lands, so its keys are checked up
        // front. A read cannot damage anything, and a read of a key holding
        // another type finds nothing — so it runs first and only asks what the
        // key holds when it came back empty. That keeps the extra round trip
        // off every successful `GET` and `GETRANGE`.
        //
        // A read of a *collection* is the exception, and has to be. Its rows
        // outlive the key's expiry until the sweep runs, so coming back with
        // data is not evidence the key is live — which is how `LLEN` answered
        // 3 for a key `TYPE` had already called gone. Strings are excluded
        // because they never had the problem: the expiry is a column of
        // `kv_N` and every statement reading one tests it, which is why the
        // ordering above could be an optimisation in the first place.
        //
        // Two mechanisms make a collection read honest, and which applies is
        // decided here and nowhere else. The cached single-key reads carry
        // the liveness test inside their own statement and pay nothing;
        // everything else reaps its keys first, one extra statement that
        // cannot be missed by a statement that forgot the predicate.
        let writes = !self.write_keys().is_empty();
        let mut probed = false;
        if writes || self.reply_hides_wrong_type() {
            match self.sql_probe_types(client, db) {
                Err(err) => return err,
                Ok(p) => probed = p,
            }
        }
        let guarded = self.collection_read_guarded();
        if !writes && !guarded && self.reads_a_collection() {
            self.sql_reap_expired(client, db);
        } else if !writes && !self.reaps_on_miss() {
            // Redis deletes a key it finds past its deadline on a read,
            // counts it in expired_keys, invalidates it and raises
            // `expired`, and DBSIZE no longer counts it: reap first. In the
            // ordinary case, nothing expired, that is one indexed lookup on
            // the keys that carry a deadline.
            let keys: Vec<&[u8]> = self.typed_keys().iter().map(|(k, _)| *k).collect();
            Self::reap_keys(client, db, &keys);
        }
        // The probe reaped these already, in the same statement.
        let stale = self.lazily_expires();
        if !probed && !stale.is_empty() {
            Self::reap_keys(client, db, &stale);
        }
        let response = self.execute_inner(client, db);
        // GET and MGET filter expired keys out in their own statement, so a
        // key they return is live and only a miss can have met an expired
        // one: reaping then, rather than before every read, keeps the hot
        // reads to one statement. A miss asks what each missed key holds, in
        // one statement that only reads, and reaps only the keys it found
        // past their deadline. The reap deletes across every table, and run
        // on every miss it cost one six times what a hit did, for the rare
        // miss that had anything to delete.
        let missed: Vec<&[u8]> = match (self, &response) {
            (Command::Get { key }, Response::Null) => vec![key.as_slice()],
            (Command::MGet { keys }, Response::Array(values)) => keys
                .iter()
                .zip(values)
                .filter(|(_, v)| v.is_none())
                .map(|(k, _)| k.as_slice())
                .collect(),
            _ => Vec::new(),
        };
        if !missed.is_empty() {
            let kinds = Self::sql_missed_kinds(client, db, &missed);
            let expired: Vec<&[u8]> = kinds
                .iter()
                .filter(|(_, kind)| kind.is_none())
                .map(|(k, _)| k.as_slice())
                .collect();
            Self::reap_keys(client, db, &expired);
            // MGET answers nil for a key of another type; GET refuses it.
            let other_type = kinds
                .iter()
                .any(|(_, kind)| kind.is_some_and(|k| k != crate::memdb::KeyKind::String));
            if matches!(self, Command::Get { .. }) && other_type {
                return wrong_type();
            }
            return response;
        }
        if !writes && response.found_nothing() {
            // A guarded read that found nothing may have refused an expired
            // key whose rows are still in the tables. They have to go before
            // the type check looks, or `LLEN` on an expired hash would answer
            // WRONGTYPE where Redis answers 0 — and going now spares the
            // sweep the work. This is the miss path, so the statement costs
            // nothing where it matters.
            if guarded {
                self.sql_reap_expired(client, db);
            }
            if let Some(err) = self.sql_type_error(client, db) {
                return err;
            }
        }
        response
    }

    /// The reads that reap the keys they missed after running, not before.
    fn reaps_on_miss(&self) -> bool {
        matches!(self, Command::Get { .. } | Command::MGet { .. })
    }

    /// Reads whose reply is not empty even when a key holds the wrong type, so
    /// the miss-path type check above would never fire. A multi-key read gets
    /// its reply from the keys that are fine — `SUNION str set` answered the
    /// set — and the per-element replies (`SMISMEMBER`, `HMGET`, `ZMSCORE`)
    /// are as long as the request whatever the key holds. A scan's reply is
    /// never empty either: it always carries a cursor.
    fn reply_hides_wrong_type(&self) -> bool {
        matches!(
            self,
            Command::SMisMember { .. }
                | Command::HMGet { .. }
                | Command::ZMScore { .. }
                | Command::CollectionScan { .. }
        ) || self.typed_keys().len() > 1
    }

    /// Everything one key holds, whatever type holds it. `COPY` is the only
    /// command that has to move a whole key between two backends, so it is the
    /// only one that needs the value in a form neither of them stores.
    pub(crate) fn dump_key(
        client: &mut SpiClient<'_>,
        db: u8,
        key: &[u8],
    ) -> Option<(KeyDump, i64)> {
        Self::dump_key_at(client, db, key, crate::storage::slot(db))
    }

    /// `dump_key` from a named backend: the memory slot, or `db`'s tables.
    pub(crate) fn dump_key_at(
        client: &mut SpiClient<'_>,
        db: u8,
        key: &[u8],
        mem_slot: Option<usize>,
    ) -> Option<(KeyDump, i64)> {
        use crate::memdb::KeyKind;
        if let Some(slot) = mem_slot {
            return crate::shm::with_db(slot, |m| m.dump(key)).ok().flatten();
        }
        let kind = Self::sql_key_kinds(client, db, &[key])
            .first()
            .and_then(|(_, k)| *k)?;
        let idx = db as usize;
        let dump = match kind {
            KeyKind::Stream => KeyDump::Stream(
                client
                    .select_kept(
                        &format!("SELECT value FROM redis.stream_{db} WHERE key = $1"),
                        &[key.into()],
                    )
                    .ok()?
                    .first()
                    .get(1)
                    .ok()
                    .flatten()?,
            ),
            KeyKind::String => KeyDump::String(
                client
                    .select_kept(&sql::GET[idx], &[key.into()])
                    .ok()?
                    .first()
                    .get(1)
                    .ok()
                    .flatten()?,
            ),
            kind => {
                // In the collection's order, with its encoding row: a set's
                // and a hash's by `seq`, a sorted set's by score.
                let enc_col = format!("(SELECT encoding FROM redis.encoding_{db} WHERE key = $1)");
                let sql = match kind {
                    KeyKind::List => format!(
                        "SELECT value, NULL::float8, NULL::bytea, NULL::smallint \
                         FROM redis.list_{db} WHERE key = $1 ORDER BY pos"
                    ),
                    KeyKind::Set => format!(
                        "SELECT member, NULL::float8, NULL::bytea, {enc_col} \
                         FROM redis.set_{db} WHERE key = $1 ORDER BY seq"
                    ),
                    KeyKind::Zset => format!(
                        "SELECT member, score, NULL::bytea, {enc_col} \
                         FROM redis.zset_{db} WHERE key = $1 ORDER BY score, member"
                    ),
                    _ => format!(
                        "SELECT field, NULL::float8, value, {enc_col} \
                         FROM redis.hash_{db} WHERE key = $1 ORDER BY seq"
                    ),
                };
                let tbl = client.select_kept(&sql, &[key.into()]).ok()?;
                let mut items: Vec<DumpRow> = Vec::new();
                let mut row_enc = None;
                for row in tbl {
                    let Ok(Some(a)) = row.get::<Vec<u8>>(1) else {
                        continue;
                    };
                    let score = row.get::<f64>(2).ok().flatten();
                    let value = row.get::<Vec<u8>>(3).ok().flatten();
                    row_enc = row.get::<i16>(4).ok().flatten().and_then(Enc::from_code);
                    items.push((a, score, value));
                }
                match kind {
                    KeyKind::List => KeyDump::List(items.into_iter().map(|(a, _, _)| a).collect()),
                    KeyKind::Set => {
                        let members = items.into_iter().map(|(a, _, _)| a).collect();
                        let (members, enc) = crate::store::set_order(members, row_enc);
                        KeyDump::Set(members, enc)
                    }
                    KeyKind::Zset => {
                        let names: Vec<&[u8]> =
                            items.iter().map(|(a, _, _)| a.as_slice()).collect();
                        let enc = crate::store::natural_enc(KeyKind::Zset, row_enc, &names);
                        KeyDump::Zset(
                            items
                                .into_iter()
                                .map(|(a, s, _)| (a, s.unwrap_or(0.0)))
                                .collect(),
                            enc,
                        )
                    }
                    _ => {
                        let pairs: Vec<(Vec<u8>, Vec<u8>)> = items
                            .into_iter()
                            .map(|(a, _, v)| (a, v.unwrap_or_default()))
                            .collect();
                        let flat: Vec<&[u8]> = pairs
                            .iter()
                            .flat_map(|(f, v)| [f.as_slice(), v.as_slice()])
                            .collect();
                        let enc = crate::store::natural_enc(KeyKind::Hash, row_enc, &flat);
                        KeyDump::Hash(pairs, enc)
                    }
                }
            }
        };
        let expires_at = client
            .select_kept(&sql::EXPIRE_US[idx], &[key.into()])
            .ok()
            .and_then(|t| t.first().get::<i64>(1).ok().flatten())
            .filter(|us| *us > 0)
            .unwrap_or(0);
        Some((dump, expires_at))
    }

    /// Write back what `dump_key` read, into whichever backend `db` names.
    pub(crate) fn restore_key(client: &mut SpiClient<'_>, db: u8, key: &[u8], dump: &KeyDump) {
        Self::restore_key_at(client, db, key, dump, crate::storage::slot(db));
    }

    /// `restore_key` into a named backend.
    pub(crate) fn restore_key_at(
        client: &mut SpiClient<'_>,
        db: u8,
        key: &[u8],
        dump: &KeyDump,
        mem_slot: Option<usize>,
    ) {
        if let Some(slot) = mem_slot {
            let _ = crate::shm::with_db(slot, |m| m.restore(key, dump, 0));
            return;
        }
        match dump {
            KeyDump::Stream(v) => {
                let sql = format!(
                    "INSERT INTO redis.stream_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
                );
                let _ = client.update_kept(&sql, &[key.into(), v.clone().into()]);
            }
            KeyDump::String(v) => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
                );
                let _ = client.update_kept(&sql, &[key.into(), v.clone().into()]);
            }
            KeyDump::List(items) => {
                let sql = format!(
                    "INSERT INTO redis.list_{db} (key, pos, value) \
                     SELECT $1, i - 1, v FROM unnest($2::bytea[]) WITH ORDINALITY AS t(v, i)"
                );
                let vals: Vec<Option<Vec<u8>>> = items.iter().map(|v| Some(v.clone())).collect();
                let _ = client.update_kept(&sql, &[key.into(), vals.into()]);
            }
            // Members and fields take `seq` in the order the dump has them,
            // and the encoding row is written to match the dump's encoding.
            KeyDump::Set(items, enc) => {
                let has_int = items
                    .iter()
                    .any(|m| crate::memdb::parse_stored_int(m).is_some());
                let row =
                    crate::store::enc_row(crate::memdb::KeyKind::Set, *enc, has_int).map(Enc::code);
                let sql = format!(
                    "WITH ins AS (INSERT INTO redis.set_{db} (key, member) \
                                  SELECT $1, m FROM unnest($2::bytea[]) WITH ORDINALITY AS t(m, i) \
                                  ORDER BY i ON CONFLICT DO NOTHING), \
                          {} \
                     SELECT 1",
                    crate::store::enc_write_ctes(db, 1, 3)
                );
                let vals: Vec<Option<Vec<u8>>> = items.iter().map(|v| Some(v.clone())).collect();
                let _ = client.update_kept(&sql, &[key.into(), vals.into(), row.into()]);
            }
            KeyDump::Hash(pairs, enc) => {
                let row =
                    crate::store::enc_row(crate::memdb::KeyKind::Hash, *enc, false).map(Enc::code);
                let sql = format!(
                    "WITH ins AS (INSERT INTO redis.hash_{db} (key, field, value) \
                                  SELECT $1, f, v FROM unnest($2::bytea[], $3::bytea[]) \
                                         WITH ORDINALITY AS t(f, v, i) \
                                  ORDER BY i \
                                  ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value), \
                          {} \
                     SELECT 1",
                    crate::store::enc_write_ctes(db, 1, 4)
                );
                let fs: Vec<Option<Vec<u8>>> = pairs.iter().map(|(f, _)| Some(f.clone())).collect();
                let vs: Vec<Option<Vec<u8>>> = pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                let _ = client.update_kept(&sql, &[key.into(), fs.into(), vs.into(), row.into()]);
            }
            KeyDump::Zset(pairs, enc) => {
                let row =
                    crate::store::enc_row(crate::memdb::KeyKind::Zset, *enc, false).map(Enc::code);
                let sql = format!(
                    "WITH ins AS (INSERT INTO redis.zset_{db} (key, member, score) \
                                  SELECT $1, m, s FROM unnest($2::bytea[], $3::float8[]) AS t(m, s) \
                                  ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score), \
                          {} \
                     SELECT 1",
                    crate::store::enc_write_ctes(db, 1, 4)
                );
                let ms: Vec<Option<Vec<u8>>> = pairs.iter().map(|(m, _)| Some(m.clone())).collect();
                let ss: Vec<Option<f64>> = pairs.iter().map(|(_, s)| Some(*s)).collect();
                let _ = client.update_kept(&sql, &[key.into(), ms.into(), ss.into(), row.into()]);
            }
        }
    }

    /// Move a string from `key` to `newkey`, clearing the destination first.
    /// `RENAME` answers `+OK`; `RENAMENX` turns that into `1` once it has
    /// established the destination was free.
    pub(crate) fn rename_to(
        client: &mut SpiClient<'_>,
        db: u8,
        key: &[u8],
        newkey: &[u8],
    ) -> Response {
        let kind = Self::sql_key_kinds(client, db, &[key])
            .first()
            .and_then(|(_, k)| *k);
        let Some(kind) = kind else {
            return Response::Error("ERR no such key".to_string());
        };
        if key == newkey {
            return Response::Ok;
        }
        // Whatever the destination held goes, whatever its type.
        Self::sql_drop_key(client, db, newkey);
        if kind != crate::memdb::KeyKind::String {
            // A collection or stream moves as a dump and a restore; its expiry
            // row in `expiry_N` moves with it.
            let Some((dump, _)) = Self::dump_key(client, db, key) else {
                return Response::Error("ERR no such key".to_string());
            };
            let expiry = format!(
                "WITH old AS (DELETE FROM redis.expiry_{db} WHERE key = $1 RETURNING expires_at) \
                 INSERT INTO redis.expiry_{db} (key, expires_at) SELECT $2, expires_at FROM old"
            );
            if let Err(e) = client.update_kept(&expiry, &[key.into(), newkey.into()]) {
                return spi_failed("RENAME", e);
            }
            Self::sql_drop_key_data(client, db, key);
            Self::restore_key(client, db, newkey, &dump);
            return Response::Ok;
        }
        Self::rename_string(client, db, key, newkey)
    }

    /// RENAME of a string: one `UPDATE` of its `kv_N` row.
    fn rename_string(client: &mut SpiClient<'_>, db: u8, key: &[u8], newkey: &[u8]) -> Response {
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
            if let Err(e) = client.update_kept(&del, &[newkey.into(), key.into()]) {
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
        match client.update_kept(&sql, &[key.into(), newkey.into()]) {
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
    pub(crate) fn sql_key_kinds(
        client: &mut SpiClient<'_>,
        db: u8,
        keys: &[&[u8]],
    ) -> Vec<(Vec<u8>, Option<crate::memdb::KeyKind>)> {
        Self::key_kinds(client, db, keys, false)
    }

    /// `sql_key_kinds`, with a string past its deadline reported as `None`
    /// too rather than left out: what a read that missed needs to know, to
    /// reap only the keys that are there to reap.
    fn sql_missed_kinds(
        client: &mut SpiClient<'_>,
        db: u8,
        keys: &[&[u8]],
    ) -> Vec<(Vec<u8>, Option<crate::memdb::KeyKind>)> {
        Self::key_kinds(client, db, keys, true)
    }

    fn key_kinds(
        client: &mut SpiClient<'_>,
        db: u8,
        keys: &[&[u8]],
        expired_strings: bool,
    ) -> Vec<(Vec<u8>, Option<crate::memdb::KeyKind>)> {
        let sql = format!(
            "SELECT a.key, {} AS kind FROM unnest($1::bytea[]) AS a(key)",
            sql::kind_case(db as usize, "a.key", expired_strings)
        );
        let args: Vec<Option<Vec<u8>>> = keys.iter().map(|k| Some(k.to_vec())).collect();
        let Ok(tbl) = client.select_kept(&sql, &[args.into()]) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for row in tbl {
            let (Ok(Some(key)), Ok(Some(kind))) = (row.get::<Vec<u8>>(1), row.get::<String>(2))
            else {
                continue;
            };
            let Some(kind) = Self::parse_kind(&kind) else {
                continue;
            };
            out.push((key, kind));
        }
        out
    }

    /// A `sql::kind_case` answer: `Some(None)` for `expired`, `None` for
    /// anything it does not name.
    fn parse_kind(kind: &str) -> Option<Option<crate::memdb::KeyKind>> {
        use crate::memdb::KeyKind;
        Some(match kind {
            "string" => Some(KeyKind::String),
            "list" => Some(KeyKind::List),
            "set" => Some(KeyKind::Set),
            "hash" => Some(KeyKind::Hash),
            "zset" => Some(KeyKind::Zset),
            "stream" => Some(KeyKind::Stream),
            "expired" => None,
            _ => return None,
        })
    }

    /// Delete a key's data from every table but `expiry_N` — for a RENAME, which
    /// has already moved the expiry row to the new name.
    fn sql_drop_key_data(client: &mut SpiClient<'_>, db: u8, key: &[u8]) {
        let tables: Vec<String> = std::iter::once("kv")
            .chain(sql::COLLECTION_TABLES)
            .chain(["encoding"])
            .enumerate()
            .map(|(i, t)| format!("d{i} AS (DELETE FROM redis.{t}_{db} WHERE key = $1)"))
            .collect();
        let sql = format!("WITH {} SELECT 1", tables.join(", "));
        let _ = client.update_kept(&sql, &[key.into()]);
    }

    /// Delete everything every table holds for `key`. What `SET` does to a key
    /// that was a list: Redis replaces the value rather than refusing.
    pub(crate) fn sql_drop_key(client: &mut SpiClient<'_>, db: u8, key: &[u8]) {
        let sql = format!(
            "WITH d1 AS (DELETE FROM redis.kv_{db}     WHERE key = $1), \
                  d2 AS (DELETE FROM redis.hash_{db}   WHERE key = $1), \
                  d3 AS (DELETE FROM redis.list_{db}   WHERE key = $1), \
                  d4 AS (DELETE FROM redis.set_{db}    WHERE key = $1), \
                  d5 AS (DELETE FROM redis.zset_{db}   WHERE key = $1), \
                  d6 AS (DELETE FROM redis.expiry_{db} WHERE key = $1), \
                  d7 AS (DELETE FROM redis.stream_{db} WHERE key = $1), \
                  d8 AS (DELETE FROM redis.encoding_{db} WHERE key = $1) \
             SELECT 1"
        );
        let _ = client.update_kept(&sql, &[key.into()]);
    }

    /// The durable half's `mem_type_error`: refuse a command whose key holds
    /// another type, and clear a key the command is about to replace.
    fn sql_type_error(&self, client: &mut SpiClient<'_>, db: u8) -> Option<Response> {
        self.sql_probe_types(client, db).err()
    }

    /// `sql_type_error`'s work, and whether it probed: one statement reaps
    /// whichever of the command's keys have passed their deadline — the keys
    /// it types, replaces, and the string writes `lazily_expires` names — and
    /// reports what the others hold. A reaped key reads as absent, and is
    /// reported as Redis reports an expiry it notices on access.
    fn sql_probe_types(&self, client: &mut SpiClient<'_>, db: u8) -> Result<bool, Response> {
        let typed = self.typed_keys();
        if typed.is_empty() {
            // Nothing to refuse, so nothing to look up. The commands that only
            // replace a key — `SET` and its kin — clear what was there from
            // inside their own statement rather than paying a round trip to
            // find out there was nothing to clear, which is the common case and
            // halved their throughput when they did.
            return Ok(false);
        }
        let overwrites = self.overwritten_keys();
        let stale = self.lazily_expires();
        let mut lookup: Vec<&[u8]> = typed
            .iter()
            .map(|(k, _)| *k)
            .chain(overwrites.iter().map(|(k, _)| *k))
            .chain(stale.iter().copied())
            .collect();
        lookup.sort_unstable();
        lookup.dedup();
        // Only a command that reaps anyway — a string write building on the
        // value it replaces — merges the reap into the probe. For the rest the
        // read-only probe is cheaper: a statement with DELETEs in it costs
        // more to run when there is nothing to delete, which is nearly always,
        // and makes the transaction a writing one.
        if stale.is_empty() {
            return self
                .sql_type_error_read_only(client, db, &typed, &overwrites, &lookup)
                .map(|_| false);
        }
        let (kinds, reaped) = Self::sql_reap_and_kinds(client, db, &lookup);
        if !reaped.is_empty() {
            crate::server::count_expired(reaped.len() as u64);
            crate::tracking::invalidate(&reaped, 0);
            let flags = crate::notify::flags();
            for key in &reaped {
                crate::notify::emit(flags, crate::notify::EXPIRED, "expired", key, db);
            }
        }

        let kind_of = |key: &[u8]| {
            kinds
                .iter()
                .find(|(k, _)| k.as_slice() == key)
                .map(|(_, kind)| *kind)
        };
        for (key, accepted) in &typed {
            if let Some(kind) = kind_of(key)
                && !accepted.contains(&kind)
            {
                return Err(wrong_type());
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
                    let _ = client.update_kept(&sql, &[(*key).into()]);
                }
                None => {}
            }
        }
        Ok(true)
    }

    /// The probe without the reap: what each key holds, read only, and a drop
    /// for a collection the probe finds past its deadline.
    fn sql_type_error_read_only(
        &self,
        client: &mut SpiClient<'_>,
        db: u8,
        typed: &[(&[u8], &'static [crate::memdb::KeyKind])],
        overwrites: &[(&[u8], crate::memdb::KeyKind)],
        lookup: &[&[u8]],
    ) -> Result<(), Response> {
        let kinds = Self::sql_key_kinds(client, db, lookup);
        // Reap before deciding anything. A key past its expiry is one the
        // sweep has not reached yet: `TYPE` and `EXISTS` already call it gone,
        // and its rows are still in the collection tables, which is where
        // `LLEN`, `SMEMBERS`, `HGETALL` and the rest read from.
        for (key, kind) in &kinds {
            if kind.is_none() {
                Self::sql_drop_key(client, db, key);
            }
        }
        let kind_of = |key: &[u8]| {
            kinds
                .iter()
                .find(|(k, _)| k.as_slice() == key)
                .and_then(|(_, kind)| *kind)
        };
        for (key, accepted) in typed {
            if let Some(kind) = kind_of(key)
                && !accepted.contains(&kind)
            {
                return Err(wrong_type());
            }
        }
        for (key, becomes) in overwrites {
            match kind_of(key) {
                Some(kind) if kind != *becomes => Self::sql_drop_key(client, db, key),
                // Replacing a collection with one of its own type keeps the
                // rows the command is about to rewrite, but not the expiry:
                // Redis discards a key's TTL whenever its value is replaced.
                Some(_) => {
                    let sql = format!("DELETE FROM redis.expiry_{db} WHERE key = $1");
                    let _ = client.update_kept(&sql, &[(*key).into()]);
                }
                None => {}
            }
        }
        Ok(())
    }

    /// In one statement: delete whichever of `keys` have passed their
    /// deadline, and the type each of the rest holds. The deletes and the
    /// classification see the same snapshot, so a key the statement reaps is
    /// classified from rows it is removing; it is left out of the kinds and
    /// returned among the reaped instead.
    fn sql_reap_and_kinds(client: &mut SpiClient<'_>, db: u8, keys: &[&[u8]]) -> ReapedKinds {
        use crate::memdb::KeyKind;
        let drops = sql::COLLECTION_TABLES
            .iter()
            .enumerate()
            .map(|(i, t)| {
                format!(
                    "d{i} AS (DELETE FROM redis.{t}_{db} t WHERE t.key IN (SELECT key FROM dead))"
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "WITH dead AS (DELETE FROM redis.expiry_{db} \
                            WHERE key = ANY($1::bytea[]) AND expires_at <= now() RETURNING key), \
                  {drops}, \
                  denc AS (DELETE FROM redis.encoding_{db} t WHERE t.key IN (SELECT key FROM dead)), \
                  s AS (DELETE FROM redis.kv_{db} \
                         WHERE key = ANY($1::bytea[]) AND expires_at IS NOT NULL \
                           AND expires_at <= now() RETURNING key) \
             SELECT a.key, \
                    CASE \
                      WHEN EXISTS (SELECT 1 FROM redis.kv_{db} t WHERE t.key = a.key \
                                     AND (t.expires_at IS NULL OR t.expires_at > now())) THEN 'string' \
                      WHEN EXISTS (SELECT 1 FROM redis.expiry_{db} e WHERE e.key = a.key \
                                     AND e.expires_at <= now()) THEN NULL \
                      WHEN EXISTS (SELECT 1 FROM redis.list_{db} t WHERE t.key = a.key) THEN 'list' \
                      WHEN EXISTS (SELECT 1 FROM redis.set_{db}  t WHERE t.key = a.key) THEN 'set' \
                      WHEN EXISTS (SELECT 1 FROM redis.hash_{db} t WHERE t.key = a.key) THEN 'hash' \
                      WHEN EXISTS (SELECT 1 FROM redis.zset_{db} t WHERE t.key = a.key) THEN 'zset' \
                      WHEN EXISTS (SELECT 1 FROM redis.stream_{db} t WHERE t.key = a.key) THEN 'stream' \
                    END AS kind, \
                    (EXISTS (SELECT 1 FROM dead WHERE dead.key = a.key) \
                       OR EXISTS (SELECT 1 FROM s WHERE s.key = a.key)) AS reaped \
             FROM unnest($1::bytea[]) AS a(key)"
        );
        let args: Vec<Option<Vec<u8>>> = keys.iter().map(|k| Some(k.to_vec())).collect();
        let Ok(tbl) = client.update_kept(&sql, &[args.into()]) else {
            return (Vec::new(), Vec::new());
        };
        let (mut kinds, mut reaped) = (Vec::new(), Vec::new());
        for row in tbl {
            let Ok(Some(key)) = row.get::<Vec<u8>>(1) else {
                continue;
            };
            if row.get::<bool>(3).ok().flatten().unwrap_or(false) {
                reaped.push(key);
                continue;
            }
            let kind = match row.get::<String>(2).ok().flatten().as_deref() {
                Some("string") => KeyKind::String,
                Some("list") => KeyKind::List,
                Some("set") => KeyKind::Set,
                Some("hash") => KeyKind::Hash,
                Some("zset") => KeyKind::Zset,
                Some("stream") => KeyKind::Stream,
                _ => continue,
            };
            kinds.push((key, kind));
        }
        (kinds, reaped)
    }

    /// Commands the shared-memory backend cannot serve alone, even for a
    /// memory-half database: `FLUSHALL` reaches both halves, `COPY` may, the
    /// routed-table INSERT reaches user tables, `DUMP`, `RESTORE` and
    /// `MEMORY USAGE` share `COPY`'s whole-key machinery, and a composite may
    /// touch any database. They run on the SQL path, which branches per
    /// backend inside.
    pub fn needs_sql(&self) -> bool {
        matches!(
            self,
            Command::TablePublish { .. }
                | Command::FlushAll { .. }
                | Command::Copy { .. }
                | Command::Dump { .. }
                | Command::MemoryUsage { .. }
                | Command::Restore { .. }
                | Command::Composite(_)
                | Command::Script(_)
                | Command::Admin(_)
                | Command::Publish { .. }
                | Command::PubSubChannels { .. }
                | Command::PubSubNumSub { .. }
                | Command::PubSubNumPat
                | Command::Block(_)
                | Command::BlockTry { .. }
        )
    }

    /// Run a sub-command on the backend that serves `db`.
    /// An admin command reaching the dispatcher, from inside `EXEC` or a
    /// script, run as the connection would run it.
    fn admin(client: &mut SpiClient<'_>, db: u8, a: &crate::admin::Admin) -> Response {
        let origin = crate::tracking::origin();
        let handle = crate::server::ClientHandle::detached(origin);
        let mut conn = crate::admin::Conn {
            client: &handle,
            db,
            reply: crate::admin::ReplyMode::On,
            session: crate::acl::session_of(origin),
            may_wait: false,
        };
        crate::admin::execute(a, &mut conn, &mut |cmd, d| Self::route(client, d, cmd))
    }

    fn route(client: &mut SpiClient<'_>, db: u8, cmd: Command) -> Response {
        if uses_shared_memory(db) && !cmd.needs_sql() {
            cmd.execute_mem(db)
        } else {
            cmd.execute(client, db)
        }
    }

    fn execute_inner(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
        if let Some(r) = crate::store::run(self, &mut crate::store::SqlStore { client, db }) {
            return r;
        }
        match self {
            // Written once, in `store.rs`, and answered above.
            Command::HGet { .. }
            | Command::HMGet { .. }
            | Command::HGetAll { .. }
            | Command::HKeys { .. }
            | Command::HVals { .. }
            | Command::HLen { .. }
            | Command::HExists { .. }
            | Command::HStrLen { .. }
            | Command::HSet { .. }
            | Command::HMSet { .. }
            | Command::HSetNx { .. }
            | Command::HDel { .. }
            | Command::HIncrBy { .. }
            | Command::HIncrByFloat { .. }
            | Command::HRandField { .. }
            | Command::SAdd { .. }
            | Command::SRem { .. }
            | Command::SIsMember { .. }
            | Command::SMisMember { .. }
            | Command::SMembers { .. }
            | Command::SCard { .. }
            | Command::SPop { .. }
            | Command::SRandMember { .. }
            | Command::SMove { .. }
            | Command::SUnion { .. }
            | Command::SInter { .. }
            | Command::SDiff { .. }
            | Command::SUnionStore { .. }
            | Command::SInterStore { .. }
            | Command::SDiffStore { .. }
            | Command::SInterCard { .. }
            | Command::ZAdd { .. }
            | Command::ZIncrBy { .. }
            | Command::ZRem { .. }
            | Command::ZScore { .. }
            | Command::ZMScore { .. }
            | Command::ZCard { .. }
            | Command::ZCount { .. }
            | Command::ZLexCount { .. }
            | Command::ZRank { .. }
            | Command::ZRange { .. }
            | Command::ZRangeByScore { .. }
            | Command::ZRangeByLex { .. }
            | Command::ZPopMin { .. }
            | Command::ZPopMax { .. }
            | Command::ZRandMember { .. }
            | Command::ZRemRangeByRank { .. }
            | Command::ZRemRangeByScore { .. }
            | Command::ZRemRangeByLex { .. }
            | Command::LPush { .. }
            | Command::RPush { .. }
            | Command::LPushX { .. }
            | Command::RPushX { .. }
            | Command::LPop { .. }
            | Command::RPop { .. }
            | Command::LLen { .. }
            | Command::LRange { .. }
            | Command::LIndex { .. }
            | Command::LSet { .. }
            | Command::LInsert { .. }
            | Command::LRem { .. }
            | Command::LTrim { .. }
            | Command::LMove { .. }
            | Command::LPos { .. }
            | Command::Get { .. }
            | Command::Set { .. }
            | Command::SetEx { .. }
            | Command::PSetEx { .. }
            | Command::SetNx { .. }
            | Command::GetSet { .. }
            | Command::GetDel { .. }
            | Command::GetEx { .. }
            | Command::MGet { .. }
            | Command::MSet { .. }
            | Command::MSetNx { .. }
            | Command::Append { .. }
            | Command::Strlen { .. }
            | Command::GetRange { .. }
            | Command::SetRange { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::IncrBy { .. }
            | Command::DecrBy { .. }
            | Command::IncrByFloat { .. }
            | Command::GetBit { .. }
            | Command::BitCount { .. }
            | Command::SetBit { .. }
            | Command::Del { .. }
            | Command::Unlink { .. }
            | Command::Exists { .. }
            | Command::Touch { .. }
            | Command::Type { .. }
            | Command::Ttl { .. }
            | Command::PTtl { .. }
            | Command::ExpireTime { .. }
            | Command::PExpireTime { .. }
            | Command::Expire { .. }
            | Command::PExpire { .. }
            | Command::ExpireAt { .. }
            | Command::PExpireAt { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. }
            | Command::Keys { .. }
            | Command::Scan { .. }
            | Command::CollectionScan { .. }
            | Command::DbSize
            | Command::Keyspace
            | Command::RandomKey
            | Command::FlushDb { .. }
            | Command::ObjectEncoding { .. }
            | Command::ZsetToSkiplist { .. }
            | Command::ObjectAccess { .. }
            | Command::ObjectOther(_) => {
                Response::Error("ERR internal error: command not routed to store".to_string())
            }
            // Two databases in tables swap their tables rather than their
            // keys: Redis's SWAPDB moves no key either.
            Command::Composite(op @ crate::composite::Op::SwapDb { a, b })
                if *a != *b
                    && [*a, *b].iter().all(|&d| {
                        crate::storage::slot(d).is_none() && !crate::storage::migrating(d)
                    }) =>
            {
                match client.update(&sql::swap_tables(*a as usize, *b as usize), None, &[]) {
                    Ok(_) => {
                        // A WATCH on a key either database held is broken, as by
                        // a flush; one on a missing key only if the swap brought
                        // it in (`watch_broken`).
                        for d in [*a, *b] {
                            crate::server::bump_flush_epoch(d);
                            crate::server::bump_swap_epoch(d);
                        }
                        crate::dirty::force(crate::dirty::of_op(op, &Response::Ok));
                        Response::Ok
                    }
                    Err(e) => spi_failed("SWAPDB", e),
                }
            }
            // Each sub-command goes to the backend that serves its database.
            Command::Composite(op) => {
                op.run(db, &mut |d, cmd: Command| Self::route(client, d, cmd))
            }
            Command::Block(op) => {
                op.run_once(db, &mut |d, cmd: Command| Self::route(client, d, cmd))
            }
            Command::Script(sc) => crate::scripting::execute(sc, db, &mut SqlHost { client }),
            Command::BlockTry { op, first } => op
                .attempt(
                    db,
                    &mut |d, cmd: Command| Self::route(client, d, cmd),
                    *first,
                )
                .unwrap_or(Response::NullArray),
            Command::Ping { msg } => Response::Pong(msg.clone()),
            Command::Echo { msg } => Response::BulkString(msg.clone()),
            Command::Select { .. } => Response::Ok,
            // A queued AUTH the connection checked when EXEC reached it: the
            // user changes here, in its turn, so a command queued before it
            // still ran as the old one.
            Command::Auth { user, password } => {
                let name = user.as_deref().unwrap_or(b"default");
                match (
                    crate::acl::user_serial(name, password),
                    crate::server::slot_of(crate::tracking::origin()),
                ) {
                    (Some(serial), Some(slot)) => {
                        slot.user_serial
                            .store(serial, std::sync::atomic::Ordering::Relaxed);
                        Response::Ok
                    }
                    (Some(_), None) => Response::Ok,
                    (None, _) => Response::Error(crate::acl::WRONGPASS.into()),
                }
            }
            Command::Stream(c) => c.run(&mut SqlStreams { client, db }),
            Command::StreamRead { cmd, resolved } => {
                stream_read(cmd, resolved, &mut SqlStreams { client, db })
            }
            Command::Time => time_reply(),
            Command::Hello { .. } => Response::Ok,
            Command::Reset => Response::Ok,
            Command::Quit => Response::Ok,
            // Answered on the connection's thread, never dispatched.
            // An admin command reaches the dispatcher only from inside EXEC:
            // run it here, as a script's admin sub-commands are run.
            // Snapshot first, then answer as any admin command. Not through
            // `execute_inner` again: with nothing to snapshot `saved` stays
            // false, and this arm would match itself until the stack ran out.
            Command::Admin(a) if a.name == "SAVE" && !a.saved => {
                let saved = crate::storage::save_now(client);
                Self::admin(client, db, &crate::admin::Admin { saved, ..a.clone() })
            }
            Command::Admin(a) => Self::admin(client, db, a),
            Command::Monitor => Response::Error("ERR connection command dispatched".to_string()),
            // Pub/sub reaches the dispatcher only from inside EXEC.
            Command::Publish { channel, message } => {
                let cmd = Command::Publish {
                    channel: channel.clone(),
                    message: message.clone(),
                };
                let (reply, route) = crate::pubsub::shared::run(cmd).expect("a pub/sub command");
                if let Some(insert) = route {
                    Self::route(client, db, insert);
                }
                reply
            }
            Command::PubSubChannels { pattern, shard } => {
                crate::pubsub::shared::run(Command::PubSubChannels {
                    pattern: pattern.clone(),
                    shard: *shard,
                })
                .expect("a pub/sub command")
                .0
            }
            Command::PubSubNumSub { channels, shard } => {
                crate::pubsub::shared::run(Command::PubSubNumSub {
                    channels: channels.clone(),
                    shard: *shard,
                })
                .expect("a pub/sub command")
                .0
            }
            Command::PubSubNumPat => {
                crate::pubsub::shared::run(Command::PubSubNumPat)
                    .expect("a pub/sub command")
                    .0
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
                if !crate::evict::make_room() {
                    return mem_out_of_memory();
                }
                // Liveness, not row-presence: `sql_key_kinds` reports a key
                // past its expiry as a row with no kind, and a dead key must
                // not refuse the copy.
                if crate::storage::kind(client, dst_db, dst).is_some() && !*replace {
                    return Response::Integer(0);
                }
                let Some((dump, deadline_us)) = crate::storage::dump(client, db, src) else {
                    return Response::Integer(0);
                };
                // Cleared whether or not the destination read as live: a key
                // past its expiry that the sweep has not reached still has
                // rows, and the write below must not merge with them.
                crate::storage::clear(client, dst_db, dst);
                crate::storage::restore(client, dst_db, dst, &dump, deadline_us);
                // A copy is a new object: a fresh access field.
                crate::access::restored(dst_db, dst, None, None);
                crate::dirty::add(1);
                Response::Integer(1)
            }

            Command::Dump { key } => match Self::dump_key(client, db, key) {
                Some((dump, _)) => Response::BulkString(crate::rdb::serialize(&dump)),
                None => Response::Null,
            },

            // An estimate, not an allocator measurement: neither backend can
            // say what one key costs. The serialized payload tracks the data
            // the key holds, and the constant stands in for the per-key
            // bookkeeping Redis would count — enough for what clients use the
            // number for, which is comparing keys and spotting big ones.
            Command::MemoryUsage { key } => match Self::dump_key(client, db, key) {
                Some((dump, _)) => Response::Integer(
                    48 + key.len() as i64 + crate::rdb::serialize(&dump).len() as i64,
                ),
                None => Response::Null,
            },

            Command::Restore {
                key,
                ttl_ms,
                absttl,
                replace,
                payload,
                idletime,
                freq,
            } => {
                // The checks in Redis's order: the busy key wins over a
                // payload that would also have been refused.
                if crate::storage::kind(client, db, key).is_some() && !*replace {
                    return Response::Error("BUSYKEY Target key name already exists.".to_string());
                }
                // `denyoom`, for a key bound for the memory engine.
                if !crate::evict::make_room() {
                    return mem_out_of_memory();
                }
                let dump = match crate::rdb::deserialize(payload) {
                    Ok(d) => d,
                    Err(crate::rdb::RestoreError::Envelope) => {
                        return Response::Error(
                            "ERR DUMP payload version or checksum are wrong".to_string(),
                        );
                    }
                    Err(crate::rdb::RestoreError::Format) => {
                        return Response::Error("ERR Bad data format".to_string());
                    }
                };
                // Cleared whether or not the key read as live: a key past its
                // expiry that the sweep has not reached still has rows, and
                // the write below must not merge with them.
                crate::storage::clear(client, db, key);
                let deadline_us = match (*ttl_ms > 0, *absttl) {
                    (false, _) => 0,
                    (true, true) => deadline_us(0, *ttl_ms, 1000),
                    (true, false) => deadline_us(now_micros(), *ttl_ms, 1000),
                };
                crate::storage::restore(client, db, key, &dump, deadline_us);
                crate::access::restored(db, key, *idletime, *freq);
                crate::dirty::add(1);
                Response::Ok
            }

            // Every database, the durable half included — Redis's own reach,
            // and the reason the Redis port should not be exposed past
            // loopback without a password.
            Command::FlushAll { .. } => {
                // `flushAllDataAndResetRDB`: every key removed counts, and
                // with `save` set the server then saves, which starts the
                // count again.
                let removed: i64 = (0..NUM_DBS as u8)
                    .map(|d| match Self::route(client, d, Command::DbSize) {
                        Response::Integer(n) => n.max(0),
                        _ => 0,
                    })
                    .sum();
                crate::storage::flush_memory();
                // The databases in tables are truncated, which leaves no dead
                // rows; the tables of one in memory are empty, and a DELETE
                // of nothing costs less than a TRUNCATE's new files.
                let (tables, memory): (Vec<usize>, Vec<usize>) = (0..NUM_DBS).partition(|&d| {
                    crate::storage::slot(d as u8).is_none() || crate::storage::migrating(d as u8)
                });
                let truncated = if tables.is_empty() {
                    Ok(())
                } else {
                    client
                        .update_kept(&sql::truncate_dbs(&tables), &[])
                        .map(|_| ())
                };
                let flushed = truncated.and_then(|()| {
                    client
                        .update_kept(&sql::flush_dbs(&memory), &[])
                        .map(|_| ())
                });
                match flushed {
                    Ok(()) => {
                        crate::dirty::add(removed as u64);
                        if !crate::config::get_str("save").trim().is_empty() {
                            crate::server::mark_saved();
                        }
                        Response::Ok
                    }
                    Err(e) => spi_failed("FLUSHALL", e),
                }
            }

            Command::TablePublish {
                schema,
                table,
                channel,
                payload,
            } => {
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
                match client.update_kept(
                    &sql,
                    &[channel.as_slice().into(), payload.as_slice().into()],
                ) {
                    Ok(_) => Response::Null,
                    Err(e) => {
                        pgrx::warning!("pg_redis: table publish failed: {e}");
                        Response::Null
                    }
                }
            }
            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch
            | Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::SSubscribe { .. }
            | Command::SUnsubscribe { .. }
            | Command::PubSubHelp => {
                Response::Error("ERR command not allowed in this context".to_string())
            }
        }
    }

    /// Execute command via shared-memory path (no SPI, no transaction).
    /// Only valid for a database whose policy keeps it in memory.
    /// Hash/list/set/zset commands fall back to the SPI path.
    pub fn execute_mem(&self, db: u8) -> Response {
        // The slot serving `db` — not its number since databases took
        // policies (`storage.rs`).
        let Some(slot) = crate::storage::slot(db) else {
            return Response::Error(format!(
                "ERR internal error: database {db} is not in memory"
            ));
        };
        // Redis's `performEvictions`, before every command: past
        // `redis.maxmemory`, evict what the policy allows. A write that could
        // grow memory (`denyoom`) is refused if that was not enough.
        if !crate::evict::make_room() && self.may_grow() {
            return mem_out_of_memory();
        }
        let access = crate::access::mode(self, crate::tracking::origin());
        // The type check and the command under one lock, so no other worker
        // can change the key between them. What `store::run` does not know,
        // streams aside, runs after the lock is released: the SQL fallback
        // comes back here for each sub-command of a composite, which the
        // database's server-wide lock, held exclusive, keeps whole
        // (`src/dblock.rs`).
        // Whatever an earlier caller left unreported is not this command's.
        crate::memdb::take_reaped();
        crate::memdb::pause_expiry(crate::server::paused());
        let ran = crate::access::with_mode(access, || {
            crate::shm::with_db(slot, |m| {
                self.run_on_db(m).or_else(|| match self {
                    Command::Stream(c) => Some(c.run(m)),
                    Command::StreamRead { cmd, resolved } => Some(stream_read(cmd, resolved, m)),
                    _ => None,
                })
            })
        });
        // `expireIfNeeded`'s report, as the durable half's `reap_keys` makes it.
        let reaped = crate::memdb::take_reaped();
        if !reaped.is_empty() {
            crate::server::count_expired(reaped.len() as u64);
            crate::tracking::invalidate(&reaped, 0);
            let flags = crate::notify::flags();
            for key in &reaped {
                crate::notify::emit(flags, crate::notify::EXPIRED, "expired", key, db);
            }
        }
        match ran {
            Ok(Some(r)) => r,
            Ok(None) => self.execute_mem_rest(db),
            Err(crate::shm::Fault::OutOfMemory) => mem_out_of_memory(),
            Err(fault) => {
                pgrx::warning!("pg_redis: memory command failed: {fault:?}");
                Response::Error("ERR internal error executing the command".to_string())
            }
        }
    }

    /// The memory half's commands that hold no key of their own.
    fn execute_mem_rest(&self, db: u8) -> Response {
        match self {
            Command::Time => time_reply(),

            // Routed to the SQL path by `run_dispatch_batch`, which is where it
            // can reach both halves. Unreachable here, and answered rather than
            // ignored in case that routing ever changes.
            Command::FlushAll { .. } => {
                crate::storage::flush_memory();
                Response::Ok
            }

            Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch { .. }
            | Command::Unwatch
            | Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. }
            | Command::SSubscribe { .. }
            | Command::SUnsubscribe { .. }
            | Command::Publish { .. }
            | Command::PubSubChannels { .. }
            | Command::PubSubNumSub { .. }
            | Command::PubSubNumPat
            | Command::PubSubHelp => {
                Response::Error("ERR command not allowed in this context".to_string())
            }

            // TablePublish falls back: it always needs SPI regardless of storage
            // mode. So do the connection-level commands, which touch no data.
            //
            // A dispatcher batch that mixes halves calls this from inside its
            // own transaction, where starting another kills the worker
            // ("StartTransactionCommand: unexpected state STARTED"). Join the
            // open one instead, in a subtransaction like every SQL command in
            // a batch, so an error stays a reply.
            _ => {
                use pgrx::bgworkers::BackgroundWorker;
                use pgrx::prelude::*;
                if crate::pg::in_transaction() {
                    crate::worker::in_subtransaction(|| {
                        Spi::connect_mut(|client| self.execute(client, db))
                    })
                } else {
                    BackgroundWorker::transaction(|| {
                        Spi::connect_mut(|client| self.execute(client, db))
                    })
                }
            }
        }
    }

    /// The keys this command touches, each with the types it will accept. A key
    /// holding anything else is a `WRONGTYPE` — which is the whole of Redis's
    /// type discipline, since a key holds exactly one thing.
    ///
    /// Commands that replace a key whatever it held are not here but in
    /// `overwritten_keys`, and the ones that answer nil rather than erroring on
    /// the wrong type — `MGET`, `SETNX`, `MSETNX` — are in neither.
    fn typed_keys(&self) -> Vec<(&[u8], &'static [crate::memdb::KeyKind])> {
        use crate::memdb::KeyKind::{Hash, List, Set, Stream, String as Str, Zset};
        const STREAM: &[crate::memdb::KeyKind] = &[Stream];
        const STRING: &[crate::memdb::KeyKind] = &[Str];
        const HASH: &[crate::memdb::KeyKind] = &[Hash];
        const LIST: &[crate::memdb::KeyKind] = &[List];
        const SET: &[crate::memdb::KeyKind] = &[Set];
        const ZSET: &[crate::memdb::KeyKind] = &[Zset];
        /// `ZUNIONSTORE` and friends read a plain set as a sorted set whose
        /// every score is 1, so a set source is not an error.
        fn one<'a>(
            key: &'a [u8],
            kinds: &'static [crate::memdb::KeyKind],
        ) -> Vec<(&'a [u8], &'static [crate::memdb::KeyKind])> {
            vec![(key, kinds)]
        }
        fn many<'a>(
            keys: &'a [Vec<u8>],
            kinds: &'static [crate::memdb::KeyKind],
        ) -> Vec<(&'a [u8], &'static [crate::memdb::KeyKind])> {
            keys.iter().map(|k| (k.as_slice(), kinds)).collect()
        }

        match self {
            Command::Stream(c) | Command::StreamRead { cmd: c, .. } => {
                c.keys().into_iter().map(|k| (k, STREAM)).collect()
            }
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
            | Command::HStrLen { key, .. }
            | Command::HIncrBy { key, .. }
            | Command::HIncrByFloat { key, .. }
            | Command::HRandField { key, .. }
            | Command::HSetNx { key, .. } => one(key, HASH),
            Command::CollectionScan { key, kind, .. } => one(
                key,
                match kind {
                    crate::memdb::KeyKind::Hash => HASH,
                    crate::memdb::KeyKind::Set => SET,
                    _ => ZSET,
                },
            ),

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

            _ => vec![],
        }
    }

    /// Drop this command's keys if their deadline has passed, before it reads
    /// them. One statement, and in the ordinary case — nothing expired — one
    /// indexed lookup on a table holding only the keys that carry a TTL.
    fn sql_reap_expired(&self, client: &mut SpiClient<'_>, db: u8) {
        let keys: Vec<&[u8]> = self.typed_keys().iter().map(|(k, _)| *k).collect();
        Self::reap_keys(client, db, &keys);
    }

    /// `expireIfNeeded` over `keys`: whichever have passed their deadline
    /// are deleted, and reported as Redis reports an expiry it notices on
    /// access — the `expired` event, INFO's expired_keys, and an
    /// invalidation for whoever tracks them.
    pub(crate) fn reap_keys(client: &mut SpiClient<'_>, db: u8, keys: &[&[u8]]) {
        // CLIENT PAUSE pauses expiry too: the rows stay until it ends.
        if keys.is_empty() || crate::server::paused() {
            return;
        }
        let arg: Vec<Option<Vec<u8>>> = keys.iter().map(|k| Some(k.to_vec())).collect();
        let reaped: Vec<Vec<u8>> = client
            .update_kept(&sql::REAP_EXPIRED[db as usize], &[arg.into()])
            .map(|t| {
                t.filter_map(|r| r.get::<Vec<u8>>(1).ok().flatten())
                    .collect()
            })
            .unwrap_or_default();
        if reaped.is_empty() {
            return;
        }
        crate::server::count_expired(reaped.len() as u64);
        crate::tracking::invalidate(&reaped, 0);
        let flags = crate::notify::flags();
        for key in &reaped {
            crate::notify::emit(flags, crate::notify::EXPIRED, "expired", key, db);
        }
    }

    /// String writes whose statement reads the row it replaces — to append
    /// to it, add to it, test that it is there. A row past its deadline has
    /// to be gone first, or the write builds on a value Redis has already
    /// expired on lookup and leaves the result expired with it. A blind
    /// overwrite replaces the row and its deadline both, so needs nothing.
    fn lazily_expires(&self) -> Vec<&[u8]> {
        match self {
            // `DEL` looks its keys up, expiring them, then deletes what is
            // left: an expired key is not counted as deleted.
            Command::Del { keys } | Command::Unlink { keys } => {
                keys.iter().map(Vec::as_slice).collect()
            }
            Command::Set {
                key,
                nx,
                xx,
                get,
                keepttl,
                ..
            } if *nx || *xx || *get || *keepttl => vec![key.as_slice()],
            Command::SetNx { key, .. }
            | Command::SetRange { key, .. }
            | Command::SetBit { key, .. }
            | Command::GetSet { key, .. }
            | Command::GetDel { key }
            | Command::GetEx { key, .. }
            | Command::Append { key, .. }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. }
            | Command::IncrByFloat { key, .. }
            | Command::Composite(crate::composite::Op::BitField { key, .. }) => {
                vec![key.as_slice()]
            }
            Command::MSetNx { pairs } => pairs.iter().map(|(k, _)| k.as_slice()).collect(),
            _ => Vec::new(),
        }
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
            .any(|(_, kinds)| kinds.iter().any(|k| *k != crate::memdb::KeyKind::String))
    }

    /// The single-key collection reads whose cached statement carries
    /// `sql::live` in its own predicate, listed next to the statements it
    /// names. They skip the up-front reap: the liveness question rides inside
    /// the round trip the read was already making, where a separate reap
    /// statement was measured to cost two thirds of their throughput.
    ///
    /// Misclassifying in either direction is survivable, which is the point
    /// of the design. A command missing from this list pays the reap it does
    /// not need. A command on it whose statement lost the guard is caught
    /// twice over: per statement by
    /// `every_cached_collection_read_carries_the_liveness_guard`, and end to
    /// end by the expired-key e2e tests, which rebuild and re-expire the key
    /// before every read — one honest command's miss-path reap must not
    /// clean the rows and vouch for the next. That masking was observed, not
    /// theorised: an unguarded `HGET` passed the suite until the tests
    /// stopped sharing one expired key.
    fn collection_read_guarded(&self) -> bool {
        matches!(
            self,
            Command::HGet { .. }
                | Command::HGetAll { .. }
                | Command::HKeys { .. }
                | Command::HVals { .. }
                | Command::HLen { .. }
                | Command::HExists { .. }
                | Command::HStrLen { .. }
                | Command::SMembers { .. }
                | Command::SIsMember { .. }
                | Command::SCard { .. }
                | Command::ZCard { .. }
                | Command::ZScore { .. }
                | Command::LLen { .. }
        )
    }

    /// The keys this command replaces outright, with the type each becomes.
    /// Redis lets `SET` land on a key that held a list; the list goes, rather
    /// than the write being refused or the two sitting side by side.
    fn overwritten_keys(&self) -> Vec<(&[u8], crate::memdb::KeyKind)> {
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
            | Command::PSetEx { key, .. } => vec![(key.as_slice(), crate::memdb::KeyKind::String)],
            Command::MSet { pairs } => pairs
                .iter()
                .map(|(k, _)| (k.as_slice(), crate::memdb::KeyKind::String))
                .collect(),
            Command::SUnionStore { dst, .. }
            | Command::SInterStore { dst, .. }
            | Command::SDiffStore { dst, .. } => vec![(dst.as_slice(), crate::memdb::KeyKind::Set)],
            _ => vec![],
        }
    }

    /// A data command against one in-memory database: the type check, then
    /// the shared command code. `None` for what `store::run` does not serve.
    /// What `execute_mem` runs under the database's lock, and what the fuzz
    /// harness runs against a heap-backed `Db`.
    pub(crate) fn run_on_db<A: allocator_api2::alloc::Allocator + Clone>(
        &self,
        db: &mut crate::memdb::Db<A>,
    ) -> Option<Response> {
        self.mem_type_error(db)
            .or_else(|| crate::store::run(self, db))
    }

    /// Refuse a command whose key holds another type: `WRONGTYPE`. Run under
    /// the database's lock, with the command after it.
    fn mem_type_error<A: allocator_api2::alloc::Allocator + Clone>(
        &self,
        db: &mut crate::memdb::Db<A>,
    ) -> Option<Response> {
        self.typed_keys()
            .into_iter()
            .any(|(key, kinds)| db.key_kind(key).is_some_and(|k| !kinds.contains(&k)))
            .then(wrong_type)
    }

    /// Whether the command can need more memory: Redis's `denyoom`, a write
    /// that is not purely a removal. Refused past `redis.maxmemory`.
    pub(crate) fn may_grow(&self) -> bool {
        // FUNCTION LOAD and RESTORE are `denyoom` in Redis: a library is memory.
        if matches!(
            self,
            Command::Script(
                crate::scripting::ScriptCmd::FunctionLoad { .. }
                    | crate::scripting::ScriptCmd::FunctionRestore { .. }
            )
        ) {
            return true;
        }
        !self.write_keys().is_empty()
            && !matches!(
                self,
                Command::Del { .. }
                    | Command::Unlink { .. }
                    | Command::GetDel { .. }
                    | Command::LPop { .. }
                    | Command::RPop { .. }
                    | Command::LRem { .. }
                    | Command::LTrim { .. }
                    | Command::SPop { .. }
                    | Command::SRem { .. }
                    | Command::HDel { .. }
                    | Command::ZRem { .. }
                    | Command::ZPopMin { .. }
                    | Command::ZPopMax { .. }
                    | Command::ZRemRangeByRank { .. }
                    | Command::ZRemRangeByScore { .. }
                    | Command::ZRemRangeByLex { .. }
                    | Command::Expire { .. }
                    | Command::PExpire { .. }
                    | Command::ExpireAt { .. }
                    | Command::PExpireAt { .. }
                    | Command::Persist { .. }
                    | Command::FlushDb { .. }
                    | Command::FlushAll { .. }
            )
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
            Command::Stream(c) | Command::StreamRead { cmd: c, .. } if c.writes() => c.keys(),
            Command::Restore { key, .. } => vec![key.as_slice()],
            Command::LMove { src, dst, .. } => vec![src.as_slice(), dst.as_slice()],
            Command::SMove { src, dst, .. } => vec![src.as_slice(), dst.as_slice()],
            Command::SUnionStore { dst, .. }
            | Command::SInterStore { dst, .. }
            | Command::SDiffStore { dst, .. } => vec![dst.as_slice()],

            _ => vec![],
        }
    }
}

/// Redis's own wording, so a client's existing type handling recognises it.
pub(crate) fn wrong_type() -> Response {
    Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
}

/// Redis's own wording for a write refused because memory is full, so a
/// client's existing OOM handling recognises it.
pub(crate) fn mem_out_of_memory() -> Response {
    Response::Error("OOM command not allowed when used memory > 'maxmemory'.".to_string())
}

fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `TIME`: seconds and leftover microseconds, both spelled as strings. One
/// clock reading, so the two parts cannot straddle a second.
pub(crate) fn time_reply() -> Response {
    let us = now_micros();
    Response::Array(vec![
        Some((us / 1_000_000).to_string().into_bytes()),
        Some((us % 1_000_000).to_string().into_bytes()),
    ])
}

fn now_micros() -> i64 {
    crate::server::now_us()
}

/// Fetch an argument that carries Redis data. Redis keys, values, fields and
/// members are arbitrary byte strings, so these must never round-trip through
/// `String`: that replaces invalid UTF-8 with U+FFFD and cannot represent NUL.
/// Parse an integer that arrived as raw bytes, rejecting non-ASCII input.
pub(crate) fn int_from_bytes(b: &[u8]) -> Option<i64> {
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

/// The `NX`/`XX`/`GT`/`LT` condition on `EXPIRE` and its siblings. A key
/// without a deadline counts as an infinite one, as in Redis: `GT` never
/// applies to it and `LT` always does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpireCond {
    Always,
    Nx,
    Xx,
    Gt,
    Lt,
    /// `XX LT`: a deadline must exist, and the new one be sooner.
    XxLt,
}

impl ExpireCond {
    fn parse(opts: &[Vec<u8>]) -> Result<Self, String> {
        let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
        for opt in opts {
            match String::from_utf8_lossy(opt).to_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GT" => gt = true,
                "LT" => lt = true,
                other => return Err(format!("ERR Unsupported option {other}")),
            }
        }
        if nx && (xx || gt || lt) {
            return Err(
                "ERR NX and XX, GT or LT options at the same time are not compatible".to_string(),
            );
        }
        if gt && lt {
            return Err("ERR GT and LT options at the same time are not compatible".to_string());
        }
        Ok(match (nx, xx, gt, lt) {
            (true, ..) => Self::Nx,
            (_, _, true, _) => Self::Gt,
            (_, true, _, true) => Self::XxLt,
            (_, _, _, true) => Self::Lt,
            (_, true, ..) => Self::Xx,
            _ => Self::Always,
        })
    }

    /// Whether the condition admits the new deadline over `old`, where `None`
    /// is "no deadline". `XX GT` reduces to `GT`, which already requires a
    /// deadline; `XX LT` does not reduce to `LT`, which takes "none" as
    /// infinitely far and so admits it.
    pub(crate) fn allows(self, old: Option<i64>, new: i64) -> bool {
        match (self, old) {
            (Self::Always, _) => true,
            (Self::Nx, old) => old.is_none(),
            (Self::Xx, old) => old.is_some(),
            (Self::Gt, Some(old)) => new > old,
            (Self::Gt, None) => false,
            (Self::Lt, Some(old)) => new < old,
            (Self::Lt, None) => true,
            (Self::XxLt, Some(old)) => new < old,
            (Self::XxLt, None) => false,
        }
    }

    fn allows_none(self) -> bool {
        matches!(self, Self::Always | Self::Nx | Self::Lt)
    }

    /// The same test as SQL over a nullable deadline column.
    fn sql(self, old: &str, new: &str) -> String {
        match self {
            Self::Always => "true".to_string(),
            Self::Nx => format!("{old} IS NULL"),
            Self::Xx => format!("{old} IS NOT NULL"),
            Self::Gt => format!("({old} IS NOT NULL AND {new} > {old})"),
            Self::Lt => format!("({old} IS NULL OR {new} < {old})"),
            Self::XxLt => format!("({old} IS NOT NULL AND {new} < {old})"),
        }
    }
}

/// The count of `SRANDMEMBER`, `ZRANDMEMBER` and `HRANDFIELD`. Redis accepts
/// `-LONG_MAX..=LONG_MAX`, and half that when each pick also carries a score or
/// value, since the reply is then two elements per pick.
fn random_count(arg: &str, paired: bool) -> Result<i64, String> {
    let n: i64 = arg.parse().map_err(|_| NOT_AN_INTEGER.to_string())?;
    let floor = if paired { -(i64::MAX / 2) } else { -i64::MAX };
    if n < floor {
        return Err("value is out of range".to_string());
    }
    Ok(n)
}

/// Redis's default `proto-max-bulk-len`: the largest string `SETRANGE` and
/// `APPEND` may build.
const MAX_STRING_LEN: i64 = 512 * 1024 * 1024;

/// One try of a blocking stream read: the reply, or `StreamWait` with the IDs
/// to retry from.
fn stream_read(
    cmd: &crate::stream::StreamCmd,
    resolved: &Option<Vec<Vec<u8>>>,
    st: &mut dyn crate::stream::StreamStore,
) -> Response {
    match crate::stream::xread(cmd.name == "XREADGROUP", &cmd.args, st, resolved.as_deref()) {
        Ok(crate::stream::ReadOutcome::Reply(r)) => r,
        Ok(crate::stream::ReadOutcome::Wait(ids)) => Response::StreamWait(ids),
        Err(e) => e,
    }
}

/// Streams on the durable half: one row of `stream_N` per stream, its expiry
/// in `expiry_N` like any collection's.
struct SqlStreams<'a, 'b> {
    client: &'a mut SpiClient<'b>,
    db: u8,
}

impl crate::stream::StreamStore for SqlStreams<'_, '_> {
    fn load(&mut self, key: &[u8]) -> Result<Option<crate::stream::Stream>, Response> {
        let db = self.db;
        let sql = format!(
            "SELECT value FROM redis.stream_{db} WHERE key = $1 AND {}",
            sql::live(db as usize)
        );
        let row: Option<Vec<u8>> = match self.client.select_kept(&sql, &[key.into()]) {
            Ok(t) => t.first().get(1).ok().flatten(),
            Err(e) => return Err(spi_failed("stream read", e)),
        };
        match row {
            Some(v) => crate::stream::Stream::decode(&v)
                .map(Some)
                .ok_or_else(|| Response::Error("ERR corrupt stream value".into())),
            None => {
                let kind = Command::sql_key_kinds(self.client, db, &[key])
                    .first()
                    .and_then(|(_, k)| *k);
                match kind {
                    Some(k) if k != crate::memdb::KeyKind::Stream => {
                        Err(crate::stream::not_stream())
                    }
                    _ => Ok(None),
                }
            }
        }
    }

    fn store(&mut self, key: &[u8], s: &crate::stream::Stream) -> Result<(), Response> {
        let db = self.db;
        let sql = format!(
            "INSERT INTO redis.stream_{db} (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        );
        self.client
            .update_kept(&sql, &[key.into(), s.encode().into()])
            .map(|_| ())
            .map_err(|e| spi_failed("stream write", e))
    }
}

/// A memory-half deadline in microseconds: `base_us + value * unit_us`,
/// saturating rather than wrapping. The parser bounds expiries in milliseconds,
/// which leaves room to overflow once scaled to microseconds — wrapping turned
/// a far-future TTL into an immediate expiry. Never 0 either: the memory half
/// reads 0 as "no expiry", so `EXPIREAT k 0` must land in the past, not there.
fn deadline_us(base_us: i64, value: i64, unit_us: i64) -> i64 {
    base_us.saturating_add(value.saturating_mul(unit_us)).max(1)
}

fn invalid_expire(cmd: &str) -> String {
    format!("invalid expire time in '{}' command", cmd.to_lowercase())
}

/// The bytes `GETRANGE` selects. Redis clamps rather than erroring: a negative
/// index counts from the end, a start past the end is empty, and an end past
/// the end is the end.
pub(crate) fn byte_range(value: &[u8], start: i64, end: i64) -> Vec<u8> {
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
pub(crate) fn string_encoding(value: &[u8]) -> &'static str {
    /// `OBJ_ENCODING_EMBSTR_SIZE_LIMIT`.
    const EMBSTR_LIMIT: usize = 44;
    let is_int = std::str::from_utf8(value)
        .ok()
        .is_some_and(|t| crate::memdb::parse_stored_int(t.as_bytes()).is_some());
    match () {
        _ if is_int => "int",
        _ if value.len() <= EMBSTR_LIMIT => "embstr",
        _ => "raw",
    }
}

/// `HRANDFIELD`'s reply, from the hash's fields. No count is one field as a
/// bulk string; a positive count is that many distinct fields; a negative one
/// allows repeats, which is the only way to ask for more than the hash holds.
///
/// `ordered` keeps a random choice in the hash's order, as Redis does for a
/// listpack; a count covering the hash returns all of it in order either way.
pub(crate) fn random_fields(
    pairs: &[(Vec<u8>, Vec<u8>)],
    count: Option<i64>,
    withvalues: bool,
    ordered: bool,
) -> Response {
    let Some(count) = count else {
        return match crate::memdb::random_picks(pairs.len(), 1).first() {
            None => Response::Null,
            Some(&i) => Response::BulkString(pairs[i].0.clone()),
        };
    };
    let push = |out: &mut Vec<Option<Vec<u8>>>, i: usize| {
        out.push(Some(pairs[i].0.clone()));
        if withvalues {
            out.push(Some(pairs[i].1.clone()));
        }
    };
    let mut out = Vec::new();
    let mut picks = if count > 0 && count as u64 >= pairs.len() as u64 {
        (0..pairs.len()).collect()
    } else {
        crate::memdb::random_picks(pairs.len(), count)
    };
    if count > 1 && ordered {
        picks.sort_unstable();
    }
    for i in picks {
        push(&mut out, i);
    }
    Response::Array(out)
}

/// The bit `GETBIT` reads. Redis numbers bits from the *most* significant of
/// byte 0, so bit 0 is `0x80` of the first byte — the opposite of PostgreSQL's
/// `get_bit`, which counts from the least significant.
pub(crate) fn bit_at(value: &[u8], offset: i64) -> bool {
    let byte = (offset / 8) as usize;
    match value.get(byte) {
        None => false,
        Some(b) => b & (0x80 >> (offset % 8)) != 0,
    }
}

/// Set or clear a bit, growing the value with NUL bytes to reach it. Returns
/// the bit as it was, which is what `SETBIT` answers with.
pub(crate) fn set_bit_at(value: &mut Vec<u8>, offset: i64, on: bool) -> bool {
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
pub(crate) fn count_bits(value: &[u8], range: Option<(i64, i64, bool)>) -> i64 {
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

/// The reply for an SPI call that failed. Logged through PostgreSQL rather than
/// stderr, where an operator will find it.
pub(crate) fn spi_failed(cmd: &str, e: impl std::fmt::Display) -> Response {
    pgrx::warning!("pg_redis: {cmd} failed: {e}");
    Response::Error("ERR internal error executing the command".to_string())
}

/// The commands pg_redis implements, by the names `COMMAND COUNT` counts.
pub(crate) static IMPLEMENTED: std::sync::LazyLock<std::collections::HashSet<&'static str>> =
    std::sync::LazyLock::new(|| documented_commands().collect());

/// Redis 7.2's command table, by lowercase name; a subcommand is
/// `container|name`. Generated from Redis's own sources — see
/// `scripts/gen_redis_tables.exs`.
pub static COMMAND_SPECS: std::sync::LazyLock<
    std::collections::HashMap<&'static str, &'static crate::redis_tables::CommandSpec>,
> = std::sync::LazyLock::new(|| {
    crate::redis_tables::COMMANDS
        .iter()
        .map(|c| (c.name, c))
        .collect()
});

/// The scripting engine's view of the dispatcher: commands go to the backend
/// that serves their database, the script cache and the libraries to their
/// tables, and the commit hooks to the transaction the batch runs in.
struct SqlHost<'c, 'conn> {
    client: &'c mut SpiClient<'conn>,
}

impl crate::scripting::Host for SqlHost<'_, '_> {
    fn room_to_write(&mut self) -> bool {
        crate::evict::make_room()
    }

    fn exec(&mut self, db: u8, cmd: Command) -> Response {
        Command::route(self.client, db, cmd)
    }

    fn script_body(&mut self, sha: &str) -> Option<Vec<u8>> {
        let tbl = self
            .client
            .select_kept(
                "SELECT body FROM redis.scripts WHERE sha = $1",
                &[sha.into()],
            )
            .ok()?;
        tbl.first().get::<Vec<u8>>(1).ok().flatten()
    }

    fn save_script(&mut self, sha: &str, body: &[u8]) {
        let added = self
            .client
            .update_kept(
                "INSERT INTO redis.scripts (sha, body) VALUES ($1, $2) \
                 ON CONFLICT (sha) DO NOTHING RETURNING 1",
                &[sha.into(), body.into()],
            )
            .is_ok_and(|t| !t.is_empty());
        if added {
            let count = self
                .client
                .select_kept("SELECT count(*) FROM redis.scripts", &[])
                .ok()
                .and_then(|t| t.first().get::<i64>(1).ok().flatten());
            if let Some(n) = count {
                crate::server::set_cached_scripts(n);
            }
        }
    }

    fn scripts_exist(&mut self, shas: &[String]) -> Vec<bool> {
        let found: std::collections::HashSet<String> = self
            .client
            .select_kept(
                "SELECT sha FROM redis.scripts WHERE sha = ANY($1)",
                &[shas.to_vec().into()],
            )
            .map(|t| {
                t.filter_map(|r| r.get::<String>(1).ok().flatten())
                    .collect()
            })
            .unwrap_or_default();
        shas.iter().map(|s| found.contains(s)).collect()
    }

    fn flush_scripts(&mut self) {
        let _ = self.client.update_kept("DELETE FROM redis.scripts", &[]);
        crate::server::set_cached_scripts(0);
    }

    fn libraries(&mut self) -> Vec<Vec<u8>> {
        self.client
            .select_kept("SELECT code FROM redis.functions ORDER BY name", &[])
            .map(|t| {
                t.filter_map(|r| r.get::<Vec<u8>>(1).ok().flatten())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn save_library(&mut self, name: &str, code: &[u8]) {
        let _ = self.client.update_kept(
            "INSERT INTO redis.functions (name, code) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET code = EXCLUDED.code",
            &[name.into(), code.into()],
        );
    }

    fn delete_library(&mut self, name: &str) {
        let _ = self.client.update_kept(
            "DELETE FROM redis.functions WHERE name = $1",
            &[name.into()],
        );
    }

    fn flush_libraries(&mut self) {
        let _ = self.client.update_kept("DELETE FROM redis.functions", &[]);
    }

    fn on_commit(&mut self, f: fn()) {
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, f);
    }

    fn on_abort(&mut self, f: fn()) {
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, f);
    }
}

/// Commands with subcommands: `CLIENT`, `CONFIG`, `OBJECT`, `MEMORY` ...
pub(crate) static CONTAINERS: std::sync::LazyLock<std::collections::HashSet<&'static str>> =
    std::sync::LazyLock::new(|| {
        crate::redis_tables::COMMANDS
            .iter()
            .filter(|c| !c.container.is_empty())
            .map(|c| c.container)
            .collect()
    });

pub fn arity_ok(arity: i32, argc: usize) -> bool {
    if arity >= 0 {
        argc == arity as usize
    } else {
        argc >= arity.unsigned_abs() as usize
    }
}

/// Redis's arity rule, checked once ahead of every per-command parser, so a
/// stray trailing argument is the error Redis gives rather than silently
/// ignored. Arities come from Redis's own command table: positive is exact,
/// negative a minimum, both counting the name. A container's subcommand has
/// an arity of its own, and an unknown one is Redis's "unknown subcommand".
/// Only commands pg_redis implements are checked — one it does not stays an
/// unknown command, not a wrong number of arguments.
fn arity_error(cmd: &str, parts: &[Vec<u8>]) -> Option<String> {
    if !IMPLEMENTED.contains(cmd) {
        return None;
    }
    let lower = cmd.to_ascii_lowercase();
    let spec = COMMAND_SPECS.get(lower.as_str())?;
    if !arity_ok(spec.arity, parts.len()) {
        return Some(wrong_arity(cmd));
    }
    if CONTAINERS.contains(lower.as_str()) && parts.len() >= 2 {
        let sub = String::from_utf8_lossy(&parts[1]);
        let full = format!("{lower}|{}", sub.to_ascii_lowercase());
        return match COMMAND_SPECS.get(full.as_str()) {
            Some(s) if !arity_ok(s.arity, parts.len()) => Some(wrong_arity(&full)),
            Some(_) => None,
            None => Some(format!(
                "ERR unknown subcommand '{}'. Try {} HELP.",
                sub.chars().take(128).collect::<String>(),
                cmd
            )),
        };
    }
    None
}

/// `HSCAN`, `SSCAN` or `ZSCAN key cursor [MATCH pattern] [COUNT count]`.
fn parse_collection_scan(name: &str, args: &[Vec<u8>]) -> Result<Command, String> {
    use crate::memdb::KeyKind;
    if args.len() < 2 {
        return Err(wrong_arity(name));
    }
    let cursor = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|c| c.parse::<u64>().ok())
        .ok_or_else(|| "ERR invalid cursor".to_string())?;
    let mut pattern = None;
    let mut count = None;
    let mut i = 2;
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err("ERR syntax error".to_string());
        }
        if args[i].eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(args[i + 1].clone());
        } else if args[i].eq_ignore_ascii_case(b"COUNT") {
            let n: i64 = std::str::from_utf8(&args[i + 1])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| NOT_AN_INTEGER.to_string())?;
            if n < 1 {
                return Err("ERR syntax error".to_string());
            }
            count = Some(n);
        } else {
            return Err("ERR syntax error".to_string());
        }
        i += 2;
    }
    Ok(Command::CollectionScan {
        kind: match name {
            "HSCAN" => KeyKind::Hash,
            "SSCAN" => KeyKind::Set,
            _ => KeyKind::Zset,
        },
        key: args[0].clone(),
        cursor,
        count,
        pattern,
    })
}

fn wrong_arity(cmd: &str) -> String {
    format!(
        "wrong number of arguments for '{}' command",
        cmd.to_lowercase()
    )
}

/// The two parse failures Redis names the same way everywhere.
pub(crate) const NOT_AN_INTEGER: &str = "value is not an integer or out of range";
const NOT_A_FLOAT: &str = "value is not a valid float";
/// Adding infinities of opposite sign. Redis refuses rather than storing NaN,
/// which would order unpredictably against every other score.
pub(crate) const NAN_SCORE: &str = "ERR resulting score is not a number (NaN)";

/// `numkeys key [key ...] [LIMIT n]`, as `SINTERCARD` and `ZINTERCARD` both
/// spell their arguments. The LIMIT handling is shared, 0 as "no limit"
/// included; the numkeys refusals are not, because Redis's are not — measured
/// against 7.0.15, `SINTERCARD` answers "numkeys should be greater than 0" to
/// both a non-integer and a non-positive value, where `ZINTERCARD` gives the
/// integer error and the `ZUNION` family's "at least 1 input key" in turn.
fn intercard_args(args: &[Vec<u8>], cmd: &str) -> Result<(Vec<Vec<u8>>, Option<i64>), String> {
    // Arity is -3: numkeys with no key after it is an arity error, and the
    // numkeys messages only come once at least one key is on the line.
    if args.len() < 2 {
        return Err(wrong_arity(cmd));
    }
    let (parse_err, range_err) = match cmd {
        "ZINTERCARD" => (
            NOT_AN_INTEGER,
            "ERR at least 1 input key is needed for 'zintercard' command",
        ),
        _ => (
            "ERR numkeys should be greater than 0",
            "ERR numkeys should be greater than 0",
        ),
    };
    let numkeys: i64 = str_arg(args, 0, cmd)?
        .parse()
        .map_err(|_| parse_err.to_string())?;
    if numkeys <= 0 {
        return Err(range_err.to_string());
    }
    let numkeys = numkeys as usize;
    if args.len() < 1 + numkeys {
        return Err("ERR Number of keys can't be greater than number of args".to_string());
    }
    let keys: Vec<Vec<u8>> = args[1..1 + numkeys].to_vec();
    let limit = match args.get(1 + numkeys) {
        None => None,
        Some(a) if a.eq_ignore_ascii_case(b"LIMIT") => {
            // LIMIT takes exactly one value; a missing or extra one is a
            // syntax error, not an arity one.
            if args.len() != 3 + numkeys {
                return Err("ERR syntax error".to_string());
            }
            let n: i64 = str_arg(args, 2 + numkeys, cmd)?
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
    Ok((keys, limit))
}

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

/// Every live key in `db`, optionally filtered by a Redis glob. Matched here
/// rather than as SQL `LIKE` — bytea has no such operator — which also keeps
/// KEYS, SCAN and PSUBSCRIBE agreeing on what a pattern means.
pub(crate) fn matching_keys(
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
             UNION SELECT key FROM redis.stream_{db} \
         ) c \
          WHERE NOT EXISTS (SELECT 1 FROM redis.expiry_{db} e \
                             WHERE e.key = c.key AND e.expires_at <= now())"
    );
    let mut keys: Vec<Option<Vec<u8>>> = Vec::new();
    match client.select_kept(&sql, &[]) {
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
pub(crate) fn parse_score_value(b: &[u8]) -> Option<f64> {
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
pub(crate) fn parse_score_bound(s: &[u8]) -> Option<ScoreBound> {
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
pub(crate) fn parse_lex_bound(s: &[u8]) -> Option<LexBound> {
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
    // Arity already passed, so an odd or empty score/member list is Redis's
    // syntax error, not an arity one.
    let remaining = &args[i..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Err("ERR syntax error".to_string());
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
    // `zrangeGenericCommand`'s option loop: each of REV, BYSCORE and BYLEX
    // once, LIMIT with both its numbers, anything else a syntax error.
    let (mut by, mut rev) = (None, false);
    let mut limit: Option<(i64, i64)> = None;
    let mut with_scores = false;
    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        let left = args.len() - i - 1;
        match opt.as_str() {
            "WITHSCORES" => with_scores = true,
            "LIMIT" if left >= 2 => {
                let num = |a: &[u8]| {
                    std::str::from_utf8(a)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or_else(|| NOT_AN_INTEGER.to_string())
                };
                limit = Some((num(&args[i + 1])?, num(&args[i + 2])?));
                i += 2;
            }
            "REV" if !rev => rev = true,
            "BYLEX" if by.is_none() => by = Some(RangeBy::Lex),
            "BYSCORE" if by.is_none() => by = Some(RangeBy::Score),
            _ => return Err("ERR syntax error".to_string()),
        }
        i += 1;
    }
    let by = by.unwrap_or(RangeBy::Index);
    // A count of -1 is no limit at all, so it conflicts with nothing.
    if by == RangeBy::Index && limit.is_some_and(|(_, n)| n != -1) {
        return Err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
                .to_string(),
        );
    }
    if by == RangeBy::Lex && with_scores {
        return Err(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX".to_string(),
        );
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
            _ => return Err("ERR syntax error".to_string()),
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
            _ => return Err("ERR syntax error".to_string()),
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

/// Format a score the way Redis does: integer-valued doubles without a
/// decimal point, special values as `inf` / `-inf`, otherwise round-trippable
/// shortest representation.
/// Redis's `d2string`: how a double goes out as text. An integral value that
/// fits a 63-bit integer prints as one; anything else is the shortest digits
/// that read back the same, laid out as fpconv lays them out.
pub(crate) fn format_score(s: f64) -> String {
    if s.is_nan() {
        return "nan".to_string();
    }
    if s.is_infinite() {
        return if s > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    // `d2string` would print "-0", but a stored score of -0 comes back from
    // Redis as "0": zero is zero by the time it is replied.
    if s == 0.0 {
        return "0".to_string();
    }
    let half = (i64::MAX / 2) as f64;
    if (-half..=half).contains(&s) && s == s.trunc() {
        return format!("{}", s as i64);
    }
    fpconv_dtoa(s)
}

/// fpconv's `emit_digits`, over the shortest digits Rust's `{:e}` finds.
pub(crate) fn fpconv_dtoa(d: f64) -> String {
    let sci = format!("{:e}", d.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let nd = digits.len() as i32;
    // `K` is where the decimal point sits relative to the end of the digits.
    let k = exp.parse::<i32>().unwrap_or(0) - (nd - 1);
    let e = (k + nd - 1).abs();
    let neg = d < 0.0;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if k >= 0 && e < nd + 7 {
        out.push_str(digits);
        out.extend(std::iter::repeat_n('0', k as usize));
    } else if k < 0 && (k > -7 || e < 4) {
        let offset = nd + k;
        if offset <= 0 {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', (-offset) as usize));
            out.push_str(digits);
        } else {
            out.push_str(&digits[..offset as usize]);
            out.push('.');
            out.push_str(&digits[offset as usize..]);
        }
    } else {
        let nd = nd.min(18 - i32::from(neg)) as usize;
        out.push_str(&digits[..1]);
        if nd > 1 {
            out.push('.');
            out.push_str(&digits[1..nd]);
        }
        out.push('e');
        out.push(if k + digits.len() as i32 - 1 < 0 {
            '-'
        } else {
            '+'
        });
        if e > 99 {
            out.push_str(&e.to_string());
        } else {
            out.push_str(&format!("{e:02}"));
        }
    }
    out
}

pub(crate) fn score_ge_op(b: &ScoreBound) -> &'static str {
    if b.exclusive { ">" } else { ">=" }
}
pub(crate) fn score_le_op(b: &ScoreBound) -> &'static str {
    if b.exclusive { "<" } else { "<=" }
}

/// Build the lex `AND member ...` where-clause fragment plus the string
/// arguments that must be bound, starting at `$next`. `-` / `+` sentinels
/// do not need a bind value.
pub(crate) fn lex_where(min: &LexBound, max: &LexBound, mut next: usize) -> (String, Vec<Vec<u8>>) {
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

pub(crate) struct ZAddFlags {
    pub(crate) nx: bool,
    pub(crate) xx: bool,
    pub(crate) gt: bool,
    pub(crate) lt: bool,
    pub(crate) ch: bool,
}

/// The last occurrence of each key, in the order they first appear. `ON
/// CONFLICT DO UPDATE` cannot touch a row twice in one statement, and Redis
/// takes the last.
/// A hash write's pairs with each field once: where it was first named, with
/// the last value named for it. That is where Redis's listpack keeps a field
/// set twice in one `HSET`.
pub(crate) fn dedup_fields(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut at: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pairs.len());
    for (f, v) in pairs {
        match at.get(f.as_slice()) {
            Some(&i) => out[i].1 = v.clone(),
            None => {
                at.insert(f, out.len());
                out.push((f.clone(), v.clone()));
            }
        }
    }
    out
}

pub(crate) fn dedup_last<T: Clone>(items: &[T], key: impl Fn(&T) -> &[u8]) -> Vec<T> {
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

pub(crate) fn zadd_sql(db: u8, flags: &ZAddFlags) -> String {
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

    /// A cached collection read that lost `sql::live` is a stale read that
    /// no in-process check would notice, because the reply is well-formed
    /// either way. The expired-key e2e tests catch it end to end; this pins
    /// it at the source, one statement at a time, so the failure names the
    /// statement rather than a command three layers up.
    #[test]
    fn every_cached_collection_read_carries_the_liveness_guard() {
        for db in 0..NUM_DBS {
            let guard = sql::live(db);
            for (name, stmt) in [
                ("HGET", &sql::HGET[db]),
                ("HGETALL", &sql::HGETALL[db]),
                ("HKEYS", &sql::HKEYS[db]),
                ("HVALS", &sql::HVALS[db]),
                ("HLEN", &sql::HLEN[db]),
                ("HEXISTS", &sql::HEXISTS[db]),
                ("HSTRLEN", &sql::HSTRLEN[db]),
                ("SCARD", &sql::SCARD[db]),
                ("SMEMBERS", &sql::SMEMBERS[db]),
                ("SISMEMBER", &sql::SISMEMBER[db]),
                ("ZCARD", &sql::ZCARD[db]),
                ("ZSCORE", &sql::ZSCORE[db]),
                ("LLEN", &sql::LLEN[db]),
            ] {
                assert!(
                    stmt.contains(&guard),
                    "{name} statement for db {db} lost its liveness guard: {stmt}"
                );
            }
        }
    }

    /// The other direction: `collection_read_guarded` must name exactly the
    /// commands whose statements the test above covers. A command wrongly on
    /// the list skips the reap its unguarded statement needed; one wrongly
    /// off it merely pays a reap, so only the first direction can lie.
    #[test]
    fn the_guarded_list_names_the_thirteen_cached_reads_and_no_others() {
        let guarded = [
            vec!["HGET", "k", "f"],
            vec!["HGETALL", "k"],
            vec!["HKEYS", "k"],
            vec!["HVALS", "k"],
            vec!["HLEN", "k"],
            vec!["HEXISTS", "k", "f"],
            vec!["HSTRLEN", "k", "f"],
            vec!["SMEMBERS", "k"],
            vec!["SISMEMBER", "k", "m"],
            vec!["SCARD", "k"],
            vec!["ZCARD", "k"],
            vec!["ZSCORE", "k", "m"],
            vec!["LLEN", "k"],
        ];
        for args in &guarded {
            let cmd = is_ok(Command::parse(parts(args)));
            assert!(
                cmd.collection_read_guarded(),
                "{} should skip the up-front reap",
                args[0]
            );
        }
        // Collection reads that run inline SQL without the guard, and must
        // therefore keep reaping first.
        let unguarded = [
            vec!["LRANGE", "k", "0", "-1"],
            vec!["LINDEX", "k", "0"],
            vec!["SRANDMEMBER", "k"],
            vec!["ZRANGE", "k", "0", "-1"],
            vec!["ZCOUNT", "k", "-inf", "+inf"],
            vec!["SINTERCARD", "1", "k"],
            vec!["LPOS", "k", "m"],
            vec!["ZRANDMEMBER", "k"],
        ];
        for args in &unguarded {
            let cmd = is_ok(Command::parse(parts(args)));
            assert!(
                !cmd.collection_read_guarded(),
                "{} reads through inline SQL and must reap first",
                args[0]
            );
        }
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

    /// `OBJECT ENCODING` reports Redis's representation of a string, not
    /// ours: what pg_redis actually stores is an inline slot and a chunk pool.
    #[test]
    fn object_encoding_names_redis_representations() {
        assert_eq!(string_encoding(b"12345"), "int");
        assert_eq!(string_encoding(b"-1"), "int");
        // Not canonical, so not an int — `007` is a string to Redis.
        assert_eq!(string_encoding(b"007"), "embstr");
        assert_eq!(string_encoding(b"hello"), "embstr");
        assert_eq!(string_encoding(&[b'x'; 44]), "embstr");
        assert_eq!(string_encoding(&[b'x'; 45]), "raw");
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
        // No count is one field as a bulk string, not a one-element array —
        // and a drawn one: always answering the first field was a bug.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            match random_fields(&pairs, None, false, false) {
                Response::BulkString(f) => seen.insert(f),
                _ => panic!("expected a bulk string"),
            };
        }
        assert_eq!(
            seen.len(),
            2,
            "200 draws over two fields never saw one of them"
        );
        assert!(matches!(
            random_fields(&[], None, false, false),
            Response::Null
        ));

        assert_eq!(len(&random_fields(&pairs, Some(1), false, false)), 1);
        // A positive count is capped at what the hash holds.
        assert_eq!(len(&random_fields(&pairs, Some(5), false, false)), 2);
        // A negative one repeats to reach the count asked for.
        assert_eq!(len(&random_fields(&pairs, Some(-5), false, false)), 5);
        // WITHVALUES interleaves, so every count doubles.
        assert_eq!(len(&random_fields(&pairs, Some(5), true, false)), 4);
        assert_eq!(len(&random_fields(&pairs, Some(-3), true, false)), 6);
        // An empty hash is an empty array whatever the count.
        assert_eq!(len(&random_fields(&[], Some(3), false, false)), 0);
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
        use crate::memdb::KeyKind::{Hash, List, Set, String as Str, Zset};

        let cases: &[(&[&str], &[crate::memdb::KeyKind])] = &[
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
    type TypedCase<'a> = (&'a [&'a str], &'a [(&'a [u8], &'a [crate::memdb::KeyKind])]);
    /// `(argv, expected replaced keys)`.
    type OverwriteCase<'a> = (&'a [&'a str], &'a [(&'a [u8], crate::memdb::KeyKind)]);

    /// The two-key commands name both sides, and the store commands type their
    /// sources — the destination is replaced whatever it held, so it is not one
    /// of these.
    #[test]
    fn two_key_commands_type_both_sides() {
        use crate::memdb::KeyKind::{List, Set};

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
        use crate::memdb::KeyKind::{Set, String as Str};

        let cases: &[OverwriteCase] = &[
            (&["SET", "k", "v"], &[(b"k", Str)]),
            (&["SETEX", "k", "10", "v"], &[(b"k", Str)]),
            (&["PSETEX", "k", "10", "v"], &[(b"k", Str)]),
            (&["MSET", "a", "1", "b", "2"], &[(b"a", Str), (b"b", Str)]),
            (&["SUNIONSTORE", "d", "a"], &[(b"d", Set)]),
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
        &["AUTH", "secret"] => Command::Auth { user: None, ref password } if password == b"secret",
        &["AUTH", "user", "mypass"] => Command::Auth { user: Some(_), ref password } if password == b"mypass",
        // Server administration goes to admin.rs whole, subcommand and all;
        // the container's own arity is checked here, the rest on execution.
        &["INFO"] => Command::Admin(ref a) if a.name == "INFO",
        &["CLIENT", "ID"] => Command::Admin(ref a) if a.name == "CLIENT",
        &["CLIENT", "SETNAME", "myconn"] => Command::Admin(ref a) if a.args.len() == 2,
        &["COMMAND"] => Command::Admin(ref a) if a.name == "COMMAND",
        &["COMMAND", "COUNT"] => Command::Admin(_),
        &["CONFIG", "GET", "maxmemory"] => Command::Admin(ref a) if a.name == "CONFIG",
        &["CONFIG", "RESETSTAT"] => Command::Admin(_),
        &["SLOWLOG", "LEN"] => Command::Admin(_),
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
        &["INCRBYFLOAT", "f", "1.5"] => Command::IncrByFloat { key, delta } if key == b"f" && delta == crate::longdouble::LongDouble::parse(b"1.5"),
        &["INCRBYFLOAT", "f", "notafloat"] => Command::IncrByFloat { delta: None, .. },
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
        &["HSTRLEN", "h", "f"] => Command::HStrLen { key, field } if key == b"h" && field == b"f",
        // Server and keyspace commands added with tier 2
        &["TIME"] => Command::Time,
        &["TOUCH", "k1", "k2"] => Command::Touch { keys } if keys.len() == 2,
        &["DUMP", "k"] => Command::Dump { key } if key == b"k",
        &["RESTORE", "k", "0", "payload"] => Command::Restore { ttl_ms: 0, replace: false, absttl: false, .. },
        &["RESTORE", "k", "100", "p", "REPLACE", "ABSTTL"] => Command::Restore { ttl_ms: 100, replace: true, absttl: true, .. },
        &["RESTORE", "k", "0", "p", "IDLETIME", "3"] => Command::Restore { idletime: Some(3), freq: None, .. },
        &["RESTORE", "k", "0", "p", "FREQ", "9"] => Command::Restore { idletime: None, freq: Some(9), .. },
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

        // The same extraction `COMMAND COUNT` answers from, so this test
        // vouches for the reply as well as the page.
        let listed: Vec<&str> = documented_commands().collect();
        assert!(
            listed.len() > 100,
            "only {} commands parsed out of the doc",
            listed.len()
        );
        assert_eq!(*COMMAND_COUNT, listed.len() as i64);

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

    /// The default halves stay contiguous and cover every database, and there
    /// is a shared-memory slot for each database the default puts in memory.
    #[test]
    fn the_database_halves_partition_the_range() {
        assert!((0..NUM_DBS).all(|db| is_ephemeral(db as u8) == (db < DURABLE_FROM as usize)));
        assert!(is_ephemeral(CACHE_DB) && !is_ephemeral(DURABLE_DB));
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
                cursor: 0,
                pattern: None,
                count: None,
                kind: None
            }
        ));
    }

    #[test]
    fn parse_scan_with_match() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "MATCH", "user:*"])));
        assert!(
            matches!(cmd, Command::Scan { cursor: 0, pattern: Some(p), count: None, kind: None } if p == b"user:*")
        );
    }

    #[test]
    fn parse_scan_with_count() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "COUNT", "100"])));
        assert!(matches!(
            cmd,
            Command::Scan {
                cursor: 0,
                pattern: None,
                count: Some(100),
                kind: None
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
                cursor: 0,
                pattern: Some(_),
                count: Some(10),
                kind: None
            }
        ));
    }

    /// The argument rules the Redis acceptance suite found missing: each of
    /// these was accepted, and usually acted on, before.
    #[test]
    fn parse_refuses_what_redis_refuses() {
        let err = |p: &[&str]| Command::parse(parts(p)).unwrap_err();
        // Arity comes from Redis's own table, ahead of every parser.
        assert!(err(&["GET", "a", "b"]).contains("wrong number of arguments for 'get'"));
        assert!(err(&["LPOP", "k", "1", "1"]).contains("wrong number of arguments"));
        assert!(err(&["PING", "a", "b"]).contains("wrong number of arguments for 'ping'"));
        // SET: unknown options and doubled expiries are syntax errors.
        assert!(err(&["SET", "k", "v", "FOO"]).contains("syntax error"));
        assert!(err(&["SET", "k", "v", "EX", "1", "PX", "1"]).contains("syntax error"));
        assert!(err(&["SET", "k", "v", "EX"]).contains("syntax error"));
        assert!(err(&["SET", "k", "v", "EXAT", "0"]).contains("invalid expire time"));
        assert!(err(&["GETEX", "k", "bogus"]).contains("syntax error"));
        // EXPIRE's NX/XX/GT/LT, and the combinations Redis refuses.
        assert!(err(&["EXPIRE", "k", "1", "AB"]).contains("Unsupported option AB"));
        assert!(err(&["EXPIRE", "k", "1", "NX", "XX"]).contains("not compatible"));
        assert!(err(&["EXPIRE", "k", "1", "GT", "LT"]).contains("not compatible"));
        assert!(matches!(
            is_ok(Command::parse(parts(&["PEXPIRE", "k", "1", "xx", "gt"]))),
            Command::PExpire {
                cond: ExpireCond::Gt,
                ..
            }
        ));
        // SETRANGE past 512 MiB, unless there is nothing to write.
        assert!(err(&["SETRANGE", "k", "536870908", "world"]).contains("maximum allowed size"));
        assert!(
            err(&["SETRANGE", "k", "9223372036854775807", "A"]).contains("maximum allowed size")
        );
        is_ok(Command::parse(parts(&[
            "SETRANGE",
            "k",
            "9223372036854775807",
            "",
        ])));
        // LPOS options.
        assert!(err(&["LPOS", "k", "v", "COUNT", "-1"]).contains("COUNT can't be negative"));
        assert!(err(&["LPOS", "k", "v", "MAXLEN", "-1"]).contains("MAXLEN can't be negative"));
        assert!(err(&["LPOS", "k", "v", "RANK", "-9223372036854775808"]).contains("out of range"));
        assert!(err(&["LPOS", "k", "v", "BOGUS", "1"]).contains("syntax error"));
        // Random-member counts Redis cannot represent.
        assert!(err(&["SRANDMEMBER", "k", "-9223372036854775808"]).contains("out of range"));
        assert!(
            err(&["HRANDFIELD", "k", "-9223372036854770000", "WITHVALUES"])
                .contains("out of range")
        );
        assert!(err(&["ZRANDMEMBER", "k", "1", "BOGUS"]).contains("syntax error"));
    }

    #[test]
    fn msetnx_keeps_the_last_value_of_a_repeated_key() {
        let cmd = is_ok(Command::parse(parts(&[
            "MSETNX", "a", "1", "b", "2", "a", "3",
        ])));
        let Command::MSetNx { pairs } = cmd else {
            panic!("expected MSETNX")
        };
        assert_eq!(
            pairs,
            vec![
                (b"a".to_vec(), b"3".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
    }

    #[test]
    fn expire_conditions_treat_no_deadline_as_infinite() {
        use ExpireCond::*;
        assert!(Nx.allows(None, 5) && !Nx.allows(Some(1), 5));
        assert!(Xx.allows(Some(1), 5) && !Xx.allows(None, 5));
        assert!(Gt.allows(Some(1), 5) && !Gt.allows(Some(9), 5) && !Gt.allows(None, 5));
        assert!(Lt.allows(Some(9), 5) && !Lt.allows(Some(1), 5) && Lt.allows(None, 5));
        // `XX LT`: the XX refuses a key with no deadline before LT is asked.
        let xxlt = ExpireCond::parse(&[b"LT".to_vec(), b"XX".to_vec()]).unwrap();
        assert!(xxlt.allows(Some(9), 5) && !xxlt.allows(Some(1), 5) && !xxlt.allows(None, 5));
        assert!(!xxlt.allows_none());
        let xxgt = ExpireCond::parse(&[b"XX".to_vec(), b"GT".to_vec()]).unwrap();
        assert!(!xxgt.allows(None, 5));
    }

    #[test]
    fn memory_deadlines_saturate_and_never_mean_no_expiry() {
        assert_eq!(deadline_us(0, i64::MAX / 10, 1_000_000), i64::MAX);
        assert_eq!(deadline_us(0, i64::MIN / 10, 1_000_000), 1);
        // EXPIREAT k 0 must be in the past, not the memory half's "none".
        assert_eq!(deadline_us(0, 0, 1_000_000), 1);
    }

    #[test]
    fn parse_scan_type_and_refuses_unknown_options() {
        use crate::memdb::KeyKind;
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "TYPE", "Hash"])));
        assert!(matches!(
            cmd,
            Command::Scan {
                kind: Some(Some(KeyKind::Hash)),
                ..
            }
        ));
        // A type pg_redis has no table for matches nothing, as in Redis 7.2.
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "TYPE", "vectorset"])));
        assert!(matches!(
            cmd,
            Command::Scan {
                kind: Some(None),
                ..
            }
        ));
        assert!(Command::parse(parts(&["SCAN", "0", "BOGUS", "1"])).is_err());
        assert!(Command::parse(parts(&["SCAN", "0", "COUNT", "0"])).is_err());
        assert!(Command::parse(parts(&["SCAN", "0", "MATCH"])).is_err());
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
        // Values from Redis 7.2's own replies.
        assert_eq!(format_score(f64::MAX), "1.7976931348623157e+308");
        assert_eq!(format_score(1.5), "1.5");
        assert_eq!(format_score(0.1), "0.1");
        assert_eq!(format_score(-0.0), "0");
        assert_eq!(format_score(1e-7), "1e-07");
        assert_eq!(format_score(0.0003), "0.0003");
        assert_eq!(format_score(3.0e18), "3000000000000000000");
        assert_eq!(format_score(1e20), "1e+20");
        assert_eq!(format_score(1.2e30), "1.2e+30");
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
    fn zset_aggregates_parse_as_one_composite() {
        for (input, store) in [
            (
                &["ZUNIONSTORE", "d", "2", "a", "b", "WEIGHTS", "2", "3"][..],
                true,
            ),
            (&["ZINTER", "2", "a", "b", "WITHSCORES"][..], false),
            (&["ZDIFF", "1", "a"][..], false),
            (&["ZINTERCARD", "2", "a", "b", "LIMIT", "5"][..], false),
        ] {
            match is_ok(Command::parse(parts(input))) {
                Command::Composite(crate::composite::Op::ZAggregate(z)) => {
                    assert_eq!(z.dst.is_some(), store, "{input:?}");
                }
                other => panic!("{input:?} parsed as {other:?}"),
            }
        }
        for (input, err) in [
            (
                &["ZUNION", "0", "a"][..],
                "ERR at least 1 input key is needed for 'zunion' command",
            ),
            (&["ZINTERCARD", "3", "a"][..], "ERR syntax error"),
            (
                &["ZUNION", "x", "a"][..],
                "ERR value is not an integer or out of range",
            ),
        ] {
            assert_eq!(Command::parse(parts(input)).err().as_deref(), Some(err));
        }
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
