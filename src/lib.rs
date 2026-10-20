// Every `unsafe` block says why it is sound, and every module that needs none
// says so with `#![forbid(unsafe_code)]`: the six that do are shm, shared,
// pubsub, pg, scripting's Lua bridge, and the worker entry point here.
#![deny(clippy::undocumented_unsafe_blocks)]

pub(crate) mod access;
pub(crate) mod acl;
pub(crate) mod admin;
pub(crate) mod blocking;
mod commands;
pub(crate) mod composite;
pub(crate) mod config;
pub(crate) mod cursor;
pub(crate) mod dblock;
pub(crate) mod dirty;
pub(crate) mod eventloop;
pub(crate) mod evict;
#[cfg(test)]
mod fuzz;
pub(crate) mod geo;
pub(crate) mod hll;
pub(crate) mod latency;
pub(crate) mod lazyfree;
pub(crate) mod longdouble;
pub(crate) mod memdb;
pub(crate) mod notify;
pub(crate) mod pg;
pub(crate) mod plans;
pub(crate) mod pubheap;
pub(crate) mod pubsub;
pub(crate) mod rdb;
pub(crate) mod redis_tables;
mod resp;
pub(crate) mod resp3;
pub(crate) mod scripting;
pub(crate) mod server;
pub(crate) mod shared;
pub(crate) mod shm;
pub(crate) mod storage;
pub(crate) mod store;
pub(crate) mod stream;
pub(crate) mod sys;
pub(crate) mod tracking;
pub(crate) mod watch;
pub(crate) mod worker;

use pgrx::bgworkers::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

::pgrx::extension_sql_file!("../sql/schema.sql", bootstrap);
// The views over every database's tables, and redis.memory_keys, once the
// functions they call exist.
::pgrx::extension_sql_file!("../sql/views.sql", finalize);

#[pg_schema]
mod redis {}

pub(crate) static PORT: GucSetting<i32> = GucSetting::<i32>::new(6379);
/// Database new connections start on. 0 is Redis's own default and lands in the
/// ephemeral half; set it to 8 (or use `SELECT durable`) for WAL-backed storage.
pub(crate) static DEFAULT_DB: GucSetting<i32> = GucSetting::<i32>::new(0);
pub(crate) static NUM_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(4);
/// Loopback by default: the Redis port bypasses PostgreSQL authentication, so
/// exposing it beyond the host must be a deliberate act.
pub(crate) static LISTEN_ADDRESS: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"127.0.0.1"));
pub(crate) static MAX_CONNECTIONS: GucSetting<i32> = GucSetting::<i32>::new(128);
pub(crate) static PASSWORD: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);
/// Redis's `aclfile`: the ACL file loaded at start and by `ACL LOAD`, and
/// written by `ACL SAVE`. Unset, as in Redis, there is none.
pub(crate) static ACLFILE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);
pub(crate) static BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(64);
/// Seconds between snapshots of a `memory+snapshot` database.
pub(crate) static SNAPSHOT_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(60);
pub(crate) static DATABASE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"postgres"));

/// What memory mode does past `redis.maxmemory`: Redis's `maxmemory-policy`,
/// all eight of its values, `noeviction` by default as in Redis.
pub(crate) static MAXMEMORY_POLICY: GucSetting<shm::Policy> =
    GucSetting::<shm::Policy>::new(shm::Policy::NoEviction);

/// Megabytes the memory engine may hold — Redis's `maxmemory`. The arena is
/// reserved at this size (plus headroom) but backed only as it fills.
pub(crate) static MAXMEMORY: GucSetting<i32> = GucSetting::<i32>::new(256);
/// The pub/sub heap's size in MB: subscriptions and undelivered messages.
pub(crate) static PUBSUB_MEMORY: GucSetting<i32> = GucSetting::<i32>::new(128);

static ROUTE_CTL: shared::Block<pubsub::RouteCtl> = shared::Block::new();
static WATCH_CTL: shared::Block<watch::WatchCtl> = shared::Block::new();
static SERVER_CTL: shared::Block<server::ServerCtl> = shared::Block::new();
static CURSOR_TABLE: shared::Block<cursor::CursorTable> = shared::Block::new();
static DB_LOCKS: shared::Block<dblock::DbLocks> = shared::Block::new();
static ACCESS_TABLE: shared::Block<access::AccessTable> = shared::Block::new();
static ACL_CTL: shared::Block<acl::AclCtl> = shared::Block::new();

/// Everything this extension keeps in shared memory, reserved in the
/// postmaster's request hook.
fn request_shmem() {
    // Pub/sub, its routes, the WATCH counters and the server-wide state.
    crate::pubheap::request();
    shared::request(pubsub::route_ctl_size());
    shared::request(watch::watch_ctl_size());
    shared::request(server::server_ctl_size());
    // The SCAN cursors of databases kept in tables.
    shared::request(cursor::cursor_table_size());
    shared::request(dblock::db_locks_size());
    // The access fields of logged databases' keys (`access.rs`).
    shared::request(access::access_table_size());
    // The ACL users and log (`acl.rs`).
    shared::request(acl::ctl_size());
    // The memory engine's arena: reserved at redis.maxmemory, backed by RAM
    // only as it fills.
    shm::request();
}

/// Attach the blocks `request_shmem` reserved, in the startup hook.
fn attach_shmem() {
    pubheap::startup();
    ROUTE_CTL.attach(c"pg_redis_route_ctl", pubsub::route_ctl_size());
    WATCH_CTL.attach(c"pg_redis_watch_ctl", watch::watch_ctl_size());
    DB_LOCKS.attach(c"pg_redis_db_locks", dblock::db_locks_size());
    if !SERVER_CTL.attach(c"pg_redis_server_ctl", server::server_ctl_size())
        && let Some(ctl) = SERVER_CTL.get()
    {
        server::init(ctl);
    }
    CURSOR_TABLE.attach(c"pg_redis_cursor_table", cursor::cursor_table_size());
    ACCESS_TABLE.attach(c"pg_redis_access_table", access::access_table_size());
    ACL_CTL.attach(c"pg_redis_acl", acl::ctl_size());
    shm::startup();
}

/// The pub/sub registry, when the extension was preloaded.
pub(crate) fn pubsub_state() -> Option<&'static pubsub::Slots> {
    pubsub::registry()
}

pub(crate) fn route_state() -> Option<&'static pubsub::RouteCtl> {
    ROUTE_CTL.get()
}

/// The server-wide shared state, once it exists — see `server.rs`.
pub(crate) fn server_state() -> Option<&'static server::ServerCtl> {
    SERVER_CTL.get()
}

/// The SCAN cursor table, once it exists (`cursor.rs`).
pub(crate) fn cursor_state() -> Option<&'static cursor::CursorTable> {
    CURSOR_TABLE.get()
}

/// The per-database reader/writer locks, once they exist (`dblock.rs`).
pub(crate) fn db_locks() -> Option<&'static dblock::DbLocks> {
    DB_LOCKS.get()
}

/// The logged databases' access fields, once they exist (`access.rs`).
pub(crate) fn access_state() -> Option<&'static access::AccessTable> {
    ACCESS_TABLE.get()
}

/// The ACL users and log, once they exist (`acl.rs`).
pub(crate) fn acl_state() -> Option<&'static acl::AclCtl> {
    ACL_CTL.get()
}

/// The WATCH counters, once they exist.
pub(crate) fn watch_state() -> Option<&'static watch::WatchCtl> {
    WATCH_CTL.get()
}

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // PostgreSQL refuses a restart-only setting defined after startup with a
    // FATAL, which used to end any session that loaded the library without
    // preloading it: CREATE EXTENSION, or a pg_restore of a database that
    // has the extension. Outside preloading they are defined as superuser
    // settings; nothing reads them then.
    let preloading = pg::preloading();
    let restart_only = || {
        if preloading {
            GucContext::Postmaster
        } else {
            GucContext::Suset
        }
    };
    GucRegistry::define_int_guc(
        c"redis.port",
        c"TCP port for the Redis protocol listener",
        c"Port number pg_redis listens on for Redis RESP2 connections (default: 6379)",
        &PORT,
        1024,
        65535,
        // Bound once at worker start; a reload cannot move the listener.
        restart_only(),
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.snapshot_interval",
        c"Seconds between snapshots of a memory+snapshot database",
        c"A memory+snapshot database is copied to redis.snapshot this often, and \
          reloaded from it when the server starts; a crash loses at most this \
          much. SAVE and BGSAVE take one on demand.",
        &SNAPSHOT_INTERVAL,
        1,
        86400,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.default_db",
        c"Database new connections start on",
        c"By default databases 0-7 are in shared memory and 8-15 are WAL-logged; \
          redis.set_policy changes either. Default: 0, matching Redis. Clients override per-connection with SELECT, \
          which also accepts the names 'cache' (0) and 'durable' (8).",
        &DEFAULT_DB,
        0,
        15,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.workers",
        c"Number of background worker processes",
        c"How many parallel SPI workers handle Redis commands (default: 4). Requires server restart.",
        &NUM_WORKERS,
        1,
        64,
        restart_only(),
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.listen_address",
        c"TCP bind address for the Redis listener",
        c"IP address pg_redis binds on (default: 127.0.0.1). The Redis port does not \
          go through PostgreSQL authentication, so widening this exposes every redis.* \
          table to anything that can reach the port. Set redis.password as well.",
        &LISTEN_ADDRESS,
        restart_only(),
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.aclfile",
        c"ACL file, as Redis's aclfile",
        c"The users loaded at start and by ACL LOAD, and written by ACL SAVE, in Redis's \
          ACL file format. A relative path is relative to the data directory. Unset, \
          ACL LOAD and ACL SAVE refuse and users last until the server stops, as in Redis.",
        &ACLFILE,
        restart_only(),
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.max_connections",
        c"Maximum simultaneous Redis client connections per worker",
        c"Connections beyond this limit are rejected (default: 128).",
        &MAX_CONNECTIONS,
        1,
        65535,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.password",
        c"Password for Redis client authentication",
        c"When set, clients must AUTH before any command.",
        &PASSWORD,
        GucContext::Suset,
        GucFlags::SUPERUSER_ONLY | GucFlags::NO_SHOW_ALL,
    );

    GucRegistry::define_int_guc(
        c"redis.batch_size",
        c"Maximum commands per group-commit transaction",
        c"How many queued commands are coalesced into one PostgreSQL transaction. \
          Higher values amortise WAL flush cost across more writes; lower values \
          reduce worst-case latency. Set to 1 to disable batching (default: 64).",
        &BATCH_SIZE,
        1,
        256,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.database",
        c"Database name for pg_redis workers",
        c"The database the workers started with the server connect to by name. Run \
          CREATE EXTENSION pg_redis there. Workers added with redis.add_workers connect \
          to the database they are called from. Default: 'postgres'.",
        &DATABASE,
        restart_only(),
        GucFlags::default(),
    );

    GucRegistry::define_enum_guc(
        c"redis.maxmemory_policy",
        c"What memory mode does past redis.maxmemory",
        c"Redis's maxmemory-policy. 'noeviction' (default, as in Redis): refuse the \
          write with an OOM error. 'allkeys-lru', 'allkeys-lfu', 'allkeys-random': \
          evict any key, least recently used, least frequently used or at random. \
          'volatile-lru', 'volatile-lfu', 'volatile-random', 'volatile-ttl': evict \
          only keys that carry an expiry, the last by soonest expiry.",
        &MAXMEMORY_POLICY,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.maxmemory",
        c"Memory the shared-memory databases may use",
        c"Redis's maxmemory, for memory mode. Past it, a write that could grow memory \
          is refused with OOM unless redis.maxmemory_policy can evict enough first. \
          Reserved at startup but backed by RAM only as data fills it. Requires restart.",
        &MAXMEMORY,
        8,
        i32::MAX,
        restart_only(),
        GucFlags::UNIT_MB,
    );

    GucRegistry::define_int_guc(
        c"redis.pubsub_memory",
        c"Memory pub/sub, client tracking and ACL users may use",
        c"Holds every subscription, every undelivered pub/sub message, the client \
          tracking table and the ACL users and log, without per-name or per-message limits. A subscriber is \
          disconnected by client-output-buffer-limit pubsub, as in Redis. Reserved at \
          startup but backed by RAM only as it fills. Requires restart.",
        &PUBSUB_MEMORY,
        8,
        i32::MAX,
        restart_only(),
        GucFlags::UNIT_MB,
    );

    // Shared memory and the listener's workers exist only when preloaded;
    // the extension script tells whoever creates it otherwise.
    if !preloading {
        return;
    }

    shared::install_hooks(request_shmem, attach_shmem);

    let my_db: pg_sys::Oid = pg::database_oid();
    let db_oid_datum = pg_sys::Datum::from(my_db);
    let n = NUM_WORKERS.get() as usize;
    for idx in 0..n {
        BackgroundWorkerBuilder::new(&format!("pg_redis worker {}", idx))
            .set_function("pg_redis_worker_main")
            .set_library("pg_redis")
            .set_argument(Some(db_oid_datum))
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(WORKER_RESTART))
            .load();
    }
}

/// How long the postmaster waits before starting a worker again. A worker
/// that exits with an uncaught ERROR (status 1) is restarted after this; after
/// a crash anywhere in the cluster, the postmaster restarts every worker that
/// has a restart time as soon as it has reset shared memory. Without one,
/// either event left the Redis port closed until someone ran
/// `redis.add_workers` or restarted PostgreSQL. A worker that exits cleanly
/// (status 0: shutdown, or `redis.remove_workers`) is not restarted.
const WORKER_RESTART: std::time::Duration = std::time::Duration::from_secs(5);

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_redis_worker_main(arg: pg_sys::Datum) {
    worker::worker_main(arg);
}

/// Add n additional pg_redis workers dynamically (no server restart required).
/// They restart like the startup workers (`WORKER_RESTART`); `remove_workers`
/// stops them for good. They are not re-created by a server restart.
#[pg_extern(schema = "redis")]
fn add_workers(n: i32) -> i32 {
    if !pg::is_superuser() {
        error!("permission denied: superuser required");
    }
    if server_state().is_none() {
        error!(
            "pg_redis is not in shared_preload_libraries: add it there and restart to start the Redis listener"
        );
    }
    let schema_exists =
        Spi::get_one::<bool>("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'redis')")
            .ok()
            .flatten()
            .unwrap_or(false);
    if !schema_exists {
        error!("pg_redis is not installed in the current database");
    }
    let my_db: pg_sys::Oid = pg::database_oid();
    let db_oid_datum = pg_sys::Datum::from(my_db);
    let mut added = 0i32;
    for _ in 0..n.max(0) {
        match BackgroundWorkerBuilder::new("pg_redis worker")
            .set_function("pg_redis_worker_main")
            .set_library("pg_redis")
            .set_argument(Some(db_oid_datum))
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(WORKER_RESTART))
            .load_dynamic()
        {
            Ok(_) => added += 1,
            Err(_) => break,
        }
    }
    added
}

/// Terminate n pg_redis workers (newest first). A terminated worker exits
/// cleanly and is not restarted, startup or dynamic; a server restart brings
/// the startup pool back to `redis.workers`.
#[pg_extern(schema = "redis")]
fn remove_workers(n: i32) -> i32 {
    if !pg::is_superuser() {
        error!("permission denied: superuser required");
    }
    match Spi::get_one::<i32>(&format!(
        "SELECT count(pg_terminate_backend(pid))::int \
         FROM (SELECT pid FROM pg_stat_activity \
               WHERE backend_type LIKE 'pg_redis worker%' \
               ORDER BY backend_start DESC \
               LIMIT {}) s",
        n.max(0)
    )) {
        Ok(Some(c)) => c,
        _ => 0,
    }
}

/// Return the current number of running pg_redis workers.
/// Give a database a storage policy — `memory`, `memory+snapshot` or `logged`
/// (docs/STORAGE.md). Returns the policy it now has.
#[pg_extern(schema = "redis")]
fn set_policy(db: i32, policy: &str) -> String {
    if !pg::is_superuser() {
        error!("permission denied: superuser required");
    }
    if !(0..commands::NUM_DBS as i32).contains(&db) {
        error!(
            "database {db} is out of range: 0 to {}",
            commands::NUM_DBS - 1
        );
    }
    let Some(to) = storage::Policy::parse(policy) else {
        error!("unknown policy '{policy}': memory, memory+snapshot or logged");
    };
    match Spi::connect_mut(|client| storage::set_policy(client, db as u8, to)) {
        Ok(p) => p.name().to_string(),
        Err(e) => error!("{e}"),
    }
}

/// Every database with the policy that serves it now, and — while its data
/// is still moving — the policy it is moving from.
#[pg_extern(schema = "redis")]
fn policies() -> TableIterator<
    'static,
    (
        name!(db, i32),
        name!(policy, String),
        name!(moving_from, Option<String>),
    ),
> {
    TableIterator::new(storage::status().into_iter().map(|(db, p, from)| {
        (
            db as i32,
            p.name().to_string(),
            from.map(|f| f.name().to_string()),
        )
    }))
}

/// Bytes as text when they are UTF-8, NULL otherwise: keys and values are
/// bytes, as Redis's are, and most are text. The views in sql/views.sql show
/// them with it.
#[pg_extern(schema = "redis", immutable, parallel_safe, strict)]
fn text(bytes: &[u8]) -> Option<String> {
    std::str::from_utf8(bytes).ok().map(str::to_string)
}

/// A key or value as SQL's `bytea`.
type Bytea = Vec<u8>;

/// Every live key of a memory database, one row per element, in Redis's
/// order (a key's rows come out together, in order: number them `WITH
/// ORDINALITY`): what `redis.memory_keys(db)` builds its rows from. Read in
/// pages under the database's lock, so it is not one atomic snapshot, as
/// SCAN is not. Empty for a database that is not in memory. `field` is what
/// goes with an element: a hash's field, a sorted set member's score as
/// Redis prints it, empty for other types. The deadlines are
/// `redis.memory_deadlines(db)`'s.
#[pg_extern(schema = "redis")]
fn memory_entries(
    db: i32,
) -> TableIterator<
    'static,
    (
        name!(key, Bytea),
        name!(kind, String),
        name!(field, Bytea),
        name!(value, Bytea),
    ),
> {
    let mut rows = Vec::new();
    for key in memory_walk(db, true) {
        let kind = key.kind.name().to_string();
        // A stream has no elements here, and still one row.
        if key.elements.is_empty() {
            rows.push((key.key.clone(), kind.clone(), Vec::new(), Vec::new()));
        }
        for e in key.elements {
            let field = match e.score {
                Some(s) => commands::format_score(s).into_bytes(),
                None => e.field.unwrap_or_default(),
            };
            rows.push((key.key.clone(), kind.clone(), field, e.value));
        }
    }
    TableIterator::new(rows)
}

/// The deadline of every live key of a memory database that has one, in
/// microseconds since the epoch, read as `redis.memory_entries` reads.
#[pg_extern(schema = "redis")]
fn memory_deadlines(
    db: i32,
) -> TableIterator<'static, (name!(key, Bytea), name!(expires_at_us, i64))> {
    TableIterator::new(
        memory_walk(db, false)
            .into_iter()
            .filter(|k| k.deadline != 0)
            .map(|k| (k.key, k.deadline)),
    )
}

/// Every live key of memory database `db`, a page at a time under its lock,
/// with its elements if asked; none when `db` is not in memory.
fn memory_walk(db: i32, elements: bool) -> Vec<memdb::Copied> {
    let mut keys = Vec::new();
    let slot = u8::try_from(db)
        .ok()
        .filter(|d| (*d as usize) < commands::NUM_DBS)
        .and_then(storage::slot);
    let mut cursor = slot.map(|s| (s, 0));
    while let Some((slot, at)) = cursor {
        let (next, page) =
            shm::with_db(slot, |m| m.copy_page(at, 256, elements)).unwrap_or_else(|f| {
                pgrx::error!("pg_redis: could not read database {db} from shared memory: {f:?}")
            });
        keys.extend(page);
        cursor = (next != 0).then_some((slot, next));
    }
    keys
}

#[pg_extern(schema = "redis")]
fn worker_count() -> i64 {
    Spi::get_one::<i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity \
         WHERE backend_type LIKE 'pg_redis worker%'",
    )
    .ok()
    .flatten()
    .unwrap_or(0)
}

/// Route PUBLISH messages on `channel` to an INSERT into `schema`.`tbl`.
/// The target table must have `channel TEXT` and `payload TEXT` columns.
/// Call `redis.create_pubsub_table(schema, tbl)` to create a compatible table.
#[pg_extern(schema = "redis")]
fn route_publish(channel: &str, schema: &str, tbl: &str) {
    if !pg::is_superuser() {
        error!("permission denied: superuser required");
    }
    // Schema and table are PostgreSQL identifiers (NAMEDATALEN - 1); the channel
    // is bounded by the pub/sub slot layout instead.
    for (name, val, limit) in [
        ("channel", channel, pubsub::MAX_CHANNEL_LEN),
        ("schema", schema, 63),
        ("table", tbl, 63),
    ] {
        if val.len() > limit {
            error!("{name} must be ≤ {limit} bytes");
        }
        if val.contains('\0') {
            error!("{name} must not contain NUL bytes");
        }
    }
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO redis.pubsub_routes (channel, schema, tbl) VALUES ($1, $2, $3) \
                 ON CONFLICT (channel) DO UPDATE SET schema = EXCLUDED.schema, tbl = EXCLUDED.tbl",
                None,
                &[channel.into(), schema.into(), tbl.into()],
            )
            .unwrap_or_else(|e| error!("failed to save route: {e}"));
    });
    if let Some(ctl) = crate::route_state() {
        crate::pubsub::route_add(ctl, channel.as_bytes(), schema.as_bytes(), tbl.as_bytes());
    }
}

/// Remove the PUBLISH→table route for `channel`.
#[pg_extern(schema = "redis")]
fn unroute_publish(channel: &str) {
    if !pg::is_superuser() {
        error!("permission denied: superuser required");
    }
    Spi::connect_mut(|client| {
        client
            .update(
                "DELETE FROM redis.pubsub_routes WHERE channel = $1",
                None,
                &[channel.into()],
            )
            .unwrap_or_else(|e| error!("failed to remove route: {e}"));
    });
    if let Some(ctl) = crate::route_state() {
        crate::pubsub::route_remove(ctl, channel.as_bytes());
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::commands::NUM_DBS;
    use crate::commands::sql;
    use pgrx::prelude::*;

    /// Run one of the cached statements and return its first column, collapsing
    /// "no rows" to `None` exactly as `Command::execute` does.
    fn query_one<T: pgrx::datum::FromDatum + pgrx::datum::IntoDatum>(
        stmt: &str,
        args: &[&[u8]],
    ) -> Option<T> {
        Spi::connect(|c| {
            // Keys, fields and members are bytea now, so bind them as bytes.
            let args: Vec<pgrx::datum::DatumWithOid> = args.iter().map(|a| (*a).into()).collect();
            c.select(stmt, None, &args)
                .unwrap()
                .first()
                .get::<T>(1)
                .ok()
                .flatten()
        })
    }

    /// Clear a database's string and hash tables between assertions. DELETE
    /// rather than TRUNCATE: these tests share one server, and TRUNCATE's
    /// ACCESS EXCLUSIVE lock deadlocks against whatever else is reading.
    fn truncate(db: usize) {
        for tbl in ["kv", "hash"] {
            Spi::run(&format!("DELETE FROM redis.{tbl}_{db}")).unwrap();
        }
    }

    // ─────────────────────────── Views over the data ─────────────────────────

    /// The views read every database's tables as text, live keys only, and
    /// `redis.keys` gives each key's whole value as jsonb.
    #[pg_test]
    fn views_show_the_tables_data_as_text_and_json() {
        Spi::run(
            "INSERT INTO redis.kv_14 (key, value, expires_at) VALUES \
               ('v:str', 'hello', NULL), ('v:bin', '\\xff00', NULL), \
               ('v:gone', 'x', now() - interval '1 second')",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO redis.hash_14 (key, field, value) VALUES \
               ('v:hash', 'name', 'Ada'), ('v:hash', 'plan', 'pro')",
        )
        .unwrap();
        Spi::run("INSERT INTO redis.zset_14 (key, member, score) VALUES ('v:z', 'b', 2), ('v:z', 'a', 1)")
            .unwrap();
        let text = |q: &str| Spi::get_one::<String>(q).unwrap();
        assert_eq!(
            text("SELECT value FROM redis.strings WHERE db = 14 AND key = 'v:str'").as_deref(),
            Some("hello")
        );
        // Bytes that are not UTF-8: NULL as text, exact as bytes, base64 in json.
        assert_eq!(
            text(
                "SELECT coalesce(value, 'null') FROM redis.strings WHERE db = 14 AND key = 'v:bin'"
            )
            .as_deref(),
            Some("null")
        );
        assert_eq!(
            text("SELECT value::text FROM redis.keys WHERE db = 14 AND key = 'v:bin'").as_deref(),
            Some(r#"{"base64": "/wA="}"#)
        );
        // An expired key is gone, as it is for Redis.
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM redis.keys WHERE db = 14 AND key = 'v:gone'")
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            text("SELECT value::text FROM redis.keys WHERE db = 14 AND key = 'v:hash'").as_deref(),
            Some(r#"{"name": "Ada", "plan": "pro"}"#)
        );
        assert_eq!(
            text("SELECT value::text FROM redis.keys WHERE db = 14 AND key = 'v:z'").as_deref(),
            Some(r#"[["a", 1], ["b", 2]]"#)
        );
        for tbl in ["kv", "hash", "zset"] {
            Spi::run(&format!("DELETE FROM redis.{tbl}_14 WHERE key LIKE 'v:%'")).unwrap();
        }
    }

    /// A memory database is read from shared memory, in the same shape.
    #[pg_test]
    fn memory_keys_read_a_memory_database() {
        use crate::commands::Command;
        let run = |args: &[&str]| {
            let parts: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
            Command::parse(parts).unwrap().execute_mem(6)
        };
        run(&["DEL", "m:list", "m:str", "m:set"]);
        run(&["RPUSH", "m:list", "a", "b", "c"]);
        run(&["SET", "m:str", "v", "EX", "100"]);
        run(&["SADD", "m:set", "x"]);
        let text = |q: &str| Spi::get_one::<String>(q).unwrap();
        assert_eq!(
            text("SELECT value::text FROM redis.memory_keys(6) WHERE key = 'm:list'").as_deref(),
            Some(r#"["a", "b", "c"]"#)
        );
        assert_eq!(
            text("SELECT type || ' ' || value::text FROM redis.memory_keys(6) WHERE key = 'm:str'")
                .as_deref(),
            Some(r#"string "v""#)
        );
        assert_eq!(
            Spi::get_one::<bool>(
                "SELECT expires_at > now() FROM redis.memory_keys(6) WHERE key = 'm:str'"
            )
            .unwrap(),
            Some(true)
        );
        // A logged database has no memory to read.
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM redis.memory_keys(8)").unwrap(),
            Some(0)
        );
        run(&["DEL", "m:list", "m:str", "m:set"]);
    }

    // ─────────────────────────── Schema contract ────────────────────────────

    /// Every db must exist with the columns and constraints the generated SQL
    /// assumes, every table WAL-logged.
    #[pg_test]
    fn every_database_has_the_expected_tables() {
        for db in 0..NUM_DBS {
            Spi::run(&format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ('k', 'v') \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            ))
            .unwrap();
            Spi::run(&format!(
                "INSERT INTO redis.hash_{db} (key, field, value) VALUES ('h', 'f', 'v') \
                 ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
            ))
            .unwrap();

            let persistence = Spi::get_one::<String>(&format!(
                "SELECT relpersistence::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'redis' AND c.relname = 'kv_{db}'"
            ))
            .unwrap();
            assert_eq!(
                persistence.as_deref(),
                Some("p"),
                "redis.kv_{db} should be logged"
            );

            truncate(db);
        }
    }

    /// The background expiry sweep does a `WHERE expires_at <= now()` delete on
    /// every kv table; without this index that is a sequential scan per second.
    #[pg_test]
    fn every_kv_table_has_a_partial_expiry_index() {
        for db in 0..NUM_DBS {
            let exists = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_indexes \
                 WHERE schemaname = 'redis' AND tablename = 'kv_{db}' \
                   AND indexdef LIKE '%expires_at%')"
            ))
            .unwrap();
            assert_eq!(exists, Some(true), "redis.kv_{db} is missing the index");
        }
    }

    // ──────────────────── Cached statements against the schema ───────────────

    /// Runs the statements `Command::execute` actually issues, for every db, so
    /// a schema drift breaks here rather than on a live connection.
    #[pg_test]
    fn cached_statements_match_the_schema() {
        for db in 0..NUM_DBS {
            Spi::run(&format!(
                "INSERT INTO redis.kv_{db} (key, value) VALUES ('k', 'v')"
            ))
            .unwrap();
            Spi::run(&format!(
                "INSERT INTO redis.hash_{db} (key, field, value) VALUES ('h', 'f', 'hv')"
            ))
            .unwrap();

            assert_eq!(
                query_one::<Vec<u8>>(&sql::GET[db], &[b"k"]).as_deref(),
                Some(&b"v"[..]),
                "GET on db {db}"
            );
            assert_eq!(
                query_one::<i64>(&sql::STRLEN[db], &[b"k"]),
                Some(1),
                "STRLEN on db {db}"
            );
            // No expiry set, so the deadline reads -1 and a missing key -2.
            assert_eq!(query_one::<i64>(&sql::EXPIRE_US[db], &[b"k"]), Some(-1));
            assert_eq!(
                query_one::<i64>(&sql::EXPIRE_US[db], &[b"absent"]),
                Some(-2)
            );

            assert_eq!(
                query_one::<Vec<u8>>(&sql::HGET[db], &[b"h", b"f"]).as_deref(),
                Some(&b"hv"[..]),
                "HGET on db {db}"
            );
            assert_eq!(query_one::<i64>(&sql::HLEN[db], &[b"h"]), Some(1));
            assert_eq!(
                query_one::<Vec<u8>>(&sql::HKEYS[db], &[b"h"]).as_deref(),
                Some(&b"f"[..])
            );
            assert_eq!(
                query_one::<Vec<u8>>(&sql::HVALS[db], &[b"h"]).as_deref(),
                Some(&b"hv"[..])
            );
            assert_eq!(query_one::<i64>(&sql::SCARD[db], &[b"nosuchset"]), Some(0));
            assert_eq!(query_one::<i64>(&sql::ZCARD[db], &[b"nosuchzset"]), Some(0));
            assert_eq!(query_one::<i64>(&sql::LLEN[db], &[b"nosuchlist"]), Some(0));

            // GETDEL returns the value and removes the row in one statement.
            assert_eq!(
                query_one::<Vec<u8>>(&sql::GETDEL[db], &[b"k"]).as_deref(),
                Some(&b"v"[..])
            );
            assert_eq!(query_one::<Vec<u8>>(&sql::GET[db], &[b"k"]), None);

            truncate(db);
        }
    }

    /// An expired row must be invisible to reads even before the sweep removes it.
    #[pg_test]
    fn cached_reads_hide_expired_keys() {
        Spi::run(
            "INSERT INTO redis.kv_1 (key, value, expires_at) \
             VALUES ('gone', 'v', now() - interval '1 second'), \
                    ('live', 'v', now() + interval '1 hour')",
        )
        .unwrap();

        assert_eq!(query_one::<Vec<u8>>(&sql::GET[1], &[b"gone"]), None);
        assert_eq!(
            query_one::<Vec<u8>>(&sql::GET[1], &[b"live"]).as_deref(),
            Some(&b"v"[..])
        );
        assert!(
            query_one::<i64>(&sql::EXPIRE_US[1], &[b"live"]).unwrap_or(0) > 0,
            "the deadline should be there"
        );

        // The sweep the worker runs every second removes the expired row.
        Spi::run("DELETE FROM redis.kv_1 WHERE expires_at IS NOT NULL AND expires_at <= now()")
            .unwrap();
        let remaining =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM redis.kv_1 WHERE key = 'gone'")
                .unwrap();
        assert_eq!(remaining, Some(0), "expiry sweep must delete the row");

        // PERSIST clears the expiry so the key stops being a sweep candidate.
        Spi::connect_mut(|c| {
            c.update(&sql::PERSIST[1], None, &[b"live".as_slice().into()])
                .unwrap();
        });
        let cleared =
            Spi::get_one::<bool>("SELECT expires_at IS NULL FROM redis.kv_1 WHERE key = 'live'")
                .unwrap();
        assert_eq!(cleared, Some(true));
        truncate(1);
    }

    // ──────────────────────────── Worker management ──────────────────────────

    #[pg_test]
    fn workers_can_be_added_and_removed_at_runtime() {
        Spi::run("SELECT pg_sleep(1)").unwrap();
        let before = Spi::get_one::<i64>("SELECT redis.worker_count()")
            .unwrap()
            .unwrap_or(0);
        assert!(
            before > 0,
            "startup workers should be running, got {before}"
        );

        let added = Spi::get_one::<i32>("SELECT redis.add_workers(2)")
            .unwrap()
            .unwrap_or(0);
        assert_eq!(added, 2, "add_workers(2) should report 2");
        Spi::run("SELECT pg_sleep(1)").unwrap();

        let removed = Spi::get_one::<i32>("SELECT redis.remove_workers(2)")
            .unwrap()
            .unwrap_or(0);
        assert_eq!(removed, 2, "remove_workers(2) should terminate 2");
    }

    // ──────────────────────────── Binary safety ──────────────────────────────

    /// The point of the bytea columns: Redis keys and values are arbitrary byte
    /// strings. TEXT could not store a NUL at all and mangled anything that was
    /// not valid UTF-8, so these bytes are exactly what the old schema lost.
    const HOSTILE: &[u8] = &[0x00, 0xff, 0xfe, b'a', 0x80, b'\n', 0x01, 0xc3, 0x28];

    #[pg_test]
    fn keys_and_values_survive_arbitrary_bytes() {
        Spi::connect_mut(|c| {
            c.update(
                "INSERT INTO redis.kv_1 (key, value) VALUES ($1, $2)",
                None,
                &[HOSTILE.into(), HOSTILE.into()],
            )
            .unwrap();
        });

        // Read back through the statement GET actually issues.
        let got = query_one::<Vec<u8>>(&sql::GET[1], &[HOSTILE]);
        assert_eq!(
            got.as_deref(),
            Some(HOSTILE),
            "the key round-tripped only if it matched byte for byte, NUL included"
        );
        assert_eq!(
            query_one::<i64>(&sql::STRLEN[1], &[HOSTILE]),
            Some(HOSTILE.len() as i64),
            "STRLEN counts bytes, not characters"
        );

        // A key differing only after a NUL must be a different key. Under TEXT
        // this insert could not even be stored.
        let mut sibling = HOSTILE.to_vec();
        sibling.push(b'!');
        Spi::connect_mut(|c| {
            c.update(
                "INSERT INTO redis.kv_1 (key, value) VALUES ($1, $2)",
                None,
                &[sibling.as_slice().into(), b"other".as_slice().into()],
            )
            .unwrap();
        });
        assert_eq!(
            query_one::<Vec<u8>>(&sql::GET[1], &[HOSTILE]).as_deref(),
            Some(HOSTILE),
            "the sibling key must not have overwritten the original"
        );

        // Delete just these rows. TRUNCATE takes ACCESS EXCLUSIVE and deadlocks
        // against the other tests sharing this database.
        Spi::connect_mut(|c| {
            c.update(
                "DELETE FROM redis.kv_1 WHERE key IN ($1, $2)",
                None,
                &[HOSTILE.into(), sibling.as_slice().into()],
            )
            .unwrap();
        });
    }

    /// Hash fields and sorted-set members are equally binary, and bytea sorts
    /// bytewise — which is what Redis lexicographic ordering means.
    #[pg_test]
    fn fields_and_members_are_binary_and_sort_bytewise() {
        Spi::connect_mut(|c| {
            c.update(
                "INSERT INTO redis.hash_1 (key, field, value) VALUES ($1, $2, $3)",
                None,
                &[b"h".as_slice().into(), HOSTILE.into(), HOSTILE.into()],
            )
            .unwrap();
        });
        assert_eq!(
            query_one::<Vec<u8>>(&sql::HGET[1], &[b"h", HOSTILE]).as_deref(),
            Some(HOSTILE)
        );

        // 0x80 sorts after 'z' by byte value; a text collation would not
        // necessarily agree, and could not hold these bytes anyway.
        Spi::connect_mut(|c| {
            c.update(
                "INSERT INTO redis.zset_1 (key, member, score) \
                 VALUES ($1, $2, 1), ($1, $3, 1)",
                None,
                &[
                    b"z".as_slice().into(),
                    b"\x80".as_slice().into(),
                    b"z".as_slice().into(),
                ],
            )
            .unwrap();
        });
        let first = Spi::connect(|c| {
            c.select(
                "SELECT member FROM redis.zset_1 WHERE key = 'z' ORDER BY member LIMIT 1",
                None,
                &[],
            )
            .unwrap()
            .first()
            .get::<Vec<u8>>(1)
            .unwrap()
        });
        assert_eq!(
            first.as_deref(),
            Some(&b"z"[..]),
            "0x80 must sort after 'z'"
        );

        Spi::run("TRUNCATE redis.hash_1, redis.zset_1").unwrap();
    }

    // ─────────────────────────── Worker resilience ───────────────────────────

    /// A worker connects to `redis.database`, which usually has no
    /// `CREATE EXTENSION` and so no `redis.kv_*`. The sweep must notice rather
    /// than raise, and re-enable itself once the tables appear.
    #[pg_test]
    fn expiry_sweep_tolerates_a_database_without_the_extension() {
        assert!(
            crate::worker::kv_tables_present(),
            "this test database has the extension, so the sweep should be enabled"
        );

        let missing = Spi::get_one::<bool>("SELECT to_regclass('redis.no_such_table') IS NULL")
            .unwrap()
            .unwrap_or(false);
        assert!(missing, "to_regclass must report absent relations as NULL");
    }

    // ──────────────────────── Batch command isolation ────────────────────────

    /// A batch coalesces commands from unrelated connections into one
    /// transaction, so a failing command must roll back its own writes and
    /// nothing else. `in_subtransaction` is what provides that guarantee.
    #[pg_test]
    fn a_failed_command_in_a_batch_rolls_back_only_itself() {
        use crate::commands::Response;

        Spi::run("CREATE TEMP TABLE batch_iso (n int)").unwrap();

        // Committed: the command reported success.
        let ok = crate::worker::in_subtransaction(|| {
            Spi::run("INSERT INTO batch_iso VALUES (1)").unwrap();
            Response::Ok
        });
        assert!(matches!(ok, Response::Ok));

        // Rolled back: the command reported an error, so its write is discarded.
        let failed = crate::worker::in_subtransaction(|| {
            Spi::run("INSERT INTO batch_iso VALUES (2)").unwrap();
            Response::Error("boom".to_string())
        });
        assert!(matches!(failed, Response::Error(_)));

        // Committed again: the surrounding transaction survived the rollback.
        crate::worker::in_subtransaction(|| {
            Spi::run("INSERT INTO batch_iso VALUES (3)").unwrap();
            Response::Ok
        });

        let rows: Vec<Option<i32>> = Spi::connect(|c| {
            c.select("SELECT n FROM batch_iso ORDER BY n", None, &[])
                .unwrap()
                .map(|r| r.get::<i32>(1).unwrap())
                .collect()
        });
        assert_eq!(
            rows,
            vec![Some(1), Some(3)],
            "only the failing command's write should have been discarded"
        );

        Spi::run("DROP TABLE batch_iso").unwrap();
    }

    /// A PostgreSQL error raised while running a command has to come back as
    /// an error reply: uncaught it ends the worker process, and `INCR` past
    /// `bigint` is one command from any client. The surrounding transaction has
    /// to survive it.
    #[pg_test]
    fn a_sql_error_becomes_a_reply_rather_than_ending_the_worker() {
        use crate::commands::Response;

        Spi::run("CREATE TEMP TABLE raise_iso (n int)").unwrap();

        let overflowed = crate::worker::in_subtransaction(|| {
            Spi::run("INSERT INTO raise_iso VALUES (1)").unwrap();
            Spi::run("SELECT 9223372036854775807::bigint + 1").unwrap();
            Response::Ok
        });
        match overflowed {
            Response::Error(e) => assert!(
                e.contains("overflow"),
                "an overflow should name itself, got {e:?}"
            ),
            _ => panic!("expected an error reply from an overflowing command"),
        }

        // The subtransaction was rolled back, not merely abandoned...
        let survived = crate::worker::in_subtransaction(|| {
            Spi::run("INSERT INTO raise_iso VALUES (2)").unwrap();
            Response::Ok
        });
        assert!(matches!(survived, Response::Ok));

        let rows: Vec<Option<i32>> = Spi::connect(|c| {
            c.select("SELECT n FROM raise_iso ORDER BY n", None, &[])
                .unwrap()
                .map(|r| r.get::<i32>(1).unwrap())
                .collect()
        });
        assert_eq!(
            rows,
            vec![Some(2)],
            "the raising command's write should have been discarded, and only it"
        );

        Spi::run("DROP TABLE raise_iso").unwrap();
    }

    // ──────────────────────────── WATCH counters ─────────────────────────────

    /// The counters live in shared memory precisely so a WATCH taken on one
    /// worker sees a write dispatched through another. Exercising them directly
    /// is the closest a pg_test can get to that cross-process guarantee.
    #[pg_test]
    fn watch_counters_are_visible_through_shared_memory() {
        let ctl = crate::watch_state().expect("watch shmem must be initialised");

        let before = crate::watch::version(ctl, 0, b"k");
        crate::watch::bump(ctl, 0, b"k");
        let after = crate::watch::version(ctl, 0, b"k");
        assert_ne!(before, after, "a write must invalidate a prior WATCH");

        // An unrelated key must not be disturbed by that write. Slot collisions
        // are possible by design, so this asserts on a key known not to collide.
        let other = (0..64u32)
            .map(|i| format!("other-{i}"))
            .find(|k| {
                let v = crate::watch::version(ctl, 0, k.as_bytes());
                crate::watch::bump(ctl, 0, b"k");
                v == crate::watch::version(ctl, 0, k.as_bytes())
            })
            .expect("some key must not share a slot with \"k\"");
        let untouched = crate::watch::version(ctl, 0, other.as_bytes());
        crate::watch::bump(ctl, 0, b"k");
        assert_eq!(
            untouched,
            crate::watch::version(ctl, 0, other.as_bytes()),
            "writing one key must not invalidate a WATCH on an unrelated key"
        );

        // Same key name, different database: tracked independently.
        let db1 = crate::watch::version(ctl, 1, b"k");
        crate::watch::bump(ctl, 0, b"k");
        assert_eq!(db1, crate::watch::version(ctl, 1, b"k"));
    }

    // ──────────────────────────── Pub/sub routing ────────────────────────────

    #[pg_test]
    fn route_publish_upserts_and_unroute_removes() {
        Spi::run("DELETE FROM redis.pubsub_routes WHERE channel = 'ch'").unwrap();

        Spi::run("SELECT redis.route_publish('ch', 'public', 'tbl1')").unwrap();
        Spi::run("SELECT redis.route_publish('ch', 'public', 'tbl2')").unwrap();
        let tbl =
            Spi::get_one::<String>("SELECT tbl FROM redis.pubsub_routes WHERE channel = 'ch'")
                .unwrap();
        assert_eq!(
            tbl.as_deref(),
            Some("tbl2"),
            "re-routing a channel should replace the target, not duplicate it"
        );

        Spi::run("SELECT redis.unroute_publish('ch')").unwrap();
        let count =
            Spi::get_one::<i64>("SELECT count(*) FROM redis.pubsub_routes WHERE channel = 'ch'")
                .unwrap();
        assert_eq!(count, Some(0));
    }

    #[pg_test]
    fn create_pubsub_table_accepts_the_routed_insert() {
        Spi::run("DROP TABLE IF EXISTS public.pg_redis_route_tbl").unwrap();
        Spi::run("SELECT redis.create_pubsub_table('public', 'pg_redis_route_tbl')").unwrap();

        // Exactly the column set Command::TablePublish writes.
        Spi::run(
            "INSERT INTO public.pg_redis_route_tbl (channel, payload) VALUES ('mychan', 'hello')",
        )
        .unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM public.pg_redis_route_tbl WHERE channel = 'mychan'",
        )
        .unwrap();
        assert_eq!(count, Some(1));
        // The payload is readable as text, and as JSON from there.
        let text = Spi::get_one::<String>(
            "SELECT channel_text || ' ' || payload_text FROM public.pg_redis_route_tbl",
        )
        .unwrap();
        assert_eq!(text.as_deref(), Some("mychan hello"));

        Spi::run("DROP TABLE IF EXISTS public.pg_redis_route_tbl").unwrap();
    }

    /// Identifiers are interpolated into the CREATE TABLE, so a quoted name
    /// must be handled as a name and not as SQL.
    #[pg_test]
    fn create_pubsub_table_quotes_identifiers() {
        let odd = r#"pg redis "odd" tbl"#;
        Spi::connect_mut(|c| {
            c.update(
                "SELECT redis.create_pubsub_table('public', $1)",
                None,
                &[odd.into()],
            )
            .unwrap();
        });
        let exists = Spi::connect(|c| {
            c.select(
                "SELECT EXISTS (SELECT 1 FROM pg_tables \
                 WHERE schemaname = 'public' AND tablename = $1)",
                None,
                &[odd.into()],
            )
            .unwrap()
            .first()
            .get::<bool>(1)
            .unwrap()
        });
        assert_eq!(exists, Some(true));
        Spi::run(&format!(
            r#"DROP TABLE IF EXISTS public."{}""#,
            odd.replace('"', "\"\"")
        ))
        .unwrap();
    }

    // ──────────────────────────── SCAN cursors ───────────────────────────────

    /// The database these tests fill: logged by default, and touched by no
    /// other test.
    const SCAN_DB: u8 = 13;

    fn clear_scan_db() {
        for tbl in [
            "kv", "hash", "list", "set", "zset", "stream", "expiry", "encoding",
        ] {
            Spi::run(&format!("DELETE FROM redis.{tbl}_{SCAN_DB}")).unwrap();
        }
    }

    /// One command through the SQL path, as a worker runs it.
    fn run(argv: &[&[u8]]) -> crate::commands::Response {
        let cmd =
            crate::commands::Command::parse(argv.iter().map(|a| a.to_vec()).collect::<Vec<_>>())
                .unwrap();
        Spi::connect_mut(|c| cmd.execute(c, SCAN_DB))
    }

    /// A whole iteration of `argv` (the cursor goes after `cursor_at`
    /// arguments), with `between` run after each call: every element, and
    /// the number of calls.
    fn iterate(
        argv: &[&[u8]],
        cursor_at: usize,
        mut between: impl FnMut(usize),
    ) -> (Vec<Vec<u8>>, usize) {
        use crate::commands::Response;
        let mut seen = Vec::new();
        let mut cursor = 0u64;
        for calls in 1..10_000 {
            let text = cursor.to_string();
            let mut line: Vec<&[u8]> = argv.to_vec();
            line.insert(cursor_at, text.as_bytes());
            match run(&line) {
                Response::ScanResult { cursor: next, keys } => {
                    seen.extend(keys.into_iter().flatten());
                    if next == 0 {
                        return (seen, calls);
                    }
                    assert!(next <= i64::MAX as u64, "the cursor must fit an i64");
                    cursor = next;
                }
                other => panic!("{argv:?}: expected a scan reply, got {other:?}"),
            }
            between(calls);
        }
        panic!("{argv:?}: the iteration did not end");
    }

    fn sorted(mut v: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        v.sort();
        v.dedup();
        v
    }

    /// Keys in every table, a live key with a deadline and two that are past
    /// theirs, iterated ten at a time: every live key comes back, once each
    /// since nothing changes, and the dead ones never.
    #[pg_test]
    fn scan_on_tables_returns_every_live_key_across_every_table() {
        clear_scan_db();
        let db = SCAN_DB;
        Spi::run(&format!(
            "INSERT INTO redis.kv_{db} (key, value) \
               SELECT convert_to('k:' || lpad(i::text, 4, '0'), 'UTF8'), 'v' \
                 FROM generate_series(1, 300) i; \
             INSERT INTO redis.kv_{db} (key, value, expires_at) VALUES \
               ('kv:live', 'v', now() + interval '1 hour'), \
               ('kv:dead', 'v', now() - interval '1 second'); \
             INSERT INTO redis.hash_{db} (key, field, value) \
               SELECT convert_to('h:' || i, 'UTF8'), convert_to('f' || j, 'UTF8'), 'v' \
                 FROM generate_series(1, 40) i, generate_series(1, 5) j; \
             INSERT INTO redis.list_{db} (key, pos, value) \
               SELECT convert_to('l:' || i, 'UTF8'), j, 'v' \
                 FROM generate_series(1, 30) i, generate_series(1, 4) j; \
             INSERT INTO redis.set_{db} (key, member) \
               SELECT convert_to('s:' || i, 'UTF8'), convert_to('m' || j, 'UTF8') \
                 FROM generate_series(1, 30) i, generate_series(1, 3) j; \
             INSERT INTO redis.zset_{db} (key, member, score) \
               SELECT convert_to('z:' || i, 'UTF8'), convert_to('m' || j, 'UTF8'), j \
                 FROM generate_series(1, 30) i, generate_series(1, 3) j; \
             INSERT INTO redis.stream_{db} (key, value) VALUES ('x:1', 'v'), ('x:2', 'v'); \
             INSERT INTO redis.expiry_{db} (key, expires_at) VALUES \
               ('s:1', now() - interval '1 second'), ('h:1', now() + interval '1 hour')"
        ))
        .unwrap();
        let want: Vec<Vec<u8>> = Spi::connect(|c| {
            c.select(
                &format!(
                    "SELECT key FROM (SELECT key FROM redis.kv_{db} \
                       UNION SELECT key FROM redis.hash_{db} UNION SELECT key FROM redis.list_{db} \
                       UNION SELECT key FROM redis.set_{db} UNION SELECT key FROM redis.zset_{db} \
                       UNION SELECT key FROM redis.stream_{db}) u \
                     WHERE key NOT IN ('kv:dead'::bytea, 's:1'::bytea)"
                ),
                None,
                &[],
            )
            .unwrap()
            .filter_map(|r| r.get::<Vec<u8>>(1).unwrap())
            .collect()
        });
        assert_eq!(want.len(), 300 + 1 + 40 + 30 + 29 + 30 + 2);

        let (seen, calls) = iterate(&[b"SCAN", b"COUNT", b"10"], 1, |_| {});
        assert!(
            calls > 40,
            "ten a call over {} keys took {calls}",
            want.len()
        );
        assert_eq!(
            seen.len(),
            want.len(),
            "nothing changed, so nothing repeats"
        );
        assert_eq!(sorted(seen), sorted(want.clone()));

        // MATCH and TYPE filter each page; the iteration still covers it all.
        let (hashes, _) = iterate(&[b"SCAN", b"COUNT", b"10", b"TYPE", b"hash"], 1, |_| {});
        assert_eq!(hashes.len(), 40);
        let (matched, _) = iterate(&[b"SCAN", b"MATCH", b"z:*"], 1, |_| {});
        assert_eq!(matched.len(), 30);
        // One call with a large COUNT is the whole keyspace and ends at once.
        let (all, calls) = iterate(&[b"SCAN", b"COUNT", b"100000"], 1, |_| {});
        assert_eq!((all.len(), calls), (want.len(), 1));
        clear_scan_db();
    }

    /// Keys deleted between calls, including ones the cursor has passed and
    /// ones it has not reached: every key that stayed is still returned.
    #[pg_test]
    fn scan_on_tables_skips_nothing_when_keys_are_deleted_mid_iteration() {
        clear_scan_db();
        let db = SCAN_DB;
        Spi::run(&format!(
            "INSERT INTO redis.kv_{db} (key, value) \
               SELECT convert_to('k:' || lpad(i::text, 4, '0'), 'UTF8'), 'v' \
                 FROM generate_series(1, 500) i; \
             INSERT INTO redis.set_{db} (key, member) \
               SELECT convert_to('k:' || lpad(i::text, 4, '0') || 's', 'UTF8'), 'm' \
                 FROM generate_series(1, 100) i"
        ))
        .unwrap();
        let mut deleted = std::collections::BTreeSet::new();
        let (seen, _) = iterate(&[b"SCAN", b"COUNT", b"10"], 1, |call| {
            // Spread over the whole keyspace, behind and ahead of the cursor.
            for j in 0..3 {
                let i = (call * 37 + j * 211) % 500 + 1;
                let key = format!("k:{i:04}");
                Spi::connect_mut(|c| {
                    c.update(
                        &format!("DELETE FROM redis.kv_{db} WHERE key = $1"),
                        None,
                        &[key.as_bytes().into()],
                    )
                    .unwrap();
                });
                deleted.insert(key.into_bytes());
            }
        });
        let seen: std::collections::BTreeSet<Vec<u8>> = seen.into_iter().collect();
        for i in 1..=500 {
            let key = format!("k:{i:04}").into_bytes();
            assert!(
                deleted.contains(&key) || seen.contains(&key),
                "k:{i:04} was never deleted and never returned"
            );
        }
        for i in 1..=100 {
            assert!(seen.contains(format!("k:{i:04}s").as_bytes()));
        }
        clear_scan_db();
    }

    /// Fifty keys share a prefix longer than a cursor keeps, more than a page
    /// of them: the iteration still ends and returns each.
    #[pg_test]
    fn scan_on_tables_ends_when_long_keys_share_a_prefix() {
        clear_scan_db();
        let prefix = "p".repeat(crate::cursor::BOUND_LEN + 20);
        Spi::run(&format!(
            "INSERT INTO redis.kv_{SCAN_DB} (key, value) \
               SELECT convert_to('{prefix}' || lpad(i::text, 3, '0'), 'UTF8'), 'v' \
                 FROM generate_series(1, 50) i; \
             INSERT INTO redis.kv_{SCAN_DB} (key, value) \
               SELECT convert_to('q' || i, 'UTF8'), 'v' FROM generate_series(1, 20) i"
        ))
        .unwrap();
        let (seen, _) = iterate(&[b"SCAN", b"COUNT", b"10"], 1, |_| {});
        assert_eq!(sorted(seen).len(), 70);
        clear_scan_db();
    }

    /// HSCAN, SSCAN and ZSCAN page through one collection by its primary
    /// key, with deletes mid-way, MATCH, and the scores ZSCAN pairs.
    #[pg_test]
    fn collection_scans_on_tables_page_through_and_skip_nothing() {
        clear_scan_db();
        let db = SCAN_DB;
        Spi::run(&format!(
            "INSERT INTO redis.hash_{db} (key, field, value) \
               SELECT 'h', convert_to('f' || lpad(i::text, 3, '0'), 'UTF8'), \
                      convert_to('v' || i, 'UTF8') FROM generate_series(1, 300) i; \
             INSERT INTO redis.set_{db} (key, member) \
               SELECT 's', convert_to('m' || lpad(i::text, 3, '0'), 'UTF8') \
                 FROM generate_series(1, 300) i; \
             INSERT INTO redis.zset_{db} (key, member, score) \
               SELECT 'z', convert_to('m' || lpad(i::text, 3, '0'), 'UTF8'), i + 0.5 \
                 FROM generate_series(1, 300) i; \
             INSERT INTO redis.kv_{db} (key, value) VALUES ('str', 'v'); \
             INSERT INTO redis.encoding_{db} (key, encoding) VALUES ('h', 3)"
        ))
        .unwrap();

        // HSCAN: field, value pairs; delete fields as it goes. 300 fields fit
        // a listpack, which HSCAN would return whole; the row makes it a hash
        // table, as one grown past 512 and shrunk is.
        let mut deleted = std::collections::BTreeSet::new();
        let (flat, calls) = iterate(&[b"HSCAN", b"h", b"COUNT", b"10"], 2, |call| {
            let field = format!("f{:03}", (call * 53) % 300 + 1);
            Spi::connect_mut(|c| {
                c.update(
                    &format!("DELETE FROM redis.hash_{db} WHERE key = 'h' AND field = $1"),
                    None,
                    &[field.as_bytes().into()],
                )
                .unwrap();
            });
            deleted.insert(field.into_bytes());
        });
        assert!(calls > 20);
        assert_eq!(flat.len() % 2, 0);
        let pairs: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = flat
            .chunks(2)
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect();
        for i in 1..=300 {
            let f = format!("f{i:03}").into_bytes();
            if !deleted.contains(&f) {
                assert_eq!(
                    pairs.get(&f),
                    Some(&format!("v{i}").into_bytes()),
                    "f{i:03}"
                );
            }
        }

        // SSCAN: members only.
        let (members, calls) = iterate(&[b"SSCAN", b"s", b"COUNT", b"10"], 2, |_| {});
        assert_eq!((sorted(members).len(), calls > 20), (300, true));

        // ZSCAN: member, score pairs, with MATCH.
        let (flat, _) = iterate(
            &[b"ZSCAN", b"z", b"MATCH", b"m00*", b"COUNT", b"7"],
            2,
            |_| {},
        );
        let got: Vec<(String, String)> = flat
            .chunks(2)
            .map(|p| {
                (
                    String::from_utf8(p[0].clone()).unwrap(),
                    String::from_utf8(p[1].clone()).unwrap(),
                )
            })
            .collect();
        let want: Vec<(String, String)> = (1..=9)
            .map(|i| (format!("m{i:03}"), format!("{i}.5")))
            .collect();
        assert_eq!(got, want);

        // A missing key is an empty collection; another type is WRONGTYPE.
        let (none, calls) = iterate(&[b"SSCAN", b"nosuchkey"], 2, |_| {});
        assert_eq!((none.len(), calls), (0, 1));
        match run(&[b"HSCAN", b"s", b"0"]) {
            crate::commands::Response::Error(e) => assert!(e.starts_with("WRONGTYPE"), "{e}"),
            other => panic!("HSCAN on a set answered {other:?}"),
        }
        match run(&[b"ZSCAN", b"str", b"0"]) {
            crate::commands::Response::Error(e) => assert!(e.starts_with("WRONGTYPE"), "{e}"),
            other => panic!("ZSCAN on a string answered {other:?}"),
        }
        clear_scan_db();
    }

    // ───────────────────── Order and encodings on the tables ─────────────────────
    //
    // The order a hash's fields and a set's members come back in on the
    // tables, and the encoding `OBJECT ENCODING` reports, against what Redis
    // 7.2 answers.

    /// The database these tests use: logged by default, and touched by no
    /// other test.
    const ORDER_DB: u8 = 12;

    fn clear_order_db() {
        for db in [ORDER_DB, ORDER_DB - 1] {
            for tbl in [
                "kv", "hash", "list", "set", "zset", "stream", "expiry", "encoding",
            ] {
                Spi::run(&format!("DELETE FROM redis.{tbl}_{db}")).unwrap();
            }
        }
    }

    fn run_line(line: &str) -> crate::commands::Response {
        run_order_argv(line.split(' ').map(|p| p.as_bytes().to_vec()).collect())
    }

    fn run_order_argv(argv: Vec<Vec<u8>>) -> crate::commands::Response {
        let cmd = crate::commands::Command::parse(argv).unwrap();
        Spi::connect_mut(|c| cmd.execute(c, ORDER_DB))
    }

    fn order_words(prefix: &str, items: impl Iterator<Item = String>) -> String {
        std::iter::once(prefix.to_string())
            .chain(items)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn order_bulks(r: crate::commands::Response) -> Vec<String> {
        match r {
            crate::commands::Response::Array(v) => v
                .into_iter()
                .map(|b| String::from_utf8(b.unwrap_or_default()).unwrap())
                .collect(),
            other => panic!("not an array: {other:?}"),
        }
    }

    fn order_encoding(key: &str) -> String {
        match run_line(&format!("OBJECT ENCODING {key}")) {
            crate::commands::Response::BulkString(v) => String::from_utf8(v).unwrap(),
            other => panic!("not a bulk string: {other:?}"),
        }
    }

    fn order_row(db: u8, key: &str) -> Option<i16> {
        Spi::get_one_with_args::<i16>(
            &format!("SELECT (SELECT encoding FROM redis.encoding_{db} WHERE key = $1)"),
            &[key.as_bytes().into()],
        )
        .unwrap()
    }

    #[pg_test]
    fn hash_fields_keep_insertion_order_through_deletes_and_updates() {
        clear_order_db();
        run_line("HSET h c 1 a 2 b 3");
        run_line("HDEL h a");
        run_line("HSET h d 4");
        run_line("HSET h c 9");
        assert_eq!(order_bulks(run_line("HKEYS h")), ["c", "b", "d"]);
        assert_eq!(order_bulks(run_line("HVALS h")), ["9", "3", "4"]);
        assert_eq!(
            order_bulks(run_line("HGETALL h")),
            ["c", "9", "b", "3", "d", "4"]
        );
        assert_eq!(order_encoding("h"), "listpack");
        assert_eq!(
            order_row(ORDER_DB, "h"),
            None,
            "a listpack hash needs no row"
        );
        // HSCAN returns a listpack whole, in order, whatever COUNT asks.
        match run_line("HSCAN h 0 COUNT 1") {
            crate::commands::Response::ScanResult { cursor, keys } => {
                assert_eq!(cursor, 0);
                assert_eq!(keys.len(), 6);
            }
            other => panic!("{other:?}"),
        }
        // A field named twice stays where it was first named.
        run_line("HSET n x 1 y 2 x 3");
        assert_eq!(order_bulks(run_line("HGETALL n")), ["x", "3", "y", "2"]);
        clear_order_db();
    }

    #[pg_test]
    fn a_hash_grown_past_the_limit_stays_a_hashtable() {
        clear_order_db();
        run_line(&order_words(
            "HSET big",
            (0..513).map(|i| format!("f{i} v")),
        ));
        assert_eq!(order_encoding("big"), "hashtable");
        run_line(&order_words("HDEL big", (3..513).map(|i| format!("f{i}"))));
        assert_eq!(order_encoding("big"), "hashtable");
        assert_eq!(order_row(ORDER_DB, "big"), Some(3));
        run_line(&format!("HSET long a 1 b {}", "x".repeat(65)));
        run_line("HDEL long b");
        assert_eq!(order_encoding("long"), "hashtable");
        // The row goes with the last field, so the next hash of that name
        // starts as a listpack.
        run_line("HDEL long a");
        assert_eq!(order_row(ORDER_DB, "long"), None);
        run_line("HSET long a 1");
        assert_eq!(order_encoding("long"), "listpack");
        // DEL takes the row too.
        run_line("DEL big");
        assert_eq!(order_row(ORDER_DB, "big"), None);
        clear_order_db();
    }

    #[pg_test]
    fn sets_come_back_as_intsets_and_listpacks_would_have_them() {
        clear_order_db();
        run_line("SADD s 5 -3 100 2");
        assert_eq!(order_bulks(run_line("SMEMBERS s")), ["-3", "2", "5", "100"]);
        assert_eq!(order_encoding("s"), "intset");
        assert_eq!(order_row(ORDER_DB, "s"), None, "an intset needs no row");
        // A string makes it a listpack, the integers first in sorted order.
        run_line("SADD s x 1");
        assert_eq!(order_encoding("s"), "listpack");
        assert_eq!(
            order_bulks(run_line("SMEMBERS s")),
            ["-3", "2", "5", "100", "x", "1"]
        );
        // Without the string it stays a listpack, in its order.
        run_line("SREM s x 100");
        assert_eq!(order_bulks(run_line("SMEMBERS s")), ["-3", "2", "5", "1"]);
        assert_eq!(order_encoding("s"), "listpack");
        match run_line("SSCAN s 0 COUNT 1") {
            crate::commands::Response::ScanResult { cursor, keys } => {
                assert_eq!((cursor, keys.len()), (0, 4))
            }
            other => panic!("{other:?}"),
        }
        // SUNION builds an intset of the integers, so sorts them.
        assert_eq!(order_bulks(run_line("SUNION s")), ["-3", "1", "2", "5"]);
        run_line("SADD l c a b d");
        run_line("SREM l a");
        run_line("SADD l e");
        assert_eq!(order_bulks(run_line("SMEMBERS l")), ["c", "b", "d", "e"]);
        assert_eq!(
            order_bulks(run_line("SRANDMEMBER l 10")),
            ["c", "b", "d", "e"]
        );
        run_line("SADD A c a b 3 1");
        run_line("SADD B b c z");
        assert_eq!(order_bulks(run_line("SINTER A B")), ["b", "c"]);
        assert_eq!(
            order_bulks(run_line("SUNION A B")),
            ["c", "a", "b", "3", "1", "z"]
        );
        assert_eq!(order_bulks(run_line("SDIFF A B")), ["a", "3", "1"]);
        run_line("SADD I 9 3 1");
        assert_eq!(
            order_bulks(run_line("SUNION I A")),
            ["1", "3", "9", "c", "a", "b"]
        );
        run_line("SDIFFSTORE D A B");
        assert_eq!(order_bulks(run_line("SMEMBERS D")), ["a", "3", "1"]);
        assert_eq!(order_encoding("D"), "listpack");
        run_line("SADD li a 30 10 20");
        run_line("SREM li a");
        run_line("SINTERSTORE lo li li");
        assert_eq!(order_encoding("lo"), "intset");
        assert_eq!(order_bulks(run_line("SMEMBERS lo")), ["10", "20", "30"]);
        clear_order_db();
    }

    #[pg_test]
    fn sets_convert_once_and_stay_converted() {
        clear_order_db();
        run_line(&order_words("SADD big", (0..200).map(|i| format!("m{i}"))));
        assert_eq!(order_encoding("big"), "hashtable");
        run_line(&order_words("SREM big", (3..200).map(|i| format!("m{i}"))));
        assert_eq!(order_encoding("big"), "hashtable");
        run_line(&order_words("SADD ints", (1..=513).map(|i| i.to_string())));
        assert_eq!(order_encoding("ints"), "hashtable");
        run_line(&order_words("SADD dup", (0..200).map(|_| "a".to_string())));
        assert_eq!(order_encoding("dup"), "hashtable");
        run_line("SADD mix a 1");
        run_line("SREM mix a");
        assert_eq!(order_encoding("mix"), "listpack");
        run_line("SMOVE mix dst 1");
        assert_eq!(order_encoding("dst"), "intset");
        // Once a listpack, always one: moving the string out again leaves
        // the integer in a listpack.
        run_line("SADD dst x");
        run_line("SMOVE dst mix x");
        assert_eq!(order_encoding("dst"), "listpack");
        assert_eq!(order_bulks(run_line("SMEMBERS mix")), ["x"]);
        clear_order_db();
    }

    #[pg_test]
    fn sorted_sets_stay_skiplists() {
        clear_order_db();
        run_line(&order_words(
            "ZADD z",
            (0..129).map(|i| format!("{i} m{i}")),
        ));
        assert_eq!(order_encoding("z"), "skiplist");
        run_line("ZREMRANGEBYRANK z 0 100");
        assert_eq!(order_encoding("z"), "skiplist");
        run_line("ZADD small 2 b 1 a");
        assert_eq!(order_encoding("small"), "listpack");
        match run_line("ZSCAN small 0 COUNT 1") {
            crate::commands::Response::ScanResult { cursor, keys } => {
                assert_eq!((cursor, keys.len()), (0, 4))
            }
            other => panic!("{other:?}"),
        }
        run_line(&format!("ZINCRBY small 1 {}", "x".repeat(65)));
        assert_eq!(order_encoding("small"), "skiplist");
        clear_order_db();
    }

    /// RENAME, COPY and MOVE keep a converted key converted; a DUMP payload
    /// restores as Redis loads one, a small hash table as a listpack.
    #[pg_test]
    fn whole_key_moves_keep_the_encoding_and_the_order() {
        clear_order_db();
        run_line(&order_words("HSET h", (0..600).map(|i| format!("f{i} v"))));
        run_line(&order_words("HDEL h", (3..600).map(|i| format!("f{i}"))));
        run_line("RENAME h h2");
        assert_eq!(order_encoding("h2"), "hashtable");
        assert_eq!(order_row(ORDER_DB, "h"), None);
        run_line("COPY h2 h3");
        assert_eq!(order_encoding("h3"), "hashtable");
        let payload = match run_line("DUMP h2") {
            crate::commands::Response::BulkString(p) => p,
            other => panic!("{other:?}"),
        };
        run_order_argv(vec![
            b"RESTORE".to_vec(),
            b"h4".to_vec(),
            b"0".to_vec(),
            payload,
        ]);
        assert_eq!(order_encoding("h4"), "listpack");

        run_line("SADD s z 3 1");
        run_line("SREM s z");
        run_line("COPY s s2");
        assert_eq!(order_bulks(run_line("SMEMBERS s2")), ["3", "1"]);
        assert_eq!(order_encoding("s2"), "listpack");
        let payload = match run_line("DUMP s") {
            crate::commands::Response::BulkString(p) => p,
            other => panic!("{other:?}"),
        };
        run_order_argv(vec![
            b"RESTORE".to_vec(),
            b"s3".to_vec(),
            b"0".to_vec(),
            payload,
        ]);
        assert_eq!(order_bulks(run_line("SMEMBERS s3")), ["3", "1"]);
        assert_eq!(order_encoding("s3"), "listpack");
        run_line(&format!("MOVE h2 {}", ORDER_DB - 1));
        assert_eq!(order_row(ORDER_DB - 1, "h2"), Some(3));
        clear_order_db();
    }

    #[pg_test]
    fn config_set_moves_the_thresholds() {
        clear_order_db();
        run_line("CONFIG SET hash-max-listpack-entries 2");
        run_line("HSET cs a 1 b 2");
        assert_eq!(order_encoding("cs"), "listpack");
        run_line("HSET cs c 3");
        assert_eq!(order_encoding("cs"), "hashtable");
        run_line("CONFIG SET hash-max-listpack-entries 512");
        clear_order_db();
    }
}

/// Required by `cargo pgrx test`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_redis'"]
    }
}
