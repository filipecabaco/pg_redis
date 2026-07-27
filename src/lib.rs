mod commands;
pub(crate) mod htab;
pub(crate) mod mem;
pub(crate) mod pubsub;
mod resp;
pub(crate) mod watch;
pub(crate) mod worker;

use pgrx::bgworkers::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

::pgrx::extension_sql_file!("../sql/schema.sql", bootstrap);

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
pub(crate) static BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(64);
pub(crate) static DATABASE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"postgres"));

pub(crate) static STORAGE_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"memory"));

/// What memory mode does when a shared-memory table fills. Defaults to
/// `noeviction`, as Redis's own `maxmemory-policy` does.
pub(crate) static MAXMEMORY_POLICY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"noeviction"));

/// Boot value of `redis.mem_max_entries`, and the sizing memory mode assumes
/// when it is asked about its limits without shared memory attached.
///
/// Halved from 16384 when MAX_KEY_LEN grew to 512, to hold the shared-memory
/// request steady. Hashing the key out of the composite tables took that
/// request from ~376 MiB to ~202 MiB, and the chunked value pool took it to
/// ~72 MiB, so there is room to raise this again if you need the key capacity
/// more than the memory. It also sets the size of each pool, and with it the
/// largest value memory mode accepts — see `mem::max_total_val_len`.
pub(crate) const MEM_MAX_ENTRIES_DEFAULT: i32 = 8192;
pub(crate) static MEM_MAX_ENTRIES: GucSetting<i32> =
    GucSetting::<i32>::new(MEM_MAX_ENTRIES_DEFAULT);

#[derive(Copy, Clone, PartialEq)]
pub enum StorageMode {
    Postgres,
    Memory,
}

pub fn storage_mode() -> StorageMode {
    match STORAGE_MODE.get().as_deref().and_then(|s| s.to_str().ok()) {
        Some("memory") => StorageMode::Memory,
        _ => StorageMode::Postgres,
    }
}

static mut SHMEM_CTL: *mut mem::MemControlBlock = std::ptr::null_mut();
static mut PUBSUB_CTL: *mut pubsub::PubsubCtl = std::ptr::null_mut();
static mut PUBSUB_SLOTS: *mut pubsub::PubsubSlot = std::ptr::null_mut();
static mut ROUTE_CTL: *mut pubsub::RouteCtl = std::ptr::null_mut();
static mut WATCH_CTL: *mut watch::WatchCtl = std::ptr::null_mut();
static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

unsafe extern "C-unwind" fn pg_redis_shmem_request() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
            prev();
        }
        // Pub/sub shared memory — always allocated.
        // Uses an AtomicU8 spinlock (no pg_sys LWLock needed), so no LWLock tranche required.
        pg_sys::RequestAddinShmemSpace(pubsub::pubsub_ctl_size());
        pg_sys::RequestAddinShmemSpace(pubsub::pubsub_slots_size());
        pg_sys::RequestAddinShmemSpace(pubsub::route_ctl_size());
        // WATCH version counters — needed in both storage modes.
        pg_sys::RequestAddinShmemSpace(watch::watch_ctl_size());

        // Memory-mode shared memory — only when storage_mode = 'memory'.
        if storage_mode() == StorageMode::Memory {
            pg_sys::RequestAddinShmemSpace(mem::mem_ctl_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_hash_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_set_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_zset_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_list_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_list_meta_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_zset_meta_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_set_meta_htab_total_size());
            pg_sys::RequestAddinShmemSpace(mem::mem_val_pool_total_size());
            pg_sys::RequestNamedLWLockTranche(
                c"pg_redis_mem".as_ptr(),
                (mem::NUM_MEM_DBS * 5) as i32,
            );
        }
    }
}

unsafe extern "C-unwind" fn pg_redis_shmem_startup() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
            prev();
        }

        // Always init pub/sub ctl + slots.
        let mut found = false;
        PUBSUB_CTL = pg_sys::ShmemInitStruct(
            c"pg_redis_pubsub_ctl".as_ptr(),
            pubsub::pubsub_ctl_size(),
            &mut found,
        )
        .cast();
        PUBSUB_SLOTS = pg_sys::ShmemInitStruct(
            c"pg_redis_pubsub_slots".as_ptr(),
            pubsub::pubsub_slots_size(),
            &mut found,
        )
        .cast();

        ROUTE_CTL = pg_sys::ShmemInitStruct(
            c"pg_redis_route_ctl".as_ptr(),
            pubsub::route_ctl_size(),
            &mut found,
        )
        .cast();
        if !found {
            std::ptr::write_bytes(ROUTE_CTL, 0, 1);
        }

        let mut watch_found = false;
        WATCH_CTL = pg_sys::ShmemInitStruct(
            c"pg_redis_watch_ctl".as_ptr(),
            watch::watch_ctl_size(),
            &mut watch_found,
        )
        .cast();
        if !watch_found {
            std::ptr::write_bytes(WATCH_CTL, 0, 1);
        }

        // Memory-mode init — conditional.
        if storage_mode() == StorageMode::Memory {
            let mut mem_found = false;
            let ctl: *mut mem::MemControlBlock = pg_sys::ShmemInitStruct(
                c"pg_redis_ctl".as_ptr(),
                mem::mem_ctl_size(),
                &mut mem_found,
            )
            .cast();

            let locks = pg_sys::GetNamedLWLockTranche(c"pg_redis_mem".as_ptr());
            for i in 0..mem::NUM_MEM_DBS {
                (*ctl).lwlock[i] = std::ptr::addr_of_mut!((*locks.add(i)).lock);
                (*ctl).hash_lwlock[i] =
                    std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS + i)).lock);
                (*ctl).set_lwlock[i] =
                    std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS * 2 + i)).lock);
                (*ctl).zset_lwlock[i] =
                    std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS * 3 + i)).lock);
                (*ctl).list_lwlock[i] =
                    std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS * 4 + i)).lock);
            }

            if !mem_found {
                mem::mem_init_tables(ctl);
            }

            SHMEM_CTL = ctl;
        }
    }
}

pub(crate) fn shmem_ctl() -> *mut mem::MemControlBlock {
    unsafe { SHMEM_CTL }
}

/// Returns `Some((ctl, slots))` when pub/sub shared memory is initialised, `None` otherwise.
pub(crate) fn pubsub_state() -> Option<(*mut pubsub::PubsubCtl, *mut pubsub::PubsubSlot)> {
    let ctl = unsafe { PUBSUB_CTL };
    let slots = unsafe { PUBSUB_SLOTS };
    if ctl.is_null() || slots.is_null() {
        None
    } else {
        Some((ctl, slots))
    }
}

pub(crate) fn route_state() -> Option<*mut pubsub::RouteCtl> {
    let ctl = unsafe { ROUTE_CTL };
    if ctl.is_null() { None } else { Some(ctl) }
}

/// `Some(ctl)` once WATCH's shared-memory counters exist, `None` when the
/// extension was not loaded via `shared_preload_libraries`.
pub(crate) fn watch_state() -> Option<*mut watch::WatchCtl> {
    let ctl = unsafe { WATCH_CTL };
    if ctl.is_null() { None } else { Some(ctl) }
}

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_int_guc(
        c"redis.port",
        c"TCP port for the Redis protocol listener",
        c"Port number pg_redis listens on for Redis RESP2 connections (default: 6379)",
        &PORT,
        1024,
        65535,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.default_db",
        c"Database new connections start on",
        c"Databases 0-7 are ephemeral (shared memory, or UNLOGGED tables when \
          redis.storage_mode = 'auto'); 8-15 are WAL-logged and survive restart. \
          Default: 0, matching Redis. Clients override per-connection with SELECT, \
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
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.listen_address",
        c"TCP bind address for the Redis listener",
        c"IP address pg_redis binds on (default: 127.0.0.1). The Redis port does not \
          go through PostgreSQL authentication, so widening this exposes every redis.* \
          table to anything that can reach the port. Set redis.password as well.",
        &LISTEN_ADDRESS,
        GucContext::Suset,
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
        c"Used when the extension is loaded via shared_preload_libraries, where no \
          database is selected at startup. Workers connect to this database by name. \
          Ignored when loaded via CREATE EXTENSION (workers use that database's OID). \
          Default: 'postgres'.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.storage_mode",
        c"Storage backend for the ephemeral databases (0-7)",
        c"'auto': use UNLOGGED PostgreSQL tables. 'memory' (default): use shared-memory \
          hash tables, bypassing SPI and transactions entirely. Data is lost on server \
          restart. Requires restart to take effect.",
        &STORAGE_MODE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"redis.maxmemory_policy",
        c"What memory mode does when a shared-memory table is full",
        c"'noeviction' (default, as in Redis): refuse the write with an OOM error. \
          'allkeys-random': evict entries to make room, collections whole. \
          'volatile-ttl': evict only keys that carry an expiry, soonest first. \
          Expired keys are always reclaimed first, whatever the policy.",
        &MAXMEMORY_POLICY,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.mem_max_entries",
        c"Maximum keys per data type per ephemeral database in memory mode",
        c"Controls the size of each shared-memory hash table. Larger values use more RAM \
          (proportional). Default 8192. Requires server restart.",
        &MEM_MAX_ENTRIES,
        256,
        1048576,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(pg_redis_shmem_request);
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(pg_redis_shmem_startup);
    }

    let my_db: pg_sys::Oid = unsafe { pg_sys::MyDatabaseId };
    let db_oid_datum = pg_sys::Datum::from(my_db);
    let n = NUM_WORKERS.get() as usize;
    for idx in 0..n {
        BackgroundWorkerBuilder::new(&format!("pg_redis worker {}", idx))
            .set_function("pg_redis_worker_main")
            .set_library("pg_redis")
            .set_argument(Some(db_oid_datum))
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .load();
    }
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_redis_worker_main(arg: pg_sys::Datum) {
    worker::worker_main(arg);
}

/// Add n additional pg_redis workers dynamically (no server restart required).
/// Dynamic workers will not restart after being terminated.
#[pg_extern(schema = "redis")]
fn add_workers(n: i32) -> i32 {
    if !unsafe { pg_sys::superuser() } {
        error!("permission denied: superuser required");
    }
    let schema_exists =
        Spi::get_one::<bool>("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'redis')")
            .ok()
            .flatten()
            .unwrap_or(false);
    if !schema_exists {
        error!("pg_redis is not installed in the current database");
    }
    let my_db: pg_sys::Oid = unsafe { pg_sys::MyDatabaseId };
    let db_oid_datum = pg_sys::Datum::from(my_db);
    let mut added = 0i32;
    for _ in 0..n.max(0) {
        match BackgroundWorkerBuilder::new("pg_redis worker")
            .set_function("pg_redis_worker_main")
            .set_library("pg_redis")
            .set_argument(Some(db_oid_datum))
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(None)
            .load_dynamic()
        {
            Ok(_) => added += 1,
            Err(_) => break,
        }
    }
    added
}

/// Terminate n pg_redis workers (newest first). Dynamic workers will not restart;
/// startup workers will restart after ~5 seconds. To permanently reduce the startup
/// pool, lower redis.workers and restart the server.
#[pg_extern(schema = "redis")]
fn remove_workers(n: i32) -> i32 {
    if !unsafe { pg_sys::superuser() } {
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
    if !unsafe { pg_sys::superuser() } {
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
        unsafe {
            crate::pubsub::route_add(ctl, channel.as_bytes(), schema.as_bytes(), tbl.as_bytes());
        }
    }
}

/// Remove the PUBLISH→table route for `channel`.
#[pg_extern(schema = "redis")]
fn unroute_publish(channel: &str) {
    if !unsafe { pg_sys::superuser() } {
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
        unsafe {
            crate::pubsub::route_remove(ctl, channel.as_bytes());
        }
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

    /// Clear a database's string and hash tables between assertions.
    ///
    /// DELETE rather than TRUNCATE on purpose: pgrx runs these tests against one
    /// shared server, and TRUNCATE's ACCESS EXCLUSIVE lock deadlocks against
    /// whichever other test is reading the same table.
    fn truncate(db: usize) {
        for tbl in ["kv", "hash"] {
            Spi::run(&format!("DELETE FROM redis.{tbl}_{db}")).unwrap();
        }
    }

    // ─────────────────────────── Schema contract ────────────────────────────

    /// Every db must exist with the columns and constraints the generated SQL
    /// assumes — including the UNLOGGED/logged split on even/odd databases.
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
            let ephemeral = crate::commands::is_ephemeral(db as u8);
            assert_eq!(
                persistence.as_deref(),
                Some(if ephemeral { "u" } else { "p" }),
                "redis.kv_{db} should be {}",
                if ephemeral { "UNLOGGED" } else { "logged" }
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
            // No expiry set, so TTL/PTTL report -1 and a missing key reports -2.
            assert_eq!(query_one::<i64>(&sql::TTL[db], &[b"k"]), Some(-1));
            assert_eq!(query_one::<i64>(&sql::PTTL[db], &[b"k"]), Some(-1));
            assert_eq!(query_one::<i64>(&sql::TTL[db], &[b"absent"]), Some(-2));
            assert_eq!(query_one::<i64>(&sql::EXPIRETIME[db], &[b"k"]), Some(-1));
            assert_eq!(query_one::<i64>(&sql::PEXPIRETIME[db], &[b"k"]), Some(-1));

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

            // INCR upserts and returns the new value; a second call increments.
            assert_eq!(query_one::<i64>(&sql::INCR[db], &[b"n"]), Some(1));
            assert_eq!(query_one::<i64>(&sql::INCR[db], &[b"n"]), Some(2));
            assert_eq!(query_one::<i64>(&sql::DECR[db], &[b"n"]), Some(1));

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
            query_one::<i64>(&sql::TTL[1], &[b"live"]).unwrap_or(0) >= 3599,
            "TTL should report the remaining hour"
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

    /// Under `shared_preload_libraries` a worker connects to `redis.database`,
    /// which usually has no `CREATE EXTENSION` and therefore no `redis.kv_*`.
    /// The expiry sweep must notice that instead of raising `relation does not
    /// exist`, which killed the worker and left it restarting once a second
    /// forever. The guard also has to re-enable itself once the tables appear.
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
    /// nothing else. This is the guarantee the per-command SAVEPOINT statements
    /// used to provide and that `in_subtransaction` now provides directly.
    #[pg_test]
    fn a_failed_command_in_a_batch_rolls_back_only_itself() {
        use crate::commands::Response;

        Spi::run("CREATE TEMP TABLE batch_iso (n int)").unwrap();

        // Committed: the command reported success.
        let ok = unsafe {
            crate::worker::in_subtransaction(|| {
                Spi::run("INSERT INTO batch_iso VALUES (1)").unwrap();
                Response::Ok
            })
        };
        assert!(matches!(ok, Response::Ok));

        // Rolled back: the command reported an error, so its write is discarded.
        let failed = unsafe {
            crate::worker::in_subtransaction(|| {
                Spi::run("INSERT INTO batch_iso VALUES (2)").unwrap();
                Response::Error("boom".to_string())
            })
        };
        assert!(matches!(failed, Response::Error(_)));

        // Committed again: the surrounding transaction survived the rollback.
        unsafe {
            crate::worker::in_subtransaction(|| {
                Spi::run("INSERT INTO batch_iso VALUES (3)").unwrap();
                Response::Ok
            })
        };

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

    // ──────────────────────────── WATCH counters ─────────────────────────────

    /// The counters live in shared memory precisely so a WATCH taken on one
    /// worker sees a write dispatched through another. Exercising them directly
    /// is the closest a pg_test can get to that cross-process guarantee.
    #[pg_test]
    fn watch_counters_are_visible_through_shared_memory() {
        let ctl = crate::watch_state().expect("watch shmem must be initialised");

        let before = unsafe { crate::watch::version(ctl, 0, b"k") };
        unsafe { crate::watch::bump(ctl, 0, b"k") };
        let after = unsafe { crate::watch::version(ctl, 0, b"k") };
        assert_ne!(before, after, "a write must invalidate a prior WATCH");

        // An unrelated key must not be disturbed by that write. Slot collisions
        // are possible by design, so this asserts on a key known not to collide.
        let other = (0..64u32)
            .map(|i| format!("other-{i}"))
            .find(|k| {
                let v = unsafe { crate::watch::version(ctl, 0, k.as_bytes()) };
                unsafe { crate::watch::bump(ctl, 0, b"k") };
                v == unsafe { crate::watch::version(ctl, 0, k.as_bytes()) }
            })
            .expect("some key must not share a slot with \"k\"");
        let untouched = unsafe { crate::watch::version(ctl, 0, other.as_bytes()) };
        unsafe { crate::watch::bump(ctl, 0, b"k") };
        assert_eq!(
            untouched,
            unsafe { crate::watch::version(ctl, 0, other.as_bytes()) },
            "writing one key must not invalidate a WATCH on an unrelated key"
        );

        // Same key name, different database: tracked independently.
        let db1 = unsafe { crate::watch::version(ctl, 1, b"k") };
        unsafe { crate::watch::bump(ctl, 0, b"k") };
        assert_eq!(db1, unsafe { crate::watch::version(ctl, 1, b"k") });
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
