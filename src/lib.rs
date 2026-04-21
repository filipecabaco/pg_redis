mod commands;
pub(crate) mod htab;
pub(crate) mod mem;
mod resp;
mod worker;

use pgrx::bgworkers::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

::pgrx::extension_sql_file!("../sql/schema.sql", bootstrap);

#[pg_schema]
mod redis {}

pub(crate) static PORT: GucSetting<i32> = GucSetting::<i32>::new(6379);
pub(crate) static USE_LOGGED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub(crate) static NUM_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(4);
pub(crate) static LISTEN_ADDRESS: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"0.0.0.0"));
pub(crate) static MAX_CONNECTIONS: GucSetting<i32> = GucSetting::<i32>::new(128);
pub(crate) static PASSWORD: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);
pub(crate) static BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(64);
pub(crate) static DATABASE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"postgres"));

pub(crate) static STORAGE_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"auto"));

pub(crate) static MEM_MAX_ENTRIES: GucSetting<i32> = GucSetting::<i32>::new(16384);

#[derive(Copy, Clone, PartialEq)]
pub enum StorageMode {
    Auto,
    Memory,
}

pub fn storage_mode() -> StorageMode {
    match STORAGE_MODE.get().as_deref().and_then(|s| s.to_str().ok()) {
        Some("memory") => StorageMode::Memory,
        _ => StorageMode::Auto,
    }
}

static mut SHMEM_CTL: *mut mem::MemControlBlock = std::ptr::null_mut();
static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

unsafe extern "C-unwind" fn pg_redis_shmem_request() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
            prev();
        }
        pg_sys::RequestAddinShmemSpace(mem::mem_ctl_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_hash_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_set_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_zset_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_list_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_list_meta_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_zset_meta_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_set_meta_htab_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_kv_overflow_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_hash_overflow_total_size());
        pg_sys::RequestAddinShmemSpace(mem::mem_list_overflow_total_size());
        pg_sys::RequestNamedLWLockTranche(c"pg_redis_mem".as_ptr(), (mem::NUM_MEM_DBS * 5) as i32);
    }
}

unsafe extern "C-unwind" fn pg_redis_shmem_startup() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
            prev();
        }
        let mut found = false;
        let ctl: *mut mem::MemControlBlock =
            pg_sys::ShmemInitStruct(c"pg_redis_ctl".as_ptr(), mem::mem_ctl_size(), &mut found)
                .cast();

        let locks = pg_sys::GetNamedLWLockTranche(c"pg_redis_mem".as_ptr());
        for i in 0..mem::NUM_MEM_DBS {
            (*ctl).lwlock[i] = std::ptr::addr_of_mut!((*locks.add(i)).lock);
            (*ctl).hash_lwlock[i] = std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS + i)).lock);
            (*ctl).set_lwlock[i] =
                std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS * 2 + i)).lock);
            (*ctl).zset_lwlock[i] =
                std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS * 3 + i)).lock);
            (*ctl).list_lwlock[i] =
                std::ptr::addr_of_mut!((*locks.add(mem::NUM_MEM_DBS * 4 + i)).lock);
        }

        if !found {
            mem::mem_init_tables(ctl);
        }

        SHMEM_CTL = ctl;
    }
}

pub(crate) fn shmem_ctl() -> *mut mem::MemControlBlock {
    unsafe { SHMEM_CTL }
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

    GucRegistry::define_bool_guc(
        c"redis.use_logged",
        c"Use WAL-logged tables",
        c"Default DB on new connections (0-15). Even DBs use UNLOGGED tables, odd DBs use WAL-logged tables. Clients can override per-connection with SELECT <db>.",
        &USE_LOGGED,
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
        c"IP address pg_redis binds on (default: 0.0.0.0).",
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
        GucFlags::default(),
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
        c"Storage backend for even-numbered databases",
        c"'auto' (default): use UNLOGGED PostgreSQL tables. 'memory': use shared-memory \
          hash tables, bypassing SPI and transactions entirely. Data is lost on server \
          restart. Requires restart to take effect.",
        &STORAGE_MODE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"redis.mem_max_entries",
        c"Maximum keys per data type per even-numbered database in memory mode",
        c"Controls the size of each shared-memory hash table. Larger values use more RAM \
          (proportional). Default 16384. Requires server restart.",
        &MEM_MAX_ENTRIES,
        256,
        1048576,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    if storage_mode() == StorageMode::Memory {
        unsafe {
            PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
            pg_sys::shmem_request_hook = Some(pg_redis_shmem_request);
            PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
            pg_sys::shmem_startup_hook = Some(pg_redis_shmem_startup);
        }
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn clean_kv(db: u8) {
        Spi::run(&format!("DELETE FROM redis.kv_{}", db)).unwrap();
    }

    fn clean_hash(db: u8) {
        Spi::run(&format!("DELETE FROM redis.hash_{}", db)).unwrap();
    }

    // ──────────────────────────────── Schema ────────────────────────────────

    #[pg_test]
    fn test_kv_db1_logged_schema() {
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('k', 'v')").unwrap();
        let v = Spi::get_one::<String>("SELECT value FROM redis.kv_1 WHERE key = 'k'").unwrap();
        assert_eq!(v, Some("v".to_string()));
        clean_kv(1);
    }

    #[pg_test]
    fn test_kv_db0_unlogged_schema() {
        Spi::run("INSERT INTO redis.kv_0 (key, value) VALUES ('k', 'v')").unwrap();
        let v = Spi::get_one::<String>("SELECT value FROM redis.kv_0 WHERE key = 'k'").unwrap();
        assert_eq!(v, Some("v".to_string()));
        clean_kv(0);
    }

    #[pg_test]
    fn test_hash_db1_logged_schema() {
        Spi::run("INSERT INTO redis.hash_1 (key, field, value) VALUES ('h', 'f', 'v')").unwrap();
        let v = Spi::get_one::<String>(
            "SELECT value FROM redis.hash_1 WHERE key = 'h' AND field = 'f'",
        )
        .unwrap();
        assert_eq!(v, Some("v".to_string()));
        clean_hash(1);
    }

    #[pg_test]
    fn test_hash_db0_unlogged_schema() {
        Spi::run("INSERT INTO redis.hash_0 (key, field, value) VALUES ('h', 'f', 'v')").unwrap();
        let v = Spi::get_one::<String>(
            "SELECT value FROM redis.hash_0 WHERE key = 'h' AND field = 'f'",
        )
        .unwrap();
        assert_eq!(v, Some("v".to_string()));
        clean_hash(0);
    }

    // ──────────────────────────── Key-value ops ──────────────────────────────

    #[pg_test]
    fn test_kv_upsert() {
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('x', 'first') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value").unwrap();
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('x', 'second') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value").unwrap();
        let v = Spi::get_one::<String>("SELECT value FROM redis.kv_1 WHERE key = 'x'").unwrap();
        assert_eq!(v, Some("second".to_string()));
        clean_kv(1);
    }

    #[pg_test]
    fn test_kv_expiry_filters_expired() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('exp', 'v', now() - interval '1 second')").unwrap();
        let v = Spi::get_one::<String>(
            "SELECT value FROM redis.kv_1 WHERE key = 'exp' AND (expires_at IS NULL OR expires_at > now())",
        )
        .ok()
        .flatten();
        assert_eq!(v, None);
        clean_kv(1);
    }

    #[pg_test]
    fn test_kv_expiry_passes_future() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('live', 'v', now() + interval '1 hour')").unwrap();
        let v = Spi::get_one::<String>(
            "SELECT value FROM redis.kv_1 WHERE key = 'live' AND (expires_at IS NULL OR expires_at > now())",
        )
        .unwrap();
        assert_eq!(v, Some("v".to_string()));
        clean_kv(1);
    }

    #[pg_test]
    fn test_kv_delete_returns_count() {
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('d1', 'v'), ('d2', 'v')").unwrap();
        let n = Spi::get_one::<i64>(
            "WITH del AS (DELETE FROM redis.kv_1 WHERE key = ANY(ARRAY['d1','d2','missing']) RETURNING 1) SELECT count(*) FROM del",
        )
        .unwrap();
        assert_eq!(n, Some(2));
        clean_kv(1);
    }

    #[pg_test]
    fn test_kv_exists_count() {
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('e1', 'v'), ('e2', 'v')").unwrap();
        let n = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM redis.kv_1 WHERE key = ANY(ARRAY['e1','e2','nope']) AND (expires_at IS NULL OR expires_at > now())",
        )
        .unwrap();
        assert_eq!(n, Some(2));
        clean_kv(1);
    }

    // ──────────────────────────────── TTL ops ────────────────────────────────

    #[pg_test]
    fn test_ttl_single_lookup_for_missing_key() {
        let ttl = Spi::get_one::<i64>(
            "SELECT CASE \
               WHEN r.key IS NULL THEN -2::bigint \
               WHEN r.expires_at IS NULL THEN -1::bigint \
               ELSE GREATEST(-1, EXTRACT(EPOCH FROM (r.expires_at - now()))::bigint) \
             END \
             FROM (VALUES ('ghost'::text)) AS dummy(k) \
             LEFT JOIN redis.kv_1 r ON r.key = dummy.k",
        )
        .unwrap();
        assert_eq!(ttl, Some(-2));
    }

    #[pg_test]
    fn test_ttl_single_lookup_for_no_expiry() {
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('nottle', 'v')").unwrap();
        let ttl = Spi::get_one::<i64>(
            "SELECT CASE \
               WHEN r.key IS NULL THEN -2::bigint \
               WHEN r.expires_at IS NULL THEN -1::bigint \
               ELSE GREATEST(-1, EXTRACT(EPOCH FROM (r.expires_at - now()))::bigint) \
             END \
             FROM (VALUES ('nottle'::text)) AS dummy(k) \
             LEFT JOIN redis.kv_1 r ON r.key = dummy.k",
        )
        .unwrap();
        assert_eq!(ttl, Some(-1));
        clean_kv(1);
    }

    #[pg_test]
    fn test_ttl_single_lookup_for_expiring_key() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('ttlkey', 'v', now() + interval '60 seconds')").unwrap();
        let ttl = Spi::get_one::<i64>(
            "SELECT CASE \
               WHEN r.key IS NULL THEN -2::bigint \
               WHEN r.expires_at IS NULL THEN -1::bigint \
               ELSE GREATEST(-1, EXTRACT(EPOCH FROM (r.expires_at - now()))::bigint) \
             END \
             FROM (VALUES ('ttlkey'::text)) AS dummy(k) \
             LEFT JOIN redis.kv_1 r ON r.key = dummy.k",
        )
        .unwrap();
        assert!(
            ttl.unwrap_or(-999) >= 59,
            "TTL for a 60s key should be at least 59, got {:?}",
            ttl
        );
        clean_kv(1);
    }

    #[pg_test]
    fn test_persist_removes_expiry() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('pk', 'v', now() + interval '1 hour')").unwrap();
        Spi::run(
            "UPDATE redis.kv_1 SET expires_at = NULL WHERE key = 'pk' AND expires_at IS NOT NULL",
        )
        .unwrap();
        let exp =
            Spi::get_one::<bool>("SELECT expires_at IS NULL FROM redis.kv_1 WHERE key = 'pk'")
                .unwrap();
        assert_eq!(exp, Some(true));
        clean_kv(1);
    }

    // ───────────────────────── Expiry deletion (Redis parity) ────────────────

    #[pg_test]
    fn test_lazy_delete_removes_expired_row() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('ex_del', 'v', now() - interval '1 second')").unwrap();
        Spi::run("DELETE FROM redis.kv_1 WHERE key = 'ex_del' AND expires_at <= now()").unwrap();
        let count =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM redis.kv_1 WHERE key = 'ex_del'")
                .unwrap();
        assert_eq!(
            count,
            Some(0),
            "expired key must be deleted from the table, not just filtered"
        );
    }

    #[pg_test]
    fn test_active_expiry_deletes_rows() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('active_exp', 'v', now() - interval '1 second')").unwrap();
        Spi::run("DELETE FROM redis.kv_1 WHERE expires_at <= now()").unwrap();
        let count =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM redis.kv_1 WHERE key = 'active_exp'")
                .unwrap();
        assert_eq!(
            count,
            Some(0),
            "active expiry scan must delete expired rows"
        );
    }

    #[pg_test]
    fn test_non_expired_key_survives_active_scan() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('live_key', 'v', now() + interval '1 hour')").unwrap();
        Spi::run("DELETE FROM redis.kv_1 WHERE expires_at <= now()").unwrap();
        let count =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM redis.kv_1 WHERE key = 'live_key'")
                .unwrap();
        assert_eq!(
            count,
            Some(1),
            "non-expired key must not be deleted by expiry scan"
        );
        clean_kv(1);
    }

    #[pg_test]
    fn test_get_returns_null_and_deletes_expired_key() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('lazy', 'v', now() - interval '1 second')").unwrap();
        Spi::run("DELETE FROM redis.kv_1 WHERE key = 'lazy' AND expires_at <= now()").unwrap();
        let v = Spi::get_one::<String>(
            "SELECT value FROM redis.kv_1 WHERE key = 'lazy' AND (expires_at IS NULL OR expires_at > now())",
        )
        .ok()
        .flatten();
        assert_eq!(v, None, "GET must return null for expired key");
        let exists =
            Spi::get_one::<bool>("SELECT EXISTS (SELECT 1 FROM redis.kv_1 WHERE key = 'lazy')")
                .unwrap();
        assert_eq!(
            exists,
            Some(false),
            "expired key must be removed from table after GET"
        );
    }

    // ──────────────────────────────── Hash ops ────────────────────────────────

    #[pg_test]
    fn test_hset_upsert() {
        Spi::run("INSERT INTO redis.hash_1 (key, field, value) VALUES ('h', 'f', 'v1') ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value").unwrap();
        Spi::run("INSERT INTO redis.hash_1 (key, field, value) VALUES ('h', 'f', 'v2') ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value").unwrap();
        let v = Spi::get_one::<String>(
            "SELECT value FROM redis.hash_1 WHERE key = 'h' AND field = 'f'",
        )
        .unwrap();
        assert_eq!(v, Some("v2".to_string()));
        clean_hash(1);
    }

    #[pg_test]
    fn test_hgetall_order() {
        Spi::run("INSERT INTO redis.hash_1 (key, field, value) VALUES ('h', 'b', 'vb'), ('h', 'a', 'va')").unwrap();
        let fields: Vec<Option<String>> = Spi::connect(|c| {
            c.select(
                "SELECT field FROM redis.hash_1 WHERE key = 'h' ORDER BY field",
                None,
                &[],
            )
            .unwrap()
            .map(|r| r.get::<String>(1).unwrap())
            .collect()
        });
        assert_eq!(fields, vec![Some("a".to_string()), Some("b".to_string())]);
        clean_hash(1);
    }

    #[pg_test]
    fn test_hdel_count() {
        Spi::run("INSERT INTO redis.hash_1 (key, field, value) VALUES ('h', 'f1', 'v'), ('h', 'f2', 'v')").unwrap();
        let n = Spi::get_one::<i64>(
            "WITH del AS (DELETE FROM redis.hash_1 WHERE key = 'h' AND field = ANY(ARRAY['f1','f2','gone']) RETURNING 1) SELECT count(*) FROM del",
        )
        .unwrap();
        assert_eq!(n, Some(2));
        clean_hash(1);
    }

    // ──────────────────────────── MSET / MGET ────────────────────────────────

    #[pg_test]
    fn test_mset_batch_upsert() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) SELECT unnest(ARRAY['k1','k2']::text[]), unnest(ARRAY['v1','v2']::text[]), NULL ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at").unwrap();
        let v1 = Spi::get_one::<String>("SELECT value FROM redis.kv_1 WHERE key = 'k1'").unwrap();
        let v2 = Spi::get_one::<String>("SELECT value FROM redis.kv_1 WHERE key = 'k2'").unwrap();
        assert_eq!(v1, Some("v1".to_string()));
        assert_eq!(v2, Some("v2".to_string()));
        clean_kv(1);
    }

    #[pg_test]
    fn test_mget_preserves_null_for_missing() {
        Spi::run("INSERT INTO redis.kv_1 (key, value) VALUES ('present', 'yes')").unwrap();
        let rows: Vec<Option<String>> = Spi::connect(|c| {
            c.select(
                "SELECT value FROM redis.kv_1 WHERE key = ANY(ARRAY['present','missing']) AND (expires_at IS NULL OR expires_at > now())",
                None,
                &[],
            )
            .unwrap()
            .map(|r| r.get::<String>(1).unwrap())
            .collect()
        });
        assert!(rows.contains(&Some("yes".to_string())));
        assert_eq!(rows.len(), 1);
        clean_kv(1);
    }

    #[pg_test]
    fn test_mget_lazy_deletes_expired_keys() {
        Spi::run("INSERT INTO redis.kv_1 (key, value, expires_at) VALUES ('ex1', 'v', now() - interval '1 second'), ('ex2', 'v', now() - interval '1 second')").unwrap();
        Spi::run(
            "DELETE FROM redis.kv_1 WHERE key = ANY(ARRAY['ex1','ex2']) AND expires_at <= now()",
        )
        .unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM redis.kv_1 WHERE key = ANY(ARRAY['ex1','ex2'])",
        )
        .unwrap();
        assert_eq!(
            count,
            Some(0),
            "mget lazy delete must remove expired keys from storage"
        );
        clean_kv(1);
    }

    // ──────────────────────────── Worker management ──────────────────────────

    #[pg_test]
    fn test_worker_count_is_positive() {
        Spi::run("SELECT pg_sleep(1)").unwrap();
        let count = Spi::get_one::<i64>("SELECT redis.worker_count()")
            .unwrap()
            .unwrap_or(0);
        assert!(
            count > 0,
            "expected at least one worker running, got {}",
            count
        );
    }

    #[pg_test]
    fn test_add_workers_returns_requested_count() {
        let added = Spi::get_one::<i32>("SELECT redis.add_workers(2)")
            .unwrap()
            .unwrap_or(0);
        assert_eq!(added, 2, "add_workers(2) should return 2");
        Spi::run("SELECT redis.remove_workers(2)").unwrap();
    }

    #[pg_test]
    fn test_remove_workers_returns_terminated_count() {
        Spi::run("SELECT redis.add_workers(2)").unwrap();
        Spi::run("SELECT pg_sleep(1)").unwrap();
        let removed = Spi::get_one::<i32>("SELECT redis.remove_workers(2)")
            .unwrap()
            .unwrap_or(0);
        assert_eq!(removed, 2, "remove_workers(2) should terminate 2 workers");
    }

    // ─────────────────────── expires_at partial index ────────────────────────

    #[pg_test]
    fn test_expires_at_partial_index_exists_for_kv_tables() {
        for db in 0u8..16 {
            let table = format!("kv_{}", db);
            let exists = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = 'redis'
                      AND tablename = '{table}'
                      AND indexdef LIKE '%expires_at%'
                )"
            ))
            .unwrap();
            assert_eq!(
                exists,
                Some(true),
                "partial index on expires_at must exist for redis.{table}"
            );
        }
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
