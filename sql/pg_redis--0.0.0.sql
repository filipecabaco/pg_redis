\echo Use "CREATE EXTENSION pg_redis" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS redis;

CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_0  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_1  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_2  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_3  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_4  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_5  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_6  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_7  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_8  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_9  (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_10 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_11 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_12 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_13 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE UNLOGGED TABLE IF NOT EXISTS redis.kv_14 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_15 (key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at TIMESTAMPTZ);

-- Partial indexes on expires_at: used by the background expiry scan
-- (DELETE WHERE expires_at <= now()) and TTL lookups. Keys without an expiry
-- (the common case) are excluded, keeping the index small.
CREATE INDEX IF NOT EXISTS kv_0_expires_idx  ON redis.kv_0  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_1_expires_idx  ON redis.kv_1  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_2_expires_idx  ON redis.kv_2  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_3_expires_idx  ON redis.kv_3  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_4_expires_idx  ON redis.kv_4  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_5_expires_idx  ON redis.kv_5  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_6_expires_idx  ON redis.kv_6  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_7_expires_idx  ON redis.kv_7  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_8_expires_idx  ON redis.kv_8  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_9_expires_idx  ON redis.kv_9  (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_10_expires_idx ON redis.kv_10 (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_11_expires_idx ON redis.kv_11 (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_12_expires_idx ON redis.kv_12 (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_13_expires_idx ON redis.kv_13 (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_14_expires_idx ON redis.kv_14 (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS kv_15_expires_idx ON redis.kv_15 (expires_at) WHERE expires_at IS NOT NULL;

CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_0  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_1  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_2  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_3  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_4  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_5  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_6  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_7  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_8  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_9  (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_10 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_11 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_12 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_13 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.hash_14 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_15 (key TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, field));
