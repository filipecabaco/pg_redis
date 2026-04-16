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

-- Lists. `pos` is a signed BIGINT with gaps so LPUSH can decrement below
-- the current minimum and RPUSH can increment above the current maximum
-- without renumbering. LINSERT bisects gaps and renumbers when exhausted.
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_0  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_1  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_2  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_3  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_4  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_5  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_6  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_7  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_8  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_9  (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_10 (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_11 (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_12 (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_13 (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.list_14 (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_15 (key TEXT NOT NULL, pos BIGINT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (key, pos));


-- Sets. PRIMARY KEY (key, member) covers both the (key=?) scan path used by
-- SMEMBERS/SCARD and the (key=?, member=?) membership test used by SISMEMBER,
-- so no standalone key index is needed.
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_0  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_1  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_2  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_3  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_4  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_5  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_6  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_7  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_8  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_9  (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_10 (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_11 (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_12 (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_13 (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.set_14 (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_15 (key TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY (key, member));

CREATE INDEX IF NOT EXISTS hash_0_key_pattern_idx  ON redis.hash_0  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_1_key_pattern_idx  ON redis.hash_1  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_2_key_pattern_idx  ON redis.hash_2  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_3_key_pattern_idx  ON redis.hash_3  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_4_key_pattern_idx  ON redis.hash_4  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_5_key_pattern_idx  ON redis.hash_5  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_6_key_pattern_idx  ON redis.hash_6  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_7_key_pattern_idx  ON redis.hash_7  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_8_key_pattern_idx  ON redis.hash_8  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_9_key_pattern_idx  ON redis.hash_9  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_10_key_pattern_idx ON redis.hash_10 (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_11_key_pattern_idx ON redis.hash_11 (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_12_key_pattern_idx ON redis.hash_12 (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_13_key_pattern_idx ON redis.hash_13 (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_14_key_pattern_idx ON redis.hash_14 (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS hash_15_key_pattern_idx ON redis.hash_15 (key text_pattern_ops);

CREATE INDEX IF NOT EXISTS set_0_key_pattern_idx   ON redis.set_0   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_1_key_pattern_idx   ON redis.set_1   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_2_key_pattern_idx   ON redis.set_2   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_3_key_pattern_idx   ON redis.set_3   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_4_key_pattern_idx   ON redis.set_4   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_5_key_pattern_idx   ON redis.set_5   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_6_key_pattern_idx   ON redis.set_6   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_7_key_pattern_idx   ON redis.set_7   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_8_key_pattern_idx   ON redis.set_8   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_9_key_pattern_idx   ON redis.set_9   (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_10_key_pattern_idx  ON redis.set_10  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_11_key_pattern_idx  ON redis.set_11  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_12_key_pattern_idx  ON redis.set_12  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_13_key_pattern_idx  ON redis.set_13  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_14_key_pattern_idx  ON redis.set_14  (key text_pattern_ops);
CREATE INDEX IF NOT EXISTS set_15_key_pattern_idx  ON redis.set_15  (key text_pattern_ops);

-- Sorted sets. Scores are DOUBLE PRECISION to match Redis float64 semantics
-- (native +Infinity/-Infinity handling). The composite index on
-- (key, score, member) covers ZRANGE BYSCORE, ZRANGE BYLEX, ZRANK, ZCOUNT,
-- and ZLEXCOUNT; the PK (key, member) covers ZSCORE/ZREM membership lookup.
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_0  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_1  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_2  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_3  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_4  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_5  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_6  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_7  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_8  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_9  (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_10 (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_11 (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_12 (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_13 (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE UNLOGGED TABLE IF NOT EXISTS redis.zset_14 (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_15 (key TEXT NOT NULL, member TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));

CREATE INDEX IF NOT EXISTS zset_0_key_score_member  ON redis.zset_0  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_1_key_score_member  ON redis.zset_1  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_2_key_score_member  ON redis.zset_2  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_3_key_score_member  ON redis.zset_3  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_4_key_score_member  ON redis.zset_4  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_5_key_score_member  ON redis.zset_5  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_6_key_score_member  ON redis.zset_6  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_7_key_score_member  ON redis.zset_7  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_8_key_score_member  ON redis.zset_8  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_9_key_score_member  ON redis.zset_9  (key, score, member);
CREATE INDEX IF NOT EXISTS zset_10_key_score_member ON redis.zset_10 (key, score, member);
CREATE INDEX IF NOT EXISTS zset_11_key_score_member ON redis.zset_11 (key, score, member);
CREATE INDEX IF NOT EXISTS zset_12_key_score_member ON redis.zset_12 (key, score, member);
CREATE INDEX IF NOT EXISTS zset_13_key_score_member ON redis.zset_13 (key, score, member);
CREATE INDEX IF NOT EXISTS zset_14_key_score_member ON redis.zset_14 (key, score, member);
CREATE INDEX IF NOT EXISTS zset_15_key_score_member ON redis.zset_15 (key, score, member);
