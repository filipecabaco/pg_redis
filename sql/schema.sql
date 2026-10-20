CREATE SCHEMA IF NOT EXISTS redis;

-- Every one of the 16 Redis databases has WAL-logged tables. They hold its
-- data while its policy is `logged`, and stay empty while it is in memory
-- (docs/STORAGE.md). The defaults split the databases into two halves:
--
--   0-7   memory  — shared memory; these tables stay empty
--   8-15  logged  — WAL-logged, replicated, survives restart
--
-- `SELECT cache` is an alias for 0 and `SELECT durable` an alias for 8, so a
-- client never has to remember which half a number lands in.

CREATE         TABLE IF NOT EXISTS redis.kv_0  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_1  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_2  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_3  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_4  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_5  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_6  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_7  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_8  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_9  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_10 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_11 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_12 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_13 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_14 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);
CREATE         TABLE IF NOT EXISTS redis.kv_15 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL, expires_at TIMESTAMPTZ);

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

-- Hashes and sets come back in the order Redis gives them, which for a small
-- one is the order its fields or members were added (docs/STORAGE.md). `seq`
-- records that order: it is set when a row is inserted, from one sequence
-- for every table, and left alone when a field's value is updated.
CREATE SEQUENCE IF NOT EXISTS redis.seq;

CREATE         TABLE IF NOT EXISTS redis.hash_0  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_1  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_2  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_3  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_4  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_5  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_6  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_7  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_8  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_9  (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_10 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_11 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_12 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_13 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_14 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));
CREATE         TABLE IF NOT EXISTS redis.hash_15 (key BYTEA NOT NULL, field BYTEA NOT NULL, value BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, field));

-- Lists. `pos` is a signed BIGINT with gaps so LPUSH can decrement below
-- the current minimum and RPUSH can increment above the current maximum
-- without renumbering. LINSERT bisects gaps and renumbers when exhausted.
CREATE         TABLE IF NOT EXISTS redis.list_0  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_1  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_2  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_3  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_4  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_5  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_6  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_7  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_8  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_9  (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_10 (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_11 (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_12 (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_13 (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_14 (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));
CREATE         TABLE IF NOT EXISTS redis.list_15 (key BYTEA NOT NULL, pos BIGINT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY (key, pos));


-- Sets. PRIMARY KEY (key, member) covers both the (key=?) scan path used by
-- SMEMBERS/SCARD and the (key=?, member=?) membership test used by SISMEMBER,
-- so no standalone key index is needed.
CREATE         TABLE IF NOT EXISTS redis.set_0  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_1  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_2  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_3  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_4  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_5  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_6  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_7  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_8  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_9  (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_10 (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_11 (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_12 (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_13 (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_14 (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.set_15 (key BYTEA NOT NULL, member BYTEA NOT NULL, seq BIGINT NOT NULL DEFAULT nextval('redis.seq'), PRIMARY KEY (key, member));

CREATE INDEX IF NOT EXISTS hash_0_key_pattern_idx  ON redis.hash_0  (key);
CREATE INDEX IF NOT EXISTS hash_1_key_pattern_idx  ON redis.hash_1  (key);
CREATE INDEX IF NOT EXISTS hash_2_key_pattern_idx  ON redis.hash_2  (key);
CREATE INDEX IF NOT EXISTS hash_3_key_pattern_idx  ON redis.hash_3  (key);
CREATE INDEX IF NOT EXISTS hash_4_key_pattern_idx  ON redis.hash_4  (key);
CREATE INDEX IF NOT EXISTS hash_5_key_pattern_idx  ON redis.hash_5  (key);
CREATE INDEX IF NOT EXISTS hash_6_key_pattern_idx  ON redis.hash_6  (key);
CREATE INDEX IF NOT EXISTS hash_7_key_pattern_idx  ON redis.hash_7  (key);
CREATE INDEX IF NOT EXISTS hash_8_key_pattern_idx  ON redis.hash_8  (key);
CREATE INDEX IF NOT EXISTS hash_9_key_pattern_idx  ON redis.hash_9  (key);
CREATE INDEX IF NOT EXISTS hash_10_key_pattern_idx ON redis.hash_10 (key);
CREATE INDEX IF NOT EXISTS hash_11_key_pattern_idx ON redis.hash_11 (key);
CREATE INDEX IF NOT EXISTS hash_12_key_pattern_idx ON redis.hash_12 (key);
CREATE INDEX IF NOT EXISTS hash_13_key_pattern_idx ON redis.hash_13 (key);
CREATE INDEX IF NOT EXISTS hash_14_key_pattern_idx ON redis.hash_14 (key);
CREATE INDEX IF NOT EXISTS hash_15_key_pattern_idx ON redis.hash_15 (key);

CREATE INDEX IF NOT EXISTS set_0_key_pattern_idx   ON redis.set_0   (key);
CREATE INDEX IF NOT EXISTS set_1_key_pattern_idx   ON redis.set_1   (key);
CREATE INDEX IF NOT EXISTS set_2_key_pattern_idx   ON redis.set_2   (key);
CREATE INDEX IF NOT EXISTS set_3_key_pattern_idx   ON redis.set_3   (key);
CREATE INDEX IF NOT EXISTS set_4_key_pattern_idx   ON redis.set_4   (key);
CREATE INDEX IF NOT EXISTS set_5_key_pattern_idx   ON redis.set_5   (key);
CREATE INDEX IF NOT EXISTS set_6_key_pattern_idx   ON redis.set_6   (key);
CREATE INDEX IF NOT EXISTS set_7_key_pattern_idx   ON redis.set_7   (key);
CREATE INDEX IF NOT EXISTS set_8_key_pattern_idx   ON redis.set_8   (key);
CREATE INDEX IF NOT EXISTS set_9_key_pattern_idx   ON redis.set_9   (key);
CREATE INDEX IF NOT EXISTS set_10_key_pattern_idx  ON redis.set_10  (key);
CREATE INDEX IF NOT EXISTS set_11_key_pattern_idx  ON redis.set_11  (key);
CREATE INDEX IF NOT EXISTS set_12_key_pattern_idx  ON redis.set_12  (key);
CREATE INDEX IF NOT EXISTS set_13_key_pattern_idx  ON redis.set_13  (key);
CREATE INDEX IF NOT EXISTS set_14_key_pattern_idx  ON redis.set_14  (key);
CREATE INDEX IF NOT EXISTS set_15_key_pattern_idx  ON redis.set_15  (key);

-- Sorted sets. Scores are DOUBLE PRECISION to match Redis float64 semantics
-- (native +Infinity/-Infinity handling). The composite index on
-- (key, score, member) covers ZRANGE BYSCORE, ZRANGE BYLEX, ZRANK, ZCOUNT,
-- and ZLEXCOUNT; the PK (key, member) covers ZSCORE/ZREM membership lookup.
CREATE         TABLE IF NOT EXISTS redis.zset_0  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_1  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_2  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_3  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_4  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_5  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_6  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_7  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_8  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_9  (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_10 (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_11 (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_12 (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_13 (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_14 (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));
CREATE         TABLE IF NOT EXISTS redis.zset_15 (key BYTEA NOT NULL, member BYTEA NOT NULL, score DOUBLE PRECISION NOT NULL, PRIMARY KEY (key, member));

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

-- Streams. The whole stream — entries, consumer groups, pending lists — is one
-- serialised value per key (see src/stream.rs), loaded and stored back by each
-- X* command. Its expiry lives in expiry_N, as a collection's does.
CREATE         TABLE IF NOT EXISTS redis.stream_0  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_1  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_2  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_3  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_4  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_5  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_6  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_7  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_8  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_9  (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_10 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_11 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_12 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_13 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_14 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.stream_15 (key BYTEA PRIMARY KEY, value BYTEA NOT NULL);

-- Where a collection's expiry lives. Only the KV table has a column for one,
-- so without this a list, set, hash or sorted set could not carry a TTL at all
-- and every cache-with-an-expiry pattern over a collection kept its data
-- forever. A string keeps its own column: it is read on the GET path already.
CREATE         TABLE IF NOT EXISTS redis.expiry_0   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_1   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_2   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_3   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_4   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_5   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_6   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_7   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_8   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_9   (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_10  (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_11  (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_12  (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_13  (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_14  (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.expiry_15  (key BYTEA PRIMARY KEY, expires_at TIMESTAMPTZ NOT NULL);

CREATE INDEX IF NOT EXISTS expiry_0_expires_idx   ON redis.expiry_0   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_1_expires_idx   ON redis.expiry_1   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_2_expires_idx   ON redis.expiry_2   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_3_expires_idx   ON redis.expiry_3   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_4_expires_idx   ON redis.expiry_4   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_5_expires_idx   ON redis.expiry_5   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_6_expires_idx   ON redis.expiry_6   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_7_expires_idx   ON redis.expiry_7   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_8_expires_idx   ON redis.expiry_8   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_9_expires_idx   ON redis.expiry_9   (expires_at);
CREATE INDEX IF NOT EXISTS expiry_10_expires_idx  ON redis.expiry_10  (expires_at);
CREATE INDEX IF NOT EXISTS expiry_11_expires_idx  ON redis.expiry_11  (expires_at);
CREATE INDEX IF NOT EXISTS expiry_12_expires_idx  ON redis.expiry_12  (expires_at);
CREATE INDEX IF NOT EXISTS expiry_13_expires_idx  ON redis.expiry_13  (expires_at);
CREATE INDEX IF NOT EXISTS expiry_14_expires_idx  ON redis.expiry_14  (expires_at);
CREATE INDEX IF NOT EXISTS expiry_15_expires_idx  ON redis.expiry_15  (expires_at);

-- The encoding Redis would have given a hash, set or sorted set, for the keys
-- whose encoding their contents no longer show (docs/STORAGE.md). Redis
-- converts a collection that outgrows a listpack or intset and never converts
-- it back, which `OBJECT ENCODING` and the order of the elements both reveal.
-- A key with no row is in the encoding its contents imply: a listpack for a
-- hash or sorted set, an intset for a set of integers and a listpack for any
-- other set. A row records 1 listpack (a set that holds an integer), 3
-- hashtable (a hash or set) or 4 skiplist (a sorted set), and is written only
-- when a key changes encoding.
CREATE         TABLE IF NOT EXISTS redis.encoding_0  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_1  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_2  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_3  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_4  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_5  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_6  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_7  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_8  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_9  (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_10 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_11 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_12 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_13 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_14 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);
CREATE         TABLE IF NOT EXISTS redis.encoding_15 (key BYTEA PRIMARY KEY, encoding SMALLINT NOT NULL);

-- Lua scripting. The script cache is shared by every worker so that EVALSHA
-- finds a script whichever worker EVAL first saw it on; like Redis's cache it is
-- not data, so it is unlogged. Function libraries are data — Redis keeps them in
-- its RDB — so they are logged.
CREATE UNLOGGED TABLE IF NOT EXISTS redis.scripts (
    sha  TEXT PRIMARY KEY,
    body BYTEA NOT NULL
);
CREATE TABLE IF NOT EXISTS redis.functions (
    name TEXT PRIMARY KEY,
    code BYTEA NOT NULL
);

-- Storage policies (docs/STORAGE.md): the databases whose policy was set with
-- redis.set_policy. A database with no row keeps its default. from_policy is
-- set while its data is still moving from the policy it had, so a restart
-- carries the move on.
CREATE TABLE IF NOT EXISTS redis.db_policy (
    db          SMALLINT PRIMARY KEY CHECK (db BETWEEN 0 AND 15),
    policy      TEXT NOT NULL CHECK (policy IN ('memory', 'memory+snapshot', 'logged')),
    slot        SMALLINT CHECK (slot BETWEEN 0 AND 15),
    from_policy TEXT CHECK (from_policy IN ('memory', 'memory+snapshot', 'logged')),
    from_slot   SMALLINT CHECK (from_slot BETWEEN 0 AND 15)
);

-- memory+snapshot databases: each key's DUMP payload and its deadline in
-- microseconds since the epoch (0 for none), replaced whole at every
-- snapshot and reloaded into memory when the server starts.
CREATE TABLE IF NOT EXISTS redis.snapshot (
    db          SMALLINT NOT NULL,
    key         BYTEA NOT NULL,
    payload     BYTEA NOT NULL,
    deadline_us BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (db, key)
);
CREATE TABLE IF NOT EXISTS redis.snapshot_taken (
    db       SMALLINT PRIMARY KEY,
    taken_at TIMESTAMPTZ NOT NULL,
    keys     BIGINT NOT NULL
);

-- Pub/sub table routing: persists channel→table mappings across server restarts.
-- Loaded into shared memory by the first BGW on startup via CAS flag.
CREATE TABLE IF NOT EXISTS redis.pubsub_routes (
    channel TEXT PRIMARY KEY,
    schema  TEXT NOT NULL,
    tbl     TEXT NOT NULL
);

-- Helper to create a target table for pub/sub routing.
-- The table must have `channel BYTEA` and `payload BYTEA` columns to receive routed messages.
-- This one also has both as text (NULL where the bytes are not UTF-8), so a JSON payload is
-- `payload_text::jsonb`.
CREATE OR REPLACE FUNCTION redis.create_pubsub_table(p_schema text, p_tbl text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I.%I (
            id          BIGSERIAL PRIMARY KEY,
            channel     BYTEA NOT NULL,
            payload     BYTEA NOT NULL,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            channel_text TEXT GENERATED ALWAYS AS (redis.text(channel)) STORED,
            payload_text TEXT GENERATED ALWAYS AS (redis.text(payload)) STORED
        )',
        p_schema, p_tbl
    );
END;
$$;

-- pg_dump skips the tables an extension creates unless its script marks them
-- as holding user data, so without this a logical backup held none of the
-- data in `logged` databases, snapshots, policies or functions. Every logged
-- table in the schema is marked, and so is `redis.seq`, whose position the
-- order of hashes and sets depends on; `redis.scripts` is an unlogged cache
-- that Redis does not persist either.
DO $$
DECLARE
    t regclass;
BEGIN
    FOR t IN
        SELECT c.oid::regclass
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'redis' AND c.relkind IN ('r', 'S') AND c.relpersistence = 'p'
    LOOP
        PERFORM pg_catalog.pg_extension_config_dump(t, '');
    END LOOP;
END
$$;

-- Without preloading there is no Redis listener; say so to whoever creates
-- the extension instead of leaving them to find a closed port. A warning:
-- PostgreSQL hides notices while it runs an extension script.
DO $$
BEGIN
    IF NOT ('pg_redis' = ANY (string_to_array(
            replace(current_setting('shared_preload_libraries'), ' ', ''), ','))) THEN
        RAISE WARNING 'pg_redis is not in shared_preload_libraries, so the Redis listener is not running. Add it there and restart PostgreSQL.';
    END IF;
END
$$;
