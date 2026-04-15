import { test, expect, describe, beforeAll, afterAll } from "bun:test";

const REDIS_HOST = process.env.REDIS_HOST ?? "localhost";
const REDIS_PORT = process.env.REDIS_PORT ?? "6379";
const REDIS_PASSWORD = process.env.REDIS_PASSWORD ?? "testpass";
const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/postgres";

const redisUrl = `redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}`;
const sql = new Bun.sql(DATABASE_URL);
let client: Bun.RedisClient;

beforeAll(() => {
  client = new Bun.RedisClient(redisUrl);
});

afterAll(async () => {
  client.close();
  await sql.end();
});

describe("Authentication", () => {
  test("PING before AUTH returns PONG (allowed)", async () => {
    const noAuthClient = new Bun.RedisClient(`redis://${REDIS_HOST}:${REDIS_PORT}`);
    try {
      expect(await noAuthClient.ping()).toBe("PONG");
    } finally {
      noAuthClient.close();
    }
  });

  test("GET before AUTH returns NOAUTH", async () => {
    const noAuthClient = new Bun.RedisClient(`redis://${REDIS_HOST}:${REDIS_PORT}`);
    try {
      await expect(noAuthClient.get("somekey")).rejects.toThrow(/NOAUTH/);
    } finally {
      noAuthClient.close();
    }
  });

  test("AUTH with wrong password returns WRONGPASS", async () => {
    const wrongClient = new Bun.RedisClient(
      `redis://:wrongpassword@${REDIS_HOST}:${REDIS_PORT}`
    );
    try {
      await expect(wrongClient.get("somekey")).rejects.toThrow(/WRONGPASS/);
    } finally {
      wrongClient.close();
    }
  });

  test("AUTH with correct password returns OK", async () => {
    expect(await client.send("AUTH", [REDIS_PASSWORD])).toBe("OK");
  });
});

describe("BGW SPI sanity", () => {
  test("SET reaches SPI without crash", async () => {
    expect(await client.set("bgw_sanity_key", "bgw_sanity_val")).toBe("OK");
  });

  test("GET round-trips through SPI", async () => {
    expect(await client.get("bgw_sanity_key")).toBe("bgw_sanity_val");
  });

  test("DEL cleans up sanity key", async () => {
    expect(await client.del("bgw_sanity_key")).toBe(1);
  });
});

describe("Connection", () => {
  test("PING", async () => {
    expect(await client.ping()).toBe("PONG");
  });

  test("PING with arg", async () => {
    expect(await client.ping("hello")).toBe("hello");
  });

  test("ECHO", async () => {
    expect(await client.send("ECHO", ["world"])).toBe("world");
  });

  test("CLIENT SETNAME", async () => {
    expect(await client.send("CLIENT", ["SETNAME", "myapp"])).toBe("OK");
  });
});

describe("Key-Value", () => {
  test("SET", async () => {
    expect(await client.set("mykey", "myvalue")).toBe("OK");
  });

  test("GET existing", async () => {
    expect(await client.get("mykey")).toBe("myvalue");
  });

  test("GET missing", async () => {
    expect(await client.get("nokey")).toBeNull();
  });

  test("SET with EX", async () => {
    expect(await client.set("ttlkey", "ttlval", "EX", 100)).toBe("OK");
  });

  test("TTL positive", async () => {
    expect(await client.ttl("ttlkey")).toBe(100);
  });

  test("PTTL positive", async () => {
    const pttl = await client.pttl("ttlkey");
    expect(pttl).toBeGreaterThanOrEqual(99000);
    expect(pttl).toBeLessThanOrEqual(100000);
  });

  test("SET overwrites", async () => {
    expect(await client.set("mykey", "newvalue")).toBe("OK");
  });

  test("GET after overwrite", async () => {
    expect(await client.get("mykey")).toBe("newvalue");
  });

  test("SETEX", async () => {
    expect(await client.send("SETEX", ["exkey", "50", "exval"])).toBe("OK");
  });

  test("GET SETEX key", async () => {
    expect(await client.get("exkey")).toBe("exval");
  });

  test("PSETEX", async () => {
    expect(await client.send("PSETEX", ["pexkey", "50000", "pexval"])).toBe("OK");
  });

  test("GET PSETEX key", async () => {
    expect(await client.get("pexkey")).toBe("pexval");
  });
});

describe("MSET / MGET", () => {
  test("MSET", async () => {
    expect(await client.send("MSET", ["mk1", "mv1", "mk2", "mv2", "mk3", "mv3"])).toBe("OK");
  });

  test("MGET all", async () => {
    expect(await client.mget("mk1", "mk2", "mk3")).toEqual(["mv1", "mv2", "mv3"]);
  });

  test("MGET with nil slot preserved", async () => {
    expect(await client.mget("mk1", "missing", "mk3")).toEqual(["mv1", null, "mv3"]);
  });
});

describe("DEL / EXISTS", () => {
  test("EXISTS present", async () => {
    expect(await client.send("EXISTS", ["mykey"])).toBe(1);
  });

  test("EXISTS missing", async () => {
    expect(await client.send("EXISTS", ["nokey"])).toBe(0);
  });

  test("EXISTS multi", async () => {
    expect(await client.send("EXISTS", ["mk1", "mk2", "missing"])).toBe(2);
  });

  test("DEL single", async () => {
    expect(await client.del("mk1")).toBe(1);
  });

  test("DEL missing", async () => {
    expect(await client.del("mk1")).toBe(0);
  });

  test("DEL multi", async () => {
    expect(await client.del("mk2", "mk3")).toBe(2);
  });
});

describe("Expiry", () => {
  test("SET for expiry", async () => {
    expect(await client.set("expkey", "expval")).toBe("OK");
  });

  test("EXPIRE", async () => {
    expect(await client.expire("expkey", 9999)).toBe(1);
  });

  test("TTL after EXPIRE", async () => {
    expect(await client.ttl("expkey")).toBe(9999);
  });

  test("PERSIST", async () => {
    expect(await client.persist("expkey")).toBe(1);
  });

  test("TTL after PERSIST", async () => {
    expect(await client.ttl("expkey")).toBe(-1);
  });

  test("TTL missing key", async () => {
    expect(await client.ttl("nonexistent")).toBe(-2);
  });

  test("PEXPIRE", async () => {
    expect(await client.send("PEXPIRE", ["exkey", "9999000"])).toBe(1);
  });

  test("PTTL after PEXPIRE", async () => {
    const pttl = await client.pttl("exkey");
    expect(pttl).toBeGreaterThanOrEqual(9990000);
    expect(pttl).toBeLessThanOrEqual(9999000);
  });

  test("EXPIRETIME on key without expiry", async () => {
    expect(await client.expiretime("mykey")).toBe(-1);
  });

  test("PEXPIRETIME on key without expiry", async () => {
    expect(await client.pexpiretime("mykey")).toBe(-1);
  });
});

describe("TTL expiry deletion", () => {
  test("SET key with 1s TTL", async () => {
    expect(await client.set("expiring_key", "expiring_val", "EX", 1)).toBe("OK");
  });

  test("GET before expiry returns value", async () => {
    expect(await client.get("expiring_key")).toBe("expiring_val");
  });

  test("GET after expiry returns nil", async () => {
    await Bun.sleep(2000);
    expect(await client.get("expiring_key")).toBeNull();
  });

  test("expired key is physically deleted after GET", async () => {
    const rows =
      await sql`SELECT count(*)::int AS cnt FROM redis.kv_1 WHERE key = 'expiring_key'`;
    expect(rows[0].cnt).toBe(0);
  });

  test("SET key for active scan", async () => {
    expect(await client.set("active_scan_key", "active_val", "EX", 1)).toBe("OK");
  });

  test("active scan deletes without GET", async () => {
    await Bun.sleep(3000);
    const rows =
      await sql`SELECT count(*)::int AS cnt FROM redis.kv_1 WHERE key = 'active_scan_key'`;
    expect(rows[0].cnt).toBe(0);
  });

  test("TTL on deleted key returns -2", async () => {
    expect(await client.ttl("expiring_key")).toBe(-2);
  });

  test("SET key with long TTL", async () => {
    expect(await client.set("long_ttl_key", "val", "EX", 9999)).toBe("OK");
  });

  test("GET long TTL key still present after 2s", async () => {
    await Bun.sleep(2000);
    expect(await client.get("long_ttl_key")).toBe("val");
  });

  test("long TTL key row still exists in DB", async () => {
    const rows =
      await sql`SELECT count(*)::int AS cnt FROM redis.kv_1 WHERE key = 'long_ttl_key'`;
    expect(rows[0].cnt).toBe(1);
  });

  test("DEL long TTL key", async () => {
    expect(await client.del("long_ttl_key")).toBe(1);
  });
});

describe("Hash", () => {
  test("HSET single", async () => {
    expect(await client.send("HSET", ["myhash", "f1", "v1"])).toBe(1);
  });

  test("HGET existing", async () => {
    expect(await client.send("HGET", ["myhash", "f1"])).toBe("v1");
  });

  test("HGET missing field", async () => {
    expect(await client.send("HGET", ["myhash", "nof"])).toBeNull();
  });

  test("HSET multiple", async () => {
    expect(await client.send("HSET", ["myhash", "f2", "v2", "f3", "v3"])).toBe(2);
  });

  test("HGETALL", async () => {
    expect(await client.hgetall("myhash")).toEqual(["f1", "v1", "f2", "v2", "f3", "v3"]);
  });

  test("HDEL single", async () => {
    expect(await client.send("HDEL", ["myhash", "f1"])).toBe(1);
  });

  test("HDEL missing", async () => {
    expect(await client.send("HDEL", ["myhash", "f1"])).toBe(0);
  });

  test("HGETALL after del", async () => {
    expect(await client.hgetall("myhash")).toEqual(["f2", "v2", "f3", "v3"]);
  });

  test("HDEL multi", async () => {
    expect(await client.send("HDEL", ["myhash", "f2", "f3"])).toBe(2);
  });
});

describe("SELECT isolation", () => {
  test("DB 0 and DB 1 are isolated", async () => {
    await client.send("SELECT", ["0"]);
    await client.del("ulkey");
    await client.send("SELECT", ["1"]);
    await client.del("ulkey");

    await client.send("SELECT", ["0"]);
    expect(await client.set("ulkey", "ulval")).toBe("OK");
    expect(await client.get("ulkey")).toBe("ulval");

    await client.send("SELECT", ["1"]);
    expect(await client.get("ulkey")).toBeNull();

    await client.send("SELECT", ["0"]);
  });

  test("full isolation matrix", async () => {
    await client.send("SELECT", ["0"]);
    await client.del("isolation_ul");
    await client.send("SELECT", ["1"]);
    await client.del("isolation_ul");
    await client.del("isolation_lg");

    await client.send("SELECT", ["0"]);
    expect(await client.set("isolation_ul", "isolation_val")).toBe("OK");
    expect(await client.get("isolation_ul")).toBe("isolation_val");

    await client.send("SELECT", ["1"]);
    expect(await client.get("isolation_ul")).toBeNull();
    expect(await client.set("isolation_lg", "isolation_val")).toBe("OK");
    expect(await client.get("isolation_lg")).toBe("isolation_val");

    await client.send("SELECT", ["0"]);
    expect(await client.get("isolation_lg")).toBeNull();

    await client.send("SELECT", ["1"]);
    expect(await client.del("isolation_lg")).toBe(1);

    await client.send("SELECT", ["0"]);
  });
});

describe("Worker management", () => {
  test("worker_count is positive", async () => {
    const [{ positive }] = await sql`SELECT redis.worker_count() > 0 AS positive`;
    expect(positive).toBe(true);
  });

  test("add_workers(2) returns 2 and count increases", async () => {
    const [{ cnt: before }] = await sql`SELECT redis.worker_count()::int AS cnt`;
    const [{ added }] = await sql`SELECT redis.add_workers(2) AS added`;
    expect(added).toBe(2);
    await Bun.sleep(1000);
    const [{ cnt: after }] = await sql`SELECT redis.worker_count()::int AS cnt`;
    expect(after).toBeGreaterThanOrEqual(before + 2);
  });

  test("remove_workers(2) returns 2 and count decreases", async () => {
    const [{ cnt: before }] = await sql`SELECT redis.worker_count()::int AS cnt`;
    const [{ removed }] = await sql`SELECT redis.remove_workers(2) AS removed`;
    expect(removed).toBe(2);
    await Bun.sleep(1000);
    const [{ cnt: after }] = await sql`SELECT redis.worker_count()::int AS cnt`;
    expect(after).toBeLessThanOrEqual(before);
  });
});

describe("Server", () => {
  test("INFO contains redis_version", async () => {
    const info = await client.send("INFO", []) as string;
    expect(info).toContain("redis_version");
  });

  test("COMMAND returns empty array", async () => {
    expect(await client.send("COMMAND", [])).toEqual([]);
  });
});
