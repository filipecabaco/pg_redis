import { afterAll, beforeAll, describe, expect, test } from "bun:test";

const REDIS_HOST = process.env.REDIS_HOST ?? "localhost";
const REDIS_PORT = process.env.REDIS_PORT ?? "6379";
const REDIS_PASSWORD = process.env.REDIS_PASSWORD ?? "testpass";
const DATABASE_URL =
	process.env.DATABASE_URL ??
	"postgres://postgres:postgres@localhost:5432/postgres";

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
		const noAuthClient = new Bun.RedisClient(
			`redis://${REDIS_HOST}:${REDIS_PORT}`,
		);
		try {
			expect(await noAuthClient.ping()).toBe("PONG");
		} finally {
			noAuthClient.close();
		}
	});

	test("GET before AUTH returns NOAUTH", async () => {
		const noAuthClient = new Bun.RedisClient(
			`redis://${REDIS_HOST}:${REDIS_PORT}`,
		);
		try {
			await expect(noAuthClient.get("somekey")).rejects.toThrow(/NOAUTH/);
		} finally {
			noAuthClient.close();
		}
	});

	test("AUTH with wrong password returns WRONGPASS", async () => {
		const wrongClient = new Bun.RedisClient(
			`redis://:wrongpassword@${REDIS_HOST}:${REDIS_PORT}`,
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
		expect(await client.send("PSETEX", ["pexkey", "50000", "pexval"])).toBe(
			"OK",
		);
	});

	test("GET PSETEX key", async () => {
		expect(await client.get("pexkey")).toBe("pexval");
	});
});

describe("MSET / MGET", () => {
	test("MSET", async () => {
		expect(
			await client.send("MSET", ["mk1", "mv1", "mk2", "mv2", "mk3", "mv3"]),
		).toBe("OK");
	});

	test("MGET all", async () => {
		expect(await client.mget("mk1", "mk2", "mk3")).toEqual([
			"mv1",
			"mv2",
			"mv3",
		]);
	});

	test("MGET with nil slot preserved", async () => {
		expect(await client.mget("mk1", "missing", "mk3")).toEqual([
			"mv1",
			null,
			"mv3",
		]);
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
		expect(await client.set("expiring_key", "expiring_val", "EX", 1)).toBe(
			"OK",
		);
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
		expect(await client.set("active_scan_key", "active_val", "EX", 1)).toBe(
			"OK",
		);
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
		expect(await client.send("HSET", ["myhash", "f2", "v2", "f3", "v3"])).toBe(
			2,
		);
	});

	test("HGETALL", async () => {
		expect(await client.hgetall("myhash")).toEqual([
			"f1",
			"v1",
			"f2",
			"v2",
			"f3",
			"v3",
		]);
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
		const [{ positive }] =
			await sql`SELECT redis.worker_count() > 0 AS positive`;
		expect(positive).toBe(true);
	});

	test("add_workers(2) returns 2 and count increases", async () => {
		const [{ cnt: before }] =
			await sql`SELECT redis.worker_count()::int AS cnt`;
		const [{ added }] = await sql`SELECT redis.add_workers(2) AS added`;
		expect(added).toBe(2);
		await Bun.sleep(1000);
		const [{ cnt: after }] = await sql`SELECT redis.worker_count()::int AS cnt`;
		expect(after).toBeGreaterThanOrEqual(before + 2);
	});

	test("remove_workers(2) returns 2 and count decreases", async () => {
		const [{ cnt: before }] =
			await sql`SELECT redis.worker_count()::int AS cnt`;
		const [{ removed }] = await sql`SELECT redis.remove_workers(2) AS removed`;
		expect(removed).toBe(2);
		await Bun.sleep(1000);
		const [{ cnt: after }] = await sql`SELECT redis.worker_count()::int AS cnt`;
		expect(after).toBeLessThanOrEqual(before);
	});
});

describe("Server", () => {
	test("INFO contains redis_version", async () => {
		const info = (await client.send("INFO", [])) as string;
		expect(info).toContain("redis_version");
	});

	test("COMMAND returns empty array", async () => {
		expect(await client.send("COMMAND", [])).toEqual([]);
	});
});

describe("SET flags", () => {
	test("NX sets when key absent", async () => {
		await client.send("DEL", ["nx_key"]);
		expect(await client.send("SET", ["nx_key", "v1", "NX"])).toBe("OK");
		expect(await client.get("nx_key")).toBe("v1");
	});

	test("NX returns nil when key present", async () => {
		await client.set("nx_key2", "existing");
		expect(await client.send("SET", ["nx_key2", "new", "NX"])).toBeNull();
		expect(await client.get("nx_key2")).toBe("existing");
	});

	test("XX updates when key present", async () => {
		await client.set("xx_key", "existing");
		expect(await client.send("SET", ["xx_key", "updated", "XX"])).toBe("OK");
		expect(await client.get("xx_key")).toBe("updated");
	});

	test("XX returns nil when key absent", async () => {
		await client.send("DEL", ["xx_missing"]);
		expect(await client.send("SET", ["xx_missing", "v", "XX"])).toBeNull();
		expect(await client.send("EXISTS", ["xx_missing"])).toBe(0);
	});

	test("GET returns nil when key absent and writes the value", async () => {
		await client.send("DEL", ["get_key"]);
		expect(await client.send("SET", ["get_key", "first", "GET"])).toBeNull();
		expect(await client.get("get_key")).toBe("first");
	});

	test("GET returns old value when key present and updates it", async () => {
		await client.set("get_key2", "old");
		expect(await client.send("SET", ["get_key2", "new", "GET"])).toBe("old");
		expect(await client.get("get_key2")).toBe("new");
	});

	test("NX + GET returns old value without overwriting", async () => {
		await client.set("nxget_key", "existing");
		expect(await client.send("SET", ["nxget_key", "new", "NX", "GET"])).toBe(
			"existing",
		);
		expect(await client.get("nxget_key")).toBe("existing");
	});

	test("NX + GET returns nil when key absent and writes value", async () => {
		await client.send("DEL", ["nxget_absent"]);
		expect(
			await client.send("SET", ["nxget_absent", "v", "NX", "GET"]),
		).toBeNull();
		expect(await client.get("nxget_absent")).toBe("v");
	});

	test("KEEPTTL preserves existing expires_at", async () => {
		await client.send("SET", ["keepttl_key", "v1", "EX", "1000"]);
		const ttlBefore = (await client.send("TTL", ["keepttl_key"])) as number;
		expect(ttlBefore).toBeGreaterThan(0);
		expect(await client.send("SET", ["keepttl_key", "v2", "KEEPTTL"])).toBe(
			"OK",
		);
		const ttlAfter = (await client.send("TTL", ["keepttl_key"])) as number;
		expect(ttlAfter).toBeGreaterThan(0);
		expect(await client.get("keepttl_key")).toBe("v2");
	});

	test("default SET clears existing TTL", async () => {
		await client.send("SET", ["clearttl_key", "v1", "EX", "1000"]);
		expect(await client.send("SET", ["clearttl_key", "v2"])).toBe("OK");
		expect(await client.send("TTL", ["clearttl_key"])).toBe(-1);
	});

	test("EXAT sets absolute expiry", async () => {
		const future = Math.floor(Date.now() / 1000) + 500;
		expect(
			await client.send("SET", ["exat_key", "v", "EXAT", String(future)]),
		).toBe("OK");
		const ttl = (await client.send("TTL", ["exat_key"])) as number;
		expect(ttl).toBeGreaterThan(0);
		expect(ttl).toBeLessThanOrEqual(500);
	});

	test("PXAT sets absolute expiry in ms", async () => {
		const futureMs = Date.now() + 500_000;
		expect(
			await client.send("SET", ["pxat_key", "v", "PXAT", String(futureMs)]),
		).toBe("OK");
		const pttl = (await client.send("PTTL", ["pxat_key"])) as number;
		expect(pttl).toBeGreaterThan(0);
		expect(pttl).toBeLessThanOrEqual(500_000);
	});

	test("NX + XX rejected as syntax error", async () => {
		await expect(client.send("SET", ["k", "v", "NX", "XX"])).rejects.toThrow();
	});

	test("KEEPTTL + EX rejected as syntax error", async () => {
		await expect(
			client.send("SET", ["k", "v", "EX", "10", "KEEPTTL"]),
		).rejects.toThrow();
	});
});

describe("List", () => {
	beforeAll(async () => {
		await client.send("DEL", [
			"qlist",
			"rlist",
			"ranged",
			"indexed",
			"setlist",
			"inslist",
			"remlist",
			"movesrc",
			"movedst",
			"poslist",
			"trimlist",
			"pushxlist",
			"pushxmissing",
		]);
	});

	test("RPUSH appends and returns new length", async () => {
		expect(await client.send("RPUSH", ["qlist", "a", "b", "c"])).toBe(3);
		expect(await client.send("LLEN", ["qlist"])).toBe(3);
	});

	test("LRANGE returns elements in insertion order", async () => {
		expect(await client.send("LRANGE", ["qlist", "0", "-1"])).toEqual([
			"a",
			"b",
			"c",
		]);
	});

	test("LPUSH prepends and reverses input order", async () => {
		expect(await client.send("LPUSH", ["rlist", "a", "b", "c"])).toBe(3);
		expect(await client.send("LRANGE", ["rlist", "0", "-1"])).toEqual([
			"c",
			"b",
			"a",
		]);
	});

	test("LPOP single returns head element", async () => {
		expect(await client.send("LPOP", ["qlist"])).toBe("a");
		expect(await client.send("LRANGE", ["qlist", "0", "-1"])).toEqual([
			"b",
			"c",
		]);
	});

	test("RPOP single returns tail element", async () => {
		expect(await client.send("RPUSH", ["qlist", "d"])).toBe(3);
		expect(await client.send("RPOP", ["qlist"])).toBe("d");
	});

	test("LPOP count returns array", async () => {
		await client.send("DEL", ["qlist"]);
		await client.send("RPUSH", ["qlist", "a", "b", "c", "d", "e"]);
		expect(await client.send("LPOP", ["qlist", "3"])).toEqual(["a", "b", "c"]);
	});

	test("LPOP on missing key returns nil", async () => {
		expect(await client.send("LPOP", ["never-existed"])).toBeNull();
	});

	test("LRANGE with negative indexes", async () => {
		await client.send("DEL", ["ranged"]);
		await client.send("RPUSH", ["ranged", "0", "1", "2", "3", "4"]);
		expect(await client.send("LRANGE", ["ranged", "-2", "-1"])).toEqual([
			"3",
			"4",
		]);
		expect(await client.send("LRANGE", ["ranged", "0", "-3"])).toEqual([
			"0",
			"1",
			"2",
		]);
	});

	test("LRANGE start > stop returns empty", async () => {
		expect(await client.send("LRANGE", ["ranged", "5", "1"])).toEqual([]);
	});

	test("LINDEX positive and negative", async () => {
		await client.send("DEL", ["indexed"]);
		await client.send("RPUSH", ["indexed", "x", "y", "z"]);
		expect(await client.send("LINDEX", ["indexed", "0"])).toBe("x");
		expect(await client.send("LINDEX", ["indexed", "-1"])).toBe("z");
		expect(await client.send("LINDEX", ["indexed", "10"])).toBeNull();
	});

	test("LSET updates element at index", async () => {
		await client.send("DEL", ["setlist"]);
		await client.send("RPUSH", ["setlist", "a", "b", "c"]);
		expect(await client.send("LSET", ["setlist", "1", "B"])).toBe("OK");
		expect(await client.send("LRANGE", ["setlist", "0", "-1"])).toEqual([
			"a",
			"B",
			"c",
		]);
	});

	test("LSET on out-of-range returns error", async () => {
		await expect(client.send("LSET", ["setlist", "99", "x"])).rejects.toThrow();
	});

	test("LINSERT BEFORE pivot", async () => {
		await client.send("DEL", ["inslist"]);
		await client.send("RPUSH", ["inslist", "a", "c", "e"]);
		expect(await client.send("LINSERT", ["inslist", "BEFORE", "c", "b"])).toBe(
			4,
		);
		expect(await client.send("LRANGE", ["inslist", "0", "-1"])).toEqual([
			"a",
			"b",
			"c",
			"e",
		]);
	});

	test("LINSERT AFTER pivot", async () => {
		expect(await client.send("LINSERT", ["inslist", "AFTER", "c", "d"])).toBe(
			5,
		);
		expect(await client.send("LRANGE", ["inslist", "0", "-1"])).toEqual([
			"a",
			"b",
			"c",
			"d",
			"e",
		]);
	});

	test("LINSERT missing pivot returns -1", async () => {
		expect(await client.send("LINSERT", ["inslist", "BEFORE", "Z", "x"])).toBe(
			-1,
		);
	});

	test("LINSERT on missing key returns 0", async () => {
		expect(
			await client.send("LINSERT", ["nokey-linsert", "BEFORE", "p", "v"]),
		).toBe(0);
	});

	test("LREM positive count removes from head", async () => {
		await client.send("DEL", ["remlist"]);
		await client.send("RPUSH", ["remlist", "a", "x", "a", "x", "a"]);
		expect(await client.send("LREM", ["remlist", "2", "a"])).toBe(2);
		expect(await client.send("LRANGE", ["remlist", "0", "-1"])).toEqual([
			"x",
			"x",
			"a",
		]);
	});

	test("LREM negative count removes from tail", async () => {
		await client.send("DEL", ["remlist"]);
		await client.send("RPUSH", ["remlist", "a", "b", "a", "b", "a"]);
		expect(await client.send("LREM", ["remlist", "-2", "a"])).toBe(2);
		expect(await client.send("LRANGE", ["remlist", "0", "-1"])).toEqual([
			"a",
			"b",
			"b",
		]);
	});

	test("LREM zero count removes all matches", async () => {
		await client.send("DEL", ["remlist"]);
		await client.send("RPUSH", ["remlist", "a", "x", "a", "x"]);
		expect(await client.send("LREM", ["remlist", "0", "a"])).toBe(2);
		expect(await client.send("LRANGE", ["remlist", "0", "-1"])).toEqual([
			"x",
			"x",
		]);
	});

	test("LMOVE atomically moves element between lists", async () => {
		await client.send("DEL", ["movesrc", "movedst"]);
		await client.send("RPUSH", ["movesrc", "a", "b", "c"]);
		expect(
			await client.send("LMOVE", ["movesrc", "movedst", "LEFT", "RIGHT"]),
		).toBe("a");
		expect(await client.send("LRANGE", ["movesrc", "0", "-1"])).toEqual([
			"b",
			"c",
		]);
		expect(await client.send("LRANGE", ["movedst", "0", "-1"])).toEqual(["a"]);
	});

	test("LMOVE RIGHT-LEFT pops tail of src and pushes to head of dst", async () => {
		expect(
			await client.send("LMOVE", ["movesrc", "movedst", "RIGHT", "LEFT"]),
		).toBe("c");
		expect(await client.send("LRANGE", ["movedst", "0", "-1"])).toEqual([
			"c",
			"a",
		]);
	});

	test("LMOVE on empty source returns nil", async () => {
		await client.send("DEL", ["movesrc"]);
		expect(
			await client.send("LMOVE", ["movesrc", "movedst", "LEFT", "RIGHT"]),
		).toBeNull();
	});

	test("LPUSHX on missing key returns 0", async () => {
		await client.send("DEL", ["pushxmissing"]);
		expect(await client.send("LPUSHX", ["pushxmissing", "v"])).toBe(0);
		expect(await client.send("EXISTS", ["pushxmissing"])).toBe(0);
	});

	test("LPUSHX on existing key prepends and returns new length", async () => {
		await client.send("DEL", ["pushxlist"]);
		await client.send("RPUSH", ["pushxlist", "a"]);
		expect(await client.send("LPUSHX", ["pushxlist", "z"])).toBe(2);
		expect(await client.send("LRANGE", ["pushxlist", "0", "-1"])).toEqual([
			"z",
			"a",
		]);
	});

	test("LPOS finds first occurrence", async () => {
		await client.send("DEL", ["poslist"]);
		await client.send("RPUSH", ["poslist", "a", "b", "c", "b", "a"]);
		expect(await client.send("LPOS", ["poslist", "b"])).toBe(1);
		expect(await client.send("LPOS", ["poslist", "missing"])).toBeNull();
	});

	test("LPOS with negative RANK searches from tail", async () => {
		expect(await client.send("LPOS", ["poslist", "b", "RANK", "-1"])).toBe(3);
	});

	test("LPOS with COUNT returns all positions", async () => {
		expect(await client.send("LPOS", ["poslist", "b", "COUNT", "0"])).toEqual([
			1, 3,
		]);
	});

	test("LTRIM keeps inclusive range", async () => {
		await client.send("DEL", ["trimlist"]);
		await client.send("RPUSH", ["trimlist", "a", "b", "c", "d", "e"]);
		expect(await client.send("LTRIM", ["trimlist", "1", "3"])).toBe("OK");
		expect(await client.send("LRANGE", ["trimlist", "0", "-1"])).toEqual([
			"b",
			"c",
			"d",
		]);
	});

	test("LTRIM with empty range deletes all", async () => {
		await client.send("LTRIM", ["trimlist", "5", "1"]);
		expect(await client.send("LLEN", ["trimlist"])).toBe(0);
	});

	test("TYPE reports list", async () => {
		await client.send("DEL", ["typelist"]);
		await client.send("RPUSH", ["typelist", "v"]);
		expect(await client.send("TYPE", ["typelist"])).toBe("list");
	});

	test("DEL removes a list and its rows", async () => {
		await client.send("RPUSH", ["dellist", "a", "b"]);
		expect(await client.send("DEL", ["dellist"])).toBe(1);
		expect(await client.send("EXISTS", ["dellist"])).toBe(0);
	});
});

describe("Set", () => {
	const sortArr = (a: unknown) => ((a as string[]) ?? []).slice().sort();

	beforeAll(async () => {
		await client.send("DEL", [
			"sadd-set",
			"srem-set",
			"smem-set",
			"scard-set",
			"sismem-set",
			"smismem-set",
			"spop-set",
			"srand-set",
			"suniona",
			"sunionb",
			"sintera",
			"sinterb",
			"sinterc",
			"sdiffa",
			"sdiffb",
			"sunionstore-dst",
			"sinterstore-dst",
			"sdiffstore-dst",
			"smove-src",
			"smove-dst",
			"type-set",
			"del-set",
		]);
	});

	test("SADD returns count of new members and is idempotent", async () => {
		expect(await client.send("SADD", ["sadd-set", "a", "b", "c"])).toBe(3);
		expect(await client.send("SADD", ["sadd-set", "a", "d"])).toBe(1);
		expect(await client.send("SCARD", ["sadd-set"])).toBe(4);
	});

	test("SREM removes only present members", async () => {
		await client.send("SADD", ["srem-set", "a", "b", "c"]);
		expect(await client.send("SREM", ["srem-set", "b", "missing"])).toBe(1);
		expect(sortArr(await client.send("SMEMBERS", ["srem-set"]))).toEqual([
			"a",
			"c",
		]);
	});

	test("SMEMBERS returns all members (order-insensitive)", async () => {
		await client.send("SADD", ["smem-set", "x", "y", "z"]);
		expect(sortArr(await client.send("SMEMBERS", ["smem-set"]))).toEqual([
			"x",
			"y",
			"z",
		]);
	});

	test("SCARD on missing key returns 0", async () => {
		expect(await client.send("SCARD", ["scard-set"])).toBe(0);
		await client.send("SADD", ["scard-set", "a", "b"]);
		expect(await client.send("SCARD", ["scard-set"])).toBe(2);
	});

	test("SISMEMBER returns 1 for present, 0 for missing", async () => {
		await client.send("SADD", ["sismem-set", "m"]);
		expect(await client.send("SISMEMBER", ["sismem-set", "m"])).toBe(1);
		expect(await client.send("SISMEMBER", ["sismem-set", "nope"])).toBe(0);
	});

	test("SMISMEMBER returns a result per requested member in order", async () => {
		await client.send("SADD", ["smismem-set", "a", "c"]);
		expect(
			await client.send("SMISMEMBER", ["smismem-set", "a", "b", "c"]),
		).toEqual([1, 0, 1]);
	});

	test("SPOP removes a member", async () => {
		await client.send("SADD", ["spop-set", "x", "y", "z"]);
		const popped = (await client.send("SPOP", ["spop-set"])) as string;
		expect(["x", "y", "z"]).toContain(popped);
		expect(await client.send("SCARD", ["spop-set"])).toBe(2);
		expect(await client.send("SISMEMBER", ["spop-set", popped])).toBe(0);
	});

	test("SPOP with count returns an array and removes them", async () => {
		await client.send("DEL", ["spop-set"]);
		await client.send("SADD", ["spop-set", "a", "b", "c", "d"]);
		const popped = (await client.send("SPOP", ["spop-set", "3"])) as string[];
		expect(popped.length).toBe(3);
		expect(await client.send("SCARD", ["spop-set"])).toBe(1);
	});

	test("SRANDMEMBER without count does not remove", async () => {
		await client.send("SADD", ["srand-set", "a", "b", "c"]);
		const m = (await client.send("SRANDMEMBER", ["srand-set"])) as string;
		expect(["a", "b", "c"]).toContain(m);
		expect(await client.send("SCARD", ["srand-set"])).toBe(3);
	});

	test("SRANDMEMBER with negative count allows duplicates", async () => {
		await client.send("DEL", ["srand-set"]);
		await client.send("SADD", ["srand-set", "only"]);
		const picks = (await client.send("SRANDMEMBER", [
			"srand-set",
			"-3",
		])) as string[];
		expect(picks.length).toBe(3);
		expect(picks.every((p) => p === "only")).toBe(true);
		expect(await client.send("SCARD", ["srand-set"])).toBe(1);
	});

	test("SUNION merges distinct members across sets", async () => {
		await client.send("SADD", ["suniona", "a", "b"]);
		await client.send("SADD", ["sunionb", "b", "c"]);
		expect(
			sortArr(await client.send("SUNION", ["suniona", "sunionb"])),
		).toEqual(["a", "b", "c"]);
	});

	test("SINTER returns only members in every set", async () => {
		await client.send("SADD", ["sintera", "a", "b", "c"]);
		await client.send("SADD", ["sinterb", "b", "c", "d"]);
		await client.send("SADD", ["sinterc", "c", "d", "e"]);
		expect(
			sortArr(await client.send("SINTER", ["sintera", "sinterb", "sinterc"])),
		).toEqual(["c"]);
	});

	test("SDIFF subtracts later sets from the first", async () => {
		await client.send("SADD", ["sdiffa", "a", "b", "c", "d"]);
		await client.send("SADD", ["sdiffb", "c", "d", "e"]);
		expect(sortArr(await client.send("SDIFF", ["sdiffa", "sdiffb"]))).toEqual([
			"a",
			"b",
		]);
	});

	test("SUNIONSTORE writes union into dst and returns cardinality", async () => {
		expect(
			await client.send("SUNIONSTORE", [
				"sunionstore-dst",
				"suniona",
				"sunionb",
			]),
		).toBe(3);
		expect(sortArr(await client.send("SMEMBERS", ["sunionstore-dst"]))).toEqual(
			["a", "b", "c"],
		);
	});

	test("SINTERSTORE replaces any prior dst value atomically", async () => {
		await client.send("SADD", ["sinterstore-dst", "stale"]);
		expect(
			await client.send("SINTERSTORE", [
				"sinterstore-dst",
				"sintera",
				"sinterb",
			]),
		).toBe(2);
		expect(sortArr(await client.send("SMEMBERS", ["sinterstore-dst"]))).toEqual(
			["b", "c"],
		);
	});

	test("SDIFFSTORE replaces dst with the diff result", async () => {
		expect(
			await client.send("SDIFFSTORE", ["sdiffstore-dst", "sdiffa", "sdiffb"]),
		).toBe(2);
		expect(sortArr(await client.send("SMEMBERS", ["sdiffstore-dst"]))).toEqual([
			"a",
			"b",
		]);
	});

	test("SMOVE transfers member atomically between sets", async () => {
		await client.send("SADD", ["smove-src", "m1", "m2"]);
		await client.send("SADD", ["smove-dst", "m3"]);
		expect(await client.send("SMOVE", ["smove-src", "smove-dst", "m1"])).toBe(
			1,
		);
		expect(await client.send("SISMEMBER", ["smove-src", "m1"])).toBe(0);
		expect(await client.send("SISMEMBER", ["smove-dst", "m1"])).toBe(1);
	});

	test("SMOVE returns 0 when member is absent from src", async () => {
		expect(await client.send("SMOVE", ["smove-src", "smove-dst", "nope"])).toBe(
			0,
		);
	});

	test("TYPE reports set", async () => {
		await client.send("SADD", ["type-set", "m"]);
		expect(await client.send("TYPE", ["type-set"])).toBe("set");
	});

	test("DEL removes a set and its rows", async () => {
		await client.send("SADD", ["del-set", "a", "b"]);
		expect(await client.send("DEL", ["del-set"])).toBe(1);
		expect(await client.send("EXISTS", ["del-set"])).toBe(0);
	});
});

describe("Sorted Set", () => {
	beforeAll(async () => {
		await client.send("DEL", [
			"zadd-z",
			"zadd-nx",
			"zadd-xx",
			"zadd-gt",
			"zadd-lt",
			"zadd-ch",
			"zadd-incr",
			"zrem-z",
			"zscore-z",
			"zmscore-z",
			"zincrby-z",
			"zcard-z",
			"zcount-z",
			"zlexcount-z",
			"zrank-z",
			"zrange-z",
			"zrangebyscore-z",
			"zrangebylex-z",
			"zpopmin-z",
			"zpopmax-z",
			"zrand-z",
			"zremrangerank-z",
			"zremrangescore-z",
			"zremrangelex-z",
			"zua",
			"zub",
			"zuc",
			"zunion-dst",
			"zinter-dst",
			"zdiff-dst",
			"type-zset",
			"del-zset",
		]);
	});

	test("ZADD / ZSCORE / ZCARD basic round-trip", async () => {
		expect(
			await client.send("ZADD", ["zadd-z", "1", "a", "2", "b", "3", "c"]),
		).toBe(3);
		expect(await client.send("ZSCORE", ["zadd-z", "b"])).toBe("2");
		expect(await client.send("ZCARD", ["zadd-z"])).toBe(3);
	});

	test("ZADD upsert returns added count only, updates existing score", async () => {
		expect(await client.send("ZADD", ["zadd-z", "5", "a"])).toBe(0);
		expect(await client.send("ZSCORE", ["zadd-z", "a"])).toBe("5");
	});

	test("ZADD CH returns count of added + changed", async () => {
		await client.send("DEL", ["zadd-ch"]);
		await client.send("ZADD", ["zadd-ch", "1", "a", "2", "b"]);
		expect(
			await client.send("ZADD", [
				"zadd-ch",
				"CH",
				"1",
				"a",
				"9",
				"b",
				"3",
				"c",
			]),
		).toBe(2);
	});

	test("ZADD NX skips existing, XX skips absent", async () => {
		await client.send("ZADD", ["zadd-nx", "1", "a"]);
		expect(
			await client.send("ZADD", ["zadd-nx", "NX", "5", "a", "2", "b"]),
		).toBe(1);
		expect(await client.send("ZSCORE", ["zadd-nx", "a"])).toBe("1");

		await client.send("ZADD", ["zadd-xx", "1", "a"]);
		expect(
			await client.send("ZADD", ["zadd-xx", "XX", "2", "a", "3", "b"]),
		).toBe(0);
		expect(await client.send("ZSCORE", ["zadd-xx", "a"])).toBe("2");
		expect(await client.send("ZSCORE", ["zadd-xx", "b"])).toBeNull();
	});

	test("ZADD GT only raises score, LT only lowers score", async () => {
		await client.send("ZADD", ["zadd-gt", "5", "a"]);
		await client.send("ZADD", ["zadd-gt", "GT", "3", "a"]);
		expect(await client.send("ZSCORE", ["zadd-gt", "a"])).toBe("5");
		await client.send("ZADD", ["zadd-gt", "GT", "10", "a"]);
		expect(await client.send("ZSCORE", ["zadd-gt", "a"])).toBe("10");

		await client.send("ZADD", ["zadd-lt", "5", "a"]);
		await client.send("ZADD", ["zadd-lt", "LT", "10", "a"]);
		expect(await client.send("ZSCORE", ["zadd-lt", "a"])).toBe("5");
		await client.send("ZADD", ["zadd-lt", "LT", "1", "a"]);
		expect(await client.send("ZSCORE", ["zadd-lt", "a"])).toBe("1");
	});

	test("ZADD INCR returns new score", async () => {
		await client.send("DEL", ["zadd-incr"]);
		expect(await client.send("ZADD", ["zadd-incr", "INCR", "5", "a"])).toBe(
			"5",
		);
		expect(await client.send("ZADD", ["zadd-incr", "INCR", "3", "a"])).toBe(
			"8",
		);
	});

	test("ZADD INCR NX returns nil for existing member", async () => {
		expect(
			await client.send("ZADD", ["zadd-incr", "NX", "INCR", "1", "a"]),
		).toBeNull();
	});

	test("ZREM removes only present members", async () => {
		await client.send("ZADD", ["zrem-z", "1", "a", "2", "b", "3", "c"]);
		expect(await client.send("ZREM", ["zrem-z", "a", "missing"])).toBe(1);
		expect(await client.send("ZCARD", ["zrem-z"])).toBe(2);
	});

	test("ZSCORE returns nil for missing member", async () => {
		expect(await client.send("ZSCORE", ["zscore-z", "missing"])).toBeNull();
	});

	test("ZMSCORE returns array of scores or nil", async () => {
		await client.send("ZADD", ["zmscore-z", "1", "a", "2", "b"]);
		expect(
			await client.send("ZMSCORE", ["zmscore-z", "a", "missing", "b"]),
		).toEqual(["1", null, "2"]);
	});

	test("ZINCRBY increments score (creates member if absent)", async () => {
		expect(await client.send("ZINCRBY", ["zincrby-z", "5", "a"])).toBe("5");
		expect(await client.send("ZINCRBY", ["zincrby-z", "3", "a"])).toBe("8");
		expect(await client.send("ZINCRBY", ["zincrby-z", "-2", "a"])).toBe("6");
	});

	test("ZCARD on missing key returns 0", async () => {
		expect(await client.send("ZCARD", ["zcard-z"])).toBe(0);
		await client.send("ZADD", ["zcard-z", "1", "a"]);
		expect(await client.send("ZCARD", ["zcard-z"])).toBe(1);
	});

	test("ZCOUNT respects inclusive and exclusive bounds", async () => {
		await client.send("ZADD", [
			"zcount-z",
			"1",
			"a",
			"2",
			"b",
			"3",
			"c",
			"4",
			"d",
		]);
		expect(await client.send("ZCOUNT", ["zcount-z", "2", "3"])).toBe(2);
		expect(await client.send("ZCOUNT", ["zcount-z", "(1", "(4"])).toBe(2);
		expect(await client.send("ZCOUNT", ["zcount-z", "-inf", "+inf"])).toBe(4);
	});

	test("ZLEXCOUNT counts members in lex range", async () => {
		await client.send("ZADD", [
			"zlexcount-z",
			"0",
			"a",
			"0",
			"b",
			"0",
			"c",
			"0",
			"d",
		]);
		expect(await client.send("ZLEXCOUNT", ["zlexcount-z", "-", "+"])).toBe(4);
		expect(await client.send("ZLEXCOUNT", ["zlexcount-z", "[b", "[c"])).toBe(2);
		expect(await client.send("ZLEXCOUNT", ["zlexcount-z", "(a", "(d"])).toBe(2);
	});

	test("ZRANK / ZREVRANK return 0-based position", async () => {
		await client.send("ZADD", ["zrank-z", "1", "a", "2", "b", "3", "c"]);
		expect(await client.send("ZRANK", ["zrank-z", "a"])).toBe(0);
		expect(await client.send("ZRANK", ["zrank-z", "c"])).toBe(2);
		expect(await client.send("ZREVRANK", ["zrank-z", "a"])).toBe(2);
		expect(await client.send("ZRANK", ["zrank-z", "missing"])).toBeNull();
	});

	test("ZRANGE by index returns members in score order", async () => {
		await client.send("ZADD", [
			"zrange-z",
			"1",
			"a",
			"2",
			"b",
			"3",
			"c",
			"4",
			"d",
		]);
		expect(await client.send("ZRANGE", ["zrange-z", "0", "-1"])).toEqual([
			"a",
			"b",
			"c",
			"d",
		]);
		expect(await client.send("ZRANGE", ["zrange-z", "0", "1"])).toEqual([
			"a",
			"b",
		]);
	});

	test("ZRANGE WITHSCORES interleaves member and score", async () => {
		expect(
			await client.send("ZRANGE", ["zrange-z", "0", "1", "WITHSCORES"]),
		).toEqual(["a", "1", "b", "2"]);
	});

	test("ZRANGE REV reverses order", async () => {
		expect(await client.send("ZRANGE", ["zrange-z", "0", "1", "REV"])).toEqual([
			"d",
			"c",
		]);
	});

	test("ZRANGEBYSCORE with inf and exclusive bound", async () => {
		await client.send("ZADD", [
			"zrangebyscore-z",
			"1",
			"a",
			"2",
			"b",
			"3",
			"c",
			"4",
			"d",
		]);
		expect(
			await client.send("ZRANGEBYSCORE", ["zrangebyscore-z", "-inf", "+inf"]),
		).toEqual(["a", "b", "c", "d"]);
		expect(
			await client.send("ZRANGEBYSCORE", ["zrangebyscore-z", "(1", "3"]),
		).toEqual(["b", "c"]);
		expect(
			await client.send("ZRANGEBYSCORE", [
				"zrangebyscore-z",
				"-inf",
				"+inf",
				"LIMIT",
				"1",
				"2",
			]),
		).toEqual(["b", "c"]);
	});

	test("ZREVRANGEBYSCORE walks max→min", async () => {
		expect(
			await client.send("ZREVRANGEBYSCORE", ["zrangebyscore-z", "3", "1"]),
		).toEqual(["c", "b", "a"]);
	});

	test("ZRANGEBYLEX filters lex ranges", async () => {
		await client.send("ZADD", [
			"zrangebylex-z",
			"0",
			"a",
			"0",
			"b",
			"0",
			"c",
			"0",
			"d",
		]);
		expect(
			await client.send("ZRANGEBYLEX", ["zrangebylex-z", "-", "+"]),
		).toEqual(["a", "b", "c", "d"]);
		expect(
			await client.send("ZRANGEBYLEX", ["zrangebylex-z", "[b", "(d"]),
		).toEqual(["b", "c"]);
	});

	test("ZPOPMIN removes lowest-score member", async () => {
		await client.send("ZADD", ["zpopmin-z", "1", "a", "2", "b", "3", "c"]);
		const popped = (await client.send("ZPOPMIN", ["zpopmin-z"])) as string[];
		expect(popped).toEqual(["a", "1"]);
		expect(await client.send("ZCARD", ["zpopmin-z"])).toBe(2);
	});

	test("ZPOPMAX with count returns top N", async () => {
		await client.send("ZADD", ["zpopmax-z", "1", "a", "2", "b", "3", "c"]);
		const popped = (await client.send("ZPOPMAX", [
			"zpopmax-z",
			"2",
		])) as string[];
		expect(popped).toEqual(["c", "3", "b", "2"]);
		expect(await client.send("ZCARD", ["zpopmax-z"])).toBe(1);
	});

	test("ZRANDMEMBER without count returns a single member", async () => {
		await client.send("ZADD", ["zrand-z", "1", "a", "2", "b", "3", "c"]);
		const m = (await client.send("ZRANDMEMBER", ["zrand-z"])) as string;
		expect(["a", "b", "c"]).toContain(m);
		expect(await client.send("ZCARD", ["zrand-z"])).toBe(3);
	});

	test("ZRANDMEMBER WITHSCORES returns interleaved pairs", async () => {
		const out = (await client.send("ZRANDMEMBER", [
			"zrand-z",
			"2",
			"WITHSCORES",
		])) as string[];
		expect(out.length).toBe(4);
	});

	test("ZREMRANGEBYRANK removes indexes inclusively", async () => {
		await client.send("ZADD", [
			"zremrangerank-z",
			"1",
			"a",
			"2",
			"b",
			"3",
			"c",
			"4",
			"d",
		]);
		expect(
			await client.send("ZREMRANGEBYRANK", ["zremrangerank-z", "0", "1"]),
		).toBe(2);
		expect(await client.send("ZRANGE", ["zremrangerank-z", "0", "-1"])).toEqual(
			["c", "d"],
		);
	});

	test("ZREMRANGEBYSCORE removes score window", async () => {
		await client.send("ZADD", [
			"zremrangescore-z",
			"1",
			"a",
			"2",
			"b",
			"3",
			"c",
			"4",
			"d",
		]);
		expect(
			await client.send("ZREMRANGEBYSCORE", ["zremrangescore-z", "2", "3"]),
		).toBe(2);
		expect(
			await client.send("ZRANGE", ["zremrangescore-z", "0", "-1"]),
		).toEqual(["a", "d"]);
	});

	test("ZREMRANGEBYLEX removes lex window", async () => {
		await client.send("ZADD", [
			"zremrangelex-z",
			"0",
			"a",
			"0",
			"b",
			"0",
			"c",
			"0",
			"d",
		]);
		expect(
			await client.send("ZREMRANGEBYLEX", ["zremrangelex-z", "[b", "[c"]),
		).toBe(2);
		expect(await client.send("ZRANGE", ["zremrangelex-z", "0", "-1"])).toEqual([
			"a",
			"d",
		]);
	});

	test("ZUNIONSTORE sums weighted scores by default", async () => {
		await client.send("ZADD", ["zua", "1", "a", "2", "b"]);
		await client.send("ZADD", ["zub", "3", "b", "4", "c"]);
		expect(
			await client.send("ZUNIONSTORE", ["zunion-dst", "2", "zua", "zub"]),
		).toBe(3);
		expect(
			await client.send("ZRANGE", ["zunion-dst", "0", "-1", "WITHSCORES"]),
		).toEqual(["a", "1", "c", "4", "b", "5"]);
	});

	test("ZINTERSTORE with WEIGHTS and AGGREGATE MAX", async () => {
		await client.send("ZADD", ["zua", "1", "a", "2", "b"]);
		await client.send("ZADD", ["zub", "3", "b", "4", "c"]);
		expect(
			await client.send("ZINTERSTORE", [
				"zinter-dst",
				"2",
				"zua",
				"zub",
				"WEIGHTS",
				"2",
				"3",
				"AGGREGATE",
				"MAX",
			]),
		).toBe(1);
		expect(
			await client.send("ZRANGE", ["zinter-dst", "0", "-1", "WITHSCORES"]),
		).toEqual(["b", "9"]);
	});

	test("ZDIFFSTORE keeps only the first set's exclusive members", async () => {
		await client.send("ZADD", ["zua", "1", "a", "2", "b"]);
		await client.send("ZADD", ["zub", "3", "b", "4", "c"]);
		expect(
			await client.send("ZDIFFSTORE", ["zdiff-dst", "2", "zua", "zub"]),
		).toBe(1);
		expect(
			await client.send("ZRANGE", ["zdiff-dst", "0", "-1", "WITHSCORES"]),
		).toEqual(["a", "1"]);
	});

	test("TYPE reports zset", async () => {
		await client.send("ZADD", ["type-zset", "1", "a"]);
		expect(await client.send("TYPE", ["type-zset"])).toBe("zset");
	});

	test("DEL removes a zset and its rows", async () => {
		await client.send("ZADD", ["del-zset", "1", "a", "2", "b"]);
		expect(await client.send("DEL", ["del-zset"])).toBe(1);
		expect(await client.send("EXISTS", ["del-zset"])).toBe(0);
	});
});

describe("Transactions", () => {
	test("MULTI/EXEC basic batch executes all commands and returns array", async () => {
		await client.send("DEL", ["tx-key"]);
		await client.send("MULTI", []);
		await client.send("SET", ["tx-key", "tx-val"]);
		await client.send("GET", ["tx-key"]);
		const results = await client.send("EXEC", []);
		expect(results).toEqual(["OK", "tx-val"]);
	});

	test("MULTI/EXEC with multiple data types", async () => {
		await client.send("DEL", ["tx-str", "tx-hash", "tx-counter"]);
		await client.send("MULTI", []);
		await client.send("SET", ["tx-str", "hello"]);
		await client.send("HSET", ["tx-hash", "field", "value"]);
		await client.send("INCR", ["tx-counter"]);
		await client.send("GET", ["tx-str"]);
		const results = await client.send("EXEC", []);
		expect(results).toEqual(["OK", 1, 1, "hello"]);
	});

	test("DISCARD clears queue and commands are not executed", async () => {
		await client.send("DEL", ["tx-discard-key"]);
		await client.send("MULTI", []);
		await client.send("SET", ["tx-discard-key", "should-not-exist"]);
		await client.send("DISCARD", []);
		expect(await client.get("tx-discard-key")).toBeNull();
	});

	test("commands inside MULTI return QUEUED", async () => {
		await client.send("MULTI", []);
		const queued = await client.send("SET", ["tx-queued-key", "v"]);
		expect(queued).toBe("QUEUED");
		await client.send("DISCARD", []);
	});

	test("EXEC without MULTI returns error", async () => {
		await expect(client.send("EXEC", [])).rejects.toThrow(/EXEC without MULTI/);
	});

	test("DISCARD without MULTI returns error", async () => {
		await expect(client.send("DISCARD", [])).rejects.toThrow(
			/DISCARD without MULTI/,
		);
	});

	test("nested MULTI returns error", async () => {
		await client.send("MULTI", []);
		await expect(client.send("MULTI", [])).rejects.toThrow(
			/MULTI calls can not be nested/,
		);
		await client.send("DISCARD", []);
	});

	test("runtime error inside EXEC does not abort other commands", async () => {
		const c = new Bun.RedisClient(redisUrl);
		await c.send("DEL", ["tx-err-key"]);
		await c.send("SET", ["tx-err-key", "not-a-number"]);
		await c.send("MULTI", []);
		await c.send("INCR", ["tx-err-key"]);
		await c.send("SET", ["tx-err-key", "recovered"]);
		const results = (await c.send("EXEC", [])) as unknown[];
		expect(results[0]).toBeInstanceOf(Error);
		expect((results[0] as Error).message.toLowerCase()).toContain("err");
		expect(results[1]).toBe("OK");
		expect(await c.get("tx-err-key")).toBe("recovered");
		c.close();
	});

	test("WATCH + EXEC succeeds when key unchanged", async () => {
		const watchClient = new Bun.RedisClient(redisUrl);
		await watchClient.send("DEL", ["tx-watch-key"]);
		await watchClient.send("SET", ["tx-watch-key", "initial"]);
		await watchClient.send("WATCH", ["tx-watch-key"]);
		await watchClient.send("MULTI", []);
		await watchClient.send("SET", ["tx-watch-key", "updated"]);
		const results = await watchClient.send("EXEC", []);
		expect(results).toEqual(["OK"]);
		expect(await watchClient.get("tx-watch-key")).toBe("updated");
		watchClient.close();
	});

	test("WATCH + EXEC aborts when key changed on same connection between WATCH and EXEC", async () => {
		const watchClient = new Bun.RedisClient(redisUrl);
		await watchClient.send("DEL", ["tx-watch-abort-key"]);
		await watchClient.send("SET", ["tx-watch-abort-key", "initial"]);
		await watchClient.send("WATCH", ["tx-watch-abort-key"]);
		// Modify via the same connection so it goes through the same worker's version map
		await watchClient.send("SET", ["tx-watch-abort-key", "modified"]);
		await watchClient.send("MULTI", []);
		await watchClient.send("SET", ["tx-watch-abort-key", "should-not-apply"]);
		const result = await watchClient.send("EXEC", []);
		expect(result).toBeNull();
		expect(await watchClient.get("tx-watch-abort-key")).toBe("modified");
		watchClient.close();
	});

	test("UNWATCH clears watched keys so EXEC always succeeds", async () => {
		const watchClient = new Bun.RedisClient(redisUrl);
		await watchClient.send("DEL", ["tx-unwatch-key"]);
		await watchClient.send("SET", ["tx-unwatch-key", "initial"]);
		await watchClient.send("WATCH", ["tx-unwatch-key"]);
		// Modify via same connection to guarantee same worker's version map is updated
		await watchClient.send("SET", ["tx-unwatch-key", "modified"]);
		await watchClient.send("UNWATCH", []);
		await watchClient.send("MULTI", []);
		await watchClient.send("SET", ["tx-unwatch-key", "final"]);
		const results = await watchClient.send("EXEC", []);
		expect(results).toEqual(["OK"]);
		expect(await watchClient.get("tx-unwatch-key")).toBe("final");
		watchClient.close();
	});

	test("WATCH inside MULTI returns error", async () => {
		await client.send("MULTI", []);
		await expect(client.send("WATCH", ["some-key"])).rejects.toThrow(
			/not allowed inside a transaction/,
		);
		await client.send("DISCARD", []);
	});

	test("empty EXEC returns empty array", async () => {
		await client.send("MULTI", []);
		const results = await client.send("EXEC", []);
		expect(results).toEqual([]);
	});
});
