import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { errorReply, integerReply, RawRedis } from "./resp";

const REDIS_HOST = process.env.REDIS_HOST ?? "localhost";
const REDIS_PORT = process.env.REDIS_PORT ?? "6379";
const REDIS_PASSWORD = process.env.REDIS_PASSWORD ?? "testpass";
const DATABASE_URL =
	process.env.DATABASE_URL ??
	"postgres://postgres:postgres@localhost:5432/postgres";

const redisUrl = `redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}`;
const sql = new Bun.sql(DATABASE_URL);
let client: Bun.RedisClient;

let raw: RawRedis;
/** True when databases 0-7 are served from shared memory rather than tables. */
let memoryMode = false;

beforeAll(async () => {
	client = new Bun.RedisClient(redisUrl);
	raw = await new RawRedis().connect();
	// Aliased: `SHOW redis.storage_mode` names the column "redis.storage_mode",
	// dot and all, so destructuring by `storage_mode` silently yields undefined
	// and every memory-mode assertion below takes the wrong branch.
	const [{ mode }] =
		await sql`SELECT current_setting('redis.storage_mode') AS mode`;
	memoryMode = mode === "memory";
});

afterAll(async () => {
	raw.close();
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

	// The durable half increments in SQL, where the raise this used to trigger
	// ended the worker process. The PING matters more than the reply.
	test("INCR past the integer ceiling errors and leaves the worker up", async () => {
		const ceiling = "9223372036854775807";
		await raw.send("DEL", "incr-ceiling");
		await raw.send("SET", "incr-ceiling", ceiling);
		expect((await raw.send("INCR", "incr-ceiling")).toString()).toMatch(
			/^-ERR increment or decrement would overflow/,
		);
		// The refused increment must not have changed what is stored.
		expect((await raw.bulk("GET", "incr-ceiling"))?.toString()).toBe(ceiling);
		expect((await raw.send("PING")).toString()).toBe("+PONG\r\n");
		await raw.send("DEL", "incr-ceiling");
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
			await sql`SELECT count(*)::int AS cnt FROM redis.kv_8 WHERE key = 'expiring_key'`;
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
			await sql`SELECT count(*)::int AS cnt FROM redis.kv_8 WHERE key = 'active_scan_key'`;
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
			await sql`SELECT count(*)::int AS cnt FROM redis.kv_8 WHERE key = 'long_ttl_key'`;
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
	test("the cache and durable halves are isolated", async () => {
		await client.send("SELECT", ["cache"]);
		await client.del("ulkey");
		await client.send("SELECT", ["durable"]);
		await client.del("ulkey");

		await client.send("SELECT", ["cache"]);
		expect(await client.set("ulkey", "ulval")).toBe("OK");
		expect(await client.get("ulkey")).toBe("ulval");

		await client.send("SELECT", ["durable"]);
		expect(await client.get("ulkey")).toBeNull();

		await client.send("SELECT", ["cache"]);
	});

	test("full isolation matrix", async () => {
		await client.send("SELECT", ["cache"]);
		await client.del("isolation_ul");
		await client.send("SELECT", ["durable"]);
		await client.del("isolation_ul");
		await client.del("isolation_lg");

		await client.send("SELECT", ["cache"]);
		expect(await client.set("isolation_ul", "isolation_val")).toBe("OK");
		expect(await client.get("isolation_ul")).toBe("isolation_val");

		await client.send("SELECT", ["durable"]);
		expect(await client.get("isolation_ul")).toBeNull();
		expect(await client.set("isolation_lg", "isolation_val")).toBe("OK");
		expect(await client.get("isolation_lg")).toBe("isolation_val");

		await client.send("SELECT", ["cache"]);
		expect(await client.get("isolation_lg")).toBeNull();

		await client.send("SELECT", ["durable"]);
		expect(await client.del("isolation_lg")).toBe(1);

		await client.send("SELECT", ["cache"]);
	});

	test("the half names are aliases for 0 and 8, not separate databases", async () => {
		await client.send("SELECT", ["durable"]);
		expect(await client.set("alias_key", "alias_val")).toBe("OK");
		await client.send("SELECT", ["8"]);
		expect(await client.get("alias_key")).toBe("alias_val");
		expect(await client.del("alias_key")).toBe(1);

		await client.send("SELECT", ["cache"]);
		expect(await client.set("alias_key", "alias_val")).toBe("OK");
		await client.send("SELECT", ["0"]);
		expect(await client.get("alias_key")).toBe("alias_val");
		expect(await client.del("alias_key")).toBe(1);
	});

	test("an unknown database name is rejected", async () => {
		await expect(client.send("SELECT", ["ephemeral"])).rejects.toThrow();
		await expect(client.send("SELECT", ["16"])).rejects.toThrow();
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

	// Driven through the raw client: these ran only against the durable half
	// before, because `SELECT isolation` leaves the pooled client on db 0 and a
	// filtered run skips it. Being explicit about the database is what makes
	// them cover the shared-memory list at all.
	test("LINSERT BEFORE pivot", async () => {
		await raw.send("SELECT", "cache");
		await raw.send("DEL", "inslist");
		await raw.send("RPUSH", "inslist", "a", "c", "e");
		expect(
			(await raw.send("LINSERT", "inslist", "BEFORE", "c", "b")).toString(),
		).toBe(":4\r\n");
		expect(await raw.bulk("LINDEX", "inslist", "1")).toEqual(Buffer.from("b"));
	});

	test("LINSERT AFTER pivot", async () => {
		expect(
			(await raw.send("LINSERT", "inslist", "AFTER", "c", "d")).toString(),
		).toBe(":5\r\n");
		// The whole list, in order: a rewrite that renumbers positions has to
		// leave every element reachable, not just the ones before the splice.
		for (const [i, want] of ["a", "b", "c", "d", "e"].entries()) {
			expect(await raw.bulk("LINDEX", "inslist", String(i))).toEqual(
				Buffer.from(want),
			);
		}
	});

	test("LINSERT missing pivot returns -1", async () => {
		expect(
			(await raw.send("LINSERT", "inslist", "BEFORE", "Z", "x")).toString(),
		).toBe(":-1\r\n");
	});

	test("LINSERT on missing key returns 0", async () => {
		expect(
			(
				await raw.send("LINSERT", "nokey-linsert", "BEFORE", "p", "v")
			).toString(),
		).toBe(":0\r\n");
		await raw.send("SELECT", "durable");
	});

	// LREM removed the right elements but left holes, and readers walk
	// positions contiguously from min_pos — so the tail became unreachable and
	// the list silently lost its last element.
	test("LREM leaves the surviving tail reachable", async () => {
		await raw.send("SELECT", "cache");
		await raw.send("DEL", "remraw");
		await raw.send("RPUSH", "remraw", "a", "x", "a", "x", "a");
		expect(integerReply(await raw.send("LREM", "remraw", "2", "a"))).toBe(2);
		expect(integerReply(await raw.send("LLEN", "remraw"))).toBe(3);
		for (const [i, want] of ["x", "x", "a"].entries()) {
			expect(await raw.bulk("LINDEX", "remraw", String(i))).toEqual(
				Buffer.from(want),
			);
		}
		await raw.send("SELECT", "durable");
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

	// ZINCRBY's return value alone says nothing about the metadata. A member
	// that reaches the entry table and not the meta is listed by ZRANGE, counted
	// 0 by ZCARD, invisible to ZPOPMIN, and leaves the count wrong for the life
	// of the key — so assert through all three.
	test("a member ZINCRBY creates is counted and can be popped", async () => {
		await client.send("DEL", ["zincrby-meta"]);
		expect(await client.send("ZINCRBY", ["zincrby-meta", "5", "alice"])).toBe(
			"5",
		);
		expect(await client.send("ZCARD", ["zincrby-meta"])).toBe(1);
		expect(await client.send("ZRANGE", ["zincrby-meta", "0", "-1"])).toEqual([
			"alice",
		]);
		expect(await client.send("ZPOPMIN", ["zincrby-meta"])).toEqual([
			"alice",
			"5",
		]);
		expect(await client.send("ZCARD", ["zincrby-meta"])).toBe(0);

		// A member added afterwards must not inherit a count the pop left wrong.
		await client.send("ZADD", ["zincrby-meta", "1", "bob"]);
		expect(await client.send("ZCARD", ["zincrby-meta"])).toBe(1);
		await client.send("DEL", ["zincrby-meta"]);
	});

	// ZINCRBY has to move the extremes too, not just create members. ZPOPMIN
	// and ZPOPMAX read them straight out of the metadata.
	test("ZINCRBY moves the min and max the metadata reports", async () => {
		await client.send("DEL", ["zincrby-ext"]);
		await client.send("ZADD", ["zincrby-ext", "1", "a", "2", "b", "3", "c"]);
		// `a` overtakes `c`: the max moves to a member that was neither.
		expect(await client.send("ZINCRBY", ["zincrby-ext", "10", "a"])).toBe("11");
		expect(await client.send("ZPOPMAX", ["zincrby-ext"])).toEqual(["a", "11"]);
		// `b` is now the min; drop it below and the min has to follow.
		expect(await client.send("ZINCRBY", ["zincrby-ext", "-5", "b"])).toBe("-3");
		expect(await client.send("ZPOPMIN", ["zincrby-ext"])).toEqual(["b", "-3"]);
		// And the min moving *off* a member re-derives from the entries.
		expect(await client.send("ZCARD", ["zincrby-ext"])).toBe(1);
		expect(await client.send("ZPOPMIN", ["zincrby-ext"])).toEqual(["c", "3"]);
		await client.send("DEL", ["zincrby-ext"]);
	});

	// ZADD ... INCR reaches the same code path and needs the same guard.
	test("a member ZADD INCR creates is counted", async () => {
		await client.send("DEL", ["zaddincr-meta"]);
		expect(
			await client.send("ZADD", ["zaddincr-meta", "INCR", "7", "solo"]),
		).toBe("7");
		expect(await client.send("ZCARD", ["zaddincr-meta"])).toBe(1);
		expect(await client.send("ZPOPMAX", ["zaddincr-meta"])).toEqual([
			"solo",
			"7",
		]);
		await client.send("DEL", ["zaddincr-meta"]);
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

describe("Binary safety", () => {
	// Redis keys and values are arbitrary byte strings. These bytes are exactly
	// what the previous TEXT columns could not store: a NUL, and sequences that
	// are not valid UTF-8.
	const BINARY = Buffer.from([0x00, 0xff, 0xfe, 0x61, 0x80, 0x0a, 0x01]);
	const star = Buffer.from("*");
	/** Sorted hex of a key list, so binary keys compare readably. */
	const hexes = (ks: Array<Buffer | null>) =>
		ks.map((k) => k?.toString("hex") ?? "").sort();

	// Must run on `cache`. On db 8 a key is a bytea column, binary-safe for
	// free, so every assertion below passes without saying anything about the
	// shared-memory backend — which is where binary keys are actually hard.
	beforeAll(async () => {
		await raw.send("SELECT", "cache");
	});
	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	test("a value containing NUL and invalid UTF-8 round-trips byte for byte", async () => {
		const key = "binkey";
		await raw.send("SET", key, BINARY);
		expect(await raw.bulk("GET", key)).toEqual(BINARY);
		// STRLEN counts stored bytes, which is the whole point: a UTF-8 round
		// trip through the client would report 10 for these 7 bytes.
		expect((await raw.send("STRLEN", key)).toString()).toBe(
			`:${BINARY.length}\r\n`,
		);
		await raw.send("DEL", key);
	});

	test("keys differing only after a NUL are distinct keys", async () => {
		const a = Buffer.concat([BINARY, Buffer.from("one")]);
		const b = Buffer.concat([BINARY, Buffer.from("two")]);
		await raw.send("SET", a, "first");
		await raw.send("SET", b, "second");
		expect((await raw.bulk("GET", a))?.toString()).toBe("first");
		expect((await raw.bulk("GET", b))?.toString()).toBe("second");
		// Both, not one collapsed onto the other and truncated at the NUL.
		const listed = await raw.array("KEYS", Buffer.concat([BINARY, star]));
		expect(hexes(listed)).toEqual(hexes([a, b]));
		await raw.send("DEL", a);
		await raw.send("DEL", b);
	});

	// KEYS, SCAN and RANDOMKEY read the key bytes back out of the entry. A key
	// past the 128-byte inline prefix is assembled from the chunk pool: the one
	// a truncated read would mangle, and the one a lookup cannot settle by
	// comparing the prefix alone.
	test("keys of every length round-trip through KEYS, SCAN and RANDOMKEY", async () => {
		const prefix = Buffer.from("binlen:");
		const lengths = [8, 63, 64, 65, 128, 200, 511, 512];
		const keys = lengths.map((n) => {
			const k = Buffer.concat([prefix, payload(n - prefix.length)]);
			// A NUL every 32 bytes, so no key is a prefix of another only past
			// its terminator either.
			for (let i = 31; i < n; i += 32) k[i] = 0;
			k[n - 1] = n & 0xff;
			return k;
		});
		for (const [i, k] of keys.entries()) {
			await raw.send("SET", k, `v${lengths[i]}`);
		}
		for (const [i, k] of keys.entries()) {
			expect((await raw.bulk("GET", k))?.toString()).toBe(`v${lengths[i]}`);
			expect((await raw.send("STRLEN", k)).toString()).toBe(
				`:${`v${lengths[i]}`.length}\r\n`,
			);
		}

		const pattern = Buffer.concat([prefix, star]);
		expect(hexes(await raw.array("KEYS", pattern))).toEqual(hexes(keys));
		// SCAN's second element is the key array; cursor is always 0 here.
		const scanned = await raw.array("SCAN", "0", "MATCH", pattern);
		expect(scanned[1]).not.toBeNull();

		// RANDOMKEY reads a key out of an entry through the same call. Whatever
		// it names has to be a whole key: a truncating read would hand back a
		// byte string that is not a key in the database at all. (It cannot be
		// asked for one of ours — on the shared-memory half it returns the
		// first entry the scan reaches, not a uniform draw.)
		const random = await raw.bulk("RANDOMKEY");
		expect(random).not.toBeNull();
		expect(integerReply(await raw.send("EXISTS", random as Buffer))).toBe(1);

		for (const k of keys) await raw.send("DEL", k);
		expect(await raw.array("KEYS", pattern)).toEqual([]);
	});

	// Members were read back up to their first NUL, so two sharing a prefix
	// stored as two entries and read back as one truncated string.
	const NUL_A = Buffer.from("a\0one");
	const NUL_B = Buffer.from("a\0two");

	test("set members differing only after a NUL round-trip whole", async () => {
		await raw.send("DEL", "bin:set");
		expect(integerReply(await raw.send("SADD", "bin:set", NUL_A))).toBe(1);
		expect(integerReply(await raw.send("SADD", "bin:set", NUL_B))).toBe(1);
		expect(integerReply(await raw.send("SCARD", "bin:set"))).toBe(2);
		expect(integerReply(await raw.send("SISMEMBER", "bin:set", NUL_A))).toBe(1);
		// The prefix is not a member of the set, however it reads back.
		expect(integerReply(await raw.send("SISMEMBER", "bin:set", "a"))).toBe(0);
		expect(hexes(await raw.array("SMEMBERS", "bin:set"))).toEqual(
			hexes([NUL_A, NUL_B]),
		);

		// SPOP has to name something SREM can then remove.
		const popped = await raw.array("SPOP", "bin:set", "1");
		expect(popped.length).toBe(1);
		const one = popped[0] as Buffer;
		expect(hexes([one])).not.toEqual(hexes([Buffer.from("a")]));
		expect(integerReply(await raw.send("SISMEMBER", "bin:set", one))).toBe(0);
		expect(integerReply(await raw.send("SCARD", "bin:set"))).toBe(1);

		const rest = await raw.array("SMEMBERS", "bin:set");
		expect(
			(await raw.send("SREM", "bin:set", rest[0] as Buffer)).toString(),
		).toBe(":1\r\n");
		expect(integerReply(await raw.send("SCARD", "bin:set"))).toBe(0);
		await raw.send("DEL", "bin:set");
	});

	test("hash fields differing only after a NUL round-trip whole", async () => {
		await raw.send("DEL", "bin:hash");
		await raw.send("HSET", "bin:hash", NUL_A, "first");
		await raw.send("HSET", "bin:hash", NUL_B, "second");
		expect(integerReply(await raw.send("HLEN", "bin:hash"))).toBe(2);
		expect((await raw.bulk("HGET", "bin:hash", NUL_A))?.toString()).toBe(
			"first",
		);
		expect((await raw.bulk("HGET", "bin:hash", NUL_B))?.toString()).toBe(
			"second",
		);
		expect(await raw.bulk("HGET", "bin:hash", "a")).toBeNull();

		// HGETALL reads the field out of the entry; the pairs must still pair up.
		const flat = await raw.array("HGETALL", "bin:hash");
		expect(hexes([flat[0] as Buffer, flat[2] as Buffer])).toEqual(
			hexes([NUL_A, NUL_B]),
		);
		expect(hexes(await raw.array("HKEYS", "bin:hash"))).toEqual(
			hexes([NUL_A, NUL_B]),
		);
		const pairs = new Map<string, string>();
		for (let i = 0; i < flat.length; i += 2) {
			pairs.set(
				(flat[i] as Buffer).toString("hex"),
				(flat[i + 1] as Buffer).toString(),
			);
		}
		expect(pairs.get(NUL_A.toString("hex"))).toBe("first");
		expect(pairs.get(NUL_B.toString("hex"))).toBe("second");

		expect(integerReply(await raw.send("HDEL", "bin:hash", NUL_A))).toBe(1);
		expect((await raw.bulk("HGET", "bin:hash", NUL_B))?.toString()).toBe(
			"second",
		);
		await raw.send("DEL", "bin:hash");
	});

	test("sorted-set members differing only after a NUL round-trip whole", async () => {
		await raw.send("DEL", "bin:zset");
		await raw.send("ZADD", "bin:zset", "1", NUL_A);
		await raw.send("ZADD", "bin:zset", "2", NUL_B);
		expect(integerReply(await raw.send("ZCARD", "bin:zset"))).toBe(2);
		expect((await raw.bulk("ZSCORE", "bin:zset", NUL_A))?.toString()).toBe("1");
		expect(await raw.bulk("ZSCORE", "bin:zset", "a")).toBeNull();
		expect(hexes(await raw.array("ZRANGE", "bin:zset", "0", "-1"))).toEqual(
			hexes([NUL_A, NUL_B]),
		);

		// ZPOPMIN names its member out of the meta, which keeps a hash of it.
		const popped = await raw.array("ZPOPMIN", "bin:zset");
		expect((popped[0] as Buffer).toString("hex")).toBe(NUL_A.toString("hex"));
		expect((popped[1] as Buffer).toString()).toBe("1");
		expect(integerReply(await raw.send("ZCARD", "bin:zset"))).toBe(1);

		const max = await raw.array("ZPOPMAX", "bin:zset");
		expect((max[0] as Buffer).toString("hex")).toBe(NUL_B.toString("hex"));
		expect(integerReply(await raw.send("ZCARD", "bin:zset"))).toBe(0);
		await raw.send("DEL", "bin:zset");
	});

	// A member has the same two boundaries a value has. A NUL every 16 bytes
	// keeps a truncating read from passing on any of them.
	test("members of every length round-trip through every collection", async () => {
		const lengths = [1, 47, 48, 49, 112, 113, 200, 511, 512];
		const member = (n: number) => {
			const m = payload(n);
			for (let i = 15; i < n; i += 16) m[i] = 0;
			m[n - 1] = n & 0xff;
			return m;
		};
		const members = lengths.map(member);

		await raw.send("DEL", "len:set", "len:hash", "len:zset");
		for (const [i, m] of members.entries()) {
			await raw.send("SADD", "len:set", m);
			await raw.send("HSET", "len:hash", m, `v${lengths[i]}`);
			await raw.send("ZADD", "len:zset", String(i), m);
		}

		expect(hexes(await raw.array("SMEMBERS", "len:set"))).toEqual(
			hexes(members),
		);
		expect(hexes(await raw.array("HKEYS", "len:hash"))).toEqual(hexes(members));
		expect(hexes(await raw.array("ZRANGE", "len:zset", "0", "-1"))).toEqual(
			hexes(members),
		);
		for (const [i, m] of members.entries()) {
			expect(integerReply(await raw.send("SISMEMBER", "len:set", m))).toBe(1);
			expect((await raw.bulk("HGET", "len:hash", m))?.toString()).toBe(
				`v${lengths[i]}`,
			);
			expect((await raw.bulk("ZSCORE", "len:zset", m))?.toString()).toBe(
				String(i),
			);
		}

		// Removing them returns every chunk their tails took.
		for (const m of members) {
			expect(integerReply(await raw.send("SREM", "len:set", m))).toBe(1);
			expect(integerReply(await raw.send("HDEL", "len:hash", m))).toBe(1);
			expect(integerReply(await raw.send("ZREM", "len:zset", m))).toBe(1);
		}
		await raw.send("DEL", "len:set", "len:hash", "len:zset");
	});

	// A member past the inline prefix owns a chain, and only a pool that runs
	// dry reveals a removal path that fails to give it back.
	test("churning long members does not exhaust the member pools", async () => {
		if (!memoryMode) return; // db 0 is a table here; nothing is pooled

		const long = (i: number) => {
			const m = Buffer.alloc(MAX_MEMBER_LEN, 0x6d);
			m.write(`churn:${i}:`, 0);
			return m;
		};
		for (let i = 0; i < 3000; i++) {
			const m = long(i);
			await raw.send("SADD", "churn:set", m);
			await raw.send("SREM", "churn:set", m);
			await raw.send("ZADD", "churn:zset", "1", m);
			await raw.send("ZREM", "churn:zset", m);
			await raw.send("HSET", "churn:hash", m, "v");
			await raw.send("HDEL", "churn:hash", m);
		}
		// SPOP, ZPOPMIN and the whole-key deletes free chains of their own.
		for (let i = 0; i < 200; i++) {
			await raw.send("SADD", "churn:set", long(i));
			await raw.send("ZADD", "churn:zset", String(i), long(i));
		}
		for (let i = 0; i < 200; i++) {
			await raw.send("SPOP", "churn:set");
			await raw.send("ZPOPMIN", "churn:zset");
		}
		for (let round = 0; round < 8; round++) {
			for (let i = 0; i < 200; i++) {
				await raw.send("SADD", "churn:set", long(i));
				await raw.send("ZADD", "churn:zset", String(i), long(i));
			}
			await raw.send("DEL", "churn:set", "churn:zset");
		}

		// If any of those leaked, the pool is dry by now and this is an OOM.
		const m = long(9999);
		expect(integerReply(await raw.send("SADD", "churn:set", m))).toBe(1);
		expect(hexes(await raw.array("SMEMBERS", "churn:set"))).toEqual(hexes([m]));
		await raw.send("DEL", "churn:set", "churn:zset", "churn:hash");
	});
});

describe("Pub/Sub pattern matching", () => {
	// A pattern whose wildcards run past the end of the channel name used to
	// panic inside PUBLISH while the pub/sub spinlock was held, wedging pub/sub
	// for every worker process. The server must stay responsive afterwards.
	const HOSTILE_PATTERNS = ["*?", "*[a]", "?", "*a", "[abc"];

	test("wildcard patterns past the end of a channel do not wedge the server", async () => {
		for (const pattern of HOSTILE_PATTERNS) {
			const subscriber = new Bun.RedisClient(redisUrl);
			try {
				await subscriber.send("PSUBSCRIBE", [pattern]);
				// Empty channel name: nothing is left for the wildcards to consume.
				await client.send("PUBLISH", ["", "payload"]);
				await client.send("PUBLISH", ["a", "payload"]);
			} finally {
				subscriber.close();
			}
			// If the spinlock had leaked, this would hang rather than reply.
			expect(await client.send("PUBSUB", ["NUMPAT"])).toBeDefined();
			expect(await client.ping()).toBe("PONG");
		}
	});

	test("PSUBSCRIBE delivers only to matching channels", async () => {
		const subscriber = new Bun.RedisClient(redisUrl);
		try {
			await subscriber.send("PSUBSCRIBE", ["news.*"]);
			await Bun.sleep(100);

			expect(await client.send("PUBLISH", ["news.sports", "hit"])).toBe(1);
			// `news.*` must not match a channel that merely starts with `news`.
			expect(await client.send("PUBLISH", ["newsletter", "miss"])).toBe(0);
			// ...nor one that only ends with the literal part of the pattern.
			expect(await client.send("PUBLISH", ["other.news", "miss"])).toBe(0);
		} finally {
			subscriber.close();
		}
	});
});

describe("Pub/Sub size limits", () => {
	// Names and payloads live in fixed-size shared-memory slots. Truncating them
	// would leave a client subscribed to a channel it can never receive on, so
	// the server rejects them instead.
	const MAX_CHANNEL = 255;
	const MAX_PAYLOAD = 512;

	test("a channel name at the limit works end to end", async () => {
		const channel = "c".repeat(MAX_CHANNEL);
		const subscriber = new Bun.RedisClient(redisUrl);
		try {
			await subscriber.send("SUBSCRIBE", [channel]);
			await Bun.sleep(100);
			expect(await client.send("PUBLISH", [channel, "hi"])).toBe(1);
		} finally {
			subscriber.close();
		}
	});

	test("a payload at the limit is delivered", async () => {
		const subscriber = new Bun.RedisClient(redisUrl);
		try {
			await subscriber.send("SUBSCRIBE", ["limit-chan"]);
			await Bun.sleep(100);
			const payload = "p".repeat(MAX_PAYLOAD);
			expect(await client.send("PUBLISH", ["limit-chan", payload])).toBe(1);
		} finally {
			subscriber.close();
		}
	});

	test("an over-long channel name is rejected, not truncated", async () => {
		const tooLong = "c".repeat(MAX_CHANNEL + 1);
		await expect(client.send("PUBLISH", [tooLong, "hi"])).rejects.toThrow(
			/limit/,
		);
		const subscriber = new Bun.RedisClient(redisUrl);
		try {
			await expect(subscriber.send("SUBSCRIBE", [tooLong])).rejects.toThrow(
				/limit/,
			);
		} finally {
			subscriber.close();
		}
	});

	test("an over-long payload is rejected, not truncated", async () => {
		await expect(
			client.send("PUBLISH", ["limit-chan", "p".repeat(MAX_PAYLOAD + 1)]),
		).rejects.toThrow(/limit/);
	});
});

// The shared-memory backend stores the first INLINE_VAL_LEN (64) bytes of a
// value inside the entry and spills the rest into a pool of fixed-size chunks,
// one chunk per CHUNK_LEN bytes, chained by index. `redis-benchmark` writes
// 3-byte values, so nothing in the benchmark suite has ever crossed either
// boundary, and the spill/reclaim paths were where two silent-truncation bugs
// lived.
const INLINE_VAL_LEN = 64;
const CHUNK_LEN = 64;
// An eighth of a pool of `redis.mem_max_entries` chunks, capped at 64 KiB —
// which is where the default 8192 lands. See docs/IMPLEMENTATION.md.
const MAX_TOTAL_VAL_LEN = 64 * 1024;
// Members carry the same 512-byte bound as keys: a share of a chunk pool, not
// the size of an array. See docs/IMPLEMENTATION.md.
const MAX_MEMBER_LEN = 512;
const MEM_MAX_KEY = 512;

/** Distinct, position-dependent bytes, so a truncation cannot pass by luck. */
function payload(n: number): Buffer {
	const b = Buffer.alloc(n);
	for (let i = 0; i < n; i++) b[i] = 33 + (i % 90);
	return b;
}

/** Sizes either side of every boundary the value path has. */
const VALUE_SIZES = [
	1,
	INLINE_VAL_LEN - 1, // 63    last fully-inline size
	INLINE_VAL_LEN, // 64    exactly fills the inline slot
	INLINE_VAL_LEN + 1, // 65    first byte to reach the chunk pool
	INLINE_VAL_LEN + CHUNK_LEN, // 128   inline slot plus exactly one full chunk
	INLINE_VAL_LEN + CHUNK_LEN + 1, // 129   first byte of a second chunk
	200, // comfortably mid-chunk
	511, // the cap before the pool replaced the fixed overflow row...
	512, // ...and one byte past it, now unremarkable
	MAX_TOTAL_VAL_LEN - 1, // 65535
	MAX_TOTAL_VAL_LEN, // 65536 largest the memory backend accepts
];

// Both storage backends, the same assertions. Every case here was found by the
// parity harness, and all but the last two were an error reply — or, before
// `in_subtransaction` learned to catch, a dead worker.
for (const half of ["cache", "durable"]) {
	describe(`Enumerating keys of every type (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");
		});
		afterAll(async () => {
			await raw.send("FLUSHDB");
			await raw.send("SELECT", "durable");
		});

		// The entry tables key on (key hash, member) and the directory on a
		// hash, so a collection's name was written down nowhere: KEYS saw
		// strings only, while DBSIZE counted everything.
		test("KEYS lists every type, not only strings", async () => {
			await raw.send("FLUSHDB");
			await raw.send("SET", "kn:str", "v");
			await raw.send("RPUSH", "kn:list", "a");
			await raw.send("SADD", "kn:set", "m");
			await raw.send("HSET", "kn:hash", "f", "v");
			await raw.send("ZADD", "kn:zset", "1", "m");

			const keys = async (pat: string) =>
				(await raw.array("KEYS", pat)).map((k) => k?.toString()).sort();
			expect(await keys("*")).toEqual([
				"kn:hash",
				"kn:list",
				"kn:set",
				"kn:str",
				"kn:zset",
			]);
			// DBSIZE and KEYS have to agree about what exists.
			expect(integerReply(await raw.send("DBSIZE"))).toBe(5);
			// Globs apply to collections the same way.
			expect(await keys("kn:s*")).toEqual(["kn:set", "kn:str"]);
			expect(await keys("*hash")).toEqual(["kn:hash"]);
			expect(await keys("nomatch*")).toEqual([]);
			await raw.send("FLUSHDB");
		});

		test("a name goes when its key does, by DEL and by emptying", async () => {
			await raw.send("FLUSHDB");
			await raw.send("RPUSH", "kn:l", "a");
			await raw.send("SADD", "kn:s", "m");
			await raw.send("HSET", "kn:h", "f", "v");
			await raw.send("ZADD", "kn:z", "1", "m");
			const keys = async () =>
				(await raw.array("KEYS", "kn:*")).map((k) => k?.toString()).sort();
			expect(await keys()).toEqual(["kn:h", "kn:l", "kn:s", "kn:z"]);

			// DEL takes the name with the data...
			await raw.send("DEL", "kn:l");
			expect(await keys()).toEqual(["kn:h", "kn:s", "kn:z"]);
			// ...and so does removing the last member.
			await raw.send("SREM", "kn:s", "m");
			await raw.send("HDEL", "kn:h", "f");
			await raw.send("ZREM", "kn:z", "m");
			expect(await keys()).toEqual([]);
			expect(integerReply(await raw.send("DBSIZE"))).toBe(0);
		});

		test("an expired collection stops being listed", async () => {
			await raw.send("FLUSHDB");
			await raw.send("RPUSH", "kn:exp", "a");
			await raw.send("EXPIRE", "kn:exp", "1");
			expect((await raw.array("KEYS", "kn:*")).length).toBe(1);
			await Bun.sleep(1600);
			expect((await raw.array("KEYS", "kn:*")).length).toBe(0);
			expect(integerReply(await raw.send("DBSIZE"))).toBe(0);
		});

		// The name is an inline prefix with the tail chained into the pool, so
		// a key past that prefix is the case that exercises the chain.
		test("a long name round-trips whole through the pool", async () => {
			await raw.send("FLUSHDB");
			for (const len of [39, 40, 41, 104, MEM_MAX_KEY]) {
				const name = `kn:${"n".repeat(len - 3)}`;
				await raw.send("SADD", name, "m");
				const listed = (await raw.array("KEYS", "kn:n*")).map((k) =>
					k?.toString(),
				);
				expect(listed).toEqual([name]);
				expect(listed[0]?.length).toBe(len);
				await raw.send("DEL", name);
			}
		});

		// A meta entry owns its name's chain. A removal that does not hand it
		// back leaves the next occupant of that slot holding chunks already on
		// the free list — which crashed the worker outright, not subtly.
		test("churning long collection names does not exhaust the pool", async () => {
			await raw.send("FLUSHDB");
			const name = (i: number) =>
				`kn:churn:${i}:${"x".repeat(MEM_MAX_KEY)}`.slice(0, MEM_MAX_KEY);
			for (let i = 0; i < 400; i++) {
				await raw.send("SADD", name(i), "m");
				await raw.send("DEL", name(i));
			}
			// If the chain leaked, the pool is dry and this fails.
			await raw.send("SADD", name(0), "m");
			expect(
				(await raw.array("KEYS", "kn:churn:0:*")).map((k) => k?.toString()),
			).toEqual([name(0)]);
			await raw.send("FLUSHDB");
		});
	});
}

for (const half of ["cache", "durable"]) {
	describe(`Commands added in phase 5 (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
		});
		afterAll(async () => {
			await raw.send("SELECT", "durable");
		});

		test("FLUSHDB empties this database and leaves the other alone", async () => {
			await raw.send("SET", "p5:k", "v");
			await raw.send("RPUSH", "p5:l", "a");
			await raw.send("HSET", "p5:h", "f", "v");
			await raw.send("SADD", "p5:s", "m");
			await raw.send("ZADD", "p5:z", "1", "m");
			// A marker on the half this test is not on.
			const other = half === "cache" ? "durable" : "cache";
			await raw.send("SELECT", other);
			await raw.send("SET", "p5:other", "keep");
			await raw.send("SELECT", half);

			expect(integerReply(await raw.send("DBSIZE"))).toBeGreaterThanOrEqual(5);
			expect((await raw.send("FLUSHDB")).toString()).toBe("+OK\r\n");
			expect(integerReply(await raw.send("DBSIZE"))).toBe(0);
			for (const k of ["p5:k", "p5:l", "p5:h", "p5:s", "p5:z"]) {
				expect((await raw.send("TYPE", k)).toString()).toBe("+none\r\n");
			}

			await raw.send("SELECT", other);
			expect((await raw.bulk("GET", "p5:other"))?.toString()).toBe("keep");
			await raw.send("DEL", "p5:other");
			await raw.send("SELECT", half);
		});

		test("DBSIZE counts every type, not only strings", async () => {
			await raw.send("FLUSHDB");
			await raw.send("SET", "p5:k", "v");
			await raw.send("RPUSH", "p5:l", "a");
			await raw.send("HSET", "p5:h", "f", "v");
			await raw.send("SADD", "p5:s", "m");
			await raw.send("ZADD", "p5:z", "1", "m");
			expect(integerReply(await raw.send("DBSIZE"))).toBe(5);
			await raw.send("FLUSHDB");
		});

		// Redis pads the gap with NUL bytes, which no UTF-8 client can express
		// — so this goes through the raw socket and asserts on the bytes.
		test("SETRANGE overwrites, extends and pads with NUL", async () => {
			await raw.send("DEL", "p5:k");
			await raw.send("SET", "p5:k", "Hello World");
			expect(
				integerReply(await raw.send("SETRANGE", "p5:k", "6", "Redis")),
			).toBe(11);
			expect((await raw.bulk("GET", "p5:k"))?.toString()).toBe("Hello Redis");

			await raw.send("DEL", "p5:k");
			expect(integerReply(await raw.send("SETRANGE", "p5:k", "5", "abc"))).toBe(
				8,
			);
			const padded = await raw.bulk("GET", "p5:k");
			expect(padded?.length).toBe(8);
			expect([...(padded ?? [])]).toEqual([0, 0, 0, 0, 0, 97, 98, 99]);

			// An empty value writes nothing and reports the length as it stands.
			expect(integerReply(await raw.send("SETRANGE", "p5:k", "0", ""))).toBe(8);
			expect(
				integerReply(await raw.send("SETRANGE", "p5:absent", "0", "")),
			).toBe(0);
			expect(integerReply(await raw.send("EXISTS", "p5:absent"))).toBe(0);
			await raw.send("DEL", "p5:k");
		});

		test("GETRANGE clamps rather than erroring", async () => {
			await raw.send("DEL", "p5:k");
			await raw.send("SET", "p5:k", "Hello");
			const range = async (a: string, b: string) =>
				(await raw.bulk("GETRANGE", "p5:k", a, b))?.toString();
			expect(await range("0", "-1")).toBe("Hello");
			expect(await range("0", "4")).toBe("Hello");
			expect(await range("-3", "-1")).toBe("llo");
			expect(await range("0", "100")).toBe("Hello");
			expect(await range("10", "20")).toBe("");
			expect(await range("3", "1")).toBe("");
			expect(
				(await raw.bulk("GETRANGE", "p5:absent", "0", "-1"))?.toString(),
			).toBe("");
			await raw.send("DEL", "p5:k");
		});

		test("HINCRBYFLOAT creates, increments and refuses a non-float", async () => {
			await raw.send("DEL", "p5:h");
			expect(
				(await raw.bulk("HINCRBYFLOAT", "p5:h", "f", "10.5"))?.toString(),
			).toBe("10.5");
			expect(
				(await raw.bulk("HINCRBYFLOAT", "p5:h", "f", "0.1"))?.toString(),
			).toBe("10.6");
			expect(
				(await raw.bulk("HINCRBYFLOAT", "p5:h", "f", "-5"))?.toString(),
			).toBe("5.6");
			await raw.send("HSET", "p5:h", "word", "abc");
			expect(
				(await raw.send("HINCRBYFLOAT", "p5:h", "word", "1")).toString(),
			).toMatch(/^-ERR/);
			// The refused field keeps its value, and no empty field is left.
			expect((await raw.bulk("HGET", "p5:h", "word"))?.toString()).toBe("abc");
			expect(integerReply(await raw.send("HLEN", "p5:h"))).toBe(2);
			await raw.send("DEL", "p5:h");
		});

		test("RENAMENX renames only onto a free name", async () => {
			await raw.send("DEL", "p5:a", "p5:b", "p5:c");
			await raw.send("SET", "p5:a", "1");
			expect(integerReply(await raw.send("RENAMENX", "p5:a", "p5:b"))).toBe(1);
			expect((await raw.bulk("GET", "p5:b"))?.toString()).toBe("1");
			expect(integerReply(await raw.send("EXISTS", "p5:a"))).toBe(0);

			await raw.send("SET", "p5:c", "3");
			expect(integerReply(await raw.send("RENAMENX", "p5:c", "p5:b"))).toBe(0);
			expect((await raw.bulk("GET", "p5:b"))?.toString()).toBe("1");
			expect((await raw.bulk("GET", "p5:c"))?.toString()).toBe("3");

			expect(errorReply(await raw.send("RENAMENX", "p5:missing", "p5:b"))).toBe(
				"ERR no such key",
			);
			await raw.send("DEL", "p5:b", "p5:c");
		});

		test("GETEX reads the value and moves the expiry", async () => {
			await raw.send("DEL", "p5:k");
			await raw.send("SET", "p5:k", "v");
			await raw.send("EXPIRE", "p5:k", "100");
			// No option: the expiry is left exactly as it was.
			expect((await raw.bulk("GETEX", "p5:k"))?.toString()).toBe("v");
			expect(integerReply(await raw.send("TTL", "p5:k"))).toBeGreaterThan(0);

			expect((await raw.bulk("GETEX", "p5:k", "PERSIST"))?.toString()).toBe(
				"v",
			);
			expect(integerReply(await raw.send("TTL", "p5:k"))).toBe(-1);

			expect((await raw.bulk("GETEX", "p5:k", "EX", "50"))?.toString()).toBe(
				"v",
			);
			const ttl = integerReply(await raw.send("TTL", "p5:k"));
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(50);

			expect(await raw.bulk("GETEX", "p5:absent")).toBeNull();
			await raw.send("DEL", "p5:k");
		});

		test("RPOPLPUSH moves the tail onto the head", async () => {
			await raw.send("DEL", "p5:src", "p5:dst");
			await raw.send("RPUSH", "p5:src", "a", "b", "c");
			expect(
				(await raw.bulk("RPOPLPUSH", "p5:src", "p5:dst"))?.toString(),
			).toBe("c");
			expect(
				(await raw.bulk("RPOPLPUSH", "p5:src", "p5:dst"))?.toString(),
			).toBe("b");
			expect(
				(await raw.array("LRANGE", "p5:dst", "0", "-1")).map((b) =>
					b?.toString(),
				),
			).toEqual(["b", "c"]);
			expect(
				(await raw.array("LRANGE", "p5:src", "0", "-1")).map((b) =>
					b?.toString(),
				),
			).toEqual(["a"]);
			// A missing source is nil, and creates nothing.
			expect(await raw.bulk("RPOPLPUSH", "p5:missing", "p5:dst")).toBeNull();
			await raw.send("DEL", "p5:src", "p5:dst");
		});

		test("SETBIT, GETBIT and BITCOUNT count from the most significant bit", async () => {
			await raw.send("DEL", "p5:b");
			// Bit 0 is 0x80 of byte 0, not 0x01 — the opposite of PostgreSQL.
			expect(integerReply(await raw.send("SETBIT", "p5:b", "0", "1"))).toBe(0);
			expect([...((await raw.bulk("GET", "p5:b")) ?? [])]).toEqual([0x80]);
			expect(integerReply(await raw.send("GETBIT", "p5:b", "0"))).toBe(1);
			expect(integerReply(await raw.send("GETBIT", "p5:b", "1"))).toBe(0);
			// Setting it back reports the bit as it was.
			expect(integerReply(await raw.send("SETBIT", "p5:b", "0", "0"))).toBe(1);
			expect([...((await raw.bulk("GET", "p5:b")) ?? [])]).toEqual([0x00]);
			// A far offset grows the value with NUL bytes.
			await raw.send("DEL", "p5:b");
			expect(integerReply(await raw.send("SETBIT", "p5:b", "15", "1"))).toBe(0);
			expect([...((await raw.bulk("GET", "p5:b")) ?? [])]).toEqual([
				0x00, 0x01,
			]);
			// Past the end reads zero rather than erroring.
			expect(integerReply(await raw.send("GETBIT", "p5:b", "999"))).toBe(0);
			expect(integerReply(await raw.send("GETBIT", "p5:absent", "0"))).toBe(0);

			// Redis's own documented examples.
			await raw.send("SET", "p5:fb", "foobar");
			expect(integerReply(await raw.send("BITCOUNT", "p5:fb"))).toBe(26);
			expect(integerReply(await raw.send("BITCOUNT", "p5:fb", "0", "0"))).toBe(
				4,
			);
			expect(integerReply(await raw.send("BITCOUNT", "p5:fb", "1", "1"))).toBe(
				6,
			);
			expect(
				integerReply(await raw.send("BITCOUNT", "p5:fb", "5", "30", "BIT")),
			).toBe(17);
			expect(integerReply(await raw.send("BITCOUNT", "p5:absent"))).toBe(0);
			await raw.send("DEL", "p5:b", "p5:fb");
		});

		test("OBJECT ENCODING reports Redis's representation", async () => {
			await raw.send(
				"DEL",
				"p5:k",
				"p5:n",
				"p5:big",
				"p5:h",
				"p5:s",
				"p5:i",
				"p5:l",
				"p5:z",
			);
			await raw.send("SET", "p5:k", "hello");
			await raw.send("SET", "p5:n", "12345");
			await raw.send("SET", "p5:big", "x".repeat(45));
			await raw.send("HSET", "p5:h", "f", "v");
			await raw.send("SADD", "p5:s", "a", "b");
			await raw.send("SADD", "p5:i", "1", "2");
			await raw.send("RPUSH", "p5:l", "a");
			await raw.send("ZADD", "p5:z", "1", "m");
			const enc = async (k: string) =>
				(await raw.send("OBJECT", "ENCODING", k))
					.toString()
					.replace(/^\+|\r\n$/g, "");
			expect(await enc("p5:k")).toBe("embstr");
			expect(await enc("p5:n")).toBe("int");
			expect(await enc("p5:big")).toBe("raw");
			expect(await enc("p5:h")).toBe("listpack");
			expect(await enc("p5:s")).toBe("listpack");
			expect(await enc("p5:i")).toBe("intset");
			expect(await enc("p5:l")).toBe("listpack");
			expect(await enc("p5:z")).toBe("listpack");
			// A missing key is nil here, not the error every other command gives.
			expect(await raw.bulk("OBJECT", "ENCODING", "p5:absent")).toBeNull();
			await raw.send(
				"DEL",
				"p5:k",
				"p5:n",
				"p5:big",
				"p5:h",
				"p5:s",
				"p5:i",
				"p5:l",
				"p5:z",
			);
		});

		test("HRANDFIELD takes distinct fields unless the count is negative", async () => {
			await raw.send("DEL", "p5:h");
			await raw.send("HSET", "p5:h", "a", "1", "b", "2");
			expect((await raw.bulk("HRANDFIELD", "p5:h"))?.length).toBeGreaterThan(0);
			expect((await raw.array("HRANDFIELD", "p5:h", "1")).length).toBe(1);
			// Capped at what the hash holds...
			expect((await raw.array("HRANDFIELD", "p5:h", "5")).length).toBe(2);
			// ...unless the count is negative, which repeats.
			expect((await raw.array("HRANDFIELD", "p5:h", "-5")).length).toBe(5);
			expect(
				(await raw.array("HRANDFIELD", "p5:h", "2", "WITHVALUES")).length,
			).toBe(4);
			expect(await raw.bulk("HRANDFIELD", "p5:absent")).toBeNull();
			expect((await raw.array("HRANDFIELD", "p5:absent", "3")).length).toBe(0);
			await raw.send("DEL", "p5:h");
		});

		test("SINTERCARD counts the intersection and honours LIMIT", async () => {
			await raw.send("DEL", "p5:s1", "p5:s2");
			await raw.send("SADD", "p5:s1", "a", "b", "c");
			await raw.send("SADD", "p5:s2", "b", "c", "d");
			expect(
				integerReply(await raw.send("SINTERCARD", "2", "p5:s1", "p5:s2")),
			).toBe(2);
			expect(
				integerReply(
					await raw.send("SINTERCARD", "2", "p5:s1", "p5:s2", "LIMIT", "1"),
				),
			).toBe(1);
			// LIMIT 0 means no limit, not an empty answer.
			expect(
				integerReply(
					await raw.send("SINTERCARD", "2", "p5:s1", "p5:s2", "LIMIT", "0"),
				),
			).toBe(2);
			expect(
				integerReply(await raw.send("SINTERCARD", "2", "p5:s1", "p5:absent")),
			).toBe(0);
			await raw.send("DEL", "p5:s1", "p5:s2");
		});

		test("COPY duplicates a key of any type, and declines an occupied name", async () => {
			await raw.send("DEL", "p5:src", "p5:dst");
			await raw.send("SET", "p5:src", "v");
			expect(integerReply(await raw.send("COPY", "p5:src", "p5:dst"))).toBe(1);
			expect((await raw.bulk("GET", "p5:dst"))?.toString()).toBe("v");
			// The destination is occupied now, so this declines...
			await raw.send("SET", "p5:src", "w");
			expect(integerReply(await raw.send("COPY", "p5:src", "p5:dst"))).toBe(0);
			expect((await raw.bulk("GET", "p5:dst"))?.toString()).toBe("v");
			// ...unless REPLACE says otherwise.
			expect(
				integerReply(await raw.send("COPY", "p5:src", "p5:dst", "REPLACE")),
			).toBe(1);
			expect((await raw.bulk("GET", "p5:dst"))?.toString()).toBe("w");
			expect(integerReply(await raw.send("COPY", "p5:absent", "p5:dst2"))).toBe(
				0,
			);

			// Every type, not just strings.
			await raw.send("DEL", "p5:l", "p5:l2", "p5:h", "p5:h2", "p5:z", "p5:z2");
			await raw.send("RPUSH", "p5:l", "a", "b", "c");
			expect(integerReply(await raw.send("COPY", "p5:l", "p5:l2"))).toBe(1);
			expect(
				(await raw.array("LRANGE", "p5:l2", "0", "-1")).map((b) =>
					b?.toString(),
				),
			).toEqual(["a", "b", "c"]);
			await raw.send("HSET", "p5:h", "f", "v");
			expect(integerReply(await raw.send("COPY", "p5:h", "p5:h2"))).toBe(1);
			expect((await raw.bulk("HGET", "p5:h2", "f"))?.toString()).toBe("v");
			await raw.send("ZADD", "p5:z", "1.5", "m");
			expect(integerReply(await raw.send("COPY", "p5:z", "p5:z2"))).toBe(1);
			expect((await raw.bulk("ZSCORE", "p5:z2", "m"))?.toString()).toBe("1.5");
			await raw.send(
				"DEL",
				"p5:src",
				"p5:dst",
				"p5:l",
				"p5:l2",
				"p5:h",
				"p5:h2",
				"p5:z",
				"p5:z2",
			);
		});

		test("an unknown command names the arguments it was given", async () => {
			expect(errorReply(await raw.send("NOSUCHCOMMAND"))).toBe(
				"ERR unknown command 'NOSUCHCOMMAND', with args beginning with: ",
			);
			expect(errorReply(await raw.send("NOSUCHCOMMAND", "arg"))).toBe(
				"ERR unknown command 'NOSUCHCOMMAND', with args beginning with: 'arg' ",
			);
		});
	});
}

for (const half of ["cache", "durable"]) {
	describe(`Key type discipline (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
		});
		afterAll(async () => {
			await raw.send("SELECT", "durable");
		});

		// Each of the five tables was keyed independently and none asked the
		// others, so one key could hold a string and a list and a set at once.
		test("a key holds one type, and the others are refused", async () => {
			await raw.send("DEL", "p4:k");
			await raw.send("SET", "p4:k", "v");
			for (const cmd of [
				["LPUSH", "p4:k", "a"],
				["RPUSH", "p4:k", "a"],
				["SADD", "p4:k", "m"],
				["HSET", "p4:k", "f", "v"],
				["ZADD", "p4:k", "1", "m"],
				["LLEN", "p4:k"],
				["SMEMBERS", "p4:k"],
				["HGETALL", "p4:k"],
				["ZCARD", "p4:k"],
			]) {
				expect(errorReply(await raw.send(...cmd))).toBe(
					"WRONGTYPE Operation against a key holding the wrong kind of value",
				);
			}
			expect((await raw.send("TYPE", "p4:k")).toString()).toBe("+string\r\n");
			expect((await raw.bulk("GET", "p4:k"))?.toString()).toBe("v");
			await raw.send("DEL", "p4:k");
		});

		test("reading a collection as a string is refused too", async () => {
			await raw.send("DEL", "p4:l");
			await raw.send("RPUSH", "p4:l", "a");
			for (const cmd of [
				["GET", "p4:l"],
				["INCR", "p4:l"],
				["APPEND", "p4:l", "x"],
				["STRLEN", "p4:l"],
				["GETDEL", "p4:l"],
			]) {
				expect(errorReply(await raw.send(...cmd))).toBe(
					"WRONGTYPE Operation against a key holding the wrong kind of value",
				);
			}
			expect(integerReply(await raw.send("LLEN", "p4:l"))).toBe(1);
			await raw.send("DEL", "p4:l");
		});

		// Redis replaces rather than refuses here, and the old value goes.
		test("SET replaces a key of any type, and its old value is gone", async () => {
			await raw.send("DEL", "p4:k");
			await raw.send("RPUSH", "p4:k", "a", "b");
			expect((await raw.send("SET", "p4:k", "v")).toString()).toBe("+OK\r\n");
			expect((await raw.send("TYPE", "p4:k")).toString()).toBe("+string\r\n");
			expect((await raw.bulk("GET", "p4:k"))?.toString()).toBe("v");
			// The list must not still be sitting behind the string.
			await raw.send("DEL", "p4:k");
			expect(integerReply(await raw.send("LLEN", "p4:k"))).toBe(0);
		});

		// NX and XX decide on whether the key is there, so clearing it up front
		// would answer their own question for them.
		test("SET NX declines over a collection and leaves it whole", async () => {
			await raw.send("DEL", "p4:k");
			await raw.send("RPUSH", "p4:k", "a");
			expect((await raw.send("SET", "p4:k", "v", "NX")).toString()).toBe(
				"$-1\r\n",
			);
			expect((await raw.send("TYPE", "p4:k")).toString()).toBe("+list\r\n");
			expect(integerReply(await raw.send("LLEN", "p4:k"))).toBe(1);
			// XX takes the opposite branch: the key exists, so the write lands.
			expect((await raw.send("SET", "p4:k", "v", "XX")).toString()).toBe(
				"+OK\r\n",
			);
			expect((await raw.send("TYPE", "p4:k")).toString()).toBe("+string\r\n");
			await raw.send("DEL", "p4:k");
		});

		// Only the KV row had a column for an expiry, so every cache-with-a-TTL
		// pattern over a collection kept its data forever.
		test("a collection carries an expiry, and PERSIST clears it", async () => {
			for (const [make, len] of [
				[
					["RPUSH", "p4:e", "a"],
					["LLEN", "p4:e"],
				],
				[
					["SADD", "p4:e", "m"],
					["SCARD", "p4:e"],
				],
				[
					["HSET", "p4:e", "f", "v"],
					["HLEN", "p4:e"],
				],
				[
					["ZADD", "p4:e", "1", "m"],
					["ZCARD", "p4:e"],
				],
			] as const) {
				await raw.send("DEL", "p4:e");
				await raw.send(...make);
				expect(integerReply(await raw.send("EXPIRE", "p4:e", "100"))).toBe(1);
				const ttl = integerReply(await raw.send("TTL", "p4:e"));
				expect(ttl).toBeGreaterThan(0);
				expect(ttl).toBeLessThanOrEqual(100);
				expect(integerReply(await raw.send("PERSIST", "p4:e"))).toBe(1);
				expect(integerReply(await raw.send("TTL", "p4:e"))).toBe(-1);
				expect(integerReply(await raw.send(...len))).toBe(1);
				await raw.send("DEL", "p4:e");
			}
		});

		test("an expired collection is gone, not merely untyped", async () => {
			await raw.send("DEL", "p4:e");
			await raw.send("RPUSH", "p4:e", "a", "b");
			expect(integerReply(await raw.send("EXPIRE", "p4:e", "1"))).toBe(1);
			await Bun.sleep(1200);

			// Key-level reads are exact the moment the deadline passes: they go
			// through the directory, which treats a passed expiry as absent.
			expect(integerReply(await raw.send("TTL", "p4:e"))).toBe(-2);
			expect((await raw.send("TYPE", "p4:e")).toString()).toBe("+none\r\n");
			expect(integerReply(await raw.send("EXISTS", "p4:e"))).toBe(0);

			// `LLEN` reads the list's own meta and does not consult the
			// directory, so the count survives until the sweep runs — up to a
			// second later. Redis expires lazily on access and has no such
			// window; see "An expired collection outlives its own expiry" in
			// docs/IMPLEMENTATION.md §12. Polled rather than slept on, so this
			// asserts the end state without pinning the sweep's phase.
			for (
				let i = 0;
				i < 40 && integerReply(await raw.send("LLEN", "p4:e"));
				i++
			) {
				await Bun.sleep(100);
			}
			expect(integerReply(await raw.send("LLEN", "p4:e"))).toBe(0);

			// And the key is free for another type to take.
			expect(integerReply(await raw.send("SADD", "p4:e", "m"))).toBe(1);
			await raw.send("DEL", "p4:e");
		});

		test("replacing a collection discards the expiry with the value", async () => {
			await raw.send("DEL", "p4:a", "p4:b", "p4:dst");
			await raw.send("SADD", "p4:a", "x", "y");
			await raw.send("SADD", "p4:b", "y", "z");
			await raw.send("SINTERSTORE", "p4:dst", "p4:a", "p4:b");
			expect(integerReply(await raw.send("EXPIRE", "p4:dst", "100"))).toBe(1);
			expect(integerReply(await raw.send("TTL", "p4:dst"))).toBeGreaterThan(0);
			await raw.send("SUNIONSTORE", "p4:dst", "p4:a", "p4:b");
			expect(integerReply(await raw.send("SCARD", "p4:dst"))).toBe(3);
			expect(integerReply(await raw.send("TTL", "p4:dst"))).toBe(-1);
			await raw.send("DEL", "p4:a", "p4:b", "p4:dst");
		});

		// The hash table had no per-key count, so nothing knew its last field
		// had gone; the key lingered and TYPE still called it a hash.
		test("emptying a collection removes the key", async () => {
			await raw.send("DEL", "p4:h", "p4:s", "p4:z", "p4:l");
			await raw.send("HSET", "p4:h", "f", "v");
			expect(integerReply(await raw.send("HDEL", "p4:h", "f"))).toBe(1);
			expect((await raw.send("TYPE", "p4:h")).toString()).toBe("+none\r\n");
			expect(integerReply(await raw.send("EXISTS", "p4:h"))).toBe(0);
			// So the name is free for another type.
			expect(integerReply(await raw.send("RPUSH", "p4:h", "a"))).toBe(1);

			await raw.send("SADD", "p4:s", "m");
			await raw.send("SREM", "p4:s", "m");
			expect((await raw.send("TYPE", "p4:s")).toString()).toBe("+none\r\n");

			await raw.send("ZADD", "p4:z", "1", "m");
			await raw.send("ZREM", "p4:z", "m");
			expect((await raw.send("TYPE", "p4:z")).toString()).toBe("+none\r\n");

			await raw.send("RPUSH", "p4:l", "a");
			await raw.send("LPOP", "p4:l");
			expect((await raw.send("TYPE", "p4:l")).toString()).toBe("+none\r\n");
			await raw.send("DEL", "p4:h", "p4:s", "p4:z", "p4:l");
		});

		test("HLEN counts the fields of its own key only", async () => {
			await raw.send("DEL", "p4:h1", "p4:h2");
			await raw.send("HSET", "p4:h1", "a", "1", "b", "2");
			await raw.send("HSET", "p4:h2", "c", "3");
			expect(integerReply(await raw.send("HLEN", "p4:h1"))).toBe(2);
			expect(integerReply(await raw.send("HLEN", "p4:h2"))).toBe(1);
			await raw.send("HDEL", "p4:h1", "a");
			expect(integerReply(await raw.send("HLEN", "p4:h1"))).toBe(1);
			expect(integerReply(await raw.send("HLEN", "p4:h2"))).toBe(1);
			expect(integerReply(await raw.send("HLEN", "p4:absent"))).toBe(0);
			await raw.send("DEL", "p4:h1", "p4:h2");
		});
	});
}

for (const half of ["cache", "durable"]) {
	describe(`Multi-argument and overwrite semantics (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
		});
		afterAll(async () => {
			await raw.send("SELECT", "durable");
		});

		test("HINCRBY creates, increments and refuses a non-integer", async () => {
			await raw.send("DEL", "p0:h");
			expect(integerReply(await raw.send("HINCRBY", "p0:h", "n", "5"))).toBe(5);
			expect(integerReply(await raw.send("HINCRBY", "p0:h", "n", "-2"))).toBe(
				3,
			);
			expect((await raw.bulk("HGET", "p0:h", "n"))?.toString()).toBe("3");
			await raw.send("HSET", "p0:h", "word", "abc");
			expect(
				(await raw.send("HINCRBY", "p0:h", "word", "1")).toString(),
			).toMatch(/^-ERR/);
			expect((await raw.bulk("HGET", "p0:h", "word"))?.toString()).toBe("abc");
			await raw.send("DEL", "p0:h");
		});

		test("HSET counts the fields it added, not the ones it wrote", async () => {
			await raw.send("DEL", "p0:h");
			expect(
				(await raw.send("HSET", "p0:h", "a", "1", "b", "2")).toString(),
			).toBe(":2\r\n");
			expect(integerReply(await raw.send("HSET", "p0:h", "a", "9"))).toBe(0);
			expect(
				(await raw.send("HSET", "p0:h", "a", "8", "c", "3")).toString(),
			).toBe(":1\r\n");
			expect((await raw.bulk("HGET", "p0:h", "a"))?.toString()).toBe("8");
			await raw.send("DEL", "p0:h");
		});

		// `ON CONFLICT DO UPDATE` cannot touch a row twice in one statement, so
		// these raised. Redis takes the last of each.
		test("an argument list may name the same key twice", async () => {
			await raw.send("DEL", "p0:k", "p0:h", "p0:z");
			expect(
				(await raw.send("MSET", "p0:k", "v1", "p0:k", "v2")).toString(),
			).toBe("+OK\r\n");
			expect((await raw.bulk("GET", "p0:k"))?.toString()).toBe("v2");

			expect(
				(await raw.send("HSET", "p0:h", "f", "v1", "f", "v2")).toString(),
			).toBe(":1\r\n");
			expect((await raw.bulk("HGET", "p0:h", "f"))?.toString()).toBe("v2");

			expect(
				(await raw.send("ZADD", "p0:z", "1", "m", "2", "m")).toString(),
			).toBe(":1\r\n");
			expect((await raw.bulk("ZSCORE", "p0:z", "m"))?.toString()).toBe("2");
			await raw.send("DEL", "p0:k", "p0:h", "p0:z");
		});

		test("EXISTS counts each argument", async () => {
			await raw.send("DEL", "p0:k");
			await raw.send("SET", "p0:k", "v");
			expect(integerReply(await raw.send("EXISTS", "p0:k", "p0:k"))).toBe(2);
			expect(integerReply(await raw.send("EXISTS", "p0:k", "p0:missing"))).toBe(
				1,
			);
			await raw.send("DEL", "p0:k");
		});

		test("LINDEX past either end is nil, not an error", async () => {
			await raw.send("DEL", "p0:l");
			await raw.send("RPUSH", "p0:l", "a");
			expect(await raw.bulk("LINDEX", "p0:l", "-100")).toBeNull();
			expect(await raw.bulk("LINDEX", "p0:l", "100")).toBeNull();
			expect((await raw.bulk("LINDEX", "p0:l", "-1"))?.toString()).toBe("a");
			await raw.send("DEL", "p0:l");
		});

		test("RENAME replaces its destination and carries the expiry", async () => {
			await raw.send("DEL", "p0:src", "p0:dst");
			await raw.send("SET", "p0:src", "one", "EX", "100");
			await raw.send("SET", "p0:dst", "two");
			expect((await raw.send("RENAME", "p0:src", "p0:dst")).toString()).toBe(
				"+OK\r\n",
			);
			expect((await raw.bulk("GET", "p0:dst"))?.toString()).toBe("one");
			expect(integerReply(await raw.send("EXISTS", "p0:src"))).toBe(0);
			// The expiry moves with the key rather than being dropped in transit.
			const ttl = Number(
				(await raw.send("TTL", "p0:dst")).toString().replace(/[^0-9-]/g, ""),
			);
			expect(ttl).toBeGreaterThan(0);

			// A rename that finds no source must leave the destination alone.
			expect(
				(await raw.send("RENAME", "p0:missing", "p0:dst")).toString(),
			).toMatch(/^-ERR no such key/);
			expect((await raw.bulk("GET", "p0:dst"))?.toString()).toBe("one");

			// ...and a key renamed to itself survives it.
			expect((await raw.send("RENAME", "p0:dst", "p0:dst")).toString()).toBe(
				"+OK\r\n",
			);
			expect((await raw.bulk("GET", "p0:dst"))?.toString()).toBe("one");
			await raw.send("DEL", "p0:dst");
		});

		// The insert's uniqueness check still sees rows the same statement
		// deleted, so a destination that overlapped the result lost the overlap.
		test("a store command replaces a destination that overlaps its result", async () => {
			await raw.send("DEL", "p0:s1", "p0:s2", "p0:dst");
			await raw.send("SADD", "p0:s1", "a", "b", "c");
			await raw.send("SADD", "p0:s2", "b", "c", "d");
			expect(
				(await raw.send("SINTERSTORE", "p0:dst", "p0:s1", "p0:s2")).toString(),
			).toBe(":2\r\n");
			expect(
				(await raw.send("SUNIONSTORE", "p0:dst", "p0:s1", "p0:s2")).toString(),
			).toBe(":4\r\n");
			expect(integerReply(await raw.send("SCARD", "p0:dst"))).toBe(4);
			const members = (await raw.array("SMEMBERS", "p0:dst"))
				.map((m) => m?.toString())
				.sort();
			expect(members).toEqual(["a", "b", "c", "d"]);

			expect(
				(await raw.send("SDIFFSTORE", "p0:dst", "p0:s1", "p0:s2")).toString(),
			).toBe(":1\r\n");
			expect(integerReply(await raw.send("SCARD", "p0:dst"))).toBe(1);
			await raw.send("DEL", "p0:s1", "p0:s2", "p0:dst");
		});

		test("GETSET discards the expiry with the value it replaces", async () => {
			await raw.send("DEL", "p0:k");
			await raw.send("SET", "p0:k", "v", "EX", "100");
			expect((await raw.bulk("GETSET", "p0:k", "v2"))?.toString()).toBe("v");
			expect(integerReply(await raw.send("TTL", "p0:k"))).toBe(-1);
			await raw.send("DEL", "p0:k");
		});
	});
}

describe("Value size limits", () => {
	// These limits belong to the ephemeral half. On db 8 every value is a
	// bytea column with no cap, so the whole describe would pass vacuously.
	beforeAll(async () => {
		await raw.send("SELECT", "cache");
	});
	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	test("string values round-trip byte for byte at every boundary", async () => {
		for (const n of VALUE_SIZES) {
			const v = payload(n);
			await raw.send("SET", "vs:str", v);
			expect(await raw.bulk("GET", "vs:str")).toEqual(v);
			expect((await raw.send("STRLEN", "vs:str")).toString()).toBe(`:${n}\r\n`);
		}
		await raw.send("DEL", "vs:str");
	});

	test("hash values round-trip byte for byte at every boundary", async () => {
		for (const n of VALUE_SIZES) {
			const v = payload(n);
			await raw.send("HSET", "vs:hash", "f", v);
			expect(await raw.bulk("HGET", "vs:hash", "f")).toEqual(v);
		}
		await raw.send("DEL", "vs:hash");
	});

	test("list values round-trip byte for byte at every boundary", async () => {
		for (const n of VALUE_SIZES) {
			const v = payload(n);
			await raw.send("DEL", "vs:list");
			await raw.send("RPUSH", "vs:list", v);
			expect(await raw.bulk("LINDEX", "vs:list", "0")).toEqual(v);
		}
		await raw.send("DEL", "vs:list");
	});

	// Shrinking a value has to release the chunks it used to own. Leaving them
	// chained to the entry makes the next read splice a stale tail onto the new
	// value — and never returns them to the pool.
	test("shrinking a value past the inline boundary drops its overflow tail", async () => {
		const big = payload(300);
		const small = payload(10);
		await raw.send("SET", "vs:shrink", big);
		expect(await raw.bulk("GET", "vs:shrink")).toEqual(big);
		await raw.send("SET", "vs:shrink", small);
		expect(await raw.bulk("GET", "vs:shrink")).toEqual(small);

		await raw.send("HSET", "vs:shrinkh", "f", big);
		await raw.send("HSET", "vs:shrinkh", "f", small);
		expect(await raw.bulk("HGET", "vs:shrinkh", "f")).toEqual(small);

		await raw.send("DEL", "vs:shrink");
		await raw.send("DEL", "vs:shrinkh");
	});

	test("APPEND growing a value across the inline boundary keeps every byte", async () => {
		await raw.send("DEL", "vs:app");
		const head = payload(60);
		const tail = payload(30);
		await raw.send("SET", "vs:app", head);
		await raw.send("APPEND", "vs:app", tail);
		expect(await raw.bulk("GET", "vs:app")).toEqual(
			Buffer.concat([head, tail]),
		);
		await raw.send("DEL", "vs:app");
	});

	// Every APPEND past the inline slot is a read-modify-write that releases the
	// whole chain and allocates a longer one. Sixteen of them in a row walk a
	// value from one chunk to a thousand, so a chain rebuilt one link short —
	// or one that keeps the released chunks — shows up as wrong bytes here
	// rather than as a pool that quietly runs down.
	test("APPEND grows a value one chunk at a time to the cap", async () => {
		await raw.send("DEL", "vs:grow");
		const step = MAX_TOTAL_VAL_LEN / 16;
		let expected = Buffer.alloc(0);
		for (let i = 0; i < 16; i++) {
			const part = payload(step);
			await raw.send(i === 0 ? "SET" : "APPEND", "vs:grow", part);
			expected = Buffer.concat([expected, part]);
			expect((await raw.send("STRLEN", "vs:grow")).toString()).toBe(
				`:${expected.length}\r\n`,
			);
		}
		expect(await raw.bulk("GET", "vs:grow")).toEqual(expected);
		await raw.send("DEL", "vs:grow");
	});

	// One reply, several chains walked back to back. A chain walk that reads one
	// link too far only shows up when the next value's chunks are adjacent to
	// this one's — which is what allocating them in sequence arranges.
	test("commands returning several large values splice each chain correctly", async () => {
		const vals = [payload(MAX_TOTAL_VAL_LEN / 2), payload(300), payload(7000)];

		// biome-ignore format: a key and its value per line reads worse than the row
		await raw.send("MSET", "vs:m0", vals[0], "vs:m1", vals[1], "vs:m2", vals[2]);
		expect(await raw.array("MGET", "vs:m0", "vs:m1", "vs:m2")).toEqual(vals);

		await raw.send("DEL", "vs:mh");
		await raw.send("HSET", "vs:mh", "a", vals[0], "b", vals[1], "c", vals[2]);
		// HMGET, not HVALS: a hash has no field order to assert against.
		expect(await raw.array("HMGET", "vs:mh", "a", "b", "c")).toEqual(vals);

		await raw.send("DEL", "vs:ml");
		await raw.send("RPUSH", "vs:ml", vals[0], vals[1], vals[2]);
		expect(await raw.array("LRANGE", "vs:ml", "0", "-1")).toEqual(vals);

		await raw.send("DEL", "vs:m0", "vs:m1", "vs:m2", "vs:mh", "vs:ml");
	});

	// The pool is finite, so filling it has to end in a refusal rather than a
	// truncated value or a dead worker — and deleting what filled it has to
	// hand every chunk back. Written as a loop rather than a fixed count so it
	// keeps testing the property if `redis.mem_max_entries` ever changes.
	test("a full chunk pool refuses cleanly and recovers on delete", async () => {
		if (!memoryMode) return; // db 0 is a table here; nothing is pooled

		const big = payload(MAX_TOTAL_VAL_LEN);
		const stored: string[] = [];
		let refusal = "";

		for (let i = 0; i < 64 && !refusal; i++) {
			const key = `vs:fill:${i}`;
			const reply = (await raw.send("SET", key, big)).toString();
			if (reply === "+OK\r\n") stored.push(key);
			else refusal = reply;
		}

		expect(refusal).toMatch(/^-OOM /);
		expect(stored.length).toBeGreaterThan(1);
		// Nothing that was stored was damaged by the write that failed.
		expect(await raw.bulk("GET", stored[0])).toEqual(big);
		expect(await raw.bulk("GET", stored[stored.length - 1])).toEqual(big);
		// ...and the key the refusal names was never created.
		expect(await raw.bulk("GET", `vs:fill:${stored.length}`)).toBeNull();

		for (const key of stored) await raw.send("DEL", key);

		// Every chunk is back: the pool takes the same number of values again.
		for (const key of stored) {
			expect((await raw.send("SET", key, big)).toString()).toBe("+OK\r\n");
		}
		for (const key of stored) await raw.send("DEL", key);
	});

	// Storing the first key of an MSET and then reporting OOM is what the
	// pre-check exists to prevent. Filling the pool is the cheap way there.
	test("an MSET that does not fit stores none of it", async () => {
		if (!memoryMode) return; // db 0 is a table here; nothing is pooled

		const big = payload(MAX_TOTAL_VAL_LEN);
		const stored: string[] = [];
		for (let i = 0; i < 64; i++) {
			const key = `vs:atomic:${i}`;
			if ((await raw.send("SET", key, big)).toString() !== "+OK\r\n") break;
			stored.push(key);
		}
		expect(stored.length).toBeGreaterThan(1);

		// One value's worth of room was just freed, so an MSET of two is short.
		await raw.send("DEL", stored.pop() as string);
		const reply = (
			await raw.send("MSET", "vs:atomic:a", big, "vs:atomic:b", big)
		).toString();
		expect(reply).toMatch(/^-OOM /);
		expect(await raw.bulk("GET", "vs:atomic:a")).toBeNull();
		expect(await raw.bulk("GET", "vs:atomic:b")).toBeNull();

		// ...and a full table still takes an MSET that only overwrites, because
		// the check counts the keys that are actually absent.
		expect(
			(
				await raw.send("MSET", stored[0], "small", stored[1], "small")
			).toString(),
		).toBe("+OK\r\n");
		for (const key of stored) await raw.send("DEL", key);
	});

	// The pool is finite and shared by every value in its table, so a path that
	// drops a value without returning its chunks is invisible until the pool
	// runs dry. Each of these rewrites far more than a pool's worth of chunks
	// through one key: with any of the overwrite, delete or replace paths
	// leaking, the run stops storing values partway through.
	test("rewriting a large value many times does not exhaust the chunk pool", async () => {
		const big = payload(MAX_TOTAL_VAL_LEN);
		const rounds = 24;

		for (let i = 0; i < rounds; i++) {
			await raw.send("SET", "vs:churn", big);
			await raw.send("DEL", "vs:churn");
		}
		await raw.send("SET", "vs:churn", big);
		expect(await raw.bulk("GET", "vs:churn")).toEqual(big);
		await raw.send("DEL", "vs:churn");

		for (let i = 0; i < rounds; i++) {
			await raw.send("HSET", "vs:churnh", "f", big);
			await raw.send("HDEL", "vs:churnh", "f");
		}
		await raw.send("HSET", "vs:churnh", "f", big);
		expect(await raw.bulk("HGET", "vs:churnh", "f")).toEqual(big);
		await raw.send("DEL", "vs:churnh");

		for (let i = 0; i < rounds; i++) {
			await raw.send("RPUSH", "vs:churnl", big);
			await raw.send("LPOP", "vs:churnl");
		}
		await raw.send("RPUSH", "vs:churnl", big);
		expect(await raw.bulk("LINDEX", "vs:churnl", "0")).toEqual(big);
		await raw.send("DEL", "vs:churnl");

		// The expiry sweep frees values nobody deleted, and has to return their
		// chunks too. Four of these fill half the pool; the eight that follow
		// only fit if the sweep gave them back. 1.5s covers the sweep's own
		// one-second period.
		for (let i = 0; i < 4; i++) {
			await raw.send("SET", `vs:exp:${i}`, big, "PX", "50");
		}
		await Bun.sleep(1500);
		for (let i = 0; i < 8; i++) {
			expect((await raw.send("SET", `vs:swept:${i}`, big)).toString()).toBe(
				"+OK\r\n",
			);
		}
		expect(await raw.bulk("GET", "vs:swept:7")).toEqual(big);
		for (let i = 0; i < 8; i++) await raw.send("DEL", `vs:swept:${i}`);
	});

	// A KV entry owns two chains: its value's and its key's. Both have to come
	// back when the entry goes. A leaked key chain is invisible until the pool
	// is dry, so the only way to see it is to churn enough keys to empty one —
	// 512-byte keys are 6 chunks each, so 4,000 of them is three pools' worth.
	test("churning long keys does not exhaust the chunk pool", async () => {
		if (!memoryMode) return; // db 0 is a table here; nothing is pooled
		const longKey = (i: number) => {
			const k = Buffer.alloc(MEM_MAX_KEY, 0x6b);
			k.write(`churnkey:${i}:`, 0);
			return k;
		};
		for (let i = 0; i < 4000; i++) {
			await raw.send("SET", longKey(i), "v");
			await raw.send("DEL", longKey(i));
		}
		// If either chain leaked, the pool is empty by now and this is an OOM.
		await raw.send("SET", longKey(0), payload(MAX_TOTAL_VAL_LEN / 8));
		expect(await raw.bulk("GET", longKey(0))).toEqual(
			payload(MAX_TOTAL_VAL_LEN / 8),
		);
		// ...and the key still reads back whole, 512 bytes of it.
		const listed = await raw.array("KEYS", Buffer.from("churnkey:0:*"));
		expect(listed.map((k) => k?.toString("hex"))).toEqual([
			longKey(0).toString("hex"),
		]);
		await raw.send("DEL", longKey(0));
	});

	test("a value one byte over the limit is refused in memory mode, stored otherwise", async () => {
		const over = payload(MAX_TOTAL_VAL_LEN + 1);
		await raw.send("DEL", "vs:over");
		const reply = (await raw.send("SET", "vs:over", over)).toString();
		if (memoryMode) {
			expect(reply).toMatch(/limit/);
			// A refusal must not leave a partial value behind.
			expect(await raw.bulk("GET", "vs:over")).toBeNull();
		} else {
			expect(reply).toBe("+OK\r\n");
			expect(await raw.bulk("GET", "vs:over")).toEqual(over);
		}
		await raw.send("DEL", "vs:over");
	});

	test("hash fields and keys are bounded the same way", async () => {
		const okField = payload(MAX_MEMBER_LEN);
		const bigField = payload(MAX_MEMBER_LEN + 1);
		const okKey = payload(MEM_MAX_KEY);
		const bigKey = payload(MEM_MAX_KEY + 1);

		await raw.send("HSET", "vs:lim", okField, "v");
		expect((await raw.bulk("HGET", "vs:lim", okField))?.toString()).toBe("v");
		await raw.send("SET", okKey, "v");
		expect((await raw.bulk("GET", okKey))?.toString()).toBe("v");

		const fieldReply = (
			await raw.send("HSET", "vs:lim", bigField, "v")
		).toString();
		const keyReply = (await raw.send("SET", bigKey, "v")).toString();
		if (memoryMode) {
			expect(fieldReply).toMatch(/limit/);
			expect(keyReply).toMatch(/limit/);
			// The over-long key must not have collided onto its own prefix.
			expect((await raw.bulk("GET", okKey))?.toString()).toBe("v");
		} else {
			expect(fieldReply).toMatch(/^:/);
			expect(keyReply).toBe("+OK\r\n");
			await raw.send("DEL", bigKey);
		}

		await raw.send("DEL", "vs:lim");
		await raw.send("DEL", okKey);
	});
});

describe("Pub/Sub table routing", () => {
	const ROUTE_TABLE = "pg_redis_e2e_route_test";

	beforeAll(async () => {
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(ROUTE_TABLE)}`;
		await sql`SELECT redis.create_pubsub_table('public', ${ROUTE_TABLE})`;
		await sql`SELECT redis.route_publish('e2e-route-ch', 'public', ${ROUTE_TABLE})`;
	});

	afterAll(async () => {
		await sql`SELECT redis.unroute_publish('e2e-route-ch')`;
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(ROUTE_TABLE)}`;
	});

	test("PUBLISH to routed channel inserts a row in the target table", async () => {
		await client.send("PUBLISH", ["e2e-route-ch", "hello-world"]);
		// Give the BGW dispatcher time to process the fire-and-forget INSERT
		await Bun.sleep(500);
		const rows =
			await sql`SELECT convert_from(channel, 'UTF8') AS channel, convert_from(payload, 'UTF8') AS payload FROM public.${sql.unsafe(ROUTE_TABLE)} WHERE channel = 'e2e-route-ch'`;
		expect(rows.length).toBeGreaterThanOrEqual(1);
		expect(rows[0].payload).toBe("hello-world");
	});

	test("PUBLISH to unrouted channel does not insert a row", async () => {
		const before =
			await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ROUTE_TABLE)}`;
		await client.send("PUBLISH", ["no-route-ch", "ignored"]);
		await Bun.sleep(300);
		const after =
			await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ROUTE_TABLE)}`;
		expect(Number(after[0].n)).toBe(Number(before[0].n));
	});

	test("unroute_publish stops future inserts", async () => {
		const TABLE2 = "pg_redis_e2e_route_test2";
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(TABLE2)}`;
		await sql`SELECT redis.create_pubsub_table('public', ${TABLE2})`;
		await sql`SELECT redis.route_publish('e2e-tmp-ch', 'public', ${TABLE2})`;
		await sql`SELECT redis.unroute_publish('e2e-tmp-ch')`;
		await client.send("PUBLISH", ["e2e-tmp-ch", "should-not-appear"]);
		await Bun.sleep(300);
		const rows =
			await sql`SELECT count(*) AS n FROM public.${sql.unsafe(TABLE2)}`;
		expect(Number(rows[0].n)).toBe(0);
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(TABLE2)}`;
	});
});

describe("Pub/Sub multi-channel persistency", () => {
	const ORDERS_TABLE = "pubsub_e2e_orders";
	const ALERTS_TABLE = "pubsub_e2e_alerts";
	const AUDIT_TABLE = "pubsub_e2e_audit";

	beforeAll(async () => {
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(ORDERS_TABLE)}`;
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(ALERTS_TABLE)}`;
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(AUDIT_TABLE)}`;

		await sql`SELECT redis.create_pubsub_table('public', ${ORDERS_TABLE})`;
		await sql`SELECT redis.create_pubsub_table('public', ${ALERTS_TABLE})`;
		await sql`SELECT redis.create_pubsub_table('public', ${AUDIT_TABLE})`;

		// orders channel → orders table (dedicated)
		await sql`SELECT redis.route_publish('orders', 'public', ${ORDERS_TABLE})`;
		// alerts channel → alerts table (dedicated)
		await sql`SELECT redis.route_publish('alerts', 'public', ${ALERTS_TABLE})`;
		// two channels sharing the same audit table
		await sql`SELECT redis.route_publish('user.created', 'public', ${AUDIT_TABLE})`;
		await sql`SELECT redis.route_publish('user.deleted', 'public', ${AUDIT_TABLE})`;
	});

	afterAll(async () => {
		await sql`SELECT redis.unroute_publish('orders')`;
		await sql`SELECT redis.unroute_publish('alerts')`;
		await sql`SELECT redis.unroute_publish('user.created')`;
		await sql`SELECT redis.unroute_publish('user.deleted')`;
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(ORDERS_TABLE)}`;
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(ALERTS_TABLE)}`;
		await sql`DROP TABLE IF EXISTS public.${sql.unsafe(AUDIT_TABLE)}`;
	});

	test("each channel routes only to its own dedicated table", async () => {
		await client.send("PUBLISH", ["orders", "order-123"]);
		await client.send("PUBLISH", ["alerts", "alert-456"]);
		await Bun.sleep(500);

		const orderRows =
			await sql`SELECT convert_from(channel, 'UTF8') AS channel, convert_from(payload, 'UTF8') AS payload FROM public.${sql.unsafe(ORDERS_TABLE)}`;
		const alertRows =
			await sql`SELECT convert_from(channel, 'UTF8') AS channel, convert_from(payload, 'UTF8') AS payload FROM public.${sql.unsafe(ALERTS_TABLE)}`;

		expect(orderRows.length).toBe(1);
		expect(orderRows[0].channel).toBe("orders");
		expect(orderRows[0].payload).toBe("order-123");

		expect(alertRows.length).toBe(1);
		expect(alertRows[0].channel).toBe("alerts");
		expect(alertRows[0].payload).toBe("alert-456");

		// cross-contamination check: orders payload must not appear in alerts table
		const crossCheck =
			await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ALERTS_TABLE)} WHERE channel = 'orders'`;
		expect(Number(crossCheck[0].n)).toBe(0);
	});

	test("multiple messages on the same channel all persist in order", async () => {
		await client.send("PUBLISH", ["orders", "order-A"]);
		await client.send("PUBLISH", ["orders", "order-B"]);
		await client.send("PUBLISH", ["orders", "order-C"]);
		await Bun.sleep(500);

		const rows =
			await sql`SELECT convert_from(payload, 'UTF8') AS payload FROM public.${sql.unsafe(ORDERS_TABLE)} WHERE channel = 'orders' ORDER BY inserted_at, id`;
		const payloads = rows.map((r: { payload: string }) => r.payload);
		expect(payloads).toContain("order-A");
		expect(payloads).toContain("order-B");
		expect(payloads).toContain("order-C");
	});

	test("two channels routing to the same table both write rows with correct channel labels", async () => {
		await client.send("PUBLISH", ["user.created", "user-1"]);
		await client.send("PUBLISH", ["user.deleted", "user-2"]);
		await client.send("PUBLISH", ["user.created", "user-3"]);
		await Bun.sleep(500);

		const createdRows =
			await sql`SELECT convert_from(payload, 'UTF8') AS payload FROM public.${sql.unsafe(AUDIT_TABLE)} WHERE channel = 'user.created' ORDER BY inserted_at, id`;
		const deletedRows =
			await sql`SELECT convert_from(payload, 'UTF8') AS payload FROM public.${sql.unsafe(AUDIT_TABLE)} WHERE channel = 'user.deleted' ORDER BY inserted_at, id`;

		expect(createdRows.length).toBe(2);
		expect(createdRows[0].payload).toBe("user-1");
		expect(createdRows[1].payload).toBe("user-3");

		expect(deletedRows.length).toBe(1);
		expect(deletedRows[0].payload).toBe("user-2");

		const total =
			await sql`SELECT count(*) AS n FROM public.${sql.unsafe(AUDIT_TABLE)}`;
		expect(Number(total[0].n)).toBe(3);
	});

	test("unrouted channel does not pollute any persistence table", async () => {
		const before = {
			orders: Number(
				(
					await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ORDERS_TABLE)}`
				)[0].n,
			),
			alerts: Number(
				(
					await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ALERTS_TABLE)}`
				)[0].n,
			),
			audit: Number(
				(
					await sql`SELECT count(*) AS n FROM public.${sql.unsafe(AUDIT_TABLE)}`
				)[0].n,
			),
		};

		await client.send("PUBLISH", ["unregistered.channel", "ghost-message"]);
		await Bun.sleep(300);

		const ordersAfter = Number(
			(
				await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ORDERS_TABLE)}`
			)[0].n,
		);
		const alertsAfter = Number(
			(
				await sql`SELECT count(*) AS n FROM public.${sql.unsafe(ALERTS_TABLE)}`
			)[0].n,
		);
		const auditAfter = Number(
			(
				await sql`SELECT count(*) AS n FROM public.${sql.unsafe(AUDIT_TABLE)}`
			)[0].n,
		);

		expect(ordersAfter).toBe(before.orders);
		expect(alertsAfter).toBe(before.alerts);
		expect(auditAfter).toBe(before.audit);
	});
});

describe("COPY across the storage halves", () => {
	beforeAll(async () => {
		await raw.send("SELECT", "cache");
		await raw.send("DEL", "p5:x", "p5:xl");
		await raw.send("SELECT", "durable");
		await raw.send("DEL", "p5:x", "p5:xl");
	});

	// The two halves are different storage engines; COPY is the only command
	// that reads from one and writes to the other.
	test("a cache key is promoted to the durable half and back", async () => {
		await raw.send("SELECT", "cache");
		await raw.send("SET", "p5:x", "cached");
		await raw.send("RPUSH", "p5:xl", "a", "b");
		expect(
			integerReply(await raw.send("COPY", "p5:x", "p5:x", "DB", "8")),
		).toBe(1);
		expect(
			integerReply(await raw.send("COPY", "p5:xl", "p5:xl", "DB", "8")),
		).toBe(1);

		await raw.send("SELECT", "durable");
		expect((await raw.bulk("GET", "p5:x"))?.toString()).toBe("cached");
		expect(
			(await raw.array("LRANGE", "p5:xl", "0", "-1")).map((b) => b?.toString()),
		).toEqual(["a", "b"]);

		// And the other way: durable back into the cache.
		await raw.send("SET", "p5:x", "durable-value");
		expect(
			integerReply(await raw.send("COPY", "p5:x", "p5:back", "DB", "0")),
		).toBe(1);
		await raw.send("SELECT", "cache");
		expect((await raw.bulk("GET", "p5:back"))?.toString()).toBe(
			"durable-value",
		);
		// The source is untouched by either direction.
		expect((await raw.bulk("GET", "p5:x"))?.toString()).toBe("cached");

		await raw.send("DEL", "p5:x", "p5:xl", "p5:back");
		await raw.send("SELECT", "durable");
		await raw.send("DEL", "p5:x", "p5:xl");
	});
});

// ─────────────────────── Concurrent clients ────────────────────────────────
//
// `redis.workers` defaults to 4, and a worker is a *process*: four dispatchers
// sharing one set of HTABs, pools and LWLocks in memory mode, and one set of
// tables on the durable half. Everything else in this file drives a single
// connection in sequence, so none of it has ever put two writers on the same
// key at once — the case the locking exists for.
//
// Each test runs on both halves. The durable half is not a formality: its
// concurrency comes from PostgreSQL's own MVCC and a different set of
// statements, so a defect on one half says nothing about the other.

/** Distinct connections, so the writes really do arrive in parallel. */
async function fanOut<T>(
	n: number,
	body: (c: Bun.RedisClient, i: number) => Promise<T>,
): Promise<T[]> {
	const clients = Array.from({ length: n }, () => new Bun.RedisClient(redisUrl));
	try {
		return await Promise.all(clients.map((c, i) => body(c, i)));
	} finally {
		for (const c of clients) c.close();
	}
}

/** Both storage halves, by the name `SELECT` takes. */
const HALVES = ["cache", "durable"] as const;

describe("Concurrent clients", () => {
	const WRITERS = 8;
	const PER_WRITER = 25;

	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	for (const half of HALVES) {
		test(`${half}: every increment of a shared counter lands`, async () => {
			const key = `conc:${half}:counter`;
			await raw.send("SELECT", half);
			await raw.send("DEL", key);

			await fanOut(WRITERS, async (c) => {
				await c.send("SELECT", [half]);
				for (let i = 0; i < PER_WRITER; i++) await c.send("INCR", [key]);
			});

			// A read-modify-write that is not atomic loses updates under
			// contention, and loses them silently: the reply to every INCR is
			// still a plausible number.
			expect((await raw.bulk("GET", key))?.toString()).toBe(
				String(WRITERS * PER_WRITER),
			);
			await raw.send("DEL", key);
		});

		test(`${half}: every element pushed to a shared list survives`, async () => {
			const key = `conc:${half}:list`;
			await raw.send("SELECT", half);
			await raw.send("DEL", key);

			await fanOut(WRITERS, async (c, w) => {
				await c.send("SELECT", [half]);
				for (let i = 0; i < PER_WRITER; i++)
					await c.send("RPUSH", [key, `${w}:${i}`]);
			});

			// Order across writers is not defined, so this asserts on the set.
			// LLEN reads the meta's count and LRANGE walks the entries, so
			// comparing them also catches a count that drifted from the data.
			const expected = new Set<string>();
			for (let w = 0; w < WRITERS; w++)
				for (let i = 0; i < PER_WRITER; i++) expected.add(`${w}:${i}`);

			expect(integerReply(await raw.send("LLEN", key))).toBe(expected.size);
			const got = (await raw.array("LRANGE", key, "0", "-1")).map((b) =>
				b?.toString(),
			);
			expect(got.length).toBe(expected.size);
			expect(new Set(got)).toEqual(expected);
			await raw.send("DEL", key);
		});

		test(`${half}: a shared set counts every distinct member once`, async () => {
			const key = `conc:${half}:set`;
			await raw.send("SELECT", half);
			await raw.send("DEL", key);

			// Half the members are written by two writers each, so SCARD has to
			// be right about duplicates as well as about totals.
			await fanOut(WRITERS, async (c, w) => {
				await c.send("SELECT", [half]);
				for (let i = 0; i < PER_WRITER; i++)
					await c.send("SADD", [key, `m:${(w % (WRITERS / 2)) * PER_WRITER + i}`]);
			});

			const distinct = (WRITERS / 2) * PER_WRITER;
			expect(integerReply(await raw.send("SCARD", key))).toBe(distinct);
			const members = (await raw.array("SMEMBERS", key)).map((b) =>
				b?.toString(),
			);
			expect(members.length).toBe(distinct);
			expect(new Set(members).size).toBe(distinct);
			await raw.send("DEL", key);
		});

		test(`${half}: concurrent hash fields all arrive, and HLEN agrees`, async () => {
			const key = `conc:${half}:hash`;
			await raw.send("SELECT", half);
			await raw.send("DEL", key);

			await fanOut(WRITERS, async (c, w) => {
				await c.send("SELECT", [half]);
				for (let i = 0; i < PER_WRITER; i++)
					await c.send("HSET", [key, `f:${w}:${i}`, `${w}-${i}`]);
				// One field every writer shares, incremented rather than set:
				// the count must not double-count a field written eight times.
				for (let i = 0; i < PER_WRITER; i++)
					await c.send("HINCRBY", [key, "shared", "1"]);
			});

			expect(integerReply(await raw.send("HLEN", key))).toBe(
				WRITERS * PER_WRITER + 1,
			);
			expect((await raw.bulk("HGET", key, "shared"))?.toString()).toBe(
				String(WRITERS * PER_WRITER),
			);
			await raw.send("DEL", key);
		});

		test(`${half}: a sorted set's cardinality matches its members`, async () => {
			const key = `conc:${half}:zset`;
			await raw.send("SELECT", half);
			await raw.send("DEL", key);

			await fanOut(WRITERS, async (c, w) => {
				await c.send("SELECT", [half]);
				for (let i = 0; i < PER_WRITER; i++)
					await c.send("ZADD", [key, String(w * PER_WRITER + i), `z:${w}:${i}`]);
			});

			// ZCARD is the meta's count and ZRANGE walks the entries; a member
			// added without the meta hearing about it shows up as a mismatch.
			const total = WRITERS * PER_WRITER;
			expect(integerReply(await raw.send("ZCARD", key))).toBe(total);
			const members = (await raw.array("ZRANGE", key, "0", "-1")).map((b) =>
				b?.toString(),
			);
			expect(members.length).toBe(total);
			expect(new Set(members).size).toBe(total);
			await raw.send("DEL", key);
		});

		test(`${half}: a key read while it is repeatedly rewritten is never torn`, async () => {
			const key = `conc:${half}:torn`;
			await raw.send("SELECT", half);
			await raw.send("DEL", key);

			// Sizes either side of the inline slot and of a chunk boundary, so
			// a reader can catch a writer between the inline prefix and the
			// pooled tail if the two are not written under one lock.
			const shapes = ["a".repeat(10), "b".repeat(300), "c".repeat(4096)];
			for (const s of shapes) await raw.send("SET", key, s);

			const seen = await fanOut(4, async (c, i) => {
				await c.send("SELECT", [half]);
				const bad: string[] = [];
				for (let n = 0; n < 60; n++) {
					if (i === 0) {
						await c.send("SET", [key, shapes[n % shapes.length]]);
						continue;
					}
					const v = (await c.send("GET", [key])) as string | null;
					// Every legal value is one character repeated. A read that
					// spliced two writes together is neither.
					if (v !== null && !shapes.includes(v)) bad.push(v.slice(0, 32));
				}
				return bad;
			});

			expect(seen.flat()).toEqual([]);
			await raw.send("DEL", key);
		});
	}
});

// ───────────────────── Pressure and FLUSHDB ────────────────────────────────
//
// The memory half has a fixed chunk pool, and `FLUSHDB` re-formats it rather
// than handing chains back one at a time. That made a removed entry outlive
// the chunks it named: the next key to land in its dynahash slot released them
// a second time, which spliced the free list into a cycle and inflated its
// count. The next large write walked off the end of the list and the worker
// segfaulted — taking the postmaster, and every other connection, with it.
//
// Nothing here is memory-specific to *write*, so both halves run it: on db 8
// it is an ordinary data-integrity check, and on db 0 it is the regression.

describe("Pressure and FLUSHDB", () => {
	// Past the 64-byte inline slot by several chunks, so every value owns a
	// pooled tail. A value inside the inline slot exercises none of this.
	const BIG = "v".repeat(4096);

	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	for (const half of HALVES) {
		test(`${half}: keys written after a flush read back whole`, async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");

			// Fill, flush, refill. The refill is what lands in the slots the
			// flush vacated, and what used to inherit their chains.
			for (let round = 0; round < 3; round++) {
				for (let i = 0; i < 40; i++)
					await raw.send("SET", `flush:${round}:${i}`, BIG);
				await raw.send("FLUSHDB");
				expect(integerReply(await raw.send("DBSIZE"))).toBe(0);
			}

			for (let i = 0; i < 40; i++)
				await raw.send("SET", `after:${i}`, `${i}:${BIG}`);
			for (let i = 0; i < 40; i++) {
				const v = await raw.bulk("GET", `after:${i}`);
				expect(v?.toString()).toBe(`${i}:${BIG}`);
			}
			await raw.send("FLUSHDB");
		});

		test(`${half}: collections written after a flush read back whole`, async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");

			const M = "m".repeat(400);
			for (let round = 0; round < 3; round++) {
				for (let i = 0; i < 20; i++) {
					await raw.send("RPUSH", `fl:list:${i}`, BIG);
					await raw.send("HSET", `fl:hash:${i}`, `f${i}`, BIG);
					await raw.send("SADD", `fl:set:${i}`, `${i}:${M}`);
					await raw.send("ZADD", `fl:zset:${i}`, "1", `${i}:${M}`);
				}
				await raw.send("FLUSHDB");
			}

			for (let i = 0; i < 20; i++) {
				await raw.send("RPUSH", `post:list:${i}`, `${i}:${BIG}`);
				await raw.send("HSET", `post:hash:${i}`, "f", `${i}:${BIG}`);
				await raw.send("SADD", `post:set:${i}`, `${i}:${M}`);
			}
			for (let i = 0; i < 20; i++) {
				expect(
					(await raw.bulk("LINDEX", `post:list:${i}`, "0"))?.toString(),
				).toBe(`${i}:${BIG}`);
				expect((await raw.bulk("HGET", `post:hash:${i}`, "f"))?.toString()).toBe(
					`${i}:${BIG}`,
				);
				expect(
					integerReply(await raw.send("SISMEMBER", `post:set:${i}`, `${i}:${M}`)),
				).toBe(1);
			}
			await raw.send("FLUSHDB");
		});

		test(`${half}: an exhausted pool refuses rather than corrupting`, async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");

			// Write until something is refused, then prove the refusal was
			// clean: everything already stored still reads back byte for byte.
			// On db 8 nothing is refused and the loop just stores them all,
			// which is the correct answer for a half with no such limit.
			const stored: number[] = [];
			let refusal: string | null = null;
			for (let i = 0; i < 200; i++) {
				const rep = (await raw.send("SET", `oom:${i}`, `${i}:${BIG}`)).toString();
				if (rep.startsWith("+OK")) stored.push(i);
				else {
					refusal = rep;
					break;
				}
			}
			if (refusal !== null) {
				expect(refusal).toMatch(/^-(OOM|ERR)/);
				expect(half).toBe("cache");
			}
			for (const i of stored) {
				expect((await raw.bulk("GET", `oom:${i}`))?.toString()).toBe(
					`${i}:${BIG}`,
				);
			}

			// And freeing must make the pool usable again, not merely present.
			for (const i of stored) await raw.send("DEL", `oom:${i}`);
			expect((await raw.send("SET", "oom:again", BIG)).toString()).toBe(
				"+OK\r\n",
			);
			expect((await raw.bulk("GET", "oom:again"))?.toString()).toBe(BIG);
			await raw.send("FLUSHDB");
		});
	}
});

// ─────────────────── Expiry and collections ────────────────────────────────
//
// A collection's rows do not carry its expiry. On the memory half the deadline
// is in the key directory; on the durable half it is a row in `redis.expiry_N`.
// Either way a sweep reclaims the rows about once a second, and until it runs
// the key is in two states at once: `TYPE` and `EXISTS` call it gone, because
// they read the deadline, while `LLEN`, `SMEMBERS`, `HGETALL` and `ZRANGE` read
// the rows and find them still there.
//
// That is the one case where a client is served data for a key the same server
// reports as absent, which is worse than either answer alone.

describe("Expiry and collections", () => {
	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	/// Drive `key` past its deadline and hand back the moment `TYPE` first says
	/// `none`. Throws rather than returning if that never happens, so a test
	/// built on it cannot pass by never reaching the window it is about.
	async function pastItsExpiry(key: string, build: string[]) {
		for (let attempt = 0; attempt < 40; attempt++) {
			await raw.send("DEL", key);
			await raw.send(...build);
			await raw.send("PEXPIRE", key, "30");
			const deadline = Date.now() + 800;
			while (Date.now() < deadline) {
				if (/none/.test((await raw.send("TYPE", key)).toString())) return;
			}
		}
		throw new Error(`${key} never read as expired`);
	}

	const cases: [string, string, string[], [string, string[]][]][] = [
		["a list", "x:l", ["RPUSH", "x:l", "a", "b", "c"], [
			["LLEN", ["LLEN", "x:l"]],
			["LRANGE", ["LRANGE", "x:l", "0", "-1"]],
			["LINDEX", ["LINDEX", "x:l", "0"]]]],
		["a set", "x:s", ["SADD", "x:s", "m1", "m2"], [
			["SCARD", ["SCARD", "x:s"]],
			["SMEMBERS", ["SMEMBERS", "x:s"]],
			["SISMEMBER", ["SISMEMBER", "x:s", "m1"]],
			["SRANDMEMBER", ["SRANDMEMBER", "x:s"]]]],
		["a hash", "x:h", ["HSET", "x:h", "f", "v"], [
			["HLEN", ["HLEN", "x:h"]],
			["HGETALL", ["HGETALL", "x:h"]],
			["HGET", ["HGET", "x:h", "f"]],
			["HKEYS", ["HKEYS", "x:h"]]]],
		["a sorted set", "x:z", ["ZADD", "x:z", "1", "m"], [
			["ZCARD", ["ZCARD", "x:z"]],
			["ZRANGE", ["ZRANGE", "x:z", "0", "-1"]],
			["ZSCORE", ["ZSCORE", "x:z", "m"]],
			["ZCOUNT", ["ZCOUNT", "x:z", "-inf", "+inf"]]]],
	];

	for (const half of HALVES) {
		for (const [label, key, build, reads] of cases) {
			test(`${half}: ${label} past its expiry reads empty, not stale`, async () => {
				await raw.send("SELECT", half);
				await pastItsExpiry(key, build);

				// TYPE has just said `none`. Every read of the same key has to
				// agree, before the sweep has run and without waiting for it.
				expect(integerReply(await raw.send("EXISTS", key))).toBe(0);
				for (const [name, cmd] of reads) {
					const rep = (await raw.send(...cmd)).toString();
					const isEmpty =
						rep === ":0\r\n" || rep === "$-1\r\n" || rep === "*0\r\n" || rep === "*-1\r\n";
					expect(`${name}=${rep.replace(/\r\n/g, "|")}`).toBe(
						`${name}=${isEmpty ? rep.replace(/\r\n/g, "|") : "<empty>"}`,
					);
				}
				await raw.send("DEL", key);
			});
		}

		test(`${half}: a string past its expiry stays consistent too`, async () => {
			await raw.send("SELECT", half);
			await pastItsExpiry("x:str", ["SET", "x:str", "sv"]);
			expect(await raw.bulk("GET", "x:str")).toBeNull();
			expect(integerReply(await raw.send("STRLEN", "x:str"))).toBe(0);
			expect(integerReply(await raw.send("EXISTS", "x:str"))).toBe(0);
		});

		test(`${half}: a key rewritten after expiring is the new key only`, async () => {
			await raw.send("SELECT", half);
			await pastItsExpiry("x:re", ["RPUSH", "x:re", "old1", "old2", "old3"]);

			// The reap must not leave the old members behind for the new key,
			// and the new key must not inherit the old deadline.
			await raw.send("RPUSH", "x:re", "new");
			expect(integerReply(await raw.send("LLEN", "x:re"))).toBe(1);
			expect(
				(await raw.array("LRANGE", "x:re", "0", "-1")).map((b) => b?.toString()),
			).toEqual(["new"]);
			expect(integerReply(await raw.send("TTL", "x:re"))).toBe(-1);
			await raw.send("DEL", "x:re");
		});
	}
});
