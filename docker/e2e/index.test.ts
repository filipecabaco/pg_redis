import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { decode, errorReply, integerReply, RawRedis, type Resp, scanReply } from "./resp";

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

beforeAll(async () => {
	client = new Bun.RedisClient(redisUrl);
	raw = await new RawRedis().connect();
});

afterAll(async () => {
	raw.close();
	client.close();
	await sql.end();
});

describe("Authentication", () => {
	// As in Redis: only AUTH, HELLO, QUIT and RESET answer before AUTH.
	test("PING before AUTH returns NOAUTH", async () => {
		const noAuthClient = new Bun.RedisClient(
			`redis://${REDIS_HOST}:${REDIS_PORT}`,
		);
		try {
			await expect(noAuthClient.ping()).rejects.toThrow(/NOAUTH/);
		} finally {
			noAuthClient.close();
		}
	});

	// Redis refuses a HELLO that neither comes authenticated nor carries AUTH,
	// and leaves the protocol where it was. It used to switch the connection
	// to RESP3 and describe the server to anyone.
	test("HELLO before AUTH returns Redis's NOAUTH and keeps RESP2", async () => {
		const conn = await new RawRedis().connect({ password: "" });
		try {
			for (const args of [["HELLO"], ["HELLO", "3"]]) {
				const reply = (await conn.send(...args)).toString();
				expect(reply).toStartWith("-NOAUTH HELLO must be called with the client already authenticated");
			}
			expect((await conn.send("HELLO", "3", "AUTH", "default", "nope")).toString()).toStartWith(
				"-WRONGPASS",
			);
			// Still RESP2: a RESP3 null would be `_`.
			expect((await conn.send("PING")).toString()).toStartWith("-NOAUTH");
		} finally {
			conn.close();
		}
	});

	// The server says what Redis says: client libraries branch on HELLO's
	// `server` field, and tools watch INFO's run_id to notice a restart.
	test("HELLO and INFO identify the server as Redis does", async () => {
		const conn = await new RawRedis().connect();
		try {
			const hello = (await conn.send("HELLO", "2")).toString();
			expect(hello).toContain("$6\r\nserver\r\n$5\r\nredis\r\n");
			const info = (await conn.bulk("INFO", "server"))?.toString() ?? "";
			expect(info).toMatch(/run_id:[0-9a-f]{40}\r\n/);
			expect(info).not.toContain(`run_id:${"0".repeat(40)}`);
			expect(info).not.toContain("os:PostgreSQL");
		} finally {
			conn.close();
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

// Autovacuum decides what to vacuum and analyze from pg_stat_user_tables. A
// worker that never reported its writes there left every logged table
// unvacuumed however much it churned.
describe("Table statistics", () => {
	test("the workers' writes reach pg_stat_user_tables", async () => {
		const db15 = new Bun.RedisClient(`${redisUrl}/15`);
		const counted = async () =>
			(
				await sql`SELECT (n_tup_ins + n_tup_upd + n_tup_del)::int AS n
				          FROM pg_stat_user_tables WHERE relid = 'redis.kv_15'::regclass`
			)[0].n;
		const before = await counted();
		for (let i = 0; i < 20; i++) await db15.set(`stats:${i}`, "v");
		await db15.send("DEL", Array.from({ length: 20 }, (_, i) => `stats:${i}`));
		db15.close();
		// Sent at most once a second, and read back from a snapshot that a
		// new transaction refreshes.
		let after = before;
		for (let t = 0; t < 30 && after < before + 40; t++) {
			await Bun.sleep(200);
			after = await counted();
		}
		expect(after).toBeGreaterThanOrEqual(before + 40);
	});
});

// Two logged databases swap their tables, and a logged FLUSHDB truncates
// them: no key is moved or deleted row by row.
describe("SWAPDB and FLUSHDB on logged databases", () => {
	const A = 13;
	const B = 14;
	let conn: RawRedis;

	beforeAll(async () => {
		conn = await new RawRedis().connect();
		for (const db of [A, B]) {
			await conn.send("SELECT", String(db));
			await conn.send("FLUSHDB");
		}
	});

	afterAll(async () => {
		for (const db of [A, B]) {
			await conn.send("SELECT", String(db));
			await conn.send("FLUSHDB");
		}
		conn.close();
	});

	test("SWAPDB moves every type, TTLs included, and the views follow", async () => {
		await conn.send("SELECT", String(A));
		await conn.send("SET", "swap:s", "in-a", "EX", "1000");
		await conn.send("RPUSH", "swap:l", "1", "2");
		await conn.send("HSET", "swap:h", "f", "v");
		await conn.send("SELECT", String(B));
		await conn.send("SADD", "swap:set", "m");

		expect((await conn.send("SWAPDB", String(A), String(B))).toString()).toBe("+OK\r\n");

		// The connection is still on B, which now holds what A did.
		expect((await conn.bulk("GET", "swap:s"))?.toString()).toBe("in-a");
		expect(integerReply(await conn.send("TTL", "swap:s"))).toBeGreaterThan(900);
		expect(integerReply(await conn.send("LLEN", "swap:l"))).toBe(2);
		expect((await conn.bulk("HGET", "swap:h", "f"))?.toString()).toBe("v");
		expect(integerReply(await conn.send("EXISTS", "swap:set"))).toBe(0);
		await conn.send("SELECT", String(A));
		expect(integerReply(await conn.send("SISMEMBER", "swap:set", "m"))).toBe(1);
		expect(integerReply(await conn.send("DBSIZE"))).toBe(1);

		const rows = await sql`SELECT db, key FROM redis.keys WHERE db IN (${A}, ${B}) ORDER BY db, key`;
		expect(rows.map((r: { db: number; key: string }) => `${r.db}:${r.key}`)).toEqual([
			`${A}:swap:set`,
			`${B}:swap:h`,
			`${B}:swap:l`,
			`${B}:swap:s`,
		]);
		// Every index is named after the table it is on.
		const misnamed = await sql`
			SELECT indexname FROM pg_indexes
			WHERE schemaname = 'redis' AND tablename ~ ${`_(${A}|${B})$`}
			  AND indexname NOT LIKE tablename || '\\_%'`;
		expect(misnamed.length).toBe(0);
	});

	test("SWAPDB breaks a WATCH on a key either database held, and no other", async () => {
		const watcher = await new RawRedis().connect();
		const run = async (key: string) => {
			await watcher.send("SELECT", String(A));
			await watcher.send("WATCH", key);
			await conn.send("SWAPDB", String(A), String(B));
			await watcher.send("MULTI");
			await watcher.send("PING");
			return (await watcher.send("EXEC")).toString();
		};
		await conn.send("SELECT", String(A));
		await conn.send("FLUSHDB");
		await conn.send("SELECT", String(B));
		await conn.send("FLUSHDB");

		await conn.send("SELECT", String(A));
		await conn.send("SET", "w:here", "1");
		expect(await run("w:here")).toBe("*-1\r\n"); // held by A, now in B

		await conn.send("SELECT", String(A));
		await conn.send("SET", "w:there", "1"); // A holds it; the swap moves it to B
		await conn.send("SWAPDB", String(A), String(B));
		// Now B holds w:there and A does not: watching it on A, missing, the
		// swap brings it in.
		expect(await run("w:there")).toBe("*-1\r\n");

		expect(await run("w:nowhere")).toBe("*1\r\n+PONG\r\n");
		watcher.close();
	});

	test("FLUSHDB empties its database's tables and leaves the other's", async () => {
		await conn.send("SELECT", String(A));
		await conn.send("FLUSHDB");
		await conn.send("SELECT", String(B));
		await conn.send("FLUSHDB");
		await conn.send("SET", "keep", "1");
		await conn.send("SELECT", String(A));
		for (let i = 0; i < 50; i++) await conn.send("HSET", `f:${i}`, "a", "b");
		await conn.send("FLUSHDB");
		expect(integerReply(await conn.send("DBSIZE"))).toBe(0);
		const [{ n }] = await sql`SELECT count(*)::int AS n FROM redis.hashes WHERE db = ${A}`;
		expect(n).toBe(0);
		await conn.send("SELECT", String(B));
		expect((await conn.bulk("GET", "keep"))?.toString()).toBe("1");
	});

	test("writers on both databases keep going through FLUSHDB and SWAPDB", async () => {
		const writers = Array.from({ length: 8 }, (_, i) => new Bun.RedisClient(`${redisUrl}/${i % 2 ? A : B}`));
		let stop = false;
		const failures: string[] = [];
		const write = async (c: Bun.RedisClient, i: number) => {
			for (let n = 0; !stop; n++) {
				try {
					await c.set(`load:${i}:${n % 20}`, "v");
					await c.send("HSET", [`load:h:${i}`, `f${n % 20}`, "v"]);
				} catch (e) {
					failures.push(String(e));
				}
			}
		};
		const load = writers.map(write);
		for (let round = 0; round < 20; round++) {
			await conn.send("SELECT", String(round % 2 ? A : B));
			const r = (await conn.send(round % 3 ? "SWAPDB" : "FLUSHDB", ...(round % 3 ? [String(A), String(B)] : []))).toString();
			expect(r).toBe("+OK\r\n");
		}
		stop = true;
		await Promise.all(load);
		for (const c of writers) c.close();
		expect(failures).toEqual([]);
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
		// Bun negotiates RESP3, where HGETALL is a map, as Redis replies.
		expect(await client.hgetall("myhash")).toEqual(
			{ f1: "v1", f2: "v2", f3: "v3" },
		);
	});

	test("HDEL single", async () => {
		expect(await client.send("HDEL", ["myhash", "f1"])).toBe(1);
	});

	test("HDEL missing", async () => {
		expect(await client.send("HDEL", ["myhash", "f1"])).toBe(0);
	});

	test("HGETALL after del", async () => {
		expect(await client.hgetall("myhash")).toEqual({ f2: "v2", f3: "v3" });
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

	test("COMMAND describes every accepted command", async () => {
		const all = (await client.send("COMMAND", [])) as unknown[];
		expect(all.length).toBe(await client.send("COMMAND", ["COUNT"]));
	});

	// The number of rows in docs/command-coverage.md, which a unit test pins
	// to the parser in both directions. A change here means a command was
	// added or removed: update the page and this number together.
	test("COMMAND COUNT reports the number of accepted commands", async () => {
		expect(await client.send("COMMAND", ["COUNT"])).toBe(237);
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

	// LPUSH grows a list down through negative positions. A renumber parks
	// rows at pos + 2^62 and brings back those at or past 2^62, which missed
	// every parked row that had started below zero: the next LINSERT hit
	// the primary key. Redis's list.tcl opens with this sequence.
	for (const half of ["cache", "durable"]) {
		test(`LINSERT and LREM on a list built by LPUSH (${half})`, async () => {
			await raw.send("SELECT", half);
			await raw.send("DEL", "lpushins");
			await raw.send("LPUSH", "lpushins", "a", "b", "c", "d");
			for (const [i, v] of ["e", "f", "g", "j"].entries()) {
				expect(
					(await raw.send("LINSERT", "lpushins", "AFTER", "d", v)).toString(),
				).toBe(`:${5 + i}\r\n`);
			}
			expect(
				(await raw.send("LREM", "lpushins", "0", "g")).toString(),
			).toBe(":1\r\n");
			for (const want of ["d", "j", "f", "e", "c", "b", "a"]) {
				expect(await raw.bulk("LPOP", "lpushins")).toEqual(Buffer.from(want));
			}
			await raw.send("SELECT", "durable");
		});
	}

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
		expect(await client.send("ZSCORE", ["zadd-z", "b"])).toBe(2);
		expect(await client.send("ZCARD", ["zadd-z"])).toBe(3);
	});

	test("ZADD upsert returns added count only, updates existing score", async () => {
		expect(await client.send("ZADD", ["zadd-z", "5", "a"])).toBe(0);
		expect(await client.send("ZSCORE", ["zadd-z", "a"])).toBe(5);
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
		expect(await client.send("ZSCORE", ["zadd-nx", "a"])).toBe(1);

		await client.send("ZADD", ["zadd-xx", "1", "a"]);
		expect(
			await client.send("ZADD", ["zadd-xx", "XX", "2", "a", "3", "b"]),
		).toBe(0);
		expect(await client.send("ZSCORE", ["zadd-xx", "a"])).toBe(2);
		expect(await client.send("ZSCORE", ["zadd-xx", "b"])).toBeNull();
	});

	test("ZADD GT only raises score, LT only lowers score", async () => {
		await client.send("ZADD", ["zadd-gt", "5", "a"]);
		await client.send("ZADD", ["zadd-gt", "GT", "3", "a"]);
		expect(await client.send("ZSCORE", ["zadd-gt", "a"])).toBe(5);
		await client.send("ZADD", ["zadd-gt", "GT", "10", "a"]);
		expect(await client.send("ZSCORE", ["zadd-gt", "a"])).toBe(10);

		await client.send("ZADD", ["zadd-lt", "5", "a"]);
		await client.send("ZADD", ["zadd-lt", "LT", "10", "a"]);
		expect(await client.send("ZSCORE", ["zadd-lt", "a"])).toBe(5);
		await client.send("ZADD", ["zadd-lt", "LT", "1", "a"]);
		expect(await client.send("ZSCORE", ["zadd-lt", "a"])).toBe(1);
	});

	test("ZADD INCR returns new score", async () => {
		await client.send("DEL", ["zadd-incr"]);
		expect(await client.send("ZADD", ["zadd-incr", "INCR", "5", "a"])).toBe(5);
		expect(await client.send("ZADD", ["zadd-incr", "INCR", "3", "a"])).toBe(8);
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
		).toEqual([1, null, 2]);
	});

	test("ZINCRBY increments score (creates member if absent)", async () => {
		expect(await client.send("ZINCRBY", ["zincrby-z", "5", "a"])).toBe(5);
		expect(await client.send("ZINCRBY", ["zincrby-z", "3", "a"])).toBe(8);
		expect(await client.send("ZINCRBY", ["zincrby-z", "-2", "a"])).toBe(6);
	});

	// ZINCRBY's return value alone says nothing about the metadata. A member
	// that reaches the entry table and not the meta is listed by ZRANGE, counted
	// 0 by ZCARD, invisible to ZPOPMIN, and leaves the count wrong for the life
	// of the key — so assert through all three.
	test("a member ZINCRBY creates is counted and can be popped", async () => {
		await client.send("DEL", ["zincrby-meta"]);
		expect(await client.send("ZINCRBY", ["zincrby-meta", "5", "alice"])).toBe(5);
		expect(await client.send("ZCARD", ["zincrby-meta"])).toBe(1);
		expect(await client.send("ZRANGE", ["zincrby-meta", "0", "-1"])).toEqual([
			"alice",
		]);
		expect(await client.send("ZPOPMIN", ["zincrby-meta"])).toEqual(["alice", 5]);
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
		expect(await client.send("ZINCRBY", ["zincrby-ext", "10", "a"])).toBe(11);
		expect(await client.send("ZPOPMAX", ["zincrby-ext"])).toEqual(["a", 11]);
		// `b` is now the min; drop it below and the min has to follow.
		expect(await client.send("ZINCRBY", ["zincrby-ext", "-5", "b"])).toBe(-3);
		expect(await client.send("ZPOPMIN", ["zincrby-ext"])).toEqual(["b", -3]);
		// And the min moving *off* a member re-derives from the entries.
		expect(await client.send("ZCARD", ["zincrby-ext"])).toBe(1);
		expect(await client.send("ZPOPMIN", ["zincrby-ext"])).toEqual(["c", 3]);
		await client.send("DEL", ["zincrby-ext"]);
	});

	// ZADD ... INCR reaches the same code path and needs the same guard.
	test("a member ZADD INCR creates is counted", async () => {
		await client.send("DEL", ["zaddincr-meta"]);
		expect(
			await client.send("ZADD", ["zaddincr-meta", "INCR", "7", "solo"]),
		).toBe(7);
		expect(await client.send("ZCARD", ["zaddincr-meta"])).toBe(1);
		expect(await client.send("ZPOPMAX", ["zaddincr-meta"])).toEqual(["solo", 7]);
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

	// Under RESP3 a scored reply is a list of [member, score] pairs.
	test("ZRANGE WITHSCORES pairs each member with its score", async () => {
		expect(
			await client.send("ZRANGE", ["zrange-z", "0", "1", "WITHSCORES"]),
		).toEqual([
			["a", 1],
			["b", 2],
		]);
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
		expect(await client.send("ZPOPMIN", ["zpopmin-z"])).toEqual(["a", 1]);
		expect(await client.send("ZCARD", ["zpopmin-z"])).toBe(2);
	});

	test("ZPOPMAX with count returns top N", async () => {
		await client.send("ZADD", ["zpopmax-z", "1", "a", "2", "b", "3", "c"]);
		expect(await client.send("ZPOPMAX", ["zpopmax-z", "2"])).toEqual([
			["c", 3],
			["b", 2],
		]);
		expect(await client.send("ZCARD", ["zpopmax-z"])).toBe(1);
	});

	test("ZRANDMEMBER without count returns a single member", async () => {
		await client.send("ZADD", ["zrand-z", "1", "a", "2", "b", "3", "c"]);
		const m = (await client.send("ZRANDMEMBER", ["zrand-z"])) as string;
		expect(["a", "b", "c"]).toContain(m);
		expect(await client.send("ZCARD", ["zrand-z"])).toBe(3);
	});

	test("ZRANDMEMBER WITHSCORES returns [member, score] pairs", async () => {
		const out = (await client.send("ZRANDMEMBER", [
			"zrand-z",
			"2",
			"WITHSCORES",
		])) as [string, number][];
		expect(out.length).toBe(2);
		for (const [, score] of out) expect(typeof score).toBe("number");
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
		).toEqual([
			["a", 1],
			["c", 4],
			["b", 5],
		]);
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
		).toEqual([["b", 9]]);
	});

	test("ZDIFFSTORE keeps only the first set's exclusive members", async () => {
		await client.send("ZADD", ["zua", "1", "a", "2", "b"]);
		await client.send("ZADD", ["zub", "3", "b", "4", "c"]);
		expect(
			await client.send("ZDIFFSTORE", ["zdiff-dst", "2", "zua", "zub"]),
		).toBe(1);
		expect(
			await client.send("ZRANGE", ["zdiff-dst", "0", "-1", "WITHSCORES"]),
		).toEqual([["a", 1]]);
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

	// A queued SELECT moves the commands after it, and the connection once
	// EXEC has run. Spanning both halves also puts a memory-half command in
	// the same dispatcher batch as a SQL one, which routed it to `kv_0` — a
	// table the memory half never reads — before routing was per command.
	test("SELECT inside MULTI moves later commands, across both halves", async () => {
		const c = new Bun.RedisClient(redisUrl);
		try {
			await c.send("SELECT", ["0"]);
			await c.send("DEL", ["tx-select-key"]);
			await c.send("MULTI", []);
			await c.send("SET", ["tx-select-key", "cache"]);
			await c.send("SELECT", ["9"]);
			await c.send("SET", ["tx-select-key", "durable"]);
			expect(await c.send("EXEC", [])).toEqual(["OK", "OK", "OK"]);
			// EXEC left the connection on db 9.
			expect(await c.get("tx-select-key")).toBe("durable");
			await c.send("SELECT", ["0"]);
			expect(await c.get("tx-select-key")).toBe("cache");
			await c.send("DEL", ["tx-select-key"]);
			await c.send("SELECT", ["9"]);
			await c.send("DEL", ["tx-select-key"]);
		} finally {
			c.close();
		}
	});

	test("WATCH inside MULTI returns error", async () => {
		await client.send("MULTI", []);
		// Redis 7.2's wording. Unlike a queueing error it does not flag the
		// transaction: `watchCommand` replies and returns, so EXEC would run.
		await expect(client.send("WATCH", ["some-key"])).rejects.toThrow(
			/WATCH inside MULTI is not allowed/,
		);
		await client.send("DISCARD", []);
	});

	test("empty EXEC returns empty array", async () => {
		await client.send("MULTI", []);
		const results = await client.send("EXEC", []);
		expect(results).toEqual([]);
	});
});

// MIGRATE to this same server, a database over: the network path end to end.
describe("MIGRATE", () => {
	test("moves keys with their TTL, COPY keeps them, NOKEY when none exist", async () => {
		const host = process.env.REDIS_HOST ?? "localhost";
		const port = process.env.REDIS_PORT ?? "6379";
		const pw = process.env.REDIS_PASSWORD ?? "testpass";
		const auth = pw ? ["AUTH", pw] : [];
		await raw.send("SELECT", "0");
		await raw.send("DEL", "mig:a", "mig:b");
		await raw.send("SET", "mig:a", "1", "EX", "100");
		await raw.send("SET", "mig:b", "2");
		expect((await raw.send("MIGRATE", host, port, "mig:a", "1", "5000", ...auth)).toString()).toBe("+OK\r\n");
		expect(integerReply(await raw.send("EXISTS", "mig:a"))).toBe(0);
		await raw.send("SELECT", "1");
		expect((await raw.bulk("GET", "mig:a"))?.toString()).toBe("1");
		expect(integerReply(await raw.send("TTL", "mig:a"))).toBeGreaterThan(90);
		await raw.send("DEL", "mig:a");
		await raw.send("SELECT", "0");
		const copy = await raw.send("MIGRATE", host, port, "", "1", "5000", "COPY", ...auth, "KEYS", "mig:b", "mig:none");
		expect(copy.toString()).toBe("+OK\r\n");
		expect(integerReply(await raw.send("EXISTS", "mig:b"))).toBe(1);
		expect((await raw.send("MIGRATE", host, port, "mig:none", "1", "5000", ...auth)).toString()).toBe("+NOKEY\r\n");
		// Already there without REPLACE: the target's error, the key kept.
		const busy = errorReply(await raw.send("MIGRATE", host, port, "mig:b", "1", "5000", ...auth));
		expect(busy).toStartWith("ERR Target instance replied with error: BUSYKEY");
		expect(integerReply(await raw.send("EXISTS", "mig:b"))).toBe(1);
		await raw.send("DEL", "mig:b");
		await raw.send("SELECT", "1");
		await raw.send("DEL", "mig:b");
		await raw.send("SELECT", "durable");
	});
});

// INFO's rdb_changes_since_last_save counts as Redis's server.dirty does.
describe("The dirty counter", () => {
	const changes = async () => {
		const info = (await raw.bulk("INFO", "persistence"))?.toString() ?? "";
		return Number(/rdb_changes_since_last_save:(\d+)/.exec(info)?.[1]);
	};
	for (const db of ["0", "8"]) {
		test(`writes count their changes (db ${db})`, async () => {
			await raw.send("SELECT", db);
			await raw.send("DEL", "dc:s", "dc:b", "dc:f", "dc:h");
			let before = await changes();
			await raw.send("SADD", "dc:s", "a", "b", "c");
			await raw.send("SADD", "dc:s", "a");
			await raw.send("HSET", "dc:h", "f1", "v", "f2", "v");
			expect((await changes()) - before).toBe(5);
			// SETBIT counts a created or grown string, or a changed bit.
			before = await changes();
			await raw.send("SETBIT", "dc:b", "0", "0");
			await raw.send("SETBIT", "dc:b", "0", "0");
			await raw.send("SETBIT", "dc:b", "0", "1");
			await raw.send("SETBIT", "dc:b", "90", "0");
			expect((await changes()) - before).toBe(3);
			// BITFIELD counts each write that changed something.
			before = await changes();
			await raw.send("BITFIELD", "dc:f", "SET", "i5", "0", "0");
			await raw.send("BITFIELD", "dc:f", "SET", "i5", "0", "0");
			await raw.send("BITFIELD", "dc:f", "INCRBY", "i5", "0", "1", "INCRBY", "i5", "0", "1");
			expect((await changes()) - before).toBe(3);
			before = await changes();
			await raw.send("DEL", "dc:s", "dc:b", "dc:f", "dc:h", "dc:none");
			expect((await changes()) - before).toBe(4);
			await raw.send("SAVE");
			expect(await changes()).toBe(0);
			await raw.send("SELECT", "durable");
		});
	}
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
		// SCAN returns a page per call; the whole iteration names every key.
		const scanned: Buffer[] = [];
		let cursor = "0";
		do {
			const page = scanReply(await raw.send("SCAN", cursor, "MATCH", pattern));
			scanned.push(...page.items);
			cursor = page.cursor;
		} while (cursor !== "0");
		expect([...new Set(hexes(scanned))]).toEqual(hexes(keys));

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

describe("Sharded Pub/Sub", () => {
	// A stand-alone Redis keeps sharded channels in a namespace of their own:
	// SPUBLISH reaches SSUBSCRIBE and nothing else, PUBLISH the reverse, and
	// no pattern subscription sees a sharded message.
	test("SPUBLISH and PUBLISH do not cross namespaces", async () => {
		const sharded = new Bun.RedisClient(redisUrl);
		const plain = new Bun.RedisClient(redisUrl);
		const everything = new Bun.RedisClient(redisUrl);
		try {
			await sharded.send("SSUBSCRIBE", ["orders"]);
			await plain.send("SUBSCRIBE", ["orders"]);
			await everything.send("PSUBSCRIBE", ["*"]);
			await Bun.sleep(100);

			expect(await client.send("SPUBLISH", ["orders", "s"])).toBe(1);
			// The plain subscriber and the `*` pattern, not the sharded one.
			expect(await client.send("PUBLISH", ["orders", "p"])).toBe(2);

			expect(await client.send("PUBSUB", ["SHARDCHANNELS"])).toEqual(["orders"]);
			expect(await client.send("PUBSUB", ["SHARDNUMSUB", "orders", "none"])).toEqual([
				"orders",
				1,
				"none",
				0,
			]);
			const plainChannels = (await client.send("PUBSUB", ["CHANNELS"])) as string[];
			expect(plainChannels).toContain("orders");
			expect(plainChannels.some((c) => c.includes("shard"))).toBe(false);
		} finally {
			sharded.close();
			plain.close();
			everything.close();
		}
	});

	test("SUNSUBSCRIBE outside subscribe mode answers per channel", async () => {
		// A RESP3 push frame, which Bun's client hands back as { type, data }.
		expect(await client.send("SUNSUBSCRIBE", ["a"])).toEqual({
			type: "sunsubscribe",
			data: ["a", 0],
		});
	});
});

describe("Pub/Sub and client tracking without fixed limits", () => {
	// Redis bounds none of these except by memory and the pubsub output
	// buffer limit; neither does pg_redis.

	test("a 10,000-byte channel carries a 1 MB message", async () => {
		const channel = "c".repeat(10_000);
		const payload = "p".repeat(1 << 20);
		const sub = await new RawRedis().connect();
		try {
			await sub.send("SUBSCRIBE", channel);
			expect(await client.send("PUBSUB", ["NUMSUB", channel])).toEqual([channel, 1]);
			// At least this subscriber; a pattern another test left may match too.
			expect(await client.send("PUBLISH", [channel, payload])).toBeGreaterThanOrEqual(1);
			const message = (await sub.next()).toString();
			expect(message).toContain(`$${channel.length}\r\n${channel}\r\n`);
			expect(message).toContain(`$${payload.length}\r\n${payload}\r\n`);
		} finally {
			sub.close();
		}
	});

	test("a connection holds a thousand channels", async () => {
		const sub = await new RawRedis().connect();
		try {
			const names = Array.from({ length: 1000 }, (_, i) => `many-${i}`);
			await sub.send("SUBSCRIBE", ...names);
			// One confirmation per channel; the last counts all of them.
			for (let i = 1; i < names.length; i++) await sub.next();
			const numsub = await client.send("PUBSUB", ["NUMSUB", "many-0", "many-999"]);
			expect(numsub).toEqual(["many-0", 1, "many-999", 1]);
			expect(await client.send("PUBLISH", ["many-999", "x"])).toBe(1);
		} finally {
			sub.close();
		}
	});

	test("more than 256 connections subscribe at once", async () => {
		const subs = await Promise.all(
			Array.from({ length: 300 }, () => new RawRedis().connect()),
		);
		try {
			await Promise.all(subs.map((s) => s.send("SUBSCRIBE", "crowd")));
			expect(await client.send("PUBLISH", ["crowd", "hello"])).toBe(300);
		} finally {
			for (const s of subs) s.close();
		}
	});

	// Redis's `obuf-limits`: a normal client whose replies pass the hard
	// limit gets none of them and is closed; HRANDFIELD with a huge negative
	// count stops part way instead of building the whole reply.
	test("a normal client past its output limit is closed with no reply", async () => {
		const name = `obuf-${Date.now()}`;
		// Filled on a fresh connection, on the default database the test's
		// own socket reads; the shared client may have selected another.
		const filler = await new RawRedis().connect();
		await filler.send("DEL", "obuf:list");
		for (let i = 0; i < 150; i++) await filler.send("LPUSH", "obuf:list", "x".repeat(1000));
		await client.send("CONFIG", ["SET", "client-output-buffer-limit", "normal 100000 0 0"]);
		try {
			let got = 0;
			let closed = false;
			await new Promise<void>((resolve) => {
				Bun.connect({
					hostname: process.env.REDIS_HOST ?? "localhost",
					port: Number(process.env.REDIS_PORT ?? 6379),
					socket: {
						open: (s) => {
							const pw = process.env.REDIS_PASSWORD ?? "testpass";
							const auth = pw ? `*2\r\n$4\r\nAUTH\r\n$${pw.length}\r\n${pw}\r\n` : "";
							s.write(
								`${auth}*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$${name.length}\r\n${name}\r\n` +
									"*4\r\n$6\r\nLRANGE\r\n$9\r\nobuf:list\r\n$1\r\n0\r\n$2\r\n-1\r\n",
							);
						},
						data: (_s, chunk) => {
							got += chunk.length;
						},
						close: () => {
							closed = true;
							resolve();
						},
						error: () => resolve(),
					},
				});
				setTimeout(resolve, 3000);
			});
			expect(closed).toBe(true);
			// AUTH's +OK and SETNAME's +OK, nothing of the list.
			expect(got).toBeLessThan(100);
			expect((await client.send("CLIENT", ["LIST"])) as string).not.toContain(`name=${name}`);

			const raw2 = await new RawRedis().connect();
			await raw2.send("HSET", "obuf:h", "a", "b");
			raw2.close();
			// Bounded by the limit, so it ends quickly rather than building
			// 999,999,999 entries.
			const started = performance.now();
			const r = new Bun.RedisClient(redisUrl);
			await expect(r.send("HRANDFIELD", ["obuf:h", "-999999999"])).rejects.toThrow();
			r.close();
			expect(performance.now() - started).toBeLessThan(5000);
		} finally {
			await client.send("CONFIG", ["SET", "client-output-buffer-limit", "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"]);
			await filler.send("DEL", "obuf:list", "obuf:h");
			filler.close();
		}
	});

	test("tracking remembers more than 16,384 keys, long ones included", async () => {

		const tracker = await new RawRedis().connect();
		const inval = await new RawRedis().connect();
		try {
			const invalId = integerReply(await inval.send("CLIENT", "ID"));
			await inval.send("SUBSCRIBE", "__redis__:invalidate");
			await tracker.send("CLIENT", "TRACKING", "ON", "REDIRECT", String(invalId));
			const long = `track-long-${"k".repeat(5000)}`;
			await tracker.send("GET", long);
			const keys = Array.from({ length: 20_000 }, (_, i) => `track-many-${i}`);
			for (let i = 0; i < keys.length; i += 1000) {
				await tracker.send("MGET", ...keys.slice(i, i + 1000));
			}
			const info = (await client.send("INFO", ["stats"])) as string;
			const total = Number(/tracking_total_keys:(\d+)/.exec(info)?.[1]);
			expect(total).toBeGreaterThanOrEqual(20_001);
			// A write to the long key names it in the invalidation.
			const message = inval.next();
			await client.send("SET", [long, "v"]);
			const got = (await message).toString();
			expect(got).toContain("__redis__:invalidate");
			expect(got).toContain(long);
		} finally {
			tracker.close();
			inval.close();
		}
	});
});

// Sizes where the old fixed-table engine had boundaries — a 64-byte inline
// slot, 64-byte chunks, a 64 KiB value cap, 512-byte keys and members. The
// memory engine no longer has any of them, and the values either side are
// kept because `redis-benchmark` writes 3-byte values and nothing else in the
// suite crosses them.
const INLINE_VAL_LEN = 64;
const CHUNK_LEN = 64;
const MAX_TOTAL_VAL_LEN = 64 * 1024;
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
	MAX_TOTAL_VAL_LEN, // 65536 the old cap
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
			// A bulk string, as Redis sends it. `+embstr` was a simple string,
			// which a client that checks reply types refuses.
			const enc = async (k: string) => {
				const reply = (await raw.send("OBJECT", "ENCODING", k)).toString();
				expect(reply.startsWith("$")).toBe(true);
				return reply.split("\r\n")[1];
			};
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

		test("COPY lands on a key past its expiry, and does not merge", async () => {
			// The destination is expired but likely unswept: its rows are
			// still in the tables. It must not count as occupied, and the
			// copy must not merge with what the sweep has not reached.
			await raw.send("DEL", "p5:cs", "p5:cd");
			await raw.send("RPUSH", "p5:cd", "old1", "old2", "old3");
			await raw.send("PEXPIRE", "p5:cd", "30");
			await raw.send("RPUSH", "p5:cs", "new");
			await Bun.sleep(60);
			expect(integerReply(await raw.send("COPY", "p5:cs", "p5:cd"))).toBe(1);
			expect(integerReply(await raw.send("LLEN", "p5:cd"))).toBe(1);
			expect(
				(await raw.array("LRANGE", "p5:cd", "0", "-1")).map((b) =>
					b?.toString(),
				),
			).toEqual(["new"]);
			await raw.send("DEL", "p5:cs", "p5:cd");
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

		// Found by Redis's own suite once its memory pass really ran on the
		// memory half: each was right on SQL and wrong in shared memory.
		test("ZRANGE BYSCORE REV takes the high end first, and scores only on request", async () => {
			await raw.send("DEL", "p0:z", "p0:z2");
			await raw.send("ZADD", "p0:z", "1", "a", "2", "b", "3", "c", "4", "d");
			const words = async (...a: string[]) =>
				(await raw.array(...a)).map((b) => b?.toString());
			expect(await words("ZRANGE", "p0:z", "5", "0", "BYSCORE", "REV", "LIMIT", "0", "2", "WITHSCORES")).toEqual(["d", "4", "c", "3"]);
			expect(await words("ZRANGE", "p0:z", "0", "5", "BYSCORE", "LIMIT", "0", "2")).toEqual(["a", "b"]);
			expect(await words("ZRANGE", "p0:z", "[d", "[b", "BYLEX", "REV")).toEqual(["d", "c", "b"]);
			expect(integerReply(await raw.send("ZRANGESTORE", "p0:z2", "p0:z", "1", "2", "BYSCORE"))).toBe(2);
			await raw.send("DEL", "p0:z", "p0:z2");
		});

		test("ZADD XX leaves a missing key missing; INCR refused by GT/LT is nil", async () => {
			await raw.send("DEL", "p0:z");
			expect(integerReply(await raw.send("ZADD", "p0:z", "XX", "1", "a"))).toBe(0);
			expect((await raw.send("TYPE", "p0:z")).toString()).toBe("+none\r\n");
			await raw.send("ZADD", "p0:z", "5", "a");
			expect((await raw.send("ZADD", "p0:z", "LT", "INCR", "1", "a")).toString()).toBe("$-1\r\n");
			expect((await raw.send("ZADD", "p0:z", "GT", "INCR", "-1", "a")).toString()).toBe("$-1\r\n");
			await raw.send("DEL", "p0:z");
		});

		// A key past its deadline is gone the moment it is looked up, the
		// sweep notwithstanding: a write that reads the old value starts from
		// nothing and leaves a key with no deadline.
		test("writes that read the old value treat an expired key as absent", async () => {
			const expired = async (key: string, value: string) => {
				await raw.send("SET", key, value, "PX", "1");
				await Bun.sleep(20);
			};
			const str = async (...a: string[]) => (await raw.bulk(...a))?.toString() ?? null;
			await expired("p0:x1", "5");
			expect(integerReply(await raw.send("INCR", "p0:x1"))).toBe(1);
			expect(integerReply(await raw.send("TTL", "p0:x1"))).toBe(-1);
			await expired("p0:x2", "x");
			expect(integerReply(await raw.send("APPEND", "p0:x2", "y"))).toBe(1);
			expect(await str("GET", "p0:x2")).toBe("y");
			await expired("p0:x3", "hello");
			await raw.send("SETRANGE", "p0:x3", "0", "J");
			expect(await str("GET", "p0:x3")).toBe("J");
			await expired("p0:x4", "old");
			expect(await str("GETSET", "p0:x4", "new")).toBeNull();
			await expired("p0:x5", "old");
			expect(integerReply(await raw.send("SETNX", "p0:x5", "new"))).toBe(1);
			await expired("p0:x6", "old");
			expect(integerReply(await raw.send("MSETNX", "p0:x6", "new"))).toBe(1);
			expect(await str("GET", "p0:x6")).toBe("new");
			await expired("p0:x7", "old");
			await raw.send("SET", "p0:x7", "new", "KEEPTTL");
			expect(await str("GET", "p0:x7")).toBe("new");
			expect(integerReply(await raw.send("TTL", "p0:x7"))).toBe(-1);
			await raw.send("DEL", "p0:x1", "p0:x2", "p0:x3", "p0:x4", "p0:x5", "p0:x6", "p0:x7");
		});

		test("TTL rounds to the nearest second", async () => {
			await raw.send("SET", "p0:t", "v", "PX", "9700");
			expect(integerReply(await raw.send("TTL", "p0:t"))).toBe(10);
			await raw.send("DEL", "p0:t");
		});

		// `smoveCommand`: onto itself it only answers whether the member is
		// there, and changes nothing.
		test("SMOVE onto the same set keeps the member", async () => {
			await raw.send("DEL", "p0:s");
			await raw.send("SADD", "p0:s", "m");
			expect(integerReply(await raw.send("SMOVE", "p0:s", "p0:s", "m"))).toBe(1);
			expect(integerReply(await raw.send("SMOVE", "p0:s", "p0:s", "x"))).toBe(0);
			expect(integerReply(await raw.send("SISMEMBER", "p0:s", "m"))).toBe(1);
			await raw.send("DEL", "p0:s");
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
	// The ephemeral half, where the memory engine is under test; on db 8 every
	// value is a bytea column.
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

	// Redis's maxmemory: past the limit a write that could grow memory is
	// refused before it runs, nothing already stored is touched, and deleting
	// brings the room back. The limit is lowered for the test with CONFIG SET,
	// which is also what proves CONFIG SET maxmemory takes effect.
	test("past maxmemory a write is refused cleanly and deleting makes room", async () => {
		const big = payload(1 << 20);
		await raw.send("CONFIG", "SET", "maxmemory", "16mb");
		const stored: string[] = [];
		let refusal = "";
		try {
			for (let i = 0; i < 64 && !refusal; i++) {
				const key = `vs:fill:${i}`;
				const reply = (await raw.send("SET", key, big)).toString();
				if (reply === "+OK\r\n") stored.push(key);
				else refusal = reply;
			}
			expect(refusal).toMatch(/^-OOM /);
			expect(stored.length).toBeGreaterThan(4);
			expect(await raw.bulk("GET", stored[0])).toEqual(big);
			expect(await raw.bulk("GET", `vs:fill:${stored.length}`)).toBeNull();
			// A removal is never refused, whatever the limit.
			expect(integerReply(await raw.send("DEL", ...stored))).toBe(stored.length);
			expect((await raw.send("SET", stored[0], big)).toString()).toBe("+OK\r\n");
			await raw.send("DEL", stored[0]);
		} finally {
			await raw.send("CONFIG", "SET", "maxmemory", "0");
			for (const key of stored) await raw.send("DEL", key);
		}
	});

	// The limit is checked once, before the command: past it an MSET stores
	// none of its keys, never the ones that happened to fit.
	test("past maxmemory an MSET stores none of it", async () => {
		const big = payload(1 << 20);
		await raw.send("CONFIG", "SET", "maxmemory", "16mb");
		const stored: string[] = [];
		try {
			for (let i = 0; i < 64; i++) {
				const key = `vs:atomic:${i}`;
				if ((await raw.send("SET", key, big)).toString() !== "+OK\r\n") break;
				stored.push(key);
			}
			const reply = (await raw.send("MSET", "vs:atomic:a", big, "vs:atomic:b", big)).toString();
			expect(reply).toMatch(/^-OOM /);
			expect(await raw.bulk("GET", "vs:atomic:a")).toBeNull();
			expect(await raw.bulk("GET", "vs:atomic:b")).toBeNull();
		} finally {
			await raw.send("CONFIG", "SET", "maxmemory", "0");
			await raw.send("DEL", ...stored, "vs:atomic:a", "vs:atomic:b");
		}
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

	// No value cap below Redis's own 512 MiB, on either half.
	test("values far past 64 KiB are stored whole on both halves", async () => {
		for (const n of [MAX_TOTAL_VAL_LEN + 1, 1 << 20, 4 << 20]) {
			const v = payload(n);
			expect((await raw.send("SET", "vs:over", v)).toString()).toBe("+OK\r\n");
			expect(await raw.bulk("GET", "vs:over")).toEqual(v);
			await raw.send("HSET", "vs:overh", "f", v);
			expect(await raw.bulk("HGET", "vs:overh", "f")).toEqual(v);
		}
		await raw.send("DEL", "vs:over", "vs:overh");
	});

	// Redis puts no length limit on a key or a field short of 512 MiB.
	test("long keys and hash fields are stored whole on both halves", async () => {
		for (const n of [MAX_MEMBER_LEN + 1, 64 * 1024]) {
			// Letters after the prefix, so the key cannot read as a glob.
			const key = Buffer.alloc(n, 0x6b);
			key.write("vslong:", 0);
			const field = payload(n);
			expect((await raw.send("SET", key, "v")).toString()).toBe("+OK\r\n");
			expect((await raw.bulk("GET", key))?.toString()).toBe("v");
			const listed = await raw.array("KEYS", Buffer.from("vslong:*"));
			expect(listed.map((k) => k?.toString("hex"))).toEqual([key.toString("hex")]);
			expect(integerReply(await raw.send("HSET", "vs:lim", field, "v"))).toBe(1);
			expect((await raw.bulk("HGET", "vs:lim", field))?.toString()).toBe("v");
			await raw.send("DEL", key, "vs:lim");
		}
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

// Redis adds floats in C `long double`, the x87 80-bit format on x86-64, and
// prints 17 decimal places: digits a 64-bit double drops. Both halves compute
// it the same way, on memory db 0 and on logged db 8.
describe("Float increments in long double", () => {
	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	for (const db of ["0", "8"]) {
		test(`db ${db}: INCRBYFLOAT keeps the digits x86-64 Redis keeps`, async () => {
			await raw.send("SELECT", db);
			await raw.send("SET", "ld:n2", "3");
			expect(
				(await raw.bulk("INCRBYFLOAT", "ld:n2", "1.000000000000000005"))?.toString(),
			).toBe("4.00000000000000001");
			expect((await raw.bulk("GET", "ld:n2"))?.toString()).toBe("4.00000000000000001");
			expect((await raw.bulk("INCRBYFLOAT", "ld:n2", "0x1p-2"))?.toString()).toBe(
				"4.25000000000000001",
			);
			await raw.send("DEL", "ld:n2");
		});

		test(`db ${db}: HINCRBYFLOAT keeps them too`, async () => {
			await raw.send("SELECT", db);
			await raw.send("HSET", "ld:h", "n2", "3");
			expect(
				(await raw.bulk("HINCRBYFLOAT", "ld:h", "n2", "1.000000000000000005"))?.toString(),
			).toBe("4.00000000000000001");
			expect((await raw.bulk("HGET", "ld:h", "n2"))?.toString()).toBe(
				"4.00000000000000001",
			);
			await raw.send("DEL", "ld:h");
		});

		test(`db ${db}: a bad increment is refused after the type check`, async () => {
			await raw.send("SELECT", db);
			await raw.send("DEL", "ld:list", "ld:none");
			await raw.send("RPUSH", "ld:list", "a");
			expect((await raw.send("INCRBYFLOAT", "ld:list", "abc")).toString()).toMatch(
				/^-WRONGTYPE/,
			);
			expect((await raw.send("INCRBYFLOAT", "ld:none", "abc")).toString()).toBe(
				"-ERR value is not a valid float\r\n",
			);
			expect((await raw.send("INCRBYFLOAT", "ld:none", "inf")).toString()).toBe(
				"-ERR increment would produce NaN or Infinity\r\n",
			);
			expect(integerReply(await raw.send("EXISTS", "ld:none"))).toBe(0);
			await raw.send("DEL", "ld:list");
		});
	}
});

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

		// The guarantee a lock built on SET NX rests on. The memory half used to
		// test existence and write under two lock acquisitions, so two workers
		// could both be told OK.
		test(`${half}: SET NX has exactly one winner`, async () => {
			for (let round = 0; round < 20; round++) {
				const key = `conc:${half}:lock:${round}`;
				await raw.send("SELECT", half);
				await raw.send("DEL", key);
				const won: string[] = [];
				await fanOut(WRITERS, async (c, i) => {
					await c.send("SELECT", [half]);
					if ((await c.send("SET", [key, `owner-${i}`, "NX", "PX", "30000"])) === "OK") {
						won.push(`owner-${i}`);
					}
				});
				expect(won.length).toBe(1);
				expect((await raw.bulk("GET", key))?.toString()).toBe(won[0]);
				await raw.send("DEL", key);
			}
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
			["HKEYS", ["HKEYS", "x:h"]],
			["HVALS", ["HVALS", "x:h"]],
			["HEXISTS", ["HEXISTS", "x:h", "f"]],
			["HSTRLEN", ["HSTRLEN", "x:h", "f"]]]],
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
				expect(integerReply(await raw.send("EXISTS", key))).toBe(0);

				// TYPE says `none`, and every read of the same key has to
				// agree, before the sweep has run and without waiting for it.
				// The key is re-built and re-expired for every read: a read
				// that answers honestly may also reap, and stale rows one
				// command cleaned up must not vouch for the next.
				for (const [name, cmd] of reads) {
					await pastItsExpiry(key, build);
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

// ─────────────────── RANDOMKEY ──────────────────────────────────────────────
//
// The harness compares RANDOMKEY by shape, because any live key is a valid
// answer — which is exactly why the two defects here needed their own tests.
// The durable half read kv_N alone, so a database holding only collections
// answered nil while DBSIZE counted them. The memory half handed back the
// first live string and consulted the collections only when there was none,
// so a database of mostly collections saw strings almost every draw.

for (const half of HALVES) {
	describe(`RANDOMKEY (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");
		});
		afterAll(async () => {
			await raw.send("FLUSHDB");
			await raw.send("SELECT", "durable");
		});

		test("a database holding only a collection is not empty to it", async () => {
			await raw.send("FLUSHDB");
			await raw.send("RPUSH", "rk:only", "a");
			expect((await raw.bulk("RANDOMKEY"))?.toString()).toBe("rk:only");
		});

		test("every type is reachable, not only strings", async () => {
			await raw.send("FLUSHDB");
			await raw.send("SET", "rk:str", "v");
			await raw.send("RPUSH", "rk:list", "a");
			await raw.send("SADD", "rk:set", "m");
			await raw.send("HSET", "rk:hash", "f", "v");
			await raw.send("ZADD", "rk:zset", "1", "m");

			// 250 uniform draws miss one of five keys with probability
			// 5·(4/5)^250 ≈ 10⁻²⁴ — a miss here is a skew, not bad luck.
			const seen = new Set<string>();
			for (let i = 0; i < 250; i++) {
				const k = await raw.bulk("RANDOMKEY");
				if (k) seen.add(k.toString());
			}
			expect([...seen].sort()).toEqual([
				"rk:hash",
				"rk:list",
				"rk:set",
				"rk:str",
				"rk:zset",
			]);
		});

		test("an expired collection is not a valid draw", async () => {
			await raw.send("FLUSHDB");
			await raw.send("RPUSH", "rk:dead", "a");
			await raw.send("PEXPIRE", "rk:dead", "30");
			// Past the deadline but likely ahead of the sweep: the rows may
			// still be in the tables, and the draw must not see them.
			await Bun.sleep(80);
			expect(await raw.bulk("RANDOMKEY")).toBeNull();
		});
	});
}

// ───────────────────────────── SCAN cursors ─────────────────────────────────
//
// SCAN, HSCAN, SSCAN and ZSCAN return a page per call and a cursor to resume
// from. Redis's guarantee is what these pin: a full iteration returns every
// element present from its start to its end at least once, however the keys
// change in between, and it ends. The memory half's cursor is a position in
// the dictionary; the durable half's names the last key returned, kept in
// shared memory so another connection, on another worker, can resume it.

for (const half of HALVES) {
	describe(`Incremental cursors (${half})`, () => {
		let other: RawRedis;
		beforeAll(async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");
			other = await new RawRedis().connect();
			await other.send("SELECT", half);
		});
		afterAll(async () => {
			other.close();
			await raw.send("FLUSHDB");
			await raw.send("SELECT", "durable");
		});

		/**
		 * A whole iteration of `cmd`, the cursor going in at `at`: every
		 * element (repeats included) and the number of calls. `between` runs
		 * after each call that is not the last; `conn` picks the connection
		 * for each call.
		 */
		async function iterate(
			cmd: string[],
			at: number,
			between: (call: number) => Promise<void> = async () => {},
			conn: (call: number) => RawRedis = () => raw,
		): Promise<{ items: Buffer[]; calls: number }> {
			const items: Buffer[] = [];
			let cursor = "0";
			for (let call = 1; call < 20_000; call++) {
				const line = [...cmd];
				line.splice(at, 0, cursor);
				const page = scanReply(await conn(call).send(...line));
				// An unsigned 64-bit integer in decimal, as clients parse it.
				expect(page.cursor).toMatch(/^\d+$/);
				expect(BigInt(page.cursor) < 2n ** 64n).toBe(true);
				items.push(...page.items);
				if (page.cursor === "0") return { items, calls: call };
				cursor = page.cursor;
				await between(call);
			}
			throw new Error(`${cmd.join(" ")} did not end`);
		}

		const strings = (items: Buffer[]) => new Set(items.map((b) => b.toString()));
		const name = (i: number) => `cur:${String(i).padStart(4, "0")}`;

		async function thousandKeys() {
			await raw.send("FLUSHDB");
			const args: string[] = [];
			for (let i = 0; i < 1000; i++) args.push(name(i), "v");
			expect((await raw.send("MSET", ...args)).toString()).toBe("+OK\r\n");
		}

		test("SCAN pages through 1,000 keys and returns every one", async () => {
			await thousandKeys();
			const { items, calls } = await iterate(["SCAN", "COUNT", "10"], 1);
			expect(calls).toBeGreaterThan(1);
			const seen = strings(items);
			expect(seen.size).toBe(1000);
			for (let i = 0; i < 1000; i++) expect(seen.has(name(i))).toBe(true);
			// The default COUNT is 10 too, so the default takes more than one call.
			expect((await iterate(["SCAN"], 1)).calls).toBeGreaterThan(1);
		});

		test("a cursor resumes on another connection", async () => {
			await thousandKeys();
			const { items, calls } = await iterate(
				["SCAN", "COUNT", "10"],
				1,
				undefined,
				(call) => (call % 2 === 0 ? other : raw),
			);
			expect(calls).toBeGreaterThan(1);
			expect(strings(items).size).toBe(1000);
		});

		test("deleting and adding keys mid-iteration hides no untouched key", async () => {
			await thousandKeys();
			const deleted = new Set<string>();
			const { items } = await iterate(["SCAN", "COUNT", "10"], 1, async (call) => {
				// Behind the cursor and ahead of it, wherever it is.
				for (const j of [0, 1, 2]) {
					const k = name((call * 37 + j * 331) % 1000);
					await raw.send("DEL", k);
					deleted.add(k);
				}
				await raw.send("SET", `cur:new:${call}`, "v");
			});
			const seen = strings(items);
			for (let i = 0; i < 1000; i++) {
				const k = name(i);
				if (!deleted.has(k)) expect(seen.has(k)).toBe(true);
			}
		});

		test("MATCH and TYPE filter each page and the iteration still ends", async () => {
			await thousandKeys();
			for (let i = 0; i < 20; i++) {
				await raw.send("RPUSH", `other:list:${i}`, "a");
				await raw.send("HSET", `other:hash:${i}`, "f", "v");
			}
			const listed = await iterate(["SCAN", "COUNT", "10", "MATCH", "other:*"], 1);
			// Most pages of strings match nothing, and still are not the end.
			expect(listed.calls).toBeGreaterThan(10);
			expect(strings(listed.items).size).toBe(40);
			const hashes = strings((await iterate(["SCAN", "TYPE", "hash", "COUNT", "25"], 1)).items);
			expect([...hashes].sort()).toEqual(
				Array.from({ length: 20 }, (_, i) => `other:hash:${i}`).sort(),
			);
			const lists = await iterate(["SCAN", "MATCH", "other:list:1*", "TYPE", "list"], 1);
			expect([...strings(lists.items)].sort()).toEqual(
				["1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19"].map(
					(n) => `other:list:${n}`,
				).sort(),
			);
		});

		// A client that lost a reply sends the same cursor again. Redis gives
		// it the same page; so does pg_redis for the latest two cursors.
		test("resending the previous cursor gives the same page again", async () => {
			await thousandKeys();
			const names = (p: { items: Buffer[] }) => p.items.map((b) => b.toString()).sort();
			const first = scanReply(await raw.send("SCAN", "0", "COUNT", "10"));
			const second = scanReply(await raw.send("SCAN", first.cursor, "COUNT", "10"));
			expect(second.cursor).not.toBe("0");
			const latest = scanReply(await raw.send("SCAN", first.cursor, "COUNT", "10"));
			expect(names(latest)).toEqual(names(second));
			// And the iteration carries on from the newer cursor.
			const third = scanReply(await raw.send("SCAN", second.cursor, "COUNT", "10"));
			expect(third.items.length).toBeGreaterThan(0);
		});

		test("a cursor the server never issued restarts or resumes, and ends", async () => {
			await thousandKeys();
			for (const made_up of ["12345", "9223372036854775807", "18446744073709551615"]) {
				const page = scanReply(await raw.send("SCAN", made_up, "COUNT", "10"));
				expect(page.cursor).toMatch(/^\d+$/);
				let cursor = page.cursor;
				for (let n = 0; cursor !== "0"; n++) {
					expect(n).toBeLessThan(5000);
					cursor = scanReply(await raw.send("SCAN", cursor, "COUNT", "50")).cursor;
				}
			}
			// Not an integer at all is the error it always was.
			expect((await raw.send("SCAN", "notanumber")).toString()).toMatch(/^-ERR/);
			expect((await raw.send("SSCAN", "s", "-1")).toString()).toBe(
				"-ERR invalid cursor\r\n",
			);
		});

		const member = (i: number) => `m${String(i).padStart(3, "0")}`;

		async function collections(n: number) {
			await raw.send("FLUSHDB");
			const h: string[] = [];
			const s: string[] = [];
			const z: string[] = [];
			for (let i = 0; i < n; i++) {
				h.push(member(i), `v${i}`);
				s.push(member(i));
				z.push(String(i + 0.5), member(i));
			}
			await raw.send("HSET", "c:h", ...h);
			await raw.send("SADD", "c:s", ...s);
			await raw.send("ZADD", "c:z", ...z);
		}

		// 600: a hash of 512 fields or fewer is a listpack, which HSCAN
		// returns whole in one call, as Redis does; past that it pages.
		test("HSCAN, SSCAN and ZSCAN page through a few hundred elements", async () => {
			await collections(600);
			const hscan = await iterate(["HSCAN", "c:h", "COUNT", "10"], 2);
			expect(hscan.calls).toBeGreaterThan(1);
			const fields = new Map<string, string>();
			for (let i = 0; i < hscan.items.length; i += 2) {
				fields.set(hscan.items[i].toString(), hscan.items[i + 1].toString());
			}
			expect(fields.size).toBe(600);
			for (let i = 0; i < 600; i++) expect(fields.get(member(i))).toBe(`v${i}`);

			const sscan = await iterate(["SSCAN", "c:s", "COUNT", "10"], 2);
			expect(sscan.calls).toBeGreaterThan(1);
			expect(strings(sscan.items).size).toBe(600);

			const zscan = await iterate(["ZSCAN", "c:z", "COUNT", "10"], 2);
			expect(zscan.calls).toBeGreaterThan(1);
			const scores = new Map<string, string>();
			for (let i = 0; i < zscan.items.length; i += 2) {
				scores.set(zscan.items[i].toString(), zscan.items[i + 1].toString());
			}
			expect(scores.size).toBe(600);
			for (let i = 0; i < 600; i++) expect(scores.get(member(i))).toBe(`${i}.5`);

			// MATCH applies to fields and members; a missing key is empty.
			const matched = await iterate(["ZSCAN", "c:z", "MATCH", "m00*"], 2);
			expect(matched.items.map((b) => b.toString()).filter((_, i) => i % 2 === 0).sort()).toEqual(
				Array.from({ length: 10 }, (_, i) => member(i)),
			);
			expect(await iterate(["SSCAN", "c:none"], 2)).toEqual({ items: [], calls: 1 });
			expect((await raw.send("HSCAN", "c:s", "0")).toString()).toMatch(/^-WRONGTYPE/);
		});

		test("removing elements mid-HSCAN, SSCAN and ZSCAN hides none that stayed", async () => {
			await collections(600);
			for (const [cmd, key, del] of [
				["HSCAN", "c:h", "HDEL"],
				["SSCAN", "c:s", "SREM"],
				["ZSCAN", "c:z", "ZREM"],
			]) {
				const removed = new Set<string>();
				const { items } = await iterate([cmd, key, "COUNT", "10"], 2, async (call) => {
					for (const j of [0, 1]) {
						const m = member((call * 41 + j * 149) % 600);
						await raw.send(del, key, m);
						removed.add(m);
					}
				});
				const step = cmd === "SSCAN" ? 1 : 2;
				const seen = new Set(items.filter((_, i) => i % step === 0).map((b) => b.toString()));
				for (let i = 0; i < 600; i++) {
					const m = member(i);
					if (!removed.has(m)) expect(seen.has(m)).toBe(true);
				}
			}
		});
	});
}

// ───────────────────── Collection order and encodings ───────────────────────
//
// Redis keeps a small hash or set packed — a listpack, or an intset for a
// set of integers — and converts it to a hash table once it outgrows a
// threshold, for good. The encoding shows twice: in OBJECT ENCODING, and in
// the order elements come back, since a listpack keeps insertion order and
// deletes in place and an intset is sorted. Every expected value here is
// what redis:7.2-alpine answered for the same commands.

for (const half of HALVES) {
	describe(`Collection order and encodings (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
			await raw.send("FLUSHDB");
		});
		afterAll(async () => {
			await raw.send("FLUSHDB");
			await raw.send("SELECT", "durable");
		});

		const list = async (...args: string[]) =>
			(await raw.array(...args)).map((b) => b?.toString());
		const enc = async (key: string) =>
			(await raw.bulk("OBJECT", "ENCODING", key))?.toString();
		const many = (n: number, f: (i: number) => string[]) =>
			Array.from({ length: n }, (_, i) => f(i)).flat();

		test("a hash keeps insertion order across HDEL and updates", async () => {
			await raw.send("HSET", "o:h", "c", "1", "a", "2", "b", "3");
			await raw.send("HDEL", "o:h", "a");
			await raw.send("HSET", "o:h", "d", "4");
			await raw.send("HSET", "o:h", "c", "9");
			expect(await list("HGETALL", "o:h")).toEqual(["c", "9", "b", "3", "d", "4"]);
			expect(await list("HKEYS", "o:h")).toEqual(["c", "b", "d"]);
			expect(await list("HVALS", "o:h")).toEqual(["9", "3", "4"]);
			expect(await enc("o:h")).toBe("listpack");
			// A listpack is scanned whole in one call, whatever COUNT says.
			const page = scanReply(await raw.send("HSCAN", "o:h", "0", "COUNT", "1"));
			expect(page.cursor).toBe("0");
			expect(page.items.map((b) => b.toString())).toEqual(["c", "9", "b", "3", "d", "4"]);
			// HRANDFIELD with a count covering the hash returns it in order.
			expect(await list("HRANDFIELD", "o:h", "10", "WITHVALUES")).toEqual([
				"c", "9", "b", "3", "d", "4",
			]);
		});

		test("a hash grown past the listpack limit and shrunk stays a hashtable", async () => {
			// 512 fields is Redis 7.2's compiled-in hash-max-listpack-entries.
			await raw.send("HSET", "o:h129", ...many(129, (i) => [`f${i}`, "v"]));
			expect(await enc("o:h129")).toBe("listpack");
			await raw.send("HSET", "o:big", ...many(513, (i) => [`f${i}`, "v"]));
			expect(await enc("o:big")).toBe("hashtable");
			await raw.send("HDEL", "o:big", ...many(510, (i) => [`f${i}`]));
			expect(integerReply(await raw.send("HLEN", "o:big"))).toBe(3);
			expect(await enc("o:big")).toBe("hashtable");
			// A value past 64 bytes converts it too, and its removal does not
			// convert it back.
			await raw.send("HSET", "o:long", "a", "1", "b", "x".repeat(65));
			await raw.send("HDEL", "o:long", "b");
			expect(await enc("o:long")).toBe("hashtable");
		});

		test("a set of integers is an intset and comes back sorted", async () => {
			await raw.send("SADD", "o:i", "5", "-3", "100", "2");
			expect(await list("SMEMBERS", "o:i")).toEqual(["-3", "2", "5", "100"]);
			expect(await enc("o:i")).toBe("intset");
			// A string converts it: the integers keep their sorted order and
			// later members are appended.
			await raw.send("SADD", "o:i", "x");
			await raw.send("SADD", "o:i", "1");
			expect(await list("SMEMBERS", "o:i")).toEqual(["-3", "2", "5", "100", "x", "1"]);
			expect(await enc("o:i")).toBe("listpack");
			// Losing the string does not make it an intset again.
			await raw.send("SREM", "o:i", "x");
			expect(await enc("o:i")).toBe("listpack");
			expect(await list("SMEMBERS", "o:i")).toEqual(["-3", "2", "5", "100", "1"]);
			// Not what Redis reads as integers: a listpack from the start.
			for (const m of ["007", "-0", "+1", "1.0", "9223372036854775808"]) {
				await raw.send("DEL", "o:n");
				await raw.send("SADD", "o:n", m);
				expect(await enc("o:n")).toBe("listpack");
			}
			await raw.send("SADD", "o:edge", "9223372036854775807", "-9223372036854775808");
			expect(await list("SMEMBERS", "o:edge")).toEqual([
				"-9223372036854775808",
				"9223372036854775807",
			]);
			expect(await enc("o:edge")).toBe("intset");
		});

		test("a listpack set removes in place and SSCAN returns it whole", async () => {
			await raw.send("SADD", "o:l", "c", "a", "b", "d");
			await raw.send("SREM", "o:l", "a");
			await raw.send("SADD", "o:l", "e");
			expect(await list("SMEMBERS", "o:l")).toEqual(["c", "b", "d", "e"]);
			expect(await enc("o:l")).toBe("listpack");
			const page = scanReply(await raw.send("SSCAN", "o:l", "0", "COUNT", "1"));
			expect(page.cursor).toBe("0");
			expect(page.items.map((b) => b.toString())).toEqual(["c", "b", "d", "e"]);
			expect(await list("SRANDMEMBER", "o:l", "10")).toEqual(["c", "b", "d", "e"]);
		});

		test("a set that gained and lost a non-integer reports listpack", async () => {
			await raw.send("SADD", "o:g", "1", "2");
			expect(await enc("o:g")).toBe("intset");
			await raw.send("SADD", "o:g", "x");
			await raw.send("SREM", "o:g", "x");
			expect(await enc("o:g")).toBe("listpack");
			expect(await list("SMEMBERS", "o:g")).toEqual(["1", "2"]);
		});

		test("sets convert to hashtable for good", async () => {
			await raw.send("SADD", "o:s200", ...many(200, (i) => [`m${i}`]));
			expect(await enc("o:s200")).toBe("hashtable");
			await raw.send("SREM", "o:s200", ...many(197, (i) => [`m${i}`]));
			expect(await enc("o:s200")).toBe("hashtable");
			await raw.send("SADD", "o:i512", ...many(512, (i) => [String(i + 1)]));
			expect(await enc("o:i512")).toBe("intset");
			await raw.send("SADD", "o:i512", "513");
			expect(await enc("o:i512")).toBe("hashtable");
			await raw.send("SADD", "o:long", "a", "x".repeat(65));
			await raw.send("SREM", "o:long", "x".repeat(65));
			expect(await enc("o:long")).toBe("hashtable");
			// SADD sizes a set by the members named, repeats included.
			await raw.send("SADD", "o:dup", ...many(200, () => ["a"]));
			expect(await enc("o:dup")).toBe("hashtable");
		});

		test("set operations come back in the order their result set keeps", async () => {
			await raw.send("SADD", "o:A", "c", "a", "b", "3", "1");
			await raw.send("SADD", "o:B", "b", "c", "z");
			await raw.send("SADD", "o:I", "9", "3", "1");
			await raw.send("SADD", "o:J", "1", "3", "7");
			expect(await list("SINTER", "o:A", "o:B")).toEqual(["b", "c"]);
			expect(await list("SUNION", "o:A", "o:B")).toEqual(["c", "a", "b", "3", "1", "z"]);
			expect(await list("SDIFF", "o:A", "o:B")).toEqual(["a", "3", "1"]);
			expect(await list("SINTER", "o:I", "o:J")).toEqual(["1", "3"]);
			expect(await list("SUNION", "o:I", "o:J")).toEqual(["1", "3", "7", "9"]);
			expect(await list("SUNION", "o:I", "o:A")).toEqual(["1", "3", "9", "c", "a", "b"]);
			expect(await list("SDIFF", "o:A", "o:I")).toEqual(["c", "a", "b"]);
			expect(integerReply(await raw.send("SINTERSTORE", "o:D1", "o:A", "o:B"))).toBe(2);
			expect(await enc("o:D1")).toBe("listpack");
			expect(await list("SMEMBERS", "o:D1")).toEqual(["b", "c"]);
			expect(integerReply(await raw.send("SUNIONSTORE", "o:D2", "o:I", "o:J"))).toBe(4);
			expect(await enc("o:D2")).toBe("intset");
			expect(integerReply(await raw.send("SUNIONSTORE", "o:D3", "o:A", "o:B"))).toBe(6);
			expect(await list("SMEMBERS", "o:D3")).toEqual(["c", "a", "b", "3", "1", "z"]);
			// A listpack holding only integers, stored by SINTERSTORE or
			// rebuilt by SUNION, becomes an intset and sorts.
			await raw.send("SADD", "o:li", "a", "30", "10", "20");
			await raw.send("SREM", "o:li", "a");
			expect(await list("SMEMBERS", "o:li")).toEqual(["30", "10", "20"]);
			expect(await list("SUNION", "o:li")).toEqual(["10", "20", "30"]);
			expect(integerReply(await raw.send("SINTERSTORE", "o:lo", "o:li", "o:li"))).toBe(3);
			expect(await enc("o:lo")).toBe("intset");
			// SPOP of the whole set answers as SUNION would.
			await raw.send("SADD", "o:q", "a", "3", "1");
			await raw.send("SREM", "o:q", "a");
			expect(await list("SPOP", "o:q", "5")).toEqual(["1", "3"]);
			// SMOVE into an intset converts it the same way SADD does.
			await raw.send("SADD", "o:sm1", "a", "5");
			await raw.send("SADD", "o:sm2", "9", "1");
			await raw.send("SMOVE", "o:sm1", "o:sm2", "a");
			expect(await list("SMEMBERS", "o:sm2")).toEqual(["1", "9", "a"]);
			expect(await enc("o:sm2")).toBe("listpack");
		});

		test("a sorted set converted to skiplist stays one", async () => {
			await raw.send("ZADD", "o:z", ...many(129, (i) => [String(i), `m${i}`]));
			expect(await enc("o:z")).toBe("skiplist");
			await raw.send("ZREMRANGEBYRANK", "o:z", "0", "100");
			expect(integerReply(await raw.send("ZCARD", "o:z"))).toBe(28);
			expect(await enc("o:z")).toBe("skiplist");
			await raw.send("ZADD", "o:zl", "1", "a", "2", "x".repeat(65));
			await raw.send("ZREM", "o:zl", "x".repeat(65));
			expect(await enc("o:zl")).toBe("skiplist");
			await raw.send("ZADD", "o:small", "2", "b", "1", "a", "3", "c");
			expect(await enc("o:small")).toBe("listpack");
			const page = scanReply(await raw.send("ZSCAN", "o:small", "0", "COUNT", "1"));
			expect(page.cursor).toBe("0");
			expect(page.items.map((b) => b.toString())).toEqual(["a", "1", "b", "2", "c", "3"]);
			// The whole sorted set from ZRANDMEMBER comes highest first.
			expect(await list("ZRANDMEMBER", "o:small", "5", "WITHSCORES")).toEqual([
				"c", "3", "b", "2", "a", "1",
			]);
		});

		test("a list is a listpack until it outgrows 8 KB", async () => {
			await raw.send("RPUSH", "o:list", ...many(200, (i) => [`a${i}`]));
			expect(await enc("o:list")).toBe("listpack");
			await raw.send("RPUSH", "o:list2", ...many(90, () => ["x".repeat(100)]));
			expect(await enc("o:list2")).toBe("quicklist");
		});

		test("COPY and RENAME keep the encoding, DUMP and RESTORE load it as Redis does", async () => {
			await raw.send("HSET", "o:hb", ...many(600, (i) => [`f${i}`, "v"]));
			await raw.send("HDEL", "o:hb", ...many(597, (i) => [`f${i + 3}`]));
			expect(await enc("o:hb")).toBe("hashtable");
			await raw.send("COPY", "o:hb", "o:hc");
			expect(await enc("o:hc")).toBe("hashtable");
			await raw.send("RENAME", "o:hc", "o:hd");
			expect(await enc("o:hd")).toBe("hashtable");
			// A payload loads as Redis's rdbLoadObject would: three fields
			// fit a listpack.
			const payload = await raw.bulk("DUMP", "o:hb");
			expect((await raw.send("RESTORE", "o:he", "0", payload as Buffer)).toString()).toBe(
				"+OK\r\n",
			);
			expect(await enc("o:he")).toBe("listpack");
			// A listpack's payload keeps its order and its encoding.
			await raw.send("SADD", "o:sp", "a", "30", "10");
			await raw.send("SREM", "o:sp", "a");
			const sp = await raw.bulk("DUMP", "o:sp");
			await raw.send("RESTORE", "o:sq", "0", sp as Buffer);
			expect(await list("SMEMBERS", "o:sq")).toEqual(["30", "10"]);
			expect(await enc("o:sq")).toBe("listpack");
		});

		test("CONFIG SET moves the thresholds", async () => {
			try {
				await raw.send("CONFIG", "SET", "hash-max-listpack-entries", "2");
				await raw.send("HSET", "o:cs", "a", "1", "b", "2");
				expect(await enc("o:cs")).toBe("listpack");
				await raw.send("HSET", "o:cs", "c", "3");
				expect(await enc("o:cs")).toBe("hashtable");
				await raw.send("CONFIG", "SET", "set-max-intset-entries", "3");
				await raw.send("SADD", "o:ci", "1", "2", "3");
				expect(await enc("o:ci")).toBe("intset");
				await raw.send("SADD", "o:ci", "4");
				expect(await enc("o:ci")).toBe("hashtable");
			} finally {
				await raw.send("CONFIG", "SET", "hash-max-listpack-entries", "512");
				await raw.send("CONFIG", "SET", "set-max-intset-entries", "512");
			}
		});
	});
}

// ─────────────────── TIME, TOUCH, HSTRLEN, ZINTERCARD ───────────────────────

describe("TIME", () => {
	test("answers one clock reading as two strings", async () => {
		const before = Math.floor(Date.now() / 1000);
		const parts = (await raw.array("TIME")).map((b) => Number(b?.toString()));
		const after = Math.ceil(Date.now() / 1000);
		expect(parts.length).toBe(2);
		expect(parts[0]).toBeGreaterThanOrEqual(before);
		expect(parts[0]).toBeLessThanOrEqual(after);
		expect(parts[1]).toBeGreaterThanOrEqual(0);
		expect(parts[1]).toBeLessThan(1_000_000);
	});

	test("inside MULTI it executes at EXEC time", async () => {
		await raw.send("MULTI");
		await raw.send("TIME");
		// One queued reply, itself the two-part array.
		expect((await raw.send("EXEC")).toString().startsWith("*1\r\n*2\r\n")).toBe(
			true,
		);
	});

	test("rejects arguments", async () => {
		expect((await raw.send("TIME", "x")).toString()).toContain(
			"wrong number of arguments",
		);
	});
});

for (const half of HALVES) {
	describe(`TOUCH (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
		});
		afterAll(async () => {
			await raw.send("SELECT", "durable");
		});

		test("counts the keys that exist, duplicates included", async () => {
			await raw.send("SET", "to:a", "1");
			await raw.send("RPUSH", "to:l", "x");
			expect(integerReply(await raw.send("TOUCH", "to:a", "to:l"))).toBe(2);
			expect(integerReply(await raw.send("TOUCH", "to:a", "to:a"))).toBe(2);
			expect(
				integerReply(await raw.send("TOUCH", "to:a", "to:missing")),
			).toBe(1);
			expect(integerReply(await raw.send("TOUCH", "to:missing"))).toBe(0);
			await raw.send("DEL", "to:a", "to:l");
		});

		test("an expired key does not count", async () => {
			await raw.send("SET", "to:exp", "v", "PX", "30");
			await Bun.sleep(60);
			expect(integerReply(await raw.send("TOUCH", "to:exp"))).toBe(0);
		});
	});

	describe(`HSTRLEN (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
		});
		afterAll(async () => {
			await raw.send("SELECT", "durable");
		});

		test("byte length of one field, 0 when either is missing", async () => {
			await raw.send("DEL", "hsl:h");
			await raw.send("HSET", "hsl:h", "f", "hello");
			expect(integerReply(await raw.send("HSTRLEN", "hsl:h", "f"))).toBe(5);
			expect(integerReply(await raw.send("HSTRLEN", "hsl:h", "nope"))).toBe(0);
			expect(integerReply(await raw.send("HSTRLEN", "hsl:none", "f"))).toBe(0);
			await raw.send("DEL", "hsl:h");
		});

		test("a key of another type is refused", async () => {
			await raw.send("SET", "hsl:str", "v");
			expect((await raw.send("HSTRLEN", "hsl:str", "f")).toString()).toContain(
				"WRONGTYPE",
			);
			await raw.send("DEL", "hsl:str");
		});
	});

	describe(`ZINTERCARD (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
			await raw.send("DEL", "zic:a", "zic:b", "zic:c");
			await raw.send("ZADD", "zic:a", "1", "m1", "2", "m2", "3", "m3");
			await raw.send("ZADD", "zic:b", "1", "m2", "2", "m3", "3", "m4");
		});
		afterAll(async () => {
			await raw.send("DEL", "zic:a", "zic:b", "zic:c");
			await raw.send("SELECT", "durable");
		});

		test("the intersection's size, without building it", async () => {
			expect(
				integerReply(await raw.send("ZINTERCARD", "2", "zic:a", "zic:b")),
			).toBe(2);
			expect(integerReply(await raw.send("ZINTERCARD", "1", "zic:a"))).toBe(3);
			expect(
				integerReply(await raw.send("ZINTERCARD", "2", "zic:a", "zic:none")),
			).toBe(0);
		});

		test("LIMIT caps the count and 0 lifts the cap", async () => {
			expect(
				integerReply(
					await raw.send("ZINTERCARD", "2", "zic:a", "zic:b", "LIMIT", "1"),
				),
			).toBe(1);
			expect(
				integerReply(
					await raw.send("ZINTERCARD", "2", "zic:a", "zic:b", "LIMIT", "0"),
				),
			).toBe(2);
		});

		test("refuses bad arguments the way 7.0.15 spells it", async () => {
			// Arity is -3, so a bare numkeys is an arity error. Past that,
			// ZINTERCARD's refusals differ from SINTERCARD's, measured: the
			// integer error for a non-number, the ZUNION family's message for
			// a non-positive count.
			expect((await raw.send("ZINTERCARD", "0")).toString()).toContain(
				"wrong number of arguments",
			);
			expect((await raw.send("ZINTERCARD", "0", "zic:a")).toString()).toContain(
				"at least 1 input key is needed",
			);
			expect((await raw.send("ZINTERCARD", "x", "zic:a")).toString()).toContain(
				"not an integer",
			);
			expect(
				(
					await raw.send("ZINTERCARD", "1", "zic:a", "LIMIT", "-1")
				).toString(),
			).toContain("LIMIT can't be negative");
		});
	});
}

// ─────────────────── DUMP and RESTORE ───────────────────────────────────────

for (const half of HALVES) {
	describe(`DUMP and RESTORE (${half})`, () => {
		beforeAll(async () => {
			await raw.send("SELECT", half);
		});
		afterAll(async () => {
			await raw.send("SELECT", "durable");
		});

		test("every type round-trips through its payload", async () => {
			await raw.send("DEL", "dr:s", "dr:l", "dr:se", "dr:h", "dr:z");
			await raw.send("SET", "dr:s", "hello");
			await raw.send("RPUSH", "dr:l", "a", "b", "c");
			await raw.send("SADD", "dr:se", "m1", "m2");
			await raw.send("HSET", "dr:h", "f1", "v1", "f2", "v2");
			await raw.send("ZADD", "dr:z", "1.5", "m1", "-2", "m2");

			for (const key of ["dr:s", "dr:l", "dr:se", "dr:h", "dr:z"]) {
				const payload = await raw.bulk("DUMP", key);
				expect(payload).not.toBeNull();
				await raw.send("DEL", key);
				expect(
					(await raw.send("RESTORE", key, "0", payload as Buffer)).toString(),
				).toBe("+OK\r\n");
			}

			expect(await raw.bulk("GET", "dr:s")).toEqual(Buffer.from("hello"));
			expect(
				(await raw.array("LRANGE", "dr:l", "0", "-1")).map((b) =>
					b?.toString(),
				),
			).toEqual(["a", "b", "c"]);
			expect(
				(await raw.array("SMEMBERS", "dr:se")).map((b) => b?.toString()).sort(),
			).toEqual(["m1", "m2"]);
			expect(
				(await raw.array("HGETALL", "dr:h")).map((b) => b?.toString()),
			).toEqual(["f1", "v1", "f2", "v2"]);
			expect(
				(await raw.array("ZRANGE", "dr:z", "0", "-1", "WITHSCORES")).map((b) =>
					b?.toString(),
				),
			).toEqual(["m2", "-2", "m1", "1.5"]);
			await raw.send("DEL", "dr:s", "dr:l", "dr:se", "dr:h", "dr:z");
		});

		test("a missing key dumps as nil, and so does an expired one", async () => {
			await raw.send("DEL", "dr:none");
			expect(await raw.bulk("DUMP", "dr:none")).toBeNull();
			await raw.send("RPUSH", "dr:gone", "a");
			await raw.send("PEXPIRE", "dr:gone", "30");
			await Bun.sleep(60);
			expect(await raw.bulk("DUMP", "dr:gone")).toBeNull();
		});

		test("the TTL argument is milliseconds, 0 meaning none", async () => {
			await raw.send("DEL", "dr:t");
			await raw.send("SET", "dr:t", "v");
			const payload = (await raw.bulk("DUMP", "dr:t")) as Buffer;
			await raw.send("DEL", "dr:t");

			await raw.send("RESTORE", "dr:t", "0", payload);
			expect(integerReply(await raw.send("TTL", "dr:t"))).toBe(-1);
			await raw.send("DEL", "dr:t");

			await raw.send("RESTORE", "dr:t", "60000", payload);
			const ttl = integerReply(await raw.send("TTL", "dr:t"));
			expect(ttl).toBeGreaterThan(55);
			expect(ttl).toBeLessThanOrEqual(60);
			await raw.send("DEL", "dr:t");

			// ABSTTL: the value is a deadline, not a duration.
			const deadline = Date.now() + 90_000;
			await raw.send(
				"RESTORE", "dr:t", String(deadline), payload, "ABSTTL",
			);
			const absttl = integerReply(await raw.send("TTL", "dr:t"));
			expect(absttl).toBeGreaterThan(85);
			expect(absttl).toBeLessThanOrEqual(90);
			await raw.send("DEL", "dr:t");
		});

		test("an occupied key is BUSYKEY unless REPLACE", async () => {
			await raw.send("DEL", "dr:busy");
			await raw.send("SET", "dr:busy", "old");
			const payload = (await raw.bulk("DUMP", "dr:busy")) as Buffer;
			await raw.send("SET", "dr:busy", "other");
			expect(
				(await raw.send("RESTORE", "dr:busy", "0", payload)).toString(),
			).toContain("BUSYKEY");
			expect(
				(
					await raw.send("RESTORE", "dr:busy", "0", payload, "REPLACE")
				).toString(),
			).toBe("+OK\r\n");
			expect(await raw.bulk("GET", "dr:busy")).toEqual(Buffer.from("old"));
			await raw.send("DEL", "dr:busy");
		});

		test("REPLACE swaps the whole key, its old type included", async () => {
			await raw.send("DEL", "dr:sw");
			await raw.send("SET", "dr:sw", "str");
			const strPayload = (await raw.bulk("DUMP", "dr:sw")) as Buffer;
			await raw.send("DEL", "dr:sw");
			await raw.send("RPUSH", "dr:sw", "a", "b");
			await raw.send(
				"RESTORE", "dr:sw", "0", strPayload, "REPLACE",
			);
			expect((await raw.send("TYPE", "dr:sw")).toString()).toContain("string");
			expect(await raw.bulk("GET", "dr:sw")).toEqual(Buffer.from("str"));
			await raw.send("DEL", "dr:sw");
		});

		test("a key past its expiry is not BUSYKEY, and does not merge", async () => {
			await raw.send("DEL", "dr:tmp", "dr:exp");
			await raw.send("RPUSH", "dr:tmp", "new");
			const payload = (await raw.bulk("DUMP", "dr:tmp")) as Buffer;
			await raw.send("DEL", "dr:tmp");

			// Expired but likely unswept: rows still in the tables. Without
			// REPLACE this must restore, and onto a clean slate.
			await raw.send("RPUSH", "dr:exp", "old1", "old2", "old3");
			await raw.send("PEXPIRE", "dr:exp", "30");
			await Bun.sleep(60);
			expect(
				(await raw.send("RESTORE", "dr:exp", "0", payload)).toString(),
			).toBe("+OK\r\n");
			expect(integerReply(await raw.send("LLEN", "dr:exp"))).toBe(1);
			expect(
				(await raw.array("LRANGE", "dr:exp", "0", "-1")).map((b) =>
					b?.toString(),
				),
			).toEqual(["new"]);
			await raw.send("DEL", "dr:exp");
		});

		test("a corrupt payload is refused loudly", async () => {
			await raw.send("DEL", "dr:bad");
			expect(
				(await raw.send("RESTORE", "dr:bad", "0", "garbage")).toString(),
			).toContain("DUMP payload version or checksum are wrong");

			// Flip one bit in a valid payload: the checksum must catch it.
			await raw.send("SET", "dr:bad", "v");
			const payload = (await raw.bulk("DUMP", "dr:bad")) as Buffer;
			await raw.send("DEL", "dr:bad");
			payload[0] ^= 0x01;
			expect(
				(await raw.send("RESTORE", "dr:bad", "0", payload)).toString(),
			).toContain("DUMP payload version or checksum are wrong");
			expect(integerReply(await raw.send("EXISTS", "dr:bad"))).toBe(0);
		});
	});
}

// The payload names no database and no storage engine, so it also moves keys
// between the halves — the same trip COPY makes, through a client instead.
describe("DUMP and RESTORE across the halves", () => {
	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	test("a cache key restores into the durable half intact", async () => {
		await raw.send("SELECT", "cache");
		await raw.send("DEL", "dr:x");
		await raw.send("ZADD", "dr:x", "1", "a", "2", "b");
		const payload = (await raw.bulk("DUMP", "dr:x")) as Buffer;
		await raw.send("DEL", "dr:x");
		await raw.send("SELECT", "durable");
		await raw.send("DEL", "dr:x");
		expect((await raw.send("RESTORE", "dr:x", "0", payload)).toString()).toBe(
			"+OK\r\n",
		);
		expect(
			(await raw.array("ZRANGE", "dr:x", "0", "-1", "WITHSCORES")).map((b) =>
				b?.toString(),
			),
		).toEqual(["a", "1", "b", "2"]);
		await raw.send("DEL", "dr:x");
	});
});

// ─────────────────── Storage policies (docs/STORAGE.md) ──────────────────────

describe("Storage policies", () => {
	const persistence = async (table: string) => {
		const [{ p }] = await sql`
			SELECT c.relpersistence::text AS p FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'redis' AND c.relname = ${table}`;
		return p;
	};
	const policyOf = async (db: number) => {
		const [{ policy }] = await sql`SELECT policy FROM redis.policies() WHERE db = ${db}`;
		return policy;
	};

	afterAll(async () => {
		await sql`SELECT redis.set_policy(13, 'logged')`;
		await sql`DELETE FROM redis.db_policy WHERE db = 13`;
		await raw.send("SELECT", "durable");
	});

	test("the defaults are the old split", async () => {
		expect(await policyOf(8)).toBe("logged");
		expect(await policyOf(15)).toBe("logged");
		expect(await policyOf(0)).toBe("memory");
		expect(await policyOf(7)).toBe("memory");
	});

	test("every database's tables are WAL-logged", async () => {
		expect(await persistence("kv_0")).toBe("p");
		expect(await persistence("hash_7")).toBe("p");
		expect(await persistence("kv_13")).toBe("p");
	});

	test("unlogged is not a policy", async () => {
		// `.then`: a Bun query only runs once something awaits it, which
		// `rejects` does not.
		await expect(sql`SELECT redis.set_policy(13, 'unlogged')`.then(() => "ok")).rejects.toThrow(
			/unknown policy 'unlogged'/,
		);
		expect(await policyOf(13)).toBe("logged");
	});

	test("an unknown policy or database is refused", async () => {
		expect(sql`SELECT redis.set_policy(13, 'tape')`.then(() => "ok")).rejects.toThrow(/unknown policy/);
		expect(sql`SELECT redis.set_policy(16, 'logged')`.then(() => "ok")).rejects.toThrow(/out of range/);
	});
});

describe("Moving a database between memory and tables", () => {
	const movingFrom = async (db: number) => {
		const [{ moving_from }] = await sql`SELECT moving_from FROM redis.policies() WHERE db = ${db}`;
		return moving_from as string | null;
	};
	const settled = async (db: number) => {
		for (let i = 0; i < 200 && (await movingFrom(db)) !== null; i++) await Bun.sleep(50);
		return movingFrom(db);
	};
	const str = async (...a: string[]) => (await raw.bulk(...a))?.toString() ?? null;
	const fill = async (prefix: string) => {
		await raw.send("SET", `${prefix}:s`, "v", "EX", "1000");
		await raw.send("HSET", `${prefix}:h`, "f", "v");
		await raw.send("RPUSH", `${prefix}:l`, "a", "b");
		await raw.send("SADD", `${prefix}:set`, "m");
		await raw.send("ZADD", `${prefix}:z`, "1.5", "m");
		await raw.send("XADD", `${prefix}:x`, "1-1", "f", "v");
	};
	const check = async (prefix: string) => {
		expect(await str("GET", `${prefix}:s`)).toBe("v");
		const ttl = integerReply(await raw.send("TTL", `${prefix}:s`));
		expect(ttl).toBeGreaterThan(900);
		expect(await str("HGET", `${prefix}:h`, "f")).toBe("v");
		expect((await raw.array("LRANGE", `${prefix}:l`, "0", "-1")).map(String)).toEqual(["a", "b"]);
		expect(integerReply(await raw.send("SISMEMBER", `${prefix}:set`, "m"))).toBe(1);
		expect(await str("ZSCORE", `${prefix}:z`, "m")).toBe("1.5");
		expect(integerReply(await raw.send("XLEN", `${prefix}:x`))).toBe(1);
	};

	afterAll(async () => {
		await settled(13);
		await sql`SELECT redis.set_policy(13, 'logged')`;
		await settled(13);
		await settled(7);
		await sql`SELECT redis.set_policy(7, 'memory')`;
		await settled(7);
		await sql`DELETE FROM redis.db_policy WHERE db IN (7, 13)`;
		await raw.send("SELECT", "durable");
	});

	test("a memory database moves to logged tables, readable throughout", async () => {
		await raw.send("SELECT", "7");
		await raw.send("FLUSHDB");
		await fill("mv");
		const [{ set_policy }] = await sql`SELECT redis.set_policy(7, 'logged')`;
		expect(set_policy).toBe("logged");
		await check("mv");
		expect(await settled(7)).toBeNull();
		const [{ n }] = await sql`SELECT count(*)::int AS n FROM redis.kv_7`;
		expect(n).toBe(1);
		await check("mv");
		expect((await str("INFO", "server")) ?? "").toContain("pg_redis_table_mode:logged");
	});

	test("a durable database takes the freed memory slot", async () => {
		await raw.send("SELECT", "13");
		await raw.send("FLUSHDB");
		await fill("mv13");
		await sql`SELECT redis.set_policy(13, 'memory')`;
		await check("mv13");
		expect(await settled(13)).toBeNull();
		const [{ n }] = await sql`SELECT count(*)::int AS n FROM redis.kv_13`;
		expect(n).toBe(0);
		await check("mv13");
	});

	test("a move turned round keeps every key, and DBSIZE is exact throughout", async () => {
		await raw.send("SELECT", "13");
		await raw.send("FLUSHDB");
		const keys = Array.from({ length: 3000 }, (_, i) => `many:${i}`);
		for (let i = 0; i < keys.length; i += 500) {
			await raw.send("MSET", ...keys.slice(i, i + 500).flatMap((k) => [k, k]));
		}
		// Straight back: it turns round if the sweep has not finished yet,
		// and moves back if it has. Either way nothing may be lost.
		await sql`SELECT redis.set_policy(13, 'logged')`;
		const [{ set_policy }] = await sql`SELECT redis.set_policy(13, 'memory')`;
		expect(set_policy).toBe("memory");
		expect(await str("GET", "many:2999")).toBe("many:2999");
		expect(integerReply(await raw.send("DBSIZE"))).toBe(3000);
		expect(await settled(13)).toBeNull();
		expect(integerReply(await raw.send("DBSIZE"))).toBe(3000);
		expect(await str("GET", "many:0")).toBe("many:0");
		await raw.send("FLUSHDB");
	});

	test("every database can be in memory at once", async () => {
		// The arena has a slot for each of the sixteen: none is refused for
		// want of one.
		const before = await sql`SELECT db, policy FROM redis.policies() WHERE policy <> 'memory'`;
		for (const { db } of before) {
			await settled(db);
			await sql`SELECT redis.set_policy(${db}, 'memory')`;
		}
		const [{ n }] = await sql`SELECT count(*)::int AS n FROM redis.policies() WHERE policy = 'memory'`;
		expect(n).toBe(16);
		for (const { db, policy } of before) {
			await settled(db);
			await sql`SELECT redis.set_policy(${db}, ${policy})`;
			await settled(db);
		}
	});
});

describe("memory+snapshot", () => {
	const rows = async (db: number) => {
		const [{ n }] = await sql`SELECT count(*)::int AS n FROM redis.snapshot WHERE db = ${db}`;
		return n as number;
	};
	const takenAt = async (db: number) => {
		const r = await sql`SELECT taken_at FROM redis.snapshot_taken WHERE db = ${db}`;
		return r.length ? new Date(r[0].taken_at).getTime() : 0;
	};

	afterAll(async () => {
		await sql`SELECT redis.set_policy(6, 'memory')`;
		await sql`DELETE FROM redis.db_policy WHERE db = 6`;
		await raw.send("SELECT", "durable");
	});

	test("SAVE writes the snapshot, BGSAVE asks for one, and memory drops it", async () => {
		await raw.send("SELECT", "6");
		await raw.send("FLUSHDB");
		await raw.send("SET", "snap:s", "v", "EX", "1000");
		await raw.send("HSET", "snap:h", "f", "v");
		await raw.send("ZADD", "snap:z", "2", "m");
		const [{ set_policy }] = await sql`SELECT redis.set_policy(6, 'memory+snapshot')`;
		expect(set_policy).toBe("memory+snapshot");
		const before = integerReply(await raw.send("LASTSAVE"));
		expect((await raw.send("SAVE")).toString()).toBe("+OK\r\n");
		expect(await rows(6)).toBe(3);
		expect(integerReply(await raw.send("LASTSAVE"))).toBeGreaterThanOrEqual(before);
		const [{ deadline_us }] = await sql`SELECT deadline_us::text FROM redis.snapshot WHERE db = 6 AND key = 'snap:s'`;
		expect(Number(deadline_us)).toBeGreaterThan(Date.now() * 1000);

		await raw.send("SET", "snap:new", "v");
		const first = await takenAt(6);
		await Bun.sleep(1100);
		expect((await raw.send("BGSAVE")).toString()).toBe("+Background saving started\r\n");
		for (let i = 0; i < 100 && (await takenAt(6)) === first; i++) await Bun.sleep(50);
		expect(await takenAt(6)).toBeGreaterThan(first);
		expect(await rows(6)).toBe(4);

		await sql`SELECT redis.set_policy(6, 'memory')`;
		expect(await rows(6)).toBe(0);
		expect((await raw.bulk("GET", "snap:new"))?.toString()).toBe("v");
		await raw.send("FLUSHDB");
	});
});

// A script, an EXEC and a multi-step command hold their database for their
// whole run, on every worker: the listener spreads connections over the
// workers, so these clients run on all of them at once.
describe("Atomicity across workers", () => {
	const CLIENTS = 16;
	const ROUNDS = 40;
	const connect = (n = CLIENTS) =>
		Promise.all(Array.from({ length: n }, () => new RawRedis().connect()));
	const closeAll = (conns: RawRedis[]) => {
		for (const c of conns) c.close();
	};
	/** An EXEC's raw reply with no nil and no error in it. */
	const execOk = (r: Buffer) => {
		const s = r.toString();
		return s.startsWith("*") && !s.startsWith("*-1") && !s.includes("\r\n-");
	};

	afterAll(async () => {
		await raw.send("SELECT", "durable");
	});

	const RMW =
		"local v = tonumber(redis.call('GET', KEYS[1]) or '0') " +
		"redis.call('SET', KEYS[1], tostring(v + 1)) return v + 1";

	for (const db of ["0", "8"]) {
		test(`db ${db}: a read-modify-write script loses no increment`, async () => {
			const key = `atomic:${db}:script`;
			await raw.send("SELECT", db);
			await raw.send("DEL", key);
			const conns = await connect();
			const errors: string[] = [];
			try {
				await Promise.all(
					conns.map(async (c) => {
						await c.send("SELECT", db);
						for (let i = 0; i < ROUNDS; i++) {
							const e = errorReply(await c.send("EVAL", RMW, "1", key));
							if (e) errors.push(e);
						}
					}),
				);
			} finally {
				closeAll(conns);
			}
			expect(errors).toEqual([]);
			expect((await raw.bulk("GET", key))?.toString()).toBe(String(CLIENTS * ROUNDS));
			await raw.send("DEL", key);
		}, 60_000);

		// No WATCH: the GET and the SET are separate commands, and only the
		// transaction holding the database keeps another one out between them.
		// Each EXEC reads the token the one before it wrote, so if they run one
		// at a time every token is read exactly once.
		test(`db ${db}: MULTI GET/SET transactions run one at a time`, async () => {
			const key = `atomic:${db}:multi`;
			await raw.send("SELECT", db);
			await raw.send("SET", key, "start");
			const conns = await connect();
			const read: string[] = [];
			const interleaved: string[] = [];
			try {
				await Promise.all(
					conns.map(async (c, n) => {
						await c.send("SELECT", db);
						for (let i = 0; i < ROUNDS; i++) {
							await c.send("MULTI");
							await c.send("GET", key);
							await c.send("SET", key, `t${n}:${i}`);
							await c.send("GET", key);
							const [before, , after] = await c.array("EXEC");
							// Nothing ran between the SET and the GET after it.
							if (after?.toString() !== `t${n}:${i}`) interleaved.push(`t${n}:${i}`);
							read.push(before?.toString() ?? "");
						}
					}),
				);
			} finally {
				closeAll(conns);
			}
			expect(interleaved).toEqual([]);
			const last = (await raw.bulk("GET", key))?.toString() ?? "";
			const written = new Set<string>(["start"]);
			for (let n = 0; n < CLIENTS; n++)
				for (let i = 0; i < ROUNDS; i++) written.add(`t${n}:${i}`);
			// Two transactions that read the same token both ran their GET
			// before either SET: a lost update.
			expect(new Set(read).size).toBe(read.length);
			expect(new Set([...read, last])).toEqual(written);
			await raw.send("DEL", key);
		}, 60_000);
	}

	test("a script on db 0 holds db 0 on every worker and leaves db 1 alone", async () => {
		const LONG_MS = 1500;
		const script = await new RawRedis().connect();
		const db1 = await connect(8);
		const db0 = await connect(8);
		try {
			await script.send("SELECT", "0");
			await script.send("DEL", "atomic:busy");
			for (const c of db1) await c.send("SELECT", "1");
			for (const c of db0) await c.send("SELECT", "0");
			// Time stands still inside a script, as in Redis, so the script
			// holds its database by sleeping rather than watching TIME.
			await script.send("DEBUG", "SET-DISABLE-DENY-SCRIPTS", "1");
			const busy =
				"redis.call('DEBUG', 'SLEEP', tonumber(ARGV[1]) / 1000) " +
				"redis.call('SET', KEYS[1], 'done') return 1";
			const started = performance.now();
			const running = script.send("EVAL", busy, "1", "atomic:busy", String(LONG_MS));
			await Bun.sleep(200);
			// Commands on db 1 go ahead on the workers not running the script.
			// The one running it serves nobody until it ends, as before.
			const fast = await Promise.all(
				db1.map(async (c, i) => {
					const t = performance.now();
					await c.send("SET", `atomic:free:${i}`, "x");
					await c.send("GET", `atomic:free:${i}`);
					return performance.now() - t;
				}),
			);
			// Commands on db 0 wait for the script on every worker, and see
			// what it wrote.
			const seen = await Promise.all(
				db0.map(async (c) => {
					const r = await c.bulk("GET", "atomic:busy");
					return { at: performance.now() - started, value: r?.toString() };
				}),
			);
			expect(errorReply(await running)).toBeNull();
			expect(Math.min(...fast)).toBeLessThan(500);
			for (const s of seen) {
				expect(s.value).toBe("done");
				expect(s.at).toBeGreaterThan(LONG_MS - 100);
			}
			await script.send("DEL", "atomic:busy");
			for (const [i, c] of db1.entries()) await c.send("DEL", `atomic:free:${i}`);
		} finally {
			await script.send("DEBUG", "SET-DISABLE-DENY-SCRIPTS", "0");
			script.close();
			closeAll(db1);
			closeAll(db0);
		}
	}, 30_000);

	test("SWAPDB, MOVE, and SELECT in MULTI and in scripts under load", async () => {
		const [A, B, L] = ["3", "4", "11"];
		const KEYS = 60;
		const ROUND = 25;
		for (const db of [A, B, L]) {
			await raw.send("SELECT", db);
			await raw.send("FLUSHDB");
		}
		await raw.send("SELECT", A);
		for (let i = 0; i < KEYS; i++) await raw.send("SET", `mv:${i}`, "v");

		// Starts on the higher database and selects the lower one.
		const crossScript =
			`redis.call('SELECT', '${L}') redis.call('INCR', 'xs') ` +
			`redis.call('SELECT', '${A}') redis.call('INCR', 'xs') return 1`;
		const errors: string[] = [];
		const check = (r: Buffer) => {
			const e = errorReply(r);
			if (e) errors.push(e);
		};
		const conns = await connect();
		try {
			await Promise.all(
				conns.map(async (c, n) => {
					for (let i = 0; i < ROUND; i++) {
						switch (n % 4) {
							case 0:
								await c.send("SELECT", A);
								check(await c.send("SWAPDB", A, B));
								break;
							case 1: {
								// In A or B, wherever SWAPDB left it, or already
								// moved by another client: MOVE answers 0 then.
								const k = `mv:${(n * ROUND + i) % KEYS}`;
								await c.send("SELECT", (n + i) % 2 ? A : B);
								check(await c.send("MOVE", k, L));
								await c.send("SELECT", L);
								check(await c.send("MOVE", k, A));
								break;
							}
							case 2: {
								await c.send("SELECT", A);
								await c.send("MULTI");
								await c.send("SELECT", L);
								await c.send("INCR", "ym");
								await c.send("SELECT", A);
								await c.send("INCR", "ym");
								const r = await c.send("EXEC");
								if (!execOk(r)) errors.push(r.toString());
								break;
							}
							case 3:
								await c.send("SELECT", L);
								check(await c.send("EVAL", crossScript, "0"));
								break;
						}
					}
				}),
			);
		} finally {
			closeAll(conns);
		}
		expect(errors).toEqual([]);

		const count = async (db: string, key: string) => {
			await raw.send("SELECT", db);
			const v = await raw.bulk("GET", key);
			return v ? Number(v.toString()) : 0;
		};
		const per = (CLIENTS / 4) * ROUND;
		// Every increment landed once: in L, and in A or B as SWAPDB left them.
		for (const key of ["xs", "ym"]) {
			expect(await count(L, key)).toBe(per);
			expect((await count(A, key)) + (await count(B, key))).toBe(per);
		}
		// MOVE and SWAPDB move keys; none is lost or doubled.
		let total = 0;
		for (const db of [A, B, L]) {
			await raw.send("SELECT", db);
			total += (await raw.array("KEYS", "mv:*")).length;
		}
		expect(total).toBe(KEYS);
		for (const db of [A, B, L]) {
			await raw.send("SELECT", db);
			await raw.send("FLUSHDB");
		}
	}, 120_000);
});

// Redis 7.2's eviction policies and access clock (src/evict.rs, src/access.rs,
// src/memdb/lru.rs). Memory database 0 for eviction; logged database 8 for
// OBJECT, which must answer there too. Every test puts the limit and the
// policy back, and the memory databases are emptied first, since an allkeys
// policy may take a key from any of them.
describe("Eviction and access tracking", () => {
	const usedMemory = async (): Promise<number> => {
		const info = (await raw.bulk("INFO", "memory"))?.toString() ?? "";
		return Number(info.match(/used_memory:(\d+)/)?.[1] ?? 0);
	};
	const stat = async (name: string): Promise<number> => {
		const info = (await raw.bulk("INFO", "stats"))?.toString() ?? "";
		return Number(info.match(new RegExp(`${name}:(\\d+)`))?.[1] ?? -1);
	};
	const reset = async () => {
		await raw.send("CONFIG", "SET", "maxmemory", "0", "maxmemory-policy", "noeviction");
		await raw.send("CONFIG", "SET", "notify-keyspace-events", "", "lfu-decay-time", "1");
	};
	const within = <T>(p: Promise<T>, ms: number) =>
		Promise.race([p, Bun.sleep(ms).then(() => null)]);
	const exists = async (key: string) => integerReply(await raw.send("EXISTS", key)) === 1;
	const value = "x".repeat(200);

	beforeAll(async () => {
		for (let db = 0; db < 8; db++) {
			await raw.send("SELECT", String(db));
			await raw.send("FLUSHDB");
		}
		await raw.send("SELECT", "0");
	});

	afterAll(async () => {
		await reset();
		await raw.send("SELECT", "0");
		await raw.send("FLUSHDB");
		await raw.send("SELECT", "durable");
	});

	test("every policy is accepted, reported, and refused when misspelt", async () => {
		try {
			for (const p of [
				"noeviction",
				"allkeys-lru",
				"allkeys-lfu",
				"allkeys-random",
				"volatile-lru",
				"volatile-lfu",
				"volatile-random",
				"volatile-ttl",
			]) {
				expect((await raw.send("CONFIG", "SET", "maxmemory-policy", p)).toString()).toBe("+OK\r\n");
				expect((await raw.array("CONFIG", "GET", "maxmemory-policy"))[1]?.toString()).toBe(p);
				expect((await raw.bulk("INFO", "memory"))?.toString()).toContain(`maxmemory_policy:${p}`);
			}
			expect(errorReply(await raw.send("CONFIG", "SET", "maxmemory-policy", "lru"))).toStartWith(
				"ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: volatile-lru,",
			);
			expect(errorReply(await raw.send("CONFIG", "SET", "maxmemory-samples", "0"))).toContain(
				"argument must be between 1 and 2147483647 inclusive",
			);
		} finally {
			await reset();
		}
	});

	for (const policy of ["volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl"]) {
		test(`${policy} evicts only keys with an expiry, then refuses writes`, async () => {
			await raw.send("FLUSHDB");
			try {
				for (let i = 0; i < 200; i++) {
					await raw.send("SET", `p:${i}`, value);
					await raw.send("SET", `v:${i}`, value, "EX", String(1000 + i));
				}
				const before = await stat("evicted_keys");
				const used = await usedMemory();
				await raw.send("CONFIG", "SET", "maxmemory-policy", policy);
				await raw.send("CONFIG", "SET", "maxmemory", String(used - 20_000));
				expect((await raw.send("SET", "trigger", "1")).toString()).toBe("+OK\r\n");
				const evicted = (await stat("evicted_keys")) - before;
				expect(evicted).toBeGreaterThan(10);
				for (let i = 0; i < 200; i++) expect(await exists(`p:${i}`)).toBe(true);
				const gone: number[] = [];
				for (let i = 0; i < 200; i++) if (!(await exists(`v:${i}`))) gone.push(i);
				expect(gone.length).toBe(evicted);
				if (policy === "volatile-ttl") {
					// The soonest deadlines go first: what went is from the low end.
					const mean = gone.reduce((a, b) => a + b, 0) / gone.length;
					expect(mean).toBeLessThan(60);
				}
				// With every volatile key gone, nothing may be evicted: OOM.
				await raw.send("CONFIG", "SET", "maxmemory", "1");
				expect(errorReply(await raw.send("SET", "more", value))).toStartWith("OOM ");
				expect(await exists("p:0")).toBe(true);
				for (let i = 0; i < 200; i++) expect(await exists(`v:${i}`)).toBe(false);
			} finally {
				await reset();
				await raw.send("FLUSHDB");
			}
		});
	}

	// Redis's own LRU test, in outline: keys touched after a pause outlive
	// keys left alone, because an idle key scores higher in the pool.
	test("allkeys-lru keeps the keys used recently", async () => {
		await raw.send("FLUSHDB");
		try {
			await raw.send("CONFIG", "SET", "maxmemory-policy", "allkeys-lru");
			const empty = await usedMemory();
			for (let i = 0; i < 1000; i++) await raw.send("SET", `k:${i}`, value);
			const full = await usedMemory();
			await Bun.sleep(2100);
			for (let i = 0; i < 500; i++) await raw.send("GET", `k:${i}`);
			await raw.send("CONFIG", "SET", "maxmemory", String(full - Math.floor((full - empty) * 0.25)));
			await raw.send("PING");
			let touched = 0;
			let idle = 0;
			for (let i = 0; i < 1000; i++) {
				const here = await exists(`k:${i}`);
				if (here && i < 500) touched++;
				if (here && i >= 500) idle++;
			}
			expect(idle).toBeLessThan(450);
			expect(touched).toBeGreaterThan(475);
		} finally {
			await reset();
			await raw.send("FLUSHDB");
		}
	});

	test("allkeys-lfu keeps the keys used often", async () => {
		await raw.send("FLUSHDB");
		try {
			// The counter decays a step for each minute boundary its clock
			// crosses, as Redis's does, which took k:999 to 4 whenever one
			// fell during the test. Without decay the counts are only the reads.
			await raw.send("CONFIG", "SET", "maxmemory-policy", "allkeys-lfu", "lfu-decay-time", "0");
			const empty = await usedMemory();
			for (let i = 0; i < 1000; i++) await raw.send("SET", `k:${i}`, value);
			const full = await usedMemory();
			// The counter climbs by chance: past 6 it takes one in eleven reads
			// at the default lfu-log-factor, so 20 rounds left it at 6 one run
			// in six. A hundred leave it there one run in ten thousand.
			for (let round = 0; round < 100; round++) {
				await Promise.all(Array.from({ length: 200 }, (_, i) => raw.send("GET", `k:${i}`)));
			}
			expect(integerReply(await raw.send("OBJECT", "FREQ", "k:0"))).toBeGreaterThan(6);
			expect(integerReply(await raw.send("OBJECT", "FREQ", "k:999"))).toBe(5);
			await raw.send("CONFIG", "SET", "maxmemory", String(full - Math.floor((full - empty) * 0.25)));
			await raw.send("PING");
			let hot = 0;
			for (let i = 0; i < 200; i++) if (await exists(`k:${i}`)) hot++;
			expect(hot).toBeGreaterThan(190);
			expect(integerReply(await raw.send("DBSIZE"))).toBeLessThan(950);
		} finally {
			await reset();
			await raw.send("FLUSHDB");
		}
	});

	test("an evicted key raises `evicted` and invalidates a tracking client", async () => {
		await raw.send("FLUSHDB");
		const sub = await new RawRedis().connect();
		const inval = await new RawRedis().connect();
		const tracker = await new RawRedis().connect();
		try {
			await raw.send("CONFIG", "SET", "notify-keyspace-events", "Ee");
			await sub.send("PSUBSCRIBE", "__keyevent@0__:evicted");
			const invalId = integerReply(await inval.send("CLIENT", "ID"));
			await inval.send("SUBSCRIBE", "__redis__:invalidate");
			await tracker.send("SELECT", "0");
			await tracker.send("CLIENT", "TRACKING", "on", "REDIRECT", String(invalId));
			await raw.send("SET", "ev:k", value, "EX", "1000");
			await tracker.send("GET", "ev:k");
			const before = await stat("evicted_keys");
			const event = sub.next();
			const invalidation = inval.next();
			// The only key with an expiry: volatile-ttl takes it and nothing
			// else, and with no command to start it, the worker's next round.
			await raw.send("CONFIG", "SET", "maxmemory-policy", "volatile-ttl");
			await raw.send("CONFIG", "SET", "maxmemory", String((await usedMemory()) - 1));
			const got = (await within(event, 3000))?.toString() ?? "";
			expect(got).toContain("__keyevent@0__:evicted");
			expect(got).toContain("ev:k");
			const inv = (await within(invalidation, 3000))?.toString() ?? "";
			expect(inv).toContain("__redis__:invalidate");
			expect(inv).toContain("ev:k");
			expect(await stat("evicted_keys")).toBe(before + 1);
			expect(await stat("evicted_clients")).toBe(0);
		} finally {
			await reset();
			sub.close();
			inval.close();
			tracker.close();
		}
	});

	for (const half of ["0", "8"]) {
		test(`OBJECT IDLETIME grows and resets on access (db ${half})`, async () => {
			await raw.send("SELECT", half);
			try {
				await raw.send("DEL", "at:k");
				await raw.send("SET", "at:k", "v");
				expect(integerReply(await raw.send("OBJECT", "IDLETIME", "at:k"))).toBeLessThanOrEqual(1);
				await Bun.sleep(2100);
				// TTL, TYPE and EXISTS look without touching.
				await raw.send("TTL", "at:k");
				await raw.send("TYPE", "at:k");
				await raw.send("EXISTS", "at:k");
				expect(integerReply(await raw.send("OBJECT", "IDLETIME", "at:k"))).toBeGreaterThanOrEqual(2);
				await raw.send("GET", "at:k");
				expect(integerReply(await raw.send("OBJECT", "IDLETIME", "at:k"))).toBeLessThanOrEqual(1);
				expect(await raw.bulk("OBJECT", "IDLETIME", "at:absent")).toBeNull();
				expect(errorReply(await raw.send("OBJECT", "FREQ", "at:k"))).toBe(
					"ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
				);
			} finally {
				await raw.send("DEL", "at:k");
				await raw.send("SELECT", "0");
			}
		});

		test(`OBJECT FREQ counts under LFU (db ${half})`, async () => {
			await raw.send("SELECT", half);
			try {
				await raw.send("CONFIG", "SET", "maxmemory-policy", "allkeys-lfu");
				await raw.send("DEL", "af:k");
				await raw.send("SET", "af:k", "v");
				expect(integerReply(await raw.send("OBJECT", "FREQ", "af:k"))).toBe(5);
				for (let i = 0; i < 30; i++) await raw.send("GET", "af:k");
				expect(integerReply(await raw.send("OBJECT", "FREQ", "af:k"))).toBeGreaterThan(5);
				expect(errorReply(await raw.send("OBJECT", "IDLETIME", "af:k"))).toBe(
					"ERR An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
				);
				expect(await raw.bulk("OBJECT", "FREQ", "af:absent")).toBeNull();
			} finally {
				await reset();
				await raw.send("DEL", "af:k");
				await raw.send("SELECT", "0");
			}
		});

		test(`CLIENT NO-TOUCH leaves the clock alone but TOUCH still touches (db ${half})`, async () => {
			await raw.send("SELECT", half);
			try {
				await raw.send("SET", "nt:k", "v");
				await Bun.sleep(2100);
				await raw.send("CLIENT", "NO-TOUCH", "ON");
				await raw.send("GET", "nt:k");
				expect(integerReply(await raw.send("OBJECT", "IDLETIME", "nt:k"))).toBeGreaterThanOrEqual(2);
				await raw.send("TOUCH", "nt:k");
				expect(integerReply(await raw.send("OBJECT", "IDLETIME", "nt:k"))).toBeLessThanOrEqual(1);
			} finally {
				await raw.send("CLIENT", "NO-TOUCH", "OFF");
				await raw.send("DEL", "nt:k");
				await raw.send("SELECT", "0");
			}
		});

		test(`RESTORE sets IDLETIME and FREQ, and REFCOUNT follows sharing (db ${half})`, async () => {
			await raw.send("SELECT", half);
			try {
				await raw.send("SET", "rs:k", "bar");
				const dump = (await raw.bulk("DUMP", "rs:k")) as Buffer;
				await raw.send("DEL", "rs:k");
				await raw.send("CONFIG", "SET", "maxmemory-policy", "allkeys-lru");
				expect((await raw.send("RESTORE", "rs:k", "0", dump, "IDLETIME", "1000")).toString()).toBe("+OK\r\n");
				const idle = integerReply(await raw.send("OBJECT", "IDLETIME", "rs:k"));
				expect(idle).toBeGreaterThanOrEqual(1000);
				expect(idle).toBeLessThanOrEqual(1010);
				await raw.send("DEL", "rs:k");
				await raw.send("CONFIG", "SET", "maxmemory-policy", "allkeys-lfu");
				await raw.send("RESTORE", "rs:k", "0", dump, "FREQ", "100");
				expect(integerReply(await raw.send("OBJECT", "FREQ", "rs:k"))).toBe(100);
				expect(errorReply(await raw.send("RESTORE", "rs:k", "0", dump, "REPLACE", "FREQ", "256"))).toBe(
					"ERR Invalid FREQ value, must be >= 0 and <= 255",
				);
				expect(errorReply(await raw.send("RESTORE", "rs:k", "0", dump, "REPLACE", "IDLETIME", "1", "FREQ", "1"))).toBe(
					"ERR syntax error",
				);
				// Small integers are shared objects unless an LRU or LFU policy
				// has a limit to keep per key.
				await raw.send("CONFIG", "SET", "maxmemory", "1gb");
				await raw.send("SET", "rs:n", "100");
				expect(integerReply(await raw.send("OBJECT", "REFCOUNT", "rs:n"))).toBe(1);
				await raw.send("CONFIG", "SET", "maxmemory-policy", "noeviction");
				expect(integerReply(await raw.send("OBJECT", "REFCOUNT", "rs:n"))).toBe(2147483647);
				expect(integerReply(await raw.send("OBJECT", "REFCOUNT", "rs:k"))).toBe(1);
				expect(await raw.bulk("OBJECT", "REFCOUNT", "rs:absent")).toBeNull();
			} finally {
				await reset();
				await raw.send("DEL", "rs:k", "rs:n");
				await raw.send("SELECT", "0");
			}
		});
	}
});

describe("ACL", () => {
	/** A RESP2 map reply as an object. */
	const asMap = (v: Resp): Record<string, Resp> => {
		const a = v as Resp[];
		const out: Record<string, Resp> = {};
		for (let i = 0; i < a.length; i += 2) out[a[i] as string] = a[i + 1];
		return out;
	};
	const cmd = async (c: RawRedis, ...args: string[]) => decode(await c.send(...args));
	/** A connection authenticated as `user`. */
	const as = async (user: string, password: string) => {
		const c = await new RawRedis().connect({ password: "" });
		expect(await cmd(c, "AUTH", user, password)).toBe("OK");
		return c;
	};
	let admin: RawRedis;
	const users = ["acl-app", "acl-ro", "acl-ps", "acl-gone", "acl-saved", "acl-multi", "acl-blk", "acl-file", "acl-bad"];
	/** `aclfile`, relative to the data directory; empty when none is set. */
	let aclfile = "";
	let dataDir = "";

	beforeAll(async () => {
		admin = await new RawRedis().connect();
		await admin.send("ACL", "DELUSER", ...users);
		await admin.send("ACL", "LOG", "RESET");
		aclfile = ((await cmd(admin, "CONFIG", "GET", "aclfile")) as string[])[1] ?? "";
		dataDir = (await sql`SHOW data_directory`)[0].data_directory as string;
	});

	afterAll(async () => {
		await admin.send("ACL", "DELUSER", ...users);
		if (aclfile) await cmd(admin, "ACL", "SAVE");
		admin.close();
	});

	test("the default user carries redis.password", async () => {
		const d = asMap(await cmd(admin, "ACL", "GETUSER", "default"));
		expect(d.flags).toEqual(["on", "sanitize-payload"]);
		const hash = new Bun.CryptoHasher("sha256").update(REDIS_PASSWORD).digest("hex");
		expect(d.passwords).toEqual([hash]);
		expect(await cmd(admin, "ACL", "WHOAMI")).toBe("default");
		const c = await new RawRedis().connect({ password: "" });
		expect(await cmd(c, "AUTH", "default", "wrong")).toEqual({
			error: "WRONGPASS invalid username-password pair or user is disabled.",
		});
		expect(await cmd(c, "AUTH", "default", REDIS_PASSWORD)).toBe("OK");
		c.close();
		const log = asMap(((await cmd(admin, "ACL", "LOG", "1")) as Resp[])[0]);
		expect(log).toMatchObject({ reason: "auth", object: "AUTH", username: "default" });
	});

	test("SETUSER, GETUSER and LIST describe a user as Redis does", async () => {
		expect(
			await cmd(
				admin,
				"ACL",
				"SETUSER",
				"acl-app",
				"on",
				">pw",
				"~app:*",
				"%R~shared:*",
				"&news:*",
				"+@all",
				"-@dangerous",
				"(+get ~sel:*)",
			),
		).toBe("OK");
		const list = (await cmd(admin, "ACL", "LIST")) as string[];
		expect(list).toContain(
			"user acl-app on sanitize-payload #30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4 ~app:* %R~shared:* resetchannels &news:* +@all -@dangerous (~sel:* resetchannels -@all +get)",
		);
		const u = asMap(await cmd(admin, "ACL", "GETUSER", "acl-app"));
		expect(u.commands).toBe("+@all -@dangerous");
		expect(u.keys).toBe("~app:* %R~shared:*");
		expect(u.channels).toBe("&news:*");
		expect((u.selectors as Resp[]).map(asMap)[0].keys).toBe("~sel:*");
		expect(await cmd(admin, "ACL", "SETUSER", "acl-app", "+nosuch")).toEqual({
			error: "ERR Error in ACL SETUSER modifier '+nosuch': Unknown command or category name in ACL",
		});
	});

	test("commands, keys and channels are refused with Redis's NOPERM texts", async () => {
		await admin.send("ACL", "LOG", "RESET");
		const c = await as("acl-app", "pw");
		expect(await cmd(c, "ACL", "WHOAMI")).toBe("acl-app");
		expect(await cmd(c, "SET", "app:1", "v")).toBe("OK");
		expect(await cmd(c, "GET", "shared:1")).toBeNull();
		expect(await cmd(c, "SET", "shared:1", "v")).toEqual({ error: "NOPERM No permissions to access a key" });
		expect(await cmd(c, "GET", "other")).toEqual({ error: "NOPERM No permissions to access a key" });
		expect(await cmd(c, "GET", "sel:1")).toBeNull();
		expect(await cmd(c, "FLUSHALL")).toEqual({
			error: "NOPERM User acl-app has no permissions to run the 'flushall' command",
		});
		expect(await cmd(c, "PUBLISH", "news:1", "m")).toBe(0);
		expect(await cmd(c, "PUBLISH", "sport", "m")).toEqual({ error: "NOPERM No permissions to access a channel" });
		c.close();
		await admin.send("DEL", "app:1");
		const log = ((await cmd(admin, "ACL", "LOG")) as Resp[]).map(asMap);
		expect(log[0]).toMatchObject({ reason: "channel", context: "toplevel", object: "sport", username: "acl-app" });
		expect(log[1]).toMatchObject({ reason: "command", object: "flushall" });
		expect(log[2]).toMatchObject({ reason: "key", object: "other" });
		expect(log[3]).toMatchObject({ reason: "key", object: "shared:1" });
		expect(log[0]["client-info"]).toContain("user=acl-app");
		const stats = (await cmd(admin, "INFO", "stats")) as string;
		expect(stats).toMatch(/acl_access_denied_channel:[1-9]/);
	});

	test("DRYRUN answers for another user without running anything", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-ro", "on", "nopass", "+@read", "%R~r*", "resetchannels");
		expect(await cmd(admin, "ACL", "DRYRUN", "acl-ro", "GET", "r1")).toBe("OK");
		expect(await cmd(admin, "ACL", "DRYRUN", "acl-ro", "SET", "r1", "v")).toBe(
			"User acl-ro has no permissions to run the 'set' command",
		);
		expect(await cmd(admin, "ACL", "DRYRUN", "acl-ro", "GET", "x")).toBe(
			"User acl-ro has no permissions to access the 'x' key",
		);
		expect(await cmd(admin, "ACL", "DRYRUN", "nobody", "GET", "x")).toEqual({ error: "ERR User 'nobody' not found" });
	});

	test("MULTI refuses at queue time and EXEC checks again", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-multi", "on", ">pw", "~*", "+multi", "+exec", "+incr", "+get");
		const c = await as("acl-multi", "pw");
		expect(await cmd(c, "MULTI")).toBe("OK");
		expect(await cmd(c, "SET", "k", "v")).toEqual({
			error: "NOPERM User acl-multi has no permissions to run the 'set' command",
		});
		expect(await cmd(c, "EXEC")).toEqual({ error: "EXECABORT Transaction discarded because of previous errors." });
		expect(await cmd(c, "MULTI")).toBe("OK");
		expect(await cmd(c, "INCR", "acl:ctr")).toBe("QUEUED");
		expect(await cmd(c, "GET", "acl:ctr")).toBe("QUEUED");
		await cmd(admin, "ACL", "SETUSER", "acl-multi", "-incr");
		const r = (await cmd(c, "EXEC")) as Resp[];
		expect((r[0] as { error: string }).error).toStartWith(
			"NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call.",
		);
		expect(r[1]).toBeNull();
		const log = asMap(((await cmd(admin, "ACL", "LOG", "1")) as Resp[])[0]);
		expect(log).toMatchObject({ context: "multi", object: "incr" });
		c.close();
		await admin.send("DEL", "acl:ctr");
	});

	test("a queued AUTH switches the user in its turn at EXEC", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-later", "on", ">pw2", "~*", "+@all", "-set");
		await cmd(admin, "ACL", "SETUSER", "acl-multi", "+acl|whoami", "+auth");
		const c = await as("acl-multi", "pw");
		expect(await cmd(c, "MULTI")).toBe("OK");
		expect(await cmd(c, "ACL", "WHOAMI")).toBe("QUEUED");
		expect(await cmd(c, "AUTH", "acl-later", "wrong")).toBe("QUEUED");
		expect(await cmd(c, "AUTH", "acl-later", "pw2")).toBe("QUEUED");
		expect(await cmd(c, "ACL", "WHOAMI")).toBe("QUEUED");
		const r = (await cmd(c, "EXEC")) as Resp[];
		expect(r[0]).toBe("acl-multi");
		expect((r[1] as { error: string }).error).toStartWith("WRONGPASS");
		expect(r[2]).toBe("OK");
		expect(r[3]).toBe("acl-later");
		expect(await cmd(c, "ACL", "WHOAMI")).toBe("acl-later");
		c.close();
		await cmd(admin, "ACL", "DELUSER", "acl-later");
	});

	test("redis.call checks the script caller's permissions", async () => {

		await cmd(admin, "ACL", "SETUSER", "acl-multi", "+eval", "resetkeys", "~ok:*");
		const c = await as("acl-multi", "pw");
		expect(await cmd(c, "EVAL", "return redis.call('get', KEYS[1])", "1", "ok:1")).toBeNull();
		const r = (await cmd(c, "EVAL", "return redis.call('get', 'secret')", "0")) as { error: string };
		expect(r.error).toStartWith("ERR ACL failure in script: No permissions to access a key");
		// false, which Lua returns to the client as nil.
		expect(await cmd(c, "EVAL", "return redis.acl_check_cmd('get', 'secret')", "0")).toBeNull();
		expect(await cmd(c, "EVAL", "return redis.acl_check_cmd('get', 'ok:2')", "0")).toBe(1);
		c.close();
		const log = asMap(((await cmd(admin, "ACL", "LOG", "1")) as Resp[])[0]);
		expect(log).toMatchObject({ context: "lua", reason: "key", object: "secret" });
	});

	test("DELUSER closes the user's connections; off does not", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-gone", "on", ">pw", "+@all", "~*");
		const c = await as("acl-gone", "pw");
		expect(await cmd(c, "CLIENT", "SETNAME", "acl-gone-conn")).toBe("OK");
		expect(await cmd(admin, "CLIENT", "LIST")).toMatch(/name=acl-gone-conn .* user=acl-gone /);
		expect(await cmd(admin, "ACL", "SETUSER", "acl-gone", "off")).toBe("OK");
		expect(await cmd(c, "PING")).toBe("PONG");
		expect(await cmd(admin, "ACL", "DELUSER", "acl-gone")).toBe(1);
		expect(await cmd(admin, "CLIENT", "LIST")).not.toContain("name=acl-gone-conn");
		expect((await c.send("PING")).length).toBe(0);
		expect(await cmd(admin, "CLIENT", "KILL", "USER", "acl-gone")).toEqual({ error: "ERR No such user 'acl-gone'" });
	});

	test("CLIENT KILL USER closes that user's clients", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-gone", "on", ">pw", "+@all", "~*");
		const c = await as("acl-gone", "pw");
		expect(await cmd(admin, "CLIENT", "KILL", "USER", "acl-gone")).toBe(1);
		expect((await c.send("PING")).length).toBe(0);
	});

	test("a subscriber that loses its channel is closed", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-ps", "on", ">pw", "+@all", "resetchannels", "&foo:*");
		const c = await as("acl-ps", "pw");
		await cmd(c, "CLIENT", "SETNAME", "acl-ps-sub");
		expect(await cmd(c, "SUBSCRIBE", "foo:1")).toEqual(["subscribe", "foo:1", 1]);
		// Still allowed: the subscriber stays.
		await cmd(admin, "ACL", "SETUSER", "acl-ps", "&bar");
		expect(await cmd(admin, "CLIENT", "LIST")).toContain("name=acl-ps-sub");
		await cmd(admin, "ACL", "SETUSER", "acl-ps", "resetchannels", "&bar");
		expect(await cmd(admin, "CLIENT", "LIST")).not.toContain("name=acl-ps-sub");
		c.close();
	});

	test("a blocked command is refused when the rules change before it is served", async () => {
		await cmd(admin, "ACL", "SETUSER", "acl-blk", "on", ">pw", "+@all", "~*");
		const c = await as("acl-blk", "pw");
		await admin.send("DEL", "acl:list");
		const blocked = c.send("BLPOP", "acl:list", "0");
		await Bun.sleep(100);
		await cmd(admin, "ACL", "SETUSER", "acl-blk", "resetkeys");
		await admin.send("LPUSH", "acl:list", "x");
		expect(decode(await blocked)).toEqual({ error: "NOPERM No permissions to access a key" });
		c.close();
		await admin.send("DEL", "acl:list");
	});

	// As in Redis: without an ACL file there is nothing to save to or load from.
	test("ACL SAVE and ACL LOAD refuse without an aclfile", async () => {
		if (aclfile) return;
		const refusal = {
			error:
				"ERR This Redis instance is not configured to use an ACL file. You may want to specify " +
				"users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a " +
				"Redis configuration file set) in order to store users in the Redis configuration.",
		};
		expect(await cmd(admin, "ACL", "SAVE")).toEqual(refusal);
		expect(await cmd(admin, "ACL", "LOAD")).toEqual(refusal);
	});

	test("ACL SAVE writes the aclfile and ACL LOAD reads it back", async () => {
		if (!aclfile) return;
		const read = async () =>
			(await sql`SELECT pg_read_file(${aclfile}) AS text`)[0].text as string;
		await cmd(admin, "ACL", "SETUSER", "acl-saved", "on", ">pw", "~s:*", "+get");
		const c = await as("acl-saved", "pw");
		expect(await cmd(admin, "ACL", "SAVE")).toBe("OK");
		const line = (await read()).split("\n").find((l) => l.startsWith("user acl-saved "));
		expect(line).toContain("#30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4 ~s:*");
		expect(await cmd(admin, "ACL", "DELUSER", "acl-saved")).toBe(1);
		expect(await cmd(admin, "ACL", "GETUSER", "acl-saved")).toBeNull();
		expect(await cmd(admin, "ACL", "LOAD")).toBe("OK");
		expect(asMap(await cmd(admin, "ACL", "GETUSER", "acl-saved")).keys).toBe("~s:*");
		const again = await as("acl-saved", "pw");
		expect(await cmd(again, "GET", "s:1")).toBeNull();
		// LOAD replaces every user but default: their clients are closed.
		expect(await cmd(admin, "ACL", "LOAD")).toBe("OK");
		expect((await again.send("PING")).length).toBe(0);
		c.close();
		// A line an operator wrote, with a cleartext password, loads too.
		// The file, as the server sees it: one line per row, which COPY writes
		// as they are while no line holds a tab or a backslash.
		const write = (text: string) =>
			sql.unsafe(
				`COPY (SELECT unnest(string_to_array(convert_from(decode('${Buffer.from(text).toString("hex")}', 'hex'), 'UTF8'), E'\\n'))) TO '${dataDir}/${aclfile}'`,
			);
		await write(`${await read()}user acl-file on >filepw ~* +ping`);
		expect(await cmd(admin, "ACL", "LOAD")).toBe("OK");
		const viaFile = await as("acl-file", "filepw");
		expect(await cmd(viaFile, "PING")).toBe("PONG");
		viaFile.close();
		// A wrong line loads nothing, and says where it is.
		const before = await cmd(admin, "ACL", "USERS");
		await write(`${await read()}user acl-bad on +nosuch`);
		const bad = await cmd(admin, "ACL", "LOAD");
		expect(JSON.stringify(bad)).toContain(
			`${aclfile}:`,
		);
		expect(JSON.stringify(bad)).toContain(
			"Error in applying operation '+nosuch': Unknown command or category name in ACL. WARNING: ACL errors detected, no change to the previously active ACL rules was performed",
		);
		expect(await cmd(admin, "ACL", "USERS")).toEqual(before);
		expect(await cmd(admin, "ACL", "SAVE")).toBe("OK");
		expect(await cmd(admin, "ACL", "WHOAMI")).toBe("default");
		expect(await cmd(admin, "PING")).toBe("PONG");
	});

	test("CONFIG SET requirepass sets the default user's password", async () => {
		expect(await cmd(admin, "CONFIG", "SET", "requirepass", "other")).toBe("OK");
		const c = await new RawRedis().connect({ password: "" });
		expect(await cmd(c, "AUTH", REDIS_PASSWORD)).toEqual({
			error: "WRONGPASS invalid username-password pair or user is disabled.",
		});
		expect(await cmd(c, "AUTH", "other")).toBe("OK");
		expect(await cmd(admin, "CONFIG", "SET", "requirepass", REDIS_PASSWORD)).toBe("OK");
		c.close();
	});
});
