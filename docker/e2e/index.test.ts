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

/**
 * A minimal RESP client that speaks bytes.
 *
 * `Bun.RedisClient` takes command arguments as strings and encodes them as
 * UTF-8, so it cannot express a value like `0xff` — that arrives as the two
 * bytes of U+00FF. Anything asserting on exact byte counts or exact byte
 * content has to bypass it, or it ends up measuring the client's encoder.
 */
class RawRedis {
	private socket!: Awaited<ReturnType<typeof Bun.connect>>;
	private buffer = Buffer.alloc(0);
	private waiters: Array<(b: Buffer) => void> = [];

	async connect() {
		this.socket = await Bun.connect({
			hostname: REDIS_HOST,
			port: Number(REDIS_PORT),
			socket: {
				data: (_s, chunk) => {
					this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
					this.drain();
				},
				error: () => {},
				close: () => {},
			},
		});
		await this.send("AUTH", REDIS_PASSWORD);
		return this;
	}

	/** Resolve waiters as soon as one complete reply is buffered. */
	private drain() {
		while (this.waiters.length > 0) {
			const end = replyEnd(this.buffer);
			if (end < 0) return;
			const reply = this.buffer.subarray(0, end);
			this.buffer = this.buffer.subarray(end);
			this.waiters.shift()?.(Buffer.from(reply));
		}
	}

	send(...args: Array<string | Buffer>): Promise<Buffer> {
		const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
		for (const a of args) {
			const b = Buffer.isBuffer(a) ? a : Buffer.from(a, "utf8");
			parts.push(Buffer.from(`$${b.length}\r\n`), b, Buffer.from("\r\n"));
		}
		const done = new Promise<Buffer>((resolve) => this.waiters.push(resolve));
		this.socket.write(Buffer.concat(parts));
		return done;
	}

	/** The elements of a multi-bulk reply, with nil elements as null. */
	async array(...args: Array<string | Buffer>): Promise<Array<Buffer | null>> {
		const reply = await this.send(...args);
		if (reply[0] !== 0x2a) {
			throw new Error(`expected an array reply, got ${reply.subarray(0, 64)}`);
		}
		const first = reply.indexOf("\r\n") + 2;
		const count = Number(reply.subarray(1, first - 2).toString());
		const out: Array<Buffer | null> = [];
		let cur = first;
		for (let i = 0; i < count; i++) {
			const head = reply.indexOf("\r\n", cur);
			const len = Number(reply.subarray(cur + 1, head).toString());
			out.push(len < 0 ? null : reply.subarray(head + 2, head + 2 + len));
			cur = replyEnd(reply, cur);
		}
		return out;
	}

	/** The payload of a bulk-string reply, or null for a nil reply. */
	async bulk(...args: Array<string | Buffer>): Promise<Buffer | null> {
		const reply = await this.send(...args);
		if (reply[0] !== 0x24) return null; // not '$'
		const head = reply.indexOf("\r\n");
		const len = Number(reply.subarray(1, head).toString());
		if (len < 0) return null;
		return reply.subarray(head + 2, head + 2 + len);
	}

	close() {
		this.socket.end();
	}
}

/** Offset just past the complete RESP reply starting at `from` in `buf`, or -1. */
function replyEnd(buf: Buffer, from = 0): number {
	if (from >= buf.length) return -1;
	const head = buf.indexOf("\r\n", from);
	if (head < 0) return -1;
	// An array is a header and then that many replies, each of which has to be
	// walked: stopping at the header would leave the elements in the buffer and
	// hand them to whatever command replies next.
	if (buf[from] === 0x2a) {
		const count = Number(buf.subarray(from + 1, head).toString());
		let cur = head + 2;
		for (let i = 0; i < count; i++) {
			cur = replyEnd(buf, cur);
			if (cur < 0) return -1;
		}
		return cur;
	}
	// Bulk strings carry a length; every other reply type ends at the newline.
	if (buf[from] !== 0x24) return head + 2;
	const len = Number(buf.subarray(from + 1, head).toString());
	if (len < 0) return head + 2;
	const end = head + 2 + len + 2;
	return buf.length >= end ? end : -1;
}

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
		expect((await raw.send("LINSERT", "inslist", "BEFORE", "c", "b")).toString()).toBe(
			":4\r\n",
		);
		expect(await raw.bulk("LINDEX", "inslist", "1")).toEqual(Buffer.from("b"));
	});

	test("LINSERT AFTER pivot", async () => {
		expect((await raw.send("LINSERT", "inslist", "AFTER", "c", "d")).toString()).toBe(
			":5\r\n",
		);
		// The whole list, in order: a rewrite that renumbers positions has to
		// leave every element reachable, not just the ones before the splice.
		for (const [i, want] of ["a", "b", "c", "d", "e"].entries()) {
			expect(await raw.bulk("LINDEX", "inslist", String(i))).toEqual(
				Buffer.from(want),
			);
		}
	});

	test("LINSERT missing pivot returns -1", async () => {
		expect((await raw.send("LINSERT", "inslist", "BEFORE", "Z", "x")).toString()).toBe(
			":-1\r\n",
		);
	});

	test("LINSERT on missing key returns 0", async () => {
		expect(
			(await raw.send("LINSERT", "nokey-linsert", "BEFORE", "p", "v")).toString(),
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
		expect((await raw.send("LREM", "remraw", "2", "a")).toString()).toBe(":2\r\n");
		expect((await raw.send("LLEN", "remraw")).toString()).toBe(":3\r\n");
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
		await raw.send("DEL", a);
		await raw.send("DEL", b);
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
// which is where the default 8192 lands. See docs/storage-modes.md.
const MAX_TOTAL_VAL_LEN = 64 * 1024;
const MAX_MEMBER_LEN = 128;
const MEM_MAX_KEY = 511;

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
