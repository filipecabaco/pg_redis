/**
 * The same commands to pg_redis and to a real Redis, replies compared byte for
 * byte, on both halves — they are different backends and can differ from Redis
 * in different places.
 *
 *   bun run parity.ts                     # report, exit 1 on a new difference
 *   bun run parity.ts --update-baseline   # record what differs today
 */

import { RawRedis } from "./resp";

const PG_HOST = process.env.PG_REDIS_HOST ?? "localhost";
const PG_PORT = Number(process.env.PG_REDIS_PORT ?? 6379);
const PG_PASSWORD = process.env.PG_REDIS_PASSWORD ?? "testpass";
const REDIS_HOST = process.env.REDIS_REF_HOST ?? "localhost";
const REDIS_PORT = Number(process.env.REDIS_REF_PORT ?? 6380);
const REDIS_PASSWORD = process.env.REDIS_REF_PASSWORD ?? "";

const BASELINE = new URL("./parity-baseline.json", import.meta.url).pathname;
const updating = process.argv.includes("--update-baseline");

type Cmd = Array<string>;
type Case = { name: string; cmds: Cmd[] };

/**
 * Replies Redis leaves unordered: comparing them verbatim measures two
 * hash-table iteration orders, not two behaviours.
 */
const UNORDERED = new Set([
	"SMEMBERS",
	"SUNION",
	"SINTER",
	"SDIFF",
	"HGETALL",
	"HKEYS",
	"HVALS",
	"KEYS",
	"ZRANDMEMBER",
	"SRANDMEMBER",
	"SPOP",
]);

/** Draws, clocks and builds: only the reply shape is comparable. `TIME` is
 * two clocks, and `DUMP` two valid spellings of one value — a hashtable's
 * iteration order alone makes byte identity impossible. */
const SHAPE_ONLY = new Set([
	"RANDOMKEY",
	"INFO",
	"DBSIZE",
	"COMMAND",
	"CONFIG",
	"CLIENT",
	"SCAN",
	"HSCAN",
	"SSCAN",
	"ZSCAN",
	"DEBUG",
	"MEMORY",
	"TIME",
	"DUMP",
]);

/** RESP type name for a reply, used when only the shape is comparable. */
function shape(reply: Buffer): string {
	const kind = String.fromCharCode(reply[0]);
	if (kind === "*") {
		const head = reply.indexOf("\r\n");
		return `*${reply.subarray(1, head).toString()}`;
	}
	if (kind === "$") {
		const head = reply.indexOf("\r\n");
		const len = Number(reply.subarray(1, head).toString());
		return len < 0 ? "$nil" : "$str";
	}
	return kind;
}

/** Split a multi-bulk reply into its elements, keeping the raw bytes. */
function elements(reply: Buffer): string[] {
	const head = reply.indexOf("\r\n");
	const count = Number(reply.subarray(1, head).toString());
	const out: string[] = [];
	let cur = head + 2;
	for (let i = 0; i < count; i++) {
		const h = reply.indexOf("\r\n", cur);
		const len = Number(reply.subarray(cur + 1, h).toString());
		if (len < 0) {
			out.push("nil");
			cur = h + 2;
		} else {
			out.push(reply.subarray(h + 2, h + 2 + len).toString("latin1"));
			cur = h + 2 + len + 2;
		}
	}
	return out;
}

/**
 * A clock reading, reduced to what is stable: Redis answers -2 for a key that
 * is gone, -1 for one with no expiry, and a positive number otherwise. Comparing
 * the number itself would compare two clocks; comparing only the shape would
 * hide a missing expiry, which is a real difference.
 */
const CLOCK = new Set(["TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME"]);

/** The comparable form of one reply, given the command that produced it. */
function normalize(cmd: Cmd, reply: Buffer): string {
	const name = cmd[0].toUpperCase();
	if (SHAPE_ONLY.has(name)) return shape(reply);
	if (CLOCK.has(name) && reply[0] === 0x3a) {
		const n = Number(reply.subarray(1, reply.indexOf("\r\n")).toString());
		return n < 0 ? `:${n}` : ":positive";
	}
	// A count makes these an array, but the members are still a sample.
	if ((name === "SPOP" || name === "SRANDMEMBER") && cmd.length > 2) {
		return shape(reply);
	}
	if (UNORDERED.has(name) && reply[0] === 0x2a) {
		return `*[${elements(reply).sort().join("|")}]`;
	}
	return reply.toString("latin1").replace(/\r\n/g, "\\n");
}

/**
 * How two replies differ, or null when they agree. Wording that differs is a
 * message to align; an error only one side raises is a behaviour.
 */
function classify(mine: string, theirs: string): string | null {
	if (mine === theirs) return null;
	const mineErr = mine.startsWith("-");
	const theirsErr = theirs.startsWith("-");
	if (mineErr && theirsErr) {
		const code = (s: string) => s.slice(1).split(" ")[0];
		return code(mine) === code(theirs) ? "error-text" : "error-code";
	}
	if (mineErr !== theirsErr) return "error-vs-value";
	return "value";
}

const cases: Case[] = [];
const c = (name: string, ...cmds: Cmd[]) => cases.push({ name, cmds });

// ─────────────────────────────── Strings ────────────────────────────────────

c("SET and GET", ["SET", "k", "v"], ["GET", "k"], ["GET", "missing"]);
c(
	"SET flags",
	["SET", "k", "v1"],
	["SET", "k", "v2", "NX"],
	["SET", "k", "v3", "XX"],
	["GET", "k"],
	["SET", "fresh", "v", "XX"],
	["SET", "fresh", "v", "NX"],
	["SET", "k", "v4", "GET"],
	["SET", "k", "v5", "EX", "100"],
	["SET", "k", "v6", "KEEPTTL"],
	["SET", "k", "v7", "EX", "0"],
	["SET", "k", "v8", "EX", "-1"],
	["SET", "k", "v9", "EX", "notanumber"],
);
c(
	"APPEND, STRLEN, GETSET, GETDEL",
	["APPEND", "s", "ab"],
	["APPEND", "s", "cd"],
	["GET", "s"],
	["STRLEN", "s"],
	["STRLEN", "missing"],
	["GETSET", "s", "new"],
	["GETSET", "fresh", "v"],
	["GETDEL", "s"],
	["GET", "s"],
	["GETDEL", "missing"],
);
c(
	"SETNX, MSET, MSETNX, MGET",
	["SETNX", "a", "1"],
	["SETNX", "a", "2"],
	["MSET", "b", "1", "c", "2"],
	["MSETNX", "c", "9", "d", "9"],
	["MSETNX", "e", "1", "f", "2"],
	["MGET", "a", "b", "c", "d", "e", "missing"],
);
c(
	"counters",
	["INCR", "n"],
	["INCRBY", "n", "5"],
	["DECR", "n"],
	["DECRBY", "n", "3"],
	["GET", "n"],
	["INCRBYFLOAT", "n", "1.5"],
	["INCRBYFLOAT", "n", "-0.5"],
	["SET", "word", "abc"],
	["INCR", "word"],
	["INCRBY", "word", "1"],
	["INCRBYFLOAT", "word", "1"],
	["SET", "big", "9223372036854775807"],
	["INCR", "big"],
	["SET", "float", "1.5"],
	["INCR", "float"],
	["INCRBYFLOAT", "float", "1.5"],
	["INCRBY", "n", "notanumber"],
);
c(
	"integer-shaped strings keep their text",
	["SET", "z", "007"],
	["GET", "z"],
	["INCR", "z"],
	["GET", "z"],
	["SET", "sp", " 1"],
	["INCR", "sp"],
);

// ──────────────────────────────── Expiry ────────────────────────────────────

c(
	"TTL and PERSIST",
	["SET", "k", "v"],
	["TTL", "k"],
	["TTL", "missing"],
	["PTTL", "missing"],
	["EXPIRE", "k", "100"],
	["PERSIST", "k"],
	["PERSIST", "k"],
	["TTL", "k"],
	["EXPIRE", "missing", "100"],
	["PERSIST", "missing"],
);
c(
	"EXPIRE with a past deadline deletes",
	["SET", "k", "v"],
	["EXPIRE", "k", "0"],
	["EXISTS", "k"],
	["SET", "k2", "v"],
	["EXPIRE", "k2", "-1"],
	["EXISTS", "k2"],
	["SET", "k3", "v"],
	["EXPIREAT", "k3", "1"],
	["EXISTS", "k3"],
	["SET", "k4", "v"],
	["PEXPIREAT", "k4", "1"],
	["EXISTS", "k4"],
);

// ───────────────────────────── Keyspace ─────────────────────────────────────

c(
	"DEL, EXISTS, TYPE, RENAME",
	["MSET", "a", "1", "b", "2"],
	["EXISTS", "a", "b", "missing"],
	["EXISTS", "a", "a"],
	["TYPE", "a"],
	["TYPE", "missing"],
	["DEL", "a", "missing"],
	["UNLINK", "b"],
	["RENAME", "missing", "other"],
	["SET", "r1", "v"],
	["RENAME", "r1", "r2"],
	["GET", "r2"],
	["EXISTS", "r1"],
);
c(
	"type errors",
	["LPUSH", "list", "a"],
	["GET", "list"],
	["INCR", "list"],
	["APPEND", "list", "x"],
	["STRLEN", "list"],
	["HGET", "list", "f"],
	["SADD", "list", "m"],
	["ZADD", "list", "1", "m"],
	["SET", "str", "v"],
	["LPUSH", "str", "a"],
	["LRANGE", "str", "0", "-1"],
	["SMEMBERS", "str"],
	["HGETALL", "str"],
	["ZRANGE", "str", "0", "-1"],
	["SCARD", "str"],
	["LLEN", "str"],
);
c(
	"arity",
	["GET"],
	["SET", "k"],
	["MSET", "k"],
	["EXPIRE", "k"],
	["LPUSH", "k"],
	["HSET", "k", "f"],
	["ZADD", "k", "1"],
	["SETRANGE", "k"],
);

// ───────────────────────────────── Hash ─────────────────────────────────────

c(
	"hash basics",
	["HSET", "h", "f1", "v1"],
	["HSET", "h", "f1", "v2"],
	["HSET", "h", "f2", "v2", "f3", "v3"],
	["HGET", "h", "f1"],
	["HGET", "h", "missing"],
	["HGET", "missing", "f"],
	["HLEN", "h"],
	["HLEN", "missing"],
	["HDEL", "h", "f1", "missing"],
	["HDEL", "missing", "f"],
	["HEXISTS", "h", "f2"],
	["HEXISTS", "h", "f1"],
	["HMGET", "h", "f2", "missing"],
	["HMGET", "missing", "f"],
	["HGETALL", "h"],
	["HGETALL", "missing"],
	["HKEYS", "missing"],
	["HVALS", "missing"],
);
c(
	"HSETNX and HINCRBY",
	["HSETNX", "h", "f", "1"],
	["HSETNX", "h", "f", "2"],
	["HGET", "h", "f"],
	["HINCRBY", "h", "n", "5"],
	["HINCRBY", "h", "n", "-2"],
	["HINCRBY", "h", "f", "1"],
	["HSET", "h", "float", "1.5"],
	["HINCRBY", "h", "float", "1"],
	["HINCRBYFLOAT", "h", "n", "0.5"],
	["HMSET", "h2", "a", "1", "b", "2"],
	["HGET", "h2", "b"],
);

// ───────────────────────────────── List ─────────────────────────────────────

c(
	"list push and pop",
	["RPUSH", "l", "a", "b", "c"],
	["LPUSH", "l", "z"],
	["LRANGE", "l", "0", "-1"],
	["LLEN", "l"],
	["LPOP", "l"],
	["RPOP", "l"],
	["LRANGE", "l", "0", "-1"],
	["LPOP", "missing"],
	["RPOP", "missing"],
	["LLEN", "missing"],
	["LRANGE", "missing", "0", "-1"],
	["LPUSHX", "missing", "v"],
	["RPUSHX", "missing", "v"],
	["LPUSHX", "l", "v"],
);
c(
	"list ranges and index",
	["RPUSH", "l", "a", "b", "c", "d", "e"],
	["LRANGE", "l", "1", "3"],
	["LRANGE", "l", "-3", "-1"],
	["LRANGE", "l", "-100", "100"],
	["LRANGE", "l", "3", "1"],
	["LRANGE", "l", "10", "20"],
	["LINDEX", "l", "0"],
	["LINDEX", "l", "-1"],
	["LINDEX", "l", "99"],
	["LINDEX", "missing", "0"],
	["LSET", "l", "0", "A"],
	["LSET", "l", "99", "x"],
	["LSET", "missing", "0", "x"],
	["LRANGE", "l", "0", "-1"],
);
c(
	"LREM, LINSERT, LTRIM, LPOS",
	["RPUSH", "l", "a", "b", "a", "c", "a"],
	["LREM", "l", "2", "a"],
	["LRANGE", "l", "0", "-1"],
	["LREM", "l", "-1", "a"],
	["LREM", "l", "0", "missing"],
	["LINSERT", "l", "BEFORE", "b", "x"],
	["LINSERT", "l", "AFTER", "b", "y"],
	["LINSERT", "l", "BEFORE", "nope", "z"],
	["LINSERT", "missing", "BEFORE", "a", "z"],
	["LRANGE", "l", "0", "-1"],
	["LPOS", "l", "b"],
	["LPOS", "l", "nope"],
	["LTRIM", "l", "0", "1"],
	["LRANGE", "l", "0", "-1"],
	["LTRIM", "l", "5", "10"],
	["EXISTS", "l"],
);
c(
	"LMOVE and RPOPLPUSH",
	["RPUSH", "src", "a", "b", "c"],
	["LMOVE", "src", "dst", "LEFT", "RIGHT"],
	["LMOVE", "src", "dst", "RIGHT", "LEFT"],
	["LRANGE", "src", "0", "-1"],
	["LRANGE", "dst", "0", "-1"],
	["RPOPLPUSH", "src", "dst"],
	["LRANGE", "dst", "0", "-1"],
	["LMOVE", "missing", "dst", "LEFT", "RIGHT"],
	["RPOPLPUSH", "missing", "dst"],
);
c(
	"pop with count",
	["RPUSH", "l", "a", "b", "c"],
	["LPOP", "l", "2"],
	["LPOP", "l", "5"],
	["LPOP", "l", "1"],
	["RPUSH", "l2", "a", "b", "c"],
	["RPOP", "l2", "2"],
	["LPOP", "missing", "2"],
);

// ────────────────────────────────── Set ─────────────────────────────────────

c(
	"set basics",
	["SADD", "s", "a", "b", "c"],
	["SADD", "s", "a"],
	["SCARD", "s"],
	["SCARD", "missing"],
	["SISMEMBER", "s", "a"],
	["SISMEMBER", "s", "nope"],
	["SISMEMBER", "missing", "a"],
	["SMISMEMBER", "s", "a", "nope"],
	["SREM", "s", "a", "nope"],
	["SREM", "missing", "a"],
	["SMEMBERS", "s"],
	["SMEMBERS", "missing"],
);
c(
	"set algebra",
	["SADD", "s1", "a", "b", "c"],
	["SADD", "s2", "b", "c", "d"],
	["SUNION", "s1", "s2"],
	["SINTER", "s1", "s2"],
	["SDIFF", "s1", "s2"],
	["SDIFF", "s2", "s1"],
	["SUNION", "s1", "missing"],
	["SINTER", "s1", "missing"],
	["SDIFF", "missing", "s1"],
	["SUNIONSTORE", "d1", "s1", "s2"],
	["SCARD", "d1"],
	["SINTERSTORE", "d2", "s1", "s2"],
	["SCARD", "d2"],
	["SDIFFSTORE", "d3", "s1", "s2"],
	["SCARD", "d3"],
	["SINTERSTORE", "d4", "s1", "missing"],
	["EXISTS", "d4"],
);
c(
	"SMOVE",
	["SADD", "s1", "a", "b"],
	["SADD", "s2", "c"],
	["SMOVE", "s1", "s2", "a"],
	["SMOVE", "s1", "s2", "nope"],
	["SMOVE", "missing", "s2", "a"],
	["SCARD", "s1"],
	["SCARD", "s2"],
	["SMEMBERS", "s2"],
	["SMOVE", "s2", "s2", "c"],
	["SMEMBERS", "s2"],
);

// ─────────────────────────────── Sorted set ─────────────────────────────────

c(
	"zset basics",
	["ZADD", "z", "1", "a", "2", "b", "3", "c"],
	["ZADD", "z", "9", "a"],
	["ZCARD", "z"],
	["ZCARD", "missing"],
	["ZSCORE", "z", "b"],
	["ZSCORE", "z", "nope"],
	["ZSCORE", "missing", "b"],
	["ZMSCORE", "z", "b", "nope"],
	["ZRANGE", "z", "0", "-1"],
	["ZRANGE", "z", "0", "-1", "WITHSCORES"],
	["ZRANGE", "missing", "0", "-1"],
	["ZREVRANGE", "z", "0", "-1"],
	["ZRANK", "z", "b"],
	["ZRANK", "z", "nope"],
	["ZREVRANK", "z", "b"],
	["ZREM", "z", "b", "nope"],
	["ZREM", "missing", "b"],
);
c(
	"ZADD flags",
	["ZADD", "z", "1", "a"],
	["ZADD", "z", "NX", "5", "a"],
	["ZSCORE", "z", "a"],
	["ZADD", "z", "XX", "5", "a"],
	["ZADD", "z", "XX", "1", "fresh"],
	["ZADD", "z", "GT", "1", "a"],
	["ZADD", "z", "GT", "9", "a"],
	["ZADD", "z", "LT", "10", "a"],
	["ZADD", "z", "CH", "1", "a", "2", "new"],
	["ZADD", "z", "INCR", "5", "a"],
	["ZADD", "z", "NX", "INCR", "5", "a"],
	["ZADD", "z", "XX", "INCR", "5", "absent"],
	["ZADD", "z", "NX", "XX", "1", "a"],
	["ZADD", "z", "GT", "LT", "1", "a"],
	["ZADD", "z", "notanumber", "a"],
);
c(
	"zset scores at the edges",
	["ZADD", "z", "inf", "a", "-inf", "b", "1.5", "c"],
	["ZSCORE", "z", "a"],
	["ZSCORE", "z", "b"],
	["ZSCORE", "z", "c"],
	["ZINCRBY", "z", "1", "c"],
	["ZINCRBY", "z", "-inf", "a"],
	["ZINCRBY", "z", "1", "fresh"],
	["ZRANGE", "z", "0", "-1", "WITHSCORES"],
	["ZADD", "z", "1e400", "huge"],
);
c(
	"ZRANGEBYSCORE and ZCOUNT",
	["ZADD", "z", "1", "a", "2", "b", "3", "c"],
	["ZRANGEBYSCORE", "z", "1", "2"],
	["ZRANGEBYSCORE", "z", "(1", "2"],
	["ZRANGEBYSCORE", "z", "-inf", "+inf"],
	["ZRANGEBYSCORE", "z", "+inf", "-inf"],
	["ZRANGEBYSCORE", "z", "1", "3", "LIMIT", "1", "1"],
	["ZRANGEBYSCORE", "z", "1", "3", "WITHSCORES"],
	["ZRANGEBYSCORE", "z", "bad", "3"],
	["ZCOUNT", "z", "1", "2"],
	["ZCOUNT", "z", "(1", "+inf"],
	["ZCOUNT", "missing", "1", "2"],
	["ZREVRANGEBYSCORE", "z", "3", "1"],
);
c(
	"ZRANGEBYLEX",
	["ZADD", "z", "0", "a", "0", "b", "0", "c"],
	["ZRANGEBYLEX", "z", "-", "+"],
	["ZRANGEBYLEX", "z", "[a", "[b"],
	["ZRANGEBYLEX", "z", "(a", "+"],
	["ZRANGEBYLEX", "z", "+", "-"],
	["ZRANGEBYLEX", "z", "a", "b"],
	["ZLEXCOUNT", "z", "-", "+"],
	["ZREVRANGEBYLEX", "z", "+", "-"],
);
c(
	"ZPOPMIN and ZPOPMAX",
	["ZADD", "z", "1", "a", "2", "b", "3", "c"],
	["ZPOPMIN", "z"],
	["ZPOPMAX", "z"],
	["ZPOPMIN", "z", "5"],
	["ZPOPMIN", "z"],
	["ZPOPMIN", "missing"],
	["ZPOPMAX", "missing"],
	["ZADD", "tie", "1", "b", "1", "a"],
	["ZPOPMIN", "tie"],
	["ZPOPMAX", "tie"],
);
c(
	"zset range removal",
	["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"],
	["ZREMRANGEBYRANK", "z", "0", "0"],
	["ZRANGE", "z", "0", "-1"],
	["ZREMRANGEBYSCORE", "z", "3", "3"],
	["ZRANGE", "z", "0", "-1"],
	["ZREMRANGEBYRANK", "missing", "0", "0"],
	["ZADD", "lex", "0", "a", "0", "b"],
	["ZREMRANGEBYLEX", "lex", "[a", "[a"],
	["ZRANGE", "lex", "0", "-1"],
);
c(
	"zset store commands",
	["ZADD", "z1", "1", "a", "2", "b"],
	["ZADD", "z2", "3", "b", "4", "c"],
	["ZUNIONSTORE", "u", "2", "z1", "z2"],
	["ZRANGE", "u", "0", "-1", "WITHSCORES"],
	["ZINTERSTORE", "i", "2", "z1", "z2"],
	["ZRANGE", "i", "0", "-1", "WITHSCORES"],
	["ZDIFFSTORE", "d", "2", "z1", "z2"],
	["ZRANGE", "d", "0", "-1", "WITHSCORES"],
	["ZUNIONSTORE", "w", "2", "z1", "z2", "WEIGHTS", "2", "3"],
	["ZRANGE", "w", "0", "-1", "WITHSCORES"],
	["ZUNIONSTORE", "m", "2", "z1", "z2", "AGGREGATE", "MIN"],
	["ZRANGE", "m", "0", "-1", "WITHSCORES"],
	["ZUNION", "2", "z1", "z2"],
	["ZINTER", "2", "z1", "z2"],
	["ZDIFF", "2", "z1", "z2"],
);

// ─────────────────────────── Server and connection ──────────────────────────

c("PING and ECHO", ["PING"], ["PING", "hello"], ["ECHO", "hi"], ["ECHO"]);
c("unknown command", ["NOSUCHCOMMAND"], ["NOSUCHCOMMAND", "arg"]);

// ───────────────────── Corner cases the community trips on ──────────────────

c(
	"an empty collection does not exist",
	["SADD", "s", "m"],
	["SREM", "s", "m"],
	["EXISTS", "s"],
	["TYPE", "s"],
	["RPUSH", "l", "v"],
	["LPOP", "l"],
	["EXISTS", "l"],
	["HSET", "h", "f", "v"],
	["HDEL", "h", "f"],
	["EXISTS", "h"],
	["ZADD", "z", "1", "m"],
	["ZREM", "z", "m"],
	["EXISTS", "z"],
	["RPUSH", "l2", "a", "b"],
	["LTRIM", "l2", "5", "10"],
	["EXISTS", "l2"],
	["ZADD", "z2", "1", "a"],
	["ZREMRANGEBYRANK", "z2", "0", "-1"],
	["EXISTS", "z2"],
);
c(
	"a store command with an empty result deletes its destination",
	["SADD", "dst", "leftover"],
	["SADD", "s1", "a"],
	["SADD", "s2", "b"],
	["SINTERSTORE", "dst", "s1", "s2"],
	["EXISTS", "dst"],
	["ZADD", "zdst", "1", "leftover"],
	["ZADD", "z1", "1", "a"],
	["ZADD", "z2", "1", "b"],
	["ZINTERSTORE", "zdst", "2", "z1", "z2"],
	["EXISTS", "zdst"],
);
c(
	"duplicate arguments in one command",
	["SADD", "s", "a", "a", "b"],
	["SCARD", "s"],
	["ZADD", "z", "1", "a", "2", "a"],
	["ZSCORE", "z", "a"],
	["HSET", "h", "f", "v1", "f", "v2"],
	["HGET", "h", "f"],
	["MSET", "k", "v1", "k", "v2"],
	["GET", "k"],
	["DEL", "k", "k"],
	["SREM", "s", "a", "a"],
	["SCARD", "s"],
);
c(
	"source and destination are the same key",
	["SET", "k", "v"],
	["RENAME", "k", "k"],
	["GET", "k"],
	["SADD", "s", "m"],
	["SMOVE", "s", "s", "m"],
	["SCARD", "s"],
	["RPUSH", "l", "a", "b", "c"],
	["LMOVE", "l", "l", "LEFT", "RIGHT"],
	["LRANGE", "l", "0", "-1"],
	["LMOVE", "l", "l", "RIGHT", "LEFT"],
	["LRANGE", "l", "0", "-1"],
	["ZADD", "z", "1", "a"],
	["ZUNIONSTORE", "z", "1", "z"],
	["ZRANGE", "z", "0", "-1", "WITHSCORES"],
);
c(
	"how a score is printed back",
	["ZADD", "z", "1.0", "a"],
	["ZSCORE", "z", "a"],
	["ZADD", "z", "3.0e3", "b"],
	["ZSCORE", "z", "b"],
	["ZADD", "z", "-0", "c"],
	["ZSCORE", "z", "c"],
	["ZADD", "z", "0.1", "d"],
	["ZINCRBY", "z", "0.2", "d"],
	["ZADD", "z", "3.0000000000000002", "e"],
	["ZSCORE", "z", "e"],
	["ZADD", "z", "17179869184", "f"],
	["ZSCORE", "z", "f"],
	["ZADD", "z", "nan", "bad"],
);
c(
	"how a float counter is printed back",
	["SET", "n", "10.5"],
	["INCRBYFLOAT", "n", "0.1"],
	["SET", "n2", "3"],
	["INCRBYFLOAT", "n2", "1.000000000000000005"],
	["SET", "n3", "5.0e3"],
	["INCRBYFLOAT", "n3", "200"],
	["SET", "n4", "1"],
	["INCRBYFLOAT", "n4", "-1"],
	["INCRBYFLOAT", "n4", "inf"],
	["INCRBYFLOAT", "n4", "nan"],
);
c(
	"what counts as an integer",
	["SET", "a", ""],
	["INCR", "a"],
	["SET", "b", " 1"],
	["INCR", "b"],
	["SET", "c", "1 "],
	["INCR", "c"],
	["SET", "d", "+1"],
	["INCR", "d"],
	["SET", "e", "1e2"],
	["INCR", "e"],
	["SET", "f", "0x10"],
	["INCR", "f"],
	["SET", "n", "-0"],
	["INCR", "n"],
	["SET", "big", "99999999999999999999999999"],
	["INCR", "big"],
);
c(
	"the expiry an operation keeps or clears",
	["SET", "k", "v", "EX", "100"],
	["APPEND", "k", "x"],
	["TTL", "k"],
	["SET", "n", "1", "EX", "100"],
	["INCR", "n"],
	["TTL", "n"],
	["SET", "g", "v", "EX", "100"],
	["GETSET", "g", "v2"],
	["TTL", "g"],
	["SET", "o", "v", "EX", "100"],
	["SET", "o", "v2"],
	["TTL", "o"],
	["RPUSH", "l", "a"],
	["EXPIRE", "l", "100"],
	["RPUSH", "l", "b"],
	["TTL", "l"],
	["SET", "r", "v", "EX", "100"],
	["RENAME", "r", "r2"],
	["TTL", "r2"],
);
c(
	"expiry arguments that make no sense",
	["SETEX", "k", "0", "v"],
	["SETEX", "k", "-1", "v"],
	["PSETEX", "k", "0", "v"],
	["EXISTS", "k"],
	["SET", "k", "v"],
	["EXPIRE", "k", "9999999999999999"],
	["PEXPIRE", "k", "99999999999999999999"],
	["EXPIRE", "k", "notanumber"],
	["SET", "past", "v", "EXAT", "1"],
	["EXISTS", "past"],
	["TTL", "k"],
);
c(
	"LPOS options",
	["RPUSH", "l", "a", "b", "c", "b", "a"],
	["LPOS", "l", "b"],
	["LPOS", "l", "b", "RANK", "2"],
	["LPOS", "l", "b", "RANK", "-1"],
	["LPOS", "l", "b", "RANK", "0"],
	["LPOS", "l", "b", "COUNT", "0"],
	["LPOS", "l", "b", "COUNT", "1"],
	["LPOS", "l", "nope", "COUNT", "0"],
	["LPOS", "missing", "b"],
);
c(
	"counts that are zero or negative",
	["SADD", "s", "a", "b", "c"],
	["SPOP", "s", "0"],
	["SPOP", "s", "-1"],
	["SRANDMEMBER", "s", "0"],
	["SCARD", "s"],
	["RPUSH", "l", "a", "b"],
	["LPOP", "l", "0"],
	["LPOP", "l", "-1"],
	["ZADD", "z", "1", "a"],
	["ZPOPMIN", "z", "0"],
	["ZPOPMIN", "z", "-1"],
	["ZRANDMEMBER", "z", "0"],
);
c(
	"commands and keywords are case-insensitive",
	["set", "k", "v"],
	["GeT", "k"],
	["set", "k2", "v", "ex", "100"],
	["ttl", "k2"],
	["set", "k2", "v2", "Nx"],
	["zadd", "z", "ch", "1", "m"],
	["zrange", "z", "0", "-1", "withscores"],
);
c(
	"KEYS patterns",
	["MSET", "ka", "1", "kb", "2", "kc", "3", "kz", "4"],
	["KEYS", "k?"],
	["KEYS", "k[a-c]"],
	["KEYS", "k[^a]"],
	["KEYS", "ka*"],
	["KEYS", "*"],
	["KEYS", "nomatch*"],
);
c(
	"ranges past the ends",
	["RPUSH", "l", "a", "b", "c"],
	["LRANGE", "l", "-100", "-1"],
	["LRANGE", "l", "0", "100"],
	["LRANGE", "l", "-1", "-100"],
	["LINDEX", "l", "-100"],
	["ZADD", "z", "1", "a", "2", "b"],
	["ZRANGE", "z", "-100", "100"],
	["ZRANGE", "z", "5", "10"],
	["ZREMRANGEBYRANK", "z", "-100", "-100"],
	["ZRANGE", "z", "0", "-1"],
);
c(
	"lexical ranges",
	["ZADD", "z", "0", "a", "0", "b", "0", "c"],
	["ZRANGEBYLEX", "z", "[", "+"],
	["ZRANGEBYLEX", "z", "(", "+"],
	["ZRANGEBYLEX", "z", "-", "-"],
	["ZRANGEBYLEX", "z", "+", "+"],
	["ZRANGEBYLEX", "z", "[c", "[a"],
	["ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "1"],
	["ZLEXCOUNT", "z", "[a", "(c"],
);
c(
	"ZADD flag combinations",
	["ZADD", "z", "GT", "NX", "1", "a"],
	["ZADD", "z", "LT", "NX", "1", "a"],
	["ZADD", "z", "CH", "INCR", "1", "a"],
	["ZADD", "z", "INCR", "1", "a", "2", "b"],
	["ZADD", "z", "1", "a", "notanumber", "b"],
	["ZSCORE", "z", "a"],
	["ZSCORE", "z", "b"],
);
c(
	"an empty key name is a key",
	["SET", "", "v"],
	["GET", ""],
	["EXISTS", ""],
	["STRLEN", ""],
	["SET", "empty", ""],
	["GET", "empty"],
	["STRLEN", "empty"],
	["APPEND", "empty", ""],
	["EXISTS", "empty"],
	["DEL", ""],
);
c(
	"transactions",
	["MULTI"],
	["SET", "k", "v"],
	["INCR", "k"],
	["GET", "k"],
	["EXEC"],
	["GET", "k"],
	["MULTI"],
	["NOSUCHCOMMAND"],
	["EXEC"],
	["EXEC"],
	["DISCARD"],
	["MULTI"],
	["MULTI"],
	["DISCARD"],
);
c("SELECT out of range", ["SELECT", "99"], ["SELECT", "-1"], ["SELECT", "abc"]);

// ─────────────────── Patterns people actually deploy ────────────────────────

c(
	"rate limiter: INCR then EXPIRE on the first hit",
	["INCR", "rate:user1"],
	["EXPIRE", "rate:user1", "60"],
	["INCR", "rate:user1"],
	["TTL", "rate:user1"],
	["GET", "rate:user1"],
	["INCRBY", "rate:user1", "10"],
	["GET", "rate:user1"],
);
c(
	"session store: SET with a TTL, read, refresh",
	["SET", "sess:abc", "user=1", "EX", "3600"],
	["GET", "sess:abc"],
	["EXPIRE", "sess:abc", "7200"],
	["PERSIST", "sess:abc"],
	["TTL", "sess:abc"],
	["SET", "sess:abc", "user=2", "KEEPTTL"],
	["GET", "sess:abc"],
	["DEL", "sess:abc"],
	["GET", "sess:abc"],
);
c(
	"distributed lock: SET NX PX, then release",
	["SET", "lock:job", "token1", "NX", "PX", "30000"],
	["SET", "lock:job", "token2", "NX", "PX", "30000"],
	["GET", "lock:job"],
	["DEL", "lock:job"],
	["SET", "lock:job", "token2", "NX", "PX", "30000"],
	["GET", "lock:job"],
	["SET", "lock:job", "token3", "XX", "PX", "60000"],
	["GET", "lock:job"],
);
c(
	"leaderboard",
	["ZADD", "board", "10", "alice", "30", "bob", "20", "carol"],
	["ZINCRBY", "board", "5", "alice"],
	["ZREVRANGE", "board", "0", "2", "WITHSCORES"],
	["ZREVRANK", "board", "bob"],
	["ZRANK", "board", "bob"],
	["ZSCORE", "board", "alice"],
	["ZCOUNT", "board", "15", "+inf"],
	["ZRANGEBYSCORE", "board", "-inf", "+inf", "LIMIT", "0", "2"],
	["ZREVRANGEBYSCORE", "board", "+inf", "-inf", "LIMIT", "0", "1"],
	["ZCARD", "board"],
	["ZREM", "board", "carol"],
	["ZCARD", "board"],
);
c(
	"work queue: LPUSH producer, RPOP consumer",
	["LPUSH", "queue", "job1"],
	["LPUSH", "queue", "job2", "job3"],
	["LLEN", "queue"],
	["RPOP", "queue"],
	["RPOP", "queue", "2"],
	["RPOP", "queue"],
	["EXISTS", "queue"],
	["LPUSHX", "queue", "job4"],
	["EXISTS", "queue"],
);
c(
	"deduplication and membership",
	["SADD", "seen", "id1"],
	["SADD", "seen", "id1"],
	["SADD", "seen", "id2", "id3"],
	["SCARD", "seen"],
	["SISMEMBER", "seen", "id1"],
	["SMISMEMBER", "seen", "id1", "id9"],
	["SREM", "seen", "id1"],
	["SISMEMBER", "seen", "id1"],
);
c(
	"cache aside",
	["GET", "cache:1"],
	["SET", "cache:1", "value", "EX", "300"],
	["GET", "cache:1"],
	["TTL", "cache:1"],
	["SET", "cache:1", "value2", "XX", "EX", "600"],
	["GET", "cache:1"],
	["DEL", "cache:1"],
	["GET", "cache:1"],
	["SET", "cache:2", "v", "NX", "EX", "300"],
	["SET", "cache:2", "v2", "NX", "EX", "300"],
	["GET", "cache:2"],
);
c(
	"tag intersection with a stored result",
	["SADD", "tag:red", "a", "b", "c"],
	["SADD", "tag:big", "b", "c", "d"],
	["SINTERSTORE", "tag:result", "tag:red", "tag:big"],
	["SMEMBERS", "tag:result"],
	["EXPIRE", "tag:result", "60"],
	["TTL", "tag:result"],
	["SUNIONSTORE", "tag:result", "tag:red", "tag:big"],
	["SCARD", "tag:result"],
	["TTL", "tag:result"],
	["SDIFFSTORE", "tag:only", "tag:red", "tag:big"],
	["SMEMBERS", "tag:only"],
);
c(
	"a hash as an object",
	["HSET", "user:1", "name", "ada", "age", "36"],
	["HGET", "user:1", "name"],
	["HGETALL", "user:1"],
	["HINCRBY", "user:1", "age", "1"],
	["HGET", "user:1", "age"],
	["HLEN", "user:1"],
	["HDEL", "user:1", "age"],
	["HEXISTS", "user:1", "age"],
	["HKEYS", "user:1"],
	["HMGET", "user:1", "name", "age"],
	["HSETNX", "user:1", "name", "grace"],
	["HGET", "user:1", "name"],
);
c(
	"a sorted set as a time index",
	["ZADD", "events", "1000", "e1", "2000", "e2", "3000", "e3"],
	["ZRANGEBYSCORE", "events", "1500", "3000"],
	["ZRANGEBYSCORE", "events", "(1000", "+inf"],
	["ZREMRANGEBYSCORE", "events", "-inf", "1500"],
	["ZRANGE", "events", "0", "-1", "WITHSCORES"],
	["ZCOUNT", "events", "-inf", "+inf"],
);

// ─────────────────────────────── Tricky ─────────────────────────────────────

c(
	"counters at their limits",
	["SET", "hi", "9223372036854775807"],
	["INCR", "hi"],
	["GET", "hi"],
	["SET", "lo", "-9223372036854775808"],
	["DECR", "lo"],
	["GET", "lo"],
	["SET", "n", "10"],
	["INCRBY", "n", "9223372036854775807"],
	["DECRBY", "n", "-9223372036854775808"],
	["GET", "n"],
);
c(
	"overwriting across types",
	["SADD", "k", "m"],
	["DEL", "k"],
	["SET", "k", "v"],
	["GET", "k"],
	["SADD", "s1", "a"],
	["SADD", "s2", "b"],
	["SET", "dst", "iamastring"],
	["SINTERSTORE", "dst", "s1", "s1"],
	["TYPE", "dst"],
	["SMEMBERS", "dst"],
);
c(
	"RENAME onto an existing key",
	["SET", "src", "one", "EX", "100"],
	["SET", "dst", "two"],
	["RENAME", "src", "dst"],
	["GET", "dst"],
	["TTL", "dst"],
	["EXISTS", "src"],
	["RENAME", "src", "dst"],
);
c(
	"append and read back a long value",
	["SET", "k", "0123456789"],
	["APPEND", "k", "0123456789"],
	["STRLEN", "k"],
	["GET", "k"],
	["APPEND", "fresh", "start"],
	["GET", "fresh"],
	["STRLEN", "fresh"],
);
c(
	"a member that is itself a number",
	["ZADD", "z", "1", "10", "2", "9"],
	["ZRANGE", "z", "0", "-1"],
	["ZSCORE", "z", "10"],
	["SADD", "s", "10", "9"],
	["SMEMBERS", "s"],
	["HSET", "h", "1", "one"],
	["HGET", "h", "1"],
	["HKEYS", "h"],
);
c(
	"commands Redis has that pg_redis does not",
	["SETRANGE", "k", "0", "hello"],
	["GETRANGE", "k", "0", "-1"],
	["RENAMENX", "k", "k2"],
	["HRANDFIELD", "h"],
	["GETEX", "k"],
	["COPY", "k", "k3"],
	["SINTERCARD", "2", "s1", "s2"],
	["OBJECT", "ENCODING", "k"],
	["SETBIT", "b", "0", "1"],
	["BITCOUNT", "b"],
	["FLUSHDB"],
);
c(
	"TOUCH counts like EXISTS",
	["SET", "k", "v"],
	["RPUSH", "l", "a"],
	["TOUCH", "k", "l"],
	["TOUCH", "k", "k"],
	["TOUCH", "missing"],
	["TOUCH"],
);
c(
	"HSTRLEN",
	["HSET", "h", "f", "hello"],
	["HSTRLEN", "h", "f"],
	["HSTRLEN", "h", "nope"],
	["HSTRLEN", "missing", "f"],
	["SET", "k", "v"],
	["HSTRLEN", "k", "f"],
);
c(
	"ZINTERCARD",
	["ZADD", "za", "1", "m1", "2", "m2", "3", "m3"],
	["ZADD", "zb", "1", "m2", "2", "m3", "3", "m4"],
	["ZINTERCARD", "2", "za", "zb"],
	["ZINTERCARD", "2", "za", "zb", "LIMIT", "1"],
	["ZINTERCARD", "2", "za", "zb", "LIMIT", "0"],
	["ZINTERCARD", "2", "za", "missing"],
	["ZINTERCARD", "0"],
	["ZINTERCARD", "0", "za"],
	["ZINTERCARD", "-1", "za"],
	["ZINTERCARD", "x", "za"],
	["SINTERCARD", "0"],
	["SINTERCARD", "0", "za"],
	["SINTERCARD", "x", "za"],
	["ZINTERCARD", "1", "za", "LIMIT", "-1"],
	["SET", "s", "v"],
	["ZINTERCARD", "1", "s"],
);
c(
	"TIME is two parts, and takes no arguments",
	["TIME"],
	["TIME", "x"],
);
c(
	"DUMP and RESTORE refusals",
	["SET", "k", "v"],
	["DUMP", "k"],
	["DUMP", "missing"],
	["RESTORE", "fresh", "0", "garbage"],
	["RESTORE", "k", "0", "garbage"],
	["RESTORE", "fresh", "-1", "garbage"],
	["RESTORE", "fresh", "notanumber", "garbage"],
	["EXISTS", "fresh"],
);

// ────────────────────────────────── Runner ──────────────────────────────────

type Difference = {
	half: string;
	case: string;
	/** Position in the case, so a command repeated in one case stays distinct. */
	at: number;
	cmd: string;
	kind: string;
	pg_redis: string;
	redis: string;
};

const REPLY_TIMEOUT_MS = Number(process.env.PARITY_TIMEOUT_MS ?? 5000);

/**
 * A connection that survives the server dropping it: a command that kills the
 * server never replies, so `NO REPLY` is recorded and the connection remade.
 */
class Server {
	private client!: RawRedis;
	constructor(
		private readonly opts: { host: string; port: number; password: string },
		private readonly select?: string,
	) {}

	async connect() {
		this.client = await new RawRedis().connect(this.opts);
		if (this.select) await this.client.send("SELECT", this.select);
		return this;
	}

	/** The reply, or null when none arrived before `REPLY_TIMEOUT_MS`. */
	async send(...cmd: string[]): Promise<Buffer | null> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		const timeout = new Promise<null>((resolve) => {
			timer = setTimeout(() => resolve(null), REPLY_TIMEOUT_MS);
		});
		try {
			const reply = await Promise.race([this.client.send(...cmd), timeout]);
			if (reply === null) {
				try {
					this.client.close();
				} catch {}
				await this.connect();
			}
			return reply;
		} finally {
			clearTimeout(timer);
		}
	}
}

async function runCase(server: Server, kase: Case): Promise<string[]> {
	// The whole database, not the tokens this file happens to mention. Deleting
	// a derived key list meant anything another suite had written was still
	// there, and `KEYS *` reported it as a difference on work that was fine.
	await server.send("FLUSHDB");
	const out: string[] = [];
	for (const cmd of kase.cmds) {
		if (process.env.PARITY_TRACE) console.error(`  > ${cmd.join(" ")}`);
		const reply = await server.send(...cmd);
		out.push(
			reply === null ? "NO REPLY — connection dropped" : normalize(cmd, reply),
		);
	}
	return out;
}

async function compareHalf(
	half: string,
	pg: Server,
	redis: Server,
): Promise<Difference[]> {
	const found: Difference[] = [];
	for (const kase of cases) {
		if (process.env.PARITY_TRACE) console.error(`[${half}] ${kase.name}`);
		const [mine, theirs] = [
			await runCase(pg, kase),
			await runCase(redis, kase),
		];
		for (const [i, cmd] of kase.cmds.entries()) {
			const kind = classify(mine[i], theirs[i]);
			if (kind) {
				found.push({
					half,
					case: kase.name,
					at: i,
					cmd: cmd.join(" "),
					kind,
					pg_redis: mine[i],
					redis: theirs[i],
				});
			}
		}
	}
	return found;
}

const pgOpts = { host: PG_HOST, port: PG_PORT, password: PG_PASSWORD };
const redisOpts = {
	host: REDIS_HOST,
	port: REDIS_PORT,
	password: REDIS_PASSWORD,
};
const reference = await new Server(redisOpts).connect();

const differences: Difference[] = [];
for (const half of ["cache", "durable"]) {
	const pg = await new Server(pgOpts, half).connect();
	differences.push(...(await compareHalf(half, pg, reference)));
}

const key = (d: Difference) => `${d.half} | ${d.case} | ${d.at} | ${d.cmd}`;
const commands = cases.reduce((n, k) => n + k.cmds.length, 0) * 2;

if (updating) {
	const baseline = differences.map((d) => ({ ...d }));
	await Bun.write(BASELINE, `${JSON.stringify(baseline, null, "\t")}\n`);
	console.log(
		`recorded ${baseline.length} differences over ${commands} replies`,
	);
	process.exit(0);
}

const known = new Map<string, Difference>();
try {
	for (const d of await Bun.file(BASELINE).json()) known.set(key(d), d);
} catch {
	console.error(`no baseline at ${BASELINE}; run with --update-baseline`);
}

const fresh = differences.filter((d) => !known.has(key(d)));
const seen = new Set(differences.map(key));
const fixed = [...known.values()].filter((d) => !seen.has(key(d)));

const show = (title: string, ds: Difference[]) => {
	if (ds.length === 0) return;
	console.log(`\n${title}`);
	for (const d of ds) {
		console.log(`  [${d.half}] ${d.case} — ${d.cmd}   (${d.kind})`);
		console.log(`      pg_redis: ${d.pg_redis}`);
		console.log(`      redis:    ${d.redis}`);
	}
};

console.log(
	`${commands} replies compared, ${differences.length} differ ` +
		`(${known.size} known)`,
);
show("NEW differences:", fresh);
show("No longer differing — update the baseline:", fixed);
process.exit(fresh.length > 0 ? 1 : 0);
