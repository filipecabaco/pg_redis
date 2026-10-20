// What every showcase shares: where pg_redis is, and a way to say what
// happened. A showcase is a plain app; `check` only reports, and the runner
// fails if any check did.

import pg from "pg";

export const REDIS = {
	host: process.env.REDIS_HOST ?? "localhost",
	port: Number(process.env.REDIS_PORT ?? 6379),
	password: process.env.REDIS_PASSWORD || undefined,
};

export const redisUrl = (db) =>
	`redis://${REDIS.password ? `:${encodeURIComponent(REDIS.password)}@` : ""}${REDIS.host}:${REDIS.port}/${db}`;

export const sql = () =>
	new pg.Client({
		connectionString: process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/postgres",
	});

/**
 * How much slower than native the machine is: 1 on a server, more in the
 * browser demo's emulated CPU. The bounds on a wait and the leases that must
 * outlive a slow event loop (a job's lock) scale by it, so a slow machine
 * takes longer instead of failing; intended delays and idle thresholds are
 * lower bounds that hold anywhere, and do not.
 */
export const SLOW = Number(process.env.SHOWCASE_SLOW ?? 1);
export const ms = (n) => Math.round(n * SLOW);

let failed = 0;

export function check(what, ok, detail = "") {
	console.log(`  ${ok ? "✓" : "✗"} ${what}${detail ? `  (${detail})` : ""}`);
	if (!ok) failed++;
}

export const failures = () => failed;

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Poll until `cond` holds or `ms` pass; whether it held. */
export async function until(cond, wait = 10_000, every = 50) {
	const end = Date.now() + ms(wait);
	while (Date.now() < end) {
		if (await cond()) return true;
		await sleep(every);
	}
	return false;
}
