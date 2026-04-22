/**
 * Pub/Sub benchmark for pg_redis vs Redis.
 *
 * Each scenario reports two rates:
 *   pub/s  — PUBLISH command throughput (publisher side, awaited sequentially)
 *   recv/s — delivery throughput (total messages received / total elapsed time)
 *
 * For fan-out, recv/s = pub/s × N because one PUBLISH fans to N subscribers.
 * pub/s stays flat as N grows; recv/s scales linearly.
 *
 * Scenarios (without routing):
 *   1 pub → 1, 4, 16, 32, 64 subs   fan-out at different scales
 *   PUBLISH (no subs)                raw scan cost, no delivery
 *
 * Scenarios (with routing, pg_redis only — set DATABASE_URL):
 *   PUBLISH (routing, no subs)       overhead of fire-and-forget table INSERT
 *   1 pub → 1, 4 subs (routing)      delivery + INSERT in parallel
 *
 * Run inside Docker: REDIS_HOST=postgres REDIS_PORT=6379 bun run bench_pubsub.ts
 * With routing:      DATABASE_URL=postgres://... bun run bench_pubsub.ts
 */

const HOST = process.env.REDIS_HOST ?? "localhost";
const PORT = process.env.REDIS_PORT ?? "6379";
const PASS = process.env.REDIS_PASSWORD ?? "testpass";
const DATABASE_URL = process.env.DATABASE_URL;
const url = `redis://:${PASS}@${HOST}:${PORT}`;

const WARMUP = 10;
const MESSAGES = 200;
const TIMEOUT_MS = 60_000;
const DRAIN_MS = 500;

async function connect() {
	const c = new Bun.RedisClient(url);
	await c.connect();
	return c;
}

function latch(n: number) {
	let count = 0;
	let res: () => void;
	let rej: (e: Error) => void;
	const promise = new Promise<void>((r, e) => { res = r; rej = e; });
	const tid = setTimeout(
		() => rej(new Error(`latch(${n}) timeout, got ${count}`)),
		TIMEOUT_MS,
	);
	const tick = () => { if (++count >= n) { clearTimeout(tid); res(); } };
	return { promise, tick };
}

function report(
	label: string,
	pubRps: number,
	recvRps: number | null,
	totalMs: number,
) {
	const L = label.padEnd(38);
	const S = pubRps.toLocaleString().padStart(9);
	const R = recvRps !== null ? recvRps.toLocaleString().padStart(9) : "      n/a";
	console.log(`  ${L}  ${S} pub/s  ${R} recv/s  (${Math.round(totalMs)} ms)`);
}

let seq = 0;

// ── 1 pub → 1 sub ────────────────────────────────────────────────────────────
async function bench1to1() {
	const pub = await connect();
	const sub = await connect();

	const wCh = `bench:${seq++}`;
	const { promise: wp, tick: wt } = latch(WARMUP);
	await sub.subscribe(wCh, wt);
	for (let i = 0; i < WARMUP; i++) await pub.publish(wCh, "w");
	await wp;
	await Bun.sleep(DRAIN_MS);

	const ch = `bench:${seq++}`;
	const { promise, tick } = latch(MESSAGES);
	await sub.subscribe(ch, tick);

	const t0 = performance.now();
	for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
	const pubMs = performance.now() - t0;
	await promise;
	const totalMs = performance.now() - t0;

	report(
		"1 pub → 1 sub",
		Math.round(MESSAGES / pubMs * 1000),
		Math.round(MESSAGES / totalMs * 1000),
		totalMs,
	);
}

// ── 1 pub → N subs ───────────────────────────────────────────────────────────
async function benchFanout(n: number) {
	const pub = await connect();
	const subs = await Promise.all(Array.from({ length: n }, connect));

	const wCh = `bench:${seq++}`;
	const { promise: wp, tick: wt } = latch(WARMUP * n);
	for (const s of subs) await s.subscribe(wCh, wt);
	for (let i = 0; i < WARMUP; i++) await pub.publish(wCh, "w");
	await wp;
	await Bun.sleep(DRAIN_MS);

	const ch = `bench:${seq++}`;
	const { promise, tick } = latch(MESSAGES * n);
	for (const s of subs) await s.subscribe(ch, tick);

	const t0 = performance.now();
	for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
	const pubMs = performance.now() - t0;
	await promise;
	const totalMs = performance.now() - t0;

	report(
		`1 pub → ${n} subs (fan-out)`,
		Math.round(MESSAGES / pubMs * 1000),
		Math.round(MESSAGES * n / totalMs * 1000),
		totalMs,
	);
}

// ── PUBLISH with no subscribers ───────────────────────────────────────────────
async function benchPublishOnly() {
	const pub = await connect();
	const ch = `bench:${seq++}`;
	for (let i = 0; i < WARMUP; i++) await pub.publish(ch, "w");

	const t0 = performance.now();
	for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
	const pubMs = performance.now() - t0;

	report("PUBLISH (no subscribers)", Math.round(MESSAGES / pubMs * 1000), null, pubMs);
}

// ── Routing helpers (pg_redis only) ──────────────────────────────────────────

async function withRouting(fn: (ch: string) => Promise<void>) {
	if (!DATABASE_URL) return;
	const db = new Bun.sql(DATABASE_URL);
	const ch = `bench:route:${seq++}`;
	const tbl = `bench_route_${seq}`;
	await db`SELECT redis.create_pubsub_table('public', ${tbl})`;
	await db`SELECT redis.route_publish(${ch}, 'public', ${tbl})`;
	try {
		await fn(ch);
	} finally {
		await db`SELECT redis.unroute_publish(${ch})`;
		await db`DROP TABLE IF EXISTS public.${db.unsafe(tbl)}`;
		await db.end();
	}
}

// ── PUBLISH with routing, no subscribers ─────────────────────────────────────
async function benchPublishOnlyWithRouting() {
	await withRouting(async (ch) => {
		const pub = await connect();
		for (let i = 0; i < WARMUP; i++) await pub.publish(ch, "w");
		await Bun.sleep(DRAIN_MS);

		const t0 = performance.now();
		for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
		const pubMs = performance.now() - t0;

		report("PUBLISH (routing, no subs)", Math.round(MESSAGES / pubMs * 1000), null, pubMs);
	});
}

// ── Fan-out with routing ──────────────────────────────────────────────────────
async function benchFanoutWithRouting(n: number) {
	await withRouting(async (ch) => {
		const pub = await connect();
		const subs = await Promise.all(Array.from({ length: n }, connect));

		const { promise: wp, tick: wt } = latch(WARMUP * n);
		for (const s of subs) await s.subscribe(ch, wt);
		for (let i = 0; i < WARMUP; i++) await pub.publish(ch, "w");
		await wp;
		await Bun.sleep(DRAIN_MS);

		const { promise, tick } = latch(MESSAGES * n);
		for (const s of subs) await s.subscribe(ch, tick);

		const t0 = performance.now();
		for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
		const pubMs = performance.now() - t0;
		await promise;
		const totalMs = performance.now() - t0;

		report(
			`1 pub → ${n} subs (routing)`,
			Math.round(MESSAGES / pubMs * 1000),
			Math.round(MESSAGES * n / totalMs * 1000),
			totalMs,
		);
	});
}

// ─────────────────────────────────────────────────────────────────────────────
const header = `  ${"Scenario".padEnd(38)}  ${"pub/s".padStart(9)}         ${"recv/s".padStart(9)}`;
console.log(`\nPub/Sub benchmark  →  ${HOST}:${PORT}  (${MESSAGES.toLocaleString()} messages)`);
console.log("─".repeat(header.length));
console.log(header);
console.log("─".repeat(header.length));

try {
	await bench1to1();
	await benchFanout(4);
	await benchFanout(16);
	await benchFanout(32);
	await benchFanout(64);
	await benchPublishOnly();
} catch (e) {
	console.error("\nBenchmark error:", e);
	process.exit(1);
}

if (DATABASE_URL) {
	console.log("\nWith table routing  →  persistent INSERT per PUBLISH");
	console.log("─".repeat(header.length));
	console.log(header);
	console.log("─".repeat(header.length));
	try {
		await benchPublishOnlyWithRouting();
		await benchFanoutWithRouting(1);
		await benchFanoutWithRouting(4);
	} catch (e) {
		console.error("\nRouting benchmark error:", e);
		process.exit(1);
	}
}

console.log("");
process.exit(0);
