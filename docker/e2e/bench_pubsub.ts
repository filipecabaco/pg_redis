/**
 * Pub/Sub benchmark for pg_redis vs Redis.
 *
 * Uses awaited PUBLISH to avoid overrunning the subscriber's ring buffer.
 * Measures end-to-end delivery throughput: msg/sec from publish to callback.
 *
 * Scenarios:
 *   1 pub → 1 sub            exact channel match
 *   1 pub → N subs           fan-out delivery rate (total deliveries/sec)
 *   PUBLISH (no subs)        raw slot-scan cost
 *
 * Run inside Docker: REDIS_HOST=postgres REDIS_PORT=6379 bun run bench_pubsub.ts
 */

const HOST = process.env.REDIS_HOST ?? "localhost";
const PORT = process.env.REDIS_PORT ?? "6379";
const PASS = process.env.REDIS_PASSWORD ?? "testpass";
const url = `redis://:${PASS}@${HOST}:${PORT}`;

const WARMUP = 10;
const MESSAGES = 200;
const TIMEOUT_MS = 60_000;
const DRAIN_MS = 500; // allow subscriber poll loop to drain after warmup

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

async function measure(label: string, n: number, fn: () => Promise<void>) {
	const t0 = performance.now();
	await fn();
	const ms = performance.now() - t0;
	const rps = Math.round((n / ms) * 1000);
	console.log(`  ${label.padEnd(42)} ${rps.toLocaleString().padStart(9)} msg/sec  (${Math.round(ms)} ms)`);
}

let seq = 0;

// ── 1 pub → 1 sub ────────────────────────────────────────────────────────────
async function bench1to1() {
	const pub = await connect();
	const sub = await connect();

	// warmup — await delivery then drain
	const wCh = `bench:${seq++}`;
	const { promise: wp, tick: wt } = latch(WARMUP);
	await sub.subscribe(wCh, wt);
	for (let i = 0; i < WARMUP; i++) await pub.publish(wCh, "w");
	await wp;
	await Bun.sleep(DRAIN_MS);

	// measure
	const ch = `bench:${seq++}`;
	const { promise, tick } = latch(MESSAGES);
	await sub.subscribe(ch, tick);

	await measure("1 pub → 1 sub", MESSAGES, async () => {
		for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
		await promise;
	});
}

// ── 1 pub → N subs ───────────────────────────────────────────────────────────
async function benchFanout(n: number) {
	const pub = await connect();
	const subs = await Promise.all(Array.from({ length: n }, connect));

	// warmup — await delivery then drain
	const wCh = `bench:${seq++}`;
	const { promise: wp, tick: wt } = latch(WARMUP * n);
	for (const s of subs) await s.subscribe(wCh, wt);
	for (let i = 0; i < WARMUP; i++) await pub.publish(wCh, "w");
	await wp;
	await Bun.sleep(DRAIN_MS);

	// measure
	const ch = `bench:${seq++}`;
	const { promise, tick } = latch(MESSAGES * n);
	for (const s of subs) await s.subscribe(ch, tick);

	await measure(`1 pub → ${n} subs (fan-out)`, MESSAGES * n, async () => {
		for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
		await promise;
	});
}

// ── PUBLISH with no subscribers ───────────────────────────────────────────────
async function benchPublishOnly() {
	const pub = await connect();
	const ch = `bench:${seq++}`;
	for (let i = 0; i < WARMUP; i++) await pub.publish(ch, "w");

	await measure("PUBLISH (no subscribers)", MESSAGES, async () => {
		for (let i = 0; i < MESSAGES; i++) await pub.publish(ch, "x");
	});
}

// ─────────────────────────────────────────────────────────────────────────────
console.log(`\nPub/Sub benchmark  →  ${HOST}:${PORT}  (${MESSAGES.toLocaleString()} messages)`);
console.log("─".repeat(62));

try {
	await bench1to1();
	await benchFanout(4);
	await benchFanout(16);
	await benchPublishOnly();
} catch (e) {
	console.error("\nBenchmark error:", e);
	process.exit(1);
}

console.log("");
process.exit(0);
