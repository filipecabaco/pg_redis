// An order pipeline on a Redis stream with a consumer group: the checkout
// service appends orders, and fulfilment workers share them out, each order
// handled by one worker and acknowledged once done. On database 8 the
// stream, the group's progress and the running totals survive a restart.
//
// What it shows: work split across workers; a worker that dies with orders
// read and not acknowledged, recovered by XAUTOCLAIM; processing that is
// idempotent, so an order delivered twice is counted once; and the totals
// reported from SQL.

import IORedis from "ioredis";
import { REDIS, check, sleep, sql, until } from "../lib.mjs";

const DB = 8;
const REGIONS = ["eu", "us", "apac"];

/** A fulfilment worker: reads its share, does the work, acknowledges. */
async function work(redis, stream, group, consumer, { crashAfterRead = false, stop }) {
	while (!stop()) {
		const res = await redis.xreadgroup(
			"GROUP", group, consumer, "COUNT", 5, "BLOCK", 200, "STREAMS", stream, ">",
		);
		if (!res) continue;
		const [, entries] = res[0];
		// Read, then gone: the orders stay pending under this consumer's name.
		if (crashAfterRead) return entries.length;
		for (const [id, fields] of entries) await fulfil(redis, stream, group, id, fields);
	}
	return 0;
}

/** Idempotent: an order already fulfilled is only acknowledged again. */
async function fulfil(redis, stream, group, id, fields) {
	const order = Object.fromEntries(
		Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i], fields[2 * i + 1]]),
	);
	const tx = redis.multi();
	tx.sadd(`${stream}:fulfilled`, order.order_id);
	const [[, added]] = await tx.exec();
	if (added === 1) {
		await redis.hincrbyfloat(`${stream}:revenue`, order.region, order.total);
	}
	await redis.xack(stream, group, id);
}

export async function run() {
	const stream = `orders:${Date.now()}`;
	const group = "fulfilment";
	const checkout = new IORedis({ ...REDIS, db: DB });
	await checkout.xgroup("CREATE", stream, group, "$", "MKSTREAM");

	let expected = 0;
	for (let i = 1; i <= 50; i++) {
		const total = (i * 7.25) % 120;
		expected += total;
		await checkout.xadd(stream, "*", "order_id", 1000 + i, "region", REGIONS[i % 3], "total", total.toFixed(2));
	}

	// Three workers start; worker-c dies right after its first read.
	let done = false;
	const conns = ["a", "b", "c"].map(() => new IORedis({ ...REDIS, db: DB }));
	const crashed = await work(conns[2], stream, group, "worker-c", { crashAfterRead: true, stop: () => done });
	const running = [
		work(conns[0], stream, group, "worker-a", { stop: () => done }),
		work(conns[1], stream, group, "worker-b", { stop: () => done }),
	];
	// The two live workers drain the rest, everything but worker-c's five.
	await until(async () => (await conns[0].scard(`${stream}:fulfilled`)) === 45);
	// Long enough for worker-c's five to be idle past the janitor's threshold.
	await sleep(600);

	const [pendingBefore] = await checkout.xpending(stream, group);
	check(
		"worker-c's orders are left pending when it dies",
		crashed === 5 && Number(pendingBefore) === 5,
		`${crashed} read, ${pendingBefore} pending`,
	);

	// The janitor: anything pending for over half a second goes to it.
	const janitor = new IORedis({ ...REDIS, db: DB });
	const [, claimed] = await janitor.xautoclaim(stream, group, "janitor", 500, "0-0");
	for (const [id, fields] of claimed) await fulfil(janitor, stream, group, id, fields);
	check("XAUTOCLAIM hands them to a live consumer", claimed.length === 5, `${claimed.length} claimed`);

	// A redelivery after a crash (the janitor sees an order twice) is harmless.
	if (claimed[0]) await fulfil(janitor, stream, group, claimed[0][0], claimed[0][1]);

	done = true;
	await Promise.all(running);
	const [pendingAfter] = await checkout.xpending(stream, group);
	const fulfilled = await checkout.scard(`${stream}:fulfilled`);
	check("every order fulfilled once, nothing pending", fulfilled === 50 && Number(pendingAfter) === 0, `${fulfilled} fulfilled`);

	// The revenue per region, for the finance report, straight from SQL.
	const db = sql();
	await db.connect();
	const { rows } = await db.query(
		`SELECT field AS region, value::numeric AS revenue
		 FROM redis.hashes WHERE db = 8 AND key = $1 ORDER BY revenue DESC`,
		[`${stream}:revenue`],
	);
	await db.end();
	const total = rows.reduce((s, r) => s + Number(r.revenue), 0);
	check(
		"SQL reports the revenue per region, adding up to the orders",
		rows.length === 3 && Math.abs(total - expected) < 0.01,
		rows.map((r) => `${r.region} ${r.revenue}`).join(", "),
	);

	await checkout.del(stream, `${stream}:fulfilled`, `${stream}:revenue`);
	await Promise.all([checkout, janitor, ...conns].map((c) => c.quit()));
}
