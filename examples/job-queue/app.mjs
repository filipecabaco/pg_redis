// A background job queue on BullMQ, the most used Redis job queue for Node,
// unchanged. Jobs live on database 8, the logged one, so a queue survives a
// restart and its state can be read with SQL.
//
// What it shows: jobs spread over workers; a transient failure retried with
// exponential backoff; a delayed job; a worker that dies mid-job, whose job
// another worker takes over once its lock lapses.

import { Queue, QueueEvents, Worker } from "bullmq";
import IORedis from "ioredis";
import { REDIS, check, ms, sleep, sql, until } from "../lib.mjs";

const DB = 8;
const connection = () => new IORedis({ ...REDIS, db: DB, maxRetriesPerRequest: null });

export async function run() {
	const name = `emails-${Date.now()}`;
	const queue = new Queue(name, { connection: connection() });
	queue.setMaxListeners(50);
	const events = new QueueEvents(name, { connection: connection() });
	await events.waitUntilReady();

	// A mail provider that times out on the first try of every third message.
	const tries = new Map();
	const sent = [];
	const deliver = async (job) => {
		const n = (tries.get(job.id) ?? 0) + 1;
		tries.set(job.id, n);
		if (job.data.flaky && n === 1) throw new Error("SMTP timeout");
		sent.push({ to: job.data.to, at: Date.now(), worker: job.processedBy });
		return { messageId: `msg-${job.id}` };
	};

	const workers = ["worker-a", "worker-b"].map(
		(id) => new Worker(name, deliver, { connection: connection(), concurrency: 4, name: id }),
	);

	const signups = Array.from({ length: 12 }, (_, i) => ({
		name: "welcome",
		data: { to: `user${i}@example.com`, flaky: i % 3 === 0 },
		opts: { attempts: 3, backoff: { type: "exponential", delay: 100 } },
	}));
	const jobs = await queue.addBulk(signups);
	const delayedAt = Date.now();
	const reminder = await queue.add(
		"reminder",
		{ to: "late@example.com" },
		{ delay: 1500, attempts: 1 },
	);

	await Promise.all([...jobs, reminder].map((j) => j.waitUntilFinished(events, ms(20_000))));
	const counts = await queue.getJobCounts("completed", "failed", "delayed", "waiting", "active");

	check("every job completed", counts.completed === 13 && counts.failed === 0, JSON.stringify(counts));
	const retried = [...tries.entries()].filter(([, n]) => n > 1).length;
	check("the transient failures were retried, with backoff", retried === 4, `${retried} retried`);
	const late = sent.find((s) => s.to === "late@example.com");
	check(
		"the delayed job waited for its delay",
		late && late.at - delayedAt >= 1500,
		late ? `${late.at - delayedAt} ms` : "never ran",
	);

	// A worker that dies with a job in hand: it never completes it, and its
	// lock is not renewed. BullMQ's stalled-job check gives the job to a
	// worker that is alive.
	for (const w of workers) await w.close();
	const crashName = `${name}-crash`;
	const crashQueue = new Queue(crashName, { connection: connection() });
	const crashEvents = new QueueEvents(crashName, { connection: connection() });
	await crashEvents.waitUntilReady();
	let picked = false;
	const doomed = new Worker(
		crashName,
		async () => {
			picked = true;
			await new Promise(() => {});
		},
		{ connection: connection(), lockDuration: ms(1000), stalledInterval: ms(500) },
	);
	const invoice = await crashQueue.add("invoice", { order: 1001 }, { attempts: 3 });
	await until(() => picked, 10_000);
	// Gone without a goodbye: no more lock renewals, nothing completed.
	doomed.off("error", () => {});
	await doomed.close(true).catch(() => {});
	const rescuer = new Worker(crashName, async (job) => `invoiced ${job.data.order}`, {
		connection: connection(),
		lockDuration: ms(1000),
		stalledInterval: ms(500),
	});
	const result = await invoice.waitUntilFinished(crashEvents, ms(20_000)).catch((e) => `failed: ${e.message}`);
	check("a crashed worker's job was taken over and finished", result === "invoiced 1001", String(result));

	// The queue is ordinary rows: count BullMQ's keys for it from SQL.
	const db = sql();
	await db.connect();
	const { rows } = await db.query(
		`SELECT count(*)::int AS keys FROM (
		   SELECT key FROM redis.hash_8 UNION SELECT key FROM redis.zset_8
		   UNION SELECT key FROM redis.list_8 UNION SELECT key FROM redis.kv_8
		   UNION SELECT key FROM redis.stream_8) k
		 WHERE key LIKE $1::bytea`,
		[`bull:${name}:%`],
	);
	await db.end();
	check("the queue's state is in the logged tables", rows[0].keys > 13, `${rows[0].keys} keys`);

	await rescuer.close();
	for (const q of [queue, crashQueue]) await q.obliterate({ force: true }).catch(() => {});
	await Promise.all([queue.close(), crashQueue.close(), events.close(), crashEvents.close()]);
	await sleep(50);
}
