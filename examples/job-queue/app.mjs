// A background job queue on BullMQ, the most used Redis job queue for Node,
// unchanged. Jobs live on database 8, the logged one, so a queue survives a
// restart and its state can be read with SQL.
//
// On a slow machine (SHOWCASE_SLOW > 1, the browser demo) it does the same
// with less: half the signups and a shorter lock, and JOB_QUEUE_DB can put
// the queue on a memory database, where BullMQ's scripts touch no tables.
//
// What it shows: jobs spread over workers; a transient failure retried with
// exponential backoff; a delayed job; a worker that dies mid-job, whose job
// another worker takes over once its lock lapses.

import { Queue, QueueEvents, Worker } from "bullmq";
import IORedis from "ioredis";
import { REDIS, SLOW, check, ms, sleep, sql, until } from "../lib.mjs";

const DB = Number(process.env.JOB_QUEUE_DB ?? 8);
const LOGGED = DB >= 8;
const LIGHT = SLOW > 1;
const SIGNUPS = LIGHT ? 6 : 12;
// The crashed worker's job moves once its lock lapses and the stall check
// runs: the lock has to outlive a slow event loop, and no more.
const LOCK = LIGHT ? { lockDuration: 2000, stalledInterval: 1000 } : { lockDuration: ms(1000), stalledInterval: ms(500) };
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

	const signups = Array.from({ length: SIGNUPS }, (_, i) => ({
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

	check("every job completed", counts.completed === SIGNUPS + 1 && counts.failed === 0, JSON.stringify(counts));
	const retried = [...tries.entries()].filter(([, n]) => n > 1).length;
	check("the transient failures were retried, with backoff", retried === SIGNUPS / 3, `${retried} retried`);
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
		{ connection: connection(), ...LOCK },
	);
	const invoice = await crashQueue.add("invoice", { order: 1001 }, { attempts: 3 });
	await until(() => picked, 10_000);
	// Gone without a goodbye: no more lock renewals, nothing completed.
	doomed.off("error", () => {});
	await doomed.close(true).catch(() => {});
	const rescuer = new Worker(crashName, async (job) => `invoiced ${job.data.order}`, {
		connection: connection(),
		...LOCK,
	});
	const result = await invoice.waitUntilFinished(crashEvents, ms(20_000)).catch((e) => `failed: ${e.message}`);
	check("a crashed worker's job was taken over and finished", result === "invoiced 1001", String(result));

	// The queue's state from SQL: on a logged database its keys are rows,
	// and a memory database is read from shared memory.
	const from = LOGGED ? `redis.keys WHERE db = ${DB} AND` : `redis.memory_keys(${DB}) WHERE`;
	const db = sql();
	await db.connect();
	const { rows } = await db.query(
		`SELECT count(*)::int AS keys, count(*) FILTER (WHERE type = 'list')::int AS lists
		 FROM ${from} key LIKE $1`,
		[`bull:${name}:%`],
	);
	await db.end();
	check(
		`the queue's state is in database ${DB}, ${LOGGED ? "in tables" : "in memory"}, read from SQL`,
		rows[0].keys > SIGNUPS,
		`${rows[0].keys} keys, ${rows[0].lists} lists`,
	);

	await rescuer.close();
	for (const q of [queue, crashQueue]) await q.obliterate({ force: true }).catch(() => {});
	await Promise.all([queue.close(), crashQueue.close(), events.close(), crashEvents.close()]);
	await sleep(50);
}
