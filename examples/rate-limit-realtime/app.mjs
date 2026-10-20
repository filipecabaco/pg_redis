// Two instances of one service behind a load balancer, sharing what has to
// be shared through pg_redis, as a scaled-out deployment does with Redis:
//
// - an API rate limit with express-rate-limit and rate-limit-redis, counted
//   once across every instance;
// - live updates with Socket.IO and its Redis adapter, so a message sent on
//   one instance reaches the clients connected to the other.

import { createServer } from "node:http";
import { createAdapter } from "@socket.io/redis-adapter";
import express from "express";
import { rateLimit } from "express-rate-limit";
import IORedis from "ioredis";
import { RedisStore } from "rate-limit-redis";
import { Server } from "socket.io";
import { io as connect } from "socket.io-client";
import { REDIS, check, until } from "../lib.mjs";

/** One instance of the service. */
async function instance(name, prefix) {
	const limits = new IORedis({ ...REDIS, db: 0 });
	const pub = new IORedis({ ...REDIS });
	const sub = pub.duplicate();
	const app = express();
	app.use(
		"/api",
		rateLimit({
			windowMs: 60_000,
			limit: 5,
			standardHeaders: "draft-7",
			legacyHeaders: false,
			store: new RedisStore({ sendCommand: (...args) => limits.call(...args), prefix }),
		}),
	);
	app.get("/api/quote", (_req, res) => res.json({ instance: name, price: 42 }));
	const http = createServer(app);
	const io = new Server(http, { adapter: createAdapter(pub, sub) });
	io.on("connection", (socket) => {
		socket.on("watch", (order) => socket.join(`order:${order}`));
	});
	await new Promise((r) => http.listen(0, r));
	return {
		name,
		io,
		url: `http://127.0.0.1:${http.address().port}`,
		close: async () => {
			io.close();
			await Promise.all([limits.quit(), pub.quit(), sub.quit()]);
		},
	};
}

export async function run() {
	const prefix = `rl:${Date.now()}:`;
	const a = await instance("a", prefix);
	const b = await instance("b", prefix);

	// Seven requests from one client, alternating instances as a load
	// balancer would: the limit of five holds across both.
	const statuses = [];
	for (let i = 0; i < 7; i++) {
		const r = await fetch(`${(i % 2 ? b : a).url}/api/quote`);
		statuses.push(r.status);
	}
	check(
		"the limit is shared: the 6th and 7th requests are refused on either instance",
		statuses.join(",") === "200,200,200,200,200,429,429",
		statuses.join(","),
	);

	// One client on each instance watches the same order.
	const onA = connect(a.url, { transports: ["websocket"] });
	const onB = connect(b.url, { transports: ["websocket"] });
	await until(() => onA.connected && onB.connected);
	const got = { a: [], b: [] };
	onA.on("status", (m) => got.a.push(m));
	onB.on("status", (m) => got.b.push(m));
	onA.emit("watch", 1001);
	onB.emit("watch", 1001);
	await until(async () => (await a.io.in("order:1001").fetchSockets()).length === 2);
	const watching = await a.io.in("order:1001").fetchSockets();
	check("instance a sees the watchers connected to both", watching.length === 2, `${watching.length} sockets`);

	// The order ships; instance a sends the update. Both clients get it.
	a.io.to("order:1001").emit("status", { order: 1001, status: "shipped" });
	await until(() => got.a.length && got.b.length, 5_000);
	check(
		"an update sent on instance a reaches the client on instance b",
		got.b[0]?.status === "shipped" && got.a[0]?.status === "shipped",
		JSON.stringify(got.b[0]),
	);

	onA.close();
	onB.close();
	await a.close();
	await b.close();
}
