// An Express app with its sessions and its cache in pg_redis, through the
// libraries every Express app uses: express-session with connect-redis, and
// a cache-aside layer on ioredis.
//
// Sessions go to database 8, the logged one: they outlive the app and the
// server, and they are rows SQL can read, so "who is signed in" is a query
// joined with the users table. The cache goes to database 0, in memory,
// where a TTL and eviction are what a cache wants.

import RedisStore from "connect-redis";
import express from "express";
import session from "express-session";
import IORedis from "ioredis";
import { REDIS, check, sql } from "../lib.mjs";

const USERS = [
	{ id: 1, email: "ada@example.com", password: "lovelace" },
	{ id: 2, email: "grace@example.com", password: "hopper" },
];

/** The app, as it would be deployed: nothing in it knows it is not Redis. */
function app() {
	const sessions = new IORedis({ ...REDIS, db: 8 });
	const cache = new IORedis({ ...REDIS, db: 0 });
	const a = express();
	a.use(express.json());
	a.use(
		session({
			store: new RedisStore({ client: sessions, prefix: "sess:" }),
			secret: "change-me",
			resave: false,
			saveUninitialized: false,
			cookie: { maxAge: 24 * 3600 * 1000 },
		}),
	);

	a.post("/login", (req, res) => {
		const u = USERS.find((u) => u.email === req.body.email && u.password === req.body.password);
		if (!u) return res.status(401).end();
		req.session.userId = u.id;
		res.json({ ok: true });
	});
	a.get("/me", (req, res) => {
		const u = USERS.find((u) => u.id === req.session.userId);
		return u ? res.json({ email: u.email }) : res.status(401).end();
	});
	a.post("/logout", (req, res) => req.session.destroy(() => res.json({ ok: true })));

	// Cache-aside: answer from the cache, or compute, store with a TTL, answer.
	let computed = 0;
	a.get("/products", async (_req, res) => {
		const hit = await cache.get("cache:/products");
		if (hit) return res.set("X-Cache", "HIT").type("json").send(hit);
		computed++;
		const body = JSON.stringify([{ sku: "P-1", price: 42 }, { sku: "P-2", price: 9.99 }]);
		await cache.set("cache:/products", body, "EX", 30);
		res.set("X-Cache", "MISS").type("json").send(body);
	});
	a.get("/stats", (_req, res) => res.json({ computed }));

	return { a, close: () => Promise.all([sessions.quit(), cache.quit()]) };
}

function listen(a) {
	return new Promise((resolve) => {
		const s = a.listen(0, () => resolve({ url: `http://127.0.0.1:${s.address().port}`, s }));
	});
}

export async function run() {
	const db = sql();
	await db.connect();
	await db.query("CREATE TABLE IF NOT EXISTS app_users (id int PRIMARY KEY, email text NOT NULL)");
	for (const u of USERS) {
		await db.query("INSERT INTO app_users VALUES ($1, $2) ON CONFLICT (id) DO NOTHING", [u.id, u.email]);
	}
	const flush = new IORedis({ ...REDIS, db: 0 });
	await flush.del("cache:/products");
	await flush.quit();

	let first = app();
	let { url, s } = await listen(first.a);
	const post = (path, body, cookie) =>
		fetch(url + path, {
			method: "POST",
			headers: { "content-type": "application/json", ...(cookie ? { cookie } : {}) },
			body: JSON.stringify(body ?? {}),
		});

	const login = await post("/login", { email: "ada@example.com", password: "lovelace" });
	const cookie = login.headers.get("set-cookie")?.split(";")[0];
	check("signing in sets a session cookie", login.ok && cookie?.startsWith("connect.sid="));
	const me = await (await fetch(url + "/me", { headers: { cookie } })).json();
	check("the session knows who is signed in", me.email === "ada@example.com", me.email);

	const r1 = await fetch(url + "/products");
	const r2 = await fetch(url + "/products");
	const stats = await (await fetch(url + "/stats")).json();
	check(
		"the second request is served from the cache",
		r1.headers.get("x-cache") === "MISS" && r2.headers.get("x-cache") === "HIT" && stats.computed === 1,
		`${r1.headers.get("x-cache")}, then ${r2.headers.get("x-cache")}`,
	);

	// Redeploy: a new app process, same store. The user stays signed in.
	s.close();
	await first.close();
	first = app();
	({ url, s } = await listen(first.a));
	const again = await fetch(url + "/me", { headers: { cookie } });
	check("the session outlives the app", again.ok && (await again.json()).email === "ada@example.com");

	// Who is signed in, from SQL: connect-redis stores each session as JSON.
	const { rows } = await db.query(`
		SELECT u.email, k.expires_at > now() AS live
		FROM redis.kv_8 k
		JOIN app_users u ON u.id = (convert_from(k.value, 'UTF8')::jsonb ->> 'userId')::int
		WHERE k.key LIKE 'sess:%'`);
	check(
		"SQL lists the signed-in users, joined with app_users",
		rows.some((r) => r.email === "ada@example.com" && r.live),
		rows.map((r) => r.email).join(", "),
	);

	await post("/logout", {}, cookie);
	const after = await fetch(url + "/me", { headers: { cookie } });
	const { rows: left } = await db.query(`
		SELECT count(*)::int AS n FROM redis.kv_8 k
		WHERE k.key LIKE 'sess:%'
		  AND (convert_from(k.value, 'UTF8')::jsonb ->> 'userId')::int = 1`);
	check("signing out deletes the session", after.status === 401 && left[0].n === 0);

	s.close();
	await first.close();
	await db.end();
}
