/**
 * pg_redis client-compatibility e2e suite
 *
 * Verifies that popular Redis client libraries can connect to pg_redis and
 * execute common operations. Each library section is independent — a failure
 * in one does not prevent the others from running.
 *
 * Exit code: 0 if all pass, 1 if any fail.
 */

import { createClient } from "redis";
import Redis from "ioredis";

const HOST = process.env.REDIS_HOST ?? "localhost";
const PORT = Number(process.env.REDIS_PORT ?? 6379);

let pass = 0;
let fail = 0;

function ok(desc) {
  console.log(`  PASS  ${desc}`);
  pass++;
}

function ko(desc, err) {
  console.error(`  FAIL  ${desc}`);
  console.error(`        ${err?.message ?? err}`);
  fail++;
}

async function section(name, fn) {
  console.log(`\n--- ${name} ---`);
  try {
    await fn();
  } catch (err) {
    ko(`${name} — unexpected top-level error`, err);
  }
}

async function assert(desc, fn) {
  try {
    await fn();
    ok(desc);
  } catch (err) {
    ko(desc, err);
  }
}

function expect(label, actual, expected) {
  if (actual !== expected)
    throw new Error(`expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

function expectNull(label, actual) {
  if (actual !== null)
    throw new Error(`expected null, got ${JSON.stringify(actual)}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// node-redis (redis@4)
// Sends: CLIENT SETINFO lib-name, CLIENT SETINFO lib-ver (no HELLO by default)
// ─────────────────────────────────────────────────────────────────────────────
await section("node-redis (redis@4)", async () => {
  const client = createClient({ socket: { host: HOST, port: PORT } });
  client.on("error", () => {}); // suppress unhandled-error noise

  await assert("connects", () => client.connect());

  await assert("PING", async () => {
    const r = await client.ping();
    expect("PING", r, "PONG");
  });

  await assert("SET / GET roundtrip", async () => {
    await client.set("nr:key", "hello");
    const v = await client.get("nr:key");
    expect("GET", v, "hello");
  });

  await assert("SET with EX / TTL", async () => {
    await client.set("nr:ttl", "val", { EX: 100 });
    const t = await client.ttl("nr:ttl");
    if (t <= 0 || t > 100) throw new Error(`TTL out of range: ${t}`);
  });

  await assert("DEL", async () => {
    const n = await client.del("nr:key");
    expect("DEL", n, 1);
  });

  await assert("GET missing key returns null", async () => {
    const v = await client.get("nr:missing");
    expectNull("GET missing", v);
  });

  await assert("MSET / MGET", async () => {
    await client.mSet({ "nr:m1": "a", "nr:m2": "b" });
    const vals = await client.mGet(["nr:m1", "nr:m2", "nr:missing"]);
    expect("MGET[0]", vals[0], "a");
    expect("MGET[1]", vals[1], "b");
    expectNull("MGET[2]", vals[2]);
    await client.del("nr:m1", "nr:m2");
  });

  await assert("INCR / INCRBY", async () => {
    await client.set("nr:counter", "0");
    await client.incr("nr:counter");
    const v = await client.incrBy("nr:counter", 4);
    expect("INCRBY result", v, 5);
    await client.del("nr:counter");
  });

  await assert("HSET / HGET / HGETALL", async () => {
    await client.hSet("nr:hash", { f1: "v1", f2: "v2" });
    const v = await client.hGet("nr:hash", "f1");
    expect("HGET", v, "v1");
    const all = await client.hGetAll("nr:hash");
    expect("HGETALL f1", all.f1, "v1");
    expect("HGETALL f2", all.f2, "v2");
    await client.del("nr:hash");
  });

  await assert("EXISTS", async () => {
    await client.set("nr:exists", "1");
    const n = await client.exists("nr:exists");
    expect("EXISTS present", n, 1);
    await client.del("nr:exists");
    const m = await client.exists("nr:exists");
    expect("EXISTS missing", m, 0);
  });

  await assert("EXPIRE / PERSIST / TTL", async () => {
    await client.set("nr:exp", "val");
    await client.expire("nr:exp", 9999);
    const t = await client.ttl("nr:exp");
    if (t <= 0 || t > 9999) throw new Error(`TTL unexpected: ${t}`);
    await client.persist("nr:exp");
    const t2 = await client.ttl("nr:exp");
    expect("TTL after PERSIST", t2, -1);
    await client.del("nr:exp");
  });

  await assert("TYPE", async () => {
    await client.set("nr:type:str", "x");
    const t = await client.type("nr:type:str");
    expect("TYPE string", t, "string");
    await client.hSet("nr:type:hash", "f", "v");
    const t2 = await client.type("nr:type:hash");
    expect("TYPE hash", t2, "hash");
    const t3 = await client.type("nr:type:none");
    expect("TYPE none", t3, "none");
    await client.del("nr:type:str", "nr:type:hash");
  });

  await assert("DBSIZE returns non-negative integer", async () => {
    const n = await client.dbSize();
    if (typeof n !== "number" || n < 0) throw new Error(`DBSIZE: ${n}`);
  });

  await assert("CLIENT ID returns integer", async () => {
    const id = await client.clientId();
    if (typeof id !== "number" && typeof id !== "bigint")
      throw new Error(`CLIENT ID not a number: ${id}`);
  });

  await client.quit();
});

// ─────────────────────────────────────────────────────────────────────────────
// ioredis (ioredis@5)
// Sends: CLIENT SETINFO lib-name, CLIENT SETINFO lib-ver (no HELLO)
// ─────────────────────────────────────────────────────────────────────────────
await section("ioredis (ioredis@5)", async () => {
  const client = new Redis({
    host: HOST,
    port: PORT,
    enableReadyCheck: false,
    lazyConnect: true,
    maxRetriesPerRequest: 1,
  });

  await assert("connects", () => client.connect());

  await assert("PING", async () => {
    const r = await client.ping();
    expect("PING", r, "PONG");
  });

  await assert("SET / GET roundtrip", async () => {
    await client.set("io:key", "hello");
    const v = await client.get("io:key");
    expect("GET", v, "hello");
  });

  await assert("SET with EX / TTL", async () => {
    await client.set("io:ttl", "val", "EX", 100);
    const t = await client.ttl("io:ttl");
    if (t <= 0 || t > 100) throw new Error(`TTL out of range: ${t}`);
  });

  await assert("DEL", async () => {
    const n = await client.del("io:key");
    expect("DEL", n, 1);
  });

  await assert("GET missing key returns null", async () => {
    const v = await client.get("io:missing");
    expectNull("GET missing", v);
  });

  await assert("MSET / MGET", async () => {
    await client.mset("io:m1", "a", "io:m2", "b");
    const vals = await client.mget("io:m1", "io:m2", "io:missing");
    expect("MGET[0]", vals[0], "a");
    expect("MGET[1]", vals[1], "b");
    expectNull("MGET[2]", vals[2]);
    await client.del("io:m1", "io:m2");
  });

  await assert("INCR / INCRBY", async () => {
    await client.set("io:counter", "0");
    await client.incr("io:counter");
    const v = await client.incrby("io:counter", 4);
    expect("INCRBY result", v, 5);
    await client.del("io:counter");
  });

  await assert("HSET / HGET / HGETALL", async () => {
    await client.hset("io:hash", "f1", "v1", "f2", "v2");
    const v = await client.hget("io:hash", "f1");
    expect("HGET", v, "v1");
    const all = await client.hgetall("io:hash");
    expect("HGETALL f1", all.f1, "v1");
    expect("HGETALL f2", all.f2, "v2");
    await client.del("io:hash");
  });

  await assert("EXISTS", async () => {
    await client.set("io:exists", "1");
    const n = await client.exists("io:exists");
    expect("EXISTS present", n, 1);
    await client.del("io:exists");
    const m = await client.exists("io:exists");
    expect("EXISTS missing", m, 0);
  });

  await assert("EXPIRE / PERSIST / TTL", async () => {
    await client.set("io:exp", "val");
    await client.expire("io:exp", 9999);
    const t = await client.ttl("io:exp");
    if (t <= 0 || t > 9999) throw new Error(`TTL unexpected: ${t}`);
    await client.persist("io:exp");
    const t2 = await client.ttl("io:exp");
    expect("TTL after PERSIST", t2, -1);
    await client.del("io:exp");
  });

  await assert("TYPE", async () => {
    await client.set("io:type:str", "x");
    const t = await client.type("io:type:str");
    expect("TYPE string", t, "string");
    await client.hset("io:type:hash", "f", "v");
    const t2 = await client.type("io:type:hash");
    expect("TYPE hash", t2, "hash");
    const t3 = await client.type("io:type:none");
    expect("TYPE none", t3, "none");
    await client.del("io:type:str", "io:type:hash");
  });

  await assert("DBSIZE returns non-negative integer", async () => {
    const n = await client.dbsize();
    if (typeof n !== "number" || n < 0) throw new Error(`DBSIZE: ${n}`);
  });

  await assert("CLIENT ID returns integer", async () => {
    const id = await client.client("ID");
    const n = Number(id);
    if (isNaN(n)) throw new Error(`CLIENT ID not a number: ${id}`);
  });

  client.disconnect();
});

// ─────────────────────────────────────────────────────────────────────────────
// Summary
// ─────────────────────────────────────────────────────────────────────────────
console.log(`\n===============================`);
console.log(`  PASS: ${pass}   FAIL: ${fail}`);
console.log(`===============================\n`);

process.exit(fail > 0 ? 1 : 0);
