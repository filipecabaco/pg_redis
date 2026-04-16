/**
 * SET flag patterns against pg_redis.
 *
 * Demonstrates:
 *   - Distributed lock acquire/release with NX + EX
 *   - Cache-aside conditional update with XX
 *   - Atomic read-modify with GET
 */

const url = process.env.REDIS_URL ?? "redis://:testpass@localhost:6379";
const client = new Bun.RedisClient(url);

async function distributedLock() {
  const token = crypto.randomUUID();
  const acquired = await client.send("SET", ["lock:resource", token, "NX", "EX", "30"]);
  if (acquired === "OK") {
    console.log(`acquired lock with token ${token}`);
  } else {
    console.log("lock already held");
  }
}

async function cacheAsideUpdate() {
  await client.set("cache:user:1", JSON.stringify({ name: "alice" }));
  const updated = await client.send(
    "SET",
    ["cache:user:1", JSON.stringify({ name: "alice updated" }), "XX"],
  );
  console.log(`XX update result: ${updated}`);
}

async function atomicReadModify() {
  await client.set("counter:state", "5");
  const old = await client.send("SET", ["counter:state", "10", "GET"]);
  console.log(`old value: ${old}, new value: ${await client.get("counter:state")}`);
}

await distributedLock();
await cacheAsideUpdate();
await atomicReadModify();

client.close();
