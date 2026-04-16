/**
 * Set patterns against pg_redis.
 *
 * Demonstrates:
 *   - Unique visitor tracking: SADD per visit, SCARD for uniques
 *   - Tag-based filtering: SINTER across tag sets
 *   - Online users: SADD on login, SREM on logout
 */

const url = process.env.REDIS_URL ?? "redis://:testpass@localhost:6379";
const client = new Bun.RedisClient(url);

async function uniqueVisitors() {
  await client.send("DEL", ["visitors:2026-04-16"]);
  const visits = ["u1", "u2", "u1", "u3", "u2", "u4"];
  for (const user of visits) {
    await client.send("SADD", ["visitors:2026-04-16", user]);
  }
  const uniques = await client.send("SCARD", ["visitors:2026-04-16"]);
  console.log(`unique visitors today: ${uniques}`);
}

async function tagFiltering() {
  await client.send("DEL", ["tag:rust", "tag:db", "tag:open-source"]);
  await client.send("SADD", ["tag:rust", "post:1", "post:2", "post:4"]);
  await client.send("SADD", ["tag:db", "post:2", "post:3", "post:4"]);
  await client.send("SADD", ["tag:open-source", "post:2", "post:4", "post:5"]);
  const matches = await client.send("SINTER", ["tag:rust", "tag:db", "tag:open-source"]) as string[];
  console.log(`posts tagged rust ∩ db ∩ open-source: ${matches.sort().join(", ")}`);
}

async function onlineUsers() {
  await client.send("DEL", ["online"]);
  for (const user of ["u1", "u2", "u3"]) {
    await client.send("SADD", ["online", user]);
  }
  console.log(`online now: ${await client.send("SCARD", ["online"])}`);
  await client.send("SREM", ["online", "u2"]);
  const remaining = await client.send("SMEMBERS", ["online"]) as string[];
  console.log(`after u2 logs out: ${remaining.sort().join(", ")}`);
}

await uniqueVisitors();
await tagFiltering();
await onlineUsers();

client.close();
