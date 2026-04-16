/**
 * List patterns against pg_redis.
 *
 * Demonstrates:
 *   - Simple FIFO job queue: producer RPUSH, consumer LPOP
 *   - Capped activity feed: LPUSH + LTRIM to keep last N entries
 *   - Reliable queue using LMOVE between pending and processing lists
 */

const url = process.env.REDIS_URL ?? "redis://:testpass@localhost:6379";
const client = new Bun.RedisClient(url);

async function jobQueue() {
  await client.send("DEL", ["jobs"]);
  for (let i = 1; i <= 3; i++) {
    await client.send("RPUSH", ["jobs", JSON.stringify({ id: i, type: "send_email" })]);
  }
  while (true) {
    const job = await client.send("LPOP", ["jobs"]);
    if (job === null) break;
    console.log(`processing ${job}`);
  }
}

async function cappedFeed() {
  await client.send("DEL", ["feed:user:1"]);
  for (let i = 0; i < 1000; i++) {
    await client.send("LPUSH", ["feed:user:1", `event-${i}`]);
    await client.send("LTRIM", ["feed:user:1", "0", "9"]);
  }
  const top10 = await client.send("LRANGE", ["feed:user:1", "0", "-1"]) as string[];
  console.log(`feed length: ${top10.length}, head: ${top10[0]}`);
}

async function reliableQueue() {
  await client.send("DEL", ["pending", "processing"]);
  for (let i = 1; i <= 3; i++) {
    await client.send("RPUSH", ["pending", `job-${i}`]);
  }
  while (true) {
    const job = await client.send("LMOVE", ["pending", "processing", "LEFT", "RIGHT"]);
    if (job === null) break;
    console.log(`took ${job}`);
    await client.send("LREM", ["processing", "1", String(job)]);
  }
}

await jobQueue();
await cappedFeed();
await reliableQueue();

client.close();
