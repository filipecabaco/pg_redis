const redis = new Bun.RedisClient(
  process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
);

const DEFAULT_TTL = 60; // seconds

export async function cached<T>(
  key: string,
  fetch: () => Promise<T>,
  ttl = DEFAULT_TTL
): Promise<{ data: T; hit: boolean }> {
  const raw = await redis.get(key);
  if (raw !== null) {
    return { data: JSON.parse(raw) as T, hit: true };
  }

  const data = await fetch();
  await redis.set(key, JSON.stringify(data), "EX", ttl);
  return { data, hit: false };
}

export async function invalidate(...keys: string[]) {
  if (keys.length > 0) await redis.del(...keys);
}

export async function invalidatePattern(prefix: string, ids: (string | number)[]) {
  const keys = ids.map((id) => `${prefix}:${id}`);
  await invalidate(...keys);
}
