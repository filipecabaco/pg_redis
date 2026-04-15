import { test, expect, beforeAll, afterAll } from "bun:test";

const BASE = "http://localhost:3001";

// Spin up a minimal test server that wires the same routes
import { cached, invalidate } from "./cache";
import { getProduct, getProductsByCategory, getAllProducts, updateProductPrice } from "./db";

const server = Bun.serve({
  port: 3001,
  routes: {
    "/products": {
      GET: async () => {
        const { data, hit } = await cached("test:products:all", getAllProducts, 30);
        return Response.json(data, { headers: { "X-Cache": hit ? "HIT" : "MISS" } });
      },
    },
    "/products/:id": {
      GET: async (req) => {
        const id = Number(req.params.id);
        const { data, hit } = await cached(`test:product:${id}`, () => getProduct(id));
        if (!data) return new Response("Not Found", { status: 404 });
        return Response.json(data, { headers: { "X-Cache": hit ? "HIT" : "MISS" } });
      },
      PATCH: async (req) => {
        const id = Number(req.params.id);
        const body = await req.json() as { price?: number };
        if (typeof body.price !== "number") {
          return Response.json({ error: "price is required" }, { status: 400 });
        }
        await updateProductPrice(id, body.price);
        await invalidate(`test:product:${id}`, "test:products:all");
        return Response.json({ ok: true });
      },
    },
    "/products/category/:category": {
      GET: async (req) => {
        const { category } = req.params;
        const { data, hit } = await cached(
          `test:products:category:${category}`,
          () => getProductsByCategory(category),
          30
        );
        return Response.json(data, { headers: { "X-Cache": hit ? "HIT" : "MISS" } });
      },
    },
  },
});

beforeAll(async () => {
  await invalidate("test:products:all", "test:product:1", "test:products:category:electronics");
});

afterAll(() => {
  server.stop();
});

test("GET /products returns all products with a cache MISS on first request", async () => {
  const res = await fetch(`${BASE}/products`);
  const products = await res.json();

  expect(res.status).toBe(200);
  expect(res.headers.get("X-Cache")).toBe("MISS");
  expect(Array.isArray(products)).toBe(true);
  expect(products.length).toBeGreaterThan(0);
});

test("GET /products returns a cache HIT on second request", async () => {
  const res = await fetch(`${BASE}/products`);
  expect(res.headers.get("X-Cache")).toBe("HIT");
});

test("GET /products/:id returns a single product with a cache MISS on first request", async () => {
  const res = await fetch(`${BASE}/products/1`);
  const product = await res.json();

  expect(res.status).toBe(200);
  expect(res.headers.get("X-Cache")).toBe("MISS");
  expect(product.id).toBe(1);
  expect(typeof product.name).toBe("string");
});

test("GET /products/:id returns a cache HIT on second request", async () => {
  const res = await fetch(`${BASE}/products/1`);
  expect(res.headers.get("X-Cache")).toBe("HIT");
});

test("GET /products/:id returns 404 for unknown id", async () => {
  const res = await fetch(`${BASE}/products/99999`);
  expect(res.status).toBe(404);
});

test("GET /products/category/:category returns filtered products", async () => {
  const res = await fetch(`${BASE}/products/category/electronics`);
  const products = await res.json();

  expect(res.status).toBe(200);
  expect(res.headers.get("X-Cache")).toBe("MISS");
  expect(products.every((p: { category: string }) => p.category === "electronics")).toBe(true);
});

test("PATCH /products/:id updates price and invalidates cache", async () => {
  const patch = await fetch(`${BASE}/products/1`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ price: 1799.99 }),
  });
  expect(patch.status).toBe(200);
  expect((await patch.json()).ok).toBe(true);

  // Cache was invalidated — next GET must be a MISS and return updated price
  const res = await fetch(`${BASE}/products/1`);
  const product = await res.json();
  expect(res.headers.get("X-Cache")).toBe("MISS");
  expect(parseFloat(product.price)).toBe(1799.99);
});

test("PATCH /products/:id with missing price returns 400", async () => {
  const res = await fetch(`${BASE}/products/1`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  expect(res.status).toBe(400);
});
