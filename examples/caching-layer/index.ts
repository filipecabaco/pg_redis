import { cached, invalidate } from "./cache";
import { getProduct, getProductsByCategory, getAllProducts, updateProductPrice } from "./db";

const PORT = Number(process.env.PORT ?? 3000);

Bun.serve({
  port: PORT,
  routes: {
    "/products": {
      GET: async () => {
        const { data, hit } = await cached("products:all", getAllProducts, 30);
        return Response.json(data, { headers: { "X-Cache": hit ? "HIT" : "MISS" } });
      },
    },

    "/products/:id": {
      GET: async (req) => {
        const id = Number(req.params.id);
        const { data, hit } = await cached(`product:${id}`, () => getProduct(id));
        if (!data) return new Response("Not Found", { status: 404 });
        return Response.json(data, { headers: { "X-Cache": hit ? "HIT" : "MISS" } });
      },

      PATCH: async (req) => {
        const id = Number(req.params.id);
        const body = await req.json() as { price?: number };
        if (typeof body.price !== "number") {
          return Response.json({ error: "price is required and must be a number" }, { status: 400 });
        }
        await updateProductPrice(id, body.price);
        await invalidate(`product:${id}`, "products:all");
        return Response.json({ ok: true });
      },
    },

    "/products/category/:category": {
      GET: async (req) => {
        const { category } = req.params;
        const { data, hit } = await cached(
          `products:category:${category}`,
          () => getProductsByCategory(category),
          30
        );
        return Response.json(data, { headers: { "X-Cache": hit ? "HIT" : "MISS" } });
      },
    },
  },
});

console.log(`Listening on http://localhost:${PORT}`);
console.log(`
Routes:
  GET  /products                     - list all products (cached 30s)
  GET  /products/:id                 - get product by id (cached 60s)
  GET  /products/category/:category  - list by category (cached 30s)
  PATCH /products/:id                - update price, invalidates cache
`);
