export const sql = new Bun.sql({
  url: process.env.DATABASE_URL ?? "postgres://postgres:postgres@127.0.0.1:5432/postgres",
});

export type Product = {
  id: number;
  name: string;
  description: string | null;
  price: string;
  stock: number;
  category: string;
};

export async function getProduct(id: number): Promise<Product | null> {
  const rows = await sql<Product[]>`SELECT * FROM products WHERE id = ${id}`;
  return rows[0] ?? null;
}

export async function getProductsByCategory(category: string): Promise<Product[]> {
  return sql<Product[]>`SELECT * FROM products WHERE category = ${category} ORDER BY name`;
}

export async function getAllProducts(): Promise<Product[]> {
  return sql<Product[]>`SELECT * FROM products ORDER BY category, name`;
}

export async function updateProductPrice(id: number, price: number): Promise<void> {
  await sql`UPDATE products SET price = ${price} WHERE id = ${id}`;
}
