CREATE EXTENSION IF NOT EXISTS pg_redis;

CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  price NUMERIC(10, 2) NOT NULL,
  stock INTEGER NOT NULL DEFAULT 0,
  category TEXT NOT NULL
);

INSERT INTO products (name, description, price, stock, category) VALUES
  ('Laptop Pro 15',   'High-performance laptop with M3 chip',   1999.99, 42,  'electronics'),
  ('Wireless Mouse',  'Ergonomic wireless mouse, 2.4GHz',           29.99, 150, 'accessories'),
  ('Mechanical Keyboard', 'Tenkeyless, brown switches',           89.99,  75,  'accessories'),
  ('4K Monitor 27"',  'IPS panel, 144Hz, USB-C',                 399.99,  30,  'electronics'),
  ('USB-C Hub 7-in-1','Multiport adapter with HDMI, SD, USB-A',   49.99, 200,  'accessories'),
  ('Noise-Cancelling Headphones', 'ANC, 30h battery',            249.99,  60,  'audio'),
  ('Webcam 1080p',    'Built-in microphone, plug-and-play',        69.99,  90,  'accessories'),
  ('Portable SSD 1TB','USB 3.2 Gen 2, 1050MB/s read',            109.99,  55,  'storage');
