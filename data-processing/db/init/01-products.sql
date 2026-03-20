CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  sku VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(64) NOT NULL,
  price_usd NUMERIC(12,2) NOT NULL,
  stock INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO products (sku, name, category, price_usd, stock)
VALUES
  ('CPU-RYZEN-7600X', 'AMD Ryzen 5 7600X', 'cpu', 249.99, 15),
  ('CPU-I5-14600K', 'Intel Core i5-14600K', 'cpu', 329.00, 12),
  ('RAM-DDR5-32GB-6000', 'DDR5 32GB 6000MHz Kit', 'ram', 119.50, 25),
  ('RAM-DDR4-16GB-3200', 'DDR4 16GB 3200MHz Kit', 'ram', 49.99, 40),
  ('SSD-NVME-1TB', 'NVMe SSD 1TB', 'storage', 89.90, 30),
  ('GPU-RTX-4060', 'NVIDIA GeForce RTX 4060', 'gpu', 299.00, 8)
ON CONFLICT DO NOTHING;
