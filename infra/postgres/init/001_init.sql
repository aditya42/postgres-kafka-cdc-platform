CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_user') THEN
    CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'cdc_password';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.customers (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  full_name TEXT NOT NULL,
  country_code CHAR(2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES public.customers(id),
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')),
  amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.order_items (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES public.orders(id) ON DELETE CASCADE,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cdc_publication') THEN
    CREATE PUBLICATION cdc_publication FOR TABLE public.customers, public.orders, public.order_items;
  END IF;
END $$;

INSERT INTO public.customers(email, full_name, country_code)
VALUES
  ('alice@example.com', 'Alice Chen', 'US'),
  ('bruno@example.com', 'Bruno Silva', 'BR'),
  ('chandra@example.com', 'Chandra Rao', 'IN')
ON CONFLICT (email) DO NOTHING;

INSERT INTO public.orders(customer_id, status, amount, currency)
SELECT id, 'PAID', 129.99, 'USD' FROM public.customers WHERE email='alice@example.com'
AND NOT EXISTS (SELECT 1 FROM public.orders WHERE amount=129.99);

INSERT INTO public.orders(customer_id, status, amount, currency)
SELECT id, 'PENDING', 79.50, 'USD' FROM public.customers WHERE email='bruno@example.com'
AND NOT EXISTS (SELECT 1 FROM public.orders WHERE amount=79.50);
