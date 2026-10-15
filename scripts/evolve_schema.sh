#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T postgres psql -U postgres -d commerce <<'SQL'
ALTER TABLE public.orders
  ADD COLUMN IF NOT EXISTS sales_channel TEXT;

UPDATE public.orders
SET sales_channel = CASE WHEN id % 2 = 0 THEN 'mobile' ELSE 'web' END,
    updated_at = now()
WHERE sales_channel IS NULL;

INSERT INTO public.orders(customer_id, status, amount, currency, sales_channel)
SELECT id, 'PAID', 199.00, 'USD', 'partner'
FROM public.customers
ORDER BY id
LIMIT 1;
SQL

echo "Applied schema evolution: public.orders.sales_channel"
echo "Wait ~10 seconds, then run: make schemas"
