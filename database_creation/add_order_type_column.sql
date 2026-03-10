-- Add order_type column to orders table (sourced from Shopify order metafield ORDER_TYPE)
ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_type VARCHAR(100);

COMMENT ON COLUMN orders.order_type IS 'Order type from Shopify metafield (e.g. MODA_ORDERS). Namespace: custom, Key: ORDER_TYPE';

CREATE INDEX IF NOT EXISTS idx_orders_order_type ON orders(order_type);
