-- ============================================
-- MIGRATION : history_synced on queue + customer columns on inventory_history
-- ============================================

-- 1. Queue tracking : Phase A marque history_synced = TRUE apres insert direct
--    Phase B enrichit les rows restantes (history_synced = FALSE, processed > 30 min)
ALTER TABLE inventory_snapshot_queue
  ADD COLUMN IF NOT EXISTS history_synced BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_queue_history_pending
  ON inventory_snapshot_queue (status, history_synced, processed_at)
  WHERE status = 'completed' AND history_synced = FALSE;

-- 2. Customer info sur inventory_history (rempli en Phase B via ShopifyQL + REST lookup)
ALTER TABLE inventory_history
  ADD COLUMN IF NOT EXISTS customer_id BIGINT,
  ADD COLUMN IF NOT EXISTS customer_email VARCHAR(255),
  ADD COLUMN IF NOT EXISTS customer_name VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_inventory_history_customer_id
  ON inventory_history (customer_id) WHERE customer_id IS NOT NULL;
