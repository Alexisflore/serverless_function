-- Disable inventory_history triggers on the inventory table.
-- inventory_history is now populated by ShopifyQL adjustments in process_inventory_queue().
-- The trigger functions are kept in place so they can be re-enabled if needed.

DROP TRIGGER IF EXISTS trigger_log_inventory_insert ON inventory;
DROP TRIGGER IF EXISTS trigger_log_inventory_update ON inventory;
DROP TRIGGER IF EXISTS trigger_log_inventory_delete ON inventory;
