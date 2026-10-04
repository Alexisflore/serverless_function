-- =================================================================================
-- MIGRATION : Ajout de l'organisation commerciale UK + index queues par shop
-- =================================================================================
-- Objectif :
--   1. Ajouter 'UK' dans la dimension commercial_organization (requis AVANT toute
--      écriture UK : les 11 tables de faits ont une FK sur commercial_organisation).
--   2. Indexer les queues webhook par shop : process_inventory_queue et
--      process_draft_orders_delete_queue filtrent désormais par shop
--      (= SHOPIFY_STORE_DOMAIN du job).
--
-- Idempotente : peut être rejouée sans effet de bord.
--
-- Ordre de déploiement : appliquer AVANT d'activer process-daily-data-uk.yml
-- et avant le backfill UK.
--
-- Rollback (uniquement si aucune ligne UK n'a été écrite) :
--   DELETE FROM commercial_organization WHERE commercial_organization_code = 'UK';
--   DROP INDEX IF EXISTS inventory_snapshot_queue_shop_status_idx;
--   DROP INDEX IF EXISTS draft_orders_delete_queue_shop_status_idx;
-- =================================================================================

BEGIN;

INSERT INTO commercial_organization (commercial_organization_code, commercial_organization_label)
VALUES ('UK', 'United-Kingdom')
ON CONFLICT (commercial_organization_code) DO UPDATE
    SET commercial_organization_label = EXCLUDED.commercial_organization_label,
        updated_at = NOW();

CREATE INDEX IF NOT EXISTS inventory_snapshot_queue_shop_status_idx
    ON inventory_snapshot_queue (shop, status);

CREATE INDEX IF NOT EXISTS draft_orders_delete_queue_shop_status_idx
    ON draft_orders_delete_queue (shop, status);

COMMIT;

-- Vérification :
-- SELECT * FROM commercial_organization ORDER BY 1;
-- SELECT shop, status, count(*) FROM inventory_snapshot_queue GROUP BY 1, 2 ORDER BY 1, 2;
-- SELECT shop, status, count(*) FROM draft_orders_delete_queue GROUP BY 1, 2 ORDER BY 1, 2;
