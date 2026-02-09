-- ============================================
-- MIGRATION 1 : Enum pour inventory_snapshot_queue.status
-- ============================================

-- Creer l'enum pour les statuts de la queue (idempotent)
DO $$ BEGIN
    CREATE TYPE queue_status AS ENUM ('pending', 'processing', 'completed', 'failed');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- Dropper l'index partiel qui reference status = 'pending'::text
-- (sinon la conversion en enum echoue : queue_status = text n'existe pas)
DROP INDEX IF EXISTS inventory_snapshot_queue_pending_idx;

-- Convertir la colonne status de text vers enum
ALTER TABLE inventory_snapshot_queue
  ALTER COLUMN status DROP DEFAULT,
  ALTER COLUMN status TYPE queue_status USING status::queue_status,
  ALTER COLUMN status SET DEFAULT 'pending';

-- Recreer l'index partiel avec le type enum
CREATE INDEX inventory_snapshot_queue_pending_idx
  ON inventory_snapshot_queue (status, created_at)
  WHERE (status = 'pending');

-- ============================================
-- MIGRATION 2 : Recreer la vue inventory_changes_with_diff
-- Ajout de toutes les colonnes de quantite
-- Format : "(+1) 1" ou "(-1) 0" ou "0" pour les colonnes de quantite
-- Conservation des colonnes diff_* en integer
-- ============================================

DROP VIEW IF EXISTS inventory_changes_with_diff;

CREATE VIEW inventory_changes_with_diff AS
WITH base AS (
    SELECT
        h.*,
        h.available       - COALESCE(LAG(h.available)       OVER w, h.available)       AS diff_available,
        h.on_hand         - COALESCE(LAG(h.on_hand)         OVER w, h.on_hand)         AS diff_on_hand,
        h.committed       - COALESCE(LAG(h.committed)       OVER w, h.committed)       AS diff_committed,
        h.damaged         - COALESCE(LAG(h.damaged)         OVER w, h.damaged)         AS diff_damaged,
        h.incoming        - COALESCE(LAG(h.incoming)        OVER w, h.incoming)        AS diff_incoming,
        h.quality_control - COALESCE(LAG(h.quality_control) OVER w, h.quality_control) AS diff_quality_control,
        h.reserved        - COALESCE(LAG(h.reserved)        OVER w, h.reserved)        AS diff_reserved,
        h.safety_stock    - COALESCE(LAG(h.safety_stock)    OVER w, h.safety_stock)    AS diff_safety_stock
    FROM inventory_history h
    WINDOW w AS (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at)
)
SELECT
    id,
    inventory_item_id,
    location_id,
    variant_id,
    product_id,
    sku,
    recorded_at,
    change_type,

    -- Colonnes formatees "(diff) value" en TEXT
    CASE
        WHEN diff_available > 0 THEN '(+' || diff_available::text || ') ' || available::text
        WHEN diff_available < 0 THEN '('  || diff_available::text || ') ' || available::text
        ELSE available::text
    END AS available,

    CASE
        WHEN diff_on_hand > 0 THEN '(+' || diff_on_hand::text || ') ' || on_hand::text
        WHEN diff_on_hand < 0 THEN '('  || diff_on_hand::text || ') ' || on_hand::text
        ELSE on_hand::text
    END AS on_hand,

    CASE
        WHEN diff_committed > 0 THEN '(+' || diff_committed::text || ') ' || committed::text
        WHEN diff_committed < 0 THEN '('  || diff_committed::text || ') ' || committed::text
        ELSE committed::text
    END AS committed,

    CASE
        WHEN diff_damaged > 0 THEN '(+' || diff_damaged::text || ') ' || damaged::text
        WHEN diff_damaged < 0 THEN '('  || diff_damaged::text || ') ' || damaged::text
        ELSE damaged::text
    END AS damaged,

    CASE
        WHEN diff_incoming > 0 THEN '(+' || diff_incoming::text || ') ' || incoming::text
        WHEN diff_incoming < 0 THEN '('  || diff_incoming::text || ') ' || incoming::text
        ELSE incoming::text
    END AS incoming,

    CASE
        WHEN diff_quality_control > 0 THEN '(+' || diff_quality_control::text || ') ' || quality_control::text
        WHEN diff_quality_control < 0 THEN '('  || diff_quality_control::text || ') ' || quality_control::text
        ELSE quality_control::text
    END AS quality_control,

    CASE
        WHEN diff_reserved > 0 THEN '(+' || diff_reserved::text || ') ' || reserved::text
        WHEN diff_reserved < 0 THEN '('  || diff_reserved::text || ') ' || reserved::text
        ELSE reserved::text
    END AS reserved,

    CASE
        WHEN diff_safety_stock > 0 THEN '(+' || diff_safety_stock::text || ') ' || safety_stock::text
        WHEN diff_safety_stock < 0 THEN '('  || diff_safety_stock::text || ') ' || safety_stock::text
        ELSE safety_stock::text
    END AS safety_stock,

    -- Colonnes diff_* en integer (pour filtrage et comprehension)
    diff_available,
    diff_on_hand,
    diff_committed,
    diff_damaged,
    diff_incoming,
    diff_quality_control,
    diff_reserved,
    diff_safety_stock

FROM base
ORDER BY recorded_at DESC;
