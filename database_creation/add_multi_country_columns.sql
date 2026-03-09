-- =================================================================================
-- MIGRATION : Ajout des colonnes multi-pays (data_source, company_code, commercial_organisation)
-- =================================================================================
-- Objectif : Préparer la base de données pour supporter plusieurs pays/stores
--   - data_source          : source des données (ex: 'Shopify')
--   - company_code         : code entreprise (ex: 'ADAM_LIPPES')
--   - commercial_organisation : pays/organisation commerciale (US, JP, FR, UK)
--
-- Les IDs Shopify sont globalement uniques, donc les clés primaires existantes
-- restent inchangées. Les nouvelles colonnes servent au filtrage et au reporting.
-- =================================================================================

BEGIN;

-- ============================================
-- ÉTAPE 1 : Ajout des colonnes à toutes les tables
-- ============================================

-- 1. orders
ALTER TABLE orders ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 2. orders_details
ALTER TABLE orders_details ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE orders_details ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE orders_details ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 3. transaction
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE transaction ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 4. customers
ALTER TABLE customers ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE customers ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE customers ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 5. draft_order
ALTER TABLE draft_order ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE draft_order ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE draft_order ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 6. payout
ALTER TABLE payout ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE payout ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE payout ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 7. payout_transaction
ALTER TABLE payout_transaction ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE payout_transaction ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE payout_transaction ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 8. inventory
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 9. inventory_history
ALTER TABLE inventory_history ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE inventory_history ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE inventory_history ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 10. locations
ALTER TABLE locations ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE locations ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE locations ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- 11. products
ALTER TABLE products ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'Shopify';
ALTER TABLE products ADD COLUMN IF NOT EXISTS company_code VARCHAR(50) DEFAULT 'ADAM_LIPPES';
ALTER TABLE products ADD COLUMN IF NOT EXISTS commercial_organisation VARCHAR(10) DEFAULT 'US';

-- ============================================
-- ÉTAPE 2 : Contraintes NOT NULL (les DEFAULTs remplissent les lignes existantes)
-- ============================================

ALTER TABLE orders ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE orders ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE orders ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE orders_details ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE orders_details ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE orders_details ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE transaction ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE transaction ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE transaction ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE customers ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE customers ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE customers ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE draft_order ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE draft_order ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE draft_order ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE payout ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE payout ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE payout ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE payout_transaction ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE payout_transaction ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE payout_transaction ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE inventory ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE inventory ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE inventory ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE inventory_history ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE inventory_history ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE inventory_history ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE locations ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE locations ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE locations ALTER COLUMN commercial_organisation SET NOT NULL;

ALTER TABLE products ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE products ALTER COLUMN company_code SET NOT NULL;
ALTER TABLE products ALTER COLUMN commercial_organisation SET NOT NULL;

-- ============================================
-- ÉTAPE 3 : Index pour performance des requêtes filtrées par pays
-- ============================================

CREATE INDEX IF NOT EXISTS idx_orders_commercial_org ON orders(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_orders_details_commercial_org ON orders_details(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_transaction_commercial_org ON transaction(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_customers_commercial_org ON customers(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_draft_order_commercial_org ON draft_order(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_payout_commercial_org ON payout(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_payout_transaction_commercial_org ON payout_transaction(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_inventory_commercial_org ON inventory(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_inventory_history_commercial_org ON inventory_history(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_locations_commercial_org ON locations(commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_products_commercial_org ON products(commercial_organisation);

-- Index composites utiles (date + pays pour les requêtes courantes)
CREATE INDEX IF NOT EXISTS idx_orders_created_org ON orders(created_at, commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_transaction_date_org ON transaction(date, commercial_organisation);
CREATE INDEX IF NOT EXISTS idx_inventory_history_recorded_org ON inventory_history(recorded_at DESC, commercial_organisation);

-- ============================================
-- ÉTAPE 4 : Commentaires sur les nouvelles colonnes
-- ============================================

COMMENT ON COLUMN orders.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN orders.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN orders.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN orders_details.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN orders_details.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN orders_details.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN transaction.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN transaction.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN transaction.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN customers.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN customers.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN customers.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN draft_order.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN draft_order.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN draft_order.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN payout.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN payout.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN payout.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN payout_transaction.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN payout_transaction.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN payout_transaction.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN inventory.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN inventory.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN inventory.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN inventory_history.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN inventory_history.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN inventory_history.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN locations.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN locations.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN locations.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

COMMENT ON COLUMN products.data_source IS 'Source des données (ex: Shopify)';
COMMENT ON COLUMN products.company_code IS 'Code entreprise (ex: ADAM_LIPPES)';
COMMENT ON COLUMN products.commercial_organisation IS 'Organisation commerciale / pays (US, JP, FR, UK)';

-- ============================================
-- ÉTAPE 5 : Mise à jour des vues
-- ============================================

-- Vue inventory_changes_with_diff : ajout de commercial_organisation
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
    WINDOW w AS (PARTITION BY h.inventory_item_id, h.location_id, h.commercial_organisation ORDER BY h.recorded_at)
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
    data_source,
    company_code,
    commercial_organisation,

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

-- Vue inventory_snapshot_latest : ajout de commercial_organisation
CREATE OR REPLACE VIEW inventory_snapshot_latest AS
SELECT DISTINCT ON (inventory_item_id, location_id, commercial_organisation)
    inventory_item_id,
    location_id,
    variant_id,
    product_id,
    sku,
    available,
    committed,
    damaged,
    incoming,
    on_hand,
    quality_control,
    reserved,
    safety_stock,
    recorded_at,
    change_type,
    data_source,
    company_code,
    commercial_organisation
FROM inventory_history
ORDER BY inventory_item_id, location_id, commercial_organisation, recorded_at DESC;

-- Vue inventory_stock_movements : ajout de commercial_organisation
CREATE OR REPLACE VIEW inventory_stock_movements AS
SELECT
    ih.id,
    ih.inventory_item_id,
    ih.location_id,
    ih.variant_id,
    ih.product_id,
    ih.sku,
    ih.recorded_at,
    ih.change_type,
    ih.available,
    ih.available_stock_movement,
    ih.data_source,
    ih.company_code,
    ih.commercial_organisation,
    CASE
        WHEN ih.available_stock_movement > 0 THEN 'ENTRÉE'
        WHEN ih.available_stock_movement < 0 THEN 'SORTIE'
        ELSE 'AUCUN MOUVEMENT'
    END AS movement_type,
    ABS(ih.available_stock_movement) AS movement_quantity
FROM inventory_history ih
ORDER BY ih.recorded_at DESC;

-- ============================================
-- ÉTAPE 6 : Mise à jour des fonctions utilitaires
-- ============================================

-- Fonction get_inventory_at_date : ajout de commercial_organisation en paramètre optionnel
CREATE OR REPLACE FUNCTION get_inventory_at_date(
    p_date TIMESTAMP WITH TIME ZONE,
    p_commercial_org VARCHAR(10) DEFAULT NULL
)
RETURNS TABLE (
    inventory_item_id BIGINT,
    location_id BIGINT,
    variant_id BIGINT,
    product_id BIGINT,
    sku VARCHAR(255),
    available INTEGER,
    committed INTEGER,
    damaged INTEGER,
    incoming INTEGER,
    on_hand INTEGER,
    quality_control INTEGER,
    reserved INTEGER,
    safety_stock INTEGER,
    recorded_at TIMESTAMP WITH TIME ZONE,
    commercial_organisation VARCHAR(10)
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (h.inventory_item_id, h.location_id, h.commercial_organisation)
        h.inventory_item_id,
        h.location_id,
        h.variant_id,
        h.product_id,
        h.sku,
        h.available,
        h.committed,
        h.damaged,
        h.incoming,
        h.on_hand,
        h.quality_control,
        h.reserved,
        h.safety_stock,
        h.recorded_at,
        h.commercial_organisation
    FROM inventory_history h
    WHERE h.recorded_at <= p_date
      AND (p_commercial_org IS NULL OR h.commercial_organisation = p_commercial_org)
    ORDER BY h.inventory_item_id, h.location_id, h.commercial_organisation, h.recorded_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Fonction get_item_history : ajout de commercial_organisation
CREATE OR REPLACE FUNCTION get_item_history(
    p_inventory_item_id BIGINT,
    p_location_id BIGINT,
    p_days_back INTEGER DEFAULT 30,
    p_commercial_org VARCHAR(10) DEFAULT NULL
)
RETURNS TABLE (
    id BIGINT,
    recorded_at TIMESTAMP WITH TIME ZONE,
    change_type VARCHAR(50),
    available INTEGER,
    on_hand INTEGER,
    committed INTEGER,
    diff_available INTEGER,
    diff_on_hand INTEGER,
    diff_committed INTEGER,
    commercial_organisation VARCHAR(10)
) AS $$
BEGIN
    RETURN QUERY
    WITH history AS (
        SELECT
            h.id,
            h.recorded_at,
            h.change_type,
            h.available,
            h.on_hand,
            h.committed,
            h.commercial_organisation,
            LAG(h.available) OVER (ORDER BY h.recorded_at) AS prev_available,
            LAG(h.on_hand) OVER (ORDER BY h.recorded_at) AS prev_on_hand,
            LAG(h.committed) OVER (ORDER BY h.recorded_at) AS prev_committed
        FROM inventory_history h
        WHERE h.inventory_item_id = p_inventory_item_id
          AND h.location_id = p_location_id
          AND h.recorded_at >= NOW() - (p_days_back || ' days')::INTERVAL
          AND (p_commercial_org IS NULL OR h.commercial_organisation = p_commercial_org)
        ORDER BY h.recorded_at DESC
    )
    SELECT
        history.id,
        history.recorded_at,
        history.change_type,
        history.available,
        history.on_hand,
        history.committed,
        history.available - COALESCE(history.prev_available, history.available) AS diff_available,
        history.on_hand - COALESCE(history.prev_on_hand, history.on_hand) AS diff_on_hand,
        history.committed - COALESCE(history.prev_committed, history.committed) AS diff_committed,
        history.commercial_organisation
    FROM history;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ÉTAPE 7 : Vérification
-- ============================================

DO $$
DECLARE
    tbl TEXT;
    col_count INTEGER;
BEGIN
    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'Vérification de la migration multi-pays';
    RAISE NOTICE '========================================================================';

    FOR tbl IN
        SELECT unnest(ARRAY[
            'orders', 'orders_details', 'transaction', 'customers',
            'draft_order', 'payout', 'payout_transaction',
            'inventory', 'inventory_history', 'locations', 'products'
        ])
    LOOP
        SELECT COUNT(*) INTO col_count
        FROM information_schema.columns
        WHERE table_name = tbl
          AND column_name IN ('data_source', 'company_code', 'commercial_organisation');

        IF col_count = 3 THEN
            RAISE NOTICE '  ✅ % : 3/3 colonnes présentes', tbl;
        ELSE
            RAISE NOTICE '  ❌ % : %/3 colonnes présentes', tbl, col_count;
        END IF;
    END LOOP;

    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'Migration terminée avec succès !';
    RAISE NOTICE '========================================================================';
END $$;

COMMIT;
