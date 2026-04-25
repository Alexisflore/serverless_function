-- ============================================
-- MIGRATION: Add billing_* columns on `customers` (denormalized from `orders`)
--
-- Source de vérité = `orders.billing_*` (15 colonnes posées à chaque commande).
-- On copie la facturation de la commande LA PLUS RÉCENTE par client
-- (DISTINCT ON ordonné par created_at DESC, _id_order DESC en départage).
--
-- Sécurité : ALTER ... ADD COLUMN IF NOT EXISTS, donc rejouable.
-- Le backfill est borné aux clients qui ont au moins une commande
-- avec un minimum d'info de facturation (address1 / city / country / zip).
-- ============================================

-- 1. Ajout des colonnes (mêmes longueurs que `orders`)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_first_name      VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_last_name       VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_name            VARCHAR(200);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_company         TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_address1        TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_address2        TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_city            VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_province        VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_province_code   VARCHAR(20);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_country         VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_country_code    VARCHAR(10);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_zip             VARCHAR(20);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_phone           VARCHAR(50);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_latitude        NUMERIC(10, 6);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_longitude       NUMERIC(10, 6);

-- Traçabilité : commande source de la facturation courante
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_order_id           TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_order_created_at   TIMESTAMP;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS billing_address_updated_at TIMESTAMP;

-- 2. Backfill : commande la plus récente par client (avec billing non vide)
WITH latest_billing AS (
    SELECT DISTINCT ON (o._id_customer)
        o._id_customer,
        o._id_order,
        o.created_at,
        o.billing_first_name,
        o.billing_last_name,
        o.billing_name,
        o.billing_company,
        o.billing_address1,
        o.billing_address2,
        o.billing_city,
        o.billing_province,
        o.billing_province_code,
        o.billing_country,
        o.billing_country_code,
        o.billing_zip,
        o.billing_phone,
        o.billing_latitude,
        o.billing_longitude
    FROM orders o
    WHERE o._id_customer IS NOT NULL
      AND (
            o.billing_address1   IS NOT NULL
         OR o.billing_city       IS NOT NULL
         OR o.billing_country    IS NOT NULL
         OR o.billing_zip        IS NOT NULL
         OR o.billing_phone      IS NOT NULL
      )
    ORDER BY
        o._id_customer,
        o.created_at DESC NULLS LAST,
        o._id_order  DESC
)
UPDATE customers c
SET
    billing_first_name         = lb.billing_first_name,
    billing_last_name          = lb.billing_last_name,
    billing_name               = lb.billing_name,
    billing_company            = lb.billing_company,
    billing_address1           = lb.billing_address1,
    billing_address2           = lb.billing_address2,
    billing_city               = lb.billing_city,
    billing_province           = lb.billing_province,
    billing_province_code      = lb.billing_province_code,
    billing_country            = lb.billing_country,
    billing_country_code       = lb.billing_country_code,
    billing_zip                = lb.billing_zip,
    billing_phone              = lb.billing_phone,
    billing_latitude           = lb.billing_latitude,
    billing_longitude          = lb.billing_longitude,
    billing_order_id           = lb._id_order,
    billing_order_created_at   = lb.created_at,
    billing_address_updated_at = NOW()
FROM latest_billing lb
WHERE c.customer_id = lb._id_customer;

-- 3. Index utiles pour les filtres / segmentations
CREATE INDEX IF NOT EXISTS idx_customers_billing_country_code
    ON customers (billing_country_code);

CREATE INDEX IF NOT EXISTS idx_customers_billing_country
    ON customers (billing_country);

CREATE INDEX IF NOT EXISTS idx_customers_billing_city
    ON customers (billing_city);

CREATE INDEX IF NOT EXISTS idx_customers_billing_zip
    ON customers (billing_zip);

CREATE INDEX IF NOT EXISTS idx_customers_billing_order_id
    ON customers (billing_order_id);

-- Commentaires
COMMENT ON COLUMN customers.billing_address1           IS 'Dénormalisé depuis orders.billing_address1 de la commande la plus récente (created_at DESC)';
COMMENT ON COLUMN customers.billing_country            IS 'Dénormalisé depuis orders.billing_country de la commande la plus récente';
COMMENT ON COLUMN customers.billing_order_id           IS 'orders._id_order utilisé comme source pour les colonnes billing_*';
COMMENT ON COLUMN customers.billing_order_created_at   IS 'orders.created_at de la commande source';
COMMENT ON COLUMN customers.billing_address_updated_at IS 'Date du dernier rafraîchissement billing_* (refresh_customer_billing_addresses)';
