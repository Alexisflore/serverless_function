-- ============================================
-- MIGRATION: Denormalize default_address + metafields on customers
-- Adds individual columns extracted from JSONB blobs for easier filtering/reporting
-- ============================================

-- 1. Default address flat columns (from default_address JSONB)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_first_name VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_last_name VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_company VARCHAR(255);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_address1 VARCHAR(255);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_address2 VARCHAR(255);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_city VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_province VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_province_code VARCHAR(10);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_country VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_country_code VARCHAR(10);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_zip VARCHAR(30);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS default_address_phone VARCHAR(50);

-- 2. Metafield flat columns (from metafields JSONB)
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_title VARCHAR(50);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_clienteling_tags TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_assigned_store VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_sales_assistant VARCHAR(200);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_store_attachment VARCHAR(100);
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_contact_preferences TEXT;
ALTER TABLE customers ADD COLUMN IF NOT EXISTS mf_clienteling_registration_date DATE;

-- 3. Backfill default_address flat columns from existing JSONB data
UPDATE customers SET
  default_address_first_name  = default_address->>'firstName',
  default_address_last_name   = default_address->>'lastName',
  default_address_company     = default_address->>'company',
  default_address_address1    = default_address->>'address1',
  default_address_address2    = default_address->>'address2',
  default_address_city        = default_address->>'city',
  default_address_province    = default_address->>'province',
  default_address_province_code = default_address->>'provinceCode',
  default_address_country     = default_address->>'country',
  default_address_country_code = default_address->>'countryCodeV2',
  default_address_zip         = default_address->>'zip',
  default_address_phone       = default_address->>'phone'
WHERE default_address IS NOT NULL AND default_address != '{}';

-- 4. Backfill metafield flat columns from existing JSONB data
UPDATE customers SET
  mf_title                        = metafields->>'custom.title',
  mf_clienteling_tags             = metafields->>'bspk.clienteling_tags',
  mf_assigned_store               = metafields->>'custom.assigned_store',
  mf_sales_assistant              = metafields->>'custom.sales_assistant',
  mf_store_attachment             = metafields->>'custom.store_attachment',
  mf_contact_preferences          = metafields->>'custom.contact_preferences',
  mf_clienteling_registration_date = (metafields->>'custom.clienteling_registration_date')::DATE
WHERE metafields IS NOT NULL AND metafields != '{}';

-- 5. Indexes on the most useful new columns
CREATE INDEX IF NOT EXISTS idx_customers_default_address_country_code
  ON customers (default_address_country_code);

CREATE INDEX IF NOT EXISTS idx_customers_mf_store_attachment
  ON customers (mf_store_attachment) WHERE mf_store_attachment IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_customers_mf_assigned_store
  ON customers (mf_assigned_store) WHERE mf_assigned_store IS NOT NULL;
