DROP VIEW IF EXISTS products_clean;

CREATE VIEW products_clean AS
SELECT
    variant_id,
    product_id,
    inventory_item_id,
    COALESCE(cogs, 0) AS cogs,
    status,
    vendor,
    barcode,
    sku,
    COALESCE(NULLIF(UPPER(TRIM(value_color)), ''), 'UNKNOWN') AS value_color,
    COALESCE(NULLIF(UPPER(TRIM(value_size)), ''), 'UNKNOWN') AS value_size,
    title,
    price,
    compare_at_price,
    weight,
    weight_unit,
    position,
    INITCAP(product_title) AS product_title,
    product_handle,
    COALESCE(NULLIF(UPPER(TRIM(product_type)), ''), 'UNKNOWN') AS product_type,
    tags,
    created_at,
    updated_at,
    image_url,
    imported_at,
    data_source,
    company_code,
    commercial_organisation
FROM products;

COMMENT ON VIEW products_clean IS
'Vue normalisee de products : product_type/value_color/value_size en MAJUSCULES avec fallback UNKNOWN, product_title en Title Case, cogs NULL -> 0.';
