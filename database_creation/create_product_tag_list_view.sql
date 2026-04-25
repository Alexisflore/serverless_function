-- =============================================================================
-- Vue : product_tag_list
-- =============================================================================
-- Objectif :
--   Eclater les tags de la table `products` en une ligne par couple (tag, product_id).
--   La liste des tags uniques s'obtient via :  SELECT DISTINCT tag FROM product_tag_list;
--
-- Source des tags :
--   `tags` (TEXT) : chaine separee par virgules, telle que renvoyee par Shopify.
--   (La table `products` ne possede pas de colonne `tags_list` JSONB,
--    contrairement a `orders`.)
--
-- Granularite :
--   La table `products` contient une ligne par variante (`variant_id` = PK),
--   mais les tags sont definis au niveau produit. On deduplique donc sur
--   `product_id` pour ne pas multiplier les lignes par variante.
--
-- Idempotent : la vue est recreee a chaque execution.
-- =============================================================================

DROP VIEW IF EXISTS product_tag_list;

CREATE VIEW product_tag_list AS
SELECT DISTINCT
    btrim(t.tag) AS tag,
    p.product_id
FROM products p
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(p.tags, ',')) AS tag
    WHERE p.tags IS NOT NULL
      AND btrim(p.tags) <> ''
) AS t
WHERE btrim(t.tag) <> ''
  AND p.product_id IS NOT NULL;

COMMENT ON VIEW product_tag_list IS
  'Une ligne par couple (tag, product_id). Eclate products.tags (TEXT, separe par virgules) avec deduplication sur product_id pour eviter les doublons entre variantes.';

-- =============================================================================
-- Verifications rapides
-- =============================================================================
--   -- Liste des tags distincts
--   SELECT DISTINCT tag FROM product_tag_list ORDER BY tag;
--
--   -- Nombre de produits par tag
--   SELECT tag, COUNT(*) AS nb_products
--   FROM product_tag_list
--   GROUP BY tag
--   ORDER BY nb_products DESC;
--
--   -- Tous les produits pour un tag donne
--   SELECT product_id FROM product_tag_list WHERE tag = 'SS24';
