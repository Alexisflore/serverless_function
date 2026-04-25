-- =============================================================================
-- Vue : order_tag_list
-- =============================================================================
-- Objectif :
--   Eclater les tags de la table `orders` en une ligne par couple (tag, commande).
--   Chaque commande apparait autant de fois qu'elle possede de tags.
--   La liste des tags uniques s'obtient via :  SELECT DISTINCT tag FROM order_tag_list;
--
-- Source des tags :
--   1) `tags_list` (JSONB array) si renseigne (cas standard, alimente par la pipeline).
--   2) Sinon, fallback sur le champ texte `tags` (chaine separee par virgules).
--
-- Idempotent : la vue est recreee a chaque execution.
-- =============================================================================

DROP VIEW IF EXISTS order_tag_list;

CREATE VIEW order_tag_list AS
SELECT
    btrim(t.tag) AS tag,
    o._id_order
FROM orders o
CROSS JOIN LATERAL (
    SELECT jsonb_array_elements_text(o.tags_list) AS tag
    WHERE jsonb_typeof(o.tags_list) = 'array'

    UNION ALL

    SELECT unnest(string_to_array(o.tags, ','))
    WHERE o.tags_list IS NULL
      AND o.tags IS NOT NULL
      AND btrim(o.tags) <> ''
) AS t
WHERE btrim(t.tag) <> '';

COMMENT ON VIEW order_tag_list IS
  'Une ligne par couple (tag, _id_order). Eclate orders.tags_list (JSONB) avec fallback sur orders.tags (TEXT).';

-- =============================================================================
-- Verifications rapides
-- =============================================================================
--   -- Liste des tags distincts
--   SELECT DISTINCT tag FROM order_tag_list ORDER BY tag;
--
--   -- Nombre de commandes par tag
--   SELECT tag, COUNT(*) AS nb_orders
--   FROM order_tag_list
--   GROUP BY tag
--   ORDER BY nb_orders DESC;
--
--   -- Toutes les commandes pour un tag donne
--   SELECT _id_order FROM order_tag_list WHERE tag = 'US';
