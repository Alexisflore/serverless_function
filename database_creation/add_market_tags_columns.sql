-- Script SQL pour ajouter les colonnes market et tags_list à la table orders
-- À exécuter dans Supabase AVANT de tester l'insertion

-- 1. Ajouter la colonne market
ALTER TABLE orders ADD COLUMN IF NOT EXISTS market VARCHAR(5);

-- 2. Ajouter la colonne tags_list pour stocker les tags comme array JSON
ALTER TABLE orders ADD COLUMN IF NOT EXISTS tags_list JSONB;

-- 3. Créer des commentaires pour documenter les colonnes
COMMENT ON COLUMN orders.market IS 'Marché de la commande (US, JP) extrait des tags, NULL si pas de tags';
COMMENT ON COLUMN orders.tags_list IS 'Liste des tags de la commande au format JSON array';

-- 4. Créer des index pour optimiser les requêtes
CREATE INDEX IF NOT EXISTS idx_orders_market ON orders(market);
CREATE INDEX IF NOT EXISTS idx_orders_tags_list ON orders USING GIN (tags_list);

-- 5. Vérification que les colonnes ont été ajoutées
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'orders' 
  AND column_name IN ('market', 'tags_list')
ORDER BY column_name; 