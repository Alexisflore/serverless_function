-- Script SQL pour ajouter les colonnes tags et tags_list à la table draft_order
-- À exécuter dans Supabase AVANT de relancer le traitement des draft orders

-- 1. Ajouter la colonne tags (texte brut, comme dans orders)
ALTER TABLE draft_order ADD COLUMN IF NOT EXISTS tags TEXT;

-- 2. Ajouter la colonne tags_list pour stocker les tags comme array JSON
ALTER TABLE draft_order ADD COLUMN IF NOT EXISTS tags_list JSONB;

-- 3. Documenter les colonnes
COMMENT ON COLUMN draft_order.tags IS 'Tags bruts du draft order (chaîne séparée par virgules)';
COMMENT ON COLUMN draft_order.tags_list IS 'Liste des tags du draft order au format JSON array';

-- 4. Index GIN sur tags_list pour requêtes JSONB efficaces
CREATE INDEX IF NOT EXISTS idx_draft_order_tags_list ON draft_order USING GIN (tags_list);

-- 5. Vérification
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'draft_order' 
  AND column_name IN ('tags', 'tags_list')
ORDER BY column_name;
