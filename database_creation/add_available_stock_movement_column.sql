-- Migration : Ajout de la colonne available_stock_movement dans inventory_history
-- Cette colonne représente le mouvement de stock disponible :
--   - INSERT : +available (ajout de stock)
--   - UPDATE : NEW.available - OLD.available (différence)
--   - DELETE : -available (retrait de stock)

-- ============================================
-- ÉTAPE 1 : Ajouter la colonne
-- ============================================

ALTER TABLE inventory_history 
ADD COLUMN available_stock_movement INTEGER DEFAULT 0;

COMMENT ON COLUMN inventory_history.available_stock_movement IS 
'Mouvement de stock disponible : 
 - INSERT: +available (nouveau stock ajouté)
 - UPDATE: NEW.available - OLD.available (différence)
 - DELETE: -available (stock retiré)';

-- Index pour requêtes sur les mouvements de stock
CREATE INDEX idx_inventory_history_stock_movement 
ON inventory_history(available_stock_movement) 
WHERE available_stock_movement != 0;

-- ============================================
-- ÉTAPE 2 : Modifier les fonctions trigger
-- ============================================

-- Fonction trigger pour INSERT : available_stock_movement = +available
CREATE OR REPLACE FUNCTION log_inventory_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO inventory_history (
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
        last_updated_at,
        scheduled_changes,
        change_type,
        available_stock_movement,
        recorded_at
    ) VALUES (
        NEW.inventory_item_id,
        NEW.location_id,
        NEW.variant_id,
        NEW.product_id,
        NEW.sku,
        NEW.available,
        NEW.committed,
        NEW.damaged,
        NEW.incoming,
        NEW.on_hand,
        NEW.quality_control,
        NEW.reserved,
        NEW.safety_stock,
        NEW.last_updated_at,
        NEW.scheduled_changes,
        'INSERT',
        NEW.available,  -- Mouvement = +available (nouveau stock)
        NOW()
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fonction trigger pour UPDATE : available_stock_movement = NEW.available - OLD.available
CREATE OR REPLACE FUNCTION log_inventory_update()
RETURNS TRIGGER AS $$
BEGIN
    -- Vérifier si au moins une quantité a changé
    IF (OLD.available IS DISTINCT FROM NEW.available OR
        OLD.committed IS DISTINCT FROM NEW.committed OR
        OLD.damaged IS DISTINCT FROM NEW.damaged OR
        OLD.incoming IS DISTINCT FROM NEW.incoming OR
        OLD.on_hand IS DISTINCT FROM NEW.on_hand OR
        OLD.quality_control IS DISTINCT FROM NEW.quality_control OR
        OLD.reserved IS DISTINCT FROM NEW.reserved OR
        OLD.safety_stock IS DISTINCT FROM NEW.safety_stock) THEN
        
        INSERT INTO inventory_history (
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
            last_updated_at,
            scheduled_changes,
            change_type,
            available_stock_movement,
            recorded_at
        ) VALUES (
            NEW.inventory_item_id,
            NEW.location_id,
            NEW.variant_id,
            NEW.product_id,
            NEW.sku,
            NEW.available,
            NEW.committed,
            NEW.damaged,
            NEW.incoming,
            NEW.on_hand,
            NEW.quality_control,
            NEW.reserved,
            NEW.safety_stock,
            NEW.last_updated_at,
            NEW.scheduled_changes,
            'UPDATE',
            NEW.available - OLD.available,  -- Mouvement = différence
            NOW()
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fonction trigger pour DELETE : available_stock_movement = -available
CREATE OR REPLACE FUNCTION log_inventory_delete()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO inventory_history (
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
        last_updated_at,
        scheduled_changes,
        change_type,
        available_stock_movement,
        recorded_at
    ) VALUES (
        OLD.inventory_item_id,
        OLD.location_id,
        OLD.variant_id,
        OLD.product_id,
        OLD.sku,
        OLD.available,
        OLD.committed,
        OLD.damaged,
        OLD.incoming,
        OLD.on_hand,
        OLD.quality_control,
        OLD.reserved,
        OLD.safety_stock,
        OLD.last_updated_at,
        OLD.scheduled_changes,
        'DELETE',
        -OLD.available,  -- Mouvement = -available (stock retiré)
        NOW()
    );
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ÉTAPE 3 : Recalculer pour les données existantes
-- ============================================

-- Mettre à jour les lignes existantes avec le bon available_stock_movement
-- basé sur le change_type

-- Pour les INSERT existants : mouvement = available
UPDATE inventory_history 
SET available_stock_movement = available
WHERE change_type = 'INSERT' 
  AND available_stock_movement = 0;

-- Pour les DELETE existants : mouvement = -available
UPDATE inventory_history 
SET available_stock_movement = -available
WHERE change_type = 'DELETE' 
  AND available_stock_movement = 0;

-- Pour les UPDATE existants : calculer la différence avec la ligne précédente
WITH history_with_prev AS (
    SELECT 
        id,
        inventory_item_id,
        location_id,
        available,
        LAG(available) OVER (
            PARTITION BY inventory_item_id, location_id 
            ORDER BY recorded_at
        ) as prev_available,
        change_type
    FROM inventory_history
    WHERE change_type = 'UPDATE'
)
UPDATE inventory_history h
SET available_stock_movement = hwp.available - COALESCE(hwp.prev_available, 0)
FROM history_with_prev hwp
WHERE h.id = hwp.id
  AND h.available_stock_movement = 0;

-- ============================================
-- ÉTAPE 4 : Créer une vue pour analyse des mouvements
-- ============================================

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
    -- Indicateur de type de mouvement
    CASE 
        WHEN ih.available_stock_movement > 0 THEN 'ENTRÉE'
        WHEN ih.available_stock_movement < 0 THEN 'SORTIE'
        ELSE 'AUCUN MOUVEMENT'
    END as movement_type,
    -- Valeur absolue du mouvement
    ABS(ih.available_stock_movement) as movement_quantity
FROM inventory_history ih
ORDER BY ih.recorded_at DESC;

COMMENT ON VIEW inventory_stock_movements IS 
'Vue des mouvements de stock disponible avec classification (ENTRÉE/SORTIE)';

-- ============================================
-- ÉTAPE 5 : Statistiques et vérification
-- ============================================

-- Afficher les statistiques
DO $$
DECLARE
    total_rows INTEGER;
    rows_with_movement INTEGER;
    total_entries INTEGER;
    total_exits INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM inventory_history;
    SELECT COUNT(*) INTO rows_with_movement 
    FROM inventory_history 
    WHERE available_stock_movement != 0;
    
    SELECT COUNT(*) INTO total_entries 
    FROM inventory_history 
    WHERE available_stock_movement > 0;
    
    SELECT COUNT(*) INTO total_exits 
    FROM inventory_history 
    WHERE available_stock_movement < 0;
    
    RAISE NOTICE '================================================================================';
    RAISE NOTICE 'Migration terminée avec succès !';
    RAISE NOTICE '================================================================================';
    RAISE NOTICE 'Total de lignes dans inventory_history : %', total_rows;
    RAISE NOTICE 'Lignes avec mouvement de stock : %', rows_with_movement;
    RAISE NOTICE 'Entrées de stock (movement > 0) : %', total_entries;
    RAISE NOTICE 'Sorties de stock (movement < 0) : %', total_exits;
    RAISE NOTICE '================================================================================';
END $$;

-- Afficher quelques exemples
SELECT 
    id,
    sku,
    recorded_at,
    change_type,
    available,
    available_stock_movement,
    CASE 
        WHEN available_stock_movement > 0 THEN 'ENTRÉE'
        WHEN available_stock_movement < 0 THEN 'SORTIE'
        ELSE 'NEUTRE'
    END as type_mouvement
FROM inventory_history
WHERE available_stock_movement != 0
ORDER BY recorded_at DESC
LIMIT 10;
