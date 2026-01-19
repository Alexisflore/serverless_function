-- Création de la table inventory_history pour suivre l'historique des changements d'inventaire
-- Cette table enregistre chaque modification avec un timestamp pour permettre le suivi des évolutions

DROP TABLE IF EXISTS inventory_history CASCADE;

CREATE TABLE inventory_history (
    -- ID auto-incrémenté pour identifier chaque entrée d'historique de manière unique
    id BIGSERIAL PRIMARY KEY,
    
    -- Clés de référence
    inventory_item_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    
    -- IDs de référence et SKU (copiés depuis inventory)
    variant_id BIGINT,
    product_id BIGINT,
    sku VARCHAR(255),
    
    -- Quantités par type (snapshot des valeurs à un instant T)
    available INTEGER DEFAULT 0 NOT NULL,
    committed INTEGER DEFAULT 0 NOT NULL,
    damaged INTEGER DEFAULT 0 NOT NULL,
    incoming INTEGER DEFAULT 0 NOT NULL,
    on_hand INTEGER DEFAULT 0 NOT NULL,
    quality_control INTEGER DEFAULT 0 NOT NULL,
    reserved INTEGER DEFAULT 0 NOT NULL,
    safety_stock INTEGER DEFAULT 0 NOT NULL,
    
    -- Métadonnées de synchronisation
    last_updated_at TIMESTAMP WITH TIME ZONE,
    scheduled_changes JSONB DEFAULT '[]'::jsonb,
    
    -- Timestamp de création de cette ligne d'historique
    -- C'est le champ clé qui permet de suivre l'évolution dans le temps
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    
    -- Type de changement (pour traçabilité)
    change_type VARCHAR(50) DEFAULT 'UPDATE',  -- 'INSERT', 'UPDATE', 'DELETE', 'SYNC'
    
    -- Optionnel : commentaire pour expliquer le changement
    change_comment TEXT,
    
    -- Contrainte de clé étrangère vers locations
    CONSTRAINT fk_inventory_history_location 
        FOREIGN KEY (location_id) 
        REFERENCES locations(_location_id) 
        ON DELETE CASCADE
);

-- Index pour optimiser les requêtes historiques
CREATE INDEX idx_inventory_history_item_location ON inventory_history(inventory_item_id, location_id);
CREATE INDEX idx_inventory_history_recorded_at ON inventory_history(recorded_at DESC);
CREATE INDEX idx_inventory_history_variant_id ON inventory_history(variant_id) WHERE variant_id IS NOT NULL;
CREATE INDEX idx_inventory_history_product_id ON inventory_history(product_id) WHERE product_id IS NOT NULL;
CREATE INDEX idx_inventory_history_sku ON inventory_history(sku) WHERE sku IS NOT NULL;
CREATE INDEX idx_inventory_history_location_id ON inventory_history(location_id);
CREATE INDEX idx_inventory_history_change_type ON inventory_history(change_type);

-- Index composite pour requêtes temporelles sur un item spécifique
CREATE INDEX idx_inventory_history_item_time ON inventory_history(inventory_item_id, location_id, recorded_at DESC);

-- Commentaires sur la table et les colonnes
COMMENT ON TABLE inventory_history IS 'Historique complet des changements d''inventaire. Chaque modification crée une nouvelle ligne avec un timestamp recorded_at pour suivre l''évolution dans le temps.';

COMMENT ON COLUMN inventory_history.id IS 'Identifiant unique de chaque entrée d''historique';
COMMENT ON COLUMN inventory_history.inventory_item_id IS 'ID de l''item d''inventaire Shopify';
COMMENT ON COLUMN inventory_history.location_id IS 'ID de la location';
COMMENT ON COLUMN inventory_history.variant_id IS 'ID du variant Shopify';
COMMENT ON COLUMN inventory_history.product_id IS 'ID du produit Shopify';
COMMENT ON COLUMN inventory_history.sku IS 'SKU du produit';

COMMENT ON COLUMN inventory_history.available IS 'Quantité disponible à la vente au moment de l''enregistrement';
COMMENT ON COLUMN inventory_history.committed IS 'Quantité réservée pour des commandes';
COMMENT ON COLUMN inventory_history.damaged IS 'Quantité endommagée';
COMMENT ON COLUMN inventory_history.incoming IS 'Quantité en cours de réception';
COMMENT ON COLUMN inventory_history.on_hand IS 'Quantité physiquement présente';
COMMENT ON COLUMN inventory_history.quality_control IS 'Quantité en contrôle qualité';
COMMENT ON COLUMN inventory_history.reserved IS 'Quantité réservée';
COMMENT ON COLUMN inventory_history.safety_stock IS 'Stock de sécurité';

COMMENT ON COLUMN inventory_history.recorded_at IS 'Timestamp de création de cette ligne d''historique - permet de suivre l''évolution temporelle';
COMMENT ON COLUMN inventory_history.change_type IS 'Type de modification: INSERT (nouvelle ligne), UPDATE (modification), DELETE (suppression), SYNC (synchronisation Shopify)';
COMMENT ON COLUMN inventory_history.change_comment IS 'Commentaire optionnel pour expliquer le changement';

-- ============================================
-- TRIGGERS pour capturer automatiquement les changements
-- ============================================

-- Fonction trigger pour enregistrer les INSERT dans inventory
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
        NOW()
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fonction trigger pour enregistrer les UPDATE dans inventory
-- Enregistre seulement si une des quantités a changé
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
            NOW()
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Fonction trigger pour enregistrer les DELETE dans inventory
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
        NOW()
    );
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Créer les triggers
CREATE TRIGGER trigger_log_inventory_insert
    AFTER INSERT ON inventory
    FOR EACH ROW
    EXECUTE FUNCTION log_inventory_insert();

CREATE TRIGGER trigger_log_inventory_update
    AFTER UPDATE ON inventory
    FOR EACH ROW
    EXECUTE FUNCTION log_inventory_update();

CREATE TRIGGER trigger_log_inventory_delete
    BEFORE DELETE ON inventory
    FOR EACH ROW
    EXECUTE FUNCTION log_inventory_delete();

-- ============================================
-- VUES UTILES pour l'analyse de l'historique
-- ============================================

-- Vue pour voir les changements récents avec les différences
CREATE OR REPLACE VIEW inventory_changes_with_diff AS
SELECT 
    h.id,
    h.inventory_item_id,
    h.location_id,
    h.variant_id,
    h.product_id,
    h.sku,
    h.recorded_at,
    h.change_type,
    -- Quantités actuelles
    h.available,
    h.on_hand,
    h.committed,
    -- Quantités précédentes (via LAG)
    LAG(h.available) OVER (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at) as prev_available,
    LAG(h.on_hand) OVER (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at) as prev_on_hand,
    LAG(h.committed) OVER (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at) as prev_committed,
    -- Différences calculées
    h.available - COALESCE(LAG(h.available) OVER (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at), h.available) as diff_available,
    h.on_hand - COALESCE(LAG(h.on_hand) OVER (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at), h.on_hand) as diff_on_hand,
    h.committed - COALESCE(LAG(h.committed) OVER (PARTITION BY h.inventory_item_id, h.location_id ORDER BY h.recorded_at), h.committed) as diff_committed
FROM inventory_history h
ORDER BY h.recorded_at DESC;

COMMENT ON VIEW inventory_changes_with_diff IS 'Vue montrant les changements d''inventaire avec les valeurs précédentes et les différences calculées';

-- Vue pour obtenir l'état de l'inventaire à une date donnée
CREATE OR REPLACE VIEW inventory_snapshot_latest AS
SELECT DISTINCT ON (inventory_item_id, location_id)
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
    change_type
FROM inventory_history
ORDER BY inventory_item_id, location_id, recorded_at DESC;

COMMENT ON VIEW inventory_snapshot_latest IS 'Vue montrant le dernier état connu de chaque item d''inventaire dans l''historique';

-- ============================================
-- FONCTIONS UTILITAIRES
-- ============================================

-- Fonction pour obtenir l'inventaire à une date spécifique
CREATE OR REPLACE FUNCTION get_inventory_at_date(
    p_date TIMESTAMP WITH TIME ZONE
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
    recorded_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (h.inventory_item_id, h.location_id)
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
        h.recorded_at
    FROM inventory_history h
    WHERE h.recorded_at <= p_date
    ORDER BY h.inventory_item_id, h.location_id, h.recorded_at DESC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_inventory_at_date IS 'Retourne l''état de l''inventaire à une date donnée en utilisant l''historique';

-- Fonction pour obtenir l'historique d'un item spécifique
CREATE OR REPLACE FUNCTION get_item_history(
    p_inventory_item_id BIGINT,
    p_location_id BIGINT,
    p_days_back INTEGER DEFAULT 30
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
    diff_committed INTEGER
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
            LAG(h.available) OVER (ORDER BY h.recorded_at) as prev_available,
            LAG(h.on_hand) OVER (ORDER BY h.recorded_at) as prev_on_hand,
            LAG(h.committed) OVER (ORDER BY h.recorded_at) as prev_committed
        FROM inventory_history h
        WHERE h.inventory_item_id = p_inventory_item_id
          AND h.location_id = p_location_id
          AND h.recorded_at >= NOW() - (p_days_back || ' days')::INTERVAL
        ORDER BY h.recorded_at DESC
    )
    SELECT 
        history.id,
        history.recorded_at,
        history.change_type,
        history.available,
        history.on_hand,
        history.committed,
        history.available - COALESCE(history.prev_available, history.available) as diff_available,
        history.on_hand - COALESCE(history.prev_on_hand, history.on_hand) as diff_on_hand,
        history.committed - COALESCE(history.prev_committed, history.committed) as diff_committed
    FROM history;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_item_history IS 'Retourne l''historique des changements pour un item d''inventaire spécifique sur une période donnée';

