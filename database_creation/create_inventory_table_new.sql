-- Création de la nouvelle table inventory avec contrainte unique sur (inventory_item_id, location_id)
-- Cette table stocke les quantités d'inventaire par item et par location

DROP TABLE IF EXISTS inventory CASCADE;

CREATE TABLE inventory (
    -- Clés primaires composites
    inventory_item_id BIGINT NOT NULL,
    location_id BIGINT NOT NULL,
    
    -- IDs de référence et SKU
    variant_id BIGINT,
    product_id BIGINT,
    sku VARCHAR(255),
    
    -- Quantités par type (8 types différents observés dans les données)
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
    
    -- Timestamps de gestion
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Contrainte unique sur le couple (inventory_item_id, location_id)
    CONSTRAINT inventory_pkey PRIMARY KEY (inventory_item_id, location_id),
    
    -- Contraintes de clés étrangères
    CONSTRAINT fk_inventory_location 
        FOREIGN KEY (location_id) 
        REFERENCES locations(_location_id) 
        ON DELETE CASCADE,
    
    -- Index pour les requêtes fréquentes
    CONSTRAINT chk_quantities_non_negative CHECK (
        available >= 0 AND 
        committed >= 0 AND 
        damaged >= 0 AND 
        incoming >= 0 AND 
        on_hand >= 0 AND 
        quality_control >= 0 AND 
        reserved >= 0 AND 
        safety_stock >= 0
    )
);

-- Index pour optimiser les requêtes
CREATE INDEX idx_inventory_location_id ON inventory(location_id);
CREATE INDEX idx_inventory_variant_id ON inventory(variant_id) WHERE variant_id IS NOT NULL;
CREATE INDEX idx_inventory_product_id ON inventory(product_id) WHERE product_id IS NOT NULL;
CREATE INDEX idx_inventory_sku ON inventory(sku) WHERE sku IS NOT NULL;
CREATE INDEX idx_inventory_updated_at ON inventory(updated_at);

-- Index composites pour les requêtes d'agrégation
CREATE INDEX idx_inventory_available_by_location ON inventory(location_id, available) WHERE available > 0;
CREATE INDEX idx_inventory_on_hand_by_product ON inventory(product_id, on_hand) WHERE on_hand > 0;

-- Trigger pour mettre à jour automatiquement updated_at
CREATE OR REPLACE FUNCTION update_inventory_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_inventory_updated_at
    BEFORE UPDATE ON inventory
    FOR EACH ROW
    EXECUTE FUNCTION update_inventory_updated_at();

-- Commentaires sur la table et les colonnes
COMMENT ON TABLE inventory IS 'Table d''inventaire avec quantités par inventory_item_id et location_id. Contrainte unique sur le couple (inventory_item_id, location_id).';

COMMENT ON COLUMN inventory.inventory_item_id IS 'ID unique de l''item d''inventaire Shopify';
COMMENT ON COLUMN inventory.location_id IS 'ID de la location (référence vers la table locations)';
COMMENT ON COLUMN inventory.variant_id IS 'ID du variant Shopify associé (référence vers table products ou variants)';
COMMENT ON COLUMN inventory.product_id IS 'ID du produit Shopify associé (référence vers table products)';
COMMENT ON COLUMN inventory.sku IS 'SKU du produit pour identification rapide';

COMMENT ON COLUMN inventory.available IS 'Quantité disponible à la vente';
COMMENT ON COLUMN inventory.committed IS 'Quantité réservée pour des commandes';
COMMENT ON COLUMN inventory.damaged IS 'Quantité endommagée';
COMMENT ON COLUMN inventory.incoming IS 'Quantité en cours de réception';
COMMENT ON COLUMN inventory.on_hand IS 'Quantité physiquement présente';
COMMENT ON COLUMN inventory.quality_control IS 'Quantité en contrôle qualité';
COMMENT ON COLUMN inventory.reserved IS 'Quantité réservée (autre que committed)';
COMMENT ON COLUMN inventory.safety_stock IS 'Stock de sécurité';

COMMENT ON COLUMN inventory.last_updated_at IS 'Timestamp de la dernière mise à jour des quantités depuis Shopify';
COMMENT ON COLUMN inventory.scheduled_changes IS 'Changements programmés au format JSON';
COMMENT ON COLUMN inventory.synced_at IS 'Timestamp de la dernière synchronisation avec Shopify';
