-- Script SQL pour créer la table customers dans Supabase
-- Compatible avec le script process_customer.py

-- Supprimer la table si elle existe déjà
DROP TABLE IF EXISTS customers CASCADE;

-- Créer la table customers
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    customer_id BIGINT UNIQUE NOT NULL,
    gid VARCHAR(255) UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    display_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    number_of_orders INTEGER DEFAULT 0,
    amount_spent DECIMAL(15, 2),
    amount_spent_currency VARCHAR(10),
    created_at TIMESTAMP,
    shop_updated_at TIMESTAMP,
    tags TEXT,
    note TEXT,
    verified_email BOOLEAN DEFAULT FALSE,
    valid_email_address BOOLEAN DEFAULT FALSE,
    addresses JSONB DEFAULT '[]'::jsonb,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index unique sur customer_id (clé primaire métier)
CREATE UNIQUE INDEX idx_customers_customer_id ON customers(customer_id);

-- Index unique sur gid (GraphQL ID)
CREATE UNIQUE INDEX idx_customers_gid ON customers(gid);

-- Index sur email pour améliorer les performances des requêtes
CREATE INDEX idx_customers_email ON customers(email);

-- Index sur created_at pour les requêtes de date
CREATE INDEX idx_customers_created_at ON customers(created_at);

-- Index sur shop_updated_at pour les requêtes de synchronisation
CREATE INDEX idx_customers_shop_updated_at ON customers(shop_updated_at);

-- Index sur synced_at pour les requêtes de synchronisation
CREATE INDEX idx_customers_synced_at ON customers(synced_at);

-- Index sur number_of_orders pour les requêtes de segmentation
CREATE INDEX idx_customers_number_of_orders ON customers(number_of_orders);

-- Index sur amount_spent pour les requêtes de valeur client
CREATE INDEX idx_customers_amount_spent ON customers(amount_spent);

-- Index GIN sur addresses pour les requêtes JSON
CREATE INDEX idx_customers_addresses_gin ON customers USING GIN (addresses);

-- Index sur tags pour les requêtes de segmentation (recherche textuelle)
CREATE INDEX idx_customers_tags ON customers USING GIN (to_tsvector('english', tags));

-- Commentaires sur la table
COMMENT ON TABLE customers IS 'Table pour stocker les données clients synchronisées depuis Shopify';
COMMENT ON COLUMN customers.customer_id IS 'ID unique du client dans Shopify (legacyResourceId)';
COMMENT ON COLUMN customers.gid IS 'GraphQL ID global du client dans Shopify';
COMMENT ON COLUMN customers.addresses IS 'Adresses du client stockées en format JSON';
COMMENT ON COLUMN customers.amount_spent IS 'Montant total dépensé par le client';
COMMENT ON COLUMN customers.number_of_orders IS 'Nombre total de commandes du client';
COMMENT ON COLUMN customers.synced_at IS 'Timestamp de la dernière synchronisation';
COMMENT ON COLUMN customers.shop_updated_at IS 'Timestamp de la dernière mise à jour dans Shopify';
