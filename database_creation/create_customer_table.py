#!/usr/bin/env python3
"""
Script pour créer la table customers dans Supabase pour stocker les données clients Shopify
"""

import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_customer_table():
    """
    Crée une table dans Supabase pour stocker les clients Shopify
    """
    # Récupérer les informations de connexion depuis les variables d'environnement
    db_url = os.getenv("DATABASE_URL")

    # Alternative: utiliser les paramètres individuels si DATABASE_URL n'est pas disponible
    if not db_url:
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    try:
        # Connexion à la base de données
        conn = psycopg2.connect(db_url)

        # Création d'un curseur
        cur = conn.cursor()

        # Supprimer la table si elle existe déjà pour la recréer avec les nouveaux types
        print("Suppression de la table customers existante si elle existe...")
        drop_table_query = "DROP TABLE IF EXISTS customers CASCADE;"
        cur.execute(drop_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table
        print("Création de la nouvelle table customers...")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS customers (
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
        
        -- Créer un index unique sur customer_id (clé primaire métier)
        CREATE UNIQUE INDEX idx_customers_customer_id ON customers(customer_id);
        
        -- Créer un index unique sur gid (GraphQL ID)
        CREATE UNIQUE INDEX idx_customers_gid ON customers(gid);
        
        -- Créer un index sur email pour améliorer les performances des requêtes
        CREATE INDEX idx_customers_email ON customers(email);
        
        -- Créer un index sur created_at pour les requêtes de date
        CREATE INDEX idx_customers_created_at ON customers(created_at);
        
        -- Créer un index sur shop_updated_at pour les requêtes de synchronisation
        CREATE INDEX idx_customers_shop_updated_at ON customers(shop_updated_at);
        
        -- Créer un index sur synced_at pour les requêtes de synchronisation
        CREATE INDEX idx_customers_synced_at ON customers(synced_at);
        
        -- Créer un index sur number_of_orders pour les requêtes de segmentation
        CREATE INDEX idx_customers_number_of_orders ON customers(number_of_orders);
        
        -- Créer un index sur amount_spent pour les requêtes de valeur client
        CREATE INDEX idx_customers_amount_spent ON customers(amount_spent);
        
        -- Créer un index GIN sur addresses pour les requêtes JSON
        CREATE INDEX idx_customers_addresses_gin ON customers USING GIN (addresses);
        
        -- Créer un index sur tags pour les requêtes de segmentation
        CREATE INDEX idx_customers_tags ON customers USING GIN (to_tsvector('english', tags));
        """
        
        # Exécuter la requête
        cur.execute(create_table_query)
        
        # Valider les changements
        conn.commit()
        
        print("✅ Table 'customers' créée avec succès!")
        print("📊 Structure de la table:")
        print("   - customer_id: BIGINT (clé unique, ID Shopify)")
        print("   - gid: VARCHAR(255) (GraphQL ID)")
        print("   - first_name, last_name, display_name: VARCHAR")
        print("   - email: VARCHAR(255)")
        print("   - phone: VARCHAR(50)")
        print("   - number_of_orders: INTEGER")
        print("   - amount_spent: DECIMAL(15,2)")
        print("   - amount_spent_currency: VARCHAR(10)")
        print("   - created_at, shop_updated_at: TIMESTAMP")
        print("   - tags, note: TEXT")
        print("   - verified_email, valid_email_address: BOOLEAN")
        print("   - addresses: JSONB")
        print("   - synced_at, updated_at, created_at_timestamp: TIMESTAMP")
        print("🔍 Index créés pour optimiser les performances")
        
    except psycopg2.Error as e:
        print(f"❌ Erreur lors de la création de la table: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        if conn:
            conn.rollback()
    finally:
        # Fermer les connexions
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("🔐 Connexion fermée")

if __name__ == "__main__":
    print("=== Création de la table customers ===")
    create_customer_table()
