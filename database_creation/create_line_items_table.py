import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_line_items_table():
    """
    Crée une table line_items (obsolète) et orders_details dans Supabase 
    pour stocker les éléments de ligne des commandes Shopify.
    
    Note: La table line_items est maintenue pour compatibilité, mais orders_details
    est désormais la table principale pour les détails de commandes.
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
        
        # Supprimer la table line_items si elle existe déjà pour la recréer
        drop_table_query = "DROP TABLE IF EXISTS line_items;"
        cur.execute(drop_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table line_items (obsolète)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS line_items (
            id BIGINT PRIMARY KEY,
            order_id TEXT NOT NULL,
            admin_graphql_api_id TEXT,
            
            title TEXT,
            variant_title TEXT,
            name TEXT,
            quantity INTEGER,
            price DECIMAL(15, 2),
            pre_tax_price DECIMAL(15, 2),
            total_discount DECIMAL(15, 2),
            
            sku VARCHAR(100),
            variant_id BIGINT,
            variant_inventory_management VARCHAR(100),
            product_id BIGINT,
            product_exists BOOLEAN,
            
            fulfillment_service VARCHAR(100),
            fulfillment_status VARCHAR(50),
            fulfillable_quantity INTEGER,
            grams INTEGER,
            weight DECIMAL(15, 2),
            weight_unit VARCHAR(10),
            
            vendor VARCHAR(255),
            requires_shipping BOOLEAN,
            taxable BOOLEAN,
            gift_card BOOLEAN,
            
            properties JSONB,
            tax_lines JSONB,
            duties JSONB,
            discount_allocations JSONB,
            
            price_set JSONB,
            pre_tax_price_set JSONB,
            total_discount_set JSONB,
            
            current_quantity INTEGER,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_order
                FOREIGN KEY(order_id)
                REFERENCES orders(_id_order)
                ON DELETE CASCADE
        );
        
        -- Créer un index sur order_id pour améliorer les performances des requêtes
        CREATE INDEX idx_line_items_order_id ON line_items(order_id);
        
        -- Créer un index sur product_id pour les requêtes de produits
        CREATE INDEX idx_line_items_product_id ON line_items(product_id);
        
        -- Créer un index sur variant_id pour les requêtes de variantes
        CREATE INDEX idx_line_items_variant_id ON line_items(variant_id);
        """
        
        # Exécuter la requête
        cur.execute(create_table_query)
        
        # Créer la table orders_details (nouvelle table principale pour les détails de commandes)
        # Supprimer la table orders_details si elle existe déjà pour la recréer
        drop_details_table_query = "DROP TABLE IF EXISTS orders_details;"
        cur.execute(drop_details_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table orders_details
        create_details_table_query = """
        CREATE TABLE IF NOT EXISTS orders_details (
            _id_order_detail BIGINT PRIMARY KEY,
            _id_order TEXT NOT NULL,
            _id_product BIGINT,
            current_quantity INTEGER,
            fulfillable_quantity INTEGER,
            fulfillment_service VARCHAR(100),
            fulfillment_status VARCHAR(50),
            gift_card BOOLEAN,
            grams INTEGER,
            name TEXT,
            pre_tax_price DECIMAL(15, 2),
            price DECIMAL(15, 2),
            product_exists BOOLEAN,
            origin_quantity INTEGER,
            requires_shipping BOOLEAN,
            sku VARCHAR(100),
            taxable BOOLEAN,
            title TEXT,
            total_discount DECIMAL(15, 2),
            variant_id BIGINT,
            variant_inventory_management VARCHAR(100),
            variant_title TEXT,
            vendor VARCHAR(255),
            
            tax1_name TEXT,
            tax1_rate DECIMAL(10, 6),
            tax1_value DECIMAL(15, 2),
            
            tax2_name TEXT,
            tax2_rate DECIMAL(10, 6),
            tax2_value DECIMAL(15, 2),
            
            tax3_name TEXT,
            tax3_rate DECIMAL(10, 6),
            tax3_value DECIMAL(15, 2),
            
            tax4_name TEXT,
            tax4_rate DECIMAL(10, 6),
            tax4_value DECIMAL(15, 2),
            
            tax5_name TEXT,
            tax5_rate DECIMAL(10, 6),
            tax5_value DECIMAL(15, 2),
            
            total_taxes DECIMAL(15, 2),
            amount_gross_sales DECIMAL(15, 2),
            amount_returns DECIMAL(15, 2),
            amount_discounts DECIMAL(15, 2),
            amount_net_sales DECIMAL(15, 2),
            amount_net_sales_check BOOLEAN,
            return_check BOOLEAN,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT fk_order_details
                FOREIGN KEY(_id_order)
                REFERENCES orders(_id_order)
                ON DELETE CASCADE
        );
        
        -- Créer un index sur _id_order pour améliorer les performances des requêtes
        CREATE INDEX idx_orders_details_order_id ON orders_details(_id_order);
        
        -- Créer un index sur _id_product pour les requêtes de produits
        CREATE INDEX idx_orders_details_product_id ON orders_details(_id_product);
        
        -- Créer un index sur variant_id pour les requêtes de variantes
        CREATE INDEX idx_orders_details_variant_id ON orders_details(variant_id);
        """
        
        # Exécuter la requête
        cur.execute(create_details_table_query)
        
        # Valider la transaction
        conn.commit()
        
        print("Tables 'line_items' et 'orders_details' créées avec succès!")
        print("Note: 'line_items' est maintenue pour la compatibilité, mais 'orders_details' est désormais la table principale pour les détails des commandes.")
        
    except Exception as e:
        print(f"Erreur lors de la création des tables: {e}")
    
    finally:
        # Fermer le curseur et la connexion
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_line_items_table() 