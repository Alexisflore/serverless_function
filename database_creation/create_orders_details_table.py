import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_orders_details_table():
    """
    Crée une table orders_details dans Supabase pour stocker les détails des commandes Shopify
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
        
        # Supprimer la table si elle existe déjà pour la recréer
        drop_table_query = "DROP TABLE IF EXISTS orders_details;"
        cur.execute(drop_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table orders_details
        create_table_query = """
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
            
            CONSTRAINT fk_order
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
        cur.execute(create_table_query)
        
        # Valider la transaction
        conn.commit()
        
        print("Table 'orders_details' créée avec succès!")
        
    except Exception as e:
        print(f"Erreur lors de la création de la table orders_details: {e}")
    
    finally:
        # Fermer le curseur et la connexion
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_orders_details_table() 