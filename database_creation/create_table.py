import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_orders_table():
    """
    Crée une table dans Supabase pour stocker les commandes Shopify
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
        drop_table_query = "DROP TABLE IF EXISTS orders CASCADE;"
        cur.execute(drop_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS orders (
            _id_order TEXT PRIMARY KEY,
            _id_customer BIGINT,
            order_label VARCHAR(255),
            app_id BIGINT,
            confirmed VARCHAR(50),
            financial_status VARCHAR(50),
            fulfillment_status VARCHAR(50),
            location_id BIGINT,
            contact_email VARCHAR(255),
            created_at TIMESTAMP,
            currency VARCHAR(10),
            origin_total_orders DECIMAL(15, 2),
            gross_sales DECIMAL(15, 2),
            returns DECIMAL(15, 2),
            returns_excl_taxes DECIMAL(15, 2),
            discount DECIMAL(15, 2),
            taxes DECIMAL(15, 2),
            shipping DECIMAL(15, 2),
            current_total_orders DECIMAL(15, 2),
            net_sales DECIMAL(15, 2),
            current_subtotal_price DECIMAL(15, 2),
            net_sales_check BOOLEAN,
            tax1_name VARCHAR(100),
            tax1_rate DECIMAL(15, 2),
            tax1_value_origin DECIMAL(15, 2),
            tax2_name VARCHAR(100),
            tax2_rate DECIMAL(15, 2),
            tax2_value_origin DECIMAL(15, 2),
            tax3_name VARCHAR(100),
            tax3_rate DECIMAL(15, 2),
            tax3_value_origin DECIMAL(15, 2),
            tax4_name VARCHAR(100),
            tax4_rate DECIMAL(15, 2),
            tax4_value_origin DECIMAL(15, 2),
            tax5_name VARCHAR(100),
            tax5_rate DECIMAL(15, 2),
            tax5_value_origin DECIMAL(15, 2),
            tax_check BOOLEAN,
            discount_codes TEXT,
            customer_locale VARCHAR(20),
            note TEXT,
            tags TEXT,
            landing_site TEXT,
            referring_site TEXT,
            source_name TEXT,
            billing_first_name VARCHAR(100),
            billing_address1 TEXT,
            billing_phone VARCHAR(50),
            billing_city VARCHAR(100),
            billing_zip VARCHAR(20),
            billing_province VARCHAR(100),
            billing_country VARCHAR(100),
            billing_last_name VARCHAR(100),
            billing_address2 TEXT,
            billing_company TEXT,
            billing_latitude DECIMAL(10, 6),
            billing_longitude DECIMAL(10, 6),
            billing_name VARCHAR(200),
            billing_country_code VARCHAR(10),
            billing_province_code VARCHAR(20),
            shipping_first_name VARCHAR(100),
            shipping_address1 TEXT,
            shipping_phone VARCHAR(50),
            shipping_city VARCHAR(100),
            shipping_zip VARCHAR(20),
            shipping_province VARCHAR(100),
            shipping_country VARCHAR(100),
            shipping_last_name VARCHAR(100),
            shipping_address2 TEXT,
            shipping_company TEXT,
            shipping_latitude DECIMAL(10, 6),
            shipping_longitude DECIMAL(10, 6),
            shipping_name VARCHAR(200),
            shipping_country_code VARCHAR(10),
            shipping_province_code VARCHAR(20),
            shipping_total_weight DECIMAL(15, 2),
            shipment_status VARCHAR(50),
            tracking_company VARCHAR(100),
            tracking_number VARCHAR(100),
            
            created_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Créer un index sur _id_customer pour améliorer les performances des requêtes
        CREATE INDEX idx_orders_customer_id ON orders(_id_customer);
        
        -- Créer un index sur created_at pour les requêtes de date
        CREATE INDEX idx_orders_created_at ON orders(created_at);
        """
        
        # Exécuter la requête
        cur.execute(create_table_query)
        
        # Valider la transaction
        conn.commit()
        
        print("Table 'orders' créée avec succès!")
        
    except Exception as e:
        print(f"Erreur lors de la création de la table: {e}")
    
    finally:
        # Fermer le curseur et la connexion
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_orders_table()