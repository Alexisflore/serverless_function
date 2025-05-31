import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_transaction_table():
    """
    Crée une table dans Supabase pour stocker les transactions
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
        drop_table_query = "DROP TABLE IF EXISTS transaction CASCADE;"
        cur.execute(drop_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS transaction (
            id SERIAL PRIMARY KEY,
            date TIMESTAMP NOT NULL,
            order_id BIGINT NOT NULL,
            client_id BIGINT NOT NULL,
            account_type VARCHAR(50) NOT NULL,
            transaction_description TEXT NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            transaction_currency VARCHAR(3) NOT NULL,
            location_id BIGINT,
            source_name VARCHAR(50),
            status VARCHAR(50),
            product_id BIGINT,
            created_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Créer un index sur order_id pour améliorer les performances des requêtes
        CREATE INDEX idx_transaction_order_id ON transaction(order_id);
        
        -- Créer un index sur client_id pour les requêtes par client
        CREATE INDEX idx_transaction_client_id ON transaction(client_id);
        
        -- Créer un index sur date pour les requêtes de date
        CREATE INDEX idx_transaction_date ON transaction(date);
        
        -- Créer un index sur account_type pour filtrer par type de transaction
        CREATE INDEX idx_transaction_account_type ON transaction(account_type);
        
        -- Créer un index sur location_id
        CREATE INDEX idx_transaction_location_id ON transaction(location_id);
        
        -- Créer un index sur product_id
        CREATE INDEX idx_transaction_product_id ON transaction(product_id);
        """
        
        # Exécuter la requête
        cur.execute(create_table_query)
        
        # Valider la transaction
        conn.commit()
        
        print("Table 'transaction' créée avec succès!")
        
    except Exception as e:
        print(f"Erreur lors de la création de la table: {e}")
    
    finally:
        # Fermer le curseur et la connexion
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_transaction_table()