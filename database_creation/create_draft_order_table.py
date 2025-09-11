import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_draft_order_table():
    """
    Crée une table dans Supabase pour stocker les transactions des draft orders
    avec la nouvelle structure
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
        print("Suppression de la table existante si elle existe...")
        drop_table_query = "DROP TABLE IF EXISTS draft_order CASCADE;"
        cur.execute(drop_table_query)
        conn.commit()
        
        # Création de la requête SQL pour créer la table
        print("Création de la nouvelle table draft_order...")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS draft_order (
            id SERIAL PRIMARY KEY,
            _draft_id BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            order_id BIGINT,
            client_id BIGINT NOT NULL,
            product_id BIGINT,
            type VARCHAR(50) NOT NULL,
            account_type VARCHAR(50) NOT NULL,
            transaction_description TEXT NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            status VARCHAR(50),
            transaction_currency VARCHAR(3) NOT NULL,
            source_name VARCHAR(50),
            created_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Créer un index sur _draft_id pour améliorer les performances des requêtes
        CREATE INDEX idx_draft_order_draft_id ON draft_order(_draft_id);
        
        -- Créer un index sur order_id pour les requêtes liées aux commandes
        CREATE INDEX idx_draft_order_order_id ON draft_order(order_id);
        
        -- Créer un index sur client_id pour les requêtes par client
        CREATE INDEX idx_draft_order_client_id ON draft_order(client_id);
        
        -- Créer un index sur created_at pour les requêtes de date de création
        CREATE INDEX idx_draft_order_created_at ON draft_order(created_at);
        
        -- Créer un index sur completed_at pour les requêtes de date de complétion
        CREATE INDEX idx_draft_order_completed_at ON draft_order(completed_at);
        
        -- Créer un index sur account_type pour filtrer par type de transaction
        CREATE INDEX idx_draft_order_account_type ON draft_order(account_type);
        
        -- Créer un index sur product_id
        CREATE INDEX idx_draft_order_product_id ON draft_order(product_id);
        
        -- Créer un index sur type
        CREATE INDEX idx_draft_order_type ON draft_order(type);
        """
        
        # Exécuter la requête
        cur.execute(create_table_query)
        
        # Valider la transaction
        conn.commit()
        
        print("Table 'draft_order' créée avec succès!")
        
    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la création de la table: {e}")
    
    finally:
        # Fermer le curseur et la connexion
        if 'cur' in locals():
            print("Fermeture du curseur...")
            cur.close()
        if 'conn' in locals():
            print("Fermeture de la connexion...")
            conn.close()

if __name__ == "__main__":
    create_draft_order_table()
