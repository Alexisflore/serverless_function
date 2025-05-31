import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def add_payment_method_column():
    """
    Ajoute une colonne payment_method_name à la table transaction existante
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
        
        # Vérifier si la colonne existe déjà
        check_column_query = """
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'transaction' AND column_name = 'payment_method_name';
        """
        cur.execute(check_column_query)
        
        if not cur.fetchone():
            # Ajout de la colonne payment_method_name
            alter_table_query = """
            ALTER TABLE transaction 
            ADD COLUMN payment_method_name VARCHAR(100);
            """
            
            # Exécuter la requête
            cur.execute(alter_table_query)
            
            # Valider la transaction
            conn.commit()
            
            print("Colonne 'payment_method_name' ajoutée avec succès à la table 'transaction'!")
        else:
            print("La colonne 'payment_method_name' existe déjà dans la table 'transaction'.")
        
    except Exception as e:
        print(f"Erreur lors de l'ajout de la colonne: {e}")
    
    finally:
        # Fermer le curseur et la connexion
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    add_payment_method_column() 