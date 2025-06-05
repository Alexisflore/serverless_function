"""
Script pour ajouter la colonne variant_id à la table transaction.
"""

import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

def get_db_connection():
    """Établit la connexion à la base de données PostgreSQL."""
    load_dotenv()
    
    # Essayer d'abord avec DATABASE_URL
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)
    
    # Sinon, construire l'URL à partir des variables individuelles
    db_url = "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
        user=os.getenv("SUPABASE_USER"),
        pw=os.getenv("SUPABASE_PASSWORD"),
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT"),
        db=os.getenv("SUPABASE_DB_NAME"),
    )
    return psycopg2.connect(db_url)

def check_column_exists(cursor, table_name, column_name):
    """Vérifie si une colonne existe dans une table."""
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s;
    """
    cursor.execute(query, (table_name, column_name))
    return cursor.fetchone() is not None

def add_variant_id_column():
    """Ajoute la colonne variant_id à la table transaction."""
    
    print("Connexion à la base de données...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Vérifier si la colonne existe déjà
        if check_column_exists(cursor, 'transaction', 'variant_id'):
            print("La colonne 'variant_id' existe déjà dans la table 'transaction'.")
            return
        
        print("Ajout de la colonne 'variant_id' à la table 'transaction'...")
        
        # Ajouter la colonne variant_id (BIGINT car les IDs Shopify sont des entiers 64-bit)
        alter_query = """
            ALTER TABLE transaction 
            ADD COLUMN variant_id BIGINT;
        """
        
        cursor.execute(alter_query)
        
        # Ajouter un commentaire pour documenter la colonne
        comment_query = """
            COMMENT ON COLUMN transaction.variant_id 
            IS 'ID du variant Shopify associé à cette transaction (null pour les transactions non liées à un produit spécifique)';
        """
        
        cursor.execute(comment_query)
        
        # Valider les changements
        conn.commit()
        print("✅ Colonne 'variant_id' ajoutée avec succès à la table 'transaction'.")
        
        # Afficher la structure de la table pour confirmation
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'transaction' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("\n📋 Structure actuelle de la table 'transaction':")
        print("=" * 50)
        for col_name, data_type, is_nullable in columns:
            nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
            print(f"  {col_name:<25} {data_type:<15} {nullable}")
        
    except psycopg2.Error as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Erreur générale: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("\nConnexion à la base de données fermée.")

if __name__ == "__main__":
    print("=== Ajout de la colonne variant_id à la table transaction ===")
    add_variant_id_column()
    print("=== Migration terminée ===") 