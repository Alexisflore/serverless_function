import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()

def get_db_connection():
    """
    Établit une connexion à la base de données PostgreSQL
    """
    db_url = os.getenv("DATABASE_URL")
    
    # Alternative: utiliser les paramètres individuels si DATABASE_URL n'est pas disponible
    if not db_url:
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    print(f"Connexion à la base de données avec l'URL: {db_url}")
    return psycopg2.connect(db_url)

def get_column_types():
    """
    Récupère les types de colonnes de la table orders
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Requête pour obtenir les informations sur les colonnes
        query = """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'orders'
        """
        
        cur.execute(query)
        columns_info = cur.fetchall()
        
        # Créer un dictionnaire des types de colonnes
        column_types = {}
        for column_name, data_type, max_length in columns_info:
            column_types[column_name] = {
                'type': data_type,
                'max_length': max_length
            }
            print(f"Colonne: {column_name}, Type: {data_type}, Max Length: {max_length}")
        
        return column_types
    
    except Exception as e:
        print(f"Erreur lors de la récupération des types de colonnes: {e}")
        return {}
    
    finally:
        if conn:
            conn.close()

def insert_test_order():
    """
    Insère une commande de test dans la table orders
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Récupérer les types de colonnes
        column_types = get_column_types()
        
        # Créer une commande de test simple avec des valeurs contrôlées
        now = datetime.now().isoformat()
        
        # Définir les colonnes et les valeurs
        test_order = {
            "name": "#TEST1001",
            "email": "test@example.com",
            "financial_status": "paid",
            "paid_at": now,
            "created_at": now,
            "total": 100.00,
            "subtotal": 90.00,
            "shipping": 5.00,
            "taxes": 5.00,
            "currency": "USD",
            "discount_code": "TEST10",
            "discount_amount": 10.00,
            "billing_name": "John Doe",
            "billing_address1": "123 Test St",
            "billing_city": "New York",
            "billing_zip": "10001",
            "billing_province": "NY",
            "billing_country": "US",
            "billing_phone": "555-123-4567",
            "shipping_name": "John Doe",
            "shipping_address1": "123 Test St",
            "shipping_city": "New York",
            "shipping_zip": "10001",
            "shipping_province": "NY",
            "shipping_country": "US",
            "shipping_phone": "555-123-4567",
            "lineitem_quantity": 1,
            "lineitem_name": "Test Product",
            "lineitem_price": 100.00,
            "lineitem_sku": "SKU123"
        }
        
        # Préparer les colonnes et les valeurs pour l'insertion
        columns = []
        values = []
        placeholders = []
        
        for key, value in test_order.items():
            # Vérifier si la colonne existe dans la table
            if key not in column_types:
                print(f"Colonne {key} non trouvée dans la table, ignorée.")
                continue
            
            # Récupérer le type de la colonne
            column_info = column_types[key]
            column_type = column_info['type'].lower()
            max_length = column_info['max_length']
            
            # Traiter la valeur en fonction du type de colonne
            if value is None or value == "":
                # Convertir les valeurs vides en NULL pour tous les types sauf text
                if column_type != "text" and column_type != "character varying":
                    value = None
            elif column_type in ["numeric", "decimal", "real", "double precision"]:
                # Convertir en nombre si possible
                try:
                    value = float(value) if value is not None else None
                except (ValueError, TypeError):
                    value = None
            elif column_type in ["integer", "bigint", "smallint"]:
                # Convertir en entier si possible
                try:
                    value = int(value) if value is not None else None
                except (ValueError, TypeError):
                    value = None
            elif column_type in ["timestamp", "timestamp without time zone", "timestamp with time zone", "date"]:
                # Laisser les timestamps tels quels, ils seront gérés par psycopg2
                pass
            elif column_type == "boolean":
                # Convertir en booléen
                if isinstance(value, str):
                    value = value.lower() in ["true", "t", "yes", "y", "1"]
            elif column_type == "character varying" and max_length is not None:
                # Tronquer les chaînes trop longues
                if isinstance(value, str) and len(value) > max_length:
                    value = value[:max_length]
            
            columns.append(key)
            values.append(value)
            placeholders.append("%s")
        
        # Construire la requête SQL
        sql = f"INSERT INTO orders ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        print(f"Requête SQL: {sql}")
        print(f"Valeurs: {values}")
        
        # Exécuter la requête
        cur.execute(sql, values)
        
        # Valider la transaction
        conn.commit()
        print("Commande de test insérée avec succès!")
        
    except Exception as e:
        print(f"Erreur lors de l'insertion de la commande de test: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Début du test d'insertion simple...")
    insert_test_order()
    print("Test d'insertion simple terminé!") 