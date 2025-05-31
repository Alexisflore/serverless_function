import os
import json
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from api.process_daily_data import get_daily_orders, process_orders, get_dates, get_supabase_client

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
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'orders'
        """
        
        cur.execute(query)
        columns_info = cur.fetchall()
        
        # Créer un dictionnaire des types de colonnes
        column_types = {}
        for column_name, data_type in columns_info:
            column_types[column_name] = data_type
        
        return column_types
    
    except Exception as e:
        print(f"Erreur lors de la récupération des types de colonnes: {e}")
        return {}
    
    finally:
        if conn:
            conn.close()

def insert_order_with_psycopg2(processed_data):
    """
    Insère les données de commande dans la table orders en utilisant psycopg2
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Récupérer les types de colonnes
        column_types = get_column_types()
        print(f"Types de colonnes récupérés: {len(column_types)} colonnes")
        
        for order in processed_data:
            # Préparer les colonnes et les valeurs pour l'insertion
            columns = []
            values = []
            placeholders = []
            try:
                for key, value in order.items():
                        # Convertir les clés au format de la base de données (minuscules avec underscores)
                        column_name = key.lower().replace(" ", "_")
                            
                        # Récupérer le type de la colonne
                        column_type = column_types.get(column_name, "text").lower()
                        
                        # Traiter la valeur en fonction du type de colonne
                        if value == "":
                            # Convertir les chaînes vides en NULL pour tous les types sauf text
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
                        
                        columns.append(column_name)
                        values.append(value)
                        placeholders.append("%s")
                # Construire la requête SQL
                sql = f"INSERT INTO orders ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                # Exécuter la requête
                cur.execute(sql, values)
            except Exception as e:
                print(f"Erreur {order}, {e}, values: {', '.join(placeholders)}")
                raise e

        # Valider la transaction
        conn.commit()
        print(f"{len(processed_data)} commandes insérées avec succès via psycopg2!")
        
    except Exception as e:
        print(f"Erreur lors de l'insertion des données via psycopg2: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def insert_order_with_supabase(processed_data):
    """
    Insère les données de commande dans la table orders en utilisant directement psycopg2
    au lieu de l'API Supabase, car nous n'avons pas l'URL correcte de l'API Supabase.
    """
    print("Note: Utilisation de psycopg2 au lieu de l'API Supabase car l'URL de l'API Supabase n'est pas correctement configurée.")
    insert_order_with_psycopg2(processed_data)
    return None

def main():
    """
    Fonction principale pour tester l'insertion d'une commande
    """
    print("Début du test d'insertion de commande...")
    
    # 1. Récupérer les commandes d'hier
    start_date, end_date = get_dates()
    print(f"Récupération des commandes du {start_date} au {end_date}...")
    orders = get_daily_orders(start_date, end_date)
    
    if not orders:
        print("Aucune commande trouvée pour cette période.")
        # Créer une commande de test si aucune n'est trouvée
        orders = [{
            "id": 12345,
            "name": "#TEST1001",
            "email": "test@example.com",
            "financial_status": "paid",
            "processed_at": datetime.now().isoformat(),
            "fulfillment_status": "fulfilled",
            "buyer_accepts_marketing": True,
            "currency": "USD",
            "subtotal_price": "100.00",
            "total_shipping_price_set": {"shop_money": {"amount": "10.00"}},
            "current_total_tax": "8.50",
            "total_price": "118.50",
            "discount_codes": [{"code": "TEST10"}],
            "total_discounts": "10.00",
            "shipping_lines": [{"title": "Standard Shipping"}],
            "created_at": datetime.now().isoformat(),
            "billing_address": {
                "name": "John Doe",
                "address1": "123 Test St",
                "address2": "Apt 4B",
                "company": "Test Company",
                "city": "New York",
                "zip": "10001",
                "province": "NY",
                "country": "US",
                "phone": "555-123-4567"
            },
            "shipping_address": {
                "name": "John Doe",
                "address1": "123 Test St",
                "address2": "Apt 4B",
                "company": "Test Company",
                "city": "New York",
                "zip": "10001",
                "province": "NY",
                "country": "US",
                "phone": "555-123-4567"
            },
            "note": "Test order",
            "tags": "test, example",
            "source_name": "web",
            "phone": "555-123-4567",
            "note_attributes": [{"name": "Gift", "value": "Yes"}],
            "cancelled_at": None,
            "payment_gateway_names": ["credit_card"],
            "reference": "REF12345",
            "refunds": [],
            "total_outstanding": "0.00",
            "location_id": "L123",
            "device_id": "D123",
            "confirmation_number": "CN12345",
            "current_total_duties_set": {"shop_money": {"amount": "0.00"}},
            "payment_terms": {"name": "Net 30"},
            "transactions": [{"payment_id": "PAY123", "authorization": "AUTH123"}],
            "line_items": [{
                "quantity": 1,
                "name": "Test Product",
                "price": "100.00",
                "compare_at_price": "120.00",
                "sku": "SKU123",
                "requires_shipping": True,
                "taxable": True,
                "fulfillment_status": "fulfilled",
                "vendor": "Test Vendor",
                "total_discount": "10.00"
            }],
            "tax_lines": []
        }]
    
    # 2. Traiter les commandes
    print(f"Traitement de {len(orders)} commandes...")
    processed_data = process_orders(orders)
    
    # 3. Insérer les commandes avec psycopg2
    print("Insertion des commandes avec psycopg2...")
    insert_order_with_psycopg2(processed_data)
    
    # 4. Insérer les commandes avec l'API Supabase
    print("Insertion des commandes avec l'API Supabase...")
    insert_order_with_supabase(processed_data)
    
    print("Test d'insertion terminé!")

if __name__ == "__main__":
    main() 