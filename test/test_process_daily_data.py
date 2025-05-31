#!/usr/bin/env python3
# test_process_daily_data.py

import requests
import os
import json
from datetime import datetime
import argparse
from dotenv import load_dotenv
import logging
import sys

# Ajouter le répertoire parent au chemin pour pouvoir importer les modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.lib.database import check_and_update_order

# Charger les variables d'environnement depuis .env
load_dotenv()

def parse_arguments():
    parser = argparse.ArgumentParser(description='Test de la fonction serverless process_daily_data')
    parser.add_argument('--url', type=str, default='http://localhost:3001/api/process_daily_data.py',
                        help='URL de la fonction serverless')
    parser.add_argument('--custom-secret', type=str, help='Secret personnalisé pour l\'authentification')
    parser.add_argument('--debug', action='store_true', help='Activer le mode debug pour voir plus de logs')
    parser.add_argument('--test-db', action='store_true', help='Tester directement la fonction check_and_update_order')
    parser.add_argument('--real-data', action='store_true', help='Utiliser de vraies données du jour')
    return parser.parse_args()

def test_api(url, secret):
    """
    Teste l'API en envoyant une requête avec le secret d'authentification
    """
    print(f"Envoi d'une requête à {url}")
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {secret}'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        print(f"Statut de la réponse: {response.status_code}")
        
        if response.status_code == 200:
            try:
                result = response.json()
                print("Réponse JSON reçue:")
                print(json.dumps(result, indent=2))
                return True
            except json.JSONDecodeError:
                print("La réponse n'est pas au format JSON:")
                print(response.text)
                return False
        else:
            print(f"Erreur: {response.status_code}")
            print(response.text)
            return False
    
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête: {e}")
        return False

def get_real_data():
    """
    Récupère de vraies données du jour depuis l'API ou un fichier local
    """
    # Essayer d'abord de charger depuis un fichier local pour le test
    try:
        data_file = os.path.join(os.path.dirname(__file__), 'sample_data.json')
        if os.path.exists(data_file):
            with open(data_file, 'r') as f:
                data = json.load(f)
                print(f"Données chargées depuis {data_file}")
                return data
    except Exception as e:
        print(f"Erreur lors du chargement des données depuis le fichier: {e}")
    
    # Si pas de fichier local, créer des données de test réalistes
    today = datetime.now().strftime("%Y-%m-%d")
    return [
        {
            "id": 12345,  # Utiliser un entier pour l'ID, pas une chaîne
            "order_number": "ORD-2023-001",
            "customer_name": "Client Réel",
            "order_date": today,
            "total_amount": 1250.75,
            "status": "confirmed",
            "payment_method": "credit_card",
            "shipping_address": "123 Rue Principale, Paris, 75001",
            "items_count": 3
        },
        {
            "id": 12346,
            "order_number": "ORD-2023-002",
            "customer_name": "Autre Client",
            "order_date": today,
            "total_amount": 850.25,
            "status": "processing",
            "payment_method": "bank_transfer",
            "shipping_address": "456 Avenue des Champs, Lyon, 69001",
            "items_count": 2
        }
    ]

def test_check_and_update_order(use_real_data=False):
    """
    Test direct de la fonction check_and_update_order
    """
    print("Test direct de la fonction check_and_update_order")
    
    # Connexion à la base de données
    import psycopg2
    
    # Récupérer les informations de connexion
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        user = os.environ.get("SUPABASE_USER")
        password = os.environ.get("SUPABASE_PASSWORD")
        host = os.environ.get("SUPABASE_HOST")
        port = os.environ.get("SUPABASE_PORT")
        dbname = os.environ.get("SUPABASE_DB_NAME")
        
        if not all([user, password, host, port, dbname]):
            print("Erreur: Informations de connexion à la base de données incomplètes")
            return False
            
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        # Établir la connexion
        print("Connexion à la base de données...")
        conn = psycopg2.connect(db_url, connect_timeout=10)
        cur = conn.cursor()
        
        # Récupérer les types de colonnes
        print("Récupération des types de colonnes...")
        query = """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'orders'
        """
        
        cur.execute(query)
        columns_info = cur.fetchall()
        
        if not columns_info:
            print("Erreur: Impossible de récupérer les informations sur les colonnes de la table 'orders'")
            return False
        
        # Créer un dictionnaire des types de colonnes
        column_types = {}
        for column_name, data_type, max_length in columns_info:
            column_types[column_name] = {
                'type': data_type,
                'max_length': max_length
            }
        
        # Afficher les types de colonnes pour le débogage
        print("\nTypes de colonnes dans la table 'orders':")
        for col, info in column_types.items():
            print(f"  {col}: {info['type']}" + (f" (max: {info['max_length']})" if info['max_length'] else ""))
        
        # Données de test
        if use_real_data:
            test_orders = get_real_data()
            print(f"\nUtilisation de {len(test_orders)} commandes réelles pour le test")
        else:
            # Utiliser des données de test avec des ID numériques
            test_orders = [
                {
                    "id": 99999,  # ID numérique pour éviter l'erreur de type
                    "customer_name": "Test Customer",
                    "order_date": "2023-05-15",
                    "total_amount": 150.75,
                    "status": "pending"
                },
                {
                    "id": 99999,  # Même ID pour tester la mise à jour
                    "customer_name": "Test Customer Updated",
                    "order_date": "2023-05-15",
                    "total_amount": 175.50,
                    "status": "processing"
                }
            ]
        
        # Tester chaque commande
        for i, order in enumerate(test_orders):
            print(f"\n--- TEST COMMANDE {i+1}: {order.get('id')} ---")
            
            # Vérifier si la commande existe et si une mise à jour est nécessaire
            try:
                result = check_and_update_order(cur, order, column_types)
                print(f"Résultat: {'Action nécessaire (insertion ou mise à jour)' if result else 'Aucune action nécessaire'}")
                
                # Si une action est nécessaire et que c'est la première commande, l'insérer
                if result and i == 0:
                    try:
                        # Vérifier si la commande existe déjà
                        cur.execute("SELECT 1 FROM orders WHERE id = %s", (order['id'],))
                        exists = cur.fetchone()
                        
                        if exists:
                            print(f"La commande avec ID {order['id']} existe déjà, pas besoin d'insertion")
                        else:
                            # Insérer la commande
                            columns = []
                            values = []
                            placeholders = []
                            
                            for key, value in order.items():
                                column_name = key.lower().replace(" ", "_")
                                if column_name in column_types:
                                    columns.append(column_name)
                                    values.append(value)
                                    placeholders.append("%s")
                            
                            sql = f"INSERT INTO orders ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                            cur.execute(sql, values)
                            print(f"Insertion réussie pour la commande {order['id']}")
                    except Exception as e:
                        print(f"Erreur lors de l'insertion: {e}")
                        conn.rollback()  # Annuler la transaction en cas d'erreur
            except Exception as e:
                print(f"Erreur lors du test de la commande {order.get('id')}: {e}")
                conn.rollback()  # Annuler la transaction en cas d'erreur
        
        # Nettoyage - supprimer les enregistrements de test (seulement pour les données de test, pas les vraies données)
        if not use_real_data:
            print("\n--- NETTOYAGE ---")
            try:
                cur.execute("DELETE FROM orders WHERE id = %s", (test_orders[0]["id"],))
                print(f"Enregistrement de test supprimé")
            except Exception as e:
                print(f"Erreur lors de la suppression: {e}")
        
        # Valider les changements
        conn.commit()
        print("Test terminé avec succès")
        
        return True
        
    except Exception as e:
        print(f"Erreur lors du test: {e}")
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

def main():
    args = parse_arguments()
    
    # Configurer le niveau de logging
    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Récupérer le secret
    if args.custom_secret:
        secret = args.custom_secret
    else:
        secret = os.environ.get('CRON_SECRET')
        if not secret and not args.test_db:
            print("Erreur: Aucun secret trouvé. Utilisez --custom-secret ou définissez CRON_SECRET dans .env")
            return False
    
    # Tester directement la fonction check_and_update_order si demandé
    if args.test_db:
        return test_check_and_update_order(use_real_data=args.real_data)
    
    # Sinon, tester l'API
    return test_api(args.url, secret)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 