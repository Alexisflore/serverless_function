#!/usr/bin/env python3
"""
Script pour traiter les transactions d'un ordre spécifique et mettre à jour la base de données
"""

import os
import sys
import json
import requests
from datetime import datetime
import traceback
from dotenv import load_dotenv

# Add parent directory to path to allow importing modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the modules
from api.lib.process_transactions import get_transactions_by_order, process_transactions
from api.lib.order_processor import process_orders

# Load environment variables from .env file
load_dotenv()

def get_shopify_headers():
    """Retourne les headers nécessaires pour l'API Shopify"""
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json"
    }

def get_single_order(order_id):
    """
    Récupère une commande spécifique par son ID depuis l'API Shopify
    
    Args:
        order_id (str): L'ID de l'ordre à récupérer
        
    Returns:
        dict: Les données de l'ordre ou None si erreur
    """
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}.json"
    headers = get_shopify_headers()
    
    print(f"🔍 Récupération de la commande {order_id} depuis Shopify...")
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            order_data = response.json()
            print(f"✅ Commande {order_id} récupérée avec succès")
            return order_data.get('order', {})
        else:
            print(f"❌ Erreur lors de la récupération de la commande {order_id}: {response.status_code}")
            print(f"   Réponse: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération de la commande {order_id}: {str(e)}")
        return None

def process_single_order_transactions(order_id):
    """
    Traite les transactions d'un ordre spécifique et met à jour la base de données
    
    Args:
        order_id (str): L'ID de l'ordre à traiter
        
    Returns:
        dict: Résultats du traitement
    """
    print(f"🔄 Début du traitement de l'ordre {order_id}")
    print("="*60)
    
    # Vérifier les variables d'environnement requises
    shopify_vars = [
        "SHOPIFY_STORE_DOMAIN", 
        "SHOPIFY_ACCESS_TOKEN", 
        "SHOPIFY_API_VERSION"
    ]
    
    # Pour la base de données, on vérifie soit DATABASE_URL soit les variables SUPABASE individuelles
    db_vars = [
        "SUPABASE_USER",
        "SUPABASE_PASSWORD", 
        "SUPABASE_HOST",
        "SUPABASE_PORT",
        "SUPABASE_DB_NAME"
    ]
    
    missing_vars = []
    
    # Vérifier les variables Shopify
    for var in shopify_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    # Vérifier les variables de base de données
    has_database_url = bool(os.environ.get("DATABASE_URL"))
    has_supabase_vars = all(os.environ.get(var) for var in db_vars)
    
    if not has_database_url and not has_supabase_vars:
        if not has_database_url:
            missing_vars.append("DATABASE_URL (ou les variables SUPABASE_*)")
        if not has_supabase_vars:
            for var in db_vars:
                if not os.environ.get(var):
                    missing_vars.append(var)
    
    if missing_vars:
        print("❌ Variables d'environnement manquantes:")
        for var in missing_vars:
            print(f"   - {var}")
        print("Assurez-vous que ces variables sont définies dans votre fichier .env")
        print("Pour la base de données, vous pouvez utiliser soit:")
        print("  - DATABASE_URL")
        print("  - Ou toutes les variables: SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DB_NAME")
        return {"success": False, "error": "Variables d'environnement manquantes"}
    
    try:
        # Étape 1: Récupérer et traiter l'ordre
        print(f"📦 Récupération et traitement de l'ordre {order_id}...")
        order = get_single_order(order_id)
        json.dump(order, open("order.json", "w"), indent=2, ensure_ascii=False)
        
        if not order:
            print(f"⚠️  Impossible de récupérer l'ordre {order_id}")
            return {
                "success": False,
                "order_id": order_id,
                "error": "Impossible de récupérer l'ordre depuis Shopify"
            }
        
        # Traiter l'ordre avec process_orders
        print(f"💾 Traitement de l'ordre dans la base de données...")
        order_result = process_orders([order])
        
        print(f"✅ Ordre traité: {order_result.get('orders_inserted', 0)} insérés, "
              f"{order_result.get('orders_updated', 0)} mis à jour, "
              f"{order_result.get('orders_skipped', 0)} ignorés")
        
        # Étape 2: Récupérer les transactions de l'ordre
        print(f"\n📥 Récupération des transactions pour l'ordre {order_id}...")
        transactions = get_transactions_by_order(str(order_id))
        
        if not transactions:
            print(f"⚠️  Aucune transaction trouvée pour l'ordre {order_id}")
            return {
                "success": True,
                "order_id": order_id,
                "order_processing_result": order_result,
                "transactions_found": 0,
                "message": "Ordre traité avec succès, aucune transaction trouvée"
            }
        
        print(f"✅ {len(transactions)} transactions récupérées")
        
        # Afficher un aperçu des transactions
        print("\n📋 Aperçu des transactions récupérées:")
        for i, transaction in enumerate(transactions[:5]):  # Afficher les 5 premières
            shop_amount = transaction.get('shop_amount', 0)
            amount_currency = transaction.get('amount_currency')
            currency = transaction.get('transaction_currency', 'USD')
            
            # Afficher le montant local s'il existe, sinon le montant shop
            if amount_currency is not None and currency != 'USD':
                amount_display = f"{amount_currency} {currency} (USD: {shop_amount})"
            else:
                amount_display = f"{shop_amount} USD"
                
            print(f"   {i+1}. {transaction.get('transaction_description', 'N/A')} - "
                  f"{amount_display} - {transaction.get('account_type', 'N/A')}")
        
        if len(transactions) > 5:
            print(f"   ... et {len(transactions) - 5} autres transactions")
        
        # Étape 3: Traiter et insérer les transactions dans la base de données
        print(f"\n💾 Traitement et insertion des transactions dans la base de données...")
        transactions_result = process_transactions(transactions)
        
        # Afficher les résultats
        print("\n" + "="*60)
        print("📊 RÉSULTATS DU TRAITEMENT")
        print("="*60)
        print(f"Ordre traité: {order_id}")
        print(f"Ordre - Insérés: {order_result.get('orders_inserted', 0)}")
        print(f"Ordre - Mis à jour: {order_result.get('orders_updated', 0)}")
        print(f"Ordre - Ignorés: {order_result.get('orders_skipped', 0)}")
        print(f"Détails de commande - Insérés: {order_result.get('order_details_inserted', 0)}")
        print(f"Transactions trouvées: {len(transactions)}")
        print(f"Transactions insérées: {transactions_result.get('inserted', 0)}")
        print(f"Transactions mises à jour: {transactions_result.get('updated', 0)}")
        print(f"Transactions ignorées: {transactions_result.get('skipped', 0)}")
        
        # Afficher les erreurs s'il y en a
        all_errors = []
        if order_result.get('errors'):
            all_errors.extend(order_result['errors'])
        if transactions_result.get('errors'):
            all_errors.extend(transactions_result['errors'])
        
        if all_errors:
            print(f"Erreurs rencontrées: {len(all_errors)}")
            for error in all_errors:
                print(f"   - {error}")
        
        # Sauvegarder les résultats détaillés
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = f"complete_order_analysis_{order_id}_{timestamp}.json"
        
        detailed_result = {
            "order_id": order_id,
            "timestamp": timestamp,
            "order_processing_result": order_result,
            "transactions_found": len(transactions),
            "transactions_processing_result": transactions_result,
            "order_details": {
                "id": order.get('id'),
                "name": order.get('name'),
                "total_price": order.get('total_price'),
                "financial_status": order.get('financial_status'),
                "fulfillment_status": order.get('fulfillment_status'),
                "created_at": order.get('created_at'),
                "line_items_count": len(order.get('line_items', []))
            },
            "transactions_details": [
                {
                    "description": t.get('transaction_description'),
                    "shop_amount": t.get('shop_amount'),
                    "amount_currency": t.get('amount_currency'),
                    "currency": t.get('transaction_currency'),
                    "account_type": t.get('account_type'),
                    "date": t.get('date'),
                    "exchange_rate": t.get('exchange_rate')
                } for t in transactions
            ]
        }
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_result, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 Résultats détaillés sauvegardés dans: {results_file}")
        
        return {
            "success": True,
            "order_id": order_id,
            "order_processing_result": order_result,
            "transactions_found": len(transactions),
            "transactions_processing_result": transactions_result,
            "results_file": results_file
        }
        
    except Exception as e:
        error_msg = f"Erreur lors du traitement de l'ordre {order_id}: {str(e)}"
        print(f"❌ {error_msg}")
        print("Stack trace:")
        traceback.print_exc()
        
        return {
            "success": False,
            "order_id": order_id,
            "error": error_msg
        }

def main():
    """Fonction principale"""
    # ID de l'ordre à traiter
    ORDER_ID = "6019367632967"
    
    print("🚀 SCRIPT DE TRAITEMENT COMPLET D'UN ORDRE SPÉCIFIQUE")
    print(f"Ordre cible: {ORDER_ID}")
    print(f"Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    start_time = datetime.now()
    
    # Traiter l'ordre
    result = process_single_order_transactions(ORDER_ID)
    
    # Calculer la durée
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*60)
    print("🏁 TRAITEMENT TERMINÉ")
    print("="*60)
    print(f"Durée totale: {duration}")
    print(f"Heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if result["success"]:
        print("✅ Traitement réussi!")
        # Afficher un résumé des résultats
        if result.get("order_processing_result"):
            order_stats = result["order_processing_result"]
            print(f"   📦 Ordre: {order_stats.get('orders_inserted', 0)} insérés, "
                  f"{order_stats.get('orders_updated', 0)} mis à jour")
            print(f"   📝 Détails: {order_stats.get('order_details_inserted', 0)} lignes insérées")
        
        if result.get("transactions_processing_result"):
            trans_stats = result["transactions_processing_result"]
            print(f"   💸 Transactions: {trans_stats.get('inserted', 0)} insérées, "
                  f"{trans_stats.get('updated', 0)} mises à jour")
        
        print(f"   📊 Fichier de résultats: {result.get('results_file', 'N/A')}")
    else:
        print("❌ Traitement échoué!")
        if result.get("error"):
            print(f"   Erreur: {result['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main() 