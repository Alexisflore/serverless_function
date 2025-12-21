#!/usr/bin/env python3
"""
Script simplifié pour reprocesser les transactions d'un order_id spécifique.
Utilise uniquement les fonctions de api/lib/process_transactions.py
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api.lib.process_transactions import get_transactions_by_order, process_transactions

# Load environment variables
load_dotenv()


def reprocess_order_transactions(order_id: str):
    """
    Reprocesse les transactions d'un order_id spécifique.
    
    Ce script va :
    1. Récupérer les transactions depuis Shopify via get_transactions_by_order()
    2. Supprimer TOUTES les transactions existantes pour cet order_id
    3. Réinsérer les transactions avec les données actuelles de Shopify
    
    Args:
        order_id (str): L'ID de l'ordre à reprocesser
        
    Returns:
        dict: Résultats du traitement
    """
    print("=" * 80)
    print("🔄 REPROCESSING DES TRANSACTIONS")
    print("=" * 80)
    print(f"Order ID: {order_id}")
    print(f"Heure de début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    start_time = datetime.now()
    
    try:
        # Étape 1: Récupérer les transactions depuis Shopify
        print("=" * 80)
        print("ÉTAPE 1: Récupération des transactions depuis Shopify")
        print("=" * 80)
        print()
        
        print(f"📥 Récupération des transactions pour l'order {order_id}...")
        transactions = get_transactions_by_order(str(order_id))
        
        if not transactions:
            print(f"⚠️  Aucune transaction trouvée pour l'order {order_id}")
            return {
                "success": False,
                "order_id": order_id,
                "transactions_found": 0,
                "error": "Aucune transaction trouvée"
            }
        
        print(f"✅ {len(transactions)} transaction(s) récupérée(s)")
        print()
        
        # Afficher un aperçu des transactions
        print("📋 Aperçu des transactions récupérées (10 premières):")
        print("-" * 80)
        for i, tx in enumerate(transactions[:10], 1):
            shop_amount = tx.get('shop_amount', 0)
            amount_currency = tx.get('amount_currency')
            currency = tx.get('transaction_currency', 'USD')
            
            # Afficher le montant local s'il existe
            if amount_currency is not None and currency != 'USD':
                amount_display = f"{amount_currency} {currency} (USD: {shop_amount})"
            else:
                amount_display = f"{shop_amount} USD"
            
            print(f"{i}. {tx.get('account_type', 'N/A'):12} | {amount_display:25} | {tx.get('transaction_description', 'N/A')[:40]}")
            print(f"   Date: {tx.get('date')} | Status: {tx.get('status')}")
            print(f"   Product: {tx.get('product_id')} | Variant: {tx.get('variant_id')}")
            print()
        
        if len(transactions) > 10:
            print(f"... et {len(transactions) - 10} autre(s) transaction(s)")
        print()
        
        # Étape 2: Process les transactions dans la base de données
        print("=" * 80)
        print("ÉTAPE 2: Traitement des transactions")
        print("=" * 80)
        print()
        
        print("🗑️  Suppression des anciennes transactions...")
        print("💾 Insertion des nouvelles transactions...")
        print()
        
        result = process_transactions(transactions)
        
        # Afficher les résultats
        print("=" * 80)
        print("📊 RÉSULTATS DU TRAITEMENT")
        print("=" * 80)
        print()
        
        print(f"Order ID: {order_id}")
        print(f"Transactions trouvées: {len(transactions)}")
        
        if result.get('deleted') is not None:
            print(f"Transactions supprimées: {result.get('deleted', 0)}")
        
        print(f"Transactions insérées: {result.get('inserted', 0)}")
        print(f"Transactions mises à jour: {result.get('updated', 0)}")
        print(f"Transactions ignorées: {result.get('skipped', 0)}")
        
        # Afficher les erreurs s'il y en a
        errors = result.get('errors', [])
        if errors:
            print(f"\n⚠️  Erreurs rencontrées: {len(errors)}")
            for error in errors[:5]:  # Afficher max 5 erreurs
                print(f"   - {error}")
            if len(errors) > 5:
                print(f"   ... et {len(errors) - 5} autres erreurs")
        
        # Calculer la durée
        end_time = datetime.now()
        duration = end_time - start_time
        
        print()
        print(f"⏱️  Durée totale: {duration}")
        print(f"🏁 Heure de fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Sauvegarder les résultats détaillés
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = f"reprocess_transactions_{order_id}_{timestamp}.json"
        
        detailed_result = {
            "order_id": order_id,
            "timestamp": timestamp,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "transactions_found": len(transactions),
            "processing_result": result,
            "transactions_details": [
                {
                    "date": t.get('date'),
                    "account_type": t.get('account_type'),
                    "description": t.get('transaction_description'),
                    "shop_amount": t.get('shop_amount'),
                    "amount_currency": t.get('amount_currency'),
                    "currency": t.get('transaction_currency'),
                    "status": t.get('status'),
                    "product_id": t.get('product_id'),
                    "variant_id": t.get('variant_id'),
                    "orders_details_id": t.get('orders_details_id'),
                    "quantity": t.get('quantity')
                } for t in transactions
            ]
        }
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_result, f, indent=2, ensure_ascii=False, default=str)
        
        print()
        print(f"💾 Résultats détaillés sauvegardés dans: {results_file}")
        
        success = len(errors) == 0 and result.get('inserted', 0) > 0
        
        print()
        if success:
            print("✅ TRAITEMENT RÉUSSI!")
        else:
            print("⚠️  TRAITEMENT TERMINÉ AVEC DES AVERTISSEMENTS")
        
        print("=" * 80)
        
        return {
            "success": success,
            "order_id": order_id,
            "transactions_found": len(transactions),
            "processing_result": result,
            "results_file": results_file,
            "duration_seconds": duration.total_seconds()
        }
        
    except Exception as e:
        error_msg = f"Erreur lors du reprocessing de l'order {order_id}: {str(e)}"
        print()
        print("=" * 80)
        print(f"❌ {error_msg}")
        print("=" * 80)
        
        import traceback
        print("\n🔍 Stack trace:")
        traceback.print_exc()
        
        return {
            "success": False,
            "order_id": order_id,
            "error": error_msg
        }


def main():
    """Fonction principale"""
    # ⚠️  MODIFIER CET ORDER_ID SELON VOS BESOINS
    ORDER_ID = "6229391867975"
    
    print()
    print("🚀 SCRIPT DE REPROCESSING DES TRANSACTIONS D'UN ORDER")
    print()
    print(f"   Order cible: {ORDER_ID}")
    print()
    print("   Ce script va:")
    print("   1. Récupérer les transactions depuis Shopify")
    print("   2. Supprimer TOUTES les anciennes transactions de cet order")
    print("   3. Insérer les nouvelles transactions")
    print()
    
    # Vérifier les variables d'environnement
    required_vars = {
        "Shopify": ["SHOPIFY_STORE_DOMAIN", "SHOPIFY_ACCESS_TOKEN"],
        "Database": ["DATABASE_URL"]  # ou SUPABASE_*
    }
    
    missing_vars = []
    for var in required_vars["Shopify"]:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    # Vérifier DB
    if not os.environ.get("DATABASE_URL"):
        db_vars = ["SUPABASE_USER", "SUPABASE_PASSWORD", "SUPABASE_HOST", "SUPABASE_PORT", "SUPABASE_DB_NAME"]
        if not all(os.environ.get(var) for var in db_vars):
            missing_vars.append("DATABASE_URL (ou les variables SUPABASE_*)")
    
    if missing_vars:
        print("❌ Variables d'environnement manquantes:")
        for var in missing_vars:
            print(f"   - {var}")
        print()
        print("Assurez-vous que ces variables sont définies dans votre fichier .env")
        sys.exit(1)
    
    # Demander confirmation
    print("⚠️  ATTENTION: Cette opération va supprimer et recréer toutes les transactions")
    print(f"            pour l'order {ORDER_ID}")
    print()
    
    # Commenter ces lignes pour exécution automatique
    # confirmation = input("Voulez-vous continuer ? (oui/non): ")
    # if confirmation.lower() not in ['oui', 'yes', 'y', 'o']:
    #     print("❌ Opération annulée")
    #     sys.exit(0)
    # print()
    
    # Exécuter le reprocessing
    result = reprocess_order_transactions(ORDER_ID)
    
    # Code de sortie
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()

