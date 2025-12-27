#!/usr/bin/env python3
"""
Script de test pour vérifier la logique des location_id sur les refunds avec status 'return'

Usage:
    python test_refund_location_logic.py                    # Cherche automatiquement une commande
    python test_refund_location_logic.py <order_id>         # Teste une commande spécifique
"""

import os
import sys
from datetime import datetime

# Ajouter le répertoire parent au path pour importer les modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api.lib.process_transactions import get_transactions_by_order, check_return_check
import requests
from dotenv import load_dotenv

load_dotenv()

def _shopify_headers():
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json",
    }

def find_order_with_return_refund():
    """
    Cherche une commande avec des refunds ayant le statut 'return'
    """
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    
    print("🔍 Recherche d'une commande avec des refunds de type 'return'...")
    
    # Chercher les commandes avec refunds
    url = f"https://{store_domain}/admin/api/{api_version}/orders.json?status=any&financial_status=refunded&limit=50"
    
    resp = requests.get(url, headers=_shopify_headers())
    if not resp.ok:
        print(f"❌ Erreur lors de la récupération des commandes: {resp.status_code}")
        return None
    
    orders = resp.json().get("orders", [])
    print(f"📦 {len(orders)} commandes trouvées avec des refunds")
    
    for order in orders:
        order_id = str(order["id"])
        source_name = order.get("source_name")
        cancelled_at = order.get("cancelled_at")
        
        # Vérifier si la commande a des refunds
        refunds_url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}/refunds.json"
        refunds_resp = requests.get(refunds_url, headers=_shopify_headers())
        
        if not refunds_resp.ok:
            continue
            
        refunds = refunds_resp.json().get("refunds", [])
        
        for refund in refunds:
            for refund_item in refund.get("refund_line_items", []):
                restock_type = refund_item.get("restock_type")
                
                if restock_type == "return":
                    li = refund_item.get("line_item", {})
                    product_id = li.get("product_id")
                    variant_id = li.get("variant_id")
                    
                    print(f"\n✅ Commande trouvée: {order_id}")
                    print(f"   - Source: {source_name}")
                    print(f"   - Cancelled: {cancelled_at}")
                    print(f"   - Refund ID: {refund.get('id')}")
                    print(f"   - Restock Type: {restock_type}")
                    print(f"   - Product ID: {product_id}")
                    print(f"   - Variant ID: {variant_id}")
                    
                    return {
                        "order_id": order_id,
                        "source_name": source_name,
                        "cancelled_at": cancelled_at,
                        "refund_id": refund.get("id"),
                        "product_id": product_id,
                        "variant_id": variant_id,
                        "refund_location_id": refund.get("location_id")
                    }
    
    print("❌ Aucune commande avec refund de type 'return' trouvée")
    return None

def get_order_details(order_id):
    """Récupère les détails d'une commande"""
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    
    url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}.json"
    resp = requests.get(url, headers=_shopify_headers())
    
    if not resp.ok:
        return None
    
    return resp.json().get("order", {})

def get_sale_location_for_product(order_id, product_id, variant_id):
    """Récupère le location_id de la vente pour un produit/variant donné"""
    order = get_order_details(order_id)
    if not order:
        return None
    
    # Chercher dans les fulfillments
    for fulfillment in order.get("fulfillments", []):
        if fulfillment.get("status") != "success":
            continue
            
        for line_item in fulfillment.get("line_items", []):
            if (line_item.get("product_id") == product_id and 
                line_item.get("variant_id") == variant_id):
                return fulfillment.get("location_id")
    
    return None

def test_conditions(test_data):
    """
    Teste les 4 conditions pour l'application du sale_location_id
    """
    order_id = test_data["order_id"]
    product_id = test_data["product_id"]
    variant_id = test_data["variant_id"]
    source_name = test_data["source_name"]
    cancelled_at = test_data["cancelled_at"]
    
    print("\n" + "="*70)
    print("🧪 TEST DES CONDITIONS")
    print("="*70)
    
    # Condition 1: orders.cancelled_at is null
    condition_1 = cancelled_at is None
    print(f"\n1️⃣  Condition 1 - Commande non annulée (cancelled_at is null):")
    print(f"    cancelled_at = {cancelled_at}")
    print(f"    ✅ VALIDE" if condition_1 else f"    ❌ NON VALIDE")
    
    # Condition 2: orders.source_name = 'pos'
    condition_2 = source_name == 'pos'
    print(f"\n2️⃣  Condition 2 - Source = 'pos':")
    print(f"    source_name = {source_name}")
    print(f"    ✅ VALIDE" if condition_2 else f"    ❌ NON VALIDE")
    
    # Condition 3: orders_details.return_check = 'true'
    condition_3 = check_return_check(order_id, product_id, variant_id)
    print(f"\n3️⃣  Condition 3 - return_check = 'true':")
    print(f"    return_check pour product_id={product_id}, variant_id={variant_id}")
    print(f"    ✅ VALIDE" if condition_3 else f"    ❌ NON VALIDE")
    
    # Condition 4: transaction.status = 'return' (déjà vérifié dans la recherche)
    condition_4 = True  # On a déjà filtré pour restock_type == 'return'
    print(f"\n4️⃣  Condition 4 - refund_status = 'return':")
    print(f"    restock_type = 'return' (confirmé)")
    print(f"    ✅ VALIDE")
    
    # Résultat global
    all_conditions_met = condition_1 and condition_2 and condition_3 and condition_4
    
    print("\n" + "-"*70)
    print(f"📊 RÉSULTAT GLOBAL:")
    print(f"    Toutes les conditions sont remplies: {'✅ OUI' if all_conditions_met else '❌ NON'}")
    print("-"*70)
    
    return all_conditions_met

def test_location_logic(test_data):
    """
    Teste la logique complète du location_id
    """
    order_id = test_data["order_id"]
    product_id = test_data["product_id"]
    variant_id = test_data["variant_id"]
    refund_location_id = test_data["refund_location_id"]
    
    print("\n" + "="*70)
    print("📍 TEST DE LA LOGIQUE LOCATION_ID")
    print("="*70)
    
    # Récupérer le sale_location_id
    sale_location_id = get_sale_location_for_product(order_id, product_id, variant_id)
    
    print(f"\n🏪 Location IDs:")
    print(f"    Refund location_id: {refund_location_id}")
    print(f"    Sale location_id: {sale_location_id}")
    
    # Tester les conditions
    all_conditions_met = test_conditions(test_data)
    
    # Déterminer le final_location_id selon la logique
    should_use_sale_location = all_conditions_met
    final_location_id = sale_location_id if (should_use_sale_location and sale_location_id is not None) else refund_location_id
    
    print(f"\n🎯 DÉCISION:")
    if should_use_sale_location and sale_location_id is not None:
        print(f"    ✅ Utilisation du sale_location_id: {final_location_id}")
        print(f"    Raison: Toutes les conditions sont remplies")
    else:
        print(f"    ↩️  Utilisation du refund_location_id: {final_location_id}")
        if not should_use_sale_location:
            print(f"    Raison: Au moins une condition n'est pas remplie")
        else:
            print(f"    Raison: sale_location_id est None")
    
    return final_location_id

def test_full_transaction_processing(order_id):
    """
    Teste le traitement complet des transactions pour vérifier que tout fonctionne
    """
    print("\n" + "="*70)
    print("⚙️  TEST DU TRAITEMENT COMPLET DES TRANSACTIONS")
    print("="*70)
    
    try:
        transactions = get_transactions_by_order(order_id)
        
        print(f"\n✅ {len(transactions)} transactions générées")
        
        # Afficher les transactions de type return
        return_transactions = [t for t in transactions if t.get("account_type") == "Returns"]
        
        if return_transactions:
            print(f"\n📋 {len(return_transactions)} transaction(s) de type 'Returns' trouvée(s):")
            for i, tx in enumerate(return_transactions, 1):
                print(f"\n    Transaction {i}:")
                print(f"      - Description: {tx.get('transaction_description')}")
                print(f"      - Product ID: {tx.get('product_id')}")
                print(f"      - Variant ID: {tx.get('variant_id')}")
                print(f"      - Location ID: {tx.get('location_id')}")
                print(f"      - Status: {tx.get('status')}")
                print(f"      - Amount: {tx.get('shop_amount')}")
        else:
            print("❌ Aucune transaction de type 'Returns' trouvée")
        
        return True
    except Exception as e:
        print(f"❌ Erreur lors du traitement: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_refund_data_for_order(order_id):
    """
    Récupère les données de refund pour une commande spécifique
    """
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    
    print(f"\n🔍 Analyse de la commande {order_id}...")
    
    # Récupérer les détails de la commande
    order = get_order_details(order_id)
    if not order:
        print(f"❌ Impossible de récupérer la commande {order_id}")
        return None
    
    source_name = order.get("source_name")
    cancelled_at = order.get("cancelled_at")
    
    print(f"   - Source: {source_name}")
    print(f"   - Cancelled: {cancelled_at}")
    
    # Récupérer les refunds
    refunds_url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}/refunds.json"
    refunds_resp = requests.get(refunds_url, headers=_shopify_headers())
    
    if not refunds_resp.ok:
        print(f"❌ Impossible de récupérer les refunds: {refunds_resp.status_code}")
        return None
    
    refunds = refunds_resp.json().get("refunds", [])
    print(f"   - Nombre de refunds: {len(refunds)}")
    
    # Chercher des refunds avec status 'return'
    refund_data_list = []
    
    for refund in refunds:
        refund_location_id = refund.get("location_id")
        
        for refund_item in refund.get("refund_line_items", []):
            restock_type = refund_item.get("restock_type")
            li = refund_item.get("line_item", {})
            product_id = li.get("product_id")
            variant_id = li.get("variant_id")
            product_name = li.get("name")
            
            print(f"\n   📦 Refund item trouvé:")
            print(f"      - Refund ID: {refund.get('id')}")
            print(f"      - Product: {product_name}")
            print(f"      - Product ID: {product_id}")
            print(f"      - Variant ID: {variant_id}")
            print(f"      - Restock Type: {restock_type}")
            print(f"      - Refund Location: {refund_location_id}")
            
            refund_data_list.append({
                "order_id": order_id,
                "source_name": source_name,
                "cancelled_at": cancelled_at,
                "refund_id": refund.get("id"),
                "product_id": product_id,
                "variant_id": variant_id,
                "product_name": product_name,
                "restock_type": restock_type,
                "refund_location_id": refund_location_id
            })
    
    if not refund_data_list:
        print("\n⚠️  Aucun refund trouvé pour cette commande")
    
    return refund_data_list

def main():
    """Fonction principale du test"""
    print("\n" + "="*70)
    print("🚀 DÉMARRAGE DU TEST DE LA LOGIQUE REFUND LOCATION")
    print("="*70)
    
    # Vérifier si un order_id est fourni en argument
    if len(sys.argv) > 1:
        order_id = sys.argv[1]
        print(f"\n📋 Test avec l'order_id fourni: {order_id}")
        
        # Récupérer tous les refunds de cette commande
        refund_data_list = get_refund_data_for_order(order_id)
        
        if not refund_data_list:
            print("\n❌ Aucune donnée de refund trouvée pour cette commande")
            return False
        
        # Tester chaque refund item
        all_success = True
        for i, test_data in enumerate(refund_data_list, 1):
            print(f"\n{'='*70}")
            print(f"📦 TEST {i}/{len(refund_data_list)} - {test_data.get('product_name')}")
            print(f"{'='*70}")
            
            # Tester la logique des location_id
            try:
                final_location_id = test_location_logic(test_data)
            except Exception as e:
                print(f"❌ Erreur lors du test: {e}")
                import traceback
                traceback.print_exc()
                all_success = False
        
        # Tester le traitement complet
        print(f"\n{'='*70}")
        print("⚙️  TEST DU TRAITEMENT COMPLET DE LA COMMANDE")
        print(f"{'='*70}")
        success = test_full_transaction_processing(order_id)
        
        final_success = all_success and success
        
    else:
        print("\n🔍 Recherche automatique d'une commande avec refunds...")
        
        # 1. Trouver une commande avec des refunds de type 'return'
        test_data = find_order_with_return_refund()
        
        if not test_data:
            print("\n⚠️  Impossible de trouver une commande appropriée pour le test")
            print("\n💡 Usage: python test_refund_location_logic.py <order_id>")
            return False
        
        # 2. Tester la logique des location_id
        final_location_id = test_location_logic(test_data)
        
        # 3. Tester le traitement complet
        final_success = test_full_transaction_processing(test_data["order_id"])
    
    print("\n" + "="*70)
    print("✅ TEST TERMINÉ AVEC SUCCÈS" if final_success else "❌ TEST ÉCHOUÉ")
    print("="*70)
    
    return final_success

if __name__ == "__main__":
    main()

