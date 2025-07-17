#!/usr/bin/env python3
"""
Script de test pour récupérer les transactions d'une commande Shopify en JSON
"""

import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def get_shopify_headers():
    """Retourne les headers nécessaires pour l'API Shopify"""
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json"
    }

def get_order_transactions(order_id):
    """
    Récupère toutes les transactions pour une commande spécifique depuis Shopify
    """
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}/transactions.json"
    headers = get_shopify_headers()
    
    print(f"🔍 Récupération des transactions pour la commande {order_id}")
    print(f"📡 URL: {url}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Erreur: {response.status_code} - {response.text}")
        return None
    
    data = response.json()
    transactions = data.get('transactions', [])
    
    print(f"✅ {len(transactions)} transaction(s) récupérée(s)")
    
    # Afficher un résumé des transactions
    for i, transaction in enumerate(transactions, 1):
        print(f"  {i}. ID: {transaction.get('id')} - Type: {transaction.get('kind')} - Montant: {transaction.get('amount')} {transaction.get('currency')} - Status: {transaction.get('status')}")
    
    return transactions

def save_transactions_to_file(transactions, order_id):
    """Sauvegarde les transactions dans un fichier JSON"""
    if not transactions:
        print("❌ Aucune transaction à sauvegarder")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"transactions_{order_id}_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(transactions, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Transactions sauvegardées dans: {filename}")

def main():
    """Fonction principale du script"""
    print("=" * 60)
    print("🛒 RÉCUPÉRATION DES TRANSACTIONS SHOPIFY")
    print("=" * 60)
    
    # Vérifier les variables d'environnement
    required_env_vars = [
        "SHOPIFY_STORE_DOMAIN",
        "SHOPIFY_ACCESS_TOKEN",
        "SHOPIFY_API_VERSION"
    ]
    
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Variables d'environnement manquantes: {', '.join(missing_vars)}")
        return
    
    # Demander l'ID de la commande
    try:
        order_id = input("📝 Entrez l'ID de la commande: ").strip()
        
        if not order_id:
            print("❌ ID de commande requis")
            return
        
        # Valider que c'est un nombre
        int(order_id)
        
    except ValueError:
        print("❌ L'ID de la commande doit être un nombre")
        return
    except KeyboardInterrupt:
        print("\n👋 Opération annulée")
        return
    
    # Récupérer les transactions
    print("\n" + "─" * 40)
    transactions = get_order_transactions(order_id)
    
    if transactions:
        print("\n" + "─" * 40)
        save_transactions_to_file(transactions, order_id)
        
        # Afficher quelques détails supplémentaires
        print(f"\n📊 RÉSUMÉ:")
        total_amount = sum(float(t.get('amount', 0)) for t in transactions)
        currencies = set(t.get('currency') for t in transactions)
        kinds = set(t.get('kind') for t in transactions)
        
        print(f"  💰 Montant total: {total_amount} {'/'.join(currencies) if currencies else 'N/A'}")
        print(f"  🔧 Types de transactions: {', '.join(kinds) if kinds else 'N/A'}")
    
    print("\n✅ Script terminé!")

if __name__ == "__main__":
    main()
