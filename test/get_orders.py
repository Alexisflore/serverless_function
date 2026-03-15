#!/usr/bin/env python3
"""
Script de test pour récupérer les commandes Shopify en JSON
"""

import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

def get_shopify_headers():
    """Retourne les headers nécessaires pour l'API Shopify"""
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json"
    }

def get_daily_orders(start_date, end_date):
    """
    Récupère les commandes entre start_date et end_date depuis Shopify
    Format des dates: ISO format (ex: 2024-01-01T00:00:00)
    """
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    url = f"https://{store_domain}/admin/api/{api_version}/orders.json"
    headers = get_shopify_headers()
    
    params = {
        "status": "any",
        "updated_at_min": start_date,
        "updated_at_max": end_date,
        "limit": 250
    }
    
    orders = []
    page_count = 1
    
    print(f"🔍 Récupération des commandes entre {start_date} et {end_date}")
    
    while url:
        print(f"📄 Page {page_count}...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"❌ Erreur: {response.status_code} - {response.text}")
            break

        data = response.json()
        page_orders = data.get('orders', [])
        orders.extend(page_orders)
        
        print(f"✅ {len(page_orders)} commandes récupérées sur cette page")

        # Gestion de la pagination via les headers Link
        link_header = response.headers.get('Link', '')
        url = None
        if link_header:
            for part in link_header.split(','):
                if 'rel="next"' in part:
                    url = part.split(';')[0].strip('<> ')
                    params = {}
                    page_count += 1
                    break
    
    print(f"🎉 Total: {len(orders)} commandes récupérées")
    return orders

def get_orders_by_date_range(days_back=7):
    """
    Récupère les commandes des X derniers jours
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Format ISO pour Shopify
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    
    return get_daily_orders(start_iso, end_iso)

def get_orders_for_specific_date(date_str):
    """
    Récupère les commandes pour une date spécifique
    Format de date attendu: YYYY-MM-DD (ex: 2024-01-15)
    """
    try:
        # Convertir la date en datetime
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        
        # Début et fin de la journée
        start_of_day = target_date.replace(hour=0, minute=0, second=0)
        end_of_day = target_date.replace(hour=23, minute=59, second=59)
        
        # Format ISO pour Shopify
        start_iso = start_of_day.isoformat()
        end_iso = end_of_day.isoformat()
        
        print(f"📅 Récupération des commandes pour le {date_str}")
        return get_daily_orders(start_iso, end_iso)
        
    except ValueError:
        print(f"❌ Format de date invalide. Utilisez le format YYYY-MM-DD (ex: 2024-01-15)")
        return []

def get_single_order(order_id):
    """
    Récupère une commande spécifique par son ID
    """
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}.json"
    headers = get_shopify_headers()
    
    print(f"🔍 Récupération de la commande {order_id}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        order_data = response.json()
        print(f"✅ Commande {order_id} récupérée avec succès")
        return order_data.get('order', {})
    else:
        print(f"❌ Erreur lors de la récupération de la commande {order_id}: {response.status_code}")
        return None

def save_orders_to_file(orders, filename=None):
    """
    Sauvegarde les commandes dans un fichier JSON
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"orders_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(orders, f, indent=2, ensure_ascii=False)
    
    print(f"💾 {len(orders)} commandes sauvegardées dans {filename}")

def save_single_order_to_file(order, order_id):
    """
    Sauvegarde une commande unique dans un fichier JSON
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"order_{order_id}_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(order, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Commande {order_id} sauvegardée dans {filename}")

def display_order_summary(orders):
    """
    Affiche un résumé des commandes récupérées
    """
    print(f"\n📊 RÉSUMÉ DES COMMANDES")
    print(f"{'='*50}")
    print(f"Nombre total de commandes: {len(orders)}")
    
    if orders:
        # Statistiques par statut
        status_count = {}
        total_amount = 0
        
        for order in orders:
            status = order.get('financial_status', 'unknown')
            status_count[status] = status_count.get(status, 0) + 1
            
            # Montant total
            try:
                amount = float(order.get('total_price', 0))
                total_amount += amount
            except (ValueError, TypeError):
                pass
        
        print(f"\nRépartition par statut financier:")
        for status, count in status_count.items():
            print(f"  - {status}: {count}")
        
        print(f"\nMontant total: {total_amount:.2f} {orders[0].get('currency', 'USD')}")
        
        # Première et dernière commande
        first_order = min(orders, key=lambda x: x.get('created_at', ''))
        last_order = max(orders, key=lambda x: x.get('created_at', ''))
        
        print(f"\nPremière commande: {first_order.get('created_at', 'N/A')}")
        print(f"Dernière commande: {last_order.get('created_at', 'N/A')}")

def display_single_order_info(order):
    """
    Affiche les informations d'une commande unique
    """
    print(f"\n📊 INFORMATIONS DE LA COMMANDE")
    print(f"{'='*50}")
    print(f"ID: {order.get('id', 'N/A')}")
    print(f"Numéro de commande: {order.get('name', 'N/A')}")
    print(f"Date de création: {order.get('created_at', 'N/A')}")
    print(f"Statut financier: {order.get('financial_status', 'N/A')}")
    print(f"Statut de fulfillment: {order.get('fulfillment_status', 'N/A')}")
    print(f"Montant total: {order.get('total_price', 'N/A')} {order.get('currency', 'USD')}")
    
    # Informations client
    customer = order.get('customer', {})
    if customer:
        print(f"Client: {customer.get('first_name', '')} {customer.get('last_name', '')}")
        print(f"Email: {customer.get('email', 'N/A')}")

def show_menu():
    """
    Affiche le menu des options disponibles
    """
    print(f"\n🛍️  RÉCUPÉRATION DES COMMANDES SHOPIFY")
    print("="*50)
    print("Que souhaitez-vous faire ?")
    print()
    print("1️⃣  Récupérer les commandes d'un jour précis")
    print("2️⃣  Récupérer une commande spécifique par ID")
    print("3️⃣  Récupérer les commandes des X derniers jours")
    print("4️⃣  Quitter")
    print()

if __name__ == "__main__":
    while True:
        try:
            show_menu()
            choice = input("Votre choix (1-4): ").strip()
            
            if choice == "1":
                # Récupérer les commandes d'un jour précis
                print("\n📅 Récupération des commandes d'un jour précis")
                date_input = input("Entrez la date (format YYYY-MM-DD, ex: 2024-01-15): ").strip()
                
                orders = get_orders_for_specific_date(date_input)
                
                if orders:
                    display_order_summary(orders)
                    save_orders_to_file(orders)
                    
                    # Afficher le JSON de la première commande comme exemple
                    print(f"\n📝 EXEMPLE - Première commande en JSON:")
                    print("-" * 50)
                    print(json.dumps(orders[0], indent=2, ensure_ascii=False)[:1000] + "...")
                else:
                    print("❌ Aucune commande trouvée pour cette date.")
            
            elif choice == "2":
                # Récupérer une commande spécifique
                print("\n🎯 Récupération d'une commande spécifique")
                order_id = input("Entrez l'ID de la commande: ").strip()
                
                if order_id:
                    order = get_single_order(order_id)
                    if order:
                        display_single_order_info(order)
                        save_single_order_to_file(order, order_id)
                        
                        # Afficher le JSON complet
                        print(f"\n📝 JSON COMPLET DE LA COMMANDE:")
                        print("-" * 50)
                        print(json.dumps(order, indent=2, ensure_ascii=False))
                    else:
                        print("❌ Commande non trouvée.")
                else:
                    print("❌ Veuillez entrer un ID de commande valide.")
            
            elif choice == "3":
                # Récupérer les commandes des X derniers jours
                print("\n📆 Récupération des commandes des derniers jours")
                days_input = input("Combien de jours en arrière ? (défaut: 7): ").strip()
                
                try:
                    days_back = int(days_input) if days_input else 7
                    orders = get_orders_by_date_range(days_back=days_back)
                    
                    if orders:
                        display_order_summary(orders)
                        save_orders_to_file(orders)
                        
                        # Afficher le JSON de la première commande comme exemple
                        print(f"\n📝 EXEMPLE - Première commande en JSON:")
                        print("-" * 50)
                        print(json.dumps(orders[0], indent=2, ensure_ascii=False)[:1000] + "...")
                    else:
                        print("❌ Aucune commande trouvée.")
                        
                except ValueError:
                    print("❌ Veuillez entrer un nombre valide.")
            
            elif choice == "4":
                print("\n👋 Au revoir !")
                break
            
            else:
                print("❌ Choix invalide. Veuillez choisir entre 1 et 4.")
            
            # Demander si l'utilisateur veut continuer
            if choice in ["1", "2", "3"]:
                continue_choice = input("\nVoulez-vous effectuer une autre action ? (o/n): ").strip().lower()
                if continue_choice not in ['o', 'oui', 'y', 'yes']:
                    print("\n👋 Au revoir !")
                    break
        
        except KeyboardInterrupt:
            print("\n\n👋 Arrêt du programme.")
            break
        except Exception as e:
            print(f"❌ Erreur: {str(e)}")
            import traceback
            traceback.print_exc()
            
            continue_choice = input("\nVoulez-vous continuer malgré l'erreur ? (o/n): ").strip().lower()
            if continue_choice not in ['o', 'oui', 'y', 'yes']:
                break
