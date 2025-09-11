#!/usr/bin/env python3
"""
Script de test pour récupérer les draft orders Shopify en JSON
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

def get_daily_draft_orders(start_date, end_date):
    """
    Récupère les draft orders entre start_date et end_date depuis Shopify
    Format des dates: ISO format (ex: 2024-01-01T00:00:00)
    """
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    url = f"https://{store_domain}/admin/api/{api_version}/draft_orders.json"
    headers = get_shopify_headers()
    
    params = {
        "updated_at_min": start_date,
        "updated_at_max": end_date,
        "limit": 250
    }
    
    draft_orders = []
    page_count = 1
    
    while url:
        print(f"📄 Page {page_count}...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"❌ Erreur: {response.status_code} - {response.text}")
            break

        data = response.json()
        page_draft_orders = data.get('draft_orders', [])
        draft_orders.extend(page_draft_orders)
        
        print(f"✅ {len(page_draft_orders)} draft orders récupérés sur cette page")

        # Gestion de la pagination via les headers Link
        link_header = response.headers.get('Link')
        if link_header and 'rel="next"' in link_header:
            # Extraire l'URL de la page suivante
            url = link_header.split(';')[0].strip('<>')
            params = {}  # Les paramètres sont déjà inclus dans l'URL
            page_count += 1
        else:
            url = None
    
    print(f"🎉 Total: {len(draft_orders)} draft orders récupérés")
    return draft_orders

def get_draft_orders_by_date_range(days_back=7):
    """
    Récupère les draft orders des X derniers jours
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Format ISO pour Shopify
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()
    
    return get_daily_draft_orders(start_iso, end_iso)

def get_draft_orders_for_specific_date(date_str):
    """
    Récupère les draft orders pour une date spécifique
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
        
        print(f"📅 Récupération des draft orders pour le {date_str}")
        return get_daily_draft_orders(start_iso, end_iso)
        
    except ValueError:
        print(f"❌ Format de date invalide. Utilisez le format YYYY-MM-DD (ex: 2024-01-15)")
        return []

def get_single_draft_order(draft_order_id):
    """
    Récupère un draft order spécifique par son ID
    """
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    url = f"https://{store_domain}/admin/api/{api_version}/draft_orders/{draft_order_id}.json"
    headers = get_shopify_headers()
    
    print(f"🔍 Récupération du draft order {draft_order_id}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        draft_order_data = response.json()
        print(f"✅ Draft order {draft_order_id} récupéré avec succès")
        return draft_order_data.get('draft_order', {})
    else:
        print(f"❌ Erreur lors de la récupération du draft order {draft_order_id}: {response.status_code}")
        return None

def save_draft_orders_to_file(draft_orders, filename=None):
    """
    Sauvegarde les draft orders dans un fichier JSON
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"draft_orders_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(draft_orders, f, indent=2, ensure_ascii=False)
    
    print(f"💾 {len(draft_orders)} draft orders sauvegardés dans {filename}")

def save_single_draft_order_to_file(draft_order, draft_order_id):
    """
    Sauvegarde un draft order unique dans un fichier JSON
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"draft_order_{draft_order_id}_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(draft_order, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Draft order {draft_order_id} sauvegardé dans {filename}")

def display_draft_order_summary(draft_orders):
    """
    Affiche un résumé des draft orders récupérés
    """
    print(f"\n📊 RÉSUMÉ DES DRAFT ORDERS")
    print(f"{'='*50}")
    print(f"Nombre total de draft orders: {len(draft_orders)}")
    
    if draft_orders:
        # Statistiques par statut
        status_count = {}
        total_amount = 0
        completed_count = 0
        
        for draft_order in draft_orders:
            status = draft_order.get('status', 'unknown')
            status_count[status] = status_count.get(status, 0) + 1
            
            # Compter les draft orders complétés (convertis en commandes)
            if draft_order.get('order_id'):
                completed_count += 1
            
            # Montant total
            try:
                amount = float(draft_order.get('total_price', 0))
                total_amount += amount
            except (ValueError, TypeError):
                pass
        
        print(f"\nRépartition par statut:")
        for status, count in status_count.items():
            print(f"  - {status}: {count}")
        
        print(f"\nDraft orders convertis en commandes: {completed_count}")
        print(f"Draft orders non convertis: {len(draft_orders) - completed_count}")
        
        print(f"\nMontant total: {total_amount:.2f} {draft_orders[0].get('currency', 'USD')}")
        
        # Premier et dernier draft order
        first_draft = min(draft_orders, key=lambda x: x.get('created_at', ''))
        last_draft = max(draft_orders, key=lambda x: x.get('created_at', ''))
        
        print(f"\nPremier draft order: {first_draft.get('created_at', 'N/A')}")
        print(f"Dernier draft order: {last_draft.get('created_at', 'N/A')}")

def display_single_draft_order_info(draft_order):
    """
    Affiche les informations d'un draft order unique
    """
    print(f"\n📊 INFORMATIONS DU DRAFT ORDER")
    print(f"{'='*50}")
    print(f"ID: {draft_order.get('id', 'N/A')}")
    print(f"Nom: {draft_order.get('name', 'N/A')}")
    print(f"Date de création: {draft_order.get('created_at', 'N/A')}")
    print(f"Date de mise à jour: {draft_order.get('updated_at', 'N/A')}")
    print(f"Statut: {draft_order.get('status', 'N/A')}")
    print(f"Montant total: {draft_order.get('total_price', 'N/A')} {draft_order.get('currency', 'USD')}")
    
    # Vérifier si converti en commande
    order_id = draft_order.get('order_id')
    if order_id:
        print(f"✅ Converti en commande ID: {order_id}")
    else:
        print("❌ Pas encore converti en commande")
    
    # Informations client
    customer = draft_order.get('customer', {})
    if customer:
        print(f"Client: {customer.get('first_name', '')} {customer.get('last_name', '')}")
        print(f"Email: {customer.get('email', 'N/A')}")
    
    # Nombre d'articles
    line_items = draft_order.get('line_items', [])
    print(f"Nombre d'articles: {len(line_items)}")

def show_menu():
    """
    Affiche le menu des options disponibles
    """
    print(f"\n📝 RÉCUPÉRATION DES DRAFT ORDERS SHOPIFY")
    print("="*50)
    print("Que souhaitez-vous faire ?")
    print()
    print("1️⃣  Récupérer les draft orders d'un jour précis")
    print("2️⃣  Récupérer un draft order spécifique par ID")
    print("3️⃣  Récupérer les draft orders des X derniers jours")
    print("4️⃣  Quitter")
    print()

if __name__ == "__main__":
    while True:
        try:
            show_menu()
            choice = input("Votre choix (1-4): ").strip()
            
            if choice == "1":
                # Récupérer les draft orders d'un jour précis
                print("\n📅 Récupération des draft orders d'un jour précis")
                date_input = input("Entrez la date (format YYYY-MM-DD, ex: 2024-01-15): ").strip()
                
                draft_orders = get_draft_orders_for_specific_date(date_input)
                
                if draft_orders:
                    display_draft_order_summary(draft_orders)
                    save_draft_orders_to_file(draft_orders)
                    
                    # Afficher le JSON du premier draft order comme exemple
                    print(f"\n📝 EXEMPLE - Premier draft order en JSON:")
                    print("-" * 50)
                    print(json.dumps(draft_orders[0], indent=2, ensure_ascii=False)[:1000] + "...")
                else:
                    print("❌ Aucun draft order trouvé pour cette date.")
            
            elif choice == "2":
                # Récupérer un draft order spécifique
                print("\n🎯 Récupération d'un draft order spécifique")
                draft_order_id = input("Entrez l'ID du draft order: ").strip()
                
                if draft_order_id:
                    draft_order = get_single_draft_order(draft_order_id)
                    if draft_order:
                        display_single_draft_order_info(draft_order)
                        save_single_draft_order_to_file(draft_order, draft_order_id)
                        
                        # Afficher le JSON complet
                        print(f"\n📝 JSON COMPLET DU DRAFT ORDER:")
                        print("-" * 50)
                        print(json.dumps(draft_order, indent=2, ensure_ascii=False))
                    else:
                        print("❌ Draft order non trouvé.")
                else:
                    print("❌ Veuillez entrer un ID de draft order valide.")
            
            elif choice == "3":
                # Récupérer les draft orders des X derniers jours
                print("\n📆 Récupération des draft orders des derniers jours")
                days_input = input("Combien de jours en arrière ? (défaut: 7): ").strip()
                
                try:
                    days_back = int(days_input) if days_input else 7
                    draft_orders = get_draft_orders_by_date_range(days_back=days_back)
                    
                    if draft_orders:
                        display_draft_order_summary(draft_orders)
                        save_draft_orders_to_file(draft_orders)
                        
                        # Afficher le JSON du premier draft order comme exemple
                        print(f"\n📝 EXEMPLE - Premier draft order en JSON:")
                        print("-" * 50)
                        print(json.dumps(draft_orders[0], indent=2, ensure_ascii=False)[:1000] + "...")
                    else:
                        print("❌ Aucun draft order trouvé.")
                        
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
