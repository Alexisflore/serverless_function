#!/usr/bin/env python3
"""
Script pour importer TOUS les customers depuis Shopify vers Supabase
Utilise une date très ancienne pour récupérer tous les customers
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Ajouter le répertoire parent au path pour importer les modules
sys.path.append(str(Path(__file__).parent.parent))

from api.lib.process_customer import sync_customers_since_date
import json

def import_all_customers():
    """
    Importe tous les customers depuis Shopify en utilisant une date très ancienne
    """
    print("🚀 Début de l'import de TOUS les customers depuis Shopify")
    print("=" * 60)
    
    # Utiliser une date très ancienne pour récupérer tous les customers
    # Shopify existe depuis 2006, donc 2000-01-01 devrait couvrir tous les customers
    very_old_date = datetime(2000, 1, 1)
    
    print(f"📅 Date de référence: {very_old_date.isoformat()}")
    print("⏳ Lancement de la synchronisation bulk...")
    print("   (Cela peut prendre plusieurs minutes selon le nombre de customers)")
    
    try:
        # Lancer la synchronisation complète
        result = sync_customers_since_date(very_old_date)
        
        print("\n" + "=" * 60)
        print("📊 RÉSULTATS DE L'IMPORT")
        print("=" * 60)
        
        if result.get("success"):
            stats = result.get("stats", {})
            records_processed = result.get("records_processed", 0)
            
            print(f"✅ Import réussi!")
            print(f"📈 Records traités: {records_processed}")
            print(f"➕ Nouveaux customers: {stats.get('inserted', 0)}")
            print(f"🔄 Customers mis à jour: {stats.get('updated', 0)}")
            print(f"⏭️  Records ignorés: {stats.get('skipped', 0)}")
            
            if stats.get('errors'):
                print(f"⚠️  Erreurs rencontrées: {len(stats['errors'])}")
                print("🔍 Premières erreurs:")
                for i, error in enumerate(stats['errors'][:5]):
                    print(f"   {i+1}. {error}")
                if len(stats['errors']) > 5:
                    print(f"   ... et {len(stats['errors']) - 5} autres erreurs")
            
            print(f"\n🎉 Import terminé avec succès!")
            print(f"💾 Total customers en base: {stats.get('inserted', 0) + stats.get('updated', 0)}")
            
        else:
            print(f"❌ Échec de l'import: {result.get('error', 'Erreur inconnue')}")
            if result.get('stats', {}).get('errors'):
                print("🔍 Détails des erreurs:")
                for error in result['stats']['errors']:
                    print(f"   - {error}")
    
    except Exception as e:
        print(f"💥 Erreur critique lors de l'import: {str(e)}")
        import traceback
        print("🔍 Traceback complet:")
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🏁 Fin du script d'import")

def verify_import():
    """
    Vérifie que l'import s'est bien déroulé en comptant les customers en base
    """
    try:
        from api.lib.database import get_supabase_client
        
        print("\n🔍 Vérification de l'import...")
        
        # Note: get_supabase_client() retourne probablement un client Supabase
        # mais nous devons utiliser une connexion PostgreSQL directe pour les requêtes SQL
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            db_url = "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
                user=os.getenv("SUPABASE_USER"),
                pw=os.getenv("SUPABASE_PASSWORD"),
                host=os.getenv("SUPABASE_HOST"),
                port=os.getenv("SUPABASE_PORT"),
                db=os.getenv("SUPABASE_DB_NAME"),
            )
        
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Compter le total de customers
        cur.execute("SELECT COUNT(*) FROM customers")
        total_customers = cur.fetchone()[0]
        
        # Compter les customers avec email
        cur.execute("SELECT COUNT(*) FROM customers WHERE email IS NOT NULL AND email != ''")
        customers_with_email = cur.fetchone()[0]
        
        # Compter les customers avec des commandes
        cur.execute("SELECT COUNT(*) FROM customers WHERE number_of_orders > 0")
        customers_with_orders = cur.fetchone()[0]
        
        # Dernière synchronisation
        cur.execute("SELECT MAX(synced_at) FROM customers")
        last_sync = cur.fetchone()[0]
        
        # Top 5 customers par montant dépensé
        cur.execute("""
            SELECT first_name, last_name, email, number_of_orders, amount_spent 
            FROM customers 
            WHERE amount_spent IS NOT NULL 
            ORDER BY amount_spent DESC 
            LIMIT 5
        """)
        top_customers = cur.fetchall()
        
        print(f"📊 Total customers importés: {total_customers}")
        print(f"📧 Customers avec email: {customers_with_email}")
        print(f"🛒 Customers avec commandes: {customers_with_orders}")
        print(f"🕐 Dernière synchronisation: {last_sync}")
        
        if top_customers:
            print("\n🏆 Top 5 customers par montant dépensé:")
            for i, (fname, lname, email, orders, spent) in enumerate(top_customers, 1):
                name = f"{fname or ''} {lname or ''}".strip() or "N/A"
                print(f"   {i}. {name} ({email or 'N/A'}) - {orders} commandes - ${spent or 0}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"⚠️  Erreur lors de la vérification: {str(e)}")

if __name__ == "__main__":
    print("🎯 IMPORT COMPLET DES CUSTOMERS SHOPIFY")
    print("=" * 60)
    print("⚠️  ATTENTION: Ce script va importer TOUS les customers depuis Shopify")
    print("   Assurez-vous que la table 'customers' existe dans votre base de données")
    print("=" * 60)
    
    # Demander confirmation
    response = input("\n🤔 Voulez-vous continuer? (oui/non): ").lower().strip()
    
    if response in ['oui', 'o', 'yes', 'y']:
        import_all_customers()
        verify_import()
    else:
        print("❌ Import annulé par l'utilisateur")
