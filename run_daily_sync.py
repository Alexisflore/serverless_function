#!/usr/bin/env python3
"""
Script pour exécuter le traitement quotidien des données depuis GitHub Actions
"""
import sys
from datetime import datetime
from api.lib.utils import get_dates
from api.process_daily_data import process_daily_data

def main():
    """Execute the daily data processing"""
    try:
        print("=" * 60)
        print("🚀 Démarrage du traitement quotidien des données")
        print(f"⏰ Timestamp: {datetime.now().isoformat()}")
        print("=" * 60)
        
        # Get date range
        start_date, end_date = get_dates()
        print(f"\n📅 Période analysée: {start_date} à {end_date}\n")
        
        # Process data
        response_data = process_daily_data(start_date, end_date)
        
        # Display results
        print("\n" + "=" * 60)
        print("📊 RÉSULTATS")
        print("=" * 60)
        print(f"✅ Succès: {response_data.get('success', False)}")
        print(f"📝 Message: {response_data.get('message', 'N/A')}")
        
        if response_data.get('details'):
            print(f"📋 Détails: {response_data['details']}")
        
        if response_data.get('transactions_processed'):
            print(f"💰 {response_data['transactions_processed']}")
        
        if response_data.get('products_synchronized'):
            print(f"🛍️  Produits synchronisés: {response_data['products_synchronized']}")
        
        if response_data.get('locations_synchronized'):
            print(f"🏢 Locations synchronisées: {response_data['locations_synchronized']}")
        
        if response_data.get('inventory_synchronized'):
            print(f"📦 {response_data['inventory_synchronized']}")
        
        if response_data.get('draft_orders_processed'):
            print(f"📝 {response_data['draft_orders_processed']}")
        
        if response_data.get('customers_synchronized'):
            print(f"👥 {response_data['customers_synchronized']}")
        
        print("=" * 60)
        
        # Exit with appropriate code
        if response_data.get('success', False):
            print("\n✅ Traitement terminé avec succès!")
            sys.exit(0)
        else:
            print("\n⚠️ Traitement terminé avec des erreurs!")
            if response_data.get('error'):
                print(f"❌ Erreur: {response_data['error']}")
            if response_data.get('error_details'):
                print("\n📋 Détails de l'erreur:")
                for line in response_data['error_details']:
                    print(f"  {line}")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ ERREUR FATALE: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

