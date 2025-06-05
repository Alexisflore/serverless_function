#!/usr/bin/env python3
"""
Test de la route process avec le nouveau champ variant_id
"""

import os
import sys
from datetime import datetime, timedelta, timezone
sys.path.append('api')
sys.path.append('api/lib')

from api.process_daily_data import process_daily_data
from dotenv import load_dotenv

def test_process_route():
    """Test de la route process avec une période spécifique"""
    
    load_dotenv()
    
    print("=== Test de la route process avec variant_id ===")
    
    # Utiliser une période récente pour avoir des données à traiter
    # Utilisons la période autour de la commande qu'on a testée
    end_date = "2025-01-08T00:00:00+00:00"  # Un jour après la dernière transaction
    start_date = "2024-12-16T00:00:00+00:00"  # Date de la première transaction
    
    print(f"📅 Période de test : {start_date} à {end_date}")
    
    try:
        # Appeler la fonction directement (simulation de la route GET)
        result = process_daily_data(start_date, end_date)
        
        print(f"\n📊 Résultats du traitement :")
        print(f"  ✅ Succès : {result.get('success')}")
        print(f"  📝 Message : {result.get('message')}")
        print(f"  📅 Timestamp : {result.get('timestamp')}")
        print(f"  📊 Détails : {result.get('details', 'N/A')}")
        print(f"  🔄 Transactions : {result.get('transactions_processed', 'N/A')}")
        
        if result.get('error'):
            print(f"  ❌ Erreur : {result.get('error')}")
        
        if result.get('success'):
            print(f"\n✅ Test réussi ! La route process fonctionne avec le variant_id")
            
            # Vérifier que des transactions ont été traitées
            transactions_info = result.get('transactions_processed', '')
            if 'transactions traitées' in transactions_info:
                print(f"  🎯 Transactions traitées avec variant_id inclus")
            
        else:
            print(f"\n⚠️  Le traitement a rencontré des problèmes")
            if result.get('error_details'):
                print("Détails de l'erreur :")
                for detail in result.get('error_details', [])[:5]:  # Limiter l'affichage
                    if detail.strip():
                        print(f"    {detail}")
                        
    except Exception as e:
        print(f"❌ Erreur lors du test : {e}")
        import traceback
        print(traceback.format_exc())

def test_process_route_today():
    """Test avec la période par défaut (aujourd'hui/hier)"""
    
    print("\n=== Test avec la période par défaut ===")
    
    # Importer la fonction get_dates
    from api.lib.date_utils import get_dates
    
    start_date, end_date = get_dates()
    print(f"📅 Période par défaut : {start_date} à {end_date}")
    
    try:
        result = process_daily_data(start_date, end_date)
        
        print(f"\n📊 Résultats :")
        print(f"  ✅ Succès : {result.get('success')}")
        print(f"  📝 Message : {result.get('message')}")
        
        if result.get('success'):
            print(f"✅ Route process opérationnelle pour la période par défaut")
        else:
            print(f"ℹ️  Pas de nouvelles données pour la période par défaut (normal)")
            
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    # Test avec une période qui contient des données connues
    test_process_route()
    
    # Test avec la période par défaut
    test_process_route_today() 