#!/usr/bin/env python3
"""
Script pour supprimer de façon sécurisée les commandes de test
"""

import os
import sys
from dotenv import load_dotenv

# Ajouter le répertoire parent au path pour importer les modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api.lib.insert_order import get_db_connection

load_dotenv()

def execute_delete_test_orders(dry_run=True):
    """
    Supprime les commandes de test de façon sécurisée
    
    Args:
        dry_run (bool): Si True, affiche seulement ce qui serait supprimé sans exécuter
    """
    print("=" * 80)
    print("🗑️  SUPPRESSION DES COMMANDES DE TEST")
    print("=" * 80)
    
    conn = None
    cur = None
    
    try:
        # Connexion à la base
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Compter ce qui sera supprimé
        print("📊 Analyse des données à supprimer...")
        
        count_queries = [
            ("Commandes orders", """
                SELECT COUNT(*) 
                FROM orders 
                WHERE EXISTS (
                    SELECT 1 
                    FROM jsonb_array_elements_text(tags_list) AS tag
                    WHERE LOWER(tag) = 'test_order_shopify'
                )
            """),
            ("Détails orders_details", """
                SELECT COUNT(*) 
                FROM orders_details od
                WHERE od._id_order IN (
                    SELECT _id_order
                    FROM orders 
                    WHERE EXISTS (
                        SELECT 1 
                        FROM jsonb_array_elements_text(tags_list) AS tag
                        WHERE LOWER(tag) = 'test_order_shopify'
                    )
                )
            """),
            ("Transactions transaction", """
                SELECT COUNT(*) 
                FROM transaction t
                WHERE t.order_id::text IN (
                    SELECT _id_order
                    FROM orders 
                    WHERE EXISTS (
                        SELECT 1 
                        FROM jsonb_array_elements_text(tags_list) AS tag
                        WHERE LOWER(tag) = 'test_order_shopify'
                    )
                )
            """)
        ]
        
        counts = {}
        for table_name, query in count_queries:
            cur.execute(query)
            count = cur.fetchone()[0]
            counts[table_name] = count
            print(f"   - {table_name}: {count} enregistrements")
        
        # 2. Afficher les commandes qui seront supprimées
        print(f"\n📋 Commandes de test qui seront supprimées:")
        list_query = """
            SELECT 
                _id_order,
                order_label,
                created_at,
                contact_email,
                current_total_orders,
                (SELECT tag 
                 FROM jsonb_array_elements_text(tags_list) AS tag
                 WHERE LOWER(tag) = 'test_order_shopify'
                 LIMIT 1) AS matching_tag
            FROM orders 
            WHERE EXISTS (
                SELECT 1 
                FROM jsonb_array_elements_text(tags_list) AS tag
                WHERE LOWER(tag) = 'test_order_shopify'
            )
            ORDER BY created_at DESC
        """
        
        cur.execute(list_query)
        test_orders = cur.fetchall()
        
        for order in test_orders:
            print(f"   - Order {order[0]} ({order[1]}) - {order[2]} - {order[3]} - ${order[4]} - Tag: {order[5]}")
        
        # 3. Décider si on continue
        total_records = sum(counts.values())
        
        if total_records == 0:
            print(f"\n✅ Aucune commande de test trouvée. Rien à supprimer.")
            return
        
        print(f"\n⚠️  RÉSUMÉ DE LA SUPPRESSION:")
        print(f"   - Total d'enregistrements à supprimer: {total_records}")
        for table, count in counts.items():
            print(f"   - {table}: {count}")
        
        if dry_run:
            print(f"\n🔍 MODE DRY RUN: Aucune suppression réelle n'a été effectuée")
            print(f"   Pour exécuter la suppression, appelez cette fonction avec dry_run=False")
            return
        
        # 4. Demander confirmation
        print(f"\n❓ Êtes-vous sûr de vouloir supprimer ces {total_records} enregistrements ?")
        print(f"   Cette action est IRRÉVERSIBLE!")
        
        confirmation = input("   Tapez 'DELETE_TEST_ORDERS' pour confirmer: ").strip()
        
        if confirmation != "DELETE_TEST_ORDERS":
            print(f"❌ Suppression annulée (confirmation incorrecte)")
            return
        
        # 5. Exécuter la suppression dans une transaction
        print(f"\n🗑️  Exécution de la suppression...")
        
        # Commencer la transaction
        cur.execute("BEGIN;")
        
        try:
            # Créer une table temporaire avec les IDs
            cur.execute("""
                CREATE TEMP TABLE test_order_ids AS
                SELECT _id_order
                FROM orders 
                WHERE EXISTS (
                    SELECT 1 
                    FROM jsonb_array_elements_text(tags_list) AS tag
                    WHERE LOWER(tag) = 'test_order_shopify'
                );
            """)
            
            # Supprimer les transactions
            cur.execute("""
                DELETE FROM transaction 
                WHERE order_id::text IN (SELECT _id_order FROM test_order_ids);
            """)
            transactions_deleted = cur.rowcount
            print(f"   ✅ {transactions_deleted} transactions supprimées")
            
            # Supprimer les détails de commandes
            cur.execute("""
                DELETE FROM orders_details 
                WHERE _id_order IN (SELECT _id_order FROM test_order_ids);
            """)
            details_deleted = cur.rowcount
            print(f"   ✅ {details_deleted} détails de commandes supprimés")
            
            # Supprimer les commandes
            cur.execute("""
                DELETE FROM orders 
                WHERE _id_order IN (SELECT _id_order FROM test_order_ids);
            """)
            orders_deleted = cur.rowcount
            print(f"   ✅ {orders_deleted} commandes supprimées")
            
            # Valider la transaction
            cur.execute("COMMIT;")
            
            print(f"\n🎉 SUPPRESSION RÉUSSIE!")
            print(f"   - Commandes supprimées: {orders_deleted}")
            print(f"   - Détails supprimés: {details_deleted}")
            print(f"   - Transactions supprimées: {transactions_deleted}")
            
            # 6. Vérification finale
            print(f"\n🔍 Vérification finale...")
            cur.execute("""
                SELECT COUNT(*) 
                FROM orders 
                WHERE EXISTS (
                    SELECT 1 
                    FROM jsonb_array_elements_text(tags_list) AS tag
                    WHERE LOWER(tag) = 'test_order_shopify'
                )
            """)
            remaining = cur.fetchone()[0]
            
            if remaining == 0:
                print(f"   ✅ Vérification réussie: 0 commande de test restante")
            else:
                print(f"   ⚠️  ATTENTION: {remaining} commandes de test encore présentes!")
                
        except Exception as e:
            # En cas d'erreur, annuler la transaction
            cur.execute("ROLLBACK;")
            raise e
            
    except Exception as e:
        print(f"❌ Erreur lors de la suppression: {str(e)}")
        if conn:
            conn.rollback()
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():
    """
    Fonction principale
    """
    print("🧪 SCRIPT DE SUPPRESSION DES COMMANDES DE TEST")
    print("Commandes avec le tag 'TEST_Order_Shopify' dans tags_list")
    
    # D'abord faire un dry run
    print("\n" + "="*50)
    print("1️⃣  DRY RUN - Analyse sans suppression")
    print("="*50)
    execute_delete_test_orders(dry_run=True)
    
    # Demander si on veut continuer
    print(f"\n" + "="*50)
    print("2️⃣  SUPPRESSION RÉELLE")
    print("="*50)
    
    choice = input("Voulez-vous procéder à la suppression réelle? (y/N): ").lower().strip()
    
    if choice == 'y':
        execute_delete_test_orders(dry_run=False)
    else:
        print("❌ Suppression annulée par l'utilisateur")

if __name__ == "__main__":
    main()
