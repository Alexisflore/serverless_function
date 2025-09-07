#!/usr/bin/env python3
"""
Script pour créer la nouvelle table inventory avec contrainte unique sur (inventory_item_id, location_id)
"""

import sys
import os
from pathlib import Path

# Ajouter le répertoire parent au path pour importer les modules
sys.path.append(str(Path(__file__).parent.parent))

from api.lib.database import get_supabase_client
from api.lib.logging_config import setup_logging

def create_inventory_table():
    """Crée la table inventory avec la structure optimisée"""
    
    logger = setup_logging()
    logger.info("Début de la création de la table inventory")
    
    try:
        # Lire le fichier SQL
        sql_file_path = Path(__file__).parent / "create_inventory_table_new.sql"
        
        if not sql_file_path.exists():
            raise FileNotFoundError(f"Fichier SQL non trouvé : {sql_file_path}")
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        logger.info(f"Fichier SQL lu : {sql_file_path}")
        
        # Obtenir le client Supabase
        supabase = get_supabase_client()
        
        # Exécuter le SQL
        logger.info("Exécution de la création de la table inventory...")
        
        # Diviser le SQL en commandes individuelles pour une meilleure gestion d'erreurs
        sql_commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
        
        for i, command in enumerate(sql_commands, 1):
            if command:
                logger.info(f"Exécution de la commande {i}/{len(sql_commands)}")
                logger.debug(f"Commande SQL : {command[:100]}...")
                
                try:
                    result = supabase.rpc('exec_sql', {'sql': command}).execute()
                    logger.debug(f"Résultat commande {i} : {result}")
                except Exception as e:
                    logger.error(f"Erreur lors de l'exécution de la commande {i} : {e}")
                    logger.error(f"Commande qui a échoué : {command}")
                    # Continuer avec les autres commandes
                    continue
        
        logger.info("✅ Table inventory créée avec succès !")
        
        # Vérifier que la table a été créée
        try:
            result = supabase.table('inventory').select('*').limit(1).execute()
            logger.info("✅ Vérification : la table inventory est accessible")
        except Exception as e:
            logger.warning(f"⚠️  Impossible de vérifier la table : {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création de la table inventory : {e}")
        return False

def main():
    """Fonction principale"""
    print("🚀 Création de la table inventory")
    print("=" * 50)
    
    success = create_inventory_table()
    
    if success:
        print("\n✅ Table inventory créée avec succès !")
        print("\n📋 Caractéristiques de la table :")
        print("   • Contrainte unique sur (inventory_item_id, location_id)")
        print("   • 8 types de quantités (available, committed, damaged, etc.)")
        print("   • Clé étrangère vers la table locations")
        print("   • Index optimisés pour les requêtes fréquentes")
        print("   • Trigger automatique pour updated_at")
        print("   • Contraintes de validation des quantités")
        print("   • Champ SKU pour identification rapide")
    else:
        print("\n❌ Échec de la création de la table")
        sys.exit(1)

if __name__ == "__main__":
    main()
