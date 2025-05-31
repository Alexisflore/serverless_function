import os
from dotenv import load_dotenv
from database_creation.create_table import create_orders_table
from database_creation.create_line_items_table import create_line_items_table
from create_tax_lines_table import create_tax_lines_table
from database_creation.create_transaction_table import create_transaction_table

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def create_all_tables():
    """
    Crée toutes les tables nécessaires pour stocker les données Shopify dans Supabase
    """
    print("Création de la structure de base de données pour les commandes Shopify...")
    
    # Créer les tables dans l'ordre pour respecter les contraintes de clés étrangères
    print("\n1. Création de la table 'orders'...")
    create_orders_table()
    
    print("\n4. Création de la table 'transaction'...")
    create_transaction_table()
    
    print("\nToutes les tables ont été créées avec succès!")

if __name__ == "__main__":
    create_all_tables() 