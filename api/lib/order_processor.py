import logging
import json
from api.lib.insert_order import insert_order

# Configuration du logging
logger = logging.getLogger('order_processor')

def process_orders(orders):
    """
    Process orders from Shopify API format and insert them directly into the database
    
    Args:
        orders (list): List of orders from Shopify API
        
    Returns:
        dict: Statistics about the operations performed
    """
    # Créer la structure de données pour insérer les commandes
    order_data = {"orders": []}
    
    # Traiter les commandes
    for order in orders:
        try:
            # Ajouter la commande à la liste des commandes à traiter
            order_data["orders"].append(order)
        except Exception as e:
            logger.error(f"Error processing order {order.get('id', 'unknown')}: {str(e)}")
    
    # Insérer directement les commandes dans la base de données
    logger.info(f"Inserting {len(order_data['orders'])} orders into database...")
    result = insert_order(order_data)
    
    # Retourner les statistiques
    return result