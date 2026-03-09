import logging
import json
from api.lib.insert_order import insert_order
from api.lib.shopify_api import fetch_order_metafields

# Configuration du logging pour Vercel
from .logging_config import get_logger
logger = get_logger('order_processor')

def process_orders(orders):
    """
    Process orders from Shopify API format and insert them directly into the database
    
    Args:
        orders (list): List of orders from Shopify API
        
    Returns:
        dict: Statistics about the operations performed
    """
    order_data = {"orders": []}
    
    for order in orders:
        try:
            order_data["orders"].append(order)
        except Exception as e:
            logger.error(f"Error processing order {order.get('id', 'unknown')}: {str(e)}")

    order_ids = [o["id"] for o in order_data["orders"] if o.get("id")]
    metafields_map = {}
    if order_ids:
        try:
            metafields_map = fetch_order_metafields(order_ids)
            logger.info(f"Fetched ORDER_TYPE metafield for {sum(1 for v in metafields_map.values() if v)} / {len(order_ids)} orders")
        except Exception as e:
            logger.error(f"Failed to fetch order metafields: {e}")

    for order in order_data["orders"]:
        oid = str(order.get("id", ""))
        order["_metafield_order_type"] = metafields_map.get(oid)

    logger.info(f"Inserting {len(order_data['orders'])} orders into database...")
    result = insert_order(order_data)
    
    return result