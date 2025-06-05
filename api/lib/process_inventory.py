#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, math
import logging
from typing import List, Dict
import requests
from dotenv import load_dotenv

load_dotenv()

# Configuration du logging pour Vercel
from .logging_config import get_logger
logger = get_logger('inventory')

ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
API_VERSION  = "2024-10"

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# Helpers REST
# ---------------------------------------------------------------------------

def get(endpoint: str, params: Dict = None) -> Dict:
    url = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/{endpoint}"
    logger.debug(f"Making GET request to: {url}")
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    logger.debug(f"Request successful: {url}")
    return r.json()

def chunked(iterable: List, size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]

# ---------------------------------------------------------------------------
# Étape 1 : récupérer les produits ou la liste fournie en CLI
# ---------------------------------------------------------------------------

def fetch_products(product_ids: List[str] | None = None) -> List[Dict]:
    """Renvoie la liste complète des produits (ou ceux passés en argument)."""
    logger.info(f"Fetching products: {'specific IDs' if product_ids else 'all products'}")
    if product_ids:
        logger.info(f"Fetching {len(product_ids)} specific products")
        prods = []
        for pid in product_ids:
            logger.debug(f"Fetching product ID: {pid}")
            prods.append(get(f"products/{pid}.json")["product"])
        return prods

    # pagination ∞ : on arrête quand il n'y a plus de page_info
    products = []
    endpoint = "products.json?limit=250"
    page_count = 0
    logger.info("Fetching all products with pagination")
    while endpoint:
        page_count += 1
        logger.info(f"Fetching products page {page_count}")
        resp = get(endpoint)
        products.extend(resp["products"])
        logger.debug(f"Retrieved {len(resp['products'])} products on page {page_count}")
        link = requests.utils.parse_header_links(
            requests.get(
                f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/{endpoint}",
                headers=HEADERS,
            ).headers.get("Link", "")
        )
        next_link = next((l for l in link if l["rel"] == "next"), {})
        endpoint = next_link.get("url", "").split(f"/{API_VERSION}/")[-1] if next_link else None
        time.sleep(0.2)  # adoucir le rate-limit
    
    logger.info(f"Total products fetched: {len(products)}")
    return products

# ---------------------------------------------------------------------------
# Étape 2 : récupérer les emplacements
# ---------------------------------------------------------------------------

def fetch_locations() -> Dict[int, str]:
    logger.info("Fetching locations")
    locs = get("locations.json")["locations"]
    logger.info(f"Retrieved {len(locs)} locations")
    return {loc["id"]: loc["name"] for loc in locs}

# ---------------------------------------------------------------------------
# Étape 3 : récupérer les niveaux de stock
# ---------------------------------------------------------------------------

def fetch_inventory_levels(item_ids: List[int]) -> List[Dict]:
    logger.info(f"Fetching inventory levels for {len(item_ids)} items")
    levels = []
    batch_count = 0
    total_batches = math.ceil(len(item_ids) / 50)
    
    for batch in chunked(item_ids, 50):  # max 50 IDs par appel REST
        batch_count += 1
        logger.info(f"Fetching inventory batch {batch_count}/{total_batches}")
        params = {"inventory_item_ids": ",".join(map(str, batch))}
        data = get("inventory_levels.json", params=params)
        levels.extend(data["inventory_levels"])
        logger.debug(f"Retrieved {len(data['inventory_levels'])} inventory levels in batch {batch_count}")
        time.sleep(0.2)
    
    logger.info(f"Total inventory levels fetched: {len(levels)}")
    return levels

# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def collect_inventory(product_ids: List[str] | None = None) -> List[Dict]:
    logger.info("Starting inventory collection process")
    
    logger.info("Step 1: Fetching products")
    products = fetch_products(product_ids)
    logger.info(f"Retrieved {len(products)} products")
    
    logger.info("Step 2: Fetching locations")
    locations_map = fetch_locations()
    logger.info(f"Retrieved {len(locations_map)} locations")

    # map inventory_item_id → variante
    logger.info("Step 3: Building inventory item to variant mapping")
    item_to_variant: Dict[int, Dict] = {}
    variant_count = 0
    for p in products:
        for v in p["variants"]:
            variant_count += 1
            item_to_variant[v["inventory_item_id"]] = {
                "product_id" : p["id"],
                "product_title": p["title"],
                "variant_id": v["id"],
                "variant_title": v["title"],
                "sku": v["sku"],
            }
    logger.info(f"Mapped {variant_count} variants across {len(products)} products")

    # niveaux de stock
    logger.info("Step 4: Fetching inventory levels")
    inv_levels = fetch_inventory_levels(list(item_to_variant.keys()))

    # assemblage final
    logger.info("Step 5: Assembling final inventory data")
    results = []
    for lvl in inv_levels:
        meta = item_to_variant.get(lvl["inventory_item_id"], {})
        results.append(
            {
                **meta,
                "location_id": lvl["location_id"],
                "location_name": locations_map.get(lvl["location_id"], ""),
                "available": lvl["available"],
                "incoming": lvl.get("incoming"),  # champ présent depuis 2024-04
                "updated_at": lvl["updated_at"],
            }
        )
    
    logger.info(f"Inventory collection complete. Total items: {len(results)}")
    return results

# ---------------------------------------------------------------------------
# CLI simple : python fetch_inventory.py 12345 67890
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Script execution started")
    ids = sys.argv[1:]  # facultatif
    
    if ids:
        logger.info(f"Product IDs provided via command line: {ids}")
    else:
        logger.info("No product IDs provided, fetching all inventory")
    
    inventory = collect_inventory(ids if ids else None)
    logger.info(f"Writing inventory data for {len(inventory)} items to stdout")
    print(json.dumps(inventory, indent=2, default=str))
    logger.info("Script execution completed")