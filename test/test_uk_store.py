#!/usr/bin/env python3
"""
Test de connexion au store Shopify UK (smoke test live, lecture seule).
Vérifie qu'on peut récupérer des données avec les credentials UK.

Credentials (from .env):
  SHOPIFY_STORE_DOMAIN_UK
  SHOPIFY_ACCESS_TOKEN_UK  OU  SHOPIFY_CLIENT_ID_UK + SHOPIFY_CLIENT_SECRET_UK
  (client credentials -> token récupéré au démarrage, pour les apps Shopify CLI)

Usage (script, pas pytest: les appels sont live):
  python test/test_uk_store.py

Les fonctions s'appellent check_* (et non test_*) pour que pytest ne les
collecte pas et ne déclenche pas d'appels Shopify par accident.
Le token n'est jamais affiché, ni aucune donnée client (IDs et counts seulement).
"""

import os
import sys
import time
import requests
from dotenv import load_dotenv

# Allow `python test/test_uk_store.py` to import api.lib from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

ORG = "UK"
EXPECTED_CURRENCY = "GBP"

STORE_DOMAIN = os.getenv(f"SHOPIFY_STORE_DOMAIN_{ORG}")
ACCESS_TOKEN = os.getenv(f"SHOPIFY_ACCESS_TOKEN_{ORG}")
CLIENT_ID = os.getenv(f"SHOPIFY_CLIENT_ID_{ORG}")
CLIENT_SECRET = os.getenv(f"SHOPIFY_CLIENT_SECRET_{ORG}")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")

BASE_URL = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
HEADERS = {"Content-Type": "application/json"}


def resolve_token():
    """Static token, or client credentials exchange. Returns None if unavailable."""
    global ACCESS_TOKEN
    if ACCESS_TOKEN:
        return ACCESS_TOKEN
    if STORE_DOMAIN and CLIENT_ID and CLIENT_SECRET:
        from api.lib.shopify_api import fetch_access_token
        ACCESS_TOKEN = fetch_access_token(STORE_DOMAIN, CLIENT_ID, CLIENT_SECRET)
        print(f"Access token pour {ORG} obtenu via client credentials")
    return ACCESS_TOKEN


def _get(url, max_retries=6):
    """GET with backoff on 429 (honours Retry-After)."""
    resp = None
    for attempt in range(max_retries):
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 429:
            return resp
        wait = float(resp.headers.get("Retry-After", 2 ** attempt))
        print(f"   (429 rate limit, attente {wait:.0f}s)")
        time.sleep(wait)
    return resp


def _error(resp):
    # Only the status code and a short body excerpt; the body never contains our token.
    print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")


def check_shop_info():
    """Récupère les infos de base du store et vérifie la devise."""
    print("=" * 60)
    print("1. SHOP INFO")
    print("=" * 60)
    resp = _get(f"{BASE_URL}/shop.json")
    if resp.status_code != 200:
        _error(resp)
        return False

    shop = resp.json().get("shop", {})
    print(f"   Nom        : {shop.get('name')}")
    print(f"   Domaine     : {shop.get('myshopify_domain')}")
    print(f"   Pays        : {shop.get('country_name')}")
    print(f"   Devise      : {shop.get('currency')}")
    print(f"   Timezone    : {shop.get('iana_timezone')}")
    print(f"   Plan        : {shop.get('plan_display_name')}")
    print(f"   Créé le     : {shop.get('created_at')}")

    currency = shop.get("currency")
    if currency != EXPECTED_CURRENCY:
        print(f"   ✗ Devise inattendue: {currency} (attendu {EXPECTED_CURRENCY}) — mauvais store ?")
        return False
    print(f"   ✓ Devise {EXPECTED_CURRENCY} confirmée")
    return True


def check_orders(limit=5):
    """Récupère les dernières commandes."""
    print("\n" + "=" * 60)
    print(f"2. DERNIÈRES {limit} COMMANDES")
    print("=" * 60)
    resp = _get(f"{BASE_URL}/orders.json?status=any&limit={limit}")
    if resp.status_code != 200:
        _error(resp)
        return

    orders = resp.json().get("orders", [])
    print(f"   {len(orders)} commande(s) récupérée(s)\n")
    for o in orders:
        print(f"   #{o.get('name')} | {o.get('created_at', '')[:10]} | "
              f"{o.get('total_price')} {o.get('currency')} | "
              f"status={o.get('financial_status')} | "
              f"items={len(o.get('line_items', []))}")


def check_products(limit=5):
    """Récupère les derniers produits."""
    print("\n" + "=" * 60)
    print(f"3. DERNIERS {limit} PRODUITS")
    print("=" * 60)
    resp = _get(f"{BASE_URL}/products.json?limit={limit}")
    if resp.status_code != 200:
        _error(resp)
        return

    products = resp.json().get("products", [])
    print(f"   {len(products)} produit(s) récupéré(s)\n")
    for p in products:
        variants = p.get("variants", [])
        print(f"   {(p.get('title') or '')[:50]} | {len(variants)} variant(s) | "
              f"status={p.get('status')} | vendor={p.get('vendor')}")


def check_locations():
    """Récupère les locations."""
    print("\n" + "=" * 60)
    print("4. LOCATIONS")
    print("=" * 60)
    resp = _get(f"{BASE_URL}/locations.json")
    if resp.status_code != 200:
        _error(resp)
        return

    locations = resp.json().get("locations", [])
    print(f"   {len(locations)} location(s)\n")
    for loc in locations:
        print(f"   ID={loc.get('id')} | {loc.get('name')} | "
              f"{loc.get('city')}, {loc.get('country')} | active={loc.get('active')}")


def check_customers(limit=5):
    """Récupère les derniers clients (IDs et counts seulement, pas de PII)."""
    print("\n" + "=" * 60)
    print(f"5. DERNIERS {limit} CLIENTS")
    print("=" * 60)
    resp = _get(f"{BASE_URL}/customers.json?limit={limit}")
    if resp.status_code != 200:
        _error(resp)
        return

    customers = resp.json().get("customers", [])
    print(f"   {len(customers)} client(s) récupéré(s)\n")
    for c in customers:
        print(f"   ID={c.get('id')} | orders={c.get('orders_count')} | "
              f"currency={c.get('currency')}")


def check_draft_orders(limit=5):
    """Récupère les derniers draft orders."""
    print("\n" + "=" * 60)
    print(f"6. DERNIERS {limit} DRAFT ORDERS")
    print("=" * 60)
    resp = _get(f"{BASE_URL}/draft_orders.json?limit={limit}")
    if resp.status_code != 200:
        _error(resp)
        return

    drafts = resp.json().get("draft_orders", [])
    print(f"   {len(drafts)} draft order(s) récupéré(s)\n")
    for d in drafts:
        print(f"   #{d.get('name')} | {d.get('created_at', '')[:10]} | "
              f"{d.get('total_price')} {d.get('currency')} | status={d.get('status')}")


def check_count_summary():
    """Résumé des counts."""
    print("\n" + "=" * 60)
    print("7. RÉSUMÉ DES VOLUMES")
    print("=" * 60)
    endpoints = [
        ("orders", "orders/count.json?status=any"),
        ("products", "products/count.json"),
        ("customers", "customers/count.json"),
        ("draft_orders", "draft_orders/count.json"),
    ]
    for name, endpoint in endpoints:
        resp = _get(f"{BASE_URL}/{endpoint}")
        if resp.status_code == 200:
            count = resp.json().get("count", "?")
            print(f"   {name:20s}: {count}")
        else:
            print(f"   {name:20s}: ERREUR {resp.status_code}")


if __name__ == "__main__":
    if not STORE_DOMAIN:
        print(f"ERREUR: SHOPIFY_STORE_DOMAIN_{ORG} manquant dans .env")
        sys.exit(1)

    token = resolve_token()
    if not token:
        print(
            f"ERREUR: SHOPIFY_ACCESS_TOKEN_{ORG} ou "
            f"SHOPIFY_CLIENT_ID_{ORG} + SHOPIFY_CLIENT_SECRET_{ORG} manquant dans .env"
        )
        sys.exit(1)
    HEADERS["X-Shopify-Access-Token"] = token

    print(f"\nStore {ORG} : {STORE_DOMAIN}")
    print(f"API      : {API_VERSION}")
    print("Token    : OK (non affiché)")
    print()

    ok = check_shop_info()
    if not ok:
        print("\nImpossible de valider le store. Vérifiez les credentials et la devise.")
        sys.exit(1)

    check_count_summary()
    check_orders()
    check_products()
    check_locations()
    check_customers()
    check_draft_orders()

    print("\n" + "=" * 60)
    print("TEST TERMINÉ")
    print("=" * 60)
