#!/usr/bin/env python3
"""
Test de connexion au store Shopify Japon.
Vérifie qu'on peut récupérer des données avec les credentials JP.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN_JP")
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN_JP")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

BASE_URL = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"


def test_shop_info():
    """Récupère les infos de base du store."""
    print("=" * 60)
    print("1. SHOP INFO")
    print("=" * 60)
    url = f"{BASE_URL}/shop.json"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")
        return False

    shop = resp.json().get("shop", {})
    print(f"   Nom        : {shop.get('name')}")
    print(f"   Domaine     : {shop.get('myshopify_domain')}")
    print(f"   Pays        : {shop.get('country_name')}")
    print(f"   Devise      : {shop.get('currency')}")
    print(f"   Timezone    : {shop.get('iana_timezone')}")
    print(f"   Plan        : {shop.get('plan_display_name')}")
    print(f"   Créé le     : {shop.get('created_at')}")
    return True


def test_orders(limit=5):
    """Récupère les dernières commandes."""
    print("\n" + "=" * 60)
    print(f"2. DERNIÈRES {limit} COMMANDES")
    print("=" * 60)
    url = f"{BASE_URL}/orders.json?status=any&limit={limit}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")
        return

    orders = resp.json().get("orders", [])
    print(f"   {len(orders)} commande(s) récupérée(s)\n")
    for o in orders:
        print(f"   #{o.get('name')} | {o.get('created_at', '')[:10]} | "
              f"{o.get('total_price')} {o.get('currency')} | "
              f"status={o.get('financial_status')} | "
              f"items={len(o.get('line_items', []))}")


def test_products(limit=5):
    """Récupère les derniers produits."""
    print("\n" + "=" * 60)
    print(f"3. DERNIERS {limit} PRODUITS")
    print("=" * 60)
    url = f"{BASE_URL}/products.json?limit={limit}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")
        return

    products = resp.json().get("products", [])
    print(f"   {len(products)} produit(s) récupéré(s)\n")
    for p in products:
        variants = p.get("variants", [])
        print(f"   {p.get('title')[:50]} | {len(variants)} variant(s) | "
              f"status={p.get('status')} | vendor={p.get('vendor')}")


def test_locations():
    """Récupère les locations."""
    print("\n" + "=" * 60)
    print("4. LOCATIONS")
    print("=" * 60)
    url = f"{BASE_URL}/locations.json"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")
        return

    locations = resp.json().get("locations", [])
    print(f"   {len(locations)} location(s)\n")
    for loc in locations:
        print(f"   ID={loc.get('id')} | {loc.get('name')} | "
              f"{loc.get('city')}, {loc.get('country')} | active={loc.get('active')}")


def test_customers(limit=5):
    """Récupère les derniers clients."""
    print("\n" + "=" * 60)
    print(f"5. DERNIERS {limit} CLIENTS")
    print("=" * 60)
    url = f"{BASE_URL}/customers.json?limit={limit}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")
        return

    customers = resp.json().get("customers", [])
    print(f"   {len(customers)} client(s) récupéré(s)\n")
    for c in customers:
        print(f"   ID={c.get('id')} | {c.get('first_name', '')} {c.get('last_name', '')} | "
              f"orders={c.get('orders_count')} | {c.get('email', 'N/A')}")


def test_draft_orders(limit=5):
    """Récupère les derniers draft orders."""
    print("\n" + "=" * 60)
    print(f"6. DERNIERS {limit} DRAFT ORDERS")
    print("=" * 60)
    url = f"{BASE_URL}/draft_orders.json?limit={limit}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        print(f"   ERREUR {resp.status_code}: {resp.text[:200]}")
        return

    drafts = resp.json().get("draft_orders", [])
    print(f"   {len(drafts)} draft order(s) récupéré(s)\n")
    for d in drafts:
        print(f"   #{d.get('name')} | {d.get('created_at', '')[:10]} | "
              f"{d.get('total_price')} {d.get('currency')} | status={d.get('status')}")


def test_count_summary():
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
        url = f"{BASE_URL}/{endpoint}"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 200:
            count = resp.json().get("count", "?")
            print(f"   {name:20s}: {count}")
        else:
            print(f"   {name:20s}: ERREUR {resp.status_code}")


def test_jp_customer_not_in_us(limit=5):
    """
    Récupère des customers depuis le store JP,
    puis vérifie qu'ils n'existent PAS dans le store US (par email).
    """
    print("\n" + "=" * 60)
    print(f"8. JP CUSTOMERS NOT IN US STORE (check {limit})")
    print("=" * 60)

    # -- Fetch customers from JP --
    url_jp = f"{BASE_URL}/customers.json?limit={limit}"
    resp_jp = requests.get(url_jp, headers=HEADERS)
    if resp_jp.status_code != 200:
        print(f"   ERREUR JP {resp_jp.status_code}: {resp_jp.text[:200]}")
        return

    jp_customers = resp_jp.json().get("customers", [])
    print(f"   {len(jp_customers)} client(s) JP récupéré(s)")

    # -- Setup US API --
    us_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    us_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not us_domain or not us_token:
        print("   ERREUR: SHOPIFY_STORE_DOMAIN ou SHOPIFY_ACCESS_TOKEN manquant")
        return

    us_base = f"https://{us_domain}/admin/api/{API_VERSION}"
    us_headers = {
        "X-Shopify-Access-Token": us_token,
        "Content-Type": "application/json",
    }

    # -- Check each JP customer against US --
    found_in_us = 0
    not_found_in_us = 0

    for c in jp_customers:
        email = c.get("email")
        jp_id = c.get("id")
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()

        if not email:
            print(f"   JP ID={jp_id} ({name}) — pas d'email, skip")
            continue

        url_us = f"{us_base}/customers/search.json?query=email:{email}"
        resp_us = requests.get(url_us, headers=us_headers)
        if resp_us.status_code != 200:
            print(f"   ERREUR US search {resp_us.status_code}: {resp_us.text[:200]}")
            continue

        us_matches = resp_us.json().get("customers", [])

        if us_matches:
            found_in_us += 1
            us_ids = [str(m.get("id")) for m in us_matches]
            print(f"   ⚠ JP ID={jp_id} ({name} / {email}) TROUVÉ dans US — US IDs: {', '.join(us_ids)}")
        else:
            not_found_in_us += 1
            print(f"   ✓ JP ID={jp_id} ({name} / {email}) PAS dans US")

    print(f"\n   Résultat: {not_found_in_us} uniquement JP, {found_in_us} aussi dans US")


def test_jp_products_not_in_us(limit=10):
    """
    Récupère des produits depuis le store JP (par handle),
    puis vérifie s'ils existent aussi dans le store US.
    """
    print("\n" + "=" * 60)
    print(f"9. JP PRODUCTS NOT IN US STORE (check {limit})")
    print("=" * 60)

    url_jp = f"{BASE_URL}/products.json?limit={limit}"
    resp_jp = requests.get(url_jp, headers=HEADERS)
    if resp_jp.status_code != 200:
        print(f"   ERREUR JP {resp_jp.status_code}: {resp_jp.text[:200]}")
        return

    jp_products = resp_jp.json().get("products", [])
    print(f"   {len(jp_products)} produit(s) JP récupéré(s)")

    us_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    us_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not us_domain or not us_token:
        print("   ERREUR: SHOPIFY_STORE_DOMAIN ou SHOPIFY_ACCESS_TOKEN manquant")
        return

    us_base = f"https://{us_domain}/admin/api/{API_VERSION}"
    us_headers = {
        "X-Shopify-Access-Token": us_token,
        "Content-Type": "application/json",
    }

    found_in_us = 0
    not_found_in_us = 0

    for p in jp_products:
        handle = p.get("handle")
        title = p.get("title", "")[:50]
        jp_id = p.get("id")
        jp_variants = len(p.get("variants", []))

        if not handle:
            print(f"   JP ID={jp_id} ({title}) — pas de handle, skip")
            continue

        url_us = f"{us_base}/products.json?handle={handle}"
        resp_us = requests.get(url_us, headers=us_headers)
        if resp_us.status_code != 200:
            print(f"   ERREUR US {resp_us.status_code}: {resp_us.text[:200]}")
            continue

        us_matches = resp_us.json().get("products", [])

        if us_matches:
            found_in_us += 1
            us_p = us_matches[0]
            us_variants = len(us_p.get("variants", []))
            print(f"   ✓ {handle} — DANS LES DEUX | JP ID={jp_id} ({jp_variants}v) / US ID={us_p.get('id')} ({us_variants}v)")
        else:
            not_found_in_us += 1
            print(f"   ✗ {handle} — UNIQUEMENT JP | JP ID={jp_id} ({jp_variants}v)")

    print(f"\n   Résultat: {found_in_us} dans les deux stores, {not_found_in_us} uniquement JP")


if __name__ == "__main__":
    print(f"\nStore JP : {STORE_DOMAIN}")
    print(f"API      : {API_VERSION}")
    print(f"Token    : {ACCESS_TOKEN[:12]}...{ACCESS_TOKEN[-4:]}" if ACCESS_TOKEN else "Token: MANQUANT!")
    print()

    if not STORE_DOMAIN or not ACCESS_TOKEN:
        print("ERREUR: SHOPIFY_STORE_DOMAIN_JP ou SHOPIFY_ACCESS_TOKEN_JP manquant dans .env")
        exit(1)

    ok = test_shop_info()
    if not ok:
        print("\nImpossible de se connecter au store. Vérifiez les credentials.")
        exit(1)

    test_count_summary()
    test_orders()
    test_products()
    test_locations()
    test_customers()
    test_draft_orders()
    test_jp_customer_not_in_us()
    test_jp_products_not_in_us()

    print("\n" + "=" * 60)
    print("TEST TERMINÉ")
    print("=" * 60)
