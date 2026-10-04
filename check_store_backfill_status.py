#!/usr/bin/env python3
"""
Check what data exists in Shopify <ORG> vs what's already in the DB
(rows WHERE commercial_organisation = <ORG>). Reports what needs to be backfilled.

Read-only: Shopify GETs + DB SELECTs only.

Credentials (from .env):
  SHOPIFY_STORE_DOMAIN_<ORG>
  SHOPIFY_ACCESS_TOKEN_<ORG>  OR  SHOPIFY_CLIENT_ID_<ORG> + SHOPIFY_CLIENT_SECRET_<ORG>
  (client credentials -> token fetched at startup, for Shopify CLI apps)

Usage:
  python check_store_backfill_status.py --org UK
"""

import os
import sys
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ALLOWED_ORGS = frozenset({"US", "JP", "UK"})


def _parse_org(argv: list) -> str:
    """Extract --org XX from argv (removed in place).

    Restricted to ALLOWED_ORGS: the value is used to build env var names.
    """
    if "--org" in argv:
        i = argv.index("--org")
        if i + 1 >= len(argv):
            print("ERREUR: --org requiert une valeur (ex: --org UK)")
            sys.exit(1)
        org = argv[i + 1].strip().upper()
        if org not in ALLOWED_ORGS:
            print(
                f"ERREUR: --org invalide ({argv[i + 1][:20]!r}). "
                f"Valeurs autorisées: {', '.join(sorted(ALLOWED_ORGS))}"
            )
            sys.exit(1)
        del argv[i:i + 2]
        return org
    print("ERREUR: --org est requis (ex: --org UK)")
    sys.exit(1)


ORG = _parse_org(sys.argv)

STORE_DOMAIN = os.getenv(f"SHOPIFY_STORE_DOMAIN_{ORG}")
STORE_TOKEN = os.getenv(f"SHOPIFY_ACCESS_TOKEN_{ORG}")
STORE_CLIENT_ID = os.getenv(f"SHOPIFY_CLIENT_ID_{ORG}")
STORE_CLIENT_SECRET = os.getenv(f"SHOPIFY_CLIENT_SECRET_{ORG}")

if not STORE_DOMAIN:
    print(f"ERREUR: SHOPIFY_STORE_DOMAIN_{ORG} manquant")
    sys.exit(1)

# Les client credentials priment sur un token statique : si les deux existent,
# on échange toujours. La validation du domaine est faite dans fetch_access_token.
if STORE_CLIENT_ID and STORE_CLIENT_SECRET:
    from api.lib.shopify_api import fetch_access_token
    STORE_TOKEN = fetch_access_token(STORE_DOMAIN, STORE_CLIENT_ID, STORE_CLIENT_SECRET)
    print(f"Access token pour {ORG} obtenu via client credentials")

if not STORE_TOKEN:
    print(
        f"ERREUR: SHOPIFY_ACCESS_TOKEN_{ORG} ou "
        f"SHOPIFY_CLIENT_ID_{ORG} + SHOPIFY_CLIENT_SECRET_{ORG} manquant"
    )
    sys.exit(1)

API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")

STORE_BASE = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
STORE_HEADERS = {
    "X-Shopify-Access-Token": STORE_TOKEN,
    "Content-Type": "application/json",
}

MAX_RETRIES = 6


def get_db():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    # Keyword arguments (no hand-built DSN): safe with special characters in the
    # password, and the password cannot end up inside a DSN echoed by an error.
    params = {
        "user": os.getenv("SUPABASE_USER"),
        "password": os.getenv("SUPABASE_PASSWORD"),
        "host": os.getenv("SUPABASE_HOST"),
        "port": os.getenv("SUPABASE_PORT"),
        "dbname": os.getenv("SUPABASE_DB_NAME"),
    }
    env_names = {
        "user": "SUPABASE_USER", "password": "SUPABASE_PASSWORD",
        "host": "SUPABASE_HOST", "port": "SUPABASE_PORT", "dbname": "SUPABASE_DB_NAME",
    }
    missing = [env_names[k] for k, v in params.items() if not v]
    if missing:
        print(f"ERREUR: DATABASE_URL ou variables manquantes: {', '.join(missing)}")
        sys.exit(1)
    return psycopg2.connect(**params)


def _get(url, params=None, timeout=30):
    """GET with backoff on 429 (honours Retry-After). Returns the last response."""
    resp = None
    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, headers=STORE_HEADERS, params=params, timeout=timeout)
        if resp.status_code != 429:
            return resp
        wait = float(resp.headers.get("Retry-After", 2 ** attempt))
        print(f"  (429 rate limit, attente {wait:.0f}s)", flush=True)
        time.sleep(wait)
    return resp


def _next_url(resp):
    """Return the rel="next" URL from the Link header, or None."""
    link = resp.headers.get("Link", "")
    for part in link.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip("<> ")
    return None


def shopify_count(endpoint):
    resp = _get(f"{STORE_BASE}/{endpoint}", timeout=15)
    if resp.status_code == 200:
        return resp.json().get("count", 0)
    return f"ERR {resp.status_code}"


def shopify_list_ids(resource, params=None):
    """Paginate through a REST resource and collect all IDs.

    Raises on a non-200 response: a silently truncated list would make the
    reconciliation look better (or worse) than reality.
    """
    ids = []
    url = f"{STORE_BASE}/{resource}.json"
    p = {"limit": 250, "fields": "id"}
    if params:
        p.update(params)

    key = resource.split("/")[-1]
    while url:
        resp = _get(url, params=p)
        if resp.status_code != 200:
            raise RuntimeError(f"Shopify {resource}: HTTP {resp.status_code}")
        for item in resp.json().get(key, []):
            ids.append(item.get("id"))
        url = _next_url(resp)
        p = None
    return ids


def db_count(cur, table, org=ORG):
    try:
        cur.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE commercial_organisation = %s',
            (org,),
        )
        return cur.fetchone()[0]
    except Exception as e:
        cur.connection.rollback()
        return f"ERR: {e}"


def db_ids(cur, table, id_col, org=ORG):
    try:
        cur.execute(
            f'SELECT "{id_col}" FROM "{table}" WHERE commercial_organisation = %s',
            (org,),
        )
        return {row[0] for row in cur.fetchall()}
    except Exception as e:
        cur.connection.rollback()
        print(f"  ERR lecture DB {table}.{id_col}: {e}")
        return set()


def db_distinct_count(cur, table, id_col, org=ORG):
    cur.execute(
        f'SELECT COUNT(DISTINCT "{id_col}") FROM "{table}" WHERE commercial_organisation = %s',
        (org,),
    )
    return cur.fetchone()[0]


def main():
    print("=" * 70)
    print(f"{ORG} BACKFILL STATUS CHECK")
    print("=" * 70)

    conn = get_db()
    cur = conn.cursor()

    # ---- 1. Counts overview ----
    print(f"\n--- SHOPIFY {ORG} COUNTS vs DB (commercial_organisation='{ORG}') ---\n")
    print(f"  {'Table':<25} {'Shopify ' + ORG:>12} {'DB (' + ORG + ')':>12} {'Delta':>12}")
    print("  " + "-" * 65)

    checks = [
        ("orders", "orders/count.json?status=any", "orders", "order_id"),
        ("products (variants)", "products/count.json", "products", "variant_id"),
        ("customers", "customers/count.json", "customers", "customer_id"),
        ("draft_orders", "draft_orders/count.json", "draft_order", "draft_order_id"),
        ("locations", None, "locations", "location_id"),
        ("inventory", None, "inventory", "id"),
        ("inventory_history", None, "inventory_history", "id"),
        ("transactions", None, "transaction", "id"),
        ("payouts", None, "payout", "id"),
        ("payout_transactions", None, "payout_transaction", "id"),
    ]

    for label, count_ep, db_table, id_col in checks:
        shop_cnt = shopify_count(count_ep) if count_ep else "N/A"
        db_cnt = db_count(cur, db_table)
        if isinstance(shop_cnt, int) and isinstance(db_cnt, int):
            delta = shop_cnt - db_cnt
            delta_str = f"+{delta}" if delta > 0 else str(delta)
        else:
            delta_str = "—"
        print(f"  {label:<25} {str(shop_cnt):>12} {str(db_cnt):>12} {delta_str:>12}")

    # ---- 2. Detail: Locations ----
    print(f"\n\n--- LOCATIONS {ORG} ---\n")
    resp = _get(f"{STORE_BASE}/locations.json", timeout=15)
    if resp.status_code != 200:
        print(f"  ERREUR Shopify locations: HTTP {resp.status_code}")
        shop_locs = []
    else:
        shop_locs = resp.json().get("locations", [])
    shop_loc_ids = {loc["id"] for loc in shop_locs}
    db_loc_ids = db_ids(cur, "locations", "location_id")

    missing_locs = shop_loc_ids - db_loc_ids
    extra_locs = db_loc_ids - shop_loc_ids

    for loc in shop_locs:
        status = "✓ IN DB" if loc["id"] in db_loc_ids else "✗ MISSING"
        print(f"  [{status}] ID={loc['id']} | {loc['name']} | {loc.get('city', '')}, {loc.get('country', '')}")

    if missing_locs:
        print(f"\n  → {len(missing_locs)} location(s) à backfiller")
    else:
        print(f"\n  → Toutes les locations {ORG} sont en DB")
    if extra_locs:
        print(f"  → {len(extra_locs)} location(s) en DB mais absentes de Shopify: {sorted(extra_locs)[:20]}")

    # ---- 3. Detail: Products ----
    print(f"\n\n--- PRODUCTS {ORG} ---\n")
    shop_product_ids = shopify_list_ids("products")
    db_product_ids = db_ids(cur, "products", "product_id")
    missing_products = set(shop_product_ids) - db_product_ids

    # Count variants in Shopify
    total_variants_shopify = 0
    url = f"{STORE_BASE}/products.json"
    p = {"limit": 250, "fields": "id,variants"}
    while url:
        resp = _get(url, params=p)
        if resp.status_code != 200:
            raise RuntimeError(f"Shopify products (variants): HTTP {resp.status_code}")
        for prod in resp.json().get("products", []):
            total_variants_shopify += len(prod.get("variants", []))
        url = _next_url(resp)
        p = None

    db_variant_count = db_distinct_count(cur, "products", "variant_id")
    db_product_count = db_distinct_count(cur, "products", "product_id")

    print(f"  Shopify {ORG} : {len(shop_product_ids)} products, {total_variants_shopify} variants")
    print(f"  DB ({ORG})    : {db_product_count} products, {db_variant_count} variants")
    print(f"  Missing products : {len(missing_products)}")

    # ---- 4. Detail: Customers ----
    print(f"\n\n--- CUSTOMERS {ORG} ---\n")
    shop_cust_ids = shopify_list_ids("customers")
    db_cust_ids = db_ids(cur, "customers", "customer_id")
    missing_custs = set(shop_cust_ids) - db_cust_ids

    print(f"  Shopify {ORG} : {len(shop_cust_ids)} customers")
    print(f"  DB ({ORG})    : {len(db_cust_ids)} customers")
    print(f"  Missing    : {len(missing_custs)}")
    if missing_custs:
        print(f"  Missing IDs: {list(missing_custs)[:20]}")

    # ---- 5. Detail: Orders ----
    print(f"\n\n--- ORDERS {ORG} ---\n")
    shop_order_ids = shopify_list_ids("orders", {"status": "any"})
    db_order_ids = db_ids(cur, "orders", "order_id")
    missing_orders = set(shop_order_ids) - db_order_ids

    print(f"  Shopify {ORG} : {len(shop_order_ids)} orders")
    print(f"  DB ({ORG})    : {len(db_order_ids)} orders")
    print(f"  Missing    : {len(missing_orders)}")

    # ---- 6. Detail: Draft Orders ----
    print(f"\n\n--- DRAFT ORDERS {ORG} ---\n")
    shop_draft_ids = shopify_list_ids("draft_orders")
    db_draft_ids = db_ids(cur, "draft_order", "draft_order_id")
    missing_drafts = set(shop_draft_ids) - db_draft_ids

    print(f"  Shopify {ORG} : {len(shop_draft_ids)} draft orders")
    print(f"  DB ({ORG})    : {len(db_draft_ids)} draft orders")
    print(f"  Missing    : {len(missing_drafts)}")

    # ---- 7. Detail: Inventory ----
    print(f"\n\n--- INVENTORY {ORG} ---\n")
    inv_count = db_count(cur, "inventory")
    inv_hist_count = db_count(cur, "inventory_history")

    print(f"  DB inventory rows ({ORG})         : {inv_count}")
    print(f"  DB inventory_history rows ({ORG})  : {inv_hist_count}")

    # ---- 8. Detail: Transactions / Payouts ----
    print(f"\n\n--- TRANSACTIONS & PAYOUTS {ORG} ---\n")
    tx_count = db_count(cur, "transaction")
    payout_count = db_count(cur, "payout")
    pt_count = db_count(cur, "payout_transaction")

    print(f"  DB transactions ({ORG})         : {tx_count}")
    print(f"  DB payouts ({ORG})              : {payout_count}")
    print(f"  DB payout_transactions ({ORG})  : {pt_count}")

    # ---- Summary ----
    print("\n\n" + "=" * 70)
    print("RÉSUMÉ — CE QU'IL FAUT BACKFILLER")
    print("=" * 70)

    needs_backfill = []

    if missing_locs:
        needs_backfill.append(f"  • Locations       : {len(missing_locs)} manquantes")
    if len(missing_products) > 0:
        needs_backfill.append(f"  • Products        : {len(missing_products)} products manquants ({total_variants_shopify - db_variant_count} variants)")
    if missing_custs:
        needs_backfill.append(f"  • Customers       : {len(missing_custs)} manquants")
    if missing_orders:
        needs_backfill.append(f"  • Orders          : {len(missing_orders)} manquantes")
    if missing_drafts:
        needs_backfill.append(f"  • Draft Orders    : {len(missing_drafts)} manquants")
    if inv_count == 0:
        needs_backfill.append(f"  • Inventory       : aucune donnée {ORG}")
    if inv_hist_count == 0:
        needs_backfill.append(f"  • Inv. History    : aucune donnée {ORG}")
    if tx_count == 0 and len(shop_order_ids) > 0:
        needs_backfill.append(f"  • Transactions    : aucune donnée {ORG} (mais {len(shop_order_ids)} orders existent)")
    if payout_count == 0:
        needs_backfill.append(f"  • Payouts         : aucune donnée {ORG}")

    if needs_backfill:
        print("\n".join(needs_backfill))
        print(f"\n  → Lancer: pipenv run python backfill_store_data.py --org {ORG}")
    else:
        print("\n  ✓ Tout est déjà backfillé !")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
