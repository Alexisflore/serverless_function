#!/usr/bin/env python3
"""
Backfill Store Data (any commercial_organisation) — 2 phases:
  Phase 1: Fetch all data from Shopify <ORG> → local files (data/<org>_backfill/)
  Phase 2: Read local files → insert into Supabase

Credentials (from .env):
  SHOPIFY_STORE_DOMAIN_<ORG>
  SHOPIFY_ACCESS_TOKEN_<ORG>  OR  SHOPIFY_CLIENT_ID_<ORG> + SHOPIFY_CLIENT_SECRET_<ORG>
  (client credentials → token fetched at startup, for Shopify CLI apps)

Usage:
  python backfill_store_data.py --org UK fetch      # Phase 1 only
  python backfill_store_data.py --org UK insert     # Phase 2 only (writes to Supabase)
  python backfill_store_data.py --org UK            # Both phases
"""

import os
import sys
import json
import time
import requests
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ALLOWED_ORGS = frozenset({"US", "JP", "UK"})


def _parse_org(argv: list) -> str:
    """Extract --org XX from argv (removed in place so the mode logic is unchanged).

    The value is restricted to ALLOWED_ORGS: it is used to build a directory
    path (data/<org>_backfill) and env var names, so it must never be free text.
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

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", f"{ORG.lower()}_backfill")

STORE_DOMAIN = os.getenv(f"SHOPIFY_STORE_DOMAIN_{ORG}")
STORE_TOKEN = os.getenv(f"SHOPIFY_ACCESS_TOKEN_{ORG}")
STORE_CLIENT_ID = os.getenv(f"SHOPIFY_CLIENT_ID_{ORG}")
STORE_CLIENT_SECRET = os.getenv(f"SHOPIFY_CLIENT_SECRET_{ORG}")

if not STORE_DOMAIN:
    print(f"ERROR: SHOPIFY_STORE_DOMAIN_{ORG} missing")
    sys.exit(1)

# Client credentials win over a static token: when both are set, always exchange.
# Domain validation/normalisation is done inside fetch_access_token.
if STORE_CLIENT_ID and STORE_CLIENT_SECRET:
    from api.lib.shopify_api import fetch_access_token
    STORE_TOKEN = fetch_access_token(STORE_DOMAIN, STORE_CLIENT_ID, STORE_CLIENT_SECRET)
    print(f"Access token for {ORG} obtained via client credentials")

API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")

STORE_BASE_REST = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
STORE_GQL_URL = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
STORE_HEADERS = {
    "X-Shopify-Access-Token": STORE_TOKEN,
    "Content-Type": "application/json",
}


def _store_gql(query: str, variables: dict | None = None, timeout: int = 60) -> dict:
    r = requests.post(STORE_GQL_URL, headers=STORE_HEADERS,
                      json={"query": query, "variables": variables or {}}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(data["errors"])
    return data["data"]


def _store_rest_paginate(resource: str, params: dict | None = None) -> list:
    url = f"{STORE_BASE_REST}/{resource}.json"
    p = {"limit": 250}
    if params:
        p.update(params)
    results = []
    page = 0
    while url:
        page += 1
        resp = requests.get(url, headers=STORE_HEADERS, params=p, timeout=30)
        resp.raise_for_status()
        key = resource.split("/")[-1]
        batch = resp.json().get(key, [])
        results.extend(batch)
        print(f"    page {page}: +{len(batch)} (total {len(results)})", flush=True)

        # Parse Link header properly — find the rel="next" part
        link_header = resp.headers.get("Link", "")
        url = None
        if link_header:
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip("<> ")
                    p = {}
                    break
    return results


def _ensure_data_dir():
    """Create DATA_DIR owner-only (0o700). The dumps may contain customer PII."""
    parent = os.path.dirname(DATA_DIR)
    if not os.path.isdir(parent):
        os.makedirs(parent, mode=0o700, exist_ok=True)
    os.makedirs(DATA_DIR, mode=0o700, exist_ok=True)
    # makedirs ignores mode for an existing dir and is subject to umask: enforce it.
    os.chmod(DATA_DIR, 0o700)


def _open_private(path: str):
    """Open a file for writing with mode 0o600, also fixing a pre-existing file's mode."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.chmod(path, 0o600)  # before any content is written
        return os.fdopen(fd, "w", encoding="utf-8")
    except Exception:
        os.close(fd)
        raise


def _download_bulk_text(url: str) -> str:
    """Download a bulk operation JSONL result.

    The URL is a signed link: it must never reach logs or tracebacks.
    requests embeds it in HTTPError (and in connection errors), so we re-raise
    with only the HTTP status / exception class, and suppress the chained cause.
    """
    try:
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        return resp.text
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        raise RuntimeError(f"Bulk JSONL download failed: HTTP {status}") from None
    except requests.RequestException as e:
        raise RuntimeError(f"Bulk JSONL download failed: {type(e).__name__}") from None


def _save(filename: str, data):
    _ensure_data_dir()
    path = os.path.join(DATA_DIR, filename)
    with _open_private(path) as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    size_kb = os.path.getsize(path) / 1024
    print(f"  → Saved {filename} ({len(data) if isinstance(data, list) else '?'} items, {size_kb:.0f} KB)", flush=True)


def _save_raw(filename: str, content: str):
    _ensure_data_dir()
    path = os.path.join(DATA_DIR, filename)
    with _open_private(path) as f:
        f.write(content)
    size_kb = os.path.getsize(path) / 1024
    lines = content.count("\n")
    print(f"  → Saved {filename} ({lines} lines, {size_kb:.0f} KB)", flush=True)


def _load(filename: str):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _run_store_bulk(bulk_mutation: str) -> str | None:
    status_q = """query { currentBulkOperation { id status errorCode objectCount url partialDataUrl } }"""
    terminal = {"COMPLETED", "FAILED", "CANCELED"}

    current = _store_gql(status_q).get("currentBulkOperation")
    if current and current.get("status") in ("CREATED", "RUNNING"):
        print(f"  Bulk already running (id={current['id']}). Waiting...", flush=True)
    else:
        start = _store_gql(bulk_mutation)
        ue = start["bulkOperationRunQuery"]["userErrors"]
        if ue:
            already = any("already in progress" in (e.get("message") or "") for e in ue)
            if already:
                print("  Bulk already in progress. Waiting...", flush=True)
            else:
                raise RuntimeError(ue)
        print("  Bulk operation started.", flush=True)

    while True:
        time.sleep(5)
        st = _store_gql(status_q)["currentBulkOperation"]
        print(f"  [Bulk] status={st['status']} objects={st.get('objectCount')}", flush=True)
        if st["status"] in terminal:
            if st["status"] != "COMPLETED":
                raise RuntimeError(f"Bulk ended with {st['status']} error={st.get('errorCode')}")
            return st.get("url")


# ============================================================================
# PHASE 1: FETCH FROM SHOPIFY <ORG> → LOCAL FILES
# ============================================================================

def fetch_locations():
    print("\n--- 1/7 LOCATIONS ---", flush=True)
    locs = _store_rest_paginate("locations")
    print(f"  {len(locs)} locations fetched", flush=True)

    print("  Enriching with metafields via GraphQL...", flush=True)
    for i, loc in enumerate(locs, 1):
        lid = loc["id"]
        try:
            q = f"""query {{ location(id: "gid://shopify/Location/{lid}") {{
                metafields(first: 25) {{ edges {{ node {{ namespace key value type }} }} }}
            }} }}"""
            data = _store_gql(q)
            mf_edges = data.get("location", {}).get("metafields", {}).get("edges", [])
            loc["_metafields"] = {f"{e['node']['namespace']}.{e['node']['key']}": e['node']['value'] for e in mf_edges}
            loc["_email"] = loc["_metafields"].get("custom.email")
        except Exception as e:
            print(f"  Warning: metafields for location {lid}: {e}")
            loc["_metafields"] = {}
            loc["_email"] = None

    _save("locations.json", locs)
    return locs


def fetch_products():
    print("\n--- 2/7 PRODUCTS ---", flush=True)
    print("  Fetching all products from REST API...", flush=True)
    products = _store_rest_paginate("products",
                                {"fields": "id,title,handle,status,product_type,vendor,tags,created_at,updated_at,variants,options,images"})
    total_variants = sum(len(p.get('variants', [])) for p in products)
    print(f"  ✓ {len(products)} products, {total_variants} variants", flush=True)

    inv_ids = set()
    for p in products:
        for v in p.get("variants", []):
            iid = v.get("inventory_item_id")
            if iid:
                inv_ids.add(iid)

    inv_list = list(inv_ids)
    total_batches = (len(inv_list) + 249) // 250
    print(f"  Fetching COGS for {len(inv_ids)} inventory items ({total_batches} batches)...", flush=True)
    cogs_map = {}
    for i in range(0, len(inv_list), 250):
        batch = inv_list[i:i + 250]
        batch_num = i // 250 + 1
        gids = [f"gid://shopify/InventoryItem/{x}" for x in batch]
        q = """query($ids: [ID!]!) { nodes(ids: $ids) { ... on InventoryItem { id unitCost { amount currencyCode } } } }"""
        try:
            data = _store_gql(q, {"ids": gids})
            for node in (data.get("nodes") or []):
                if not node:
                    continue
                nid = int(node["id"].split("/")[-1])
                uc = node.get("unitCost")
                cogs_map[nid] = float(uc["amount"]) if uc and uc.get("amount") else None
            print(f"    COGS batch {batch_num}/{total_batches}: +{len(batch)} items", flush=True)
        except Exception as e:
            print(f"    COGS batch {batch_num}/{total_batches}: ERROR {e}", flush=True)
        time.sleep(0.2)

    cogs_count = sum(1 for v in cogs_map.values() if v)
    print(f"  ✓ COGS: {cogs_count} with value, {len(cogs_map) - cogs_count} without", flush=True)

    # Build variant records
    variants = []
    for p in products:
        images = p.get("images", [])
        img_url = images[0].get("src") if images else None
        options = p.get("options", [])

        for v in p.get("variants", []):
            color_val = size_val = None
            for idx, opt in enumerate(options):
                opt_name = opt.get("name", "").lower()
                if opt_name in ("color", "colour", "couleur"):
                    color_val = v.get(f"option{idx + 1}")
                elif opt_name in ("size", "taille", "dimension"):
                    size_val = v.get(f"option{idx + 1}")

            variants.append({
                "variant_id": v.get("id"),
                "product_id": p.get("id"),
                "inventory_item_id": v.get("inventory_item_id"),
                "cogs": cogs_map.get(v.get("inventory_item_id")),
                "sku": v.get("sku"),
                "barcode": v.get("barcode"),
                "title": v.get("title"),
                "status": p.get("status"),
                "vendor": p.get("vendor"),
                "value_color": color_val,
                "value_size": size_val,
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "weight": v.get("weight"),
                "weight_unit": v.get("weight_unit"),
                "position": v.get("position"),
                "product_title": p.get("title"),
                "product_handle": p.get("handle"),
                "product_type": p.get("product_type"),
                "tags": p.get("tags"),
                "created_at": v.get("created_at"),
                "updated_at": v.get("updated_at"),
                "image_url": img_url,
            })

    _save("products_variants.json", variants)
    return variants


def fetch_inventory():
    print("\n--- 3/7 INVENTORY (bulk) ---", flush=True)
    print("  Discovering quantity names...", flush=True)
    names_q = """query { inventoryProperties { quantityNames { name } } }"""
    try:
        d = _store_gql(names_q)
        qty_names = [x["name"] for x in d["inventoryProperties"]["quantityNames"]]
    except Exception:
        qty_names = ["incoming", "on_hand", "available", "committed",
                     "reserved", "damaged", "safety_stock", "quality_control"]
    print(f"  Quantity names: {qty_names}", flush=True)
    print("  Launching bulk operation...", flush=True)

    names_literal = ", ".join(f'"{n}"' for n in qty_names)
    bulk_mutation = f'''mutation {{
      bulkOperationRunQuery(query: """
        {{
          inventoryItems {{
            edges {{ node {{
              id legacyResourceId sku tracked updatedAt
              variant {{ id legacyResourceId product {{ id legacyResourceId }} }}
              inventoryLevels(first: 250) {{ edges {{ node {{
                id
                location {{ id legacyResourceId name }}
                quantities(names: [{names_literal}]) {{ name quantity }}
                updatedAt
              }} }} }}
            }} }}
          }}
        }}
      """) {{ bulkOperation {{ id status }} userErrors {{ field message }} }}
    }}'''

    url = _run_store_bulk(bulk_mutation)
    if not url:
        print("  No inventory data", flush=True)
        _save("inventory.json", [])
        return []

    print("  Downloading bulk JSONL...", flush=True)
    raw_lines = _download_bulk_text(url)
    _save_raw("inventory_bulk.jsonl", raw_lines)

    print("  Parsing JSONL...", flush=True)
    items = {}
    levels_by_item = {}
    for line in raw_lines.splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        gid = obj.get("id", "")
        parent = obj.get("__parentId")
        if "InventoryItem" in gid:
            items[gid] = obj
        elif "InventoryLevel" in gid and parent:
            levels_by_item.setdefault(parent, []).append(obj)

    records = []
    for item_gid, item in items.items():
        base = {
            "inventory_item_id": item.get("legacyResourceId"),
            "sku": item.get("sku"),
            "variant_id": (item.get("variant") or {}).get("legacyResourceId"),
            "product_id": ((item.get("variant") or {}).get("product") or {}).get("legacyResourceId"),
        }
        for lvl in levels_by_item.get(item_gid, []):
            loc = lvl.get("location") or {}
            qmap = {n: 0 for n in qty_names}
            for q in (lvl.get("quantities") or []):
                if q.get("name") in qmap:
                    qmap[q["name"]] = q.get("quantity", 0)
            records.append({
                **base,
                "location_id": loc.get("legacyResourceId"),
                "last_updated_at": lvl.get("updatedAt"),
                **qmap,
                "scheduled_changes": "[]",
            })

    print(f"  {len(records)} inventory records from {len(items)} items")
    _save("inventory.json", records)
    return records


def fetch_customers():
    print("\n--- 4/7 CUSTOMERS (bulk) ---", flush=True)
    print("  Launching bulk operation...", flush=True)
    bulk_mutation = '''mutation {
      bulkOperationRunQuery(query: """
        {
          customers(query: "updated_at:>='2020-01-01'") {
            edges { node {
              id legacyResourceId firstName lastName displayName email phone
              numberOfOrders
              amountSpent { amount currencyCode }
              createdAt updatedAt tags note verifiedEmail validEmailAddress
              addresses { address1 address2 city provinceCode zip country countryCode }
              emailMarketingConsent { marketingState consentUpdatedAt marketingOptInLevel }
              smsMarketingConsent { marketingState consentUpdatedAt marketingOptInLevel consentCollectedFrom }
              defaultAddress { address1 address2 city province provinceCode country countryCodeV2 zip phone firstName lastName company }
              metafields { edges { node { namespace key value type } } }
            } }
          }
        }
      """) { bulkOperation { id status } userErrors { field message } }
    }'''

    url = _run_store_bulk(bulk_mutation)
    if not url:
        print("  No customer data", flush=True)
        _save_raw("customers_bulk.jsonl", "")
        return

    print("  Downloading customers bulk JSONL...", flush=True)
    text = _download_bulk_text(url)
    _save_raw("customers_bulk.jsonl", text)
    lines = [l for l in text.splitlines() if l.strip()]
    print(f"  {len(lines)} JSONL lines saved")


def fetch_orders():
    print("\n--- 5/7 ORDERS ---", flush=True)
    print("  Fetching all orders...", flush=True)
    orders = _store_rest_paginate("orders", {"status": "any"})
    print(f"  ✓ {len(orders)} orders", flush=True)
    _save("orders.json", orders)

    if not orders:
        print("  No orders → skipping transactions", flush=True)
        _save("transactions.json", [])
        return orders

    print(f"  Fetching transactions for {len(orders)} orders...", flush=True)
    all_txs = []
    for o in orders:
        oid = o["id"]
        try:
            url = f"{STORE_BASE_REST}/orders/{oid}/transactions.json"
            resp = requests.get(url, headers=STORE_HEADERS, timeout=15)
            resp.raise_for_status()
            txs = resp.json().get("transactions", [])
            for tx in txs:
                tx["_order_id"] = oid
                tx["_order_name"] = o.get("name")
            all_txs.extend(txs)
        except Exception as e:
            print(f"  Warning: transactions for order {oid}: {e}")
        time.sleep(0.3)

    print(f"  {len(all_txs)} transactions")
    _save("transactions.json", all_txs)
    return orders


def fetch_draft_orders():
    print("\n--- 6/7 DRAFT ORDERS ---", flush=True)
    print("  Fetching all draft orders...", flush=True)
    drafts = _store_rest_paginate("draft_orders")
    print(f"  ✓ {len(drafts)} draft orders", flush=True)
    _save("draft_orders.json", drafts)
    return drafts


def fetch_payouts():
    print("\n--- 7/7 PAYOUTS ---", flush=True)
    print("  Fetching payouts...", flush=True)
    try:
        payouts = _store_rest_paginate("shopify_payments/payouts")
        print(f"  ✓ {len(payouts)} payouts", flush=True)
        _save("payouts.json", payouts)
    except Exception as e:
        print(f"  Payouts not available (probably no Shopify Payments): {e}")
        _save("payouts.json", [])


def _timed(label, func):
    t0 = time.time()
    try:
        result = func()
        elapsed = time.time() - t0
        print(f"  ⏱ {label} done in {elapsed:.1f}s\n", flush=True)
        return result
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  ✗ {label} FAILED after {elapsed:.1f}s: {e}\n", flush=True)
        traceback.print_exc()
        return None


def phase1_fetch():
    t_start = time.time()
    print("=" * 70)
    print(f"PHASE 1 — FETCH FROM SHOPIFY {ORG} → LOCAL FILES")
    print(f"Store: {STORE_DOMAIN}")
    print(f"Output: {DATA_DIR}")
    print(f"Started at {datetime.now().isoformat()}")
    print("=" * 70, flush=True)

    _timed("Locations", fetch_locations)
    _timed("Products", fetch_products)
    _timed("Inventory", fetch_inventory)
    _timed("Customers", fetch_customers)
    _timed("Orders", fetch_orders)
    _timed("Draft Orders", fetch_draft_orders)
    _timed("Payouts", fetch_payouts)

    total = time.time() - t_start
    print("\n" + "=" * 70)
    print(f"PHASE 1 COMPLETE — Total: {total:.0f}s ({total/60:.1f} min)")
    print("=" * 70, flush=True)


# ============================================================================
# PHASE 2: READ LOCAL FILES → INSERT INTO SUPABASE
# ============================================================================

BATCH_SIZE = 1000


def _batched_process(records: list, processor_fn, label: str, batch_size: int = BATCH_SIZE) -> dict:
    """Split records into batches and call processor_fn on each, aggregating stats."""
    total = len(records)
    if total == 0:
        return processor_fn([])

    aggregated: Dict[str, Any] = {}
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size
        print(f"  [{label}] Batch {batch_num}/{total_batches} ({len(batch)} records)")

        stats = processor_fn(batch)

        if not aggregated:
            aggregated = {k: ([] if isinstance(v, list) else v) for k, v in stats.items()}
        else:
            for k, v in stats.items():
                if isinstance(v, list):
                    aggregated[k].extend(v)
                elif isinstance(v, (int, float)):
                    aggregated[k] = aggregated.get(k, 0) + v

    return aggregated


def _override_env_for_store():
    """Override SHOPIFY env vars so existing processors tag data with ORG."""
    os.environ["SHOPIFY_ACCESS_TOKEN"] = STORE_TOKEN
    os.environ["SHOPIFY_STORE_DOMAIN"] = STORE_DOMAIN
    os.environ["COMMERCIAL_ORGANISATION"] = ORG


def insert_locations():
    print("\n--- 1/7 INSERT LOCATIONS ---")
    from api.lib.location_processor import insert_locations_to_db

    locs = _load("locations.json")
    # Map metafields for the insert function
    for loc in locs:
        loc["_metafield_email"] = loc.get("_email")
        loc["_metafields_json"] = loc.get("_metafields", {})

    stats = insert_locations_to_db(locs)
    print(f"  Locations: {stats}")
    return stats


def insert_products():
    print("\n--- 2/7 INSERT PRODUCTS ---")
    from api.lib.product_processor import insert_products_to_db

    variants = _load("products_variants.json")
    print(f"  {len(variants)} variants to insert")
    stats = _batched_process(variants, insert_products_to_db, "Products")
    print(f"  Products: {stats}")
    return stats


def insert_inventory():
    print("\n--- 3/7 INSERT INVENTORY ---")
    from api.lib.process_inventory_sync import process_inventory_records

    records = _load("inventory.json")
    print(f"  {len(records)} inventory records to insert")
    stats = _batched_process(records, process_inventory_records, "Inventory")
    print(f"  Inventory: {stats}")
    return stats


def insert_customers():
    print("\n--- 4/7 INSERT CUSTOMERS ---")
    from api.lib.process_customer import process_customer_records, _build_customer_record

    path = os.path.join(DATA_DIR, "customers_bulk.jsonl")
    customers = {}
    metafields_by_parent = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            gid = obj.get("id", "")
            if "Customer" in gid:
                customers[gid] = obj
            elif "__parentId" in obj and "namespace" in obj:
                parent = obj["__parentId"]
                metafields_by_parent.setdefault(parent, []).append(obj)

    records = []
    for gid, node in customers.items():
        mf_list = metafields_by_parent.get(gid, [])
        records.append(_build_customer_record(gid, node, mf_list))

    print(f"  {len(records)} customer records to insert")
    stats = _batched_process(records, process_customer_records, "Customers")
    print(f"  Customers: {stats}")
    return stats


def insert_orders():
    print("\n--- 5/7 INSERT ORDERS ---")
    from api.lib.order_processor import process_orders
    from api.lib.shopify_api import fetch_order_metafields

    orders = _load("orders.json")
    if not orders:
        print("  No orders to insert")
        return {}

    print(f"  {len(orders)} orders to insert")
    stats = _batched_process(orders, process_orders, "Orders")
    print(f"  Orders: {stats}")
    return stats


def insert_transactions():
    print("\n--- 5b/7 INSERT TRANSACTIONS ---")
    from api.lib.process_transactions import process_transactions

    txs = _load("transactions.json")
    if not txs:
        print("  No transactions to insert")
        return {}

    print(f"  {len(txs)} transactions to insert")
    stats = _batched_process(txs, process_transactions, "Transactions")
    print(f"  Transactions: {stats}")
    return stats


def insert_draft_orders():
    print("\n--- 6/7 INSERT DRAFT ORDERS ---")
    from api.lib.process_draft_orders import process_draft_orders

    drafts = _load("draft_orders.json")
    if not drafts:
        print("  No draft orders to insert")
        return {}

    print(f"  {len(drafts)} draft orders to insert")
    stats = _batched_process(drafts, process_draft_orders, "DraftOrders")
    print(f"  Draft orders: {stats}")
    return stats


def insert_payouts():
    print("\n--- 7/7 INSERT PAYOUTS ---")
    from api.lib.process_payout import recuperer_et_enregistrer_versements_jour

    payouts = _load("payouts.json")
    if not payouts:
        print("  No payouts to insert")
        return {}

    # Payouts are typically inserted day-by-day via the payout processor
    # For backfill, we process each unique date
    dates = set()
    for p in payouts:
        d = p.get("date", "")[:10]
        if d:
            dates.add(d)

    for d in sorted(dates):
        try:
            recuperer_et_enregistrer_versements_jour(d)
            print(f"  Payout date {d}: OK")
        except Exception as e:
            print(f"  Payout date {d}: {e}")

    return {"dates_processed": len(dates)}


def phase2_insert():
    print("=" * 70)
    print("PHASE 2 — INSERT LOCAL FILES → SUPABASE")
    print(f"Source: {DATA_DIR}")
    print(f"commercial_organisation = {ORG}")
    print("=" * 70)

    _override_env_for_store()

    try:
        insert_locations()
    except Exception as e:
        print(f"  ERROR locations: {e}")
        traceback.print_exc()

    try:
        insert_products()
    except Exception as e:
        print(f"  ERROR products: {e}")
        traceback.print_exc()

    try:
        insert_inventory()
    except Exception as e:
        print(f"  ERROR inventory: {e}")
        traceback.print_exc()

    try:
        insert_customers()
    except Exception as e:
        print(f"  ERROR customers: {e}")
        traceback.print_exc()

    try:
        insert_orders()
    except Exception as e:
        print(f"  ERROR orders: {e}")
        traceback.print_exc()

    try:
        insert_transactions()
    except Exception as e:
        print(f"  ERROR transactions: {e}")
        traceback.print_exc()

    try:
        insert_draft_orders()
    except Exception as e:
        print(f"  ERROR draft_orders: {e}")
        traceback.print_exc()

    try:
        insert_payouts()
    except Exception as e:
        print(f"  ERROR payouts: {e}")
        traceback.print_exc()

    print("\n" + "=" * 70)
    print("PHASE 2 COMPLETE")
    print("=" * 70)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    if not STORE_TOKEN:
        print(f"ERROR: SHOPIFY_ACCESS_TOKEN_{ORG} or SHOPIFY_CLIENT_ID_{ORG}/SHOPIFY_CLIENT_SECRET_{ORG} missing")
        sys.exit(1)

    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode == "fetch":
        phase1_fetch()
    elif mode == "insert":
        phase2_insert()
    else:
        phase1_fetch()
        phase2_insert()
