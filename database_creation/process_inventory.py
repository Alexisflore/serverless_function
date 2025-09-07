#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, math
import logging
from typing import List, Dict
import requests
from dotenv import load_dotenv

load_dotenv()

# # Configuration du logging pour Vercel
# from .logging_config import get_logger
# logger = get_logger('inventory')

ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
API_VERSION  = "2025-07"

"""
Export all Shopify Inventory Items with per-location stock details,
then flatten to a CSV (one row per item x location).

Prereqs:
  pip install requests

Env vars (or hardcode below):
  ACCESS_TOKEN=shpat_xxx
  STORE_DOMAIN=your-store.myshopify.com

Scopes:
  read_inventory (required), plus read_products if you want product/variant fields.
Docs:
  - Bulk operations (run, poll, download JSONL) → https://shopify.dev/docs/api/usage/bulk-operations/queries
  - quantityNames discovery → inventoryProperties
"""

import os
import csv
import json
import time
import typing as t
import requests

GRAPHQL_URL = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ---------- Low-level GraphQL helpers ----------

def _gql(query: str, variables: dict | None = None, timeout: int = 60) -> dict:
    """POST a GraphQL request; raise on errors."""
    r = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(data["errors"])
    return data["data"]

# ---------- Discover inventory quantity states ----------

FALLBACK_QUANTITY_NAMES = [
    "incoming", "on_hand", "available", "committed",
    "reserved", "damaged", "safety_stock", "quality_control"
]

def discover_quantity_names() -> list[str]:
    """
    Ask the shop which inventory states (quantity names) are supported.
    If the call fails, return a sensible default list.
    """
    q = """
    query {
      inventoryProperties {
        quantityNames { name }
      }
    }
    """
    try:
        d = _gql(q)
        names = [x["name"] for x in d["inventoryProperties"]["quantityNames"]]
        return names or FALLBACK_QUANTITY_NAMES
    except Exception:
        return FALLBACK_QUANTITY_NAMES

# ---------- Run Bulk Operation and download JSONL ----------

def run_bulk_inventory_export(
    jsonl_path: str = "inventory_snapshot.jsonl",
    filter_query: str | None = None,
) -> str:
    """
    Start a bulk query for all InventoryItems (+ per-location quantities) then download JSONL.
    Returns the path to the saved JSONL file.
    """
    names = discover_quantity_names()
    names_literal = ", ".join(f'"{n}"' for n in names)
    filter_clause = f'(query: "{filter_query}")' if filter_query else ""

    # IMPORTANT: every nested connection node (inventoryLevels, scheduledChanges) must include 'id'
    # or the bulk op will fail with:
    # "The parent 'node' field for a nested connection must select the 'id' field..." (see Shopify Community threads).
    bulk_query = f'''
    mutation {{
      bulkOperationRunQuery(
        query: """
        {{
          inventoryItems{filter_clause} {{
            edges {{
              node {{
                id legacyResourceId sku tracked requiresShipping updatedAt
                unitCost {{ amount currencyCode }}
                countryCodeOfOrigin
                harmonizedSystemCode
                variant {{
                  id legacyResourceId displayName sku
                  product {{ id legacyResourceId title handle vendor productType status }}
                }}
                inventoryLevels(first: 250) {{
                  edges {{
                    node {{
                      id
                      location {{
                        id legacyResourceId name
                        address {{ address1 address2 city provinceCode zip country countryCode }}
                      }}
                      quantities(names: [{names_literal}]) {{ name quantity updatedAt }}
                      scheduledChanges(first: 10) {{
                        edges {{ node {{ expectedAt fromName toName quantity ledgerDocumentUri }} }}
                      }}
                      updatedAt
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
      ) {{
        bulkOperation {{ id status }}
        userErrors {{ field message }}
      }}
    }}
    '''

    start = _gql(bulk_query)
    ue = start["bulkOperationRunQuery"]["userErrors"]
    if ue:
        raise RuntimeError(ue)

    # Poll until COMPLETED
    status_q = """
    query {
      currentBulkOperation {
        id status errorCode objectCount url partialDataUrl
      }
    }
    """
    terminal = {"COMPLETED", "FAILED", "CANCELED"}
    url = None
    while True:
        time.sleep(5)
        st = _gql(status_q)["currentBulkOperation"]
        print(f"[Bulk] status={st['status']} objects={st.get('objectCount')} url={bool(st.get('url'))}")
        if st["status"] in terminal:
            if st["status"] != "COMPLETED":
                raise RuntimeError(f"Bulk ended with {st['status']} error={st.get('errorCode')}")
            url = st["url"]
            break

    # Download JSONL
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(jsonl_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)

    return jsonl_path

# ---------- Flatten JSONL to CSV (one row per item x location) ----------

def jsonl_to_rows(jsonl_path: str, quantity_names: list[str] | None = None) -> list[dict]:
    """
    Read the JSONL produced by the bulk op and build flat rows:
    - join InventoryLevel lines back to their parent InventoryItem via __parentId
    - one row per (InventoryItem, Location)
    - columns for each quantity name
    """
    if quantity_names is None:
        quantity_names = discover_quantity_names()

    items: dict[str, dict] = {}              # key = InventoryItem gid
    levels_by_item: dict[str, list[dict]] = {}  # parent item gid -> [level nodes]
    sched_by_level: dict[str, list[dict]] = {}  # level gid -> [scheduledChange nodes]

    def _is_type(gid: str, typename: str) -> bool:
        return gid.startswith(f"gid://shopify/{typename}/")

    # First pass: load everything into maps
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            gid = obj.get("id", "")
            parent = obj.get("__parentId")

            if _is_type(gid, "InventoryItem"):
                items[gid] = obj

            elif _is_type(gid, "InventoryLevel"):
                # attach level to its parent item
                if parent:
                    levels_by_item.setdefault(parent, []).append(obj)

            elif _is_type(gid, "InventoryScheduledChange"):
                # attach scheduled change to its parent level
                if parent:
                    sched_by_level.setdefault(parent, []).append(obj)

    # Second pass: build flat rows
    rows: list[dict] = []
    for item_gid, item in items.items():
        base = {
            "inventory_item_gid": item_gid,
            "inventory_item_id": item.get("legacyResourceId"),
            "sku": item.get("sku"),
            "tracked": item.get("tracked"),
            "requiresShipping": item.get("requiresShipping"),
            "unitCost_amount": (item.get("unitCost") or {}).get("amount"),
            "unitCost_currency": (item.get("unitCost") or {}).get("currencyCode"),
            "countryCodeOfOrigin": item.get("countryCodeOfOrigin"),
            "harmonizedSystemCode": item.get("harmonizedSystemCode"),
            # Variant / Product
            "variant_gid": ((item.get("variant") or {}).get("id")),
            "variant_id": ((item.get("variant") or {}).get("legacyResourceId")),
            "variant_sku": ((item.get("variant") or {}).get("sku")),
            "variant_displayName": ((item.get("variant") or {}).get("displayName")),
            "product_gid": (((item.get("variant") or {}).get("product") or {}).get("id")),
            "product_id": (((item.get("variant") or {}).get("product") or {}).get("legacyResourceId")),
            "product_title": (((item.get("variant") or {}).get("product") or {}).get("title")),
            "product_handle": (((item.get("variant") or {}).get("product") or {}).get("handle")),
            "product_vendor": (((item.get("variant") or {}).get("product") or {}).get("vendor")),
            "product_type": (((item.get("variant") or {}).get("product") or {}).get("productType")),
            "product_status": (((item.get("variant") or {}).get("product") or {}).get("status")),
            "inventory_item_updatedAt": item.get("updatedAt"),
        }

        for lvl in levels_by_item.get(item_gid, []):
            # location
            loc = (lvl.get("location") or {})
            addr = (loc.get("address") or {})
            row = {
                **base,
                "location_gid": loc.get("id"),
                "location_id": loc.get("legacyResourceId"),
                "location_name": loc.get("name"),
                "location_address1": addr.get("address1"),
                "location_address2": addr.get("address2"),
                "location_city": addr.get("city"),
                "location_provinceCode": addr.get("provinceCode"),
                "location_zip": addr.get("zip"),
                "location_country": addr.get("country"),
                "location_countryCode": addr.get("countryCode"),
                "level_updatedAt": lvl.get("updatedAt"),
            }
            # quantities -> columns per name
            qmap = {n: None for n in quantity_names}
            for q in (lvl.get("quantities") or []):
                name = q.get("name")
                qty = q.get("quantity")
                if name in qmap:
                    qmap[name] = qty
            # scheduled changes (keep as JSON string for readability)
            sched = sched_by_level.get(lvl.get("id", ""), [])
            row["scheduled_changes_json"] = json.dumps(sched, ensure_ascii=False)

            rows.append({**row, **qmap})

    return rows

def write_csv(rows: list[dict], csv_path: str) -> str:
    """Write rows to CSV; infer header from keys union."""
    if not rows:
        # Ensure we still create an empty CSV with a small header
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["message"])
            w.writerow(["No rows produced"])
        return csv_path

    # union of all keys keeps future-proof columns (quantity states can vary)
    header_keys: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                header_keys.append(k)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header_keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return csv_path

# ---------- Script entrypoint ----------

if __name__ == "__main__":
    # 1) Run bulk export (optionally restrict with filter_query like 'sku:ABC*' or 'vendor:NIKE')
    jsonl_file = run_bulk_inventory_export(jsonl_path="inventory_snapshot.jsonl", filter_query=None)
    print(f"Saved JSONL → {jsonl_file}")

    # 2) Flatten to rows and write CSV
    quantity_names = discover_quantity_names()
    rows = jsonl_to_rows(jsonl_file, quantity_names)
    csv_file = "inventory_snapshot_per_location.csv"
    write_csv(rows, csv_file)
    print(f"Saved CSV → {csv_file} (rows={len(rows)})")