"""
ShopifyQL helpers for inventory adjustment history.

Provides functions to:
- Call ShopifyQL with retry/throttle handling
- Fetch location name <-> id mapping
- Fetch adjustment events for a specific (item, location) pair on a given day
- Insert adjustment events into inventory_history with correct absolute stock values
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

SHOPIFYQL_GQL = """
query ShopifyQL($q: String!) {
  shopifyqlQuery(query: $q) {
    tableData {
      columns { name dataType displayName }
      rows
    }
    parseErrors
  }
}
""".strip()

LOCATIONS_GQL = """
query Locations($cursor: String) {
  locations(first: 250, after: $cursor) {
    edges { node { id name } }
    pageInfo { hasNextPage endCursor }
  }
}
""".strip()

STATE_FIELDS = [
    "available", "committed", "damaged", "incoming",
    "quality_control", "reserved", "safety_stock",
]


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _graphql(
    query: str,
    variables: Dict[str, Any],
    max_retries: int = 5,
) -> Dict[str, Any]:
    shop = os.getenv("SHOPIFY_STORE_DOMAIN", "").strip()
    token = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
    api_version = "2026-01"
    url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }

    for attempt in range(max_retries):
        resp = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=90)
        resp.raise_for_status()
        body = resp.json()

        errors = body.get("errors") or []
        if not errors:
            return body["data"]

        throttled = any(
            e.get("extensions", {}).get("code") == "THROTTLED" for e in errors
        )
        if throttled and attempt < max_retries - 1:
            wait = 60
            print(f"  [THROTTLED] waiting {wait}s (retry {attempt+1}/{max_retries}) ...", file=sys.stderr, flush=True)
            time.sleep(wait)
            continue

        raise RuntimeError(f"GraphQL errors: {json.dumps(errors, ensure_ascii=False)}")

    raise RuntimeError("Max retries exceeded")


def call_shopifyql(q: str) -> Dict[str, Any]:
    data = _graphql(SHOPIFYQL_GQL, {"q": q})
    payload = data["shopifyqlQuery"]
    if payload.get("parseErrors"):
        raise RuntimeError(f"ShopifyQL parseErrors: {payload['parseErrors']}")
    return payload.get("tableData") or {}


def _tabledata_to_dicts(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols = [c["name"] for c in (table_data.get("columns") or [])]
    out: List[Dict[str, Any]] = []
    for r in (table_data.get("rows") or []):
        if isinstance(r, dict):
            out.append(r)
        elif isinstance(r, list):
            out.append({cols[i]: r[i] for i in range(min(len(cols), len(r)))})
    return out


def _safe_int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return 0


def _normalize_ts(raw: str) -> str:
    if not raw:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return str(raw)


# ---------------------------------------------------------------------------
# Location mapping
# ---------------------------------------------------------------------------

_location_cache: Dict[str, str] | None = None


def fetch_all_locations(force_refresh: bool = False) -> Dict[str, str]:
    """Returns {numeric_location_id: location_name} with in-memory cache."""
    global _location_cache
    if _location_cache is not None and not force_refresh:
        return _location_cache

    id_to_name: Dict[str, str] = {}
    cursor = None
    while True:
        variables: Dict[str, Any] = {}
        if cursor:
            variables["cursor"] = cursor
        data = _graphql(LOCATIONS_GQL, variables)
        for edge in data["locations"]["edges"]:
            gid = edge["node"]["id"]
            name = edge["node"]["name"]
            numeric_id = gid.rsplit("/", 1)[-1]
            id_to_name[numeric_id] = name
        pi = data["locations"]["pageInfo"]
        if not pi["hasNextPage"]:
            break
        cursor = pi["endCursor"]

    _location_cache = id_to_name
    return id_to_name


# ---------------------------------------------------------------------------
# Fetch adjustments for a (item, location) pair
# ---------------------------------------------------------------------------

def fetch_adjustments_for_pair(
    inventory_item_id: int | str,
    location_name: str,
    day: dt.date,
) -> List[Dict[str, Any]]:
    """
    Query ShopifyQL for adjustment events for a specific (item, location) on a given day.
    Returns a list of raw event dicts from ShopifyQL.
    """
    next_day = day + dt.timedelta(days=1)
    escaped_name = str(location_name).replace("'", "\\'")
    q = f"""
FROM inventory_adjustment_history
SHOW
  inventory_item_id,
  inventory_location_name,
  inventory_adjustment_change,
  inventory_adjustment_count,
  inventory_change_reason,
  reference_document_type,
  inventory_state,
  second
GROUP BY
  second,
  inventory_item_id,
  inventory_location_name,
  inventory_change_reason,
  reference_document_type,
  inventory_state
HAVING inventory_adjustment_change != 0
SINCE {day.isoformat()}
UNTIL {next_day.isoformat()}
WHERE inventory_item_id = {inventory_item_id}
  AND inventory_location_name = '{escaped_name}'
ORDER BY second ASC
LIMIT 500
""".strip()

    table = call_shopifyql(q)
    rows = _tabledata_to_dicts(table)

    # Deduplicate (ShopifyQL sometimes returns each event twice)
    seen: Set[tuple] = set()
    unique: List[Dict[str, Any]] = []
    for r in rows:
        key = (
            str(r.get("inventory_item_id", "")),
            str(r.get("second", "")),
            str(r.get("inventory_state", "")),
            str(r.get("inventory_change_reason", "")),
            str(r.get("inventory_adjustment_change", "")),
        )
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique


# ---------------------------------------------------------------------------
# Insert adjustments into inventory_history
# ---------------------------------------------------------------------------

def insert_adjustments_into_history(
    conn,
    inventory_item_id: int,
    location_id: int,
    adjustments: List[Dict[str, Any]],
) -> int:
    """
    Insert ShopifyQL adjustment events into inventory_history.
    Computes absolute stock values using: initial = current - total_delta.
    Returns number of rows inserted.
    """
    if not adjustments:
        return 0

    cur = conn.cursor()

    # 1. Get current stock from inventory table
    cur.execute("""
        SELECT available, committed, damaged, incoming,
               quality_control, reserved, safety_stock,
               variant_id, product_id, sku
        FROM inventory
        WHERE inventory_item_id = %s AND location_id = %s
    """, (inventory_item_id, location_id))
    inv_row = cur.fetchone()
    if not inv_row:
        cur.close()
        return 0

    current = {
        "available": inv_row[0] or 0, "committed": inv_row[1] or 0,
        "damaged": inv_row[2] or 0, "incoming": inv_row[3] or 0,
        "quality_control": inv_row[4] or 0, "reserved": inv_row[5] or 0,
        "safety_stock": inv_row[6] or 0,
    }
    variant_id = inv_row[7]
    product_id = inv_row[8]
    sku = inv_row[9]

    # 2. Load existing keys for dedup
    existing_keys: Set[str] = set()
    cur.execute("""
        SELECT recorded_at FROM inventory_history
        WHERE inventory_item_id = %s AND location_id = %s
    """, (inventory_item_id, location_id))
    for r in cur:
        ts = r[0].strftime("%Y-%m-%d %H:%M:%S") if r[0] else ""
        existing_keys.add(ts)

    # 3. Group adjustments by timestamp
    events_by_ts: Dict[str, List[Dict]] = defaultdict(list)
    for ev in adjustments:
        ts = ev.get("second") or ""
        events_by_ts[ts].append(ev)

    # 4. Compute total delta from ALL ShopifyQL adjustments
    total_delta: Dict[str, int] = {s: 0 for s in STATE_FIELDS}
    for ev in adjustments:
        st = (ev.get("inventory_state") or "").strip().lower()
        ch = _safe_int(ev.get("inventory_adjustment_change"))
        if st in total_delta:
            total_delta[st] += ch

    # 5. Compute initial state: current stock - sum of all adjustments
    #    This is always correct because `current` (from inventory table, just
    #    UPSERTED in Phase A) reflects the state AFTER all events, and
    #    `total_delta` is the sum of all ShopifyQL changes for the period.
    running = {s: current[s] - total_delta.get(s, 0) for s in STATE_FIELDS}

    # 6. Walk forward through events chronologically, inserting new ones
    insert_sql = """
    INSERT INTO inventory_history (
        inventory_item_id, location_id, variant_id, product_id, sku,
        available, committed, damaged, incoming,
        on_hand, quality_control, reserved, safety_stock,
        available_stock_movement,
        recorded_at, change_type, change_comment
    ) VALUES %s
    """

    batch: List[tuple] = []
    for timestamp in sorted(events_by_ts.keys()):
        ts_events = events_by_ts[timestamp]
        ts_normalized = _normalize_ts(timestamp)

        avail_movement = 0
        reason = ""
        doc_type = ""
        for ev in ts_events:
            inv_state = (ev.get("inventory_state") or "").strip().lower()
            change = _safe_int(ev.get("inventory_adjustment_change"))
            if inv_state in running:
                running[inv_state] += change
            if inv_state == "available":
                avail_movement += change
            if not reason:
                reason = ev.get("inventory_change_reason") or ""
                doc_type = ev.get("reference_document_type") or ""

        if ts_normalized in existing_keys:
            continue

        on_hand = sum(running.values())
        comment_parts = [p for p in [reason, doc_type] if p]
        comment = " | ".join(comment_parts) if comment_parts else None

        row = (
            inventory_item_id, location_id, variant_id, product_id, sku,
            running["available"], running["committed"],
            running["damaged"], running["incoming"],
            on_hand, running["quality_control"],
            running["reserved"], running["safety_stock"],
            avail_movement, timestamp,
            "ADJUSTMENT", comment,
        )
        batch.append(row)
        existing_keys.add(ts_normalized)

    inserted = 0
    if batch:
        psycopg2.extras.execute_values(cur, insert_sql, batch)
        conn.commit()
        inserted = len(batch)

    cur.close()
    return inserted
