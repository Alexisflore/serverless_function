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
from api.lib.utils import get_store_context

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

_gql_restore_rate: float | None = None


def _graphql(
    query: str,
    variables: Dict[str, Any],
    max_retries: int = 5,
) -> Dict[str, Any]:
    global _gql_restore_rate

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

        cost = _extract_cost(body)

        if cost:
            if cost.get("restoreRate"):
                _gql_restore_rate = float(cost["restoreRate"])
            print(f"  [GQL] requested={cost.get('requestedQueryCost')} "
                  f"available={cost.get('currentlyAvailable')}/{cost.get('maximumAvailable')} "
                  f"restore={cost.get('restoreRate', _gql_restore_rate or '?')}/s",
                  file=sys.stderr, flush=True)

        errors = body.get("errors") or []
        if not errors:
            return body["data"]

        throttled = any(
            e.get("extensions", {}).get("code") == "THROTTLED" for e in errors
        )
        if throttled and attempt < max_retries - 1:
            wait = _calc_throttle_wait(cost, attempt)
            print(f"  [THROTTLED] waiting {wait}s (retry {attempt+1}/{max_retries})",
                  file=sys.stderr, flush=True)
            time.sleep(wait)
            continue

        raise RuntimeError(f"GraphQL errors: {json.dumps(errors, ensure_ascii=False)}")

    raise RuntimeError("Max retries exceeded")


def _extract_cost(body: Dict[str, Any]) -> Dict[str, Any] | None:
    for e in body.get("errors") or []:
        ext = e.get("extensions") or {}
        if "cost" in ext:
            return ext["cost"]
    ext_root = body.get("extensions") or {}
    return ext_root.get("cost")


def _calc_throttle_wait(cost: Dict[str, Any] | None, attempt: int) -> int:
    """Compute optimal wait time from Shopify cost data.

    Uses restoreRate from the response, or a cached value from a previous
    successful call, or falls back to a conservative estimate.
    """
    available = (cost.get("currentlyAvailable") or 0) if cost else 0
    needed = (cost.get("requestedQueryCost") or 525) if cost else 525
    deficit = max(needed - available, 0)

    restore = None
    if cost and cost.get("restoreRate"):
        restore = float(cost["restoreRate"])
    elif _gql_restore_rate:
        restore = _gql_restore_rate

    if restore and restore > 0:
        wait = int(deficit / restore) + 3
    else:
        wait = int(deficit / 50) + 5  # assume 50 pts/s as conservative default

    return max(wait, 5)


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
    """Normalize any timestamp to UTC 'YYYY-MM-DD HH:MM:SS'.
    Naive datetimes (no timezone) are assumed UTC."""
    if not raw:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        utc = parsed.astimezone(dt.timezone.utc)
        return utc.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return str(raw)


# ---------------------------------------------------------------------------
# REST API helpers (for supplementing missing fulfillment events)
# ---------------------------------------------------------------------------

def _shopify_rest(endpoint: str, max_retries: int = 3) -> Dict[str, Any]:
    """GET from Shopify REST Admin API with retry on 429 and network errors."""
    domain = os.getenv("SHOPIFY_STORE_DOMAIN", "").strip()
    token = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
    url = f"https://{domain}/admin/api/2026-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            if attempt < max_retries - 1:
                wait = 5 * (attempt + 1)
                print(f"  [REST RETRY] {exc.__class__.__name__} on {endpoint}, "
                      f"retry in {wait}s ({attempt+1}/{max_retries})",
                      file=sys.stderr, flush=True)
                time.sleep(wait)
                continue
            raise
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 2))
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"REST API max retries exceeded for {endpoint}")


def _extract_document_ids(events: List[Dict]) -> Tuple[Set[int], Set[int]]:
    """Extract numeric Order and DraftOrder IDs from reference_document_uri."""
    order_ids: Set[int] = set()
    draft_ids: Set[int] = set()
    for ev in events:
        uri = ev.get("reference_document_uri") or ""
        try:
            numeric = int(uri.rsplit("/", 1)[-1])
        except (ValueError, IndexError):
            continue
        if "/DraftOrder/" in uri:
            draft_ids.add(numeric)
        elif "/Order/" in uri:
            order_ids.add(numeric)
    return order_ids, draft_ids


def _resolve_draft_orders(
    draft_ids: Set[int],
    order_cache: Dict[int, Dict],
) -> Set[int]:
    """Fetch DraftOrders, resolve to their completed Order IDs."""
    resolved: Set[int] = set()
    for did in draft_ids:
        cache_key = f"draft_{did}"
        if cache_key not in order_cache:
            try:
                data = _shopify_rest(f"draft_orders/{did}.json")
                order_cache[cache_key] = data.get("draft_order") or {}
            except Exception as exc:
                print(f"  [WARN] Cannot fetch draft_order {did}: {exc}", file=sys.stderr)
                order_cache[cache_key] = None
                continue

        draft = order_cache[cache_key]
        if not draft:
            continue
        completed_order_id = draft.get("order_id")
        if completed_order_id:
            resolved.add(int(completed_order_id))
    return resolved


def _fetch_synthetic_fulfillment_events(
    adjustments: List[Dict],
    variant_id: int | None,
    location_id: int,
    order_cache: Dict[int, Dict] | None = None,
) -> List[Dict]:
    """Find fulfillment events missing from ShopifyQL and return synthetic events.

    For each Order (direct or resolved from DraftOrder) referenced in the
    ShopifyQL events, checks fulfillments at `location_id` and creates
    synthetic 'committed -qty' events for any missing fulfillment timestamp.
    """
    if order_cache is None:
        order_cache = {}

    order_ids, draft_ids = _extract_document_ids(adjustments)

    if draft_ids:
        resolved = _resolve_draft_orders(draft_ids, order_cache)
        order_ids |= resolved

    if not order_ids:
        return []

    existing_dts: List[dt.datetime] = []
    for ev in adjustments:
        raw = _normalize_ts(ev.get("second") or ev.get("day") or "")
        if raw:
            try:
                existing_dts.append(dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                pass

    TOLERANCE = dt.timedelta(seconds=2)

    def _ts_already_covered(ts_str: str) -> bool:
        if not ts_str:
            return False
        try:
            candidate = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return False
        return any(abs(candidate - ex) <= TOLERANCE for ex in existing_dts)

    synthetic: List[Dict] = []
    for oid in order_ids:
        if oid not in order_cache:
            try:
                order_cache[oid] = _shopify_rest(f"orders/{oid}.json")["order"]
            except Exception as exc:
                print(f"  [WARN] Cannot fetch order {oid}: {exc}", file=sys.stderr)
                order_cache[oid] = None
                continue

        order = order_cache[oid]
        if order is None:
            continue
        for ful in order.get("fulfillments", []):
            if ful.get("location_id") != location_id:
                continue
            if ful.get("status") != "success":
                continue

            ts_utc = _normalize_ts(ful.get("created_at", ""))
            if _ts_already_covered(ts_utc):
                continue

            qty = 0
            if variant_id:
                for fli in ful.get("line_items", []):
                    if fli.get("variant_id") == variant_id:
                        qty += fli.get("quantity", 0)

            if qty <= 0:
                continue

            ts_iso = ts_utc.replace(" ", "T") + "Z" if "T" not in ts_utc else ts_utc
            synthetic.append({
                "second": ts_iso,
                "inventory_state": "committed",
                "inventory_adjustment_change": -qty,
                "inventory_change_reason": "fulfillment",
                "reference_document_type": "Fulfillment",
                "_synthetic": True,
            })

    return synthetic


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
  reference_document_uri,
  inventory_state,
  second
GROUP BY
  second,
  inventory_item_id,
  inventory_location_name,
  inventory_change_reason,
  reference_document_type,
  reference_document_uri,
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

    # 3. Supplement with missing fulfillment events from REST
    synthetic = _fetch_synthetic_fulfillment_events(
        adjustments, variant_id, location_id,
    )
    if synthetic:
        print(
            f"  [SYNTHETIC] Added {len(synthetic)} missing fulfillment event(s) "
            f"for item={inventory_item_id} loc={location_id}",
            file=sys.stderr,
        )
        adjustments = adjustments + synthetic

    # 4. Group adjustments by timestamp
    events_by_ts: Dict[str, List[Dict]] = defaultdict(list)
    for ev in adjustments:
        ts = ev.get("second") or ""
        events_by_ts[ts].append(ev)

    # 5. Compute total delta from ALL adjustments (including synthetic)
    total_delta: Dict[str, int] = {s: 0 for s in STATE_FIELDS}
    for ev in adjustments:
        st = (ev.get("inventory_state") or "").strip().lower()
        ch = _safe_int(ev.get("inventory_adjustment_change"))
        if st in total_delta:
            total_delta[st] += ch

    # 5. Compute initial state: current stock - sum of all adjustments
    running = {s: current[s] - total_delta.get(s, 0) for s in STATE_FIELDS}

    # Negative-shift fallback: if any state goes negative during the
    # replay (due to missing fulfillment events from deleted orders),
    # pre-compute the minimum value per state and shift initial upward.
    sim = {s: running[s] for s in STATE_FIELDS}
    mins = {s: sim[s] for s in STATE_FIELDS}
    for ev in adjustments:
        st = (ev.get("inventory_state") or "").strip().lower()
        ch = _safe_int(ev.get("inventory_adjustment_change"))
        if st in sim:
            sim[st] += ch
            if sim[st] < mins[st]:
                mins[st] = sim[st]
    for s in STATE_FIELDS:
        if mins[s] < 0:
            running[s] += -mins[s]

    # 6. Walk forward through events chronologically, inserting new ones
    _ctx = get_store_context()

    insert_sql = """
    INSERT INTO inventory_history (
        inventory_item_id, location_id, variant_id, product_id, sku,
        available, committed, damaged, incoming,
        on_hand, quality_control, reserved, safety_stock,
        available_stock_movement,
        recorded_at, change_type, change_comment,
        data_source, company_code, commercial_organisation
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
            _ctx["data_source"], _ctx["company_code"], _ctx["commercial_organisation"],
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
