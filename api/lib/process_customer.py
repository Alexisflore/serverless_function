#!/usr/bin/env python3
"""
Shopify Customers → PostgreSQL ETL
Pattern: calqué sur ton process_inventory script
"""

import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import requests
import psycopg2
from dotenv import load_dotenv
from api.lib.utils import get_store_context

load_dotenv()

# ---------------------------------------------------------------------------
# Config + utilitaires
# ---------------------------------------------------------------------------

ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

GRAPHQL_URL = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

def _shopify_headers() -> Dict[str, str]:
    return HEADERS

def _pg_connect():
    """Connexion PostgreSQL (utilise DATABASE_URL si présent sinon variables séparées)."""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
            user=os.getenv("SUPABASE_USER"),
            pw=os.getenv("SUPABASE_PASSWORD"),
            host=os.getenv("SUPABASE_HOST"),
            port=os.getenv("SUPABASE_PORT"),
            db=os.getenv("SUPABASE_DB_NAME"),
        )
    return psycopg2.connect(db_url)

def _gql(query: str, variables: dict | None = None, timeout: int = 60) -> dict:
    r = requests.post(GRAPHQL_URL, headers=_shopify_headers(), json={"query": query, "variables": variables or {}}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(data["errors"])
    return data["data"]

def _iso_to_dt(date_str: str) -> datetime:
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    return datetime.fromisoformat(date_str)

# ---------------------------------------------------------------------------
# Bulk query pour Customers (filtre updated_at >=)
# ---------------------------------------------------------------------------

_CUSTOMER_BULK_QUERY_TEMPLATE = '''
mutation {{
  bulkOperationRunQuery(
    query: """
    {{
      customers(query: "updated_at:>='{formatted_date}'") {{
        edges {{
          node {{
            id
            legacyResourceId
            firstName
            lastName
            displayName
            email
            phone
            numberOfOrders
            amountSpent {{ amount currencyCode }}
            createdAt
            updatedAt
            tags
            note
            verifiedEmail
            validEmailAddress
            addresses {{ address1 address2 city provinceCode zip country countryCode }}
            emailMarketingConsent {{ marketingState consentUpdatedAt marketingOptInLevel }}
            smsMarketingConsent {{ marketingState consentUpdatedAt marketingOptInLevel consentCollectedFrom }}
            defaultAddress {{ address1 address2 city province provinceCode country countryCodeV2 zip phone firstName lastName company }}
            metafields {{
              edges {{
                node {{ namespace key value type }}
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

def get_bulk_customers_data_filtered(updated_since: datetime) -> List[Dict[str, Any]]:
    """
    Lance une bulk operation pour customers mis à jour depuis updated_since,
    attend la complétion, puis stream/process le JSONL (via process_customers_data_from_url).
    """
    formatted_date = updated_since.isoformat()
    mutation = _CUSTOMER_BULK_QUERY_TEMPLATE.format(formatted_date=formatted_date)

    print(f"Démarrage bulk customers updated_since={formatted_date}")
    start = _gql(mutation)
    ue = start["bulkOperationRunQuery"]["userErrors"]
    if ue:
        raise RuntimeError(ue)

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
        print(f"[Bulk customers] status={st['status']} objects={st.get('objectCount')} url={bool(st.get('url'))}")
        if st["status"] in terminal:
            if st["status"] != "COMPLETED":
                raise RuntimeError(f"Bulk ended with {st['status']} error={st.get('errorCode')}")
            url = st["url"]
            break

    if url:
        return process_customers_data_from_url(url)
    else:
        return []

# ---------------------------------------------------------------------------
# Traitement JSONL stream (URL fournie par Shopify)
# ---------------------------------------------------------------------------

def _build_customer_record(gid: str, node: dict, metafields_list: list) -> Dict[str, Any]:
    """Build a customer record dict from a parsed JSONL node + its metafield children."""
    if "node" in node:
        node = node["node"]

    amt = node.get("amountSpent") or {}
    amount = amt.get("amount") if isinstance(amt, dict) else None
    currency = amt.get("currencyCode") if isinstance(amt, dict) else None

    mf_dict = {}
    for mf in metafields_list:
        ns = mf.get("namespace", "")
        key = mf.get("key", "")
        mf_dict[f"{ns}.{key}"] = mf.get("value")

    return {
        "customer_id": node.get("legacyResourceId"),
        "gid": gid,
        "first_name": node.get("firstName"),
        "last_name": node.get("lastName"),
        "display_name": node.get("displayName"),
        "email": node.get("email"),
        "phone": node.get("phone"),
        "number_of_orders": node.get("numberOfOrders"),
        "amount_spent": amount,
        "amount_spent_currency": currency,
        "created_at": node.get("createdAt"),
        "shop_updated_at": node.get("updatedAt"),
        "tags": node.get("tags"),
        "note": node.get("note"),
        "verified_email": node.get("verifiedEmail"),
        "valid_email_address": node.get("validEmailAddress"),
        "addresses": json.dumps(node.get("addresses") or [], ensure_ascii=False),
        "email_marketing_consent": json.dumps(node.get("emailMarketingConsent") or {}, ensure_ascii=False),
        "sms_marketing_consent": json.dumps(node.get("smsMarketingConsent") or {}, ensure_ascii=False),
        "default_address": json.dumps(node.get("defaultAddress") or {}, ensure_ascii=False),
        "metafields": json.dumps(mf_dict, ensure_ascii=False) if mf_dict else None,
    }


def process_customers_data_from_url(url: str) -> List[Dict[str, Any]]:
    """
    Stream the JSONL at `url`, parse objects and build customer records ready for DB insert.
    Metafields arrive as separate JSONL lines with __parentId pointing to the customer GID.
    """
    customers: Dict[str, dict] = {}
    metafields_by_parent: Dict[str, list] = {}

    def _is_type(gid: str, typename: str) -> bool:
        return isinstance(gid, str) and gid.startswith(f"gid://shopify/{typename}/")

    print("Streaming customers depuis URL bulk...")
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            gid = obj.get("id", "")
            if _is_type(gid, "Customer"):
                customers[gid] = obj
            elif "__parentId" in obj and "namespace" in obj and "key" in obj:
                parent_id = obj["__parentId"]
                metafields_by_parent.setdefault(parent_id, []).append(obj)

    print(f"Construits {len(customers)} customers depuis le bulk ({sum(len(v) for v in metafields_by_parent.values())} metafields)")
    records: List[Dict[str, Any]] = []
    for gid, node in customers.items():
        mf_list = metafields_by_parent.get(gid, [])
        records.append(_build_customer_record(gid, node, mf_list))

    print(f"Généré {len(records)} enregistrements customers")
    return records

def process_customers_jsonl(jsonl_path: str) -> List[Dict[str, Any]]:
    """
    Alternative: lire un fichier JSONL local (même logique que le streaming).
    """
    customers: Dict[str, dict] = {}
    metafields_by_parent: Dict[str, list] = {}

    def _is_type(gid: str, typename: str) -> bool:
        return isinstance(gid, str) and gid.startswith(f"gid://shopify/{typename}/")

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            gid = obj.get("id", "")
            if _is_type(gid, "Customer"):
                customers[gid] = obj
            elif "__parentId" in obj and "namespace" in obj and "key" in obj:
                parent_id = obj["__parentId"]
                metafields_by_parent.setdefault(parent_id, []).append(obj)

    records: List[Dict[str, Any]] = []
    for gid, node in customers.items():
        mf_list = metafields_by_parent.get(gid, [])
        records.append(_build_customer_record(gid, node, mf_list))
    return records

# ---------------------------------------------------------------------------
# Persistance en DB (UPSERT)
# ---------------------------------------------------------------------------

def _truncate_field(value: str, max_length: int) -> str:
    """Tronque un champ à la longueur maximale autorisée."""
    if value and len(value) > max_length:
        return value[:max_length]
    return value

def process_customer_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = {"inserted": 0, "updated": 0, "skipped": 0, "errors": []}
    if not records:
        return stats

    conn = _pg_connect()
    cur = conn.cursor()
    
    # Limites de caractères pour chaque champ
    field_limits = {
        'first_name': 100,
        'last_name': 100,
        'display_name': 200,
        'email': 255,
        'phone': 50,
        'amount_spent_currency': 10,
        'gid': 255
    }

    _ctx = get_store_context()

    upsert_q = """
    INSERT INTO customers (
        customer_id, gid, first_name, last_name, display_name, email, phone,
        number_of_orders, amount_spent, amount_spent_currency,
        created_at, shop_updated_at, tags, note, verified_email, valid_email_address,
        addresses, synced_at,
        data_source, company_code, commercial_organisation,
        email_marketing_consent, sms_marketing_consent, default_address, metafields
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (customer_id)
    DO UPDATE SET
        gid = EXCLUDED.gid,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        display_name = EXCLUDED.display_name,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        number_of_orders = EXCLUDED.number_of_orders,
        amount_spent = EXCLUDED.amount_spent,
        amount_spent_currency = EXCLUDED.amount_spent_currency,
        created_at = EXCLUDED.created_at,
        shop_updated_at = EXCLUDED.shop_updated_at,
        tags = EXCLUDED.tags,
        note = EXCLUDED.note,
        verified_email = EXCLUDED.verified_email,
        valid_email_address = EXCLUDED.valid_email_address,
        addresses = EXCLUDED.addresses,
        updated_at = NOW(),
        synced_at = EXCLUDED.synced_at,
        email_marketing_consent = EXCLUDED.email_marketing_consent,
        sms_marketing_consent = EXCLUDED.sms_marketing_consent,
        default_address = EXCLUDED.default_address,
        metafields = EXCLUDED.metafields
    """

    try:
        for i, r in enumerate(records):
            if i % 100 == 0 and i > 0:
                print(f"Progress: {i}/{len(records)}")
            
            # Démarrer une nouvelle transaction pour chaque record pour éviter les blocages
            try:
                created = _iso_to_dt(r["created_at"]) if r.get("created_at") else None
                shop_updated = _iso_to_dt(r["shop_updated_at"]) if r.get("shop_updated_at") else None

                # Appliquer la troncature aux champs texte
                truncated_data = {}
                for field, limit in field_limits.items():
                    value = r.get(field)
                    if value:
                        truncated_data[field] = _truncate_field(str(value), limit)
                    else:
                        truncated_data[field] = value

                params = (
                    r.get("customer_id"),
                    truncated_data.get("gid"),
                    truncated_data.get("first_name"),
                    truncated_data.get("last_name"),
                    truncated_data.get("display_name"),
                    truncated_data.get("email"),
                    truncated_data.get("phone"),
                    r.get("number_of_orders"),
                    r.get("amount_spent"),
                    truncated_data.get("amount_spent_currency"),
                    created,
                    shop_updated,
                    r.get("tags"),
                    r.get("note"),
                    r.get("verified_email"),
                    r.get("valid_email_address"),
                    r.get("addresses", "[]"),
                    datetime.now(),
                    _ctx["data_source"], _ctx["company_code"], _ctx["commercial_organisation"],
                    r.get("email_marketing_consent"),
                    r.get("sms_marketing_consent"),
                    r.get("default_address"),
                    r.get("metafields"),
                )

                # check exist (for stats)
                cur.execute("SELECT 1 FROM customers WHERE customer_id = %s", (r.get("customer_id"),))
                exists = cur.fetchone()
                cur.execute(upsert_q, params)
                
                # Commit immédiatement pour éviter les transactions annulées
                conn.commit()
                
                if exists:
                    stats["updated"] += 1
                else:
                    stats["inserted"] += 1

            except Exception as exc:
                # Rollback de la transaction en cas d'erreur
                conn.rollback()
                stats["skipped"] += 1
                stats["errors"].append(str(exc))
                print(f"Error inserting customer_id={r.get('customer_id')}: {exc}")
                
                # Log des données problématiques pour débogage
                if "too long" in str(exc).lower():
                    print(f"  Données tronquées:")
                    for field, limit in field_limits.items():
                        value = r.get(field)
                        if value and len(str(value)) > limit:
                            print(f"    {field}: {len(str(value))}/{limit} chars - '{str(value)[:50]}...'")

        # Pas besoin de commit final car on commit après chaque record
    except Exception as exc:
        conn.rollback()
        stats["errors"].append(str(exc))
    finally:
        cur.close()
        conn.close()

    return stats

# ---------------------------------------------------------------------------
# Orchestrateurs
# ---------------------------------------------------------------------------

def sync_customers_since_date(dt_since: datetime) -> Dict[str, Any]:
    try:
        records = get_bulk_customers_data_filtered(dt_since)
        stats = process_customer_records(records)
        return {"success": True, "records_processed": len(records), "stats": stats}
    except Exception as e:
        return {"success": False, "error": str(e), "records_processed": 0, "stats": {"errors": [str(e)]}}

def sync_customers_last_hours(hours: int = 24) -> Dict[str, Any]:
    since = datetime.now() - timedelta(hours=hours)
    return sync_customers_since_date(since)

def sync_customers_last_days(days: int = 1) -> Dict[str, Any]:
    since = datetime.now() - timedelta(days=days)
    return sync_customers_since_date(since)

# ---------------------------------------------------------------------------
# Example run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Start sync customers (last 24h) ===")
    res = sync_customers_last_hours(24)
    print(json.dumps(res, indent=2, default=str))