#!/usr/bin/env python3
"""
Shopify Inventory → PostgreSQL ETL
Synchronise les données d'inventaire avec filtrage par date updated_at
Basé sur le pattern de process_transactions.py
"""

import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# 1. Configuration et utilitaires de base
# ---------------------------------------------------------------------------

ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
API_VERSION = "2025-01"

GRAPHQL_URL = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Accept": "application/json",
    "Content-Type": "application/json",
}

FALLBACK_QUANTITY_NAMES = [
    "incoming", "on_hand", "available", "committed",
    "reserved", "damaged", "safety_stock", "quality_control"
]

def _shopify_headers() -> Dict[str, str]:
    return HEADERS

def _pg_connect():
    """Connexion PostgreSQL"""
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
    """POST a GraphQL request; raise on errors."""
    r = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables or {}}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(data["errors"])
    return data["data"]

def _iso_to_dt(date_str: str) -> datetime:
    """Convertit 2025-03-26T19:11:42-04:00 → obj datetime en UTC."""
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    return datetime.fromisoformat(date_str)

# ---------------------------------------------------------------------------
# 2. Découverte des types de quantités
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# 3. Extraction des données d'inventaire avec filtrage par date
# ---------------------------------------------------------------------------

def get_bulk_inventory_data_filtered(
    updated_since: datetime
) -> List[Dict[str, Any]]:
    """
    Start a bulk query for InventoryItems updated since a specific date.
    Returns the processed inventory records directly without creating files.
    """
    names = discover_quantity_names()
    names_literal = ", ".join(f'"{n}"' for n in names)
    
    # Format de la date pour Shopify GraphQL (ISO 8601)
    formatted_date = updated_since.isoformat()
    
    # Requête GraphQL avec filtrage par date
    bulk_query = f'''
    mutation {{
      bulkOperationRunQuery(
        query: """
        {{
          inventoryItems(query: "updated_at:>='{formatted_date}'") {{
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

    print(f"Démarrage de l'export bulk pour les items mis à jour depuis {formatted_date}")
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

    # Process data directly from URL without saving to file
    if url:
        print("Traitement des données directement depuis l'URL")
        return process_inventory_data_from_url(url, names)
    else:
        print("Aucune donnée à traiter (pas de modifications depuis la date spécifiée)")
        return []

# ---------------------------------------------------------------------------
# 4. Traitement et transformation des données directement depuis l'URL
# ---------------------------------------------------------------------------

def process_inventory_data_from_url(url: str, quantity_names: list[str]) -> List[Dict[str, Any]]:
    """
    Process inventory data directly from Shopify bulk operation URL without creating files.
    Returns inventory records ready for database insertion.
    """
    items: dict[str, dict] = {}              # key = InventoryItem gid
    levels_by_item: dict[str, list[dict]] = {}  # parent item gid -> [level nodes]
    sched_by_level: dict[str, list[dict]] = {}  # level gid -> [scheduledChange nodes]

    def _is_type(gid: str, typename: str) -> bool:
        return gid.startswith(f"gid://shopify/{typename}/")

    # Stream and process data directly from URL
    print("Traitement des données en streaming depuis Shopify")
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        
        # Process line by line without saving to disk
        for line in resp.iter_lines(decode_unicode=True):
            if line:  # Skip empty lines
                try:
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
                except json.JSONDecodeError as e:
                    print(f"Erreur de parsing JSON sur la ligne: {line[:100]}... - {e}")
                    continue

    # Build inventory records
    inventory_records: List[Dict[str, Any]] = []
    print(f"Construction des enregistrements d'inventaire pour {len(items)} items")
    
    for item_gid, item in items.items():
        base_item_data = {
            "inventory_item_id": item.get("legacyResourceId"),
            "sku": item.get("sku"),
            "variant_id": ((item.get("variant") or {}).get("legacyResourceId")),
            "product_id": (((item.get("variant") or {}).get("product") or {}).get("legacyResourceId")),
            "inventory_item_updated_at": item.get("updatedAt"),
        }

        for lvl in levels_by_item.get(item_gid, []):
            # location info
            loc = (lvl.get("location") or {})
            
            record = {
                **base_item_data,
                "location_id": loc.get("legacyResourceId"),
                "last_updated_at": lvl.get("updatedAt"),
            }
            
            # quantities -> columns per name
            qmap = {name: 0 for name in quantity_names}  # Default to 0
            for q in (lvl.get("quantities") or []):
                name = q.get("name")
                qty = q.get("quantity", 0)
                if name in qmap:
                    qmap[name] = qty
            
            # Add quantity columns to record
            record.update(qmap)
            
            # scheduled changes (keep as JSON string)
            sched = sched_by_level.get(lvl.get("id", ""), [])
            record["scheduled_changes"] = json.dumps(sched, ensure_ascii=False)

            inventory_records.append(record)

    print(f"Généré {len(inventory_records)} enregistrements d'inventaire")
    return inventory_records

def process_inventory_jsonl(jsonl_path: str, quantity_names: list[str] | None = None) -> List[Dict[str, Any]]:
    """
    Read the JSONL produced by the bulk op and build inventory records:
    - one record per (InventoryItem, Location)
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
    print(f"Traitement du fichier JSONL: {jsonl_path}")
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

    # Second pass: build inventory records
    inventory_records: List[Dict[str, Any]] = []
    print(f"Construction des enregistrements d'inventaire pour {len(items)} items")
    
    for item_gid, item in items.items():
        base_item_data = {
            "inventory_item_id": item.get("legacyResourceId"),
            "sku": item.get("sku"),
            "variant_id": ((item.get("variant") or {}).get("legacyResourceId")),
            "product_id": (((item.get("variant") or {}).get("product") or {}).get("legacyResourceId")),
            "inventory_item_updated_at": item.get("updatedAt"),
        }

        for lvl in levels_by_item.get(item_gid, []):
            # location info
            loc = (lvl.get("location") or {})
            
            record = {
                **base_item_data,
                "location_id": loc.get("legacyResourceId"),
                "last_updated_at": lvl.get("updatedAt"),
            }
            
            # quantities -> columns per name
            qmap = {name: 0 for name in quantity_names}  # Default to 0
            for q in (lvl.get("quantities") or []):
                name = q.get("name")
                qty = q.get("quantity", 0)
                if name in qmap:
                    qmap[name] = qty
            
            # Add quantity columns to record
            record.update(qmap)
            
            # scheduled changes (keep as JSON string)
            sched = sched_by_level.get(lvl.get("id", ""), [])
            record["scheduled_changes"] = json.dumps(sched, ensure_ascii=False)

            inventory_records.append(record)

    print(f"Généré {len(inventory_records)} enregistrements d'inventaire")
    return inventory_records

# ---------------------------------------------------------------------------
# 5. Extraction par fenêtrage temporel
# ---------------------------------------------------------------------------

def get_inventory_since_date(dt_since: datetime) -> List[Dict[str, Any]]:
    """
    Récupère les données d'inventaire mises à jour depuis une date donnée.
    Traite les données directement en mémoire sans créer de fichiers temporaires.
    """
    print(f"Récupération de l'inventaire mis à jour depuis {dt_since.isoformat()}")
    
    # Récupération et traitement direct des données
    inventory_records = get_bulk_inventory_data_filtered(dt_since)
    
    return inventory_records

def get_inventory_between_dates(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """
    Récupère les données d'inventaire mises à jour entre deux dates.
    Note: Shopify GraphQL ne supporte que updated_at:>= donc on filtre côté client.
    """
    print(f"Récupération de l'inventaire entre {start.isoformat()} et {end.isoformat()}")
    
    # Récupère depuis la date de début
    all_records = get_inventory_since_date(start)
    
    # Filtre côté client pour la date de fin
    filtered_records = []
    for record in all_records:
        updated_at_str = record.get("last_updated_at") or record.get("inventory_item_updated_at")
        if updated_at_str:
            updated_at = _iso_to_dt(updated_at_str)
            if updated_at <= end:
                filtered_records.append(record)
    
    print(f"Filtré {len(filtered_records)} enregistrements sur {len(all_records)} total")
    return filtered_records

# ---------------------------------------------------------------------------
# 6. Persistance en base de données
# ---------------------------------------------------------------------------

def process_inventory_records(records: List[Dict[str, Any]]) -> Dict[str, int | list]:
    """
    Insère ou met à jour les enregistrements d'inventaire dans PostgreSQL.
    """
    print(f"Début du traitement de {len(records)} enregistrements d'inventaire...")
    stats = {
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    }

    if not records:
        print("Aucun enregistrement à traiter.")
        return stats

    print("Connexion à la base de données...")
    conn = _pg_connect()
    cur = conn.cursor()

    # Requête d'insertion (UPSERT avec ON CONFLICT)
    upsert_q = """
        INSERT INTO inventory (
            inventory_item_id, location_id, variant_id, product_id, sku,
            available, committed, damaged, incoming, on_hand, 
            quality_control, reserved, safety_stock,
            last_updated_at, scheduled_changes, synced_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (inventory_item_id, location_id)
        DO UPDATE SET
            variant_id = EXCLUDED.variant_id,
            product_id = EXCLUDED.product_id,
            sku = EXCLUDED.sku,
            available = EXCLUDED.available,
            committed = EXCLUDED.committed,
            damaged = EXCLUDED.damaged,
            incoming = EXCLUDED.incoming,
            on_hand = EXCLUDED.on_hand,
            quality_control = EXCLUDED.quality_control,
            reserved = EXCLUDED.reserved,
            safety_stock = EXCLUDED.safety_stock,
            last_updated_at = EXCLUDED.last_updated_at,
            scheduled_changes = EXCLUDED.scheduled_changes,
            updated_at = NOW(),
            synced_at = EXCLUDED.synced_at
    """

    try:
        for i, record in enumerate(records):
            if i % 100 == 0 and i > 0:
                print(f"Progression: {i}/{len(records)} enregistrements traités")
            
            try:
                # Conversion des dates
                last_updated_at = None
                if record.get("last_updated_at"):
                    last_updated_at = _iso_to_dt(record["last_updated_at"])
                
                # Préparation des paramètres
                params = (
                    record.get("inventory_item_id"),
                    record.get("location_id"),
                    record.get("variant_id"),
                    record.get("product_id"),
                    record.get("sku"),
                    record.get("available", 0),
                    record.get("committed", 0),
                    record.get("damaged", 0),
                    record.get("incoming", 0),
                    record.get("on_hand", 0),
                    record.get("quality_control", 0),
                    record.get("reserved", 0),
                    record.get("safety_stock", 0),
                    last_updated_at,
                    record.get("scheduled_changes", "[]"),
                    datetime.now(),  # synced_at
                )
                
                # Vérifier si l'enregistrement existe déjà pour les stats
                check_q = """
                    SELECT 1 FROM inventory 
                    WHERE inventory_item_id = %s AND location_id = %s
                """
                cur.execute(check_q, (record.get("inventory_item_id"), record.get("location_id")))
                exists = cur.fetchone()
                
                # Exécuter l'upsert
                cur.execute(upsert_q, params)
                
                if exists:
                    stats["updated"] += 1
                else:
                    stats["inserted"] += 1
                    
            except Exception as exc:
                stats["errors"].append(f"Erreur sur inventory_item_id={record.get('inventory_item_id')}, location_id={record.get('location_id')}: {str(exc)}")
                stats["skipped"] += 1
                print(f"Erreur sur enregistrement: {str(exc)}")

        print("Validation des changements (commit)...")
        conn.commit()
        
    except Exception as exc:
        print(f"Erreur critique, rollback: {str(exc)}")
        conn.rollback()
        stats["errors"].append(str(exc))
    finally:
        cur.close()
        conn.close()
        print("Connexion DB fermée.")

    print(f"Fin du traitement: {stats['inserted']} insérés, {stats['updated']} mis à jour, {stats['skipped']} ignorés")
    return stats

# ---------------------------------------------------------------------------
# 7. Fonctions principales d'orchestration
# ---------------------------------------------------------------------------

def sync_inventory_since_date(dt_since: datetime) -> Dict[str, Any]:
    """
    Synchronise l'inventaire depuis une date donnée.
    """
    print(f"=== Synchronisation de l'inventaire depuis {dt_since.isoformat()} ===")
    
    try:
        # Récupération des données
        inventory_records = get_inventory_since_date(dt_since)
        
        # Traitement en base
        result = process_inventory_records(inventory_records)
        
        print("=== Synchronisation terminée avec succès ===")
        return {
            "success": True,
            "records_processed": len(inventory_records),
            "stats": result
        }
        
    except Exception as e:
        print(f"=== Erreur lors de la synchronisation: {str(e)} ===")
        return {
            "success": False,
            "error": str(e),
            "records_processed": 0,
            "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}
        }

def sync_inventory_last_hours(hours: int = 24) -> Dict[str, Any]:
    """
    Synchronise l'inventaire des dernières heures.
    """
    since = datetime.now() - timedelta(hours=hours)
    return sync_inventory_since_date(since)

def sync_inventory_last_days(days: int = 1) -> Dict[str, Any]:
    """
    Synchronise l'inventaire des derniers jours.
    """
    since = datetime.now() - timedelta(days=days)
    return sync_inventory_since_date(since)

# ---------------------------------------------------------------------------
# 8. Exemple d'exécution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Démarrage de la synchronisation de l'inventaire ===")
    
    # Exemple : synchronise les dernières 24 heures
    print("Synchronisation de l'inventaire des dernières 24 heures...")
    result = sync_inventory_last_hours(24)
    
    print("=== Résultat de la synchronisation ===")
    print(json.dumps(result, indent=2, default=str))
