#!/usr/bin/env python3
"""
Shopify Inventory → PostgreSQL ETL - Synchronisation complète
Charge TOUT l'inventaire sans filtrage par date updated_at
Utile pour la synchronisation initiale ou complète
"""

import os
import json
import time
from datetime import datetime
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
# 3. Extraction complète de tous les inventaires
# ---------------------------------------------------------------------------

def get_all_inventory_data() -> List[Dict[str, Any]]:
    """
    Récupère TOUT l'inventaire sans filtrage par date.
    Utilise l'opération bulk de Shopify pour traiter de gros volumes.
    """
    names = discover_quantity_names()
    names_literal = ", ".join(f'"{n}"' for n in names)
    
    # Requête GraphQL pour TOUS les inventaires
    bulk_query = f'''
    mutation {{
      bulkOperationRunQuery(
        query: """
        {{
          inventoryItems {{
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

    print("🚀 Démarrage de l'export bulk pour TOUT l'inventaire")
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

    # Process data directly from URL
    if url:
        print("📦 Traitement de tous les données d'inventaire en streaming")
        return process_inventory_data_from_url(url, names)
    else:
        print("⚠️ Aucune donnée d'inventaire trouvée")
        return []

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
    print("📊 Traitement des données en streaming depuis Shopify")
    processed_lines = 0
    
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        
        # Process line by line without saving to disk
        for line in resp.iter_lines(decode_unicode=True):
            if line:  # Skip empty lines
                processed_lines += 1
                if processed_lines % 1000 == 0:
                    print(f"   Traité {processed_lines} lignes...")
                
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
                    print(f"Erreur de parsing JSON sur la ligne {processed_lines}: {line[:100]}... - {e}")
                    continue

    print(f"✅ Traitement terminé: {processed_lines} lignes traitées")

    # Build inventory records
    inventory_records: List[Dict[str, Any]] = []
    print(f"🔨 Construction des enregistrements d'inventaire pour {len(items)} items")
    
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

    print(f"✅ Généré {len(inventory_records)} enregistrements d'inventaire")
    return inventory_records

# ---------------------------------------------------------------------------
# 4. Persistance en base de données avec gestion par lots
# ---------------------------------------------------------------------------

def process_inventory_records_batch(records: List[Dict[str, Any]], batch_size: int = 500) -> Dict[str, int | list]:
    """
    Insère ou met à jour les enregistrements d'inventaire dans PostgreSQL par lots.
    Optimisé pour traiter de gros volumes.
    """
    print(f"🚀 Début du traitement de {len(records)} enregistrements d'inventaire (par lots de {batch_size})")
    stats = {
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    }

    if not records:
        print("⚠️ Aucun enregistrement à traiter.")
        return stats

    print("🔌 Connexion à la base de données...")
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

    # Requête pour vérifier l'existence (pour les stats)
    check_q = """
        SELECT 1 FROM inventory 
        WHERE inventory_item_id = %s AND location_id = %s
    """

    try:
        # Traitement par lots
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(records))
            batch = records[start_idx:end_idx]
            
            print(f"📦 Traitement du lot {batch_num + 1}/{total_batches} ({len(batch)} enregistrements)")
            
            for i, record in enumerate(batch):
                try:
                    # Conversion des dates
                    last_updated_at = None
                    if record.get("last_updated_at"):
                        last_updated_at = _iso_to_dt(record["last_updated_at"])
                    
                    # Vérifier si l'enregistrement existe déjà pour les stats
                    cur.execute(check_q, (record.get("inventory_item_id"), record.get("location_id")))
                    exists = cur.fetchone()
                    
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
                    
                    # Exécuter l'upsert
                    cur.execute(upsert_q, params)
                    
                    if exists:
                        stats["updated"] += 1
                    else:
                        stats["inserted"] += 1
                        
                except Exception as exc:
                    stats["errors"].append(f"Lot {batch_num + 1}, enregistrement {i + 1}: {str(exc)}")
                    stats["skipped"] += 1
                    print(f"❌ Erreur sur enregistrement {start_idx + i + 1}: {str(exc)}")
            
            # Commit après chaque lot
            conn.commit()
            print(f"✅ Lot {batch_num + 1} traité et validé")

        print("🎉 Validation finale des changements...")
        
    except Exception as exc:
        print(f"💥 Erreur critique, rollback: {str(exc)}")
        conn.rollback()
        stats["errors"].append(str(exc))
    finally:
        cur.close()
        conn.close()
        print("🔌 Connexion DB fermée.")

    print(f"📊 Fin du traitement: {stats['inserted']} insérés, {stats['updated']} mis à jour, {stats['skipped']} ignorés")
    if stats['errors']:
        print(f"⚠️ {len(stats['errors'])} erreur(s) rencontrée(s)")
    
    return stats

# ---------------------------------------------------------------------------
# 5. Fonctions principales d'orchestration
# ---------------------------------------------------------------------------

def sync_all_inventory() -> Dict[str, Any]:
    """
    Synchronise TOUT l'inventaire (synchronisation complète).
    """
    print("=" * 60)
    print("🚀 SYNCHRONISATION COMPLÈTE DE L'INVENTAIRE")
    print("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # Récupération de toutes les données
        print("📥 Récupération de tous les inventaires depuis Shopify...")
        inventory_records = get_all_inventory_data()
        
        if not inventory_records:
            return {
                "success": True,
                "message": "Aucun inventaire trouvé",
                "records_processed": 0,
                "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": []},
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
        
        # Traitement en base par lots
        print("💾 Traitement en base de données...")
        result = process_inventory_records_batch(inventory_records)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("=" * 60)
        print("✅ SYNCHRONISATION COMPLÈTE TERMINÉE")
        print("=" * 60)
        print(f"⏱️ Durée: {duration:.2f} secondes")
        print(f"📊 Enregistrements traités: {len(inventory_records)}")
        print(f"➕ Insérés: {result['inserted']}")
        print(f"🔄 Mis à jour: {result['updated']}")
        print(f"⏭️ Ignorés: {result['skipped']}")
        print(f"❌ Erreurs: {len(result['errors'])}")
        
        return {
            "success": True,
            "message": f"Synchronisation complète réussie: {len(inventory_records)} enregistrements traités",
            "records_processed": len(inventory_records),
            "stats": result,
            "duration_seconds": duration
        }
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("=" * 60)
        print("❌ ERREUR LORS DE LA SYNCHRONISATION")
        print("=" * 60)
        print(f"💥 Erreur: {str(e)}")
        print(f"⏱️ Durée avant erreur: {duration:.2f} secondes")
        
        return {
            "success": False,
            "error": str(e),
            "records_processed": 0,
            "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": [str(e)]},
            "duration_seconds": duration
        }

def clear_inventory_table() -> bool:
    """
    Vide complètement la table inventory (ATTENTION: supprime toutes les données).
    Utile avant une synchronisation complète initiale.
    """
    print("⚠️ ATTENTION: Suppression de toutes les données d'inventaire...")
    
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        
        cur.execute("DELETE FROM inventory")
        deleted_count = cur.rowcount
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"🗑️ {deleted_count} enregistrements supprimés de la table inventory")
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de la suppression: {e}")
        return False

# ---------------------------------------------------------------------------
# 6. Exemple d'exécution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        print("🗑️ Mode suppression activé")
        if clear_inventory_table():
            print("✅ Table vidée avec succès")
        else:
            print("❌ Échec de la suppression")
            sys.exit(1)
    
    print("🚀 Démarrage de la synchronisation complète de l'inventaire")
    result = sync_all_inventory()
    
    print("\n📋 RÉSULTAT FINAL:")
    print(json.dumps(result, indent=2, default=str))
    
    if not result['success']:
        sys.exit(1)
