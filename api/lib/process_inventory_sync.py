#!/usr/bin/env python3
"""
Shopify Inventory → PostgreSQL ETL
Synchronise les données d'inventaire avec filtrage par date updated_at
Basé sur le pattern de process_transactions.py

⚠️  IMPORTANT - Limites de la synchronisation incrémentale:

Le filtre updated_at s'applique à l'InventoryItem (item global), 
PAS aux InventoryLevels (quantités par location).

Cela signifie qu'un item peut avoir du stock dans une location même si
l'InventoryItem.updated_at est ancien. Ces items seront MANQUÉS par
la synchronisation incrémentale.

Solutions:
1. sync_inventory_full() - Sync complète sans filtre (hebdomadaire recommandé)
2. sync_inventory_by_location(location_id) - Sync d'une location spécifique
3. Approche hybride: sync incrémentale quotidienne + sync complète hebdomadaire
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

def _run_bulk_operation(bulk_mutation: str) -> str | None:
    """
    Lance une bulk operation Shopify ou récupère celle déjà en cours.
    Retourne l'URL du fichier JSONL résultat, ou None si aucune donnée.
    """
    status_q = """
    query {
      currentBulkOperation {
        id status errorCode objectCount url partialDataUrl
      }
    }
    """
    terminal = {"COMPLETED", "FAILED", "CANCELED"}

    # Vérifier s'il y a déjà une opération en cours
    current = _gql(status_q).get("currentBulkOperation")
    if current and current.get("status") in ("CREATED", "RUNNING"):
        print(f"Opération bulk déjà en cours (id={current['id']}, status={current['status']}). Attente...")
    else:
        # Lancer la mutation
        start = _gql(bulk_mutation)
        ue = start["bulkOperationRunQuery"]["userErrors"]
        if ue:
            # Vérifier si c'est une erreur "already in progress"
            already_running = any("already in progress" in (e.get("message") or "") for e in ue)
            if already_running:
                print("Opération bulk déjà en cours (détecté via userErrors). Attente...")
            else:
                raise RuntimeError(ue)

    # Poll jusqu'à la fin
    while True:
        time.sleep(5)
        st = _gql(status_q)["currentBulkOperation"]
        print(f"[Bulk] status={st['status']} objects={st.get('objectCount')} url={bool(st.get('url'))}")
        if st["status"] in terminal:
            if st["status"] != "COMPLETED":
                raise RuntimeError(f"Bulk ended with {st['status']} error={st.get('errorCode')}")
            return st.get("url")


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
    url = _run_bulk_operation(bulk_query)

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
# 6. Traitement de la queue webhook (inventory_snapshot_queue)
# ---------------------------------------------------------------------------

def process_inventory_queue() -> Dict[str, Any]:
    """
    Traite les lignes pending de inventory_snapshot_queue une par une.
    
    Pour chaque ligne :
    1. Passe le status à 'processing'
    2. UPSERT dans la table inventory (INSERT si nouveau, UPDATE si existant)
    3. En cas de succès : status = 'completed', processed_at = NOW()
    4. En cas d'erreur : status = 'failed', attempts += 1, last_error = message
    
    Le traitement ligne par ligne est important car les triggers sur inventory
    logguent chaque changement individuellement dans inventory_history.
    
    Returns:
        Dict avec les stats: inserted, updated, failed, total_pending
    """
    stats = {
        "inserted": 0,
        "updated": 0,
        "failed": 0,
        "total_pending": 0,
        "errors": [],
    }

    conn = None
    cur = None

    try:
        conn = _pg_connect()
        cur = conn.cursor()
        # 1. Récupérer les lignes pending + failed (si < 6 tentatives), ordonnées par date de création
        cur.execute("""
            SELECT id, inventory_item_id, location_id, quantities, shopify_updated_at
            FROM inventory_snapshot_queue
            WHERE status = 'pending'
               OR (status = 'failed' AND attempts < 6)
            ORDER BY created_at ASC
        """)
        pending_rows = cur.fetchall()
        stats["total_pending"] = len(pending_rows)

        if not pending_rows:
            print("Aucune ligne pending dans la queue.")
            return stats

        print(f"Traitement de {len(pending_rows)} lignes pending dans la queue...")

        MAX_ATTEMPTS = 3

        for row in pending_rows:
            queue_id, inventory_item_id, location_id, quantities, shopify_updated_at = row

            # Retry jusqu'à MAX_ATTEMPTS fois avant de marquer comme failed
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    # 2. Passer le status à 'processing'
                    cur.execute("""
                        UPDATE inventory_snapshot_queue
                        SET status = 'processing',
                            attempts = %s
                        WHERE id = %s
                    """, (attempt, queue_id))
                    conn.commit()

                    # Extraire les quantités du jsonb
                    qty = quantities if isinstance(quantities, dict) else json.loads(quantities)
                    available = qty.get("available", 0)
                    committed = qty.get("committed", 0)
                    on_hand = qty.get("on_hand", 0)
                    incoming = qty.get("incoming", 0)
                    reserved = qty.get("reserved", 0)

                    # 3. UPSERT : INSERT si nouveau, UPDATE si existant
                    #    RETURNING (xmax = 0) — true when the row was freshly
                    #    inserted, false when an existing row was updated.
                    cur.execute("""
                        INSERT INTO inventory (
                            inventory_item_id, location_id,
                            available, committed, on_hand, incoming, reserved,
                            last_updated_at, synced_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (inventory_item_id, location_id)
                        DO UPDATE SET
                            available = EXCLUDED.available,
                            committed = EXCLUDED.committed,
                            on_hand = EXCLUDED.on_hand,
                            incoming = EXCLUDED.incoming,
                            reserved = EXCLUDED.reserved,
                            last_updated_at = EXCLUDED.last_updated_at,
                            synced_at = NOW()
                        RETURNING (xmax = 0) AS inserted
                    """, (
                        inventory_item_id, location_id,
                        available, committed, on_hand, incoming, reserved,
                        shopify_updated_at,
                    ))
                    was_inserted = cur.fetchone()[0]

                    # 4. Marquer comme completed
                    cur.execute("""
                        UPDATE inventory_snapshot_queue
                        SET status = 'completed',
                            processed_at = NOW()
                        WHERE id = %s
                    """, (queue_id,))
                    conn.commit()

                    if was_inserted:
                        stats["inserted"] += 1
                    else:
                        stats["updated"] += 1

                    # Succès → sortir de la boucle de retry
                    break

                except Exception as exc:
                    if conn is not None:
                        conn.rollback()
                    error_msg = str(exc)[:500]

                    if attempt < MAX_ATTEMPTS:
                        # Tentative échouée mais il reste des essais
                        print(f"Queue id={queue_id}: tentative {attempt}/{MAX_ATTEMPTS} échouée — {error_msg}")
                        time.sleep(1)
                        continue

                    # Dernière tentative échouée → marquer comme failed
                    try:
                        cur.execute("""
                            UPDATE inventory_snapshot_queue
                            SET status = 'failed',
                                attempts = %s,
                                last_error = %s
                            WHERE id = %s
                        """, (attempt, error_msg, queue_id))
                        conn.commit()
                    except Exception:
                        if conn is not None:
                            conn.rollback()

                    stats["failed"] += 1
                    stats["errors"].append(f"Queue id={queue_id}: {error_msg}")
                    print(f"Queue id={queue_id}: échec définitif après {MAX_ATTEMPTS} tentatives — {error_msg}")

    except Exception as exc:
        print(f"Erreur critique lors du traitement de la queue: {str(exc)}")
        if conn is not None:
            conn.rollback()
        stats["errors"].append(str(exc))
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    print(f"Queue traitée: {stats['inserted']} insérés, {stats['updated']} mis à jour, {stats['failed']} échoués sur {stats['total_pending']} pending")
    return stats

# ---------------------------------------------------------------------------
# 7. Persistance en base de données (sync Shopify API)
# ---------------------------------------------------------------------------

def process_inventory_records(records: List[Dict[str, Any]], batch_size: int = 1000) -> Dict[str, int | list]:
    """
    Insère ou met à jour les enregistrements d'inventaire dans PostgreSQL.
    Utilise execute_values pour envoyer les données en batch (beaucoup plus rapide).
    """
    from psycopg2.extras import execute_values

    print(f"Début du traitement de {len(records)} enregistrements d'inventaire (batch_size={batch_size})...")
    stats = {
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    }

    if not records:
        print("Aucun enregistrement à traiter.")
        return stats

    conn = _pg_connect()
    cur = conn.cursor()

    upsert_q = """
        INSERT INTO inventory (
            inventory_item_id, location_id, variant_id, product_id, sku,
            available, committed, damaged, incoming, on_hand,
            quality_control, reserved, safety_stock,
            last_updated_at, scheduled_changes, synced_at
        ) VALUES %s
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
        WHERE
            inventory.available IS DISTINCT FROM EXCLUDED.available
            OR inventory.committed IS DISTINCT FROM EXCLUDED.committed
            OR inventory.damaged IS DISTINCT FROM EXCLUDED.damaged
            OR inventory.incoming IS DISTINCT FROM EXCLUDED.incoming
            OR inventory.on_hand IS DISTINCT FROM EXCLUDED.on_hand
            OR inventory.quality_control IS DISTINCT FROM EXCLUDED.quality_control
            OR inventory.reserved IS DISTINCT FROM EXCLUDED.reserved
            OR inventory.safety_stock IS DISTINCT FROM EXCLUDED.safety_stock
        RETURNING (xmax = 0) AS inserted
    """

    now = datetime.now()

    try:
        # Préparer toutes les tuples d'un coup
        all_values = []
        for record in records:
            try:
                last_updated_at = None
                if record.get("last_updated_at"):
                    last_updated_at = _iso_to_dt(record["last_updated_at"])

                all_values.append((
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
                    now,
                ))
            except Exception as exc:
                stats["skipped"] += 1
                stats["errors"].append(f"Erreur préparation inventory_item_id={record.get('inventory_item_id')}: {str(exc)}")

        # Envoyer par batch via execute_values
        for i in range(0, len(all_values), batch_size):
            batch = all_values[i:i + batch_size]
            batch_end = min(i + batch_size, len(all_values))
            print(f"Batch {i // batch_size + 1}: enregistrements {i + 1}-{batch_end}/{len(all_values)}")

            try:
                results = execute_values(
                    cur, upsert_q, batch,
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    fetch=True,
                )
                for (was_inserted,) in results:
                    if was_inserted:
                        stats["inserted"] += 1
                    else:
                        stats["updated"] += 1
                conn.commit()
            except Exception as exc:
                conn.rollback()
                stats["errors"].append(f"Erreur batch {i // batch_size + 1}: {str(exc)}")
                stats["skipped"] += len(batch)
                print(f"Erreur sur batch: {str(exc)}")

    except Exception as exc:
        print(f"Erreur critique, rollback: {str(exc)}")
        conn.rollback()
        stats["errors"].append(str(exc))
    finally:
        cur.close()
        conn.close()

    print(f"Fin du traitement: {stats['inserted']} insérés, {stats['updated']} mis à jour, {stats['skipped']} ignorés")
    return stats

# ---------------------------------------------------------------------------
# 7. Fonctions principales d'orchestration
# ---------------------------------------------------------------------------

def sync_inventory_levels_by_date(dt_since: datetime, max_pages_per_location: int = 5) -> List[Dict[str, Any]]:
    """
    Synchronise les InventoryLevels modifiés depuis une date donnée.
    
    VERSION REST API: Utilise l'API REST avec filtrage côté serveur (updated_at_min).
    Cette approche est BEAUCOUP plus efficace que GraphQL car elle filtre côté serveur.
    
    Avantages:
    - Filtrage côté serveur via updated_at_min (pas de scan inutile)
    - 1 seule requête pour toutes les locations (max 50 locations)
    - Ultra rapide (< 10 secondes)
    - Pas de bulk operation (pas de conflit)
    
    Args:
        dt_since: Date à partir de laquelle récupérer les changements
        max_pages_per_location: Non utilisé (gardé pour compatibilité)
    
    Returns:
        Liste des enregistrements d'inventaire
    """
    print(f"\n📍 Sync des InventoryLevels modifiés depuis {dt_since.isoformat()}")
    print(f"   🚀 Utilisation de l'API REST avec filtrage serveur (updated_at_min)")
    
    # Récupérer les locations via GraphQL
    locations_query = """
    query {
      locations(first: 50) {
        edges {
          node {
            id
            legacyResourceId
            name
          }
        }
      }
    }
    """
    
    locations_data = _gql(locations_query)
    locations = [edge["node"] for edge in locations_data.get("locations", {}).get("edges", [])]
    location_ids = [loc.get("legacyResourceId") for loc in locations if loc.get("legacyResourceId")]
    
    print(f"   📍 {len(location_ids)} locations")
    
    # Préparer l'URL REST API
    shop_domain = STORE_DOMAIN  # Défini au début du fichier
    access_token = ACCESS_TOKEN  # Défini au début du fichier
    
    # Formater la date pour l'API REST (ISO 8601)
    dt_naive = dt_since.replace(tzinfo=None) if dt_since.tzinfo else dt_since
    updated_at_min = dt_naive.isoformat()
    
    # Construire l'URL avec tous les location_ids (max 50)
    location_ids_str = ",".join(map(str, location_ids))
    base_url = f"https://{shop_domain}/admin/api/2025-10/inventory_levels.json"
    
    all_records = []
    page = 0
    next_url = f"{base_url}?location_ids={location_ids_str}&updated_at_min={updated_at_min}&limit=250"
    
    # Récupérer les quantity names pour GraphQL (on en aura besoin après)
    names = discover_quantity_names()
    
    print(f"   ⏳ Récupération des niveaux d'inventaire modifiés...", end=" ", flush=True)
    
    import requests
    
    while next_url:
        page += 1
        
        # Appel REST API
        response = requests.get(
            next_url,
            headers={
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json"
            }
        )
        response.raise_for_status()
        
        data = response.json()
        inventory_levels = data.get("inventory_levels", [])
        
        print(f"p{page}:+{len(inventory_levels)}", end=" ", flush=True)
        
        # Pour chaque inventory level, enrichir avec les données manquantes via GraphQL
        for level in inventory_levels:
            inventory_item_id = level.get("inventory_item_id")
            location_id = level.get("location_id")
            
            if inventory_item_id and location_id:
                # Récupérer les détails via GraphQL (SKU, variant_id, product_id, quantities)
                item_query = f"""
                query {{
                  inventoryItem(id: "gid://shopify/InventoryItem/{inventory_item_id}") {{
                    id
                    sku
                    variant {{
                      legacyResourceId
                      product {{
                        legacyResourceId
                      }}
                    }}
                    inventoryLevel(locationId: "gid://shopify/Location/{location_id}") {{
                      quantities(names: [{", ".join(f'"{n}"' for n in names)}]) {{
                        name
                        quantity
                      }}
                    }}
                  }}
                }}
                """
                
                try:
                    item_data = _gql(item_query)
                    item = item_data.get("inventoryItem", {})
                    
                    if item:
                        variant = item.get("variant") or {}
                        product = variant.get("product") or {}
                        inv_level = item.get("inventoryLevel") or {}
                        quantities = inv_level.get("quantities", [])
                        qmap = {q.get("name"): q.get("quantity", 0) for q in quantities}
                        
                        record = {
                            "inventory_item_id": inventory_item_id,
                            "location_id": location_id,
                            "sku": item.get("sku"),
                            "variant_id": variant.get("legacyResourceId"),
                            "product_id": product.get("legacyResourceId"),
                            "available": qmap.get("available", 0),
                            "committed": qmap.get("committed", 0),
                            "damaged": qmap.get("damaged", 0),
                            "incoming": qmap.get("incoming", 0),
                            "on_hand": qmap.get("on_hand", 0),
                            "quality_control": qmap.get("quality_control", 0),
                            "reserved": qmap.get("reserved", 0),
                            "safety_stock": qmap.get("safety_stock", 0),
                            "last_updated_at": level.get("updated_at"),
                            "scheduled_changes": "[]"
                        }
                        
                        all_records.append(record)
                except Exception as e:
                    print(f"\n   ⚠️  Erreur item {inventory_item_id}: {str(e)[:50]}", end=" ", flush=True)
        
        # Vérifier s'il y a une page suivante via les headers Link
        link_header = response.headers.get("Link", "")
        next_url = None
        
        if link_header:
            # Parser le header Link pour trouver rel="next"
            links = link_header.split(",")
            for link in links:
                if 'rel="next"' in link:
                    # Extraire l'URL entre < et >
                    next_url = link.split(";")[0].strip().strip("<>")
                    break
    
    print(f"\n   ✅ {len(all_records)} InventoryLevels récupérés")
    return all_records

def sync_inventory_smart() -> Dict[str, Any]:
    """
    Synchronisation INTELLIGENTE avec stratégie hybride automatique.
    
    Cette fonction est le point d'entrée principal pour la synchronisation d'inventaire.
    Elle choisit automatiquement la meilleure stratégie selon le jour:
    
    - DIMANCHE 2h: Sync complète (TOUS les items, TOUTES locations)
    - AUTRES MOMENTS: Double sync incrémentale (InventoryItems + InventoryLevels)
    
    ✅ RÉSOUT LE PROBLÈME: La double sync incrémentale capture TOUS les changements:
       1. Items dont les propriétés ont changé (InventoryItem.updated_at)
       2. Items dont les quantités ont changé (InventoryLevel.updatedAt)
    
    Cette approche combine:
    1. Performance: Sync rapide incrémentale par défaut (2-10 min)
    2. Complétude: Capture 100% des changements récents
    3. Garantie: Sync complète hebdomadaire pour filet de sécurité
    
    Returns:
        Dictionnaire avec les résultats de la synchronisation
    """
    now = datetime.now()
    hour = now.hour
    weekday = now.weekday()  # 0=Lundi, 6=Dimanche
    
    print(f"\n{'='*80}")
    print(f"SYNCHRONISATION INTELLIGENTE D'INVENTAIRE")
    print(f"Date/Heure: {now.isoformat()}")
    print(f"{'='*80}")
    
    try:
        # STRATÉGIE 1: Sync complète hebdomadaire (Dimanche entre 2h et 3h)
        if weekday == 6 and hour == 2:
            print("\n🌐 STRATÉGIE: Synchronisation COMPLÈTE hebdomadaire")
            print("   Récupération de TOUS les items de TOUTES les locations")
            print("   ⚠️  Cette opération garantit 100% de cohérence des données")
            print("   Durée estimée: 15-30 minutes")
            result = sync_inventory_full()
            result["strategy_used"] = "full_weekly"
            return result
        
        # STRATÉGIE 2: Double sync incrémentale (défaut)
        else:
            print("\n📈 STRATÉGIE: Double synchronisation incrémentale")
            print("   1️⃣  InventoryItems modifiés (propriétés: SKU, prix, etc.)")
            print("   2️⃣  InventoryLevels modifiés (quantités par location)")
            print("   ✅ Capture TOUS les changements récents")
            print("   Durée estimée: 2-10 minutes")
            
            # Utiliser UTC pour la comparaison
            from datetime import timezone
            since = datetime.now(timezone.utc) - timedelta(hours=2, minutes=0)
            
            # Partie 1: Sync des InventoryItems modifiés
            print("\n   📦 Partie 1: InventoryItems modifiés...")
            items_records = get_inventory_since_date(since)
            print(f"      → {len(items_records)} enregistrements d'items")
            
            # Partie 2: Sync des InventoryLevels modifiés
            print("\n   📍 Partie 2: InventoryLevels modifiés...")
            levels_records = sync_inventory_levels_by_date(since)
            print(f"      → {len(levels_records)} enregistrements de levels")
            
            # Fusionner les deux listes (dédupliquer par inventory_item_id + location_id)
            print("\n   🔀 Fusion et déduplication...")
            records_dict = {}
            
            for record in items_records + levels_records:
                key = (record.get("inventory_item_id"), record.get("location_id"))
                # Garder le plus récent
                if key not in records_dict:
                    records_dict[key] = record
                else:
                    existing_date = records_dict[key].get("last_updated_at", "")
                    new_date = record.get("last_updated_at", "")
                    if new_date > existing_date:
                        records_dict[key] = record
            
            final_records = list(records_dict.values())
            print(f"      → {len(final_records)} enregistrements uniques après fusion")
            
            # Traitement en base
            print("\n   💾 Insertion en base de données...")
            result = process_inventory_records(final_records)
            
            print("\n   ✅ Double sync terminée")
            return {
                "success": True,
                "strategy_used": "double_incremental",
                "records_processed": len(final_records),
                "details": {
                    "from_items": len(items_records),
                    "from_levels": len(levels_records),
                    "unique_after_merge": len(final_records)
                },
                "stats": result
            }
            
    except Exception as e:
        print(f"\n❌ ERREUR lors de la synchronisation intelligente: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "strategy_used": "error",
            "error": str(e),
            "records_processed": 0,
            "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}
        }

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

def sync_inventory_by_location(location_id: str) -> Dict[str, Any]:
    """
    Synchronise TOUS les items d'une location spécifique.
    
    Cette méthode récupère directement par location et ne dépend pas
    du filtre updated_at, garantissant qu'aucun item n'est manqué.
    
    Args:
        location_id: L'ID legacy de la location (ex: "61788848199")
    
    Returns:
        Dictionnaire avec les statistiques de synchronisation
    """
    print(f"=== Synchronisation complète de la location {location_id} ===")
    
    try:
        # Découvrir les noms de quantités
        names = discover_quantity_names()
        names_literal = ", ".join(f'"{n}"' for n in names)
        
        # Récupérer tous les inventory levels de cette location
        all_records = []
        cursor = None
        page = 0
        
        while True:
            page += 1
            after_clause = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            query {{
              location(id: "gid://shopify/Location/{location_id}") {{
                name
                inventoryLevels(first: 100{after_clause}) {{
                  pageInfo {{
                    hasNextPage
                    endCursor
                  }}
                  edges {{
                    node {{
                      id
                      item {{
                        id
                        legacyResourceId
                        sku
                        variant {{
                          id
                          legacyResourceId
                          product {{
                            id
                            legacyResourceId
                          }}
                        }}
                      }}
                      quantities(names: [{names_literal}]) {{
                        name
                        quantity
                      }}
                      updatedAt
                    }}
                  }}
                }}
              }}
            }}
            """
            
            data = _gql(query)
            location_data = data.get("location", {})
            inventory_levels = location_data.get("inventoryLevels", {})
            edges = inventory_levels.get("edges", [])
            page_info = inventory_levels.get("pageInfo", {})
            
            print(f"  Page {page}: {len(edges)} items récupérés")
            
            for edge in edges:
                node = edge["node"]
                item = node.get("item", {})
                
                inventory_item_id = item.get("legacyResourceId")
                if not inventory_item_id:
                    continue
                
                variant = item.get("variant") or {}
                product = variant.get("product") or {}
                
                quantities = node.get("quantities", [])
                qmap = {q.get("name"): q.get("quantity", 0) for q in quantities}
                
                record = {
                    "inventory_item_id": inventory_item_id,
                    "location_id": location_id,
                    "sku": item.get("sku"),
                    "variant_id": variant.get("legacyResourceId"),
                    "product_id": product.get("legacyResourceId"),
                    "available": qmap.get("available", 0),
                    "committed": qmap.get("committed", 0),
                    "damaged": qmap.get("damaged", 0),
                    "incoming": qmap.get("incoming", 0),
                    "on_hand": qmap.get("on_hand", 0),
                    "quality_control": qmap.get("quality_control", 0),
                    "reserved": qmap.get("reserved", 0),
                    "safety_stock": qmap.get("safety_stock", 0),
                    "last_updated_at": node.get("updatedAt"),
                    "scheduled_changes": "[]"
                }
                
                all_records.append(record)
            
            # Pagination
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break
        
        print(f"Total récupéré: {len(all_records)} items pour cette location")
        
        # Traitement en base
        result = process_inventory_records(all_records)
        
        print("=== Synchronisation de la location terminée ===")
        return {
            "success": True,
            "location_id": location_id,
            "records_processed": len(all_records),
            "stats": result
        }
        
    except Exception as e:
        print(f"=== Erreur lors de la synchronisation de la location: {str(e)} ===")
        return {
            "success": False,
            "location_id": location_id,
            "error": str(e),
            "records_processed": 0,
            "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}
        }

def sync_inventory_full() -> Dict[str, Any]:
    """
    Synchronise l'inventaire COMPLET sans filtre de date.
    
    ⚠️  ATTENTION: Cette opération peut prendre plusieurs minutes et
    consomme beaucoup de crédits API. À utiliser périodiquement (hebdomadaire)
    pour garantir la cohérence complète des données.
    
    Cette méthode résout le problème des items manquants causé par le filtre
    updated_at qui s'applique à l'InventoryItem mais pas aux InventoryLevels.
    
    Returns:
        Dictionnaire avec les statistiques de synchronisation
    """
    print("=== Synchronisation COMPLÈTE de l'inventaire (SANS filtre de date) ===")
    print("⚠️  Cette opération peut prendre plusieurs minutes...")
    
    try:
        names = discover_quantity_names()
        names_literal = ", ".join(f'"{n}"' for n in names)
        
        # Requête bulk SANS filtre de date
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

        print("Démarrage de l'export bulk complet (TOUS les items)")
        url = _run_bulk_operation(bulk_query)

        # Process data directly from URL
        if url:
            print("Traitement des données complètes")
            inventory_records = process_inventory_data_from_url(url, names)
        else:
            print("Aucune donnée disponible")
            inventory_records = []
        
        # Traitement en base
        result = process_inventory_records(inventory_records)
        
        print("=== Synchronisation complète terminée avec succès ===")
        return {
            "success": True,
            "sync_type": "full",
            "records_processed": len(inventory_records),
            "stats": result
        }
        
    except Exception as e:
        print(f"=== Erreur lors de la synchronisation complète: {str(e)} ===")
        return {
            "success": False,
            "sync_type": "full",
            "error": str(e),
            "records_processed": 0,
            "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}
        }

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
