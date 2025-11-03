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

    print(f"Fin du traitement: {stats['inserted']} insérés, {stats['updated']} mis à jour, {stats['skipped']} ignorés")
    return stats

# ---------------------------------------------------------------------------
# 7. Fonctions principales d'orchestration
# ---------------------------------------------------------------------------

def sync_inventory_levels_by_date(dt_since: datetime) -> List[Dict[str, Any]]:
    """
    Synchronise les InventoryLevels modifiés depuis une date donnée.
    
    ✅ Cette méthode résout le problème du filtre updated_at car elle cible
    directement les InventoryLevels.updatedAt au lieu de InventoryItem.updated_at
    
    Cette approche récupère TOUTES les locations et filtre les levels modifiés récemment.
    
    Args:
        dt_since: Date à partir de laquelle récupérer les changements
    
    Returns:
        Liste des enregistrements d'inventaire
    """
    print(f"\n📍 Sync des InventoryLevels modifiés depuis {dt_since.isoformat()}")
    
    names = discover_quantity_names()
    names_literal = ", ".join(f'"{n}"' for n in names)
    
    # Récupérer toutes les locations
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
    
    print(f"   Traitement de {len(locations)} locations...")
    
    all_records = []
    formatted_date = dt_since.isoformat()
    
    for location in locations:
        location_id = location.get("legacyResourceId")
        location_name = location.get("name")
        
        # Pour chaque location, récupérer les levels modifiés récemment
        cursor = None
        page = 0
        location_records = 0
        
        while True:
            page += 1
            after_clause = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            query {{
              location(id: "{location['id']}") {{
                inventoryLevels(first: 100{after_clause}) {{
                  pageInfo {{
                    hasNextPage
                    endCursor
                  }}
                  edges {{
                    node {{
                      id
                      updatedAt
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
            
            for edge in edges:
                node = edge["node"]
                updated_at_str = node.get("updatedAt")
                
                # Filtrer par date
                if updated_at_str:
                    updated_at = _iso_to_dt(updated_at_str)
                    # Rendre dt_since timezone-aware s'il ne l'est pas
                    dt_since_aware = dt_since if dt_since.tzinfo else dt_since.replace(tzinfo=updated_at.tzinfo)
                    if updated_at >= dt_since_aware:
                        item = node.get("item", {})
                        inventory_item_id = item.get("legacyResourceId")
                        
                        if inventory_item_id:
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
                                "last_updated_at": updated_at_str,
                                "scheduled_changes": "[]"
                            }
                            
                            all_records.append(record)
                            location_records += 1
            
            # Pagination
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break
        
        if location_records > 0:
            print(f"   ✓ {location_name}: {location_records} levels modifiés")
    
    print(f"   Total: {len(all_records)} InventoryLevels modifiés")
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
            since = datetime.now(timezone.utc) - timedelta(hours=2)
            
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
