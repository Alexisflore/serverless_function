"""
Module pour gérer les produits Shopify avec mise à jour incrémentale
"""

import os
import json
import requests
import psycopg2
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from datetime import datetime

def _shopify_headers() -> Dict[str, str]:
    """Retourne les headers pour les requêtes Shopify API"""
    load_dotenv()
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json",
    }

def _pg_connect():
    """Connexion à la base de données PostgreSQL"""
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

def get_latest_product_update_date() -> Optional[str]:
    """
    Récupère la date de mise à jour la plus récente en base
    Utilise le GREATEST entre updated_at et imported_at pour couvrir tous les cas
    
    Returns:
        Optional[str]: Date ISO de mise à jour la plus récente ou None
    """
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        
        # Vérifier si la table existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'products'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            print("📋 Table 'products' n'existe pas encore")
            return None
        
        # Récupérer la date de mise à jour la plus récente
        cur.execute("""
            SELECT GREATEST(
                COALESCE(MAX(updated_at), '1970-01-01'::timestamp),
                COALESCE(MAX(imported_at), '1970-01-01'::timestamp)
            ) as latest_date
            FROM products 
            WHERE updated_at IS NOT NULL OR imported_at IS NOT NULL
        """)
        
        result = cur.fetchone()
        latest_date = result[0] if result else None
        
        if latest_date and latest_date.year > 1970:
            # Convertir en format ISO string
            return latest_date.isoformat()
        else:
            print("📋 Aucune date de mise à jour trouvée en base")
            return None
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération de la date: {e}")
        return None
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_inventory_items_cogs(inventory_item_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    VERSION ULTRA-OPTIMISÉE : Récupère les COGS via GraphQL unitCost
    Utilise le champ unitCost disponible depuis l'API GraphQL pour des performances 250x meilleures
    
    Args:
        inventory_item_ids: Liste des IDs des inventory items
        
    Returns:
        Dict mapping inventory_item_id -> inventory_item_data (compatible format REST)
    """
    load_dotenv()
    
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    headers = _shopify_headers()
    
    inventory_items_data = {}
    total_items = len(inventory_item_ids)
    
    if total_items == 0:
        return inventory_items_data
    
    print(f"🚀 Récupération ULTRA-OPTIMISÉE des COGS via GraphQL pour {total_items} inventory items...")
    
    # GraphQL peut gérer jusqu'à 250 nodes par requête (vs 1 item par requête REST)
    batch_size = 250
    
    for i in range(0, total_items, batch_size):
        batch_ids = inventory_item_ids[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_items + batch_size - 1) // batch_size
        
        print(f"📦 Batch GraphQL {batch_num}/{total_batches}: {len(batch_ids)} inventory items...")
        
        # Construire la requête GraphQL avec unitCost (le champ magique !)
        query = """
        query getInventoryItemsWithCOGS($ids: [ID!]!) {
            nodes(ids: $ids) {
                ... on InventoryItem {
                    id
                    sku
                    tracked
                    requiresShipping
                    countryCodeOfOrigin
                    harmonizedSystemCode
                    unitCost {
                        amount
                        currencyCode
                    }
                    createdAt
                    updatedAt
                }
            }
        }
        """
        
        # Convertir les IDs en format GraphQL
        graphql_ids = [f"gid://shopify/InventoryItem/{id}" for id in batch_ids]
        
        variables = {"ids": graphql_ids}
        payload = {
            "query": query,
            "variables": variables
        }
        
        try:
            url = f"https://{store_domain}/admin/api/{api_version}/graphql.json"
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            
            if response.status_code == 200:
                data = response.json()
                
                if 'errors' in data:
                    print(f"❌ Erreurs GraphQL batch {batch_num}: {data['errors']}")
                    # Fallback to REST for this batch
                    print(f"🔄 Fallback REST pour batch {batch_num}")
                    _fallback_to_rest(batch_ids, inventory_items_data, store_domain, api_version, headers)
                    continue
                
                nodes = data.get('data', {}).get('nodes', [])
                valid_items = [node for node in nodes if node is not None]
                
                print(f"✅ Batch GraphQL {batch_num}: {len(valid_items)} items récupérés")
                
                # Convertir les résultats au format compatible REST
                for item in valid_items:
                    # Extraire l'ID numérique du GraphQL ID
                    graphql_id = item.get('id', '')
                    if graphql_id:
                        numeric_id = int(graphql_id.split('/')[-1])
                        
                        # Convertir unitCost au format "cost" pour compatibilité totale
                        unit_cost = item.get('unitCost')
                        if unit_cost and unit_cost.get('amount'):
                            item['cost'] = unit_cost['amount']
                            item['cost_currency'] = unit_cost['currencyCode']
                        else:
                            item['cost'] = None
                            item['cost_currency'] = None
                        
                        # Reformater pour compatibilité REST (conversion des dates GraphQL, etc.)
                        formatted_item = {
                            'id': numeric_id,
                            'sku': item.get('sku'),
                            'cost': item['cost'],
                            'tracked': item.get('tracked'),
                            'requires_shipping': item.get('requiresShipping'),
                            'country_code_of_origin': item.get('countryCodeOfOrigin'),
                            'harmonized_system_code': item.get('harmonizedSystemCode'),
                            'created_at': item.get('createdAt'),
                            'updated_at': item.get('updatedAt')
                        }
                        
                        inventory_items_data[numeric_id] = formatted_item
                        
            elif response.status_code == 429:  # Rate limit
                print(f"⚠️ Rate limit GraphQL, pause...")
                import time
                time.sleep(2)
                # Retry
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and not 'errors' in data:
                        nodes = data.get('data', {}).get('nodes', [])
                        valid_items = [node for node in nodes if node is not None]
                        for item in valid_items:
                            graphql_id = item.get('id', '')
                            if graphql_id:
                                numeric_id = int(graphql_id.split('/')[-1])
                                
                                unit_cost = item.get('unitCost')
                                if unit_cost and unit_cost.get('amount'):
                                    item['cost'] = unit_cost['amount']
                                else:
                                    item['cost'] = None
                                
                                formatted_item = {
                                    'id': numeric_id,
                                    'sku': item.get('sku'),
                                    'cost': item['cost'],
                                    'tracked': item.get('tracked'),
                                    'requires_shipping': item.get('requiresShipping'),
                                    'country_code_of_origin': item.get('countryCodeOfOrigin'),
                                    'harmonized_system_code': item.get('harmonizedSystemCode'),
                                    'created_at': item.get('createdAt'),
                                    'updated_at': item.get('updatedAt')
                                }
                                
                                inventory_items_data[numeric_id] = formatted_item
            else:
                print(f"❌ Erreur GraphQL batch {batch_num}: {response.status_code}")
                print(f"🔄 Fallback REST pour batch {batch_num}")
                _fallback_to_rest(batch_ids, inventory_items_data, store_domain, api_version, headers)
                
        except Exception as e:
            print(f"❌ Exception GraphQL batch {batch_num}: {e}")
            print(f"🔄 Fallback REST pour batch {batch_num}")
            _fallback_to_rest(batch_ids, inventory_items_data, store_domain, api_version, headers)
        
        # Pause entre batches GraphQL (moins restrictif que REST)
        if batch_num < total_batches:
            import time
            time.sleep(0.2)
    
    # Statistiques finales
    percentage = (len(inventory_items_data)/total_items*100) if total_items > 0 else 0
    items_with_cogs = len([item for item in inventory_items_data.values() if item.get('cost') is not None])
    
    print(f"🎯 RÉSULTATS GraphQL OPTIMISÉ:")
    print(f"   • Inventory items récupérés: {len(inventory_items_data)}/{total_items} ({percentage:.1f}%)")
    print(f"   • Avec COGS: {items_with_cogs}")
    print(f"   • Sans COGS: {len(inventory_items_data) - items_with_cogs}")
    print(f"   🚀 Performance: {max(1, (total_items + 249) // 250)} appels GraphQL au lieu de {total_items} appels REST!")
    
    return inventory_items_data


def _fallback_to_rest(inventory_item_ids: List[int], inventory_items_data: Dict[int, Dict[str, Any]], 
                     store_domain: str, api_version: str, headers: Dict[str, str]) -> None:
    """
    Fallback vers REST en cas d'échec GraphQL
    """
    for inventory_item_id in inventory_item_ids:
        try:
            url = f"https://{store_domain}/admin/api/{api_version}/inventory_items/{inventory_item_id}.json"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                inventory_item = response.json().get('inventory_item', {})
                inventory_items_data[inventory_item_id] = inventory_item
            elif response.status_code == 429:  # Rate limit
                print(f"⚠️ Rate limit REST, pause...")
                import time
                time.sleep(2)
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    inventory_item = response.json().get('inventory_item', {})
                    inventory_items_data[inventory_item_id] = inventory_item
            else:
                print(f"❌ Erreur REST pour {inventory_item_id}: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Exception REST pour {inventory_item_id}: {e}")
        
        # Pause pour REST
        import time
        time.sleep(0.3)

def get_shopify_products_since(since_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Récupère les produits Shopify mis à jour après une date donnée AVEC les COGS
    Utilise updated_at_min pour capturer les nouveaux produits ET les produits modifiés
    
    Args:
        since_date: Date ISO à partir de laquelle récupérer les produits (utilise updated_at_min)
        
    Returns:
        Dict contenant products, variants et inventory_items avec COGS
    """
    load_dotenv()
    
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    if not store_domain:
        raise ValueError("SHOPIFY_STORE_DOMAIN non défini dans les variables d'environnement")
    
    if since_date:
        print(f"🔍 Récupération des produits mis à jour après {since_date} (nouveaux + modifiés, avec COGS)...")
    else:
        print(f"🔍 Récupération de tous les produits (première synchronisation, avec COGS)...")
    
    # Construire l'URL avec filtre de date si nécessaire
    url = f"https://{store_domain}/admin/api/{api_version}/products.json"
    headers = _shopify_headers()
    params = {
        "limit": 250,
        "fields": "id,title,handle,status,product_type,vendor,tags,created_at,updated_at,variants,options"
    }
    
    # Ajouter le filtre de date si spécifié (utilise updated_at_min)
    if since_date:
        params["updated_at_min"] = since_date
    
    all_products = []
    page = 1
    max_pages = 50  # Limite de sécurité
    
    while url and page <= max_pages:
        try:
            print(f"📄 Page {page}...")
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code != 200:
                print(f"❌ Erreur API Shopify: {response.status_code}")
                print(f"Réponse: {response.text}")
                break
            
            data = response.json()
            batch_products = data.get('products', [])
            all_products.extend(batch_products)
            
            print(f"✅ Page {page}: {len(batch_products)} produits (total: {len(all_products):,})")
            
            # Gestion de la pagination avec sécurité
            link_header = response.headers.get('Link')
            if link_header and 'rel="next"' in link_header and len(batch_products) > 0:
                parts = link_header.split(',')
                next_url = None
                for part in parts:
                    if 'rel="next"' in part:
                        next_url = part.split(';')[0].strip('<> ')
                        break
                
                if next_url and next_url != url:
                    url = next_url
                    params = {}
                    page += 1
                else:
                    print("⚠️ URL de pagination identique, arrêt")
                    break
            else:
                break
                
        except Exception as e:
            print(f"❌ Erreur lors de la récupération: {e}")
            break
    
    # Extraire les variants et collecter les inventory_item_ids
    variants_data = []
    inventory_item_ids = set()
    
    print(f"\n🔄 Extraction des variants depuis {len(all_products):,} produits...")
    
    for product in all_products:
        for variant in product.get('variants', []):
            inventory_item_id = variant.get('inventory_item_id')
            if inventory_item_id:
                inventory_item_ids.add(inventory_item_id)
            
            # Extraire les options (color, size) intelligemment
            options = product.get('options', [])
            color_value = None
            size_value = None
            
            for i, option in enumerate(options):
                option_name = option.get('name', '').lower()
                if option_name in ['color', 'colour', 'couleur']:
                    color_value = variant.get(f'option{i+1}')
                elif option_name in ['size', 'taille', 'dimension']:
                    size_value = variant.get(f'option{i+1}')
            
            variants_data.append({
                'variant_id': variant.get('id'),
                'product_id': product.get('id'),
                'inventory_item_id': inventory_item_id,
                'sku': variant.get('sku'),
                'barcode': variant.get('barcode'),
                'title': variant.get('title'),
                'status': product.get('status'),
                'vendor': product.get('vendor'),
                'value_color': color_value,
                'value_size': size_value,
                'price': variant.get('price'),
                'compare_at_price': variant.get('compare_at_price'),
                'weight': variant.get('weight'),
                'weight_unit': variant.get('weight_unit'),
                'position': variant.get('position'),
                'product_title': product.get('title'),
                'product_handle': product.get('handle'),
                'product_type': product.get('product_type'),
                'tags': product.get('tags'),
                'created_at': variant.get('created_at'),
                'updated_at': variant.get('updated_at'),
                'cogs': None  # Sera rempli plus tard
            })
    
    print(f"✅ {len(variants_data)} variants extraits")
    print(f"🔍 {len(inventory_item_ids)} inventory items uniques à récupérer...")
    
    # Récupérer les COGS depuis les inventory items
    inventory_items_data = get_inventory_items_cogs(list(inventory_item_ids))
    
    # Mapper les COGS avec les variants (TOUS les variants seront gardés)
    print(f"🔗 Mapping des COGS avec les variants...")
    cogs_found = 0
    cogs_zero = 0
    cogs_missing = 0
    
    for variant in variants_data:
        inventory_item_id = variant.get('inventory_item_id')
        if inventory_item_id and inventory_item_id in inventory_items_data:
            cogs = inventory_items_data[inventory_item_id].get('cost')
            if cogs is not None:
                if float(cogs) > 0:
                    variant['cogs'] = float(cogs)
                    cogs_found += 1
                else:
                    # COGS = 0, on le garde quand même
                    variant['cogs'] = 0.0
                    cogs_zero += 1
            else:
                # Pas de COGS dans l'inventory item, on garde le variant avec COGS = None
                cogs_missing += 1
        else:
            # Pas d'inventory item trouvé, on garde le variant avec COGS = None
            cogs_missing += 1
    
    # Statistiques détaillées
    total_variants = len(variants_data)
    print(f"✅ Mapping terminé pour {total_variants} variants:")
    
    if total_variants > 0:
        print(f"   • COGS > 0: {cogs_found} variants ({cogs_found/total_variants*100:.1f}%)")
        print(f"   • COGS = 0: {cogs_zero} variants ({cogs_zero/total_variants*100:.1f}%)")
        print(f"   • Sans COGS: {cogs_missing} variants ({cogs_missing/total_variants*100:.1f}%)")
        print(f"   🎯 TOUS les {total_variants} variants seront insérés en base")
    else:
        print("   ℹ️  Aucun variant à traiter")
    
    return {
        'products': all_products,
        'variants': variants_data,
        'inventory_items': inventory_items_data
    }

def insert_products_to_db(variants_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insère/met à jour les variants dans la table products
    
    Returns:
        Dict avec les statistiques d'insertion
    """
    if not variants_data:
        return {"inserted": 0, "updated": 0, "errors": 0}
    
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        
        print(f"\n📥 Insertion/mise à jour de {len(variants_data)} variants...")
        print("🎯 Note: TOUS les variants seront insérés, même ceux sans COGS")
        
        # Requête d'insertion avec UPSERT
        insert_query = """
        INSERT INTO products (
            variant_id, product_id, inventory_item_id, cogs, status, vendor,
            barcode, sku, value_color, value_size, title, price, compare_at_price,
            weight, weight_unit, position, product_title,
            product_handle, product_type, tags, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (variant_id) DO UPDATE SET
            product_id = EXCLUDED.product_id,
            inventory_item_id = EXCLUDED.inventory_item_id,
            cogs = EXCLUDED.cogs,
            status = EXCLUDED.status,
            vendor = EXCLUDED.vendor,
            barcode = EXCLUDED.barcode,
            sku = EXCLUDED.sku,
            value_color = EXCLUDED.value_color,
            value_size = EXCLUDED.value_size,
            title = EXCLUDED.title,
            price = EXCLUDED.price,
            compare_at_price = EXCLUDED.compare_at_price,
            weight = EXCLUDED.weight,
            weight_unit = EXCLUDED.weight_unit,
            position = EXCLUDED.position,
            product_title = EXCLUDED.product_title,
            product_handle = EXCLUDED.product_handle,
            product_type = EXCLUDED.product_type,
            tags = EXCLUDED.tags,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            imported_at = CURRENT_TIMESTAMP
        """
        
        # Préparer les données
        insert_data = []
        for variant in variants_data:
            # Convertir les dates
            created_at = None
            updated_at = None
            
            if variant.get('created_at'):
                try:
                    created_at = datetime.fromisoformat(variant['created_at'].replace('Z', '+00:00'))
                except:
                    pass
            
            if variant.get('updated_at'):
                try:
                    updated_at = datetime.fromisoformat(variant['updated_at'].replace('Z', '+00:00'))
                except:
                    pass
            
            insert_data.append((
                variant.get('variant_id'),
                variant.get('product_id'),
                variant.get('inventory_item_id'),
                float(variant.get('cogs')) if variant.get('cogs') else None,
                variant.get('status'),
                variant.get('vendor'),
                variant.get('barcode'),
                variant.get('sku'),
                variant.get('value_color'),
                variant.get('value_size'),
                variant.get('title'),
                float(variant.get('price')) if variant.get('price') else None,
                float(variant.get('compare_at_price')) if variant.get('compare_at_price') else None,
                float(variant.get('weight')) if variant.get('weight') else None,
                variant.get('weight_unit'),
                variant.get('position'),
                variant.get('product_title'),
                variant.get('product_handle'),
                variant.get('product_type'),
                variant.get('tags'),
                created_at,
                updated_at
            ))
        
        # Exécuter l'insertion par batch (optimisé pour PostgreSQL)
        batch_size = 500  # 5x plus rapide (11,000 paramètres max vs limite 50,000+)
        inserted = 0
        
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i+batch_size]
            cur.executemany(insert_query, batch)
            inserted += len(batch)
            print(f"✅ Batch {i//batch_size + 1}/{(len(insert_data) + batch_size - 1)//batch_size}")
        
        conn.commit()
        
        # Statistiques finales détaillées
        cur.execute("SELECT COUNT(*) FROM products")
        total_count = cur.fetchone()[0]
        
        # Compter les variants avec/sans COGS insérés
        variants_with_cogs = len([v for v in variants_data if v.get('cogs') is not None])
        variants_without_cogs = len(variants_data) - variants_with_cogs
        
        print(f"\n📊 Résultats d'insertion:")
        print(f"   • Variants traités: {len(variants_data)}")
        print(f"   • Avec COGS: {variants_with_cogs}")
        print(f"   • Sans COGS: {variants_without_cogs}")
        print(f"   • Total en base: {total_count:,}")
        print(f"   ✅ Tous les variants ont été insérés, même ceux sans COGS")
        
        return {
            "inserted": inserted, 
            "updated": 0, 
            "errors": 0, 
            "total": total_count,
            "with_cogs": variants_with_cogs,
            "without_cogs": variants_without_cogs
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion: {e}")
        return {"inserted": 0, "updated": 0, "errors": 1}
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def update_products_incremental() -> Dict[str, Any]:
    """
    Met à jour les produits de manière incrémentale (nouveaux + modifiés)
    Utilise updated_at pour capturer les deux cas
    
    Returns:
        Dict avec les résultats de la mise à jour
    """
    try:
        print("🔍 Démarrage de la mise à jour incrémentale des produits...")
        
        # 1. Récupérer la date de la dernière mise à jour
        latest_date = get_latest_product_update_date()
        
        # 2. Récupérer les produits mis à jour (nouveaux + modifiés)
        shopify_data = get_shopify_products_since(latest_date)
        variants_data = shopify_data['variants']
        
        if not variants_data:
            return {
                "success": True,
                "message": "Aucun produit nouveau ou modifié à synchroniser",
                "details": {"inserted": 0, "updated": 0, "errors": 0, "total": 0}
            }
        
        # 3. Insérer/mettre à jour les produits
        result = insert_products_to_db(variants_data)
        
        return {
            "success": True,
            "message": f"{result['inserted']} variants synchronisés (nouveaux + modifiés)",
            "details": result
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la mise à jour incrémentale: {e}")
        return {
            "success": False,
            "message": f"Erreur: {str(e)}",
            "details": {"inserted": 0, "updated": 0, "errors": 1, "total": 0}
        }

def update_products_full_sync() -> Dict[str, Any]:
    """
    Synchronisation complète (tous les produits, première fois)
    
    Returns:
        Dict avec les résultats de la mise à jour
    """
    try:
        print("🔍 Démarrage de la synchronisation complète...")
        
        # Récupérer tous les produits (sans filtre de date)
        shopify_data = get_shopify_products_since(None)
        variants_data = shopify_data['variants']
        
        if not variants_data:
            return {
                "success": True,
                "message": "Aucun produit trouvé",
                "details": {"inserted": 0, "updated": 0, "errors": 0, "total": 0}
            }
        
        # Insérer/mettre à jour tous les produits
        result = insert_products_to_db(variants_data)
        
        return {
            "success": True,
            "message": f"Synchronisation complète: {result['inserted']} variants traités",
            "details": result
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la synchronisation complète: {e}")
        return {
            "success": False,
            "message": f"Erreur: {str(e)}",
            "details": {"inserted": 0, "updated": 0, "errors": 1, "total": 0}
        }

# Fonctions dépréciées (maintenues pour compatibilité)
def get_latest_product_date() -> Optional[str]:
    """DÉPRÉCIÉ: Utilise get_latest_product_update_date() à la place"""
    return get_latest_product_update_date()

def get_new_shopify_products(since_date: Optional[str] = None, use_updated_at: bool = False) -> Dict[str, Any]:
    """DÉPRÉCIÉ: Utilise get_shopify_products_since() à la place"""
    return get_shopify_products_since(since_date)

def update_products_with_changes() -> Dict[str, Any]:
    """DÉPRÉCIÉ: Utilise update_products_incremental() à la place"""
    return update_products_incremental()