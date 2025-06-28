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

def get_latest_product_date() -> Optional[str]:
    """
    Récupère la date de création du produit le plus récent en base
    
    Returns:
        Optional[str]: Date ISO de création du produit le plus récent ou None
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
        
        # Récupérer la date de création la plus récente
        cur.execute("""
            SELECT MAX(created_at) FROM products 
            WHERE created_at IS NOT NULL
        """)
        
        result = cur.fetchone()
        latest_date = result[0] if result else None
        
        if latest_date:
            # Convertir en format ISO string
            return latest_date.isoformat()
        else:
            print("📋 Aucun produit avec date de création trouvé en base")
            return None
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération de la date: {e}")
        return None
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_new_shopify_products(since_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Récupère les produits Shopify créés après une date donnée
    
    Args:
        since_date: Date ISO à partir de laquelle récupérer les produits
        
    Returns:
        Dict contenant products et variants
    """
    load_dotenv()
    
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    if not store_domain:
        raise ValueError("SHOPIFY_STORE_DOMAIN non défini dans les variables d'environnement")
    
    if since_date:
        print(f"🔍 Récupération des produits créés après {since_date}...")
    else:
        print(f"🔍 Récupération de tous les produits (première synchronisation)...")
    
    # Construire l'URL avec filtre de date si nécessaire
    url = f"https://{store_domain}/admin/api/{api_version}/products.json"
    headers = _shopify_headers()
    params = {
        "limit": 250,
        "fields": "id,title,handle,status,product_type,vendor,tags,created_at,updated_at,variants,options"
    }
    
    # Ajouter le filtre de date si spécifié
    if since_date:
        params["created_at_min"] = since_date
    
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
    
    # Extraire les variants
    variants_data = []
    
    print(f"\n🔄 Extraction des variants depuis {len(all_products):,} produits...")
    
    for product in all_products:
        for variant in product.get('variants', []):
            inventory_item_id = variant.get('inventory_item_id')
            
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
                'cogs': None
            })
    
    print(f"✅ {len(variants_data)} variants extraits")
    
    return {
        'products': all_products,
        'variants': variants_data
    }

def insert_products_to_db(variants_data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insère les nouveaux variants dans la table products
    
    Returns:
        Dict avec les statistiques d'insertion
    """
    if not variants_data:
        return {"inserted": 0, "updated": 0, "errors": 0}
    
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        
        print(f"\n📥 Insertion/mise à jour de {len(variants_data)} variants...")
        
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
        
        # Exécuter l'insertion par batch
        batch_size = 100
        inserted = 0
        updated = 0
        
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i+batch_size]
            cur.executemany(insert_query, batch)
            inserted += len(batch)
            print(f"✅ Batch {i//batch_size + 1}/{(len(insert_data) + batch_size - 1)//batch_size}")
        
        conn.commit()
        
        # Statistiques finales
        cur.execute("SELECT COUNT(*) FROM products")
        total_count = cur.fetchone()[0]
        
        print(f"\n📊 Résultats:")
        print(f"   • Variants traités: {len(variants_data)}")
        print(f"   • Total en base: {total_count:,}")
        
        return {"inserted": inserted, "updated": 0, "errors": 0, "total": total_count}
        
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
    Met à jour les produits de manière incrémentale
    
    Returns:
        Dict avec les résultats de la mise à jour
    """
    try:
        print("🔍 Démarrage de la mise à jour incrémentale des produits...")
        
        # 1. Récupérer la date du dernier produit
        latest_date = get_latest_product_date()
        
        # 2. Récupérer les nouveaux produits
        shopify_data = get_new_shopify_products(latest_date)
        variants_data = shopify_data['variants']
        
        if not variants_data:
            return {
                "success": True,
                "message": "Aucun nouveau produit à synchroniser",
                "details": {"inserted": 0, "updated": 0, "errors": 0, "total": 0}
            }
        
        # 3. Insérer les nouveaux produits
        result = insert_products_to_db(variants_data)
        
        return {
            "success": True,
            "message": f"{result['inserted']} nouveaux variants synchronisés",
            "details": result
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la mise à jour incrémentale: {e}")
        return {
            "success": False,
            "message": f"Erreur: {str(e)}",
            "details": {"inserted": 0, "updated": 0, "errors": 1, "total": 0}
        }