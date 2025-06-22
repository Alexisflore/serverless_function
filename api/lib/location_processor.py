"""
Module pour gérer les locations Shopify avec mise à jour incrémentale
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

def get_latest_location_date() -> Optional[str]:
    """
    Récupère la date de création de la location la plus récente en base
    
    Returns:
        Optional[str]: Date ISO de création de la location la plus récente ou None
    """
    try:
        conn = _pg_connect()
        cur = conn.cursor()
        
        # Vérifier si la table existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'locations'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            print("📋 Table 'locations' n'existe pas encore")
            return None
        
        # Récupérer la date de création la plus récente
        cur.execute("""
            SELECT MAX(created_at) FROM locations 
            WHERE created_at IS NOT NULL
        """)
        
        result = cur.fetchone()
        latest_date = result[0] if result else None
        
        if latest_date:
            # Convertir en format ISO string
            return latest_date.isoformat()
        else:
            print("📋 Aucune location avec date de création trouvée en base")
            return None
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération de la date: {e}")
        return None
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_new_shopify_locations(since_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Récupère les locations Shopify créées après une date donnée
    
    Args:
        since_date: Date ISO à partir de laquelle récupérer les locations
        
    Returns:
        List des locations
    """
    load_dotenv()
    
    store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-10")
    
    if not store_domain:
        raise ValueError("SHOPIFY_STORE_DOMAIN non défini dans les variables d'environnement")
    
    if since_date:
        print(f"🔍 Récupération des locations créées après {since_date}...")
    else:
        print(f"🔍 Récupération de toutes les locations (première synchronisation)...")
    
    # Construire l'URL
    url = f"https://{store_domain}/admin/api/{api_version}/locations.json"
    headers = _shopify_headers()
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ Erreur API Shopify: {response.status_code}")
            print(f"Réponse: {response.text}")
            return []
        
        data = response.json()
        all_locations = data.get('locations', [])
        
        # Filtrer par date si nécessaire (l'API locations ne supporte pas created_at_min)
        if since_date:
            filtered_locations = []
            since_datetime = datetime.fromisoformat(since_date.replace('Z', '+00:00'))
            
            for location in all_locations:
                created_at_str = location.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        if created_at > since_datetime:
                            filtered_locations.append(location)
                    except:
                        # En cas d'erreur de parsing, inclure la location
                        filtered_locations.append(location)
            
            print(f"✅ {len(filtered_locations)} nouvelles locations trouvées (sur {len(all_locations)} total)")
            return filtered_locations
        else:
            print(f"✅ {len(all_locations)} locations récupérées")
            return all_locations
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération: {e}")
        return []

def parse_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """Parse une date ISO8601 de Shopify en datetime Python"""
    if not date_str:
        return None
    try:
        # Shopify utilise le format ISO8601 avec timezone
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        return None

def ensure_locations_table():
    """S'assure que la table locations existe"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS locations (
        _location_id BIGINT PRIMARY KEY,
        name VARCHAR(100),
        active BOOLEAN,
        address1 VARCHAR(200),
        address2 VARCHAR(100),
        city VARCHAR(100),
        province VARCHAR(100),
        province_code VARCHAR(10),
        country VARCHAR(100),
        country_code VARCHAR(10),
        country_name VARCHAR(100),
        localized_country_name VARCHAR(100),
        localized_province_name VARCHAR(100),
        zip VARCHAR(20),
        phone VARCHAR(50),
        legacy BOOLEAN,
        admin_graphql_api_id VARCHAR(100),
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE,
        synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Index pour les recherches fréquentes
    CREATE INDEX IF NOT EXISTS idx_locations_active ON locations(active);
    CREATE INDEX IF NOT EXISTS idx_locations_country ON locations(country_code);
    CREATE INDEX IF NOT EXISTS idx_locations_name ON locations(name);
    CREATE INDEX IF NOT EXISTS idx_locations_synced_at ON locations(synced_at);
    """
    
    try:
        conn = _pg_connect()
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Erreur lors de la création de la table locations: {e}")
        return False

def insert_locations_to_db(locations: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insère ou met à jour les locations dans la base de données
    
    Args:
        locations: Liste des locations à insérer
        
    Returns:
        Dict avec les statistiques d'insertion
    """
    if not locations:
        return {"inserted": 0, "updated": 0, "errors": 0}
    
    insert_sql = """
    INSERT INTO locations (
        _location_id, name, active, address1, address2, city, province, 
        province_code, country, country_code, country_name, 
        localized_country_name, localized_province_name, zip, phone, 
        legacy, admin_graphql_api_id, created_at, updated_at, synced_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (_location_id) DO UPDATE SET
        name = EXCLUDED.name,
        active = EXCLUDED.active,
        address1 = EXCLUDED.address1,
        address2 = EXCLUDED.address2,
        city = EXCLUDED.city,
        province = EXCLUDED.province,
        province_code = EXCLUDED.province_code,
        country = EXCLUDED.country,
        country_code = EXCLUDED.country_code,
        country_name = EXCLUDED.country_name,
        localized_country_name = EXCLUDED.localized_country_name,
        localized_province_name = EXCLUDED.localized_province_name,
        zip = EXCLUDED.zip,
        phone = EXCLUDED.phone,
        legacy = EXCLUDED.legacy,
        admin_graphql_api_id = EXCLUDED.admin_graphql_api_id,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        synced_at = CURRENT_TIMESTAMP
    RETURNING (xmax = 0) AS inserted;
    """
    
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    
    try:
        conn = _pg_connect()
        cursor = conn.cursor()
        
        current_time = datetime.now()
        
        for i, location in enumerate(locations, 1):
            try:
                # Préparer les valeurs à insérer
                values = (
                    location.get('id'),                           # _location_id
                    location.get('name'),                         # name
                    location.get('active'),                       # active
                    location.get('address1'),                     # address1
                    location.get('address2'),                     # address2
                    location.get('city'),                         # city
                    location.get('province'),                     # province
                    location.get('province_code'),                # province_code
                    location.get('country'),                      # country
                    location.get('country_code'),                 # country_code
                    location.get('country_name'),                 # country_name
                    location.get('localized_country_name'),       # localized_country_name
                    location.get('localized_province_name'),      # localized_province_name
                    location.get('zip'),                          # zip
                    location.get('phone'),                        # phone
                    location.get('legacy'),                       # legacy
                    location.get('admin_graphql_api_id'),         # admin_graphql_api_id
                    parse_datetime(location.get('created_at')),   # created_at
                    parse_datetime(location.get('updated_at')),   # updated_at
                    current_time                                  # synced_at
                )
                
                cursor.execute(insert_sql, values)
                result = cursor.fetchone()
                
                if result and result[0]:  # inserted = True
                    stats["inserted"] += 1
                else:
                    stats["updated"] += 1
                
                # Afficher le progrès
                if i % 5 == 0 or i == len(locations):
                    print(f"   📍 {i}/{len(locations)} locations traitées")
                
            except Exception as e:
                print(f"❌ Erreur lors de l'insertion de la location {location.get('id', 'unknown')}: {e}")
                stats["errors"] += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Synchronisation terminée: {stats['inserted']} ajoutées, {stats['updated']} mises à jour, {stats['errors']} erreurs")
        return stats
        
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion en base: {e}")
        stats["errors"] = len(locations)
        return stats

def update_locations_incremental() -> Dict[str, Any]:
    """
    Met à jour les locations de manière incrémentale
    
    Returns:
        Dict avec les résultats de la synchronisation
    """
    print("🏢 SYNCHRONISATION INCRÉMENTALE DES LOCATIONS")
    print("=" * 50)
    
    try:
        # S'assurer que la table existe
        if not ensure_locations_table():
            return {
                "success": False,
                "error": "Impossible de créer/vérifier la table locations",
                "stats": {"inserted": 0, "updated": 0, "errors": 0}
            }
        
        # Récupérer la date de la dernière location en base
        latest_date = get_latest_location_date()
        
        # Récupérer les nouvelles locations
        locations = get_new_shopify_locations(latest_date)
        
        if not locations:
            print("✅ Aucune nouvelle location à synchroniser")
            return {
                "success": True,
                "message": "Aucune nouvelle location",
                "stats": {"inserted": 0, "updated": 0, "errors": 0}
            }
        
        # Synchroniser avec la base
        stats = insert_locations_to_db(locations)
        
        success = stats["errors"] < len(locations)  # Succès si pas toutes les locations en erreur
        
        return {
            "success": success,
            "message": f"Synchronisation {'réussie' if success else 'partiellement échouée'}",
            "stats": stats,
            "locations_processed": len(locations)
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de la synchronisation incrémentale: {e}")
        return {
            "success": False,
            "error": str(e),
            "stats": {"inserted": 0, "updated": 0, "errors": 0}
        } 