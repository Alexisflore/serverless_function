import os
import requests
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import psycopg2
from typing import List, Dict, Any

load_dotenv()

# ---------------------------------------------------------------------------
# 1. Utilitaires de base (comme dans process_transactions.py)
# ---------------------------------------------------------------------------

def _shopify_headers() -> Dict[str, str]:
    """Retourne les headers pour les requêtes Shopify API"""
    load_dotenv()
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json",
    }

def _pg_connect():
    """Connexion centralisée à PostgreSQL"""
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

def _iso_to_dt(date_str: str) -> datetime:
    """Convertit 2025-03-26T19:11:42-04:00 → obj datetime en UTC."""
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    return datetime.fromisoformat(date_str)

# ---------------------------------------------------------------------------
# 2. Récupération des draft orders depuis Shopify
# ---------------------------------------------------------------------------

def get_draft_orders_between_dates(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """
    Récupère tous les draft orders créés ou mis à jour entre les dates spécifiées
    """
    print(f"Récupération des draft orders entre {start.isoformat()} et {end.isoformat()}")
    
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    
    formatted_start = start.isoformat()
    formatted_end = end.isoformat()
    
    # Récupération des draft orders avec status=any et filtrage par date
    url = (
        f"https://{store_domain}/admin/api/{api_version}/draft_orders.json"
        f"?updated_at_min={formatted_start}&updated_at_max={formatted_end}"
        f"&limit=250"
    )
    
    all_drafts = []
    
    while url:
        print(f"Récupération des draft orders depuis: {url}")
        resp = requests.get(url, headers=_shopify_headers())
        print(resp.text)
        print(resp.status_code)
        if not resp.ok:
            print(f"[Draft Orders] {resp.status_code}: {resp.text}")
            break
            
        data = resp.json()
        drafts = data.get("draft_orders", [])
        all_drafts.extend(drafts)
        
        print(f"Récupéré {len(drafts)} draft orders (total: {len(all_drafts)})")
        
        # Pagination via Link header
        url = None
        if 'Link' in resp.headers:
            links = resp.headers['Link'].split(',')
            for link in links:
                if 'rel="next"' in link:
                    url = link.split('<')[1].split('>')[0]
                    break
    
    print(f"Total des draft orders récupérés: {len(all_drafts)}")
    return all_drafts

def get_draft_orders_since_date(dt_since: datetime) -> List[Dict[str, Any]]:
    """Récupère les draft orders depuis une date donnée"""
    print(f"Récupération des draft orders depuis {dt_since.isoformat()}")
    return get_draft_orders_between_dates(dt_since, datetime.now())

# ---------------------------------------------------------------------------
# 3. Traitement des draft orders
# ---------------------------------------------------------------------------

def process_draft_order(draft_order: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Process a single draft order and return a list of formatted transaction entries
    """
    draft_id = draft_order.get("id")
    print(f"Traitement du draft order {draft_id}...")
    transactions = []
    
    # Extract basic draft order information
    status = draft_order.get("status")
    created_at = draft_order.get("created_at")
    completed_at = draft_order.get("completed_at")
    order_id = draft_order.get("order_id")
    
    # Debug: Afficher les informations importantes du draft order
    print(f"  - Status: {status}")
    print(f"  - Created at: {created_at}")
    print(f"  - Completed at: {completed_at}")
    print(f"  - Order ID: {order_id}")
    if order_id is None:
        print(f"  - ⚠️  Order ID est null car le draft order n'est pas encore finalisé (status: {status})")
    else:
        print(f"  - ✅ Order ID présent: draft order converti en commande {order_id}")
    
    # Get customer info
    customer = draft_order.get("customer")
    client_id = customer.get("id", -1) if customer else -1
    
    # Get line items
    line_items = draft_order.get("line_items", [])
    print(f"  - Draft {draft_id} contient {len(line_items)} line items")
    
    # Currency - from draft order
    currency = draft_order.get("currency", "USD")
    
    # Process each line item
    for i, item in enumerate(line_items):
        # Basic line item info
        product_id = item.get("product_id")
        product_name = item.get("title")
        price = float(item.get("price", 0))
        quantity = int(item.get("quantity", 1))
        
        # Create transaction for each line item
        item_transaction = {
            "_draft_id": draft_id,
            "created_at": created_at,
            "completed_at": completed_at,
            "order_id": order_id,
            "client_id": client_id,
            "product_id": product_id,
            "type": "draft_order_item",
            "account_type": "Draft Orders",
            "transaction_description": f"Draft: {product_name} x {quantity}",
            "amount": price * quantity,
            "status": status,
            "transaction_currency": currency,
            "source_name": "draft_order",
            "quantity": quantity,
        }
        transactions.append(item_transaction)
        
        # Process taxes if present
        tax_lines = item.get("tax_lines", [])
        if tax_lines:
            print(f"  - Item {i+1}: {len(tax_lines)} taxes trouvées")
        for tax in tax_lines:
            try:
                tax_price = float(tax.get("price", 0)) * quantity
            except (ValueError, TypeError):
                tax_price = 0.0
                
            tax_transaction = {
                "_draft_id": draft_id,
                "created_at": created_at,
                "completed_at": completed_at,
                "order_id": order_id,
                "client_id": client_id,
                "product_id": product_id,
                "type": "draft_order_tax",
                "account_type": "Taxes",
                "transaction_description": f"Draft Tax: {tax.get('title')}",
                "amount": tax_price,
                "status": status,
                "transaction_currency": currency,
                "source_name": "draft_order",
                "quantity": quantity,
            }
            transactions.append(tax_transaction)
    
    # Process shipping line if present
    shipping_line = draft_order.get("shipping_line")
    if shipping_line:
        print("  - Frais d'expédition trouvés")
        try:
            shipping_price = float(shipping_line.get("price", 0))
        except (ValueError, TypeError):
            shipping_price = 0.0
            
        shipping_transaction = {
            "_draft_id": draft_id,
            "created_at": created_at,
            "completed_at": completed_at,
            "order_id": order_id,
            "client_id": client_id,
            "product_id": None,
            "type": "draft_order_shipping",
            "account_type": "Shipping",
            "transaction_description": "Draft Shipping",
            "amount": shipping_price,
            "status": status,
            "transaction_currency": currency,
            "source_name": "draft_order",
            "quantity": 1,
        }
        transactions.append(shipping_transaction)
    
    print(f"  - Total: {len(transactions)} transactions générées")
    return transactions

# ---------------------------------------------------------------------------
# 4. Persistance en base
# ---------------------------------------------------------------------------

def process_draft_orders(draft_transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Processes a list of draft order transactions and inserts them into the database
    """
    print(f"Traitement de {len(draft_transactions)} transactions de draft orders...")
    
    stats = {
        "transactions_inserted": 0,
        "transactions_updated": 0, 
        "transactions_skipped": 0,
        "errors": []
    }
    
    if not draft_transactions:
        print("Aucune transaction à traiter.")
        return stats
    
    print("Connexion à la base de données...")
    conn = _pg_connect()
    cur = conn.cursor()
    
    # Requêtes SQL
    check_query = """
        SELECT id FROM draft_order 
        WHERE _draft_id = %s AND account_type = %s AND type = %s 
        AND transaction_description = %s AND amount = %s
    """
    
    insert_query = """
        INSERT INTO draft_order (
            _draft_id, created_at, completed_at, order_id, client_id,
            product_id, type, account_type, transaction_description,
            amount, status, transaction_currency, source_name, quantity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    update_query = """
        UPDATE draft_order SET
        created_at = %s,
        completed_at = %s,
        order_id = %s,
        client_id = %s,
        product_id = %s,
        account_type = %s,
        transaction_currency = %s,
        source_name = %s,
        status = %s,
        quantity = %s,
        updated_at_timestamp = CURRENT_TIMESTAMP
        WHERE id = %s
    """
    
    try:
        for i, transaction in enumerate(draft_transactions):
            if i % 50 == 0 and i > 0:
                print(f"Progression: {i}/{len(draft_transactions)} transactions traitées")
            
            try:
                # For client_id, which cannot be NULL, use -1 as default
                if transaction.get('client_id') is None or transaction.get('client_id') == 'N/A':
                    transaction['client_id'] = -1
                
                # Convert date strings to datetime if they're strings
                created_at = transaction["created_at"]
                if isinstance(created_at, str):
                    created_at = _iso_to_dt(created_at)
                
                # Handle completed_at which can be NULL
                completed_at = transaction.get("completed_at")
                if completed_at and isinstance(completed_at, str):
                    completed_at = _iso_to_dt(completed_at)
                
                # Check if transaction already exists
                check_params = (
                    transaction["_draft_id"],
                    transaction["account_type"],
                    transaction["type"],
                    transaction["transaction_description"],
                    transaction["amount"]
                )
                
                cur.execute(check_query, check_params)
                existing_id = cur.fetchone()
                
                if existing_id:
                    # Transaction already exists, update it
                    update_params = (
                        created_at,
                        completed_at,
                        transaction.get("order_id"),
                        transaction["client_id"],
                        transaction.get("product_id"),
                        transaction["account_type"],
                        transaction["transaction_currency"],
                        transaction.get("source_name"),
                        transaction.get("status"),
                        transaction.get("quantity", 1),
                        existing_id[0]
                    )
                    
                    cur.execute(update_query, update_params)
                    stats["transactions_updated"] += 1
                    
                else:
                    # Transaction doesn't exist, insert it
                    insert_params = (
                        transaction["_draft_id"],
                        created_at,
                        completed_at,
                        transaction.get("order_id"),
                        transaction["client_id"],
                        transaction.get("product_id"),
                        transaction["type"],
                        transaction["account_type"],
                        transaction["transaction_description"],
                        transaction["amount"],
                        transaction.get("status"),
                        transaction["transaction_currency"],
                        transaction.get("source_name"),
                        transaction.get("quantity", 1)
                    )
                    
                    cur.execute(insert_query, insert_params)
                    stats["transactions_inserted"] += 1
                    
            except Exception as e:
                error_msg = f"Error processing draft order transaction {transaction.get('_draft_id')}: {str(e)}"
                print(f"  - ERREUR: {error_msg}")
                stats["errors"].append(error_msg)
                stats["transactions_skipped"] += 1
                
        # Commit all changes
        print("Validation des changements (commit)...")
        conn.commit()
        print("Commit réussi.")
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Database error: {str(e)}"
        print(f"ERREUR DATABASE: {error_msg}")
        stats["errors"].append(error_msg)
        
    finally:
        # Close cursor and connection
        cur.close()
        conn.close()
        print("Connexion DB fermée.")
            
    print(f"Résultats: {stats['transactions_inserted']} insérées, {stats['transactions_updated']} mises à jour, {stats['transactions_skipped']} ignorées")
    return stats

# ---------------------------------------------------------------------------
# 5. Fonction principale de récupération et traitement
# ---------------------------------------------------------------------------

def get_drafts_since_date(last_processed_date: datetime) -> List[Dict[str, Any]]:
    """
    Retrieves all draft orders created or completed since the given date
    """
    print(f"Récupération des draft orders depuis: {last_processed_date}")
    
    # Récupère tous les draft orders depuis la date
    draft_orders = get_draft_orders_since_date(last_processed_date)
    
    all_transactions = []
    
    # Process each draft order
    print(f"Traitement de {len(draft_orders)} draft orders...")
    for i, draft in enumerate(draft_orders):
        if i % 10 == 0 and i > 0:
            print(f"Progression: {i}/{len(draft_orders)} draft orders traités")
        
        draft_transactions = process_draft_order(draft)
        all_transactions.extend(draft_transactions)
    
    print(f"Total des transactions générées: {len(all_transactions)}")
    return all_transactions

def find_last_draft_order_date() -> datetime:
    """
    Queries the database to find the most recent draft order transaction date
    Returns a datetime object representing the latest date, or
    a date 30 days in the past if no draft order transactions are found
    """
    print("Recherche de la dernière date de traitement des draft orders...")
    
    try:
        # Connect to the database
        print("Connexion à la base de données...")
        conn = _pg_connect()
        cur = conn.cursor()
        
        # Query to get the most recent draft order transaction date
        print("Exécution de la requête pour trouver la dernière date...")
        try:
            query = "SELECT MAX(created_at) FROM draft_order"
            cur.execute(query)
            last_date = cur.fetchone()[0]
        except Exception:
            # If the draft_order table doesn't exist yet, fall back to transaction table
            print("Table draft_order non trouvée, recherche dans la table transaction...")
            query = "SELECT MAX(date) FROM transaction WHERE source_name = 'draft_order'"
            cur.execute(query)
            last_date = cur.fetchone()[0]
        
        # If no transactions found, return date from 30 days ago
        if not last_date:
            print("Aucune transaction précédente trouvée, utilisation d'une date par défaut (30 jours en arrière)...")
            last_date = datetime.now() - timedelta(days=30)
            
        print(f"Dernière date de transaction draft order: {last_date}")
        return last_date
        
    except Exception as e:
        print(f"ERREUR lors de la récupération de la dernière date de transaction: {e}")
        # Return a default date (30 days ago) in case of error
        default_date = datetime.now() - timedelta(days=30)
        print(f"Utilisation de la date par défaut: {default_date}")
        return default_date
        
    finally:
        # Close cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# ---------------------------------------------------------------------------
# 6. Exemple d'exécution
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    print("=== Démarrage du traitement des draft orders ===")
    
    # Find the last processed date
    last_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    
    # Get all draft orders since that date
    print("Récupération des draft orders depuis la dernière date traitée...")
    draft_transactions = get_drafts_since_date(last_date)
    
    # Process the draft orders
    result = process_draft_orders(draft_transactions)
    
    print("=== Fin du traitement des draft orders ===")
    print(f"Draft orders processed: {result['transactions_inserted']} inserted, {result['transactions_updated']} updated, {result['transactions_skipped']} skipped")
    if result['errors']:
        print(f"Errors: {len(result['errors'])}")
        for error in result['errors'][:5]:  # Show first 5 errors
            print(f"  - {error}")