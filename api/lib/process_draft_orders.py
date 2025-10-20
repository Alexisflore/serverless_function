import os
import requests
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import psycopg2
from typing import List, Dict, Any
from collections import defaultdict
from api.lib.utils import get_source_location

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
        resp = requests.get(url, headers=_shopify_headers())
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

    return all_drafts

def get_draft_orders_since_date(dt_since: datetime) -> List[Dict[str, Any]]:
    """Récupère les draft orders depuis une date donnée"""
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
    tags = draft_order.get("tags", "")
    
    # Convert tags string to list for get_source_location function
    tags_list = [tag.strip() for tag in tags.split(',') if tag.strip()] if tags else []
    
    # Debug: Afficher les informations importantes du draft order
    print(f"  - Status: {status}")
    print(f"  - Created at: {created_at}")
    print(f"  - Completed at: {completed_at}")
    print(f"  - Order ID: {order_id}")
    print(f"  - Tags: {tags}")
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
    draft_order_name = draft_order.get("name")
    draft_order_note = draft_order.get("note")
    
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
            "source_location": get_source_location(tags_list),
            "sku": item.get("sku"),
            "variant_id": item.get("variant_id"),
            "variant_title": item.get("variant_title"),
            "name": item.get("name"),
            "draft_order_name": draft_order_name,
            "draft_order_note": draft_order_note,
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
                "source_location": get_source_location(tags_list),
                "sku": item.get("sku"),
                "variant_id": item.get("variant_id"),
                "variant_title": item.get("variant_title"),
                "name": item.get("name"),
                "draft_order_name": draft_order_name,
                "draft_order_note": draft_order_note,
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
            "source_location": get_source_location(tags_list),
            "sku": None,
            "variant_id": None,
            "variant_title": None,
            "name": None,
            "draft_order_name": draft_order_name,
            "draft_order_note": draft_order_note,
        }
        transactions.append(shipping_transaction)
    
    print(f"  - Total: {len(transactions)} transactions générées")
    return transactions

# ---------------------------------------------------------------------------
# 4. Persistance en base - NOUVELLE LOGIQUE
# ---------------------------------------------------------------------------

def process_draft_orders(draft_transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Processes a list of draft order transactions and inserts them into the database.
    Groups transactions by draft_id. For each draft_id, if it already exists in the database,
    all existing entries for that draft_id are deleted before inserting the new transactions.
    """
    print(f"Traitement de {len(draft_transactions)} transactions de draft orders...")
    
    stats = {
        "transactions_inserted": 0,
        "transactions_deleted": 0,
        "transactions_skipped": 0,
        "draft_orders_processed": 0,
        "errors": []
    }
    
    if not draft_transactions:
        print("Aucune transaction à traiter.")
        return stats
    
    # Group transactions by draft_id
    transactions_by_draft_id = defaultdict(list)
    for transaction in draft_transactions:
        draft_id = transaction.get("_draft_id")
        if draft_id:
            transactions_by_draft_id[draft_id].append(transaction)
        else:
            stats["errors"].append("Transaction without _draft_id found")
            stats["transactions_skipped"] += 1
    
    print(f"Transactions groupées par draft_id: {len(transactions_by_draft_id)} draft orders à traiter")
    conn = _pg_connect()
    cur = conn.cursor()
    
    # SQL queries
    check_draft_exists_query = "SELECT COUNT(*) FROM draft_order WHERE _draft_id = %s"
    delete_draft_query = "DELETE FROM draft_order WHERE _draft_id = %s"
    insert_query = """
        INSERT INTO draft_order (
            _draft_id, created_at, completed_at, order_id, client_id,
            product_id, type, account_type, transaction_description,
            amount, status, transaction_currency, source_name, quantity, source_location,
            sku, variant_id, variant_title, name, draft_order_name, draft_order_note
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        for draft_id, transactions in transactions_by_draft_id.items():
            print(f"\n📋 Traitement du draft_id: {draft_id} ({len(transactions)} transactions)")
            
            try:
                # Check if draft_id already exists in database
                cur.execute(check_draft_exists_query, (draft_id,))
                existing_count = cur.fetchone()[0]
                
                if existing_count > 0:
                    print(f"  - ⚠️  Draft_id {draft_id} existe déjà en base ({existing_count} transactions)")
                    print(f"  - 🗑️  Suppression de toutes les transactions existantes pour ce draft_id...")
                    
                    # Delete all existing transactions for this draft_id
                    cur.execute(delete_draft_query, (draft_id,))
                    deleted_count = cur.rowcount
                    stats["transactions_deleted"] += deleted_count
                    print(f"  - ✅ {deleted_count} transactions supprimées")
                else:
                    print(f"  - ✅ Draft_id {draft_id} n'existe pas en base, insertion directe")
                
                # Insert all transactions for this draft_id
                print(f"  - 📥 Insertion de {len(transactions)} nouvelles transactions...")
                
                for i, transaction in enumerate(transactions):
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
                        
                        # Insert transaction
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
                            transaction.get("quantity", 1),
                            transaction.get("source_location"),
                            transaction.get("sku"),
                            transaction.get("variant_id"),
                            transaction.get("variant_title"),
                            transaction.get("name"),
                            transaction.get("draft_order_name"),
                            transaction.get("draft_order_note"),
                        )
                        
                        cur.execute(insert_query, insert_params)
                        stats["transactions_inserted"] += 1
                        
                    except Exception as e:
                        error_msg = f"Error inserting transaction {i+1} for draft_id {draft_id}: {str(e)}"
                        print(f"    - ERREUR: {error_msg}")
                        stats["errors"].append(error_msg)
                        stats["transactions_skipped"] += 1
                
                stats["draft_orders_processed"] += 1
                print(f"  - ✅ Draft_id {draft_id} traité avec succès")
                
            except Exception as e:
                error_msg = f"Error processing draft_id {draft_id}: {str(e)}"
                print(f"  - ERREUR: {error_msg}")
                stats["errors"].append(error_msg)
                # Continue with next draft_id
        
        # Commit all changes
        print(f"\n💾 Validation des changements (commit)...")
        conn.commit()
        print("✅ Commit réussi.")
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Database error: {str(e)}"
        print(f"❌ ERREUR DATABASE: {error_msg}")
        stats["errors"].append(error_msg)
        
    finally:
        # Close cursor and connection
        cur.close()
        conn.close()

    print(f"\n📊 Résultats finaux:")
    print(f"  - Draft orders traités: {stats['draft_orders_processed']}")
    print(f"  - Transactions supprimées: {stats['transactions_deleted']}")
    print(f"  - Transactions insérées: {stats['transactions_inserted']}")
    print(f"  - Transactions ignorées: {stats['transactions_skipped']}")
    print(f"  - Erreurs: {len(stats['errors'])}")
    
    return stats

# ---------------------------------------------------------------------------
# 5. Fonction principale de récupération et traitement
# ---------------------------------------------------------------------------

def get_drafts_between_dates(start_date, end_date) -> List[Dict[str, Any]]:
    """
    Retrieves all draft orders updated between the given dates
    Args:
        start_date: start date (datetime object or ISO string)
        end_date: end date (datetime object or ISO string)
    """
    # Convert string to datetime if necessary
    if isinstance(start_date, str):
        try:
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        except ValueError:
            # Try without timezone info
            start_date = datetime.fromisoformat(start_date)
    
    if isinstance(end_date, str):
        try:
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        except ValueError:
            # Try without timezone info
            end_date = datetime.fromisoformat(end_date)
    
    # Récupère tous les draft orders entre les dates
    draft_orders = get_draft_orders_between_dates(start_date, end_date)
    
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

def get_drafts_since_date(last_processed_date) -> List[Dict[str, Any]]:
    """
    Retrieves all draft orders created or completed since the given date
    Args:
        last_processed_date: datetime object or ISO string
    """
    # Convert string to datetime if necessary
    if isinstance(last_processed_date, str):
        try:
            last_processed_date = datetime.fromisoformat(last_processed_date.replace('Z', '+00:00'))
        except ValueError:
            # Try without timezone info
            last_processed_date = datetime.fromisoformat(last_processed_date)
    
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
    print(f"Draft orders processed: {result['transactions_inserted']} inserted, {result['transactions_deleted']} deleted, {result['transactions_skipped']} skipped")
    if result['errors']:
        print(f"Errors: {len(result['errors'])}")
        for error in result['errors'][:5]:  # Show first 5 errors
            print(f"  - {error}")