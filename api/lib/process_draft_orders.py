import os
import requests
import json
import shopify
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import psycopg2
load_dotenv()

def get_draft_orders_by_date(target_date):
    """
    Renvoie deux listes de DraftOrder :
      - created_on_date : brouillons créés à target_date
      - completed_on_date : brouillons complétés à target_date
    target_date : datetime.date
    """
    print(f"Récupération des draft orders pour la date: {target_date}")
    
    # Initialize Shopify API client
    api_version = "2024-10"
    shop_url = "https://adam-lippes.myshopify.com"
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    # Configure the Shopify session
    print("Configuration de la session Shopify...")
    session = shopify.Session(shop_url, api_version, access_token)
    shopify.ShopifyResource.activate_session(session)
    
    # Définition de l'intervalle (UTC)
    start_dt = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    def fetch_and_filter(status, timestamp_attr):
        """
        Récupère tous les drafts d'un status donné, puis filtre
        en mémoire selon l'attribut timestamp_attr ("created_at" ou "completed_at").
        """
        print(f"Récupération des draft orders avec status={status} et filtre sur {timestamp_attr}...")
        results = []
        # Curseur initial : pas de page_info
        drafts = shopify.DraftOrder.find(status=status, limit=250)
        print(f"Nombre de draft orders récupérés (première page): {len(drafts)}")
        # La collection est paginée en mode cursor-based
        page_count = 1
        while True:
            for draft in drafts:
                ts = getattr(draft, timestamp_attr)
                if ts:
                    dt = datetime.fromisoformat(ts)
                    if start_dt <= dt < end_dt:
                        results.append(draft)
            # passe à la page suivante si disponible
            if hasattr(drafts, "next_page_url") and drafts.next_page_url:
                page_count += 1
                print(f"Récupération de la page {page_count}...")
                drafts = drafts.next_page()
            else:
                break
        print(f"Nombre total de draft orders filtrés pour {status}: {len(results)}")
        return results

    try:
        # 1) Brouillons créés ce jour (`status="open"`)
        created_on_date = fetch_and_filter(status="open", timestamp_attr="created_at")
        # 2) Brouillons complétés ce jour (`status="completed"`)
        completed_on_date = fetch_and_filter(status="completed", timestamp_attr="completed_at")
        
        print(f"Résultats: {len(created_on_date)} brouillons créés, {len(completed_on_date)} brouillons complétés")
        return created_on_date, completed_on_date
    
    finally:
        # Clean up the session
        print("Nettoyage de la session Shopify...")
        shopify.ShopifyResource.clear_session()

def process_draft_order(draft_order):
    """
    Process a single draft order and return a list of formatted transaction entries
    """
    print(f"Traitement du draft order {draft_order.id}...")
    transactions = []
    
    # Extract basic draft order information
    draft_id = draft_order.id
    status = draft_order.status
    created_at = draft_order.created_at
    completed_at = getattr(draft_order, "completed_at", None)
    order_id = getattr(draft_order, "order_id", None)
    
    # Use appropriate timestamp based on status
    timestamp = completed_at if completed_at else created_at
    
    # Get customer info
    customer = draft_order.customer
    client_id = customer.id if customer else -1
    
    # Get line items
    line_items = draft_order.line_items
    print(f"  - Draft {draft_id} contient {len(line_items)} line items")
    
    # Currency - from draft order
    currency = draft_order.currency
    
    # Process each line item
    for i, item in enumerate(line_items):
        # Basic line item info
        product_id = getattr(item, "product_id", None)
        product_name = item.title
        price = float(item.price)
        quantity = item.quantity
        
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
        }
        transactions.append(item_transaction)
        
        # Process taxes if present
        tax_lines = getattr(item, "tax_lines", [])
        if tax_lines:
            print(f"  - Item {i+1}: {len(tax_lines)} taxes trouvées")
        for tax in tax_lines:
            try:
                tax_price = float(tax.price) * quantity
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
                "transaction_description": f"Draft Tax: {tax.title}",
                "amount": tax_price,
                "status": status,
                "transaction_currency": currency,
                "source_name": "draft_order",
            }
            transactions.append(tax_transaction)
    
    # Process shipping line if present
    shipping_line = getattr(draft_order, "shipping_line", None)
    if shipping_line:
        print("  - Frais d'expédition trouvés")
        try:
            shipping_price = float(shipping_line.price)
        except (ValueError, TypeError, AttributeError):
            shipping_price = 0.0
            
        shipping_transaction = {
            "_draft_id": draft_id,
            "created_at": created_at,
            "completed_at": completed_at,
            "order_id": order_id,
            "client_id": client_id,
            "product_id": product_id,
            "type": "draft_order_shipping",
            "account_type": "Shipping",
            "transaction_description": "Draft Shipping",
            "amount": shipping_price,
            "status": status,
            "transaction_currency": currency,
            "source_name": "draft_order",
        }
        transactions.append(shipping_transaction)
    
    print(f"  - Total: {len(transactions)} transactions générées")
    return transactions

def get_drafts_since_date(last_processed_date):
    """
    Retrieves all draft orders created or completed since the given date
    
    Args:
        last_processed_date: datetime object representing the starting date
        
    Returns:
        List of draft order dictionaries processed into transaction format
    """
    print(f"Récupération des draft orders depuis: {last_processed_date}")
    # Convert to date only
    target_date = last_processed_date.date()
    current_date = datetime.now().date()
    
    all_transactions = []
    
    # Process each day from the last processed date to today
    while target_date <= current_date:
        print(f"Traitement du jour: {target_date}")
        created_drafts, completed_drafts = get_draft_orders_by_date(target_date)
        
        # Process created drafts
        print(f"Traitement de {len(created_drafts)} brouillons créés...")
        for i, draft in enumerate(created_drafts):
            print(f"Brouillon créé {i+1}/{len(created_drafts)}")
            draft_transactions = process_draft_order(draft)
            all_transactions.extend(draft_transactions)
        
        # Process completed drafts
        print(f"Traitement de {len(completed_drafts)} brouillons complétés...")
        for i, draft in enumerate(completed_drafts):
            print(f"Brouillon complété {i+1}/{len(completed_drafts)}")
            draft_transactions = process_draft_order(draft)
            all_transactions.extend(draft_transactions)
        
        # Move to next day
        target_date += timedelta(days=1)
    
    print(f"Total des transactions générées: {len(all_transactions)}")
    return all_transactions

def process_draft_orders(draft_transactions):
    """
    Processes a list of draft order transactions and inserts them into the database
    
    Args:
        draft_transactions: List of transaction dictionaries from draft orders
        
    Returns:
        Dictionary with results of the operation
    """
    print(f"Traitement de {len(draft_transactions)} transactions de draft orders...")
    if not draft_transactions:
        print("Aucune transaction à traiter.")
        return {
            "transactions_inserted": 0,
            "transactions_updated": 0, 
            "transactions_skipped": 0,
            "errors": []
        }
    
    # Get database connection parameters
    db_url = os.getenv("DATABASE_URL")
    
    # Alternative: use individual parameters if DATABASE_URL is not available
    if not db_url:
        print("DATABASE_URL non trouvée, utilisation des paramètres individuels...")
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    result = {
        "transactions_inserted": 0,
        "transactions_updated": 0,
        "transactions_skipped": 0,
        "errors": []
    }
    
    try:
        # Connect to the database
        print("Connexion à la base de données...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        for i, transaction in enumerate(draft_transactions):
            print(f"Traitement de la transaction {i+1}/{len(draft_transactions)}: draft_id={transaction.get('_draft_id')}")
            try:
                # For client_id, which cannot be NULL, use -1 as default
                if transaction.get('client_id') is None or transaction.get('client_id') == 'N/A':
                    transaction['client_id'] = -1
                
                # Convert date strings to datetime if they're strings
                created_at = transaction["created_at"]
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                
                # Handle completed_at which can be NULL
                completed_at = transaction.get("completed_at")
                if completed_at and isinstance(completed_at, str):
                    completed_at = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
                
                # Check if transaction already exists based on unique attributes
                check_query = """
                SELECT id FROM draft_order 
                WHERE _draft_id = %s AND account_type = %s AND type = %s 
                AND transaction_description = %s AND amount = %s
                """
                
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
                    print(f"  - Transaction existante (id={existing_id[0]}), mise à jour...")
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
                    updated_at_timestamp = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    
                    update_params = (
                        created_at,
                        completed_at,  # Can be None
                        transaction.get("order_id"),  # Can be None
                        transaction["client_id"],
                        transaction.get("product_id"),  # Can be None
                        transaction["account_type"],
                        transaction["transaction_currency"],
                        transaction.get("source_name"),
                        transaction.get("status"),
                        existing_id[0]
                    )
                    
                    cur.execute(update_query, update_params)
                    result["transactions_updated"] += 1
                    
                else:
                    # Transaction doesn't exist, insert it
                    print("  - Nouvelle transaction, insertion...")
                    insert_query = """
                    INSERT INTO draft_order (
                        _draft_id, created_at, completed_at, order_id, client_id,
                        product_id, type, account_type, transaction_description,
                        amount, status, transaction_currency, source_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    insert_params = (
                        transaction["_draft_id"],
                        created_at,
                        completed_at,  # Can be None
                        transaction.get("order_id"),  # Can be None
                        transaction["client_id"],
                        transaction.get("product_id"),  # Can be None
                        transaction["type"],
                        transaction["account_type"],
                        transaction["transaction_description"],
                        transaction["amount"],
                        transaction.get("status"),
                        transaction["transaction_currency"],
                        transaction.get("source_name")
                    )
                    
                    cur.execute(insert_query, insert_params)
                    result["transactions_inserted"] += 1
                    
            except Exception as e:
                error_msg = f"Error processing draft order transaction {transaction.get('_draft_id')}: {str(e)}"
                print(f"  - ERREUR: {error_msg}")
                result["errors"].append(error_msg)
                result["transactions_skipped"] += 1
                
        # Commit all changes
        print("Commit des changements dans la base de données...")
        conn.commit()
        print("Commit réussi.")
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Database error: {str(e)}"
        print(f"ERREUR DATABASE: {error_msg}")
        result["errors"].append(error_msg)
        
    finally:
        # Close cursor and connection
        if 'cur' in locals():
            print("Fermeture du curseur de base de données...")
            cur.close()
        if 'conn' in locals():
            print("Fermeture de la connexion à la base de données...")
            conn.close()
            
    print(f"Résultats: {result['transactions_inserted']} insérées, {result['transactions_updated']} mises à jour, {result['transactions_skipped']} ignorées")
    return result

def find_last_draft_order_date():
    """
    Queries the database to find the most recent draft order transaction date
    Returns a datetime object representing the latest date, or
    a date 30 days in the past if no draft order transactions are found
    """
    print("Recherche de la dernière date de traitement des draft orders...")
    load_dotenv()
    
    # Get database connection parameters
    db_url = os.getenv("DATABASE_URL")
    
    # Alternative: use individual parameters if DATABASE_URL is not available
    if not db_url:
        print("DATABASE_URL non trouvée, utilisation des paramètres individuels...")
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        # Connect to the database
        print("Connexion à la base de données...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Query to get the most recent draft order transaction date
        # Try to get from the draft_order table first
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
            print("Fermeture du curseur de base de données...")
            cur.close()
        if 'conn' in locals():
            print("Fermeture de la connexion à la base de données...")
            conn.close()

# Example usage
if __name__ == '__main__':
    print("Démarrage du traitement des draft orders...")
    # Find the last processed date
    last_date = find_last_draft_order_date()
    yesterday = datetime.now() - timedelta(days=1)
    
    # Get all draft orders since that date
    print("Récupération des draft orders depuis la dernière date traitée...")
    draft_transactions = get_drafts_since_date(yesterday)
    
    # Process the draft orders
    result = process_draft_orders(draft_transactions)
    
    print(f"Draft orders processed: {result['transactions_inserted']} inserted, {result['transactions_updated']} updated, {result['transactions_skipped']} skipped")
    print("Fin du traitement des draft orders.")