import requests
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2

def get_refund_details(order_id: str, refund_id: str, client_id: str):
    """
    Récupère les détails d'un remboursement et construit une liste d'items JSON pour :
      - Chaque ligne d'article remboursé (refund_line_items)
      - Chaque taxe associée à chaque ligne
      - Chaque remise (discount) associée à chaque ligne
    Chaque item JSON comporte les informations complètes : date, order_id, client_id, type, 
    ainsi que les détails du produit, localisation, devise, etc.
    """
    load_dotenv()
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    url_refund = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}/refunds/{refund_id}.json"
    response = requests.get(url_refund, headers=headers)

    refund_transactions = []
    if response.status_code == 200:
        refund_data = response.json().get("refund", {})
        # print(json.dumps(refund_data, indent=2, ensure_ascii=False))
        refund_date = refund_data.get("created_at", "N/A")
        refund_status = refund_data.get("status", "N/A")
        location_id = refund_data.get("location_id", "N/A")

        # Traiter chaque ligne d'article remboursé
        refund_line_items = refund_data.get("refund_line_items", [])
        for refund_item in refund_line_items:
            line_item = refund_item.get("line_item", {})
            subtotal = refund_item.get("subtotal", 0)
            product_id = line_item.get("product_id", "N/A")
            transaction_currency = refund_item.get("subtotal_set", {}).get("shop_money", {}).get("currency_code", "USD")
            
            # Créer un dictionnaire pour la ligne d'article
            line_item_dict = {
                "date": refund_date,
                "order_id": refund_data.get("order_id", order_id),
                "client_id": client_id,
                "type": "refund_line_item",
                "account_type": "Refunds",
                "transaction_description": f"Refund: {line_item.get('name', 'N/A')}",
                "amount": -float(subtotal) if isinstance(subtotal, (int, float, str)) else 0.0,
                "status": refund_status,
                "transaction_currency": transaction_currency,
                "location_id": refund_item.get("location_id") or location_id,
                "source_name": None,  # Sera rempli après
                "product_id": product_id,
            }
            refund_transactions.append(line_item_dict)
            
            # Traiter chaque taxe associée à la ligne d'article
            tax_lines = line_item.get("tax_lines", [])
            for tax in tax_lines:
                try:
                    tax_price = float(tax.get("price", 0))
                except (ValueError, TypeError):
                    tax_price = 0.0
                
                tax_dict = {
                    "date": refund_date,
                    "order_id": refund_data.get("order_id", order_id),
                    "client_id": client_id,
                    "type": "refund_tax",
                    "account_type": "Taxes",
                    "transaction_description": tax.get("title", "N/A"),
                    "amount": -tax_price,
                    "status": refund_status,
                    "product_id": product_id,
                    "transaction_currency": tax.get("price_set", {}).get("shop_money", {}).get("currency_code", "USD"),
                    "location_id": refund_data.get("location_id", "N/A"),
                    "source_name": None,  # Sera rempli après
                }
                refund_transactions.append(tax_dict)
            
            # Traiter chaque remise (discount) associée à la ligne d'article
            # discount_allocations = line_item.get("discount_allocations", [])
            # for discount in discount_allocations:
            #     try:
            #         discount_amount = float(discount.get("amount", 0))
            #     except (ValueError, TypeError):
            #         discount_amount = 0.0
                
            #     discount_dict = {
            #         "date": refund_date,
            #         "order_id": refund_data.get("order_id", order_id),
            #         "client_id": client_id,
            #         "type": "refund_discount",
            #         "account_type": "Discounts", 
            #         "transaction_description": f"Discount Refund: {line_item.get('name', 'N/A')}",
            #         "amount": discount_amount,  # Note: garder le montant positif pour un remboursement de remise
            #         "status": refund_status,
            #         "product_id": product_id,
            #         "transaction_currency": discount.get("amount_set", {}).get("shop_money", {}).get("currency_code", "USD"),
            #         "location_id": location_id,
            #     }
            #     refund_transactions.append(discount_dict)

        return refund_transactions
    else:
        print(f"Erreur lors de la récupération du refund (code {response.status_code}) : {response.text}")
        return []


def get_transactions_by_date(order_id: str):
    """
    Récupère les informations d'une commande et crée une liste d'items JSON détaillés par date de transaction.
    Pour chaque fulfillment, pour chaque line item, on crée :
      - Un item de type "sales_gross" avec le montant HT initial (avant réduction), provenant du champ "price".
      - Si une réduction est appliquée (c'est-à-dire que price > pre_tax_price), un item "discount_line" est créé avec :
            discount = price - pre_tax_price (en valeur négative).
      - Pour chaque taxe associée au line item, un item de type "tax" avec le montant positif.
    Ensuite, pour chaque refund, on ajoute les items détaillés obtenus via get_refund_details.
    
    Ainsi, la liste par date permettra de reconstituer, en faisant la somme des items d'une transaction (par date),
    le montant réel payé ou remboursé.
    """
    load_dotenv()
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    url_order = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}.json"
    response = requests.get(url_order, headers=headers)

    transactions = []
    if response.status_code == 200:
        order_data = response.json().get("order", {})
        # print("Détails de la commande:", json.dumps(order_data, indent=2, ensure_ascii=False))
        customer = order_data.get("customer", {})
        if customer:
            client_id = customer.get("id", "N/A")
        else:
            client_id = "N/A"
        
        # Récupérer le source_name de la commande
        source_name = order_data.get("source_name", "N/A")

        # Traiter les fulfillments pour distinguer les transactions par date
        fulfillments = order_data.get("fulfillments", [])
        # print(json.dumps(fulfillments, indent=2, ensure_ascii=False))
        for fulfillment in fulfillments:
            location_id = fulfillment.get("location_id", "N/A")
            status = fulfillment.get("status", "N/A")
            fulfillment_date = fulfillment.get("created_at", "N/A")
            # Pour chaque line item dans le fulfillment, créer 2 items si applicable
            for line_item in fulfillment.get("line_items", []):
                product_id = line_item.get("product_id", "N/A")
                try:
                    gross_price = float(line_item.get("price", 0))
                except (ValueError, TypeError):
                    gross_price = 0.0
                try:
                    pre_tax_price = float(line_item.get("pre_tax_price", 0))
                    transaction_currency = line_item.get("pre_tax_price_set", {}).get("shop_money", {}).get("currency_code", "N/A")
                except (ValueError, TypeError):
                    pre_tax_price = 0.0

                # Item pour le montant HT brut (gross sale)
                sales_gross_item = {
                    "date": fulfillment_date,
                    "order_id": order_data.get("id", order_id),
                    "client_id": client_id,
                    "type": "sales_gross",
                    "account_type": "Sales",
                    "transaction_description": line_item.get("name", "N/A") + " Gross HT",
                    "amount": gross_price,
                    "location_id": location_id,
                    "source_name": source_name,
                    "status": status,
                    "product_id": product_id,
                    "transaction_currency": transaction_currency,
                }
                transactions.append(sales_gross_item)

                # Si une réduction a été appliquée, calculer et ajouter un item discount
                if line_item.get("discount_allocations"):
                    for discount_allocation in line_item.get("discount_allocations", []):
                        discount_value = float(discount_allocation.get("amount", 0))
                        transaction_currency = discount_allocation.get("amount_set", {}).get("shop_money", {}).get("currency_code", "N/A")
                        discount_item = {
                            "date": fulfillment_date,
                            "order_id": order_data.get("id", order_id),
                            "client_id": client_id,
                            "type": "discount_line",
                            "account_type": "Discounts",
                            "transaction_description": "Discount for " + line_item.get("name", "N/A"),
                            "amount": -discount_value,
                            "transaction_currency": transaction_currency,
                            "location_id": location_id,
                            "source_name": source_name,
                            "status": status,   
                            "product_id": product_id,
                        }
                    transactions.append(discount_item)

                # Pour chaque taxe appliquée au line item, ajouter un item tax
                tax_lines = line_item.get("tax_lines", [])
                for tax in tax_lines:
                    try:
                        tax_amt = float(tax.get("price", 0))
                    except (ValueError, TypeError):
                        tax_amt = 0.0
                    transaction_currency = tax.get("price_set", {}).get("shop_money", {}).get("currency_code", "N/A")

                    tax_item = {
                        "date": fulfillment_date,
                        "order_id": order_data.get("id", order_id),
                        "client_id": client_id,
                        "account_type": "Taxes",
                        "transaction_description": tax.get("title", "N/A"),
                        "amount": tax_amt,
                        "transaction_currency": transaction_currency,
                        "location_id": location_id,
                        "source_name": source_name,
                        "status": status,
                        "product_id": product_id,
                    }
                    transactions.append(tax_item)

        # Traiter les refunds (chaque refund possède sa propre date)
        refunds = order_data.get("refunds", [])
        if refunds:
            print("\n=== Remboursements trouvés ===")
            for refund in refunds:
                refund_id = refund.get("id")
                print(f"- Refund ID: {refund_id}")
                refund_items = get_refund_details(order_id, refund_id, client_id)
                
                # Ajouter l'information source_name à chaque item de remboursement
                for item in refund_items:
                    item["source_name"] = source_name
                
                transactions.extend(refund_items)
        else:
            print("\nAucun remboursement trouvé dans la commande.")

        # Optionnel : trier les transactions par date
        transactions.sort(key=lambda x: datetime.fromisoformat(x["date"].replace("Z", "+00:00")))
        
        total_sum = sum(item["amount"] for item in transactions)
        print(f"\nSomme totale des transactions: {total_sum} USD")
        print("\n=== Transactions détaillées ===")
        # print(json.dumps(transactions, indent=2, ensure_ascii=False))
        return transactions
    else:
        print(f"Erreur lors de la récupération de la commande (code {response.status_code}) : {response.text}")
        return []

def find_last_transaction_date():
    """
    Queries the database to find the most recent transaction date
    Returns a datetime object representing the latest transaction date, or
    a date 30 days in the past if no transactions are found
    """
    load_dotenv()
    
    # Get database connection parameters
    db_url = os.getenv("DATABASE_URL")
    
    # Alternative: use individual parameters if DATABASE_URL is not available
    if not db_url:
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        # Connect to the database
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Query to get the most recent transaction date
        query = "SELECT MAX(date) FROM transaction"
        cur.execute(query)
        
        # Fetch the result
        last_date = cur.fetchone()[0]
        
        # If no transactions found, return date from 30 days ago
        if not last_date:
            last_date = datetime.now() - timedelta(days=30)
            
        print(f"Last transaction date: {last_date}")
        return last_date
        
    except Exception as e:
        print(f"Error retrieving last transaction date: {e}")
        # Return a default date (30 days ago) in case of error
        return datetime.now() - timedelta(days=30)
        
    finally:
        # Close cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def get_transactions_between_dates(start_date, end_date):
    """
    Retrieves transactions from Shopify API between the specified dates
    
    Args:
        start_date: datetime object representing the starting date
        end_date: datetime object representing the ending date
        
    Returns:
        List of transaction dictionaries
    """
    load_dotenv()
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }
    
    # Format the dates for Shopify API
    formatted_start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    formatted_end_date = end_date.strftime("%Y-%m-%dT%H:%M:%S%z")
    
    # Get orders created between the specified dates
    url = f"https://{store_domain}/admin/api/{api_version}/orders.json?created_at_min={formatted_start_date}&created_at_max={formatted_end_date}&status=any"
    response = requests.get(url, headers=headers)
    
    all_transactions = []
    
    if response.status_code == 200:
        orders = response.json().get("orders", [])
        
        for order in orders:
            order_id = order.get("id")
            # Use the existing function to get detailed transactions for each order
            order_transactions = get_transactions_by_date(str(order_id))
            all_transactions.extend(order_transactions)
            
        return all_transactions
    else:
        print(f"Error retrieving orders from Shopify API: {response.status_code} - {response.text}")
        return []

def get_transactions_since_date(last_transaction_date):
    """
    Retrieves transactions from Shopify API starting from the given date
    
    Args:
        last_transaction_date: datetime object representing the starting date
        
    Returns:
        List of transaction dictionaries
    """
    # Use the new function with end_date as current datetime
    return get_transactions_between_dates(last_transaction_date, datetime.now())

def process_transactions(transactions):
    """
    Processes a list of transactions and inserts them into the database
    
    Args:
        transactions: List of transaction dictionaries
        
    Returns:
        Dictionary with results of the operation
    """
    if not transactions:
        return {
            "transactions_inserted": 0,
            "transactions_updated": 0, 
            "transactions_skipped": 0,
            "errors": []
        }
    
    load_dotenv()
    
    # Get database connection parameters
    db_url = os.getenv("DATABASE_URL")
    
    # Alternative: use individual parameters if DATABASE_URL is not available
    if not db_url:
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
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        for transaction in transactions:
            try:
                # Convert 'N/A' string values to None for numeric fields that can be NULL
                for field in ['location_id', 'product_id']:
                    if transaction.get(field) == 'N/A':
                        transaction[field] = None
                
                # For client_id, which cannot be NULL, use -1 as default
                if transaction.get('client_id') is None or transaction.get('client_id') == 'N/A':
                    transaction['client_id'] = -1
                
                # Check if transaction already exists based on unique attributes
                check_query = """
                SELECT id FROM transaction 
                WHERE date = %s AND order_id = %s AND account_type = %s 
                AND transaction_description = %s AND amount = %s
                """
                
                # Convert date string to datetime if it's a string
                transaction_date = transaction["date"]
                if isinstance(transaction_date, str):
                    transaction_date = datetime.fromisoformat(transaction_date.replace("Z", "+00:00"))
                
                check_params = (
                    transaction_date,
                    transaction["order_id"],
                    transaction["account_type"],
                    transaction["transaction_description"],
                    transaction["amount"]
                )
                
                cur.execute(check_query, check_params)
                existing_id = cur.fetchone()
                
                if existing_id:
                    # Transaction already exists, update it
                    update_query = """
                    UPDATE transaction SET
                    client_id = %s,
                    transaction_currency = %s,
                    location_id = %s,
                    source_name = %s,
                    status = %s,
                    product_id = %s,
                    updated_at_timestamp = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """
                    
                    update_params = (
                        transaction["client_id"],
                        transaction["transaction_currency"],
                        transaction.get("location_id"),
                        transaction.get("source_name"),
                        transaction.get("status"),
                        transaction.get("product_id"),
                        existing_id[0]
                    )
                    
                    cur.execute(update_query, update_params)
                    result["transactions_updated"] += 1
                    
                else:
                    # Transaction doesn't exist, insert it
                    insert_query = """
                    INSERT INTO transaction (
                        date, order_id, client_id, account_type, transaction_description,
                        amount, transaction_currency, location_id, source_name, status, product_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    insert_params = (
                        transaction_date,
                        transaction["order_id"],
                        transaction["client_id"],
                        transaction["account_type"],
                        transaction["transaction_description"],
                        transaction["amount"],
                        transaction["transaction_currency"],
                        transaction.get("location_id"),
                        transaction.get("source_name"),
                        transaction.get("status"),
                        transaction.get("product_id")
                    )
                    
                    cur.execute(insert_query, insert_params)
                    result["transactions_inserted"] += 1
                    
            except Exception as e:
                error_msg = f"Error processing transaction {transaction.get('order_id')}: {str(e)}"
                print(error_msg)
                result["errors"].append(error_msg)
                result["transactions_skipped"] += 1
                
        # Commit all changes
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Database error: {str(e)}"
        print(error_msg)
        result["errors"].append(error_msg)
        
    finally:
        # Close cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            
    return result

# Example usage
if __name__ == '__main__':
    ORDER_ID = "5762850816071"  # Replace with actual order ID
    get_transactions_by_date(ORDER_ID)