# api/process_daily_data.py
"""Serverless function that processes daily order data from Shopify API."""
import os
import json
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from api.lib.utils import get_dates
from api.lib.shopify_api import get_daily_orders
from api.lib.order_processor import process_orders
from api.lib.process_transactions import get_transactions_between_dates, process_transactions
from api.lib.process_payout import recuperer_et_enregistrer_versements_jour
from api.lib.product_processor import update_products_incremental
from api.lib.location_processor import update_locations_incremental
from api.lib.process_draft_orders import get_drafts_between_dates, process_draft_orders, process_draft_orders_delete_queue
from api.lib.process_inventory_sync import sync_inventory_full, process_inventory_queue
from api.lib.process_customer import sync_customers_since_date
# Force dynamic execution to prevent caching
dynamic = 'force-dynamic' #noqa

def process_daily_data(start_date, end_date):
    """
    Process daily order data and transactions for a given date range
    
    Args:
        start_date (str): Start date in ISO format
        end_date (str): End date in ISO format
        
    Returns:
        dict: Response data with processing results
    """
    response_data = {
        "success": False,
        "message": "Une erreur s'est produite",
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        start_datetime = datetime.fromisoformat(start_date)
        end_datetime = datetime.fromisoformat(end_date)
        
        # 1. Update locations incrementally (check for new locations)
        print("🏢 Mise à jour incrémentale des locations...")
        locations_result = update_locations_incremental()
        print(f"📍 Locations: {locations_result.get('message', 'Mis à jour')}")

        # 2. Get API data for the period
        print(f"📅 Traitement des orders pour la période {start_date} à {end_date}")
        orders = get_daily_orders(start_date, end_date)
        if not orders:
            print("ℹ️ Aucune commande pour cette période, poursuite des autres syncs...")
            result = {"orders_inserted": 0, "orders_updated": 0, "orders_skipped": 0, "errors": [], "orders_id_to_skip": []}
        else:
            result = process_orders(orders)

        # 3. Process transactions for the specified date range
        print("💰 Traitement des transactions...")
        transactions = get_transactions_between_dates(start_datetime, end_datetime, result.get("orders_id_to_skip", []))
        result_transactions = process_transactions(transactions)
        print(f"📊 Transactions: {result_transactions.get('inserted', 0)} insérées, {result_transactions.get('updated', 0)} mises à jour, {result_transactions.get('skipped', 0)} ignorées")

        # 4. Recuperer et enregistrer les versements pour la date
        day_date = start_date[:10]
        recuperer_et_enregistrer_versements_jour(day_date)
        response_data["success"] = True

        # 5a. Traiter la queue de webhooks inventory (pending → inventory table)
        print("📬 Traitement de la queue inventory_snapshot_queue...")
        try:
            queue_result = process_inventory_queue()
            print(f"📬 Queue: {queue_result['inserted']} insérés, {queue_result['updated']} mis à jour, {queue_result['failed']} échoués sur {queue_result['total_pending']} pending")
        except Exception as e:
            print(f"⚠️ Erreur lors du traitement de la queue inventory: {str(e)}")
            queue_result = {"inserted": 0, "updated": 0, "failed": 0, "total_pending": 0, "errors": [str(e)]}

        # 5b. Full sync hebdomadaire (dimanche 2h) comme filet de sécurité
        #     Rattrape les items manqués par les webhooks (variant_id, product_id, sku, etc.)
        inventory_result = None
        now = datetime.now()
        if now.weekday() == 6 and now.hour == 2:
            print("📦 Synchronisation COMPLÈTE hebdomadaire de l'inventaire...")
            try:
                inventory_result = sync_inventory_full()
                print(f"🏪 Full sync: {inventory_result['stats']['inserted']} insérés, {inventory_result['stats']['updated']} mis à jour, {inventory_result['stats']['skipped']} ignorés")
            except Exception as e:
                print(f"⚠️ Erreur lors du full sync inventaire: {str(e)}")
                inventory_result = {"success": False, "error": str(e), "records_processed": 0, "stats": {"inserted": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}}
        else:
            print("📦 Inventaire: pas de full sync (seulement dimanche 2h). Queue traitée.")

        # 6. Process draft orders between the dates
        print("📝 Traitement des draft orders...")
        try:
            draft_transactions = get_drafts_between_dates(start_date, end_date)
            draft_result = process_draft_orders(draft_transactions)
            print(f"📋 Draft orders: {draft_result.get('transactions_inserted', 0)} insérées, {draft_result.get('transactions_updated', 0)} mises à jour, {draft_result.get('transactions_skipped', 0)} ignorées")
        except Exception as e:
            print(f"⚠️ Erreur lors du traitement des draft orders: {str(e)}")
            draft_result = {"transactions_inserted": 0, "transactions_updated": 0, "transactions_skipped": 0, "errors": [str(e)]}

        # 6.1 Process draft orders delete queue
        print("🗑️ Traitement de la queue draft_orders_delete_queue...")
        try:
            delete_queue_result = process_draft_orders_delete_queue()
            print(f"🗑️ Queue delete: {delete_queue_result.get('deleted', 0)} draft orders marqués deleted, {delete_queue_result.get('failed', 0)} échecs sur {delete_queue_result.get('total_pending', 0)} pending")
        except Exception as e:
            print(f"⚠️ Erreur lors du traitement de la queue delete: {str(e)}")
            delete_queue_result = {"deleted": 0, "failed": 0, "total_pending": 0, "errors": [str(e)]}

        # 6.2 Process customers between the dates
        print("📝 Traitement des customers...")
        try:
            result_customers = sync_customers_since_date(end_datetime)
            print(f"📋 Customers: {result_customers.get('customers_inserted', 0)} insérées, {result_customers.get('customers_updated', 0)} mises à jour, {result_customers.get('customers_skipped', 0)} ignorées")
        except Exception as e:
            print(f"⚠️ Erreur lors du traitement des customers: {str(e)}")
            result_customers = {"customers_inserted": 0, "customers_updated": 0, "customers_skipped": 0, "errors": [str(e)]}

        # 7. Update products incrementally (nouveaux + modifiés, avec et sans COGS)
        print("🛍️ Mise à jour incrémentale des produits...")
        products_result = update_products_incremental()
        print(f"📦 Produits: {products_result.get('message', 'Mis à jour')}")

        # 8. Prepare response based on results
        if result.get("errors") and len(result.get("errors", [])) > 0 or result_transactions.get("errors") and len(result_transactions.get("errors", [])) > 0 or draft_result.get("errors") and len(draft_result.get("errors", [])) > 0:
            # Il y a eu des erreurs, mais nous avons quand même des statistiques
            response_data["success"] = False
            response_data["message"] = f"{len(orders)} commandes traitées avec des erreurs"
            response_data["error"] = result.get("errors")[0] if result.get("errors") else (result_transactions.get("errors")[0] if result_transactions.get("errors") else draft_result.get("errors")[0])
        else:
            response_data["success"] = True
            response_data["message"] = f"{len(orders)} commandes traitées avec succès"

        response_data.update({
            "details": f"Commandes insérées: {result.get('orders_inserted', 0)}, mises à jour: {result.get('orders_updated', 0)}, ignorées: {result.get('orders_skipped', 0)}",
            "timestamp": datetime.now().isoformat(),
            "analyzed_period": f"From {start_date} to {end_date}",
            "transactions_processed": f"{len(transactions)} transactions traitées avec succès",
            "products_synchronized": products_result.get('details', {}).get('inserted', 0),
            "locations_synchronized": locations_result.get('stats', {}).get('inserted', 0),
            "inventory_queue_processed": f"Queue: {queue_result['inserted']} insérés, {queue_result['updated']} mis à jour, {queue_result['failed']} échoués sur {queue_result['total_pending']} pending",
            "inventory_synchronized": f"Full sync: {inventory_result['stats']['inserted']} insérés, {inventory_result['stats']['updated']} mis à jour" if inventory_result else "Pas de full sync (seulement dimanche 2h)",
            "draft_orders_processed": f"Draft orders: {draft_result.get('transactions_inserted', 0)} insérées, {draft_result.get('transactions_updated', 0)} mises à jour, {draft_result.get('transactions_skipped', 0)} ignorées",
            "draft_orders_delete_queue_processed": f"Queue delete: {delete_queue_result.get('deleted', 0)} marqués deleted, {delete_queue_result.get('failed', 0)} échecs sur {delete_queue_result.get('total_pending', 0)} pending",
            "customers_synchronized": f"Customers: {result_customers.get('customers_inserted', 0)} insérées, {result_customers.get('customers_updated', 0)} mises à jour, {result_customers.get('customers_skipped', 0)} ignorées"
        })
        
        return response_data

    except Exception as e:
        # Capture et log de l'erreur complète
        error_details = traceback.format_exc()
        print(f"Erreur: {str(e)}\n{error_details}")
        
        response_data.update({
            "error": str(e),
            "error_details": error_details.split("\n")
        })
        
        return response_data

class handler(BaseHTTPRequestHandler):
    """
    This class handles the HTTP requests for the process_daily_data function.
    It allows for manual testing with POST - not allowed, use GET instead
    """
    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        super().end_headers()

    def do_POST(self):
        """
        For manual testing with POST - not allowed, use GET instead
        """
        self.send_response(405)
        self.end_headers()
        self.wfile.write("Method not allowed. Use GET.".encode())

    def do_GET(self):
        """
        Process daily order data from Shopify API
        """
        response_data = {
            "success": False,
            "message": "Une erreur s'est produite",
            "timestamp": datetime.now().isoformat()
        }

        try:
            # Authorization check
            auth_header = self.headers.get('Authorization', '')
            expected_auth = f"Bearer {os.environ.get('CRON_SECRET', '')}"

            if auth_header != expected_auth:
                self.send_response(401)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response_data["message"] = "Non autorisé"
                self.wfile.write(json.dumps(response_data).encode())
                return

            # Get date range and process data
            start_date, end_date = get_dates()
            response_data = process_daily_data(start_date, end_date)

            # Set appropriate response status
            if not response_data.get("success", False) and response_data.get("error"):
                self.send_response(207)  # Multi-Status
            else:
                self.send_response(200)

            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode())

        except Exception as e:
            # Capture et log de l'erreur complète
            error_details = traceback.format_exc()
            print(f"Erreur: {str(e)}\n{error_details}") 

            # Error handling - toujours répondre avec un code 500 mais avec un message JSON
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            response_data.update({
                "error": str(e),
                "error_details": error_details.split("\n")
            })

            self.wfile.write(json.dumps(response_data).encode())
