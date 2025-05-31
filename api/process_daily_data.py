# api/process_daily_data.py
"""Serverless function that processes daily order data from Shopify API."""
import os
import json
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from api.lib.date_utils import get_dates
from api.lib.shopify_api import get_daily_orders
from api.lib.order_processor import process_orders
from api.lib.process_transactions import get_transactions_between_dates, process_transactions
from api.lib.process_payout import recuperer_et_enregistrer_versements_jour
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
        # 1. Get API data for the period
        orders = get_daily_orders(start_date, end_date)

        if not orders:
            response_data["success"] = True
            response_data["message"] = "Aucune commande à traiter pour cette période"
            response_data["analyzed_period"] = f"From {start_date} to {end_date}"
            return response_data

        # 2. Process data and insert into database directly
        result = process_orders(orders)

        # 3. Convert string dates to datetime objects for processing transactions
        start_datetime = datetime.fromisoformat(start_date)
        end_datetime = datetime.fromisoformat(end_date)

        # 4. Process transactions for the specified date range
        transactions = get_transactions_between_dates(start_datetime, end_datetime)

        # 5. Process transactions
        result_transactions = process_transactions(transactions)

        day_date = start_date[:10]
        recuperer_et_enregistrer_versements_jour(day_date)

        # 6. Prepare response based on results
        if result.get("errors") and len(result.get("errors", [])) > 0 or result_transactions.get("errors") and len(result_transactions.get("errors", [])) > 0:
            # Il y a eu des erreurs, mais nous avons quand même des statistiques
            response_data["success"] = False
            response_data["message"] = f"{len(orders)} commandes traitées avec des erreurs"
            response_data["error"] = result.get("errors")[0] if result.get("errors") else "Erreurs lors du traitement"
        else:
            response_data["success"] = True
            response_data["message"] = f"{len(orders)} commandes traitées avec succès"

        response_data.update({
            "details": f"Commandes insérées: {result.get('orders_inserted', 0)}, mises à jour: {result.get('orders_updated', 0)}, ignorées: {result.get('orders_skipped', 0)}",
            "timestamp": datetime.now().isoformat(),
            "analyzed_period": f"From {start_date} to {end_date}",
            "transactions_processed": f"{len(transactions)} transactions traitées avec succès"
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
