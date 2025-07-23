#!/usr/bin/env python
# test/bulk_insert_2025_orders.py

import os
import sys
import json
from datetime import datetime, timedelta
import traceback
from dotenv import load_dotenv

# Add parent directory to path to allow importing modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Import the modules after adding the parent directory to the path
from api.lib.shopify_api import get_daily_orders
from api.lib.order_processor import process_orders
from api.process_daily_data import process_daily_data
# Load environment variables from .env file
load_dotenv()

def get_dates_for_year(year=2024):
    """
    Generator function that yields date pairs for each day of the specified year
    Each pair consists of (start_date, end_date) for a single day
    """
    start_date = datetime(year +1,7, 9)
    end_date = datetime(year + 1, 7, 20)

    current = start_date
    while current < end_date:
        # Start of day
        day_start = current.strftime("%Y-%m-%dT00:00:00")

        # End of day (next day at 00:00)
        next_day = current + timedelta(days=1)
        day_end = next_day.strftime("%Y-%m-%dT00:00:00")

        yield (day_start, day_end)

        # Move to next day
        current = next_day

def main():
    """Main function to process all orders for 2025 day by day"""
    year = 2024

    # Check if required environment variables are set
    if not os.environ.get("SHOPIFY_STORE_DOMAIN") or not os.environ.get("SHOPIFY_ACCESS_TOKEN") or not os.environ.get("SHOPIFY_API_VERSION"):
        print("ERROR: Missing required environment variables.")
        print("Make sure SHOPIFY_STORE_DOMAIN, SHOPIFY_ACCESS_TOKEN, and SHOPIFY_API_VERSION are set in your .env file")
        sys.exit(1)

    # Display environment info
    print("Environment initialized successfully")
    print(f"Shopify Domain: {os.environ.get('SHOPIFY_STORE_DOMAIN')}")
    print(f"Shopify API Version: {os.environ.get('SHOPIFY_API_VERSION')}")

    # Statistics for the entire process
    overall_stats = {
        "total_days": 365,  # or 366 for leap years
        "days_processed": 0,
        "days_with_orders": 0,
        "days_with_errors": 0,
        "total_orders_processed": 0,
        "total_orders_inserted": 0,
        "total_orders_updated": 0,
        "total_orders_skipped": 0,
        "total_transactions_processed": 0,
        "details_by_day": []
    }

    print(f"Starting bulk import of orders for year {year}, processing day by day")
    start_time = datetime.now()

    # Process each day of the year
    day_count = 0
    for start_date, end_date in get_dates_for_year(year):
        day_count += 1
        print(f"\nProcessing day {day_count}/365: {start_date[:10]}...")

        # Process the day's orders using the new process_daily_data function
        day_result = process_daily_data(start_date, end_date)
        # overall_stats["details_by_day"].append(day_result)

        # # Update overall statistics
        # overall_stats["days_processed"] += 1

        # # Check if there were orders
        # if "Aucune commande" not in day_result.get("message", ""):
        #     overall_stats["days_with_orders"] += 1
            
        #     # Extract orders processed from the message
        #     try:
        #         orders_count = int(day_result.get("message", "0").split()[0])
        #         overall_stats["total_orders_processed"] += orders_count
        #     except (ValueError, IndexError):
        #         pass

        #     # Extract order details if available
        #     if "details" in day_result:
        #         # Try to parse the details string to extract numbers
        #         details = day_result["details"]
        #         try:
        #             inserted = int(details.split("insérées: ")[1].split(",")[0])
        #             updated = int(details.split("mises à jour: ")[1].split(",")[0])
        #             skipped = int(details.split("ignorées: ")[1].split(",")[0])

        #             overall_stats["total_orders_inserted"] += inserted
        #             overall_stats["total_orders_updated"] += updated
        #             overall_stats["total_orders_skipped"] += skipped
        #         except (IndexError, ValueError):
        #             # If we can't parse the details, just continue
        #             pass
                    
        #     # Extract transaction counts if available
        #     if "transactions_processed" in day_result:
        #         try:
        #             trans_count = int(day_result["transactions_processed"].split()[0])
        #             overall_stats["total_transactions_processed"] += trans_count
        #         except (ValueError, IndexError):
        #             pass

        # if not day_result.get("success", False):
        #     overall_stats["days_with_errors"] += 1

    # Calculate duration
    end_time = datetime.now()
    # duration = end_time - start_time
    # overall_stats["duration_seconds"] = duration.total_seconds()
    # overall_stats["duration_formatted"] = str(duration)

    # Print summary
    # print("\n" + "="*50)
    # print(f"IMPORT SUMMARY FOR YEAR {year}")
    # print("="*50)
    # print(f"Total days processed: {overall_stats['days_processed']}/365")
    # print(f"Days with orders: {overall_stats['days_with_orders']}")
    # print(f"Days with errors: {overall_stats['days_with_errors']}")
    # print(f"Total orders processed: {overall_stats['total_orders_processed']}")
    # print(f"Orders inserted: {overall_stats['total_orders_inserted']}")
    # print(f"Orders updated: {overall_stats['total_orders_updated']}")
    # print(f"Orders skipped: {overall_stats['total_orders_skipped']}")
    # print(f"Total transactions processed: {overall_stats['total_transactions_processed']}")
    # print(f"Total duration: {overall_stats['duration_formatted']}")

    # # Save results to file
    # timestamp = start_time.strftime('%Y%m%d_%H%M%S')
    # results_file = f"test_results_{year}_daily_orders_import_{timestamp}.json"
    # with open(results_file, 'w') as f:
    #     json.dump(overall_stats, f, indent=2)

    # print(f"\nDetailed results saved to {results_file}")

if __name__ == "__main__":
    main()
