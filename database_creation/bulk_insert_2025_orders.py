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
    start_date = datetime(year ,3, 10)
    end_date = datetime(year + 1, 11, 23)

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
        sys.exit(1)

    print(f"Starting bulk import of orders for year {year}, processing day by day")

    # Process each day of the year
    day_count = 0
    for start_date, end_date in get_dates_for_year(year):
        day_count += 1
        print(f"\nProcessing day {day_count}/365: {start_date[:10]}...")

        # Process the day's orders using the new process_daily_data function
        day_result = process_daily_data(start_date, end_date)

if __name__ == "__main__":
    main()
