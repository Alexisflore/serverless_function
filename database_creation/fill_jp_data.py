#!/usr/bin/env python
"""
Initial full sync of Japan Shopify store data into the shared database.

Usage:
    SHOPIFY_ACCESS_TOKEN=$SHOPIFY_ACCESS_TOKEN_JP \
    SHOPIFY_STORE_DOMAIN=$SHOPIFY_STORE_DOMAIN_JP \
    COMMERCIAL_ORGANISATION=JP \
    pipenv run python database_creation/fill_jp_data.py
"""

import os
import sys
import json
from datetime import datetime, timedelta
import traceback
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

from api.lib.shopify_api import get_daily_orders
from api.lib.order_processor import process_orders
from api.lib.process_transactions import get_transactions_between_dates, process_transactions
from api.lib.process_payout import recuperer_et_enregistrer_versements_jour
from api.lib.process_draft_orders import get_drafts_between_dates, process_draft_orders
from api.lib.process_customer import sync_customers_since_date
from api.lib.process_inventory_sync import sync_inventory_full
from api.lib.product_processor import get_shopify_products_since, insert_products_to_db
from api.lib.location_processor import get_new_shopify_locations, insert_locations_to_db


def get_date_range(start_year=2024, start_month=1, start_day=1):
    """Yields (day_start, day_end) pairs from the start date to today."""
    current = datetime(start_year, start_month, start_day)
    end = datetime.now()

    while current < end:
        day_start = current.strftime("%Y-%m-%dT00:00:00")
        next_day = current + timedelta(days=1)
        day_end = next_day.strftime("%Y-%m-%dT00:00:00")
        yield (day_start, day_end)
        current = next_day


def phase1_full_syncs():
    """One-shot full syncs for data types that don't need day-by-day iteration."""
    print("\n" + "=" * 70)
    print("PHASE 1 — Full syncs (locations, products, inventory, customers)")
    print("=" * 70)

    # 1. Locations
    print("\n--- 1/4 Locations ---")
    try:
        locations = get_new_shopify_locations(None)
        if locations:
            stats = insert_locations_to_db(locations)
            print(f"Locations: {stats}")
        else:
            print("Aucune location trouvee.")
    except Exception as e:
        print(f"ERREUR locations: {e}")
        traceback.print_exc()

    # 2. Products (full, no date filter)
    print("\n--- 2/4 Products ---")
    try:
        shopify_data = get_shopify_products_since(None)
        variants = shopify_data.get("variants", [])
        if variants:
            stats = insert_products_to_db(variants)
            print(f"Products: {stats}")
        else:
            print("Aucun produit trouve.")
    except Exception as e:
        print(f"ERREUR products: {e}")
        traceback.print_exc()

    # 3. Inventory (full bulk, no date filter)
    print("\n--- 3/4 Inventory ---")
    try:
        result = sync_inventory_full()
        print(f"Inventory: {json.dumps(result, indent=2, default=str)}")
    except Exception as e:
        print(f"ERREUR inventory: {e}")
        traceback.print_exc()

    # 4. Customers (all since 2020)
    print("\n--- 4/4 Customers ---")
    try:
        result = sync_customers_since_date(datetime(2020, 1, 1))
        print(f"Customers: {json.dumps(result, indent=2, default=str)}")
    except Exception as e:
        print(f"ERREUR customers: {e}")
        traceback.print_exc()


def phase2_day_by_day():
    """Day-by-day iteration for orders, transactions, payouts, draft orders."""
    print("\n" + "=" * 70)
    print("PHASE 2 — Day-by-day (orders, transactions, payouts, draft orders)")
    print("=" * 70)

    day_count = 0
    total_orders = 0
    total_transactions = 0
    total_drafts = 0

    for day_start, day_end in get_date_range(2024, 1, 1):
        day_count += 1
        day_date = day_start[:10]

        # Orders
        try:
            orders = get_daily_orders(day_start, day_end)
        except Exception as e:
            print(f"  [{day_date}] ERREUR get_daily_orders: {e}")
            orders = []

        if orders:
            print(f"\n[Day {day_count}] {day_date} — {len(orders)} orders")
            total_orders += len(orders)

            try:
                result = process_orders(orders)
                orders_id_to_skip = result.get("orders_id_to_skip", [])
            except Exception as e:
                print(f"  ERREUR process_orders: {e}")
                orders_id_to_skip = []

            # Transactions
            try:
                start_dt = datetime.fromisoformat(day_start)
                end_dt = datetime.fromisoformat(day_end)
                txs = get_transactions_between_dates(start_dt, end_dt, orders_id_to_skip)
                if txs:
                    tx_result = process_transactions(txs)
                    total_transactions += len(txs)
                    print(f"  Transactions: {tx_result.get('inserted', 0)} inserted, {tx_result.get('updated', 0)} updated")
            except Exception as e:
                print(f"  ERREUR transactions: {e}")

            # Payouts
            try:
                recuperer_et_enregistrer_versements_jour(day_date)
            except Exception as e:
                print(f"  ERREUR payouts: {e}")
        else:
            if day_count % 30 == 0:
                print(f"  [{day_date}] ... no orders (skipping)")

        # Draft orders (check every day regardless of orders)
        try:
            drafts = get_drafts_between_dates(day_start, day_end)
            if drafts:
                draft_result = process_draft_orders(drafts)
                total_drafts += len(drafts)
                print(f"  [{day_date}] Draft orders: {draft_result.get('transactions_inserted', 0)} inserted")
        except Exception as e:
            print(f"  [{day_date}] ERREUR draft_orders: {e}")

    print(f"\nPhase 2 terminee: {day_count} jours, {total_orders} orders, {total_transactions} transactions, {total_drafts} draft orders")


def main():
    store = os.environ.get("SHOPIFY_STORE_DOMAIN", "???")
    org = os.environ.get("COMMERCIAL_ORGANISATION", "???")

    if not os.environ.get("SHOPIFY_ACCESS_TOKEN") or not os.environ.get("SHOPIFY_STORE_DOMAIN"):
        print("ERROR: Missing SHOPIFY_ACCESS_TOKEN or SHOPIFY_STORE_DOMAIN.")
        sys.exit(1)

    print("=" * 70)
    print(f"FILL JP DATA — store={store}  org={org}")
    print(f"Started at {datetime.now().isoformat()}")
    print("=" * 70)

    phase1_full_syncs()
    phase2_day_by_day()

    print("\n" + "=" * 70)
    print(f"DONE — {datetime.now().isoformat()}")
    print("=" * 70)


if __name__ == "__main__":
    main()
