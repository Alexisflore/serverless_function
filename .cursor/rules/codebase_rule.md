# Adam Lippes — Shopify-to-PostgreSQL ETL Pipeline

## Overview

This project is a **Shopify → Supabase PostgreSQL ETL pipeline** that syncs data from the Adam Lippes Shopify stores (US + JP markets) into a Supabase PostgreSQL database. It runs as:

1. **GitHub Actions cron job** — hourly (`0 * * * *`) via `run_daily_sync.py`
2. **Vercel serverless function** — HTTP GET endpoint at `api/process_daily_data.py`

The sync window is a ~25-hour rolling window (yesterday start → now + 1h), computed by `get_dates()` in UTC+2.

---

## Project Structure

```
cron_functions/
├── run_daily_sync.py                 # GitHub Actions entry point
├── api/
│   ├── process_daily_data.py         # Main orchestrator + Vercel HTTP handler
│   └── lib/
│       ├── logging_config.py         # Centralized logging (stdout for Vercel)
│       ├── utils.py                  # Date helpers, tag parsing
│       ├── database.py               # Legacy order upsert with column type introspection
│       ├── shopify_api.py            # Shopify REST (orders) + GraphQL (metafields)
│       ├── order_processor.py        # Fetches ORDER_TYPE metafield, delegates to insert_order
│       ├── insert_order.py           # Order + order_details mapping & upsert
│       ├── process_transactions.py   # Transaction ETL (~1950 lines, largest module)
│       ├── process_payout.py         # Shopify Payments payout ETL
│       ├── product_processor.py      # Product/variant sync with COGS (GraphQL unitCost)
│       ├── location_processor.py     # Location sync
│       ├── process_draft_orders.py   # Draft order ETL + delete queue
│       ├── process_inventory_sync.py # Inventory sync (bulk ops, webhook queue, full/incremental)
│       ├── process_customer.py       # Customer sync (bulk GraphQL)
│       └── shopifyql_helpers.py      # ShopifyQL queries for inventory adjustment history
├── database_creation/                # Schema DDL & migration scripts
└── test/                             # Test scripts and sample data
```

---

## Main Orchestration Flow

`process_daily_data(start_date, end_date)` in `api/process_daily_data.py` runs 8 sequential steps:

1. **Locations** — `update_locations_incremental()` — checks for new Shopify locations
2. **Orders** — `get_daily_orders()` → `process_orders()` — REST API fetch, ORDER_TYPE metafield via GraphQL, upsert into `orders` + `orders_details`
3. **Transactions** — `get_transactions_between_dates()` → `process_transactions()` — per-order financial breakdown (sales, taxes, discounts, refunds, duties, shipping, gift cards, tips)
4. **Payouts** — `recuperer_et_enregistrer_versements_jour()` — Shopify Payments deposits for the day
5. **Inventory** — two phases:
   - **5a.** `process_inventory_queue()` — process webhook-driven `inventory_snapshot_queue`
   - **5b.** `sync_inventory_full()` — weekly full sync (Sunday 2am only) as safety net
6. **Draft Orders** — `get_drafts_between_dates()` → `process_draft_orders()` + `process_draft_orders_delete_queue()`
7. **Customers** — `sync_customers_since_date()` — bulk GraphQL export + upsert
8. **Products** — `update_products_incremental()` — incremental sync with COGS via GraphQL `unitCost`

---

## Database Connection Pattern

All modules follow the same connection pattern:

```python
def _pg_connect():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    return psycopg2.connect(db_url)
```

- Primary: `DATABASE_URL` env var
- Fallback: constructed from `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_DB_NAME`
- Driver: **psycopg2** (direct SQL, no ORM)
- All upserts use `INSERT ... ON CONFLICT ... DO UPDATE`

---

## Key Helper Functions

### `api/lib/utils.py`
- `get_dates()` — returns `(yesterday_start_iso, today_plus_1h_iso)` in UTC+2
- `get_source_location(tags)` — extracts `location_id` from tags matching `STORE_{name}_{location_id}`

### `api/lib/insert_order.py`
- `get_db_connection()` — standard DB connection
- `safe_float(value, default=0.0)` — safe float conversion
- `get_nested_value(data, path, default)` — deep dict access using `"key1 > key2 > key3"` path syntax
- `format_discount_codes(order)` — joins discount codes into comma-separated string
- `parse_tags_to_list(tags_str)` — parses comma-separated or JSON tags into a Python list
- `extract_market_from_tags(tags_str)` — returns `"US"` or `"JP"` from tags (defaults to `"US"`)
- `is_test_order(tags_str)` — checks for `TEST_order_Shopify` tag (case-insensitive)
- `delete_test_order_data(order_id, cur)` — cascading delete of test order data (transactions → order_details → orders)
- `extract_tax_lines(tax_lines, index)` — extracts `(name, rate, value)` tuple from tax line at given index

### `api/lib/logging_config.py`
- `get_logger(name)` — returns a logger writing to stdout (required for Vercel)
- All modules use: `from .logging_config import get_logger; logger = get_logger('module_name')`

### `api/lib/process_transactions.py`
- `db_retry(max_retries=3, backoff_base=2)` — decorator for exponential backoff on `OperationalError`
- `_shopify_headers()` — returns Shopify API headers
- `_pg_connect()` — PostgreSQL connection

---

## Data Processing Logic

### Order Processing (`insert_order.py`)
1. Receives Shopify order JSON (`{"orders": [...]}`)
2. Filters out test orders (tag `TEST_order_Shopify`) and deletes their existing DB data
3. Maps Shopify fields to DB columns (see `order_mapped` dict)
4. Extracts `source_location` from `STORE_*` tags
5. Extracts `market` (`US`/`JP`) from tags
6. Fetches `ORDER_TYPE` metafield via GraphQL (injected as `_metafield_order_type`)
7. Computes derived fields:
   - `returns_excl_taxes = returns / (1 + sum_of_tax_rates)`
   - `net_sales = current_total_price - shipping - taxes`
   - `net_sales_check = (current_subtotal_price ≈ net_sales)` (tolerance: 0.01)
   - `tax_check = (sum_of_individual_taxes ≈ current_total_tax)` (tolerance: 0.01)
8. For each `line_item`, computes:
   - `amount_gross_sales = price × quantity`
   - `amount_returns = (origin_quantity - current_quantity) × price`
   - `amount_discounts = (pre_tax_price - price) × current_quantity`
   - `amount_net_sales = current_quantity × pre_tax_price`
   - `amount_net_sales_check` and `return_check` booleans
9. Upserts into `orders` then `orders_details` with `ON CONFLICT DO UPDATE`

### Transaction Processing (`process_transactions.py`)
- Largest module (~1950 lines)
- Per-order extraction of financial components:
  - **Sales** (fulfilled + unfulfilled line items)
  - **Discounts** (per line item and order-level)
  - **Taxes** (per line item)
  - **Financial transactions** (capture/sale/refund with payment method)
  - **Refund line items** (returned quantities + restocking)
  - **Order adjustments** (manual adjustments)
  - **Duties** (international orders)
  - **Shipping** (shipping charges + refunds)
  - **Gift cards** (applied as payment)
  - **Tips** (customer tips)
- Multi-currency support with exchange rate handling
- COGS lookup from `products` table for each line item
- `account_type` field categorizes each row: Sales, Taxes, Discounts, Returns, Refunds, Payments, Shipping, Duties, Gift Cards, Tips, Order Adjustment
- Uses `db_retry` decorator for resilient DB operations

### Inventory Sync (`process_inventory_sync.py`)
- **Webhook queue** (`inventory_snapshot_queue`): processes real-time inventory changes pushed by Shopify webhooks
- **Full sync** (`sync_inventory_full()`): bulk GraphQL to fetch all inventory levels across all locations — runs as weekly safety net (Sunday 2am)
- **ShopifyQL history** (`shopifyql_helpers.py`): queries `inventory_adjustment_history` to compute absolute stock values from deltas, populates `inventory_history`
- Quantity names tracked: `available`, `committed`, `damaged`, `incoming`, `on_hand`, `quality_control`, `reserved`, `safety_stock`

### Payout Processing (`process_payout.py`)
- Fetches Shopify Payments payouts for a given day
- Enriches each transaction with `payment_method_name`
- Upserts into `payout` + `payout_transaction` tables

---

## Database Schema (Key Tables)

| Table | PK | Description |
|---|---|---|
| `orders` | `_id_order TEXT` | Orders with financial summaries, billing/shipping, market (US/JP), up to 5 tax lines |
| `orders_details` | `_id_order_detail BIGINT` | Line items per order, with computed gross/net/returns/discounts |
| `transaction` | `id SERIAL` | Granular financial rows per order (one per account_type per item) |
| `draft_order` | `id SERIAL` | Draft order line items with taxes/shipping |
| `draft_orders_delete_queue` | `id` | Webhook queue for draft order deletions |
| `payout` | `id BIGINT` | Shopify Payments deposits |
| `payout_transaction` | `id BIGINT` | Individual transactions within a payout |
| `products` | `variant_id` | Product variants with COGS, color, size, images |
| `locations` | `_location_id BIGINT` | Shopify store locations |
| `inventory` | `(inventory_item_id, location_id)` | Current inventory levels per item per location |
| `inventory_history` | `id BIGSERIAL` | Time-series of inventory changes (triggers + ShopifyQL adjustments) |
| `inventory_snapshot_queue` | `id` | Webhook queue for real-time inventory snapshots |
| `customers` | `id SERIAL` (unique: `customer_id`) | Customer data from bulk GraphQL |

### Important: `inventory_history` has DB triggers
- `log_inventory_insert`, `log_inventory_update`, `log_inventory_delete` — auto-log changes to `inventory_history`
- Views: `inventory_changes_with_diff`, `inventory_snapshot_latest`, `inventory_stock_movements`
- Functions: `get_inventory_at_date()`, `get_item_history()`

---

## Environment Variables

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Full PostgreSQL connection URL (primary) |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_USER` / `PASSWORD` / `HOST` / `PORT` / `DB_NAME` | Fallback DB connection parts |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key |
| `CRON_SECRET` | Bearer token for HTTP endpoint auth |
| `SHOPIFY_ACCESS_TOKEN` | Shopify Admin API token (US store) |
| `SHOPIFY_STORE_DOMAIN` | `adam-lippes.myshopify.com` |
| `SHOPIFY_API_VERSION` | Target API version (some modules override) |
| `SHOPIFY_STORE_DOMAIN_JP` | Japan store domain |
| `SHOPIFY_ACCESS_TOKEN_JP` | Japan store API token |

---

## Shopify API Versions

Versions are **not unified** across the codebase:
- `.env` / `shopify_api.py` / `shopifyql_helpers.py`: `2026-01`
- `process_transactions.py` / `process_payout.py`: hardcoded `2024-10`
- `process_inventory_sync.py`: `2025-01` (GraphQL), `2025-10` (REST)
- `product_processor.py` / `process_customer.py`: from env with fallback `2024-10` / `2025-01`

---

## Conventions & Patterns

- **Language**: Code comments and log messages are in **French**, code identifiers in English
- **DB driver**: `psycopg2` with raw SQL everywhere (no ORM)
- **Upsert pattern**: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`
- **Logging**: Always use `from .logging_config import get_logger` — stdout-based for Vercel compatibility
- **Error handling**: Each order/item/transaction is processed in its own try/catch, errors are collected in a `stats["errors"]` list, processing continues
- **Retry**: `db_retry` decorator with exponential backoff on `OperationalError`
- **Shopify pagination**: REST API uses `Link` header for cursor-based pagination
- **GraphQL**: Used for metafields (ORDER_TYPE), bulk operations (customers, products/COGS), ShopifyQL (inventory history)
- **Test orders**: Filtered by tag `TEST_order_Shopify` — existing data is cascading-deleted
- **Market detection**: Tags `US` or `JP` on orders; defaults to `"US"`
- **Source location**: Extracted from tags matching `STORE_{name}_{location_id}`

---

## Dependencies (Pipfile)

- Python 3.10
- `psycopg2-binary` — PostgreSQL driver
- `supabase` — Supabase client (used only for `get_supabase_client()` in `database.py`)
- `python-dotenv` — .env file loading
- `requests` — HTTP client for Shopify APIs
- `sqlalchemy` — used only in payout table creation scripts
- `vercel` — Vercel deployment

---

## Deployment

- **Vercel**: `vercel.json` routes `api/*.py` as Python serverless functions
- **GitHub Actions**: `.github/workflows/process-daily-data.yml` runs `pipenv run python run_daily_sync.py` every hour
- **Auth**: HTTP endpoint requires `Authorization: Bearer {CRON_SECRET}`
