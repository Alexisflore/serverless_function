#!/usr/bin/env python3
"""
Shopify → PostgreSQL ETL
Inclus :
  • gestion multi-transactions (split / multi-capture / Shop Pay Installments)
  • nouvelle colonne `payment_method_name`
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time
import requests
import psycopg2
from dotenv import load_dotenv



# ---------------------------------------------------------------------------
# 1. Utilitaires de base
# ---------------------------------------------------------------------------

def _shopify_headers() -> Dict[str, str]:
    load_dotenv()
    return {
        "X-Shopify-Access-Token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
        "Content-Type": "application/json",
    }


def _pg_connect():
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
# 2. Extraction fine des remboursements (inchangé + payment_method_name)
# ---------------------------------------------------------------------------

def get_refund_details(
    order_id: str,
    refund_id: str,
    client_id: str,
    source_name: str,
    payment_method_name: str | None,
) -> List[Dict[str, Any]]:
    """
    Retourne une liste d'items détaillés liés au remboursement.
    """
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    url_refund = (
        f"https://{store_domain}/admin/api/{api_version}/orders/"
        f"{order_id}/refunds/{refund_id}.json"
    )

    resp = requests.get(url_refund, headers=_shopify_headers())
    if resp.status_code != 200:
        print(f"[Refund] {resp.status_code}: {resp.text}")
        return []

    refund = resp.json().get("refund", {})
    refund_date = refund.get("created_at")
    refund_status = refund.get("status")
    location_id = refund.get("location_id")

    items: List[Dict[str, Any]] = []

    for refund_item in refund.get("refund_line_items", []):
        li = refund_item.get("line_item", {})
        product_id = li.get("product_id")
        subtotal = float(refund_item.get("subtotal", 0))
        currency = refund_item.get("subtotal_set", {}).get("shop_money", {}).get(
            "currency_code", "USD"
        )

        # 2.1 ligne article remboursée
        items.append(
            {
                "date": refund_date,
                "order_id": order_id,
                "client_id": client_id,
                "type": "refund_line_item",
                "account_type": "Refunds",
                "transaction_description": f"Refund: {li.get('name')}",
                "amount": -subtotal,
                "transaction_currency": currency,
                "location_id": location_id,
                "source_name": source_name,
                "status": refund_status,
                "product_id": product_id,
                "payment_method_name": payment_method_name,
            }
        )

        # 2.2 taxes
        for tax in li.get("tax_lines", []):
            items.append(
                {
                    "date": refund_date,
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "refund_tax",
                    "account_type": "Taxes",
                    "transaction_description": tax.get("title"),
                    "amount": -float(tax.get("price", 0)),
                    "transaction_currency": tax.get("price_set", {})
                    .get("shop_money", {})
                    .get("currency_code", currency),
                    "location_id": location_id,
                    "source_name": source_name,
                    "status": refund_status,
                    "product_id": product_id,
                    "payment_method_name": payment_method_name,
                }
            )
    return items


# ---------------------------------------------------------------------------
# 3. Extraction d'une commande (lignes + taxes + transactions)
# ---------------------------------------------------------------------------

def get_transactions_by_order(order_id: str) -> List[Dict[str, Any]]:
    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"

    # 3.1 Charge l'ordre complet
    attempts = 0
    while attempts < 3:
        attempts += 1
        try:
            order_url = f"https://{store_domain}/admin/api/{api_version}/orders/{order_id}.json"
            order_resp = requests.get(order_url, headers=_shopify_headers())
            order_resp.raise_for_status()
            
            # Vérification pour éviter l'erreur 'NoneType' object has no attribute 'get'
            order_data = order_resp.json()
            if order_data is None:
                print(f"Réponse JSON vide pour l'ordre {order_id}")
                return []
            
            order = order_data.get("order", {})
            # Sécurisation de l'accès au client_id
            customer = order.get("customer")
            client_id = customer.get("id", -1) if customer is not None else -1
            source_name = order.get("source_name")
            fulfillments = order.get("fulfillments", [])
            refunds = order.get("refunds", [])
        except Exception as e:
            print(f"Error getting order: {e}")
            if attempts == 3:
                return []
            time.sleep(1)

    # 3.2 Charge toutes les transactions financières (split tender, etc.)
    tx_url = (
        f"https://{store_domain}/admin/api/{api_version}/orders/"
        f"{order_id}/transactions.json"
    )
    tx_resp = requests.get(tx_url, headers=_shopify_headers())
    tx_list = tx_resp.json().get("transactions", []) if tx_resp.ok else []

    # Mappe la ❶ère transaction « success » pour enrichir les lignes
    payment_method_name = None
    for t in tx_list:
        if t.get("status") == "success" and t.get("payment_details"):
            payment_method_name = (
                t["payment_details"].get("payment_method_name")
                or t.get("gateway")
            )
            break

    transactions: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    # 3.a  Lignes d'articles (HT, remises, taxes)
    # ------------------------------------------------------------------ #
    for f in fulfillments:
        location_id = f.get("location_id")
        status = f.get("status")
        created_at = f.get("created_at")

        for li in f.get("line_items", []):
            product_id = li.get("product_id")
            gross_price = float(li.get("price", 0))
            currency = li.get("price_set", {}).get("shop_money", {}).get(
                "currency_code", "USD"
            )

            #  – vente brute HT
            transactions.append(
                {
                    "date": created_at,
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "sales_gross",
                    "account_type": "Sales",
                    "transaction_description": f"{li.get('name')} Gross HT",
                    "amount": gross_price,
                    "transaction_currency": currency,
                    "location_id": location_id,
                    "source_name": source_name,
                    "status": status,
                    "product_id": product_id,
                    "payment_method_name": payment_method_name,
                }
            )

            #  – remises éventuelles
            for d in li.get("discount_allocations", []):
                discount_amount = float(d.get("amount", 0))
                disc_currency = d.get("amount_set", {}).get("shop_money", {}).get(
                    "currency_code", currency
                )
                transactions.append(
                    {
                        "date": created_at,
                        "order_id": order_id,
                        "client_id": client_id,
                        "type": "discount_line",
                        "account_type": "Discounts",
                        "transaction_description": f"Discount for {li.get('name')}",
                        "amount": -discount_amount,
                        "transaction_currency": disc_currency,
                        "location_id": location_id,
                        "source_name": source_name,
                        "status": status,
                        "product_id": product_id,
                        "payment_method_name": payment_method_name,
                    }
                )

            #  – taxes
            for tax in li.get("tax_lines", []):
                tax_amount = float(tax.get("price", 0))
                tax_currency = tax.get("price_set", {}).get("shop_money", {}).get(
                    "currency_code", currency
                )
                transactions.append(
                    {
                        "date": created_at,
                        "order_id": order_id,
                        "client_id": client_id,
                        "type": "tax_line",
                        "account_type": "Taxes",
                        "transaction_description": tax.get("title"),
                        "amount": tax_amount,
                        "transaction_currency": tax_currency,
                        "location_id": location_id,
                        "source_name": source_name,
                        "status": status,
                        "product_id": product_id,
                        "payment_method_name": payment_method_name,
                    }
                )

    # ------------------------------------------------------------------ #
    # 3.b  Transactions financières (split-tender, Shop Pay, remboursements)
    # ------------------------------------------------------------------ #
    for t in tx_list:
        transactions.append(
            {
                "date": t.get("created_at"),
                "order_id": order_id,
                "client_id": client_id,
                "type": t.get("kind"),                 # authorization, capture, sale, refund…
                "account_type": "Payments",
                "transaction_description": f"TX {t['id']}",
                "amount": float(t.get("amount", 0)),
                "transaction_currency": t.get("currency"),
                "location_id": t.get("location_id"),
                "source_name": t.get("source_name"),
                "status": t.get("status"),
                "product_id": None,
                "payment_method_name": (
                    t.get("payment_details", {}).get("payment_method_name")
                    or t.get("gateway")
                ),
            }
        )

    # ------------------------------------------------------------------ #
    # 3.c  Remboursements (réutilise get_refund_details)
    # ------------------------------------------------------------------ #
    for r in refunds:
        refund_id = r.get("id")
        transactions.extend(
            get_refund_details(
                order_id=order_id,
                refund_id=refund_id,
                client_id=client_id,
                source_name=source_name,
                payment_method_name=payment_method_name,
            )
        )

    # Trie par date
    transactions.sort(key=lambda x: _iso_to_dt(x["date"]))
    return transactions


# ---------------------------------------------------------------------------
# 4. Fenêtrage dans le temps
# ---------------------------------------------------------------------------

def get_transactions_between_dates(start: datetime, end: datetime) -> List[Dict]:
    print(f"Recherche des transactions entre {start.isoformat()} et {end.isoformat()}")
    formatted_start = start.isoformat()
    formatted_end = end.isoformat()

    store_domain = "adam-lippes.myshopify.com"
    api_version = "2024-10"
    url = (
        f"https://{store_domain}/admin/api/{api_version}/orders.json"
        f"?updated_at_min={formatted_start}&updated_at_max={formatted_end}&status=any"
    )

    resp = requests.get(url, headers=_shopify_headers())
    if not resp.ok:
        print(f"[Orders] {resp.status_code}: {resp.text}")
        return []

    txs: List[Dict] = []
    orders = resp.json().get("orders", [])
    print(f"Nombre de commandes trouvées: {len(orders)}")
    
    for order in orders:
        order_id = str(order["id"])
        print(f"Traitement de la commande: {order_id}")
        txs.extend(get_transactions_by_order(order_id))
    
    print(f"Total des transactions extraites: {len(txs)}")
    return txs


def get_transactions_since_date(dt_since: datetime):
    print(f"Récupération des transactions depuis {dt_since.isoformat()}")
    return get_transactions_between_dates(dt_since, datetime.now(datetime.UTC))


# ---------------------------------------------------------------------------
# 5. Persistance en base
# ---------------------------------------------------------------------------

def process_transactions(txs: List[Dict[str, Any]]) -> Dict[str, int | list]:
    """
    Insère ou met à jour les transactions dans PostgreSQL.
    """
    print(f"Début du traitement de {len(txs)} transactions...")
    stats = {
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    }

    if not txs:
        print("Aucune transaction à traiter.")
        return stats

    print("Connexion à la base de données...")
    conn = _pg_connect()
    cur = conn.cursor()

    insert_q = """
        INSERT INTO transaction (
            date, order_id, client_id, account_type, transaction_description,
            amount, transaction_currency, location_id, source_name, status,
            product_id, payment_method_name
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    update_q = """
        UPDATE transaction SET
            client_id = %s,
            transaction_currency = %s,
            location_id = %s,
            source_name = %s,
            status = %s,
            product_id = %s,
            payment_method_name = %s,
            updated_at_timestamp = CURRENT_TIMESTAMP
        WHERE id = %s
    """

    check_q = """
        SELECT id FROM transaction
        WHERE date = %s AND order_id = %s AND account_type = %s
          AND transaction_description = %s AND amount = %s
    """

    try:
        for i, tx in enumerate(txs):
            if i % 50 == 0 and i > 0:
                print(f"Progression: {i}/{len(txs)} transactions traitées")
            
            try:
                dt_obj = _iso_to_dt(tx["date"])
                params_check = (
                    dt_obj,
                    tx["order_id"],
                    tx["account_type"],
                    tx["transaction_description"],
                    tx["amount"],
                )
                cur.execute(check_q, params_check)
                existing = cur.fetchone()

                if existing:
                    cur.execute(
                        update_q,
                        (
                            tx["client_id"],
                            tx["transaction_currency"],
                            tx.get("location_id"),
                            tx.get("source_name"),
                            tx.get("status"),
                            tx.get("product_id"),
                            tx.get("payment_method_name"),
                            existing[0],
                        ),
                    )
                    stats["updated"] += 1
                else:
                    cur.execute(
                        insert_q,
                        (
                            dt_obj,
                            tx["order_id"],
                            tx["client_id"],
                            tx["account_type"],
                            tx["transaction_description"],
                            tx["amount"],
                            tx["transaction_currency"],
                            tx.get("location_id"),
                            tx.get("source_name"),
                            tx.get("status"),
                            tx.get("product_id"),
                            tx.get("payment_method_name"),
                        ),
                    )
                    stats["inserted"] += 1
            except Exception as exc:
                stats["errors"].append(str(exc))
                stats["skipped"] += 1
                print(f"Erreur sur transaction: {str(exc)}")

        print("Validation des changements (commit)...")
        conn.commit()
    except Exception as exc:
        print(f"Erreur critique, rollback: {str(exc)}")
        conn.rollback()
        stats["errors"].append(str(exc))
    finally:
        cur.close()
        conn.close()
        print("Connexion DB fermée.")

    print(f"Fin du traitement: {stats['inserted']} insérées, {stats['updated']} mises à jour, {stats['skipped']} ignorées")
    return stats


# ---------------------------------------------------------------------------
# 6. Exemple d'exécution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Démarrage de la synchronisation des transactions ===")
    # Exemple : re-synchronise les 2 derniers jours
    print("Récupération des transactions des 2 derniers jours...")
    since = datetime.now(datetime.UTC) - timedelta(days=2)
    all_tx = get_transactions_since_date(since)
    print(f"Traitement de {len(all_tx)} transactions...")
    result = process_transactions(all_tx)
    print("=== Synchronisation terminée ===")
    print(json.dumps(result, indent=2, default=str))
