#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extraction des versements Shopify Payments (« Deposited ») + insertion en base
Ajout du champ payment_method_name pour chaque payout_transaction
"""

import os
import json
import logging
from datetime import datetime, date
from typing import Optional, Dict, Any, List

import requests
import psycopg2
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# 0. Configuration générale & helpers
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("process_payout")

load_dotenv()
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
STORE_DOMAIN = "adam-lippes.myshopify.com"
API_VERSION = "2024-10"


def _shopify_headers() -> Dict[str, str]:
    return {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
    }


def get_db_connection():
    """Connexion PostgreSQL (fallback sur les var. séparées)."""
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


# ---------------------------------------------------------------------------
# 1. Détails commande & versement
# ---------------------------------------------------------------------------

def obtenir_details_commande(order_id: str) -> Dict[str, Any]:
    """Renvoie id + nom de la commande, quitte à fallback sur l'ID brut."""
    try:
        url = f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}/orders/{order_id}.json"
        r = requests.get(url, headers=_shopify_headers())
        if r.ok:
            order = r.json().get("order", {})
            return {"id": order.get("id"), "name": order.get("name")}
        logger.warning("Erreur req cmd %s: %s", order_id, r.status_code)
    except Exception as exc:
        logger.error("Exception commande %s: %s", order_id, exc)
    return {"id": order_id, "name": f"#{order_id}"}


def obtenir_details_versement(payout_id: str) -> Dict[str, str]:
    """Renvoie bank_reference (ou N/A) via balance/transactions ou fallback."""
    try:
        url = (
            f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
            f"/shopify_payments/balance/transactions.json?payout_id={payout_id}"
        )
        r = requests.get(url, headers=_shopify_headers())
        if r.ok:
            for t in r.json().get("transactions", []):
                if t.get("type") == "payout" and t.get("source_id") == str(payout_id):
                    ref = t.get("reference")
                    if ref:
                        return {"bank_reference": ref}

        # fallback direct payout
        url = (
            f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
            f"/shopify_payments/payouts/{payout_id}.json"
        )
        r2 = requests.get(url, headers=_shopify_headers())
        if r2.ok:
            payout = r2.json().get("payout", {})
            return {"bank_reference": payout.get("bank_reference", "N/A")}
    except Exception as exc:
        logger.error("Exception détails versement %s: %s", payout_id, exc)
    return {"bank_reference": "N/A"}


# ---------------------------------------------------------------------------
# 2. Récupération transactions d'un versement (+ payment_method_name)
# ---------------------------------------------------------------------------

from typing import Optional, Dict, Any

def _fetch_payment_method_name(
    order_id: str,
    order_tx_id: str
) -> Optional[str]:
    """
    Retourne payment_method_name pour une transaction donnée
    (order_id + transaction_id indispensables).
    """
    try:
        url = (
            f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
            f"/orders/{order_id}/transactions/{order_tx_id}.json"
        )
        r = requests.get(url, headers=_shopify_headers())
        if r.ok:
            return (
                r.json()
                .get("transaction", {})
                .get("payment_details", {})
                .get("payment_method_name")
            )
        # --- fallback : on liste toutes les transactions de la commande ---
        if r.status_code == 404:
            url2 = (
                f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
                f"/orders/{order_id}/transactions.json?limit=250"
            )
            r2 = requests.get(url2, headers=_shopify_headers())
            if r2.ok:
                for tx in r2.json().get("transactions", []):
                    if str(tx.get("id")) == str(order_tx_id):
                        return tx.get("payment_details", {}).get(
                            "payment_method_name"
                        )
    except Exception as exc:
        logger.debug("PMN fetch err %s/%s : %s", order_id, order_tx_id, exc)
    return None


def obtenir_transactions_versement(payout_id: str, limite: int = 250) -> List[Dict]:
    """Renvoie la liste complète des transactions du payout, enrichies."""
    try:
        url = (
            f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
            f"/shopify_payments/payouts/{payout_id}/transactions.json?limit={limite}"
        )
        r = requests.get(url, headers=_shopify_headers())
        if not r.ok:
            logger.error("Req tx payout %s: %s\n%s", payout_id, r.status_code, r.text)
            return []

        txs = r.json().get("transactions", [])
        logger.info("Tx récupérées pour payout %s: %d", payout_id, len(txs))

        for t in txs:
            # ① présence directe ?
            pm_name = _fetch_payment_method_name(
                t["source_order_id"],
                t["source_order_transaction_id"]
            )
            t["payment_method_name"] = pm_name
        return txs
    except Exception as exc:
        logger.error("Exception tx payout %s: %s", payout_id, exc)
        return []


# ---------------------------------------------------------------------------
# 3. Pagination sur les versements « paid »
# ---------------------------------------------------------------------------

def obtenir_versements_deposited_format_specifique(
    limite: int = 50, page_info: str | None = None
) -> tuple[str | None, bool, List[Dict[str, Any]]]:
    """
    Retourne (next_page_info, has_results, [payouts_formattés])
    Chaque payout contient ses transactions déjà formatées.
    """
    url = (
        f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
        f"/shopify_payments/payouts.json?limit={limite}&status=paid"
    )
    if page_info:
        url += f"&page_info={page_info}"

    r = requests.get(url, headers=_shopify_headers())
    if not r.ok:
        logger.error("Req payouts: %s\n%s", r.status_code, r.text)
        return None, False, []

    payouts = r.json().get("payouts", [])
    logger.info("Payouts récupérés (paid): %d", len(payouts))

    all_payouts_fmt: List[Dict[str, Any]] = []

    for payout in payouts:
        payout_id = payout.get("id")
        payout_date = payout.get("date")
        logger.info("Traitement payout %s (%s)", payout_id, payout_date)

        payout_details = obtenir_details_versement(payout_id)
        tx_raw = obtenir_transactions_versement(payout_id)

        # Cache des commandes
        orders_cache: Dict[str, Dict[str, Any]] = {}

        # Totaux cumulés
        tot_amount = tot_fee = charges_total = refunds_total = 0.0

        formatted_tx: List[Dict[str, Any]] = []

        for tx in tx_raw:
            # conversions sûres
            try:
                amount = float(tx.get("amount", 0))
            except (ValueError, TypeError):
                amount = 0.0
            try:
                fee = float(tx.get("fee", 0))
            except (ValueError, TypeError):
                fee = 0.0
            net = amount - fee

            # cumuls
            tot_amount += amount
            tot_fee += fee
            if tx.get("type") == "charge":
                charges_total += amount
            elif tx.get("type") == "refund":
                refunds_total += amount

            # Détails commande
            order_id = tx.get("source_order_id")
            if order_id:
                order_det = orders_cache.get(order_id) or obtenir_details_commande(
                    order_id
                )
                orders_cache.setdefault(order_id, order_det)
            else:
                order_det = None

            # Date ISO courte
            tx_date_short = (
                tx.get("processed_at", "").split("T")[0] if tx.get("processed_at") else ""
            )

            formatted_tx.append(
                {
                    "id": tx.get("id"),
                    "date": tx_date_short,
                    "order_id": order_id,
                    "order_name": order_det.get("name") if order_det else tx.get("order_number", ""),
                    "type": tx.get("type"),
                    "amount": amount,
                    "fee": fee,
                    "net": net,
                    "currency": tx.get("currency", "USD"),
                    "payment_method_name": tx.get("payment_method_name"),  # <-- NEW
                }
            )

        summary = {
            "total": float(payout.get("amount", 0)),
            "bank_reference": payout_details.get("bank_reference", "N/A"),
            "charges_total": charges_total,
            "refunds_total": refunds_total,
            "fees_total": tot_fee,
            "currency": payout.get("currency", "USD"),
        }

        all_payouts_fmt.append(
            {
                "id": payout_id,
                "date": payout_date,
                "status": "Deposited",
                "summary": summary,
                "transactions": formatted_tx,
            }
        )

    # pagination
    link_header = r.headers.get("Link", "")
    next_page = None
    if 'rel="next"' in link_header:
        import re
        m = re.search(r"page_info=([^&>]+)", link_header)
        if m:
            next_page = m.group(1)

    return next_page, bool(payouts), all_payouts_fmt


# ---------------------------------------------------------------------------
# 4. Version « jour » : insertion en base
# ---------------------------------------------------------------------------

def recuperer_et_enregistrer_versements_jour(
    today: str = date.today().isoformat(),
) -> Dict[str, Any]:
    """Récupère les versements du jour et les insère en DB (payout / payout_tx)."""
    stats = {
        "payouts_inserted": 0,
        "payouts_skipped": 0,
        "transactions_inserted": 0,
        "transactions_skipped": 0,
        "errors": [],
    }

    conn = cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")  # test
        logger.info("Connexion DB OK")

        # requête payouts du jour
        url = (
            f"https://{STORE_DOMAIN}/admin/api/{API_VERSION}"
            f"/shopify_payments/payouts.json?limit=50&status=paid&date={today}"
        )
        r = requests.get(url, headers=_shopify_headers())
        if not r.ok:
            raise RuntimeError(f"Req payouts day: {r.status_code} - {r.text}")

        for payout in r.json().get("payouts", []):
            payout_id = payout["id"]

            payout_details = obtenir_details_versement(payout_id)
            tx_raw = obtenir_transactions_versement(payout_id)

            orders_cache: Dict[str, Dict[str, Any]] = {}
            tot_amount = tot_fee = charges_total = refunds_total = 0.0
            formatted_tx = []

            for tx in tx_raw:
                amount = float(tx.get("amount", 0) or 0)
                fee = float(tx.get("fee", 0) or 0)
                net = amount - fee

                tot_amount += amount
                tot_fee += fee
                if tx.get("type") == "charge":
                    charges_total += amount
                elif tx.get("type") == "refund":
                    refunds_total += amount

                order_id = tx.get("source_order_id")
                if order_id:
                    order_det = orders_cache.get(order_id) or obtenir_details_commande(
                        order_id
                    )
                    orders_cache.setdefault(order_id, order_det)
                else:
                    order_det = None

                formatted_tx.append(
                    {
                        "id": tx["id"],
                        "date": tx.get("processed_at", "").split("T")[0] if tx.get("processed_at") else None,
                        "order_id": order_id,
                        "order_name": order_det.get("name") if order_det else tx.get("order_number", ""),
                        "type": tx.get("type"),
                        "amount": amount,
                        "fee": fee,
                        "net": net,
                        "currency": tx.get("currency", "USD"),
                        "payment_method_name": tx.get("payment_method_name"),  # NEW
                    }
                )

            # 1°) insert payout

            payout_date_obj = datetime.strptime(payout["date"], "%Y-%m-%d").date()
            
            # Vérifier si le payout existe déjà et le supprimer si c'est le cas
            cur.execute("SELECT id FROM payout WHERE id = %s", (payout_id,))
            if cur.fetchone():
                logger.info("Payout %s existe déjà, suppression avant réinsertion", payout_id)
                # Supprimer d'abord les transactions associées
                cur.execute("DELETE FROM payout_transaction WHERE payout_id = %s", (payout_id,))
                # Puis supprimer le payout
                cur.execute("DELETE FROM payout WHERE id = %s", (payout_id,))
            
            cur.execute(
                """
                INSERT INTO payout (
                    id, date, status, total, bank_reference,
                    charges_total, refunds_total, fees_total, currency
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    payout_id,
                    payout_date_obj,
                    "Deposited",
                    float(payout["amount"]),
                    payout_details["bank_reference"],
                    charges_total,
                    refunds_total,
                    tot_fee,
                    payout.get("currency", "USD"),
                ),
            )
            stats["payouts_inserted"] += 1

            # 2°) insert payout_transaction
            for tx in formatted_tx:
                try:
                    print(f"transaction: {tx}")
                    # Vérifier si la transaction existe déjà et la supprimer si c'est le cas
                    cur.execute("SELECT id FROM payout_transaction WHERE id = %s", (tx["id"],))
                    if cur.fetchone():
                        logger.debug("Transaction %s existe déjà, suppression avant réinsertion", tx["id"])
                        cur.execute("DELETE FROM payout_transaction WHERE id = %s", (tx["id"],))
                        
                    cur.execute(
                        """
                        INSERT INTO payout_transaction (
                            id, payout_id, date, order_id, order_name,
                            type, amount, fee, net, currency,
                            payment_method_name                      -- NEW
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            tx["id"],
                            payout_id,
                            tx["date"],
                            tx["order_id"],
                            tx["order_name"],
                            tx["type"],
                            tx["amount"],
                            tx["fee"],
                            tx["net"],
                            tx["currency"],
                            tx["payment_method_name"],             # NEW
                        ),
                    )
                    stats["transactions_inserted"] += 1
                except Exception as exc_tx:
                    conn.rollback()
                    stats["transactions_skipped"] += 1
                    stats["errors"].append(f"Tx {tx['id']}: {exc_tx}")
                    logger.error("Insert tx %s: %s", tx["id"], exc_tx)
                    continue

            conn.commit()
            logger.info("Payout %s inséré avec %d tx", payout_id, len(formatted_tx))

    except Exception as exc:
        if conn:
            conn.rollback()
        stats["errors"].append(str(exc))
        logger.error("Erreur générale: %s", exc)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    logger.info(
        "%d payouts ins, %d skip | %d tx ins, %d skip | %d erreurs",
        stats["payouts_inserted"],
        stats["payouts_skipped"],
        stats["transactions_inserted"],
        stats["transactions_skipped"],
        len(stats["errors"]),
    )
    print(json.dumps(stats, indent=2, default=str))
    return stats


# ---------------------------------------------------------------------------
# 5. Entrée du script
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    recuperer_et_enregistrer_versements_jour()
