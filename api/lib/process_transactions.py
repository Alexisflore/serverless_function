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
from decimal import Decimal, ROUND_HALF_UP
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


def get_orders_details_id(order_id: str, product_id: int | None, variant_id: int | None, name: str | None) -> int | None:
    """
    Récupère l'ID orders_details correspondant à un produit/variant d'une commande.
    
    Args:
        order_id: ID de la commande
        product_id: ID du produit (peut être None pour les transactions financières globales)
        variant_id: ID du variant (peut être None pour les transactions financières globales)
        name: nom du produit (peut être None pour les transactions financières globales)
    Returns:
        L'ID orders_details correspondant ou None si pas de correspondance
    """
    try:
        conn = _pg_connect()
        cur = conn.cursor()

        if product_id is None and variant_id is None and name is not None:
            query = """
                SELECT _id_order_detail 
                FROM orders_details 
                WHERE _id_order::bigint = %s 
                  AND name = %s
                LIMIT 1
            """
            cur.execute(query, [int(order_id), name])
        else:
            query = """
                SELECT _id_order_detail 
                FROM orders_details 
                WHERE _id_order::bigint = %s 
                AND _id_product = %s 
                AND variant_id = %s
                LIMIT 1
            """
            cur.execute(query, [int(order_id), product_id, variant_id])

        result = cur.fetchone()
        cur.close()
        conn.close()
        
        return result[0] if result else None
        
    except Exception as e:
        print(f"Erreur lors de la récupération de orders_details_id: {e}")
        return None


def _iso_to_dt(date_str: str) -> datetime:
    """Convertit 2025-03-26T19:11:42-04:00 → obj datetime en UTC."""
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    return datetime.fromisoformat(date_str)

def get_exchange_rate(shop_money: Dict[str, Any], presentment_money: Dict[str, Any]) -> float:
    """
    Calcule le taux de change entre USD et la devise locale de la commande.
    """
    if presentment_money == 0:
        return 1.0
    elif shop_money == 0:
        return 1.0
    else:
        return shop_money / presentment_money


def calculate_exchange_rate(order: Dict[str, Any]) -> tuple[float, str, str]:
    """
    Calcule le taux de change entre USD et la devise locale de la commande.
    
    Returns:
        tuple: (taux_de_change, devise_locale, devise_shop)
            - taux_de_change: multiplicateur pour convertir devise_locale vers devise_shop (USD)
            - devise_locale: la devise présentée au client (EUR, CAD, etc.)
            - devise_shop: la devise de la boutique (USD)
    """
    # Récupération des prix dans les deux devises
    total_price_set = order.get("total_price_set", {})
    shop_money = total_price_set.get("shop_money", {})  # USD (devise de la boutique)
    presentment_money = total_price_set.get("presentment_money", {})  # Devise locale
    
    # Devise de la boutique (toujours USD pour cette boutique)
    shop_currency = shop_money.get("currency_code", "USD")
    
    # Devise de présentation (celle vue par le client)
    local_currency = presentment_money.get("currency_code", shop_currency)
    
    # Calcul du taux de change
    exchange_rate = 1.0  # Par défaut, pas de conversion
    
    if shop_money and presentment_money and local_currency != shop_currency:
        usd_amount = float(shop_money.get("amount", 0))
        local_amount = float(presentment_money.get("amount", 0))
        
        if local_amount > 0:
            exchange_rate = usd_amount / local_amount
    
    return exchange_rate, local_currency, shop_currency

def apply_currency_conversion(local_amount: float, exchange_rate: float, local_currency: str, shop_currency: str) -> tuple[float, float | None]:
    """
    Applique la conversion de devise selon la logique découverte.
    
    Args:
        local_amount: Montant en devise locale
        exchange_rate: Taux de change LocalCurrency/USD (ex: 145.14407684098188 pour JPY/USD)
        local_currency: Devise locale (EUR, CAD, etc.)
        shop_currency: Devise de la boutique (USD)
    
    Returns:
        tuple: (amount_usd, amount_currency)
            - amount_usd: Montant en USD (pour le champ 'amount')
            - amount_currency: Montant en devise locale si différente de USD, sinon None
    """
    # Le exchange_rate est calculé comme USD/LocalCurrency, donc pour convertir
    # de local vers USD, on divise par l'exchange_rate inversé
    # Ou on multiplie par l'exchange_rate si celui-ci est déjà USD/Local
    amount_usd = local_amount * exchange_rate
    
    # Si la devise locale est différente de USD, on stocke les deux montants
    if local_currency != shop_currency:
        amount_currency = local_amount
    else:
        amount_currency = amount_usd
    
    return amount_usd, amount_currency

# ---------------------------------------------------------------------------
# 2. Extraction fine des remboursements (inchangé + payment_method_name)
# ---------------------------------------------------------------------------

def get_refund_details(
    order_id: str,
    refund_id: str,
    client_id: str,
    source_name: str,
    payment_method_name: str | None,
    exchange_rate: float = 1.0,
    taxes_included: bool = False,
) -> List[Dict[str, Any]]:
    """
    Retourne une liste d'items détaillés liés au remboursement.
    Traite à la fois les refund_line_items et les order_adjustments.
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
    items: List[Dict[str, Any]] = []
    refund_date = refund.get("created_at")
    location_id = refund.get("location_id")
    
    # 1. Traiter les refund_line_items (remboursements d'articles spécifiques)
    for refund_item in refund.get("refund_line_items", []):
        account_type = "Returns"
        
        # Récupérer le line_item (objet unique, pas une liste)
        li = refund_item.get("line_item", {})
        amount_shop_money = float(li.get("price_set", {}).get("shop_money", {}).get("amount", 0))
        amount_currency = float(li.get("price_set", {}).get("presentment_money", {}).get("amount", amount_shop_money))
        product_id = li.get("product_id")
        variant_id = li.get("variant_id")
        refund_status = refund_item.get("restock_type")
        refund_quantity = int(refund_item.get("quantity", 1))
        shop_currency = li.get("price_set", {}).get("shop_money", {}).get("currency_code", "USD")
        presentment_currency = li.get("price_set", {}).get("presentment_money", {}).get("currency_code", shop_currency)

        exchange_rate = amount_currency / amount_shop_money if amount_shop_money != 0 else 1.0

        # Ligne article remboursée
        orders_details_id = get_orders_details_id(order_id, product_id, li.get("variant_id"), li.get("name"))

        total_shop_tax_amount = 0
        total_presentment_tax_amount = 0
        for tax_line in li.get("tax_lines", []):
            tax_shop_amount = float(tax_line.get("price_set", {}).get("shop_money", {}).get("amount", 0))
            tax_currency = tax_line.get("price_set", {}).get("shop_money", {}).get("currency_code", shop_currency)
            tax_presentment_amount = float(tax_line.get("price_set", {}).get("presentment_money", {}).get("amount", 0))
            tax_presentment_currency = tax_line.get("price_set", {}).get("presentment_money", {}).get("currency_code", shop_currency)

            total_shop_tax_amount += tax_shop_amount
            total_presentment_tax_amount += tax_presentment_amount

            taxe_line = {
                "date": refund_date,
                "order_id": order_id,
                "client_id": client_id,
                "type": "tax_line",
                "account_type": "Taxes",
                "transaction_description": f"Taxes: {tax_line.get('title')}",
                "shop_amount": -tax_shop_amount,
                "amount_currency": -tax_presentment_amount,
                "transaction_currency": tax_presentment_currency,
                "location_id": location_id,
                "source_name": source_name,
                "status": refund_status,
                "product_id": product_id,
                "variant_id": li.get("variant_id"),
                "payment_method_name": payment_method_name,
                "orders_details_id": orders_details_id,
                "quantity": refund_quantity,
                "exchange_rate": exchange_rate,
                "shop_currency": tax_currency,
            }
            items.append(taxe_line)

        # Ligne article remboursée
        items.append(
            {
                "date": refund_date,
                "order_id": order_id,
                "client_id": client_id,
                "type": "refund_line_item",
                "account_type": account_type,
                "transaction_description": f"Return: {li.get('name')} (qty: {refund_quantity})",
                "shop_amount": -amount_shop_money if not taxes_included else -amount_shop_money + total_shop_tax_amount,
                "amount_currency": -amount_currency if not taxes_included else -amount_currency + total_presentment_tax_amount,
                "transaction_currency": presentment_currency,
                "location_id": location_id,
                "source_name": source_name,
                "status": refund_status,
                "product_id": product_id,
                "variant_id": variant_id,
                "payment_method_name": payment_method_name,
                "orders_details_id": orders_details_id,
                "quantity": refund_quantity,
                "exchange_rate": exchange_rate,
                "shop_currency": shop_currency,
            }
        )
        # Duties au niveau des line items
        for duty in li.get("duties", []):
            duty_shop_amount = float(duty.get("price_set", {}).get("shop_money", {}).get("amount", 0))
            duty_presentment_amount = float(duty.get("price_set", {}).get("presentment_money", {}).get("amount", duty_shop_amount))
            duty_currency = duty.get("price_set", {}).get("shop_money", {}).get("currency_code", shop_currency)
            duty_presentment_currency = duty.get("price_set", {}).get("presentment_money", {}).get("currency_code", shop_currency)

            exchange_rate = duty_presentment_amount / duty_shop_amount if duty_shop_amount != 0 else 1.0

            items.append({
                "date": refund_date,
                "order_id": order_id,
                "client_id": client_id,
                "type": "duties_charge",
                "account_type": "Duties",
                "transaction_description": "Return: Duties",
                "shop_amount": -duty_shop_amount,
                "amount_currency": -duty_presentment_amount,
                "transaction_currency": duty_presentment_currency,
                "location_id": location_id,
                "source_name": source_name,
                "status": "success",
                "product_id": product_id,
                "variant_id": variant_id,
                "payment_method_name": payment_method_name,
                "orders_details_id": orders_details_id,
                "quantity": 1,
                "exchange_rate": exchange_rate,
                "shop_currency": duty_currency,
            })
        for discount in li.get("discount_allocations", []):
            discount_shop_amount = float(discount.get("amount_set", {}).get("shop_money", {}).get("amount", 0))
            discount_currency = discount.get("amount_set", {}).get("shop_money", {}).get("currency_code", "USD")
            discount_presentment_amount = float(discount.get("amount_set", {}).get("presentment_money", {}).get("amount", discount_shop_amount))
            discount_presentment_currency = discount.get("amount_set", {}).get("presentment_money", {}).get("currency_code", "USD")

            exchange_rate = discount_presentment_amount / discount_shop_amount if discount_shop_amount != 0 else 1.0

            items.append({
                "date": refund_date,
                "order_id": order_id,
                "client_id": client_id,
                "type": "discount_allocation",
                "account_type": "Discounts",
                "transaction_description": f"Return: Discount index {discount.get('discount_application_index')} for {li.get('name')}",
                "shop_amount": discount_shop_amount,
                "amount_currency": discount_presentment_amount,
                "transaction_currency": discount_presentment_currency,
                "location_id": location_id,
                "source_name": source_name,
                "status": "success",
                "product_id": product_id,
                "variant_id": variant_id,
                "payment_method_name": None,
                "orders_details_id": orders_details_id,
                "quantity": 1,
                "exchange_rate": exchange_rate,
                "shop_currency": discount_currency,
            })
    
    total_shop_amount = 0
    total_amount_currency = 0
    
    for adjustment in refund.get("order_adjustments", []):
        amount_shop_money = Decimal(str(adjustment.get("amount_set", {}).get("shop_money", {}).get("amount")))
        shop_currency = adjustment.get("amount_set", {}).get("shop_money", {}).get("currency_code")
        amount_currency = Decimal(str(adjustment.get("amount_set", {}).get("presentment_money", {}).get("amount")))
        presentment_currency = adjustment.get("amount_set", {}).get("presentment_money", {}).get("currency_code")
        print(f"Adjustment - Shop: {amount_shop_money}, Currency: {amount_currency}")
        
        total_shop_amount += amount_shop_money
        total_amount_currency += amount_currency    
    # Créer une seule transaction d'ajustement pour tous les order_adjustments
    if total_shop_amount != 0:
        # Arrondir à 2 décimales et convertir en float pour la base de données
        final_shop_amount = float(total_shop_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        final_amount_currency = float(total_amount_currency.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        
        # Calcul du taux de change réel pour les order_adjustments
        calculated_adjustment_exchange_rate = amount_currency / amount_shop_money if amount_shop_money != 0 else 1.0
        
        items.append({
            "date": refund_date,
            "order_id": order_id,
            "client_id": client_id,
            "type": "refund_discrepancy",
            "account_type": "Order Adjustment",
            "transaction_description": "Order Adjustment",
            "shop_amount": final_shop_amount,
            "amount_currency": final_amount_currency,
            "transaction_currency": presentment_currency,
            "location_id": location_id,
            "source_name": source_name,
            "status": "success",
            "product_id": None,
            "variant_id": None,
            "payment_method_name": payment_method_name,
            "orders_details_id": None,
            "quantity": 1,
            "exchange_rate": calculated_adjustment_exchange_rate,
            "shop_currency": shop_currency,
        })

    return items

# ---------------------------------------------------------------------------
# 3. Extraction des charges séparées (duties, shipping, etc.)
# ---------------------------------------------------------------------------

def extract_duties_transactions(order: Dict[str, Any], order_id: str, client_id: str, source_name: str, payment_method_name: str | None) -> List[Dict[str, Any]]:
    """Extrait les transactions de duties séparées"""
    duties_transactions = []
    created_at = order.get("created_at")
    
    # Calcul du taux de change pour cette commande
    exchange_rate, local_currency, shop_currency = calculate_exchange_rate(order)
    
    # # Vérifier d'abord les duties originales (avant remboursement)
    # original_total_duties_set = order.get("original_total_duties_set")
    # current_total_duties_set = order.get("current_total_duties_set")
    
    # # Utiliser original_total_duties_set s'il existe et a un montant > 0,
    # # sinon utiliser current_total_duties_set
    # duties_set_to_use = None
    # duties_description = "Duties: Order Level Duties"
    
    # if original_total_duties_set and isinstance(original_total_duties_set, dict):
    #     original_shop = original_total_duties_set.get("shop_money", {})
    #     original_amount = float(original_shop.get("amount", 0))
    #     if original_amount > 0:
    #         duties_set_to_use = original_total_duties_set
    #         duties_description = "Duties: Original Order Level Duties"
    
    # # Si pas de duties originales, utiliser les duties actuelles
    # if not duties_set_to_use and current_total_duties_set and isinstance(current_total_duties_set, dict):
    #     current_shop = current_total_duties_set.get("shop_money", {})
    #     current_amount = float(current_shop.get("amount", 0))
    #     if current_amount > 0:
    #         duties_set_to_use = current_total_duties_set
    #         duties_description = "Duties: Current Order Level Duties"
    
    # # Traiter les duties au niveau de la commande
    # if duties_set_to_use:
    #     shop_duties = duties_set_to_use.get("shop_money", {})
    #     presentment_duties = duties_set_to_use.get("presentment_money", {})

    #     shop_amount = float(shop_duties.get("amount", 0))
    #     presentment_amount = float(presentment_duties.get("amount", 0))
    #     presentment_currency = presentment_duties.get("currency_code", local_currency)
    #     shop_currency = shop_duties.get("currency_code", shop_currency)

    #     exchange_rate = presentment_amount / shop_amount if shop_amount != 0 else 1.0

    #     if presentment_amount > 0:
    #         duties_transactions.append({
    #             "date": created_at,
    #             "order_id": order_id,
    #             "client_id": client_id,
    #             "type": "duties_charge",
    #             "account_type": "Duties",
    #             "transaction_description": duties_description,
    #             "shop_amount": shop_amount,
    #             "amount_currency": presentment_amount,
    #             "transaction_currency": presentment_currency,
    #             "location_id": None,
    #             "source_name": source_name,
    #             "status": "success",
    #             "product_id": None,
    #             "variant_id": None,
    #             "payment_method_name": payment_method_name,
    #             "orders_details_id": None,
    #             "quantity": 1,
    #             "exchange_rate": exchange_rate,
    #             "shop_currency": shop_currency,
    #         })
    
    # Collecter les line_items traités dans les fulfillments pour éviter les doublons
    processed_line_items = set()
    
    # Duties au niveau des line items dans les fulfillments
    for fulfillment in order.get("fulfillments", []):
        location_id = fulfillment.get("location_id")
        fulfillment_created_at = fulfillment.get("created_at") or created_at
        
        for line_item in fulfillment.get("line_items", []):
            product_id = line_item.get("product_id")
            variant_id = line_item.get("variant_id")
            line_item_key = (product_id, variant_id)
            processed_line_items.add(line_item_key)
            
            orders_details_id = get_orders_details_id(order_id, product_id, variant_id, line_item.get("name"))
            
            duties = line_item.get("duties", [])
            for duty in duties:
                duty_price_set = duty.get("price_set", {})
                duty_shop = duty_price_set.get("shop_money", {})
                duty_presentment = duty_price_set.get("presentment_money", {})

                shop_amount = float(duty_shop.get("amount", 0))
                presentment_amount = float(duty_presentment.get("amount", 0))
                presentment_currency = duty_presentment.get("currency_code", local_currency)
                shop_currency = duty_shop.get("currency_code", shop_currency)

                exchange_rate = presentment_amount / shop_amount if shop_amount != 0 else 1.0
                
                if presentment_amount > 0:
                    
                    duties_transactions.append({
                        "date": fulfillment_created_at,
                        "order_id": order_id,
                        "client_id": client_id,
                        "type": "duties_charge",
                        "account_type": "Duties",
                        "transaction_description": f"Duties: {line_item.get('name', 'Line Item Duty')}",
                        "shop_amount": shop_amount,
                        "amount_currency": presentment_amount,
                        "transaction_currency": presentment_currency,
                        "location_id": location_id,
                        "source_name": source_name,
                        "status": "success",
                        "product_id": product_id,
                        "variant_id": variant_id,
                        "payment_method_name": payment_method_name,
                        "orders_details_id": orders_details_id,
                        "quantity": 1,
                        "exchange_rate": exchange_rate,
                        "shop_currency": shop_currency,
                    })
    
    # Traiter les duties des line_items directement depuis la commande pour ceux non expédiés
    # (important pour les commandes sans fulfillments ou partiellement expédiées)
    for line_item in order.get("line_items", []):
        product_id = line_item.get("product_id")
        variant_id = line_item.get("variant_id")
        line_item_key = (product_id, variant_id)
        
        # Skip si déjà traité dans les fulfillments
        if line_item_key in processed_line_items:
            continue
            
        orders_details_id = get_orders_details_id(order_id, product_id, variant_id, line_item.get("name"))
        
        duties = line_item.get("duties", [])
        for duty in duties:
            duty_price_set = duty.get("price_set", {})
            duty_shop = duty_price_set.get("shop_money", {})
            duty_presentment = duty_price_set.get("presentment_money", {})

            shop_amount = float(duty_shop.get("amount", 0))
            presentment_amount = float(duty_presentment.get("amount", 0))
            presentment_currency = duty_presentment.get("currency_code", local_currency)
            shop_currency = duty_shop.get("currency_code", shop_currency)

            exchange_rate = presentment_amount / shop_amount if shop_amount != 0 else 1.0
            
            if presentment_amount > 0:
                
                duties_transactions.append({
                    "date": created_at,  # Utilise la date de création de la commande
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "duties_charge",
                    "account_type": "Duties",
                    "transaction_description": f"Duties: {line_item.get('name', 'Line Item Duty')}",
                    "shop_amount": shop_amount,
                    "amount_currency": presentment_amount,
                    "transaction_currency": presentment_currency,
                    "location_id": None,  # Pas de location_id pour les items non expédiés
                    "source_name": source_name,
                    "status": "pending",  # Statut pending pour les items non expédiés
                    "product_id": product_id,
                    "variant_id": variant_id,
                    "payment_method_name": payment_method_name,
                    "orders_details_id": orders_details_id,
                    "quantity": 1,
                    "exchange_rate": exchange_rate,
                    "shop_currency": shop_currency,
                })
    
    return duties_transactions


def extract_shipping_transactions(order: Dict[str, Any], order_id: str, client_id: str, source_name: str, payment_method_name: str | None, taxes_included: bool) -> List[Dict[str, Any]]:
    """Extrait les transactions de frais de livraison séparées"""
    shipping_transactions = []
    created_at = order.get("created_at")
    
    # Calcul du taux de change pour cette commande
    exchange_rate, local_currency, shop_currency = calculate_exchange_rate(order)
    
    # Frais de livraison principaux
    shipping_lines = order.get("shipping_lines", [])
    for shipping_line in shipping_lines:
        # Initialiser les variables de taxes pour chaque shipping line
        total_tax_amount = 0
        total_tax_amount_currency = 0
        
        # Utilise presentment_money si disponible, sinon shop_money
        price_set = shipping_line.get("price_set", {})
        presentment_money = price_set.get("presentment_money", {})
        shop_money = price_set.get("shop_money", {})
        
        if presentment_money and presentment_money.get("amount"):
            local_amount = float(presentment_money.get("amount", 0))
            currency = presentment_money.get("currency_code", local_currency)
        else:
            local_amount = float(shipping_line.get("price", 0))
            currency = shop_money.get("currency_code", shop_currency)
        
        
        # Taxes sur les frais de livraison - classées comme "Taxes" pour cohérence avec Shopify
        for tax_line in shipping_line.get("tax_lines", []):
            # Utilise presentment_money si disponible pour les taxes aussi
            tax_price_set = tax_line.get("price_set", {})
            tax_presentment = tax_price_set.get("presentment_money", {})
            tax_shop = tax_price_set.get("shop_money", {})
            
            if tax_presentment and tax_presentment.get("amount"):
                tax_local_amount = float(tax_presentment.get("amount", 0))
                tax_currency = tax_presentment.get("currency_code", local_currency)
            else:
                tax_local_amount = float(tax_line.get("price", 0))
                tax_currency = tax_shop.get("currency_code", shop_currency)
            
            if tax_local_amount > 0:
                tax_amount_usd, tax_amount_currency = apply_currency_conversion(tax_local_amount, exchange_rate, tax_currency, shop_currency)
                total_tax_amount += tax_amount_usd
                total_tax_amount_currency += tax_amount_currency
                shipping_transactions.append({
                    "date": created_at,
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "shipping_tax",
                    "account_type": "Taxes",  # Changé de "Shipping" à "Taxes"
                    "transaction_description": f"Shipping Tax: {tax_line.get('title', 'Shipping Tax')}",
                    "shop_amount": tax_amount_usd,
                    "amount_currency": tax_amount_currency,
                    "transaction_currency": tax_currency,
                    "location_id": None,
                    "source_name": source_name,
                    "status": "success",
                    "product_id": None,
                    "variant_id": None,
                    "payment_method_name": payment_method_name,
                    "orders_details_id": None,
                    "quantity": 1,
                    "exchange_rate": exchange_rate,
                    "shop_currency": shop_currency,
                })
    
        if local_amount > 0:
            amount_usd, amount_currency = apply_currency_conversion(local_amount, exchange_rate, currency, shop_currency)
            if taxes_included:
                amount_usd = amount_usd - total_tax_amount
                amount_currency = amount_currency - total_tax_amount_currency
            shipping_transactions.append({
                "date": created_at,
                "order_id": order_id,
                "client_id": client_id,
                "type": "shipping_charge",
                "account_type": "Shipping",
                "transaction_description": f"Shipping: {shipping_line.get('title', 'Shipping Fee')}",
                "shop_amount": amount_usd,
                "amount_currency": amount_currency,
                "transaction_currency": currency,
                "location_id": None,
                "source_name": source_name,
                "status": "success",
                "product_id": None,
                "variant_id": None,
                "payment_method_name": payment_method_name,
                "orders_details_id": None,
                "quantity": 1,
                "exchange_rate": exchange_rate,
                "shop_currency": shop_currency,
            })
    return shipping_transactions

def extract_gift_card_transactions(order: Dict[str, Any], order_id: str, client_id: str, source_name: str, payment_method_name: str | None, taxes_included: bool) -> List[Dict[str, Any]]:
    """Extrait les transactions de cartes cadeaux séparées"""
    gift_card_transactions = []
    created_at = order.get("created_at")
    
    # Calcul du taux de change pour cette commande
    exchange_rate, local_currency, shop_currency = calculate_exchange_rate(order)
    
    # Gift card transactions
    gift_cards = order.get("gift_cards", [])
    for gift_card in gift_cards:
        local_amount = float(gift_card.get("amount", 0))
        currency = order.get("currency", shop_currency)  # Les gift cards sont généralement dans la devise de la commande
        
        if local_amount > 0:
            amount_usd, amount_currency = apply_currency_conversion(local_amount, exchange_rate, currency, shop_currency)
            
            gift_card_transactions.append({
                "date": created_at,
                "order_id": order_id,
                "client_id": client_id,
                "type": "gift_card_payment",
                "account_type": "Gift Cards",
                "transaction_description": f"Gift Card: {gift_card.get('last_characters', 'Payment')}",
                "shop_amount": -amount_usd,  # Négatif car c'est un paiement
                "amount_currency": -amount_currency if amount_currency else None,
                "transaction_currency": currency,
                "location_id": None,
                "source_name": source_name,
                "status": "success",
                "product_id": None,
                "variant_id": None,
                "payment_method_name": "gift_card",
                "orders_details_id": None,
                "quantity": 1,
                "exchange_rate": exchange_rate,
                "shop_currency": shop_currency,
            })
    
    return gift_card_transactions

def extract_tips_transactions(order: Dict[str, Any], order_id: str, client_id: str, source_name: str, payment_method_name: str | None, taxes_included: bool) -> List[Dict[str, Any]]:
    """Extrait les transactions de pourboires séparées"""
    tips_transactions = []
    created_at = order.get("created_at")
    
    # Calcul du taux de change pour cette commande
    exchange_rate, local_currency, shop_currency = calculate_exchange_rate(order)
    
    # Recherche de pourboires dans la commande
    current_total_additional_fees_set = order.get("current_total_additional_fees_set")
    if current_total_additional_fees_set and isinstance(current_total_additional_fees_set, dict):
        shop_money = current_total_additional_fees_set.get("shop_money", {})
        presentment_money = current_total_additional_fees_set.get("presentment_money", {})
        
        # Utilise le montant en devise locale si disponible, sinon le montant shop
        if presentment_money and isinstance(presentment_money, dict) and presentment_money.get("amount"):
            local_amount = float(presentment_money.get("amount", 0))
            currency = presentment_money.get("currency_code", local_currency)
        elif shop_money and isinstance(shop_money, dict):
            local_amount = float(shop_money.get("amount", 0))
            currency = shop_money.get("currency_code", shop_currency)
        else:
            local_amount = 0
            currency = shop_currency
        
        if local_amount > 0:
            amount_usd, amount_currency = apply_currency_conversion(local_amount, exchange_rate, currency, shop_currency)
            
            tips_transactions.append({
                "date": created_at,
                "order_id": order_id,
                "client_id": client_id,
                "type": "tips_charge",
                "account_type": "Tips",
                "transaction_description": "Tips: Additional Fees",
                "shop_amount": amount_usd,
                "amount_currency": amount_currency,
                "transaction_currency": currency,
                "location_id": None,
                "source_name": source_name,
                "status": "success",
                "product_id": None,
                "variant_id": None,
                "payment_method_name": payment_method_name,
                "orders_details_id": None,
                "quantity": 1,
                "exchange_rate": exchange_rate,
                "shop_currency": shop_currency,
            })
    
    return tips_transactions

# ---------------------------------------------------------------------------
# 4. Extraction d'une commande (lignes + taxes + transactions)
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
            taxes_included = order.get("taxes_included", False)

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

    # Collecter tous les line_items traités dans les fulfillments pour éviter les doublons
    processed_line_items = set()

    # D'abord, traiter les line_items dans les fulfillments (priorité car ils ont un location_id)
    for f in fulfillments:
        location_id = f.get("location_id")
        status = f.get("status")
        if status != "success":
            continue
        created_at = f.get("created_at")

        for li in f.get("line_items", []):
            product_id = li.get("product_id")
            variant_id = li.get("variant_id")
            quantity = int(li.get("quantity", 1))  # Récupération de la quantité

            # Marquer ce line_item comme traité pour éviter les doublons
            line_item_key = (product_id, variant_id)
            processed_line_items.add(line_item_key)
            
            # Récupération de l'orders_details_id pour ce line_item
            orders_details_id = get_orders_details_id(order_id, product_id, variant_id, li.get("name"))

            # Utilise presentment_money si disponible, sinon shop_money
            price_set = li.get("price_set", {})
            presentment_money = price_set.get("presentment_money", {})
            shop_money = price_set.get("shop_money", {})

            shop_amount = float(shop_money.get("amount", 0))
            shop_currency = shop_money.get("currency_code", None)
            presentment_amount = float(presentment_money.get("amount", 0))
            presentment_currency = presentment_money.get("currency_code", None)

            exchange_rate = presentment_amount / shop_amount if shop_amount != 0 else 1.0

            #  – remises éventuelles (déjà calculées pour la quantité totale dans Shopify)
            for d in li.get("discount_allocations", []):
                # Utilise presentment_money si disponible pour les remises
                disc_amount_set = d.get("amount_set", {})
                disc_presentment = disc_amount_set.get("presentment_money", {})
                disc_shop = disc_amount_set.get("shop_money", {})

                disc_presentment_amount = float(disc_presentment.get("amount", 0))
                disc_presentment_currency = disc_presentment.get("currency_code", None)
                disc_shop_amount = float(disc_shop.get("amount", 0))
                disc_shop_currency = disc_shop.get("currency_code", shop_currency)

                exchange_rate = disc_presentment_amount / disc_shop_amount if disc_shop_amount != 0 else 1.0

                transactions.append(
                    {
                        "date": created_at,
                        "order_id": order_id,
                        "client_id": client_id,
                        "type": "discount_line",
                        "account_type": "Discounts",
                        "transaction_description": f"Discount index {d.get('discount_application_index')} for {li.get('name')} (qty: {quantity})",
                        "shop_amount": -disc_shop_amount,  # Négatif car c'est une remise
                        "amount_currency": -disc_presentment_amount,
                        "transaction_currency": disc_presentment_currency,
                        "location_id": location_id,
                        "source_name": source_name,
                        "status": status,
                        "product_id": product_id,
                        "variant_id": variant_id,
                        "payment_method_name": payment_method_name,
                        "orders_details_id": orders_details_id,
                        "quantity": quantity,
                        "exchange_rate": exchange_rate,
                        "shop_currency": disc_shop_currency,
                    }
                )

            #  – taxes (déjà calculées pour la quantité totale dans Shopify)
            # Initialiser les variables de taxes pour chaque line item
            total_shop_tax_amount = 0
            total_presentment_tax_amount = 0
            for tax in li.get("tax_lines", []):
                # Utilise presentment_money si disponible pour les taxes
                tax_price_set = tax.get("price_set", {})
                tax_presentment = tax_price_set.get("presentment_money", {})
                tax_shop = tax_price_set.get("shop_money", {})

                tax_presentment_amount = float(tax_presentment.get("amount", 0))
                tax_presentment_currency = tax_presentment.get("currency_code", None)
                tax_shop_amount = float(tax_shop.get("amount", 0))
                tax_shop_currency = tax_shop.get("currency_code", shop_currency)

                exchange_rate = tax_presentment_amount / tax_shop_amount if tax_shop_amount != 0 else 1.0

                total_shop_tax_amount += tax_shop_amount
                total_presentment_tax_amount += tax_presentment_amount

                transactions.append(
                    {
                        "date": created_at,
                        "order_id": order_id,
                        "client_id": client_id,
                        "type": "tax_line",
                        "account_type": "Taxes",
                        "transaction_description": f"{tax.get('title')} for {li.get('name')} (qty: {quantity})",
                        "shop_amount": tax_shop_amount,
                        "amount_currency": tax_presentment_amount,
                        "transaction_currency": tax_presentment_currency,
                        "location_id": location_id,
                        "source_name": source_name,
                        "status": status,
                        "product_id": product_id,
                        "variant_id": variant_id,
                        "payment_method_name": payment_method_name,
                        "orders_details_id": orders_details_id,
                        "quantity": quantity,
                        "exchange_rate": exchange_rate,
                        "shop_currency": tax_shop_currency,
                    }
                )
            # Multiplier par la quantité pour obtenir le montant total
            shop_amount = shop_amount * quantity
            presentment_amount = presentment_amount * quantity
            #  – vente brute HT
            transactions.append(
                {
                    "date": created_at,
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "sales_gross",
                    "account_type": "Sales",
                    "transaction_description": f"{li.get('name')} Gross HT (qty: {quantity})",
                    "shop_amount": shop_amount if not taxes_included else shop_amount - total_shop_tax_amount,
                    "amount_currency": presentment_amount if not taxes_included else presentment_amount - total_presentment_tax_amount,
                    "transaction_currency": presentment_currency,
                    "location_id": location_id,
                    "source_name": source_name,
                    "status": status,
                    "product_id": product_id,
                    "variant_id": variant_id,
                    "payment_method_name": payment_method_name,
                    "orders_details_id": orders_details_id,
                    "quantity": quantity,
                    "exchange_rate": exchange_rate,
                    "shop_currency": shop_currency,
                }
            )

    # ------------------------------------------------------------------ #
    # 3.a.2  Line items non-expédiés (pour commandes partiellement expédiées)
    # ------------------------------------------------------------------ #
    
    # Récupère le location_id principal depuis les fulfillments si disponible
    primary_location_id = None
    if fulfillments:
        primary_location_id = fulfillments[0].get("location_id")
    
    # Traiter les line_items directement depuis la commande pour ceux non encore expédiés
    for li in order.get("line_items", []):
        product_id = li.get("product_id")
        variant_id = li.get("variant_id")
        line_item_key = (product_id, variant_id)
        
        # Skip si déjà traité dans les fulfillments
        if line_item_key in processed_line_items:
            continue
            
        quantity = int(li.get("quantity", 1))
        
        # Récupération de l'orders_details_id pour ce line_item
        orders_details_id = get_orders_details_id(order_id, product_id, variant_id, li.get("name"))

        # Utilise presentment_money si disponible, sinon shop_money
        price_set = li.get("price_set", {})
        presentment_money = price_set.get("presentment_money", {})
        shop_money = price_set.get("shop_money", {})
        shop_amount = float(shop_money.get("amount", 0))
        shop_currency = shop_money.get("currency_code", None)
        presentment_amount = float(presentment_money.get("amount", 0))
        presentment_currency = presentment_money.get("currency_code", None)
        exchange_rate = presentment_amount / shop_amount if shop_amount != 0 else 1.0

        # Remises éventuelles
        for d in li.get("discount_allocations", []):
            disc_amount_set = d.get("amount_set", {})
            disc_presentment = disc_amount_set.get("presentment_money", {})
            disc_shop = disc_amount_set.get("shop_money", {})

            disc_presentment_amount = float(disc_presentment.get("amount", 0))
            disc_presentment_currency = disc_presentment.get("currency_code", None)
            disc_shop_amount = float(disc_shop.get("amount", 0))
            disc_shop_currency = disc_shop.get("currency_code", shop_currency)

            disc_exchange_rate = disc_presentment_amount / disc_shop_amount if disc_shop_amount != 0 else 1.0
            
            transactions.append(
                {
                    "date": order.get("created_at"),
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "discount_line",
                    "account_type": "Discounts",
                    "transaction_description": f"Discount index {d.get('discount_application_index')} for {li.get('name')} (qty: {quantity})",
                    "shop_amount": -disc_shop_amount,
                    "amount_currency": -disc_presentment_amount,
                    "transaction_currency": disc_presentment_currency,
                    "location_id": primary_location_id,
                    "source_name": source_name,
                    "status": "pending",
                    "product_id": product_id,
                    "variant_id": variant_id,
                    "payment_method_name": payment_method_name,
                    "orders_details_id": orders_details_id,
                    "quantity": quantity,
                    "exchange_rate": disc_exchange_rate,
                    "shop_currency": disc_shop_currency,
                }
            )

        # Taxes
        total_shop_tax_amount = 0
        total_presentment_tax_amount = 0
        for tax in li.get("tax_lines", []):
            tax_price_set = tax.get("price_set", {})
            tax_presentment = tax_price_set.get("presentment_money", {})
            tax_shop = tax_price_set.get("shop_money", {})

            tax_presentment_amount = float(tax_presentment.get("amount", 0))
            tax_presentment_currency = tax_presentment.get("currency_code", None)
            tax_shop_amount = float(tax_shop.get("amount", 0))
            tax_shop_currency = tax_shop.get("currency_code", None)
            
            tax_exchange_rate = tax_presentment_amount / tax_shop_amount if tax_shop_amount != 0 else 1.0

            total_shop_tax_amount += tax_shop_amount
            total_presentment_tax_amount += tax_presentment_amount

            transactions.append(
                {
                    "date": order.get("created_at"),
                    "order_id": order_id,
                    "client_id": client_id,
                    "type": "tax_line",
                    "account_type": "Taxes",
                    "transaction_description": f"{tax.get('title')} for {li.get('name')} (qty: {quantity})",
                    "shop_amount": tax_shop_amount,
                    "amount_currency": tax_presentment_amount,
                    "transaction_currency": tax_presentment_currency,
                    "location_id": primary_location_id,
                    "source_name": source_name,
                    "status": "pending",
                    "product_id": product_id,
                    "variant_id": variant_id,
                    "payment_method_name": payment_method_name,
                    "orders_details_id": orders_details_id,
                    "quantity": quantity,
                    "exchange_rate": tax_exchange_rate,
                    "shop_currency": tax_shop_currency,
                }
            )

        # Créer les transactions pour les line_items non-expédiés
        # Utiliser la date de création de la commande et un statut "pending"
        transactions.append(
            {
                "date": order.get("created_at"),  # Date de la commande au lieu du fulfillment
                "order_id": order_id,
                "client_id": client_id,
                "type": "sales_gross",
                "account_type": "Sales",
                "transaction_description": f"{li.get('name')} Gross HT (qty: {quantity})",
                "shop_amount": shop_amount if not taxes_included else shop_amount - total_shop_tax_amount,
                "amount_currency": presentment_amount if not taxes_included else presentment_amount - total_presentment_tax_amount,
                "transaction_currency": presentment_currency,
                "location_id": primary_location_id,  # Utilise le location principal
                "source_name": source_name,
                "status": "pending",  # Statut pour les articles non-expédiés
                "product_id": product_id,
                "variant_id": variant_id,
                "payment_method_name": payment_method_name,
                "orders_details_id": orders_details_id,
                "quantity": quantity,
                "exchange_rate": exchange_rate,
                "shop_currency": shop_currency,
            }
        )
    # ------------------------------------------------------------------ #
    # 3.b  Transactions financières (split-tender, Shop Pay, remboursements)
    # ------------------------------------------------------------------ #
    
    for t in tx_list:
        # Utilise le location_id de la transaction, sinon celui du fulfillment principal
        transaction_location_id = t.get("location_id") or primary_location_id
        if t.get("status") != "success" or t.get("kind") not in ["refund", "capture", "sale"]:
            continue

        # Détermine le bon account_type selon le kind de transaction
        transaction_kind = t.get("kind")
        if transaction_kind == "refund":
            account_type = "Refunds"
        else:
            account_type = "Payments"

        # Utilise le status de la transaction (ex: success depuis payment_refund_attributes)
        transaction_status = t.get("status")
        # Si c'est un refund, on peut aussi récupérer le status depuis payments_refund_attributes
        if transaction_kind == "refund" and t.get("payments_refund_attributes"):
            refund_status = t.get("payments_refund_attributes", {}).get("status")
            if refund_status:
                transaction_status = refund_status

        # Calcul des montants pour les paiements avec la nouvelle logique
        tx_amount_local = float(t.get("amount", 0))
        tx_currency = t.get("currency")
        
        # Pour les transactions de type "Payments" et "Refunds", utiliser la conversion de devise
        if account_type in ["Payments", "Refunds"]:
            # Recalculer l'exchange_rate correct pour cette transaction spécifique
            # car l'exchange_rate global peut être différent du taux de cette transaction
            tx_exchange_rate = exchange_rate
            
            # Si la transaction est dans une devise différente de USD, calculer le bon taux
            if tx_currency != shop_currency:
                # Le montant de la transaction est déjà dans la devise locale
                # Il faut le convertir en USD en utilisant le bon taux
                # Utiliser l'exchange_rate global de la commande (USD/Local)
                amount_usd = tx_amount_local / exchange_rate
                amount_local = tx_amount_local
            else:
                # Transaction déjà en USD
                amount_usd = tx_amount_local
                amount_local = tx_amount_local
            
            # shop_amount doit être en USD, amount_currency en devise locale
            amount = amount_usd
            amount_currency = amount_local if tx_currency != shop_currency else None
            
            # Pour les refunds, rendre le montant négatif
            if account_type == "Refunds":
                amount = -amount
                if amount_currency is not None:
                    amount_currency = -amount_currency
        else:
            # Pour les autres types, garder l'ancien comportement
            amount_currency = None
            amount = tx_amount_local

        transactions.append(
            {
                "date": t.get("created_at"),
                "order_id": order_id,
                "client_id": client_id,
                "type": transaction_kind,                 # authorization, capture, sale, refund…
                "account_type": account_type,
                "transaction_description": f"TX {t['id']}",
                "shop_amount": amount,
                "amount_currency": amount_currency,
                "transaction_currency": tx_currency,
                "location_id": transaction_location_id,
                "source_name": t.get("source_name") or source_name,
                "status": transaction_status,
                "product_id": None,  # Les transactions financières globales n'ont pas de produit spécifique
                "variant_id": None,  # Les transactions financières globales n'ont pas de variant spécifique
                "payment_method_name": (
                    t.get("payment_details", {}).get("payment_method_name")
                    or t.get("gateway")
                ),
                "orders_details_id": None,  # Les transactions financières globales n'ont pas d'orders_details_id spécifique
                "quantity": 1,  # Quantité par défaut pour les transactions financières globales
                "exchange_rate": exchange_rate,
                "shop_currency": shop_currency,
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
                exchange_rate=exchange_rate,
                taxes_included=taxes_included,
            )
        )

    # ------------------------------------------------------------------ #
    # 3.d  Nouvelles transactions de charges séparées
    # ------------------------------------------------------------------ #
    
    # Ajouter les transactions de duties séparées
    duties_transactions = extract_duties_transactions(order, order_id, client_id, source_name, payment_method_name)
    transactions.extend(duties_transactions)
    
    # Ajouter les transactions de shipping séparées
    shipping_transactions = extract_shipping_transactions(order, order_id, client_id, source_name, payment_method_name, taxes_included)
    transactions.extend(shipping_transactions)

    # Ajouter les transactions de gift cards séparées
    gift_card_transactions = extract_gift_card_transactions(order, order_id, client_id, source_name, payment_method_name, taxes_included)
    transactions.extend(gift_card_transactions)

    # Ajouter les transactions de tips séparées
    tips_transactions = extract_tips_transactions(order, order_id, client_id, source_name, payment_method_name, taxes_included)
    transactions.extend(tips_transactions)

    # Trie par date
    transactions.sort(key=lambda x: _iso_to_dt(x["date"]))

    return transactions


# ---------------------------------------------------------------------------
# 4. Fenêtrage dans le temps
# ---------------------------------------------------------------------------

def get_transactions_between_dates(start: datetime, end: datetime, orders_id_to_skip: List[str]) -> List[Dict]:
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
    print(f"Commandes à ignorer: {orders_id_to_skip}")
    
    for order in orders:
        order_id = str(order["id"])
        if order_id in orders_id_to_skip:
            print(f"Commande {order_id} à ignorer")
            continue
        print(f"Traitement de la commande: {order_id}")
        txs.extend(get_transactions_by_order(order_id))

    return txs

def get_transactions_since_date(dt_since: datetime):
    print(f"Récupération des transactions depuis {dt_since.isoformat()}")
    return get_transactions_between_dates(dt_since, datetime.now(), [])


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

    conn = _pg_connect()
    cur = conn.cursor()

    insert_q = """
        INSERT INTO transaction (
            date, order_id, client_id, account_type, transaction_description,
            shop_amount, amount_currency, transaction_currency, location_id, source_name, status,
            product_id, variant_id, payment_method_name, orders_details_id, quantity, exchange_rate, shop_currency
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    update_q = """
        UPDATE transaction SET
            client_id = %s,
            account_type = %s,
            shop_amount = %s,
            amount_currency = %s,
            transaction_currency = %s,
            location_id = %s,
            status = %s,
            product_id = %s,
            variant_id = %s,
            payment_method_name = %s,
            orders_details_id = %s,
            quantity = %s,
            exchange_rate = %s,
            shop_currency = %s,
            updated_at_timestamp = CURRENT_TIMESTAMP
        WHERE id = %s
    """

    # Check principal avec date exacte
    check_q = """
        SELECT id, status FROM transaction
        WHERE date = %s AND order_id = %s AND transaction_description = %s
          AND source_name = %s
    """
    
    # Check secondaire sans date pour détecter les mises à jour de statut
    check_q_without_date = """
        SELECT id, status, date FROM transaction
        WHERE order_id = %s AND transaction_description = %s
          AND source_name = %s
    """

    try:
        for i, tx in enumerate(txs):
            if i % 50 == 0 and i > 0:
                print(f"Progression: {i}/{len(txs)} transactions traitées")
            
            try:
                dt_obj = _iso_to_dt(tx["date"])
                
                # Premier check : date exacte
                params_check = (
                    dt_obj,
                    tx["order_id"],
                    tx["transaction_description"],
                    tx.get("source_name"),
                )
                cur.execute(check_q, params_check)
                existing = cur.fetchone()

                if existing:
                    # Transaction trouvée avec date exacte - mise à jour classique
                    cur.execute(
                        update_q,
                        (
                            tx["client_id"],
                            tx["account_type"],
                            tx["shop_amount"],
                            tx.get("amount_currency"),
                            tx["transaction_currency"],
                            tx.get("location_id"),
                            tx.get("status"),
                            tx.get("product_id"),
                            tx.get("variant_id"),
                            tx.get("payment_method_name"),
                            tx.get("orders_details_id"),
                            tx.get("quantity", 1),
                            tx.get("exchange_rate"),
                            tx.get("shop_currency"),
                            existing[0],
                        ),
                    )
                    stats["updated"] += 1
                else:
                    # Pas trouvé avec date exacte - check sans date pour détecter changement de statut
                    params_check_no_date = (
                        tx["order_id"],
                        tx["transaction_description"],
                        tx.get("source_name"),
                    )
                    cur.execute(check_q_without_date, params_check_no_date)
                    existing_no_date = cur.fetchone()
                    
                    if existing_no_date:
                        existing_id, existing_status, existing_date = existing_no_date
                        current_status = tx.get("status")
                        
                        # Si le statut est différent, c'est une mise à jour de la même transaction
                        if existing_status != current_status:
                            # Mise à jour avec nouvelle date et nouveau statut
                            update_q_with_date = """
                                UPDATE transaction SET
                                    date = %s,
                                    client_id = %s,
                                    account_type = %s,
                                    shop_amount = %s,
                                    amount_currency = %s,
                                    transaction_currency = %s,
                                    location_id = %s,
                                    status = %s,
                                    product_id = %s,
                                    variant_id = %s,
                                    payment_method_name = %s,
                                    orders_details_id = %s,
                                    quantity = %s,
                                    exchange_rate = %s,
                                    shop_currency = %s,
                                    updated_at_timestamp = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """
                            cur.execute(
                                update_q_with_date,
                                (
                                    dt_obj,
                                    tx["client_id"],
                                    tx["account_type"],
                                    tx["shop_amount"],
                                    tx.get("amount_currency"),
                                    tx["transaction_currency"],
                                    tx.get("location_id"),
                                    current_status,
                                    tx.get("product_id"),
                                    tx.get("variant_id"),
                                    tx.get("payment_method_name"),
                                    tx.get("orders_details_id"),
                                    tx.get("quantity", 1),
                                    tx.get("exchange_rate"),
                                    tx.get("shop_currency"),
                                    existing_id,
                                ),
                            )
                            stats["updated"] += 1
                            print(f"Mise à jour statut: {existing_status} -> {current_status} pour transaction order_id={tx['order_id']}")
                        else:
                            # Même statut mais date différente - nouvelle transaction
                            cur.execute(
                                insert_q,
                                (
                                    dt_obj,
                                    tx["order_id"],
                                    tx["client_id"],
                                    tx["account_type"],
                                    tx["transaction_description"],
                                    tx["shop_amount"],
                                    tx.get("amount_currency"),
                                    tx["transaction_currency"],
                                    tx.get("location_id"),
                                    tx.get("source_name"),
                                    tx.get("status"),
                                    tx.get("product_id"),
                                    tx.get("variant_id"),
                                    tx.get("payment_method_name"),
                                    tx.get("orders_details_id"),
                                    tx.get("quantity", 1),
                                    tx.get("exchange_rate"),
                                    tx.get("shop_currency"),
                                ),
                            )
                            stats["inserted"] += 1
                            print(f"Nouvelle transaction (même statut {current_status}) pour order_id={tx['order_id']}")
                    else:
                        # Aucune transaction similaire trouvée - nouvelle insertion
                        cur.execute(
                            insert_q,
                            (
                                dt_obj,
                                tx["order_id"],
                                tx["client_id"],
                                tx["account_type"],
                                tx["transaction_description"],
                                tx["shop_amount"],
                                tx.get("amount_currency"),
                                tx["transaction_currency"],
                                tx.get("location_id"),
                                tx.get("source_name"),
                                tx.get("status"),
                                tx.get("product_id"),
                                tx.get("variant_id"),
                                tx.get("payment_method_name"),
                                tx.get("orders_details_id"),
                                tx.get("quantity", 1),
                                tx.get("exchange_rate"),
                                tx.get("shop_currency"),
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
