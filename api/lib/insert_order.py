import psycopg2
import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('insert_order')

def get_db_connection():
    """
    Établit une connexion à la base de données
    """
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        user = os.getenv("SUPABASE_USER")
        password = os.getenv("SUPABASE_PASSWORD")
        host = os.getenv("SUPABASE_HOST")
        port = os.getenv("SUPABASE_PORT")
        dbname = os.getenv("SUPABASE_DB_NAME")
        
        if not all([user, password, host, port, dbname]):
            raise ValueError("Informations de connexion à la base de données incomplètes")
            
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    return psycopg2.connect(db_url)

def safe_float(value, default=0.0):
    """
    Convertit de façon sécurisée une valeur en nombre flottant
    
    Args:
        value: Valeur à convertir
        default: Valeur par défaut si la conversion échoue
        
    Returns:
        float: La valeur convertie ou la valeur par défaut
    """
    if value is None:
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Impossible de convertir '{value}' en nombre flottant. Utilisation de la valeur par défaut: {default}")
        return default

def get_nested_value(data, path, default=None):
    """
    Récupère une valeur dans un dictionnaire imbriqué en utilisant un chemin
    
    Args:
        data (dict): Dictionnaire source
        path (str): Chemin d'accès (ex: "customer > id" ou "billing_address > address1")
        default: Valeur par défaut si le chemin n'existe pas
        
    Returns:
        La valeur trouvée ou la valeur par défaut
    """
    if data is None:
        return default
        
    keys = path.split('>')
    current = data
    
    for key in keys:
        key = key.strip()
        if isinstance(current, dict) and key in current:
            current = current[key]
            # Si on trouve un None dans le chemin, retourner la valeur par défaut
            if current is None:
                return default
        else:
            return default
            
    return current

def format_discount_codes(order):
    """
    Formate les codes de réduction en chaîne de caractères
    
    Args:
        order (dict): Données de la commande
        
    Returns:
        str: Chaîne formatée des codes de réduction
    """
    discount_codes = order.get('discount_codes', [])
    if not discount_codes:
        return None
        
    return ', '.join([code.get('code', '') for code in discount_codes if 'code' in code])

def extract_tax_lines(tax_lines, index=0):
    """
    Extrait les informations d'une ligne de taxe spécifique
    
    Args:
        tax_lines (list): Liste des lignes de taxe
        index (int): Index de la ligne de taxe à extraire
        
    Returns:
        tuple: (nom de la taxe, taux de la taxe, montant de la taxe)
    """
    if not tax_lines or len(tax_lines) <= index or tax_lines[index] is None:
        return None, None, None
        
    tax_line = tax_lines[index]
    name = tax_line.get('title')
    
    # Convertir le taux de taxe
    rate = tax_line.get('rate')
    if rate is not None:
        try:
            rate = float(rate)
        except (ValueError, TypeError):
            rate = None
    
    # Pour les lignes de produit, utiliser 'price' pour la valeur de taxe
    value = tax_line.get('price') or tax_line.get('amount')
    
    return name, rate, value

def insert_order(order_data):
    """
    Insère une commande Shopify et ses données associées dans la base de données
    
    Args:
        order_data (dict): Données de la commande au format JSON Shopify
        
    Returns:
        dict: Statistiques sur les opérations effectuées
    """
    stats = {
        "orders_inserted": 0,
        "orders_skipped": 0,
        "orders_updated": 0,
        "order_details_inserted": 0,
        "order_details_errors": 0,
        "errors": []
    }
    
    conn = None
    cur = None

    # Vérifier si le JSON est valide
    if not order_data or not isinstance(order_data, dict):
        error_msg = "Données de commande invalides ou vides"
        logger.error(error_msg)
        stats["errors"].append(error_msg)
        return stats
    # Vérifier si le JSON contient des commandes
    orders = order_data.get('orders', [])
    if not orders or not isinstance(orders, list):
        error_msg = "Aucune commande trouvée dans les données"
        logger.error(error_msg)
        stats["errors"].append(error_msg)
        return stats
    
    logger.info(f"Début du traitement de {len(orders)} commandes")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Traiter chaque commande dans le JSON
        for order in orders:
            try:
                # Si l'ordre n'est pas un dictionnaire, ignorer
                if not isinstance(order, dict):
                    error_msg = "Commande ignorée: format invalide (non-dictionnaire)"
                    logger.warning(error_msg)
                    stats["orders_skipped"] += 1
                    stats["errors"].append(error_msg)
                    continue
                    
                # Vérifier que l'ID de la commande existe et n'est pas null
                order_id = order.get('id')
                if order_id is None or order_id == "null" or order_id == "":
                    error_msg = f"Commande ignorée: l'ID de la commande est manquant ou invalide. Données partielles: {order.get('name', 'N/A')}, {order.get('created_at', 'N/A')}"
                    logger.warning(error_msg)
                    stats["orders_skipped"] += 1
                    stats["errors"].append(error_msg)
                    continue

                # Log pour déboguer
                logger.info(f"Traitement de la commande ID: {order_id}, Nom: {order.get('name', 'N/A')}")
                
                # Mapping des champs selon le mapping fourni
                # Format : "colonne_supabase": source dans le JSON Shopify
                order_mapped = {
                    # Identification
                    "_id_order": order_id,                              # id
                    "_id_customer": get_nested_value(order, 'customer > id'), # customer > id
                    "order_label": order.get('name'),                      # name
                    "app_id": order.get('app_id'),                         # app_id (direct)
                    
                    # Statuts
                    "confirmed": order.get('confirmed'),                   # confirmed
                    "financial_status": order.get('financial_status'),     # financial_status
                    "fulfillment_status": order.get('fulfillment_status'), # fulfillment_status
                    "location_id": order.get('location_id'),               # location_id
                    
                    # Contact et dates
                    "contact_email": order.get('contact_email'),           # contact_email
                    "created_at": order.get('created_at'),                 # created_at
                    "currency": order.get('currency'),                     # currency
                    
                    # Valeurs financières
                    "origin_total_orders": order.get('total_price'),       # total_price
                    "gross_sales": order.get('total_line_items_price'),    # total_line_items_price
                    "returns": order.get('returns', 0),                    # returns (default 0)
                    "discount": order.get('current_total_discounts'),      # current_total_discounts
                    "taxes": order.get('current_total_tax'),               # current_total_tax
                    "shipping": get_nested_value(order, 'total_shipping_price_set > shop_money > amount'), # total_shipping_price_set
                    "current_total_orders": order.get('current_total_price'), # current_total_price
                    "current_subtotal_price": order.get('current_subtotal_price'), # current_subtotal_price
                    
                    # Infos client
                    "customer_locale": order.get('customer_locale'),       # customer_locale
                    "note": order.get('note'),                             # note
                    "tags": order.get('tags'),                             # tags
                    "landing_site": order.get('landing_site'),             # landing_site
                    "referring_site": order.get('referring_site'),         # referring_site
                    "source_name": order.get('source_name'),               # source_name
                    
                    # Informations de facturation
                    "billing_first_name": get_nested_value(order, 'billing_address > first_name'),
                    "billing_address1": get_nested_value(order, 'billing_address > address1'),
                    "billing_phone": get_nested_value(order, 'billing_address > phone'),
                    "billing_city": get_nested_value(order, 'billing_address > city'),
                    "billing_zip": get_nested_value(order, 'billing_address > zip'),
                    "billing_province": get_nested_value(order, 'billing_address > province'),
                    "billing_country": get_nested_value(order, 'billing_address > country'),
                    "billing_last_name": get_nested_value(order, 'billing_address > last_name'),
                    "billing_address2": get_nested_value(order, 'billing_address > address2'),
                    "billing_company": get_nested_value(order, 'billing_address > company'),
                    "billing_latitude": get_nested_value(order, 'billing_address > latitude'),
                    "billing_longitude": get_nested_value(order, 'billing_address > longitude'),
                    "billing_name": get_nested_value(order, 'billing_address > name'),
                    "billing_country_code": get_nested_value(order, 'billing_address > country_code'),
                    "billing_province_code": get_nested_value(order, 'billing_address > province_code'),
                    
                    # Informations d'expédition
                    "shipping_first_name": get_nested_value(order, 'shipping_address > first_name'),
                    "shipping_address1": get_nested_value(order, 'shipping_address > address1'),
                    "shipping_phone": get_nested_value(order, 'shipping_address > phone'),
                    "shipping_city": get_nested_value(order, 'shipping_address > city'),
                    "shipping_zip": get_nested_value(order, 'shipping_address > zip'),
                    "shipping_province": get_nested_value(order, 'shipping_address > province'),
                    "shipping_country": get_nested_value(order, 'shipping_address > country'),
                    "shipping_last_name": get_nested_value(order, 'shipping_address > last_name'),
                    "shipping_address2": get_nested_value(order, 'shipping_address > address2'),
                    "shipping_company": get_nested_value(order, 'shipping_address > company'),
                    "shipping_latitude": get_nested_value(order, 'shipping_address > latitude'),
                    "shipping_longitude": get_nested_value(order, 'shipping_address > longitude'),
                    "shipping_name": get_nested_value(order, 'shipping_address > name'),
                    "shipping_country_code": get_nested_value(order, 'shipping_address > country_code'),
                    "shipping_province_code": get_nested_value(order, 'shipping_address > province_code'),
                    "shipping_total_weight": order.get('total_weight'),    # total_weight
                    
                    # Informations de suivi
                    "shipment_status": order.get('shipment_status'),       # shipment_status
                    "tracking_company": order.get('tracking_company'),     # tracking_company
                    "tracking_number": order.get('tracking_number'),       # tracking_number
                    
                    # Formatage des codes de réduction
                    "discount_codes": format_discount_codes(order)         # discount_codes > code
                }
                
                # Extraire les informations de taxes (jusqu'à 5 taxes différentes)
                # tax*_name -> tax_lines > title
                # tax*_rate -> tax_lines > rate
                # tax*_value_origin -> tax_lines > price
                tax_lines = order.get('tax_lines', [])
                for i in range(5):
                    if i < len(tax_lines) and tax_lines[i] is not None:
                        tax_line = tax_lines[i]
                        order_mapped[f"tax{i+1}_name"] = tax_line.get('title')
                        
                        # Convertir le taux de taxe en décimal
                        tax_rate = tax_line.get('rate')
                        if tax_rate is not None:
                            try:
                                order_mapped[f"tax{i+1}_rate"] = float(tax_rate)
                            except (ValueError, TypeError):
                                order_mapped[f"tax{i+1}_rate"] = None
                        
                        order_mapped[f"tax{i+1}_value_origin"] = tax_line.get('price')
                    else:
                        order_mapped[f"tax{i+1}_name"] = None
                        order_mapped[f"tax{i+1}_rate"] = None
                        order_mapped[f"tax{i+1}_value_origin"] = None
                
                # Calculer les champs dérivés (A CALCULER)
                # 1. returns_excl_taxes = returns / (1 + somme des tax_lines_rates)
                returns = safe_float(order_mapped["returns"], 0)
                # Calculer la somme des taux de taxe
                total_tax_rate = 0
                for i in range(5):
                    tax_rate = order_mapped.get(f"tax{i+1}_rate")
                    if tax_rate is not None:
                        total_tax_rate += safe_float(tax_rate, 0)
                
                # Calculer returns_excl_taxes avec la nouvelle formule et arrondir à 1 décimale
                if returns > 0:
                    order_mapped["returns_excl_taxes"] = round(returns / (1 + total_tax_rate), 1)
                else:
                    order_mapped["returns_excl_taxes"] = 0
                
                # 2. net_sales = current_total_price - total_shipping_price_set - current_total_tax
                current_total_price = safe_float(order_mapped["current_total_orders"], 0)
                shipping = safe_float(order_mapped["shipping"], 0)
                taxes = safe_float(order_mapped["taxes"], 0)
                order_mapped["net_sales"] = current_total_price - shipping - taxes
                
                # 3. net_sales_check = true si current_subtotal_price est égal à net_sales
                current_subtotal_price = safe_float(order_mapped["current_subtotal_price"], 0)
                net_sales = order_mapped["net_sales"]
                # Comparaison avec une petite marge d'erreur pour éviter les problèmes d'arrondi
                order_mapped["net_sales_check"] = True if abs(current_subtotal_price - net_sales) < 0.01 else False
                
                # 4. tax_check = vérifier si la somme des taxes individuelles égale current_total_tax
                tax_sum = 0
                for i in range(5):
                    tax_value = order_mapped.get(f"tax{i+1}_value_origin")
                    if tax_value is not None:
                        tax_sum += safe_float(tax_value, 0)
                
                current_total_tax = safe_float(order_mapped["taxes"], 0)
                # Comparaison avec une petite marge d'erreur pour éviter les problèmes d'arrondi
                order_mapped["tax_check"] = True if abs(tax_sum - current_total_tax) < 0.01 else False
                
                # Vérification CRITIQUE: s'assurer que l'ID est défini et non null avant de continuer
                if order_mapped.get("_id_order") is None:
                    error_msg = f"ERREUR CRITIQUE: ID commande perdu après mappage. Commande ignorée: {order_id}"
                    logger.error(error_msg)
                    stats["orders_skipped"] += 1
                    stats["errors"].append(error_msg)
                    continue
                
                # Afficher des informations sur les données qui seront insérées
                logger.debug(f"Données préparées pour l'insertion: _id_order={order_mapped.get('_id_order')}, order_label={order_mapped.get('order_label')}")
                
                # Préparer les colonnes et valeurs pour l'insertion SQL
                columns = []
                values = []
                placeholders = []
                
                for key, value in order_mapped.items():
                    if value is not None:  # Ne pas insérer les valeurs NULL
                        columns.append(key)
                        values.append(value)
                        placeholders.append('%s')
                
                # Vérification supplémentaire pour s'assurer que _id_order est présent
                if "_id_order" not in columns:
                    error_msg = f"Erreur: colonne _id_order manquante après le mappage ou sa valeur est NULL. Commande ignorée: {order_id}"
                    logger.error(error_msg)
                    stats["orders_skipped"] += 1
                    stats["errors"].append(error_msg)
                    continue
                
                # Construire et exécuter la requête d'insertion
                insert_query = f"""
                INSERT INTO orders ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (_id_order) DO UPDATE
                SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns)}
                RETURNING _id_order
                """
                
                cur.execute(insert_query, values)
                result = cur.fetchone()
                
                # Vérification supplémentaire pour s'assurer que l'insertion a réussi
                if result is None or result[0] is None:
                    error_msg = f"Erreur: insertion échouée, aucun ID retourné pour la commande {order_id}"
                    logger.error(error_msg)
                    stats["errors"].append(error_msg)
                    conn.rollback()
                    continue
                
                inserted_order_id = result[0]
                
                # Déterminer si c'est une insertion ou une mise à jour
                if str(inserted_order_id) == str(order_id):
                    # Les IDs correspondent, c'est probablement une mise à jour
                    if cur.rowcount == 1:
                        stats["orders_inserted"] += 1
                        logger.info(f"Commande {inserted_order_id} insérée avec succès")
                    else:
                        stats["orders_updated"] += 1
                        logger.info(f"Commande {inserted_order_id} mise à jour avec succès")
                else:
                    # Les IDs ne correspondent pas, c'est bizarre
                    logger.warning(f"Anomalie: ID inséré ({inserted_order_id}) ne correspond pas à l'ID original ({order_id})")
                    stats["orders_inserted"] += 1
                
                # Traitement des lignes d'articles (order details)
                line_items = order.get('line_items', [])
                logger.info(f"Traitement de {len(line_items)} lignes d'articles pour la commande {order_id}")
                
                # Insérer chaque ligne d'article dans la table orders_details
                for line_item in line_items:
                    try:
                        if not isinstance(line_item, dict):
                            logger.warning(f"Ligne d'article ignorée: format invalide pour la commande {order_id}")
                            continue
                            
                        line_item_id = line_item.get('id')
                        if line_item_id is None:
                            logger.warning(f"Ligne d'article ignorée: ID manquant pour la commande {order_id}")
                            continue
                        if line_item_id == "14790792380487":
                            print(line_item)
                        # Extraire les informations de taxes pour cet article (jusqu'à 5 taxes)
                        item_tax_lines = line_item.get('tax_lines', [])
                        
                        # Créer le mapping des champs pour la table orders_details
                        detail_mapped = {
                            "_id_order_detail": line_item_id,           # line_items > id
                            "_id_order": order_id,                      # id
                            "_id_product": line_item.get('product_id'), # line_items > product_id
                            "current_quantity": line_item.get('current_quantity'), # line_items > current_quantity
                            "fulfillable_quantity": line_item.get('fulfillable_quantity'), # line_items > fulfillable_quantity
                            "fulfillment_service": line_item.get('fulfillment_service'), # line_items > fulfillment_service
                            "fulfillment_status": line_item.get('fulfillment_status'), # line_items > fulfillment_status
                            "gift_card": line_item.get('gift_card'),    # line_items > gift_card
                            "grams": line_item.get('grams'),           # line_items > grams
                            "name": line_item.get('name'),             # line_items > name
                            "pre_tax_price": line_item.get('pre_tax_price'), # line_items > pre_tax_price
                            "price": line_item.get('price'),           # line_items > price
                            "product_exists": line_item.get('product_exists'), # line_items > product_exists
                            "origin_quantity": line_item.get('quantity'), # line_items > quantity
                            "requires_shipping": line_item.get('requires_shipping'), # line_items > requires_shipping
                            "sku": line_item.get('sku'),               # line_items > sku
                            "taxable": line_item.get('taxable'),       # line_items > taxable
                            "title": line_item.get('title'),           # line_items > title
                            "total_discount": line_item.get('total_discount'), # line_items > total_discount
                            "variant_id": line_item.get('variant_id'), # line_items > variant_id
                            "variant_inventory_management": line_item.get('variant_inventory_management'), # line_items > variant_inventory_management
                            "variant_title": line_item.get('variant_title'), # line_items > variant_title
                            "vendor": line_item.get('vendor')          # line_items > vendor
                        }
                        
                        # Extraire les informations de taxes (jusqu'à 5 taxes)
                        total_tax_amount = 0.0
                        for i in range(5):
                            name, rate, value = extract_tax_lines(item_tax_lines, i)
                            detail_mapped[f"tax{i+1}_name"] = name
                            detail_mapped[f"tax{i+1}_rate"] = rate
                            detail_mapped[f"tax{i+1}_value"] = value
                            
                            # Calculer le total des taxes pour cet article
                            if value is not None:
                                total_tax_amount += safe_float(value, 0.0)
                        
                        # Calculer les nouveaux champs financiers
                        price = safe_float(line_item.get('price'), 0.0)
                        quantity = safe_float(line_item.get('quantity'), 0.0)
                        # total_discount = safe_float(line_item.get('total_discount'), 0.0)
                        
                        # Montant brut des ventes (price * quantity)
                        amount_gross_sales = price * quantity
                        
                        # Retours origin quantity - current quantity * price
                        amount_returns = (safe_float(line_item.get('quantity'), 0.0) - safe_float(line_item.get('current_quantity'), 0.0)) * price
                        
                        # Remises (pre_tax_price - price) * current_quantity
                        amount_discounts = (safe_float(line_item.get('pre_tax_price'), 0.0) - price) * safe_float(line_item.get('current_quantity'), 0.0)
                        
                        # Ventes nettes = current_quantity * pre_tax_price
                        amount_net_sales = safe_float(line_item.get('current_quantity'), 0.0) * safe_float(line_item.get('pre_tax_price'), 0.0)
                        
                        # Vérification des ventes nettes
                        amount_net_sales_check = amount_net_sales == amount_gross_sales + amount_returns + amount_discounts
                        
                        # Vérification des retours
                        # Pour l'instant, comme il n'y a pas de logique claire pour les retours au niveau article,
                        # on définit simplement return_check comme True si amount_returns est 0
                        return_check = amount_returns != 0.0
                        
                        # Ajouter les nouveaux champs au mapping
                        detail_mapped.update({
                            "total_taxes": total_tax_amount,
                            "amount_gross_sales": amount_gross_sales,
                            "amount_returns": amount_returns,
                            "amount_discounts": amount_discounts,
                            "amount_net_sales": amount_net_sales,
                            "amount_net_sales_check": amount_net_sales_check,
                            "return_check": return_check
                        })
                        
                        # Préparer l'insertion SQL pour orders_details
                        detail_columns = []
                        detail_values = []
                        detail_placeholders = []
                        
                        for key, value in detail_mapped.items():
                            if value is not None:  # Ne pas insérer les valeurs NULL
                                detail_columns.append(key)
                                detail_values.append(value)
                                detail_placeholders.append('%s')
                                
                        # Construire et exécuter la requête d'insertion pour orders_details
                        detail_insert_query = f"""
                        INSERT INTO orders_details ({', '.join(detail_columns)})
                        VALUES ({', '.join(detail_placeholders)})
                        ON CONFLICT (_id_order_detail) DO UPDATE
                        SET {', '.join(f"{col} = EXCLUDED.{col}" for col in detail_columns)}
                        RETURNING _id_order_detail
                        """
                        
                        cur.execute(detail_insert_query, detail_values)
                        detail_result = cur.fetchone()
                        
                        if detail_result and detail_result[0]:
                            stats["order_details_inserted"] += 1
                            logger.info(f"Détail de commande {detail_result[0]} inséré avec succès pour la commande {order_id}")
                        else:
                            error_msg = f"Erreur: insertion de détail échouée pour l'article {line_item_id} de la commande {order_id}"
                            logger.error(error_msg)
                            stats["order_details_errors"] += 1
                            stats["errors"].append(error_msg)
                        
                    except Exception as e:
                        error_msg = f"Erreur lors du traitement du détail de la commande {order_id}, ligne {line_item.get('id', 'unknown')}: {str(e)}"
                        logger.error(error_msg)
                        stats["order_details_errors"] += 1
                        stats["errors"].append(error_msg)
                        continue
                
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                error_msg = f"Erreur lors du traitement de la commande {order.get('id', 'unknown')}: {str(e)}"
                logger.error(error_msg)
                # Ajouter plus de détails pour le débogage
                try:
                    order_debug = json.dumps(order, default=str)[:500]
                except:
                    order_debug = "Impossible de sérialiser la commande pour le débogage"
                logger.error(f"Détails de la commande problématique: {order_debug}...")
                stats["errors"].append(error_msg)
                continue
        
    except Exception as e:
        error_msg = f"Erreur générale: {str(e)}"
        logger.error(error_msg)
        stats["errors"].append(error_msg)
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    logger.info(f"Fin du traitement: {stats['orders_inserted']} insérées, {stats['orders_updated']} mises à jour, {stats['orders_skipped']} ignorées, {stats['order_details_inserted']} détails insérés, {stats['order_details_errors']} erreurs de détails, {len(stats['errors'])} erreurs totales")
    return stats

if __name__ == "__main__":
    # Exemple d'utilisation
    with open('order_data.json', 'r') as f:
        order_data = json.load(f)
    
    stats = insert_order(order_data)
    print("Statistiques d'insertion:", stats)