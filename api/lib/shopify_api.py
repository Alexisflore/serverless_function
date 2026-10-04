import requests
import os
import time
import re
import logging

from api.lib.utils import normalize_shop_domain

logger = logging.getLogger(__name__)

SHOP_DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]*\.myshopify\.com$")


def fetch_access_token(store_domain, client_id, client_secret, timeout=30):
    """
    Échange client_id/client_secret contre un Admin API access token
    (client credentials grant — apps créées via Shopify CLI / Dev Dashboard).
    Le token est de courte durée : à récupérer à chaque exécution.
    """
    store_domain = normalize_shop_domain(store_domain)
    if not store_domain:
        raise RuntimeError("SHOPIFY_STORE_DOMAIN manquant: échange de token impossible")
    if not SHOP_DOMAIN_RE.match(store_domain):
        raise RuntimeError(f"SHOPIFY_STORE_DOMAIN invalide ({store_domain!r}): attendu <shop>.myshopify.com")
    url = f"https://{store_domain}/admin/oauth/access_token"
    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Shopify token exchange failed for {store_domain}: HTTP {resp.status_code}")
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Shopify token exchange failed for {store_domain}: no access_token in response")
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::add-mask::{token}", flush=True)
    return token


def ensure_access_token():
    """
    Si SHOPIFY_CLIENT_ID et SHOPIFY_CLIENT_SECRET sont définis, récupère TOUJOURS un token et
    écrase SHOPIFY_ACCESS_TOKEN (évite d'utiliser le token d'un autre store présent dans .env).
    À appeler AVANT d'importer les modules qui lisent l'env à l'import
    (process_customer, process_payout, process_inventory_sync).
    """
    client_id = os.environ.get("SHOPIFY_CLIENT_ID")
    client_secret = os.environ.get("SHOPIFY_CLIENT_SECRET")
    if not (client_id and client_secret):
        return False
    store_domain = normalize_shop_domain(os.environ.get("SHOPIFY_STORE_DOMAIN"))
    os.environ["SHOPIFY_ACCESS_TOKEN"] = fetch_access_token(store_domain, client_id, client_secret)
    # Les autres modules construisent leurs URLs depuis l'env : on y remet le domaine normalisé
    os.environ["SHOPIFY_STORE_DOMAIN"] = store_domain
    return True


def _graphql_request(query, variables=None, timeout=30):
    store_domain = os.environ.get("SHOPIFY_STORE_DOMAIN")
    access_token = os.environ.get("SHOPIFY_ACCESS_TOKEN")
    api_version = os.environ.get("SHOPIFY_API_VERSION")
    url = f"https://{store_domain}/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json={"query": query, "variables": variables or {}}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"Shopify GraphQL errors: {data['errors']}")
    return data.get("data", {})


def fetch_order_metafields(order_ids, namespace="custom", key="order_type", batch_size=100):
    """
    Batch-fetch a specific metafield for a list of order IDs using GraphQL nodes query.

    Args:
        order_ids: List of Shopify order IDs (numeric, e.g. [1234567890, ...])
        namespace: Metafield namespace (default "custom")
        key: Metafield key (default "ORDER_TYPE")
        batch_size: How many orders to query per GraphQL call (max ~250)

    Returns:
        dict mapping order_id (str) -> metafield value (str or None)
    """
    result = {}

    for i in range(0, len(order_ids), batch_size):
        batch = order_ids[i:i + batch_size]
        gids = [f"gid://shopify/Order/{oid}" for oid in batch]

        query = """
        query GetOrderMetafields($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Order {
              id
              metafield(namespace: "%s", key: "%s") {
                value
              }
            }
          }
        }
        """ % (namespace, key)

        try:
            data = _graphql_request(query, {"ids": gids})
            nodes = data.get("nodes") or []
            for node in nodes:
                if not node:
                    continue
                gid = node.get("id", "")
                numeric_id = gid.split("/")[-1] if "/" in gid else gid
                mf = node.get("metafield")
                result[str(numeric_id)] = mf["value"] if mf else None
        except Exception as e:
            logger.error(f"Error fetching metafields for batch starting at index {i}: {e}")
            for oid in batch:
                result.setdefault(str(oid), None)

        if i + batch_size < len(order_ids):
            time.sleep(0.5)

    return result


def fetch_location_metafields_all(location_ids, batch_size=50):
    """
    Batch-fetch ALL metafields for a list of location IDs using GraphQL nodes query.

    Returns:
        dict mapping location_id (str) -> {"email": str|None, "metafields": {namespace.key: value}}
    """
    result = {}

    for i in range(0, len(location_ids), batch_size):
        batch = location_ids[i:i + batch_size]
        gids = [f"gid://shopify/Location/{lid}" for lid in batch]

        query = """
        query GetLocationMetafields($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Location {
              id
              metafields(first: 25) {
                edges {
                  node { namespace key value type }
                }
              }
            }
          }
        }
        """

        try:
            data = _graphql_request(query, {"ids": gids})
            nodes = data.get("nodes") or []
            for node in nodes:
                if not node:
                    continue
                gid = node.get("id", "")
                numeric_id = gid.split("/")[-1] if "/" in gid else gid
                mf_dict = {}
                email_val = None
                for edge in (node.get("metafields", {}).get("edges") or []):
                    mf = edge["node"]
                    full_key = f"{mf['namespace']}.{mf['key']}"
                    mf_dict[full_key] = mf["value"]
                    if mf["namespace"] == "custom" and mf["key"] == "email":
                        email_val = mf["value"]
                result[str(numeric_id)] = {"email": email_val, "metafields": mf_dict}
        except Exception as e:
            logger.error(f"Error fetching location metafields for batch starting at index {i}: {e}")
            for lid in batch:
                result.setdefault(str(lid), {"email": None, "metafields": {}})

        if i + batch_size < len(location_ids):
            time.sleep(0.5)

    return result


def get_daily_orders(start_date, end_date):
    """
    Get the orders from start_date to end_date from Shopify
    """
    store_domain = os.environ.get("SHOPIFY_STORE_DOMAIN")
    access_token = os.environ.get("SHOPIFY_ACCESS_TOKEN")
    api_version = os.environ.get("SHOPIFY_API_VERSION")
    url = f"https://{store_domain}/admin/api/{api_version}/orders.json?"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }
    params = {
        "status": "any",
        "updated_at_min": start_date,
        "updated_at_max": end_date,
        "limit": 250
        }
    
    orders = []
    while url:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break

        data = response.json()
        orders.extend(data.get('orders', []))

        # Handle pagination via response headers
        lien = response.headers.get('Link', '')
        url = None
        if lien:
            for part in lien.split(','):
                if 'rel="next"' in part:
                    url = part.split(';')[0].strip('<> ')
                    params = {}
                    break
    return orders