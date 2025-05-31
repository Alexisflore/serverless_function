import requests
import os

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
        lien = response.headers.get('Link')
        if lien and 'rel="next"' in lien:
            # print(lien.split(';')[0].strip('<>'))
            url = lien.split(';')[0].strip('<>')
            params = {}  # Parameters are already included in the next page URL
        else:
            url = None
    return orders