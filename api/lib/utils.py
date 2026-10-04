import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

def get_dates():
    """
    Get the dates for the day and the previous day in isoformat
    """
    tz = timezone(timedelta(hours=+2))
    today = datetime.now(tz)
    yesterday = today - timedelta(days=1)
    
    # Normaliser yesterday au début de l'heure (minute=0, second=0, microsecond=0)
    yesterday_start = yesterday.replace(minute=0, second=0, microsecond=0)
    
    # Ajouter 1 heure à today en utilisant timedelta pour éviter l'erreur hour > 23
    today_plus_one = today + timedelta(hours=1)
    today_plus_one_start = today_plus_one.replace(minute=0, second=0, microsecond=0)
    
    return yesterday_start.isoformat(), today_plus_one_start.isoformat()

def get_source_location(tags: List[str]) -> Optional[int]:
    """
    Get the source location from the tags.
    
    Searches for tags that start with 'STORE_' and extracts the location_id
    from the format: STORE_{location_name}_{location_id}
    
    Args:
        tags: List of tag strings
        
    Returns:
        location_id as int if found, None otherwise
        
    Example:
        get_source_location(['STORE_Office_14378139719']) -> 14378139719
    """
    for tag in tags:
        if tag.startswith("STORE_"):
            # Split by underscore and get the last part (location_id)
            parts = tag.split("_")
            if len(parts) >= 3:
                location_id = parts[-1]
                # Verify it's numeric (location_id should be numeric)
                if location_id.isdigit():
                    return int(location_id)
    return None

def get_store_context() -> Dict[str, str]:
    """
    Returns the multi-country context for DB inserts.
    Values are driven by environment variables with sensible defaults.
    """
    return {
        "data_source": os.getenv("DATA_SOURCE", "Shopify"),
        "company_code": os.getenv("COMPANY_CODE", "ADAM_LIPPES"),
        "commercial_organisation": os.getenv("COMMERCIAL_ORGANISATION", "US"),
    }

DEFAULT_CURRENCY_BY_ORG = {"US": "USD", "JP": "JPY", "UK": "GBP"}

def get_default_currency() -> str:
    """
    Devise de repli de la boutique courante (si Shopify ne renvoie pas de devise).
    SHOP_CURRENCY prime ; sinon déduite de COMMERCIAL_ORGANISATION ; sinon USD.
    """
    override = os.getenv("SHOP_CURRENCY")
    if override:
        return override.upper()
    org = get_store_context()["commercial_organisation"].upper()
    return DEFAULT_CURRENCY_BY_ORG.get(org, "USD")

def get_current_shop_domain() -> Optional[str]:
    """
    Domaine myshopify du store traité par ce process (format du header
    X-Shopify-Shop-Domain écrit dans la colonne `shop` des queues webhook).
    """
    return normalize_shop_domain(os.getenv("SHOPIFY_STORE_DOMAIN"))


def normalize_shop_domain(domain: Optional[str]) -> Optional[str]:
    if not domain:
        return None
    domain = domain.strip().lower()
    for prefix in ("https://", "http://"):
        if domain.startswith(prefix):
            domain = domain[len(prefix):]
    return domain.rstrip("/")


if __name__ == "__main__":
    print(get_dates())