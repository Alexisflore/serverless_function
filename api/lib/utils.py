from datetime import datetime, timedelta, timezone
from typing import List, Optional
import re

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

def get_source_location(tags: List[str]) -> Optional[str]:
    """
    Get the source location from the tags.
    
    Searches for tags that start with 'STORE_' and extracts the location_id
    from the format: STORE_{location_name}_{location_id}
    
    Args:
        tags: List of tag strings
        
    Returns:
        location_id as string if found, None otherwise
        
    Example:
        get_source_location(['STORE_Office_14378139719']) -> '14378139719'
    """
    for tag in tags:
        if tag.startswith("STORE_"):
            # Split by underscore and get the last part (location_id)
            parts = tag.split("_")
            if len(parts) >= 3:
                location_id = parts[-1]
                # Verify it's numeric (location_id should be numeric)
                if location_id.isdigit():
                    return location_id
    return None

if __name__ == "__main__":
    print(get_dates())