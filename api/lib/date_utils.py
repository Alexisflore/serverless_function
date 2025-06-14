from datetime import datetime, timedelta, timezone

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

if __name__ == "__main__":
    print(get_dates())