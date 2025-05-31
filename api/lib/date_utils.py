from datetime import datetime, timedelta, timezone

def get_dates():
    """
    Get the dates for the day and the previous day in isoformat
    """
    tz = timezone(timedelta(hours=+2))
    today = datetime.now(tz)
    # yesterday = today - timedelta(days=1)
    return today.replace(hour=today.hour - 1, minute=0, second=0, microsecond=0).isoformat(), today.replace(hour=today.hour + 1, minute=0, second=0, microsecond=0).isoformat()

if __name__ == "__main__":
    print(get_dates())