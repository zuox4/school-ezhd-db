"""Utility functions."""
from datetime import datetime, timezone

def utc_now():
    """Текущее время в UTC с таймзоной."""
    return datetime.now(timezone.utc)

def utc_now_naive():
    """Наивное UTC время для БД."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

# Алиас для использования в моделях
get_db_time = utc_now_naive