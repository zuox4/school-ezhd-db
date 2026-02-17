# shared/utils.py
from datetime import datetime, timezone

def utc_now():
    """
    Возвращает текущее время в UTC с таймзоной для использования в Python
    (для сравнений, кэширования, логирования)
    """
    return datetime.now(timezone.utc)

def utc_now_naive():
    """
    Возвращает наивное UTC время (без таймзоны) для сохранения в БД.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)

# Алиас для использования в моделях
get_db_time = utc_now_naive