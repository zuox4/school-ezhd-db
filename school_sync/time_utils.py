# school_sync/time_utils.py
# Этот файл теперь просто реэкспортирует функции из shared.utils
from shared.utils import utc_now, utc_now_naive, get_db_time

# Для обратной совместимости
__all__ = ['utc_now', 'utc_now_naive', 'get_db_time']