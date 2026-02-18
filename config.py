# config.py
import os


class Config:
    # База данных
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.getenv('DB_PATH', os.path.join(BASE_DIR, 'school.db'))
    DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '5'))
    DB_MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '10'))

    # API настройки для синхронизации
    # SCHOOL_ID = int(os.getenv('SCHOOL_ID', '28'))
    # SYNC_INTERVAL = int(os.getenv('SYNC_INTERVAL', '3600'))  # 1 час

    # Другие настройки
    # SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'


config = Config()