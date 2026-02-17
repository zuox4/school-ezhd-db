# school_sync/logger_config.py
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name, log_dir='logs'):
    """
    Настройка логгера с ротацией файлов
    """
    # Создаем директорию для логов, если её нет
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Формат логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Хендлер для файла с ротацией
    log_file = os.path.join(log_dir, f'{name}.log')
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=10, encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Хендлер для консоли
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Добавляем хендлеры
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Создаем основной логгер
logger = setup_logger('school_sync')