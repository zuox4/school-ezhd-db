# school_sync/backup.py
import os
import shutil
from datetime import datetime, timezone


# Импорт logger делаем ВНУТРИ методов, чтобы избежать циклического импорта

class DatabaseBackup:
    """
    Класс для создания бэкапов базы данных
    """

    def __init__(self, db_path='school.db', backup_dir='backups'):
        """
        Инициализация менеджера бэкапов

        Args:
            db_path (str): Путь к файлу базы данных
            backup_dir (str): Директория для хранения бэкапов
        """
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.logger = None  # Будет инициализирован при первом использовании

    def _get_logger(self):
        """Ленивая инициализация логгера"""
        if self.logger is None:
            from logger_config import logger
            self.logger = logger
        return self.logger

    @staticmethod
    def now_str():
        """Возвращает строку с текущим временем для имен файлов"""
        return datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    def create_backup(self, prefix='pre_sync'):
        """
        Создает бэкап базы данных с временной меткой

        Args:
            prefix (str): Префикс имени файла

        Returns:
            str: Путь к созданному бэкапу или None
        """
        logger = self._get_logger()

        # Создаем директорию для бэкапов, если её нет
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
            logger.info(f"Создана директория для бэкапов: {self.backup_dir}")

        if not os.path.exists(self.db_path):
            logger.warning(f"Файл базы данных {self.db_path} не найден")
            return None

        # Генерируем имя файла бэкапа
        timestamp = self.now_str()
        backup_filename = f"{prefix}_{timestamp}.db"
        backup_path = os.path.join(self.backup_dir, backup_filename)

        try:
            # Копируем файл
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"✅ Создан бэкап: {backup_filename}")

            # Очищаем старые бэкапы (оставляем последние 20)
            self.cleanup_old_backups(keep_last=20)

            return backup_path
        except Exception as e:
            logger.error(f"❌ Ошибка при создании бэкапа: {e}")
            return None

    def cleanup_old_backups(self, keep_last=20):
        """
        Удаляет старые бэкапы, оставляя только последние keep_last
        """
        logger = self._get_logger()

        try:
            # Получаем список всех бэкапов
            backups = [f for f in os.listdir(self.backup_dir)
                       if f.endswith('.db') and os.path.isfile(os.path.join(self.backup_dir, f))]

            # Сортируем по дате создания (от старых к новым)
            backups.sort()

            # Если бэкапов больше чем нужно, удаляем лишние
            if len(backups) > keep_last:
                to_delete = backups[:-keep_last]
                for backup in to_delete:
                    os.remove(os.path.join(self.backup_dir, backup))
                    logger.info(f"Удален старый бэкап: {backup}")
        except Exception as e:
            logger.error(f"Ошибка при очистке старых бэкапов: {e}")

    def restore_backup(self, backup_filename):
        """
        Восстанавливает базу данных из бэкапа

        Args:
            backup_filename (str): Имя файла бэкапа

        Returns:
            bool: True если восстановление успешно
        """
        logger = self._get_logger()

        backup_path = os.path.join(self.backup_dir, backup_filename)

        if not os.path.exists(backup_path):
            logger.error(f"Бэкап {backup_filename} не найден")
            return False

        try:
            # Создаем бэкап текущей базы перед восстановлением
            self.create_backup(prefix='pre_restore')

            # Восстанавливаем
            shutil.copy2(backup_path, self.db_path)
            logger.info(f"✅ База данных восстановлена из {backup_filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении: {e}")
            return False