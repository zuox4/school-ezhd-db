import re
from datetime import datetime, timezone


class DataNormalizer:
    """
    Класс для нормализации данных (телефоны, email, ФИО)
    """

    @staticmethod
    def now_utc():
        """Возвращает текущее время в UTC с таймзоной"""
        return datetime.now(timezone.utc)

    @staticmethod
    def normalize_phone(phone):
        """
        Приводит номер телефона к формату: 7XXXXXXXXXX (11 цифр)
        """
        if not phone or not isinstance(phone, str):
            return None

        # Оставляем только цифры
        digits = re.sub(r'\D', '', phone)

        # Обработка разных форматов
        if digits.startswith('8') and len(digits) == 11:
            digits = '7' + digits[1:]
        elif len(digits) == 10:
            digits = '7' + digits
        elif digits.startswith('7') and len(digits) == 11:
            pass  # Уже правильный формат
        else:
            return None

        return digits if len(digits) == 11 and digits.startswith('7') else None

    @staticmethod
    def normalize_email(email):
        """
        Приводит email к нижнему регистру и убирает пробелы
        """
        if not email or not isinstance(email, str):
            return None

        cleaned = email.strip().lower()
        return cleaned if '@' in cleaned else None

    @staticmethod
    def extract_name_parts(full_name):
        """
        Извлекает фамилию, имя и отчество из полного имени
        """
        if not full_name or not isinstance(full_name, str):
            return None, None, None

        parts = full_name.split()

        last_name = parts[0] if len(parts) > 0 else None
        first_name = parts[1] if len(parts) > 1 else None
        middle_name = parts[2] if len(parts) > 2 else None

        return last_name, first_name, middle_name

    @staticmethod
    def is_suspicious_name(name):
        """
        Проверяет, является ли имя подозрительным (тестовым/служебным)
        """
        if not name or not isinstance(name, str):
            return True

        # Паттерны подозрительных имен
        suspicious_patterns = [
            r'^Англ_\d+', r'^Нем_\d+', r'^Фр_\d+', r'^Мат_\d+', r'^Инф_\d+',
            r'^[A-Za-z]+_\d+', r'^\d+', r'^[А-Я]{3,5}$'
        ]

        for pattern in suspicious_patterns:
            if re.match(pattern, name):
                return True

        return False