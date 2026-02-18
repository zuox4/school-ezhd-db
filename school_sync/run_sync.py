"""
Модуль для синхронизации данных школы из API mos.ru
Версия: 2.1 - с улучшенной обработкой rate limiting MAX API
"""
# school_sync/run_sync.py
import os
import hashlib
import re

import requests
import traceback
import time
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from sqlalchemy import and_, or_

# СНАЧАЛА импортируем локальные модули, которые НЕ зависят от shared
from logger_config import logger
from backup import DatabaseBackup  # backup не должен импортировать school_sync
from time_utils import utc_now, utc_now_naive
from utils import DataNormalizer

# ПОТОМ импортируем из shared
from shared.models import Staff, ClassUnit, Student, Parent, class_staff, parent_student
from shared.database import get_session, init_database  # Добавить init_database



class CacheManager:
    """Простой менеджер кэша"""

    def __init__(self, cache_ttl=300):
        self.cache = {}
        self.cache_ttl = cache_ttl
        self.hits = 0
        self.misses = 0

    def get_cache_key(self, endpoint, params):
        """Генерирует ключ кэша"""
        key = f"{endpoint}:{str(params)}"
        return hashlib.md5(key.encode()).hexdigest()

    def get(self, key):
        """Получает значение из кэша"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            # ВАЖНО: timestamp уже наивное время (сохранено через set)
            # Используем utc_now_naive() для сравнения
            if (utc_now_naive() - timestamp).total_seconds() < self.cache_ttl:
                self.hits += 1
                return data
            else:
                del self.cache[key]

        self.misses += 1
        return None

    def set(self, key, value):
        """Сохраняет значение в кэш"""
        # ВАЖНО: сохраняем наивное время для совместимости с БД
        self.cache[key] = (value, utc_now_naive())

    def get_stats(self):
        """Статистика кэша"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': f"{hit_rate:.1f}%"
        }


class SchoolDataCollector:
    """
    Основной класс для сбора и синхронизации данных школы
    """

    def __init__(self, headers, school_id=28, db_path=None):
        """
        Инициализация коллектора данных

        Args:
            headers: Заголовки для HTTP запросов
            school_id: ID школы
            db_path: Путь к файлу базы данных (опционально)
        """
        if db_path is None:
            # Используем абсолютный путь относительно текущей директории
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.db_path = os.path.join(base_dir, 'school.db')
        else:
            self.db_path = db_path

        self.headers = headers
        self.school_id = school_id
        self.base_url = "https://school.mos.ru/api/ej/core/teacher/v1"

        # Инициализация компонентов
        self.normalizer = DataNormalizer()
        self.backup = DatabaseBackup(self.db_path)  # Передаём тот же путь
        self.cache = CacheManager(cache_ttl=300)

        # Добавляем счетчики для контроля запросов к MAX API
        self.max_api_calls = 0
        self.max_api_limit = 100  # Лимит запросов в минуту
        self.max_api_reset_time = time.time() + 60

        # Кэш для MAX данных
        self._max_data_cache = {}

        try:
            # ИНИЦИАЛИЗАЦИЯ БД ПО-НОВОМУ:
            # 1. Инициализируем базу данных с указанным URL
            self.engine = init_database()

            # 2. Получаем сессию (без аргументов, так как get_session больше не принимает engine)
            self.session = get_session()

            logger.info(f"Подключение к БД успешно: {self.db_path}")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise

    def _check_max_api_limit(self):
        """Проверяет и сбрасывает счетчик запросов к MAX API"""
        current_time = time.time()

        # Сбрасываем счетчик каждую минуту
        if current_time > self.max_api_reset_time:
            self.max_api_calls = 0
            self.max_api_reset_time = current_time + 60

        # Если приближаемся к лимиту, делаем паузу
        if self.max_api_calls >= self.max_api_limit - 10:
            sleep_time = self.max_api_reset_time - current_time
            if sleep_time > 0:
                logger.warning(f"⚠️ Близок к лимиту MAX API. Ожидание {sleep_time:.1f} секунд...")
                time.sleep(sleep_time)
                self.max_api_calls = 0
                self.max_api_reset_time = time.time() + 60

        self.max_api_calls += 1

    def _parse_max_user_id(self, html_text):
        """
        Парсит HTML страницы MAX для получения user.id

        Args:
            html_text (str): HTML страницы

        Returns:
            str: MAX user ID или None
        """
        try:
            # Ищем паттерн data:{user:{id:123456,
            pattern = r'data:\{user:\{id:(\d+),'
            match = re.search(pattern, html_text)
            if match:
                return match.group(1)

            # Альтернативный поиск через BeautifulSoup
            soup = BeautifulSoup(html_text, 'html.parser')
            scripts = soup.find_all('script')

            for script in scripts:
                if script.string and 'user:{id:' in script.string:
                    match = re.search(r'user:\{id:(\d+),', script.string)
                    if match:
                        return match.group(1)

            return None
        except Exception as e:
            logger.debug(f"Ошибка парсинга MAX user.id: {e}")
            return None

    def get_max_data(self, person_id=None, staff_id=None, max_retries=3):
        """
        Получает MAX ID и ссылку для пользователя с обработкой ограничений по запросам

        Args:
            person_id: ID ученика или родителя
            staff_id: ID сотрудника
            max_retries: Максимальное количество повторных попыток

        Returns:
            dict: {'max_id': str, 'max_link': str} или None
        """
        # Проверяем лимиты запросов
        self._check_max_api_limit()

        # Формируем URL с параметрами
        if staff_id:
            url = f"https://school.mos.ru/v2/external-partners/check-for-max-user?staff_id={staff_id}"
            id_type = "staff"
            id_value = staff_id
        elif person_id:
            url = f"https://school.mos.ru/v2/external-partners/check-for-max-user?person_id={person_id}"
            id_type = "person"
            id_value = person_id
        else:
            logger.error("Не указан ни person_id, ни staff_id")
            return None

        # Проверяем кэш перед запросом
        cache_key = f"max_data_{id_type}_{id_value}"
        if cache_key in self._max_data_cache:
            cached = self._max_data_cache[cache_key]
            logger.debug(f"✅ MAX data cache HIT for {id_type}:{id_value}")
            return cached

        logger.debug(f"Запрос к MAX API для {id_type}: {id_value}")

        retry_count = 0
        base_delay = 30  # Базовая задержка в секундах

        while retry_count < max_retries:
            try:
                # Первый запрос к API mos.ru
                response = requests.get(url, headers=self.headers, timeout=10)

                # Обработка rate limiting
                if response.status_code == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', base_delay))
                    logger.warning(f"⚠️ Rate limit для MAX API. Ожидание {retry_after} секунд...")
                    time.sleep(retry_after)
                    retry_count += 1
                    continue

                if response.status_code != 200:
                    logger.debug(f"MAX ID не найден для {url}: {response.status_code}")
                    return None

                data = response.json()
                if not data or 'max_link' not in data:
                    return None

                max_link = data['max_link']

                # Добавляем задержку между запросами
                time.sleep(2)  # Увеличиваем задержку

                # Второй запрос к MAX для получения HTML
                try:
                    html_response = requests.get(
                        max_link,
                        timeout=10,
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                    )

                    if html_response.status_code == 200:
                        max_user_id = self._parse_max_user_id(html_response.text)

                        result = {
                            'max_id': max_user_id,
                            'max_link': max_link
                        }

                        # Сохраняем в кэш
                        self._max_data_cache[cache_key] = result

                        if max_user_id:
                            logger.debug(f"✅ Найден MAX user.id: {max_user_id} for {id_type}:{id_value}")
                        else:
                            logger.debug(f"⚠️ MAX user.id не найден в HTML для {id_type}:{id_value}")
                        print(result)
                        return result

                    elif html_response.status_code == 429:
                        # Rate limit от MAX
                        retry_after = int(html_response.headers.get('Retry-After', base_delay))
                        logger.warning(f"⚠️ Rate limit от MAX. Ожидание {retry_after} секунд...")
                        time.sleep(retry_after)
                        retry_count += 1
                        continue
                    else:
                        logger.debug(f"MAX HTML вернул код {html_response.status_code} для {id_type}:{id_value}")

                except requests.exceptions.RequestException as e:
                    logger.debug(f"Ошибка при запросе к MAX: {e}")

                # Если не удалось получить HTML, возвращаем None
                return None

            except requests.exceptions.RequestException as e:
                logger.debug(f"Ошибка сети при получении MAX ID: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    sleep_time = base_delay * retry_count
                    logger.debug(f"Повторная попытка через {sleep_time} секунд...")
                    time.sleep(sleep_time)

            except Exception as e:
                logger.debug(f"Ошибка при парсинге MAX ID: {e}")
                return None

        logger.warning(f"❌ Не удалось получить MAX данные после {max_retries} попыток для {id_type}:{id_value}")
        return None

    def batch_get_max_data(self, items, id_field='staff_id'):
        """
        Пакетное получение MAX ID для множества элементов

        Args:
            items: список словарей с ID
            id_field: название поля с ID ('staff_id' или 'person_id')

        Returns:
            dict: {id: max_data}
        """
        results = {}
        total_items = len(items)

        logger.info(f"📦 Пакетное получение MAX ID для {total_items} элементов")

        for i, item in enumerate(items):
            item_id = item['id']

            # Прогресс
            if (i + 1) % 10 == 0:
                logger.info(f"  Прогресс: {i + 1}/{total_items} ({((i + 1)/total_items*100):.1f}%)")

            if id_field == 'staff_id':
                max_data = self.get_max_data(staff_id=item_id, max_retries=2)
            else:
                max_data = self.get_max_data(person_id=item_id, max_retries=2)

            results[item_id] = max_data

            # Добавляем переменную задержку между запросами
            if (i + 1) % 5 == 0:  # Каждые 5 запросов делаем паузу
                sleep_time = 10  # Увеличиваем паузу
                logger.debug(f"⏸️ Пауза после {i+1} запросов MAX API на {sleep_time} секунд")
                time.sleep(sleep_time)
            else:
                # Небольшая задержка между запросами
                time.sleep(2)

        logger.info(f"✅ Пакетное получение MAX ID завершено")
        return results

    def _api_request(self, endpoint, params=None):
        """
        Выполняет запрос к API с обработкой ошибок
        """
        url = f"{self.base_url}/{endpoint}"

        # Проверка кэша
        cache_key = self.cache.get_cache_key(endpoint, params)
        cached = self.cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Cache HIT for {endpoint}")
            return cached

        try:
            logger.debug(f"API запрос: {url}, params: {params}")
            response = requests.get(url, params=params, headers=self.headers, timeout=30)

            if response.status_code != 200:
                logger.error(f"Ошибка API {response.status_code}: {url}")
                return None

            data = response.json()

            # Сохраняем в кэш
            self.cache.set(cache_key, data)

            logger.debug(f"Получен ответ: {len(data) if isinstance(data, list) else 'object'}")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка сети при запросе {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при запросе {url}: {e}")
            return None

    # ==================== РАБОТА С ПЕРСОНАЛОМ ====================

    def save_staff_from_api(self, staff_data):
        """
        Сохраняет или обновляет сотрудника из API

        Args:
            staff_data (dict): Данные сотрудника из API

        Returns:
            Staff: Объект сотрудника или None
        """
        if not isinstance(staff_data, dict):
            logger.warning(f"Передан не словарь: {type(staff_data)}")
            return None

        staff_id = staff_data.get('id')
        if not staff_id:
            logger.warning("Нет ID в данных сотрудника")
            return None

        # Проверяем наличие user_id
        user_id = staff_data.get('user_id')
        if not user_id:
            logger.debug(f"Сотрудник ID {staff_id} без user_id, пропускаем")
            return None

        # Получаем данные пользователя
        user_data = staff_data.get('user', {})
        if not user_data:
            logger.warning(f"Нет данных user для сотрудника {staff_id}")

        # Извлекаем и нормализуем данные
        last_name = user_data.get('last_name')
        first_name = user_data.get('first_name')
        middle_name = user_data.get('middle_name')

        full_name = staff_data.get('name', '')

        if not last_name and full_name:
            last_name, first_name, middle_name = self.normalizer.extract_name_parts(full_name)

        # Нормализуем контакты
        phone = self.normalizer.normalize_phone(user_data.get('phone_number'))
        email = self.normalizer.normalize_email(user_data.get('email'))

        if not email:
            email = self.normalizer.normalize_email(user_data.get('email_ezd'))

        # Парсим дату из API
        api_updated_at = None
        api_date_str = staff_data.get('updated_at')
        if api_date_str and isinstance(api_date_str, str):
            try:
                # Пробуем разные форматы
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        api_updated_at = datetime.strptime(api_date_str, fmt)
                        break
                    except ValueError:
                        continue
            except Exception as e:
                logger.debug(f"Не удалось распарсить дату {api_date_str}: {e}")

        # Получаем MAX ID с обработкой ошибок
        try:
            user_integration_id = staff_data.get('user_integration_id')
            if user_integration_id:
                max_data = self.get_max_data(staff_id=user_integration_id, max_retries=2)
                max_id = max_data.get('max_id') if max_data else None
                max_link_path = max_data.get('max_link') if max_data else None
            else:
                max_id = None
                max_link_path = None
        except Exception as e:
            max_id = None
            max_link_path = None
            logger.debug(f"Не удалось получить max_id для сотрудника {staff_id}: {e}")

        # Поиск в БД
        try:
            staff = self.session.query(Staff).filter_by(person_id=staff_id).first()
        except Exception as e:
            logger.error(f"Ошибка при поиске сотрудника {staff_id}: {e}")
            return None

        current_time = utc_now_naive()

        try:
            if not staff:
                # Новый сотрудник
                staff = Staff(
                    person_id=staff_id,
                    user_id=user_id,
                    name=full_name,
                    last_name=last_name,
                    first_name=first_name,
                    middle_name=middle_name,
                    email=email,
                    max_link_path=max_link_path,
                    phone=phone,
                    type=staff_data.get('type'),
                    updated_at_api=api_updated_at,  # Теперь это datetime или None
                    is_active=True,
                    last_seen_at=current_time,
                    created_at=current_time,
                    updated_at=current_time,
                    max_user_id=max_id
                )
                self.session.add(staff)
                logger.info(f"✅ Добавлен сотрудник: {full_name or staff_id} (user_id: {user_id})")
            else:
                # Проверяем изменения
                changes = []
                if staff.updated_at_api != api_updated_at:
                    changes.append("дата обновления")
                if staff.user_id != user_id:
                    changes.append("user_id")
                if (staff.last_name, staff.first_name, staff.middle_name) != (last_name, first_name, middle_name):
                    changes.append("ФИО")
                if staff.email != email:
                    changes.append("email")
                if staff.phone != phone:
                    changes.append("телефон")
                if staff.max_user_id != max_id:
                    changes.append("макс")
                if staff.type != staff_data.get('type'):
                    changes.append("тип")

                # Обновляем данные
                staff.user_id = user_id
                staff.name = full_name or staff.name
                staff.last_name = last_name or staff.last_name
                staff.first_name = first_name or staff.first_name
                staff.middle_name = middle_name or staff.middle_name
                staff.email = email or staff.email
                staff.phone = phone or staff.phone
                staff.type = staff_data.get('type', staff.type)
                staff.updated_at_api = api_updated_at or staff.updated_at_api
                staff.is_active = True
                staff.last_seen_at = current_time
                staff.deactivated_at = None
                staff.max_user_id = max_id
                staff.max_link_path = max_link_path
                staff.updated_at = current_time

                if changes:
                    logger.info(f"🔄 Обновлен сотрудник {full_name or staff.name}: {', '.join(changes)}")
                else:
                    logger.debug(f"⏺ Сотрудник {full_name or staff.name} - без изменений")

            return staff

        except Exception as e:
            logger.error(f"Ошибка при сохранении сотрудника {staff_id}: {e}")
            return None

    def bulk_save_staff(self, staff_data_list):
        """
        Пакетное сохранение сотрудников для ускорения
        """
        if not staff_data_list:
            return

        # Собираем все ID для пакетного поиска
        all_ids = [s.get('id') for s in staff_data_list if s.get('id')]

        # Один запрос вместо множества
        existing_staff = {
            s.person_id: s
            for s in self.session.query(Staff).filter(Staff.person_id.in_(all_ids))
        }

        new_staff = []
        update_count = 0

        for staff_data in staff_data_list:
            staff_id = staff_data.get('id')
            if not staff_id or not staff_data.get('user_id'):
                continue

            if staff_id in existing_staff:
                # Обновляем существующего
                staff = existing_staff[staff_id]
                self._update_staff_object(staff, staff_data)
                update_count += 1
            else:
                # Создаем нового
                staff = self._create_staff_object(staff_data)
                if staff:
                    new_staff.append(staff)

        # Пакетное добавление
        if new_staff:
            self.session.add_all(new_staff)

        self.session.flush()
        logger.info(f"Пакетная обработка: {len(new_staff)} новых, {update_count} обновлено")

    def _create_staff_object(self, staff_data):
        """Создает объект Staff из данных"""
        try:
            user_data = staff_data.get('user', {})
            full_name = staff_data.get('name', '')
            last_name, first_name, middle_name = self.normalizer.extract_name_parts(full_name)

            return Staff(
                person_id=staff_data['id'],
                user_id=staff_data['user_id'],
                name=full_name,
                last_name=last_name,
                first_name=first_name,
                middle_name=middle_name,
                email=self.normalizer.normalize_email(user_data.get('email')),
                phone=self.normalizer.normalize_phone(user_data.get('phone_number')),
                type=staff_data.get('type'),
                updated_at_api=staff_data.get('updated_at'),
                is_active=True,
                last_seen_at=utc_now_naive()  # ДЛЯ БД нужно наивное время
            )
        except Exception as e:
            logger.error(f"Ошибка создания объекта Staff: {e}")
            return None

    def _update_staff_object(self, staff, staff_data):
        """Обновляет существующий объект Staff"""
        user_data = staff_data.get('user', {})
        staff.last_seen_at = utc_now_naive()
        staff.is_active = True
        staff.deactivated_at = None

        # Обновляем только если изменилось
        if staff.updated_at_api != staff_data.get('updated_at'):
            staff.updated_at_api = staff_data.get('updated_at')

    def sync_all_staff(self):
        """
        Полная синхронизация всех сотрудников школы

        Returns:
            dict: Статистика синхронизации
        """
        logger.info("=" * 70)
        logger.info("👥 НАЧАЛО СИНХРОНИЗАЦИИ ПЕРСОНАЛА")
        logger.info("=" * 70)

        stats = {
            'api_ids': set(),
            'saved_ids': set(),
            'total_loaded': 0,
            'no_user_id': 0,
            'errors': 0,
            'duplicates': 0
        }

        page = 1
        page_processed_ids = set()
        max_api_retries = 3

        while True:
            logger.info(f"Загрузка страницы {page}...")

            # Добавляем повторные попытки при ошибках API
            for attempt in range(max_api_retries):
                data = self._api_request('teacher_profiles', {
                    'school_id': self.school_id,
                    'page': page
                })

                if data is not None:
                    break

                if attempt < max_api_retries - 1:
                    wait_time = 10 * (attempt + 1)
                    logger.warning(f"Ошибка загрузки страницы {page}, попытка {attempt + 2} через {wait_time}с")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Не удалось загрузить страницу {page} после {max_api_retries} попыток")
                    data = None

            if not data:
                break

            if not isinstance(data, list):
                logger.warning(f"Страница {page} вернула не список: {type(data)}")
                break

            logger.info(f"📊 Страница {page}: загружено {len(data)} записей")

            # Собираем ID для статистики
            page_ids = set()
            for item in data:
                if isinstance(item, dict):
                    api_id = item.get('id')
                    if api_id:
                        page_ids.add(api_id)
                        stats['api_ids'].add(api_id)

            # Проверка дубликатов
            if len(page_ids) < len(data):
                dup_count = len(data) - len(page_ids)
                stats['duplicates'] += dup_count
                logger.warning(f"⚠️ Найдено {dup_count} дубликатов ID на странице {page}")

            # Обработка записей
            page_success = 0
            page_no_user = 0
            page_errors = 0

            for idx, staff_data in enumerate(data, 1):
                try:
                    if not isinstance(staff_data, dict):
                        logger.warning(f"Запись {idx} не является словарем")
                        continue

                    staff_id = staff_data.get('id')
                    if not staff_id:
                        logger.warning(f"Запись {idx} без ID")
                        continue

                    # Проверка наличия user_id
                    if not staff_data.get('user_id'):
                        page_no_user += 1
                        stats['no_user_id'] += 1
                        continue

                    # Проверка дубликата на странице
                    if staff_id in page_processed_ids:
                        stats['duplicates'] += 1
                        logger.warning(f"Дубликат ID {staff_id} на странице {page}")
                        continue

                    # Проверка имени
                    if self.normalizer.is_suspicious_name(staff_data.get('name')):
                        logger.debug(f"Пропущен сотрудник с подозрительным именем: {staff_data.get('name')}")
                        continue

                    # Сохранение
                    staff = self.save_staff_from_api(staff_data)

                    # Увеличиваем задержку между сохранениями
                    time.sleep(1)

                    if staff:
                        stats['saved_ids'].add(staff.person_id)
                        page_processed_ids.add(staff_id)
                        stats['total_loaded'] += 1
                        page_success += 1

                except Exception as e:
                    page_errors += 1
                    stats['errors'] += 1
                    logger.error(f"Ошибка при обработке записи: {e}")
                    self.session.rollback()

            # Коммит страницы
            if page_success > 0:
                try:
                    self.session.commit()
                    logger.info(f"✅ Страница {page}: сохранено {page_success}, без user_id: {page_no_user}, ошибок: {page_errors}")
                except Exception as e:
                    logger.error(f"Ошибка при коммите страницы {page}: {e}")
                    self.session.rollback()
            else:
                logger.warning(f"⚠️ Страница {page}: нет успешных записей (без user_id: {page_no_user})")

            page_processed_ids.clear()

            # Проверка последней страницы
            if len(data) < 10:
                logger.info(f"📄 Страница {page} - последняя")
                break

            page += 1
            time.sleep(1)  # Увеличиваем задержку между страницами

        # Деактивация отсутствующих
        deactivated = self.deactivate_missing_staff(stats['saved_ids'])

        # Очистка записей без user_id
        cleaned = self.clean_staff_without_user_id()

        # Финальный коммит
        try:
            self.session.commit()
        except Exception as e:
            logger.error(f"Ошибка при финальном коммите: {e}")
            self.session.rollback()

        # Итоговая статистика
        logger.info("=" * 70)
        logger.info("📊 ИТОГИ СИНХРОНИЗАЦИИ ПЕРСОНАЛА")
        logger.info("=" * 70)
        logger.info(f"Уникальных ID в API: {len(stats['api_ids'])}")
        logger.info(f"Сохранено в БД: {stats['total_loaded']}")
        logger.info(f"Пропущено (без user_id): {stats['no_user_id']}")
        logger.info(f"Деактивировано (отсутствуют): {deactivated}")
        logger.info(f"Очищено (без user_id): {cleaned}")
        logger.info(f"Ошибок: {stats['errors']}")
        logger.info(f"Дубликатов: {stats['duplicates']}")

        # Статистика кэша MAX API
        logger.info(f"MAX API кэш: {len(self._max_data_cache)} записей")

        return stats

    def deactivate_missing_staff(self, active_ids):
        """
        Деактивирует сотрудников, которых нет в активном списке

        Args:
            active_ids (set): Множество активных ID

        Returns:
            int: Количество деактивированных
        """
        if not active_ids:
            return 0

        current_time = utc_now_naive()

        deactivated = self.session.query(Staff).filter(
            and_(
                Staff.is_active == True,
                Staff.person_id.notin_(active_ids)
            )
        ).update({
            'is_active': False,
            'deactivated_at': current_time,
            'updated_at': current_time
        }, synchronize_session=False)

        if deactivated > 0:
            logger.info(f"🔴 Деактивировано сотрудников (отсутствуют в API): {deactivated}")

            # Покажем примеры
            examples = self.session.query(Staff).filter(
                Staff.deactivated_at == current_time
            ).limit(3).all()

            for staff in examples:
                logger.info(f"   • {staff.name} (ID: {staff.person_id})")

        return deactivated

    def clean_staff_without_user_id(self):
        """
        Деактивирует сотрудников без user_id

        Returns:
            int: Количество деактивированных
        """
        logger.info("🧹 ОЧИСТКА СОТРУДНИКОВ БЕЗ USER_ID")

        current_time = utc_now_naive()

        # Находим активных без user_id
        staff_list = self.session.query(Staff).filter(
            and_(
                Staff.is_active == True,
                Staff.user_id.is_(None)
            )
        ).all()

        if not staff_list:
            logger.info("✅ Нет активных сотрудников без user_id")
            return 0

        logger.info(f"Найдено активных без user_id: {len(staff_list)}")

        for staff in staff_list[:5]:
            logger.info(f"   • {staff.name or 'Без имени'} (ID: {staff.person_id})")

        if len(staff_list) > 5:
            logger.info(f"   • ... и еще {len(staff_list) - 5}")

        # Деактивируем
        deactivated = self.session.query(Staff).filter(
            and_(
                Staff.is_active == True,
                Staff.user_id.is_(None)
            )
        ).update({
            'is_active': False,
            'deactivated_at': current_time,
            'updated_at': current_time
        }, synchronize_session=False)

        logger.info(f"🔴 Деактивировано: {deactivated}")
        return deactivated

    # ==================== РАБОТА С КЛАССАМИ ====================

    def save_class_units(self, class_units_data):
        """
        Сохраняет классы в БД

        Args:
            class_units_data (list): Список данных классов
        """
        logger.info("📚 ОБРАБОТКА КЛАССОВ")
        logger.info("=" * 70)

        # Если получили список ID, преобразуем
        if class_units_data and isinstance(class_units_data[0], (int, str)):
            logger.info("Получен список ID классов")
            class_units_data = [{'id': int(cid), 'name': f'Class_{cid}'} for cid in class_units_data]

        for unit_data in class_units_data:
            if not isinstance(unit_data, dict):
                continue

            class_id = unit_data.get('id')
            if not class_id:
                continue

            name = unit_data.get('name', f'Class_{class_id}')

            # Парсинг названия класса
            parallel = None
            literal = None
            if isinstance(name, str) and '-' in name:
                parts = name.split('-')
                parallel = parts[0]
                literal = parts[1] if len(parts) > 1 else None

            class_unit = self.session.query(ClassUnit).filter_by(id=class_id).first()

            if not class_unit:
                class_unit = ClassUnit(
                    id=class_id,
                    school_id=unit_data.get('school_id'),
                    class_level_id=unit_data.get('class_level_id'),
                    name=name,
                    parallel=parallel,
                    literal=literal
                )
                self.session.add(class_unit)
                logger.info(f"✅ Добавлен класс: {name}")
            else:
                changes = []
                if class_unit.name != name:
                    changes.append(f"{class_unit.name} -> {name}")

                class_unit.name = name
                class_unit.school_id = unit_data.get('school_id', class_unit.school_id)
                class_unit.class_level_id = unit_data.get('class_level_id', class_unit.class_level_id)
                class_unit.parallel = parallel
                class_unit.literal = literal
                class_unit.updated_at = utc_now_naive()

                if changes:
                    logger.info(f"🔄 Обновлен класс {name}: {', '.join(changes)}")

            # Связи с персоналом
            staff_ids = unit_data.get('mentor_ids', [])
            if staff_ids:
                class_unit.staff = []
                for staff_id in staff_ids:
                    staff = self.session.query(Staff).filter_by(person_id=staff_id, is_active=True).first()
                    if staff:
                        class_unit.staff.append(staff)
                        logger.debug(f"   🔗 Связан {staff.name} с классом {name}")
                    else:
                        logger.debug(f"   ⚠️ Сотрудник {staff_id} не найден")

            self.session.flush()

        self.session.commit()
        logger.info(f"✅ Обработано классов: {len(class_units_data)}")

    # ==================== РАБОТА С УЧЕНИКАМИ И РОДИТЕЛЯМИ ====================

    def save_student_data(self, student_data, class_unit_id):
        """
        Сохраняет данные ученика

        Args:
            student_data (dict): Данные ученика
            class_unit_id (int): ID класса

        Returns:
            tuple: (Student, action)
        """
        if not isinstance(student_data, dict):
            return None, "Пропущен"

        student_id = student_data.get('person_id')
        if not student_id:
            return None, "Пропущен"

        # Получаем MAX ID
        try:
            person_id = student_data.get('person_id')
            if person_id:
                max_data = self.get_max_data(person_id=person_id, max_retries=2)
                max_id = max_data.get('max_id') if max_data else None
                max_link_path = max_data.get('max_link') if max_data else None

            else:
                max_id = None
                max_link_path = None
        except Exception as e:
            logger.debug(f"Не удалось получить max_id для ученика {student_id}: {e}")
            max_id = None
            max_link_path = None

        # Нормализация контактов
        phone = self.normalizer.normalize_phone(student_data.get('phone_number'))
        email = self.normalizer.normalize_email(student_data.get('email_ezd'))

        student = self.session.query(Student).filter_by(person_id=student_id).first()

        if not student:
            student = Student(
                person_id=student_id,
                user_name=student_data.get('user_name'),
                last_name=student_data.get('last_name', ''),
                first_name=student_data.get('first_name', ''),
                middle_name=student_data.get('middle_name'),
                email=email,
                phone=phone,
                class_unit_id=class_unit_id,
                max_user_id=max_id,
                max_link_path=max_link_path,
                is_active=True
            )
            self.session.add(student)
            action = "Добавлен"
            logger.debug(f"   ✅ Ученик {student.last_name}: {action}")
        else:
            # Обновление
            old_data = (student.last_name, student.first_name, student.email, student.phone)
            new_data = (
                student_data.get('last_name', student.last_name),
                student_data.get('first_name', student.first_name),
                email or student.email,
                phone or student.phone
            )

            student.user_name = student_data.get('user_name', student.user_name)
            student.last_name = student_data.get('last_name', student.last_name)
            student.first_name = student_data.get('first_name', student.first_name)
            student.middle_name = student_data.get('middle_name', student.middle_name)
            student.email = email or student.email
            student.phone = phone or student.phone
            student.class_unit_id = class_unit_id
            student.is_active = True
            student.deactivated_at = None
            student.max_user_id = max_id
            student.max_link_path = max_link_path
            student.updated_at = utc_now_naive()

            if old_data != new_data:
                action = "Обновлен"
                logger.debug(f"   🔄 Ученик {student.last_name}: {action}")
            else:
                action = "Без изменений"

        self.session.flush()

        # Обработка родителей
        parents_data = student_data.get('parents', [])
        if parents_data:
            for parent_data in parents_data:
                parent, _ = self.save_parent_data(parent_data)
                if parent and self.link_parent_to_student(parent, student):
                    logger.debug(f"      🔗 Связан родитель {parent.name}")

        return student, action

    def save_parent_data(self, parent_data):
        """
        Сохраняет данные родителя

        Args:
            parent_data (dict): Данные родителя

        Returns:
            tuple: (Parent, action)
        """
        if not isinstance(parent_data, dict):
            return None, "Пропущен"

        parent_id = parent_data.get('person_id')
        if not parent_id:
            return None, "Пропущен"

        # Нормализация
        phone = self.normalizer.normalize_phone(parent_data.get('phone_number'))
        email = self.normalizer.normalize_email(parent_data.get('email'))
        full_name = parent_data.get('name', '')
        last_name, first_name, middle_name = self.normalizer.extract_name_parts(full_name)

        parent = self.session.query(Parent).filter_by(person_id=parent_id).first()

        # Получаем MAX ID
        try:
            person_id = parent_data.get('person_id')
            if person_id:
                max_data = self.get_max_data(person_id=person_id, max_retries=2)
                max_id = max_data.get('max_id') if max_data else None
                max_link_path = max_data.get('max_link') if max_data else None

            else:
                max_id = None
                max_link_path = None
        except Exception as e:
            logger.debug(f"Не удалось получить max_id для родителя {parent_id}: {e}")
            max_id = None
            max_link_path = None

        if not parent:
            parent = Parent(
                person_id=parent_id,
                name=full_name,
                last_name=last_name,
                first_name=first_name,
                middle_name=middle_name,
                email=email,
                phone=phone,
                max_user_id=max_id,
                max_link_path=max_link_path,
                is_active=True
            )
            self.session.add(parent)
            action = "Добавлен"
        else:
            parent.name = full_name or parent.name
            parent.last_name = last_name or parent.last_name
            parent.first_name = first_name or parent.first_name
            parent.middle_name = middle_name or parent.middle_name
            parent.email = email or parent.email
            parent.phone = phone or parent.phone
            parent.is_active = True
            parent.deactivated_at = None
            parent.max_user_id = max_id
            parent.max_link_path = max_link_path
            parent.updated_at = utc_now_naive()
            action = "Обновлен"

        self.session.flush()
        return parent, action

    def link_parent_to_student(self, parent, student):
        """
        Связывает родителя с учеником

        Returns:
            bool: True если связь создана
        """
        if not parent or not student:
            return False

        existing = self.session.execute(
            parent_student.select().where(
                and_(
                    parent_student.c.parent_id == parent.id,
                    parent_student.c.student_id == student.id
                )
            )
        ).first()

        if not existing:
            self.session.execute(
                parent_student.insert().values(
                    parent_id=parent.id,
                    student_id=student.id,
                    created_at=utc_now_naive()
                )
            )
            return True
        return False

    def process_class_unit(self, unit_id):
        """
        Обрабатывает один класс (учеников и родителей)

        Args:
            unit_id (int): ID класса
        """
        logger.info(f"📊 Обработка класса ID: {unit_id}")

        # Добавляем повторные попытки при ошибках
        max_retries = 3
        data = None

        for attempt in range(max_retries):
            data = self._api_request('student_profiles', {
                "page": "1",
                "class_unit_ids": str(unit_id),
                "with_deleted": "false",
                "with_parents": "true",
                "with_user_info": "true"
            })

            if data is not None:
                break

            if attempt < max_retries - 1:
                wait_time = 10 * (attempt + 1)
                logger.warning(f"Ошибка загрузки класса {unit_id}, попытка {attempt + 2} через {wait_time}с")
                time.sleep(wait_time)

        if not data:
            logger.error(f"Не удалось загрузить данные класса {unit_id}")
            return

        if not isinstance(data, list):
            logger.warning(f"Получены некорректные данные для класса {unit_id}")
            return

        logger.info(f"   Получено учеников: {len(data)}")

        # Получаем ID учеников из API
        current_ids = []
        for student in data:
            if isinstance(student, dict):
                student_id = student.get('person_id')
                if student_id:
                    current_ids.append(student_id)

        # Обработка учеников
        students_count = 0
        for idx, student_data in enumerate(data):
            if not isinstance(student_data, dict):
                continue

            student, _ = self.save_student_data(student_data, unit_id)
            if student:
                students_count += 1

            # Добавляем задержку между обработкой учеников
            if (idx + 1) % 10 == 0:
                time.sleep(2)

        # Деактивация отсутствующих
        if current_ids:
            deactivated = self.session.query(Student).filter(
                and_(
                    Student.class_unit_id == unit_id,
                    Student.is_active == True,
                    Student.person_id.notin_(current_ids)
                )
            ).update({
                "is_active": False,
                "deactivated_at": utc_now_naive()
            })

            if deactivated:
                logger.info(f"   🔴 Деактивировано учеников: {deactivated}")

        self.session.commit()
        logger.info(f"✅ Класс обработан: {students_count} учеников")

    # ==================== СТАТИСТИКА И ПОИСК ====================

    def get_staff_statistics(self):
        """
        Получает статистику по персоналу

        Returns:
            dict: Статистика
        """
        total = self.session.query(Staff).count()
        active = self.session.query(Staff).filter_by(is_active=True).count()
        deactivated = total - active

        # По типам
        types = {}
        for staff in self.session.query(Staff).filter_by(is_active=True):
            if staff.type:
                types[staff.type] = types.get(staff.type, 0) + 1

        # По контактам
        with_phone = self.session.query(Staff).filter(
            Staff.phone.isnot(None), Staff.is_active == True
        ).count()
        with_email = self.session.query(Staff).filter(
            Staff.email.isnot(None), Staff.is_active == True
        ).count()

        # MAX ID статистика
        with_max_id = self.session.query(Staff).filter(
            Staff.max_user_id.isnot(None), Staff.is_active == True
        ).count()

        return {
            'total': total,
            'active': active,
            'deactivated': deactivated,
            'by_type': types,
            'with_phone': with_phone,
            'with_email': with_email,
            'with_max_id': with_max_id
        }

    def print_staff_statistics(self):
        """Выводит статистику по персоналу"""
        stats = self.get_staff_statistics()

        logger.info("=" * 70)
        logger.info("👥 СТАТИСТИКА ПЕРСОНАЛА")
        logger.info("=" * 70)
        logger.info(f"Всего: {stats['total']}")
        logger.info(f"✅ Активных: {stats['active']}")
        logger.info(f"🔴 Деактивированных: {stats['deactivated']}")
        logger.info(f"📞 С телефоном: {stats['with_phone']}")
        logger.info(f"📧 С email: {stats['with_email']}")
        logger.info(f"🆔 С MAX ID: {stats['with_max_id']}")

        if stats['by_type']:
            logger.info("\n📋 По типам:")
            for t, count in sorted(stats['by_type'].items()):
                logger.info(f"   • {t}: {count}")

    def get_statistics(self):
        """
        Общая статистика по БД

        Returns:
            dict: Статистика
        """
        return {
            'classes': self.session.query(ClassUnit).count(),
            'students_active': self.session.query(Student).filter_by(is_active=True).count(),
            'students_total': self.session.query(Student).count(),
            'parents_active': self.session.query(Parent).filter_by(is_active=True).count(),
            'parents_total': self.session.query(Parent).count(),
            'staff_active': self.session.query(Staff).filter_by(is_active=True).count(),
            'staff_total': self.session.query(Staff).count()
        }

    def print_statistics(self):
        """Выводит общую статистику"""
        stats = self.get_statistics()

        logger.info("=" * 70)
        logger.info("📊 ОБЩАЯ СТАТИСТИКА")
        logger.info("=" * 70)
        logger.info(f"Классов: {stats['classes']}")
        logger.info(f"\nУченики:")
        logger.info(f"   ✅ Активных: {stats['students_active']}")
        logger.info(f"   💾 Всего: {stats['students_total']}")
        logger.info(f"\nРодители:")
        logger.info(f"   ✅ Активных: {stats['parents_active']}")
        logger.info(f"   💾 Всего: {stats['parents_total']}")
        logger.info(f"\nСотрудники:")
        logger.info(f"   ✅ Активных: {stats['staff_active']}")
        logger.info(f"   💾 Всего: {stats['staff_total']}")

    def find_staff_by_name(self, search_term):
        """
        Поиск сотрудников по имени

        Args:
            search_term (str): Строка поиска

        Returns:
            list: Список сотрудников
        """
        return self.session.query(Staff).filter(
            or_(
                Staff.last_name.ilike(f'%{search_term}%'),
                Staff.first_name.ilike(f'%{search_term}%'),
                Staff.name.ilike(f'%{search_term}%')
            ),
            Staff.is_active == True
        ).all()

    def get_staff_details(self, staff_id):
        """
        Детальная информация о сотруднике

        Args:
            staff_id (int): ID сотрудника

        Returns:
            dict: Детальная информация
        """
        staff = self.session.query(Staff).filter_by(person_id=staff_id, is_active=True).first()

        if staff:
            return {
                'id': staff.person_id,
                'name': staff.name,
                'email': staff.email,
                'phone': staff.phone,
                'type': staff.type,
                'classes': [c.name for c in staff.classes],
                'last_seen': staff.last_seen_at,
                'max_user_id': staff.max_user_id
            }
        return None

    def show_problematic_staff(self):
        """
        Показывает проблемные записи сотрудников

        Returns:
            dict: Статистика проблем
        """
        logger.info("🔍 ПРОВЕРКА ПРОБЛЕМНЫХ ЗАПИСЕЙ")
        logger.info("=" * 70)

        no_user = self.session.query(Staff).filter(Staff.user_id.is_(None)).count()
        no_name = self.session.query(Staff).filter(
            or_(Staff.name.is_(None), Staff.name == '')
        ).count()
        no_contacts = self.session.query(Staff).filter(
            and_(Staff.phone.is_(None), Staff.email.is_(None))
        ).count()

        logger.info(f"• Без user_id: {no_user}")
        logger.info(f"• Без имени: {no_name}")
        logger.info(f"• Без контактов: {no_contacts}")

        if no_user > 0:
            logger.info("\nПримеры без user_id:")
            examples = self.session.query(Staff).filter(
                Staff.user_id.is_(None)
            ).limit(3).all()
            for staff in examples:
                logger.info(f"   • ID {staff.person_id}: {staff.name or 'Без имени'}")

        return {'no_user_id': no_user, 'no_name': no_name, 'no_contacts': no_contacts}

    def show_inactive_staff(self, limit=20):
        """
        Показывает неактивных сотрудников

        Args:
            limit (int): Максимальное количество для показа
        """
        logger.info(f"\n📋 НЕАКТИВНЫЕ СОТРУДНИКИ")
        logger.info("=" * 70)

        # Получаем всех неактивных сотрудников
        inactive = self.session.query(Staff).filter_by(is_active=False).all()

        if not inactive:
            logger.info("  Нет неактивных сотрудников")
            return

        logger.info(f"  Всего неактивных: {len(inactive)}")

        # Группируем по причине деактивации
        deactivated_today = 0
        deactivated_this_week = 0
        no_user_id = 0
        suspicious_names = 0

        # Получаем наивное время UTC для сравнения
        now_naive = utc_now_naive()
        today_naive = now_naive.date()
        week_ago_naive = now_naive - timedelta(days=7)

        for staff in inactive:
            if staff.deactivated_at:
                # Оба значения наивные, можно сравнивать
                if staff.deactivated_at.date() == today_naive:
                    deactivated_today += 1
                if staff.deactivated_at >= week_ago_naive:
                    deactivated_this_week += 1

            if not staff.user_id:
                no_user_id += 1
            elif staff.name and (
                    '_' in staff.name or any(x in staff.name for x in ['Англ', 'Нем', 'Фр', 'Мат', 'Инф'])):
                suspicious_names += 1

        logger.info(f"\n  📊 Статистика:")
        logger.info(f"     • Деактивировано сегодня: {deactivated_today}")
        logger.info(f"     • Деактивировано за неделю: {deactivated_this_week}")
        logger.info(f"     • Без user_id: {no_user_id}")
        logger.info(f"     • Подозрительные имена: {suspicious_names}")

        # Показываем примеры
        if inactive and limit > 0:
            logger.info(f"\n  📋 Примеры неактивных сотрудников:")
            for staff in inactive[:limit]:
                deactivated_str = staff.deactivated_at.strftime(
                    '%Y-%m-%d %H:%M') if staff.deactivated_at else 'неизвестно'

                # Определяем причину
                if not staff.user_id:
                    reason = "без user_id"
                elif staff.name and ('_' in staff.name):
                    reason = "подозрительное имя"
                else:
                    reason = "отсутствует в API"

                logger.info(f"     • {staff.name or 'Без имени'} (ID: {staff.person_id})")
                logger.info(f"       Деактивирован: {deactivated_str}, причина: {reason}")

        if len(inactive) > limit:
            logger.info(f"     ... и еще {len(inactive) - limit}")

    def close(self):
        """Закрывает сессию БД"""
        self.session.close()
        logger.info("Сессия БД закрыта")


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

def main():
    """
    Основная функция программы
    """
    # Заголовки для API
    headers = {
        "accept": "*/*",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "aid": "13",
        "authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyMzQxMjEiLCJzY3AiOiJvcGVuaWQgcHJvZmlsZSIsInN0ZiI6IjMzMzA0NDE3IiwiaXNzIjoiaHR0cHM6XC9cL3NjaG9vbC5tb3MucnUiLCJyb2wiOiIiLCJzc28iOiI4YzIzYWQ3ZS0xNGU0LTQ0YzYtYmVjZC05MmRjYjk4ZjkzNmQiLCJhdWQiOiI5OjkiLCJuYmYiOjE3NzEyMjAwMjUsImF0aCI6InN1ZGlyIiwicmxzIjoiezE5Ols0OTY6MTY6W11dfSx7OTpbMTozOls1MjldLDQzOjE6WzUyOV0sNTA6OTpbNTI5XSw1NDo5Ols1MjldLDEzNjo0Ols1MjldLDE4MToxNjpbNTI5XSwxODQ6MTY6WzUyOV0sMjAyOjE3Ols1MjldLDI0NDo2MTpbNTI5XSwyNDg6MTA6WzUyOV0sNDAwOjMwOls1MjldLDUyOTo0NDpbNTI5XSw1MzA6NDY6WzUyOV0sNTM1OjQ4Ols1MjldXX0iLCJleHAiOjE3NzIwODQwMTksImlhdCI6MTc3MTIyMDAyNSwianRpIjoiNDAwYjBlZGQtMjBkYy00MjY4LThiMGUtZGQyMGRjNzI2OGE1In0.JalxH5nZPD_NlplhRFST2B7XZ2dcYkNYe538u18tYgK3DUg_-dmUeMx0a1lTBZidH2-GCpAkM24RkxSX4Rlmg6ItYZwXd2mUgrgUJ8pjtRKA6LFUG5H-Oq5lffRxiZ5JlI3rv3PxZxgtXypHgiMZ1bMFyHGF16rvFd1594ug19GL3hnoT40iitg6Mluv7LvdQJ-WrSv1GNxYYFkVZyfOofC-OmbQt1eIZxVizaYjQv6DU48OHBDc5ecbi12e-Piq_KIvoD8IDtSN7rHiulLux7BQo3lo4UJdcbISibYTFf647LT_VxsZxU545YaMWOznfsYKPexETSXJjrp3Z5lBmQ",
        "profile-id": "16073051",
        "x-mes-hostid": "9",
        "x-mes-subsystem": "diariesw",
        "cookie": "_ym_uid=1759300601173689951; _ym_d=1759300601; das_d_tag2=e0f80065-3047-4ba9-87e4-cba4f1970d90; das_d_tag2_legacy=e0f80065-3047-4ba9-87e4-cba4f1970d90; uwyii=f5554d2f-d5e9-649e-251c-13c729fa115b; mos_id=Cg+IAmjcy/wpzlnITC3vAgA=; _ymab_param=rZvv35yyrPWiXGSB0_AdMUhltTK1ivusPjQRS5QCeh4Cz2W8Lvdw97WJ0r7HyV2PMfzay_Aiu6kNYaFEw8ZEwGv2Uo0; uwyiert=4e6f0232-2b0b-ec93-68cc-2e97013c6ea4; oxxfghcd=c37ee93d-af2c-459b-b345-7b387d7ea7db#1#2592000000#30000#600000#81540; oxxfghcd_legacy=c37ee93d-af2c-459b-b345-7b387d7ea7db#1#2592000000#30000#600000#81540; aupd_current_role=9:9; auth_flag=main; Ltpatoken2=Gm5qGCN3q1tuuZyKK/L0iO6HWbwQ5hRsbx+BRT1nm4S8plGK7BzrHpv5yMRMWeM7nuywusF3fzh4gw6tOX1oMmpJxWEVelYr8YgQWmoxJZOm7vxDLT4MOyKoJFK4soXMycZ819UcQq2nuz4rp6s5h+qQ+i6PQHqJksuuqF3aMSj1TQFQEDyLau6/bgw92ztumXBfou6bqmzrykW5yyXXhO1+gIEAZQesx1D5R9DNDTsuZNrydoXJ6juuUgFvCk0gJ2ubcc0pE/RLRZrBRSsnldHaQxXjldvLrKgxRJPbGJH9InwBlt0xo7i1cn8yjIsNN9NXds2/ORhdTIguiMLHtA==; ghur=Zn_6gWSgRhWYMI1cn56gQ2WsLv0RmNtzdNHLm__duYk|; sbp_sid=000000000000000000000000000000000000; uwyiert=4e6f0232-2b0b-ec93-68cc-2e97013c6ea4; aupd_token=eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIyMzQxMjEiLCJzY3AiOiJvcGVuaWQgcHJvZmlsZSIsInN0ZiI6IjMzMzA0NDE3IiwiaXNzIjoiaHR0cHM6XC9cL3NjaG9vbC5tb3MucnUiLCJyb2wiOiIiLCJzc28iOiI4YzIzYWQ3ZS0xNGU0LTQ0YzYtYmVjZC05MmRjYjk4ZjkzNmQiLCJhdWQiOiI5OjkiLCJuYmYiOjE3NzEyMjAwMjUsImF0aCI6InN1ZGlyIiwicmxzIjoiezE5Ols0OTY6MTY6W11dfSx7OTpbMTozOls1MjldLDQzOjE6WzUyOV0sNTA6OTpbNTI5XSw1NDo5Ols1MjldLDEzNjo0Ols1MjldLDE4MToxNjpbNTI5XSwxODQ6MTY6WzUyOV0sMjAyOjE3Ols1MjldLDI0NDo2MTpbNTI5XSwyNDg6MTA6WzUyOV0sNDAwOjMwOls1MjldLDUyOTo0NDpbNTI5XSw1MzA6NDY6WzUyOV0sNTM1OjQ4Ols1MjldXX0iLCJleHAiOjE3NzIwODQwMTksImlhdCI6MTc3MTIyMDAyNSwianRpIjoiNDAwYjBlZGQtMjBkYy00MjY4LThiMGUtZGQyMGRjNzI2OGE1In0.JalxH5nZPD_NlplhRFST2B7XZ2dcYkNYe538u18tYgK3DUg_-dmUeMx0a1lTBZidH2-GCpAkM24RkxSX4Rlmg6ItYZwXd2mUgrgUJ8pjtRKA6LFUG5H-Oq5lffRxiZ5JlI3rv3PxZxgtXypHgiMZ1bMFyHGF16rvFd1594ug19GL3hnoT40iitg6Mluv7LvdQJ-WrSv1GNxYYFkVZyfOofC-OmbQt1eIZxVizaYjQv6DU48OHBDc5ecbi12e-Piq_KIvoD8IDtSN7rHiulLux7BQo3lo4UJdcbISibYTFf647LT_VxsZxU545YaMWOznfsYKPexETSXJjrp3Z5lBmQ; oxxfgh=4068a2b5-534d-4c6d-88d0-976de4286a33#0#2628000000#30000#1800000#81640; profile_type=teacher; cluster_id=1; organization_id=529; user_id=15439532; profile_id=16073051; aid=13; _ym_isad=1; JSESSIONID=node01qu8fvkd6jyuf1kacl3huiwu3a30846677.node0"
    }

    collector = None

    try:
        # Создаем коллектор
        collector = SchoolDataCollector(headers, school_id=28)

        # Создаем бэкап перед синхронизацией
        logger.info("=" * 70)
        logger.info("🔄 ПОДГОТОВКА К СИНХРОНИЗАЦИИ")
        logger.info("=" * 70)
        backup_path = collector.backup.create_backup(prefix='pre_sync')
        if backup_path:
            logger.info(f"✅ Бэкап создан: {backup_path}")

        # Проверка проблемных записей
        collector.show_problematic_staff()

        # Запрос на очистку
        if input("\nОчистить проблемные записи? (y/n): ").strip().lower() == 'y':
            collector.clean_staff_without_user_id()
            collector.session.commit()
            logger.info("✅ Очистка выполнена")
            collector.show_problematic_staff()

        # Синхронизация персонала
        # collector.sync_all_staff()
        # collector.print_staff_statistics()

        # Показ неактивных
        collector.show_inactive_staff()

        # Получение классов
        logger.info("=" * 70)
        logger.info("📚 ПОЛУЧЕНИЕ КЛАССОВ")
        logger.info("=" * 70)

        # Раскомментируйте когда будете готовы синхронизировать классы
        class_data = collector._api_request('class_units', {'with_home_based': 'true'})

        if class_data:
            logger.info(f"📚 Получено классов: {len(class_data)}")
            collector.save_class_units(class_data)

            # Обработка учеников
            for unit in class_data:
                if isinstance(unit, dict):
                    collector.process_class_unit(unit['id'])
                else:
                    collector.process_class_unit(int(unit))

        # Итоговая статистика
        collector.print_statistics()

        logger.info("=" * 70)
        logger.info("✅ СИНХРОНИЗАЦИЯ УСПЕШНО ЗАВЕРШЕНА")
        logger.info("=" * 70)

    except KeyboardInterrupt:
        logger.warning("⚠️ Синхронизация прервана пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        logger.debug(traceback.format_exc())
    finally:
        if collector:
            collector.close()


if __name__ == "__main__":
    main()