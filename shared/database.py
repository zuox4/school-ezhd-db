"""Database configuration and session management."""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator
import logging

logger = logging.getLogger(__name__)

# Настройки подключения
SQLALCHEMY_DATABASE_URL = "sqlite:///./school.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in SQLALCHEMY_DATABASE_URL else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def init_database(*args, **kwargs):
    """Инициализация базы данных (с поддержкой любых аргументов)"""
    from . import models

    drop_all = False
    if args and len(args) > 0:
        drop_all = bool(args[0])
    if 'drop_all' in kwargs:
        drop_all = bool(kwargs['drop_all'])

    if drop_all:
        logger.warning("Удаление всех таблиц!")
        Base.metadata.drop_all(bind=engine)

    logger.info("Создание таблиц...")
    Base.metadata.create_all(bind=engine)
    logger.info("Таблицы успешно созданы")

def get_session(*args, **kwargs) -> Session:
    """Получение сессии (игнорирует аргументы)"""
    return SessionLocal()

@contextmanager
def session_scope():
    """Контекстный менеджер для автоматического закрытия сессии"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при работе с БД: {e}")
        raise
    finally:
        session.close()

def get_db() -> Generator:
    """Зависимость для FastAPI"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()