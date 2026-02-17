# shared/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

# Глобальные переменные
db_session = None
engine = None


def init_database(database_url="sqlite:///school.db", echo=False):
    """
    Инициализирует базу данных и возвращает engine
    """
    engine = create_engine(
        database_url,
        echo=echo,
        connect_args={'check_same_thread': False} if 'sqlite' in database_url else {}
    )

    from .models import Base
    Base.metadata.create_all(engine)

    return engine


def init_db(db_path="school.db", pool_size=5, max_overflow=10):
    """
    Инициализация подключения к БД с пулом соединений
    """
    global engine, db_session

    db_url = f"sqlite:///{db_path}"

    engine = create_engine(
        db_url,
        echo=False,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={'check_same_thread': False}
    )

    from .models import Base
    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine)
    db_session = scoped_session(session_factory)

    return engine, db_session


def get_session(engine=None):
    """Получение сессии для работы с БД"""
    global db_session

    if engine:
        # Если передан engine, создаем новую сессию
        Session = sessionmaker(bind=engine)
        return Session()

    # Иначе используем глобальную сессию
    if db_session is None:
        raise Exception("Database not initialized. Call init_db() first.")
    return db_session


@contextmanager
def session_scope():
    """Контекстный менеджер для автоматического закрытия сессии"""
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def close_db():
    """Закрытие всех соединений"""
    global db_session, engine
    if db_session:
        db_session.remove()
    if engine:
        engine.dispose()