# shared/models/mixins.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import Mapped, mapped_column, validates
from typing import Optional
from datetime import datetime
from ..utils import utc_now, get_db_time

class PersonMixin:
    """Миксин для персональных данных."""
    name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    middle_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(11), nullable=True)

    @property
    def full_name(self) -> str:
        """Полное имя."""
        if self.last_name and self.first_name:
            return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()
        return getattr(self, 'name', f"ID:{getattr(self, 'person_id', '?')}")

    @validates('phone')
    def validate_phone(self, key: str, phone: Optional[str]) -> Optional[str]:
        if phone and len(phone) != 11:
            raise ValueError(f"Phone must be 11 digits, got {len(phone)}")
        return phone

class ActiveMixin:
    """Миксин для мягкого удаления."""
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default='1')
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    def deactivate(self) -> None:
        self.is_active = False
        self.deactivated_at = utc_now()

    def activate(self) -> None:
        self.is_active = True
        self.deactivated_at = None

class TimestampMixin:
    """Миксин для временных меток."""
    created_at: Mapped[datetime] = mapped_column(DateTime, default=get_db_time, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

class MaxUserMixin:
    """Миксин для MAX интеграции."""
    max_user_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, unique=True)