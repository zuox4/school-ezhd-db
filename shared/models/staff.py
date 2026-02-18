"""Staff model."""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, CheckConstraint
from sqlalchemy.orm import relationship, validates
from .base import Base
from .tables import class_staff
from ..utils import get_db_time, utc_now

class Staff(Base):
    """Модель персонала."""
    __tablename__ = 'staff'

    __table_args__ = (
        CheckConstraint('length(phone) = 11 OR phone IS NULL', name='check_phone_length'),
        Index('ix_staff_person_id', 'person_id', unique=True),
        Index('ix_staff_user_id', 'user_id'),
        Index('ix_staff_is_active', 'is_active'),
        Index('ix_staff_active_type', 'is_active', 'type'),
        Index('ix_staff_updated_api', 'updated_at_api'),
        Index('ix_staff_max_user_id', 'max_user_id'),
    )

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=True)
    max_user_id = Column(String(255), nullable=True, unique=True)
    max_link_path = Column(String(255), nullable=True, unique=True)
    # Персональные данные
    name = Column(String(200), nullable=True)
    last_name = Column(String(100), nullable=True)
    first_name = Column(String(100), nullable=True)
    middle_name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    phone = Column(String(11), nullable=True)

    # Служебные поля
    type = Column(String(50), nullable=True)
    updated_at_api = Column(DateTime, nullable=True)

    # Статус
    is_active = Column(Boolean, default=True, server_default='1')
    deactivated_at = Column(DateTime, nullable=True)
    last_seen_at = Column(DateTime, nullable=True)

    # Временные метки
    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    # Relationships
    classes = relationship("ClassUnit", secondary=class_staff, back_populates="staff")

    @property
    def full_name(self):
        """Полное имя."""
        if self.last_name and self.first_name:
            return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()
        return self.name or f"ID:{self.person_id}"

    @validates('phone')
    def validate_phone(self, key, phone):
        if phone and len(phone) != 11:
            raise ValueError(f"Phone must be 11 digits, got {len(phone)}")
        return phone

    def deactivate(self):
        """Мягкое удаление."""
        self.is_active = False
        self.deactivated_at = utc_now()

    def activate(self):
        """Активация."""
        self.is_active = True
        self.deactivated_at = None

    def __repr__(self):
        return f"<Staff {self.full_name} (ID: {self.person_id})>"