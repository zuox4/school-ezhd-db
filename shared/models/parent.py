"""Parent model."""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, CheckConstraint
from sqlalchemy.orm import relationship, validates
from .base import Base
from .tables import parent_student
from ..utils import get_db_time, utc_now

class Parent(Base):
    """Модель родителя."""
    __tablename__ = 'parents'

    __table_args__ = (
        CheckConstraint('length(phone) = 11 OR phone IS NULL', name='check_parent_phone_length'),
        Index('ix_parent_person_id', 'person_id', unique=True),
        Index('ix_parent_is_active', 'is_active'),
        Index('ix_parent_name', 'last_name', 'first_name'),
        Index('ix_parent_max_user_id', 'max_user_id'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, nullable=False)
    max_user_id = Column(String(255), nullable=True, unique=True)

    # Персональные данные
    name = Column(String(200), nullable=True)
    last_name = Column(String(100), nullable=True)
    first_name = Column(String(100), nullable=True)
    middle_name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    phone = Column(String(11), nullable=True)

    # Статус
    is_active = Column(Boolean, default=True, server_default='1')
    deactivated_at = Column(DateTime, nullable=True)

    # Relationships
    children = relationship("Student", secondary=parent_student, back_populates="parents")

    # Временные метки
    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    @property
    def full_name(self):
        if self.last_name and self.first_name:
            return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()
        return self.name or f"ID:{self.person_id}"

    @property
    def children_count(self):
        return len([c for c in self.children if c.is_active])

    @property
    def active_children(self):
        return [c for c in self.children if c.is_active]

    @validates('phone')
    def validate_phone(self, key, phone):
        if phone and len(phone) != 11:
            raise ValueError(f"Phone must be 11 digits, got {len(phone)}")
        return phone

    def deactivate(self):
        self.is_active = False
        self.deactivated_at = utc_now()

    def activate(self):
        self.is_active = True
        self.deactivated_at = None

    def __repr__(self):
        return f"<Parent {self.full_name} (ID: {self.person_id})>"