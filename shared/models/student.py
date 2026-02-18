"""Student model."""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Index, CheckConstraint
from sqlalchemy.orm import relationship, validates
from .base import Base
from .tables import parent_student
from ..utils import get_db_time, utc_now

class Student(Base):
    """Модель ученика."""
    __tablename__ = 'students'

    __table_args__ = (
        CheckConstraint('length(phone) = 11 OR phone IS NULL', name='check_student_phone_length'),
        Index('ix_student_person_id', 'person_id', unique=True),
        Index('ix_student_class', 'class_unit_id'),
        Index('ix_student_is_active', 'is_active'),
        Index('ix_student_name', 'last_name', 'first_name'),
        Index('ix_student_active_class', 'is_active', 'class_unit_id'),
        Index('ix_student_max_user_id', 'max_user_id'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, nullable=False)
    user_name = Column(String(100), nullable=True)
    max_user_id = Column(String(255), nullable=True, unique=True)

    # Персональные данные
    last_name = Column(String(100), nullable=False)
    first_name = Column(String(100), nullable=False)
    middle_name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    phone = Column(String(11), nullable=True)

    # Статус
    is_active = Column(Boolean, default=True, server_default='1')
    deactivated_at = Column(DateTime, nullable=True)

    # Связи
    class_unit_id = Column(Integer, ForeignKey('class_units.id', ondelete='SET NULL'))

    # Relationships
    class_unit = relationship("ClassUnit", back_populates="students")
    parents = relationship("Parent", secondary=parent_student, back_populates="children")

    # Временные метки
    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    @property
    def full_name(self):
        return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()

    @property
    def parent_count(self):
        return len([p for p in self.parents if p.is_active])

    @property
    def active_parents(self):
        return [p for p in self.parents if p.is_active]

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
        return f"<Student {self.full_name} (ID: {self.person_id})>"