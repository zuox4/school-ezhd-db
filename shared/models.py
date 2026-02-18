# shared/models.py
from sqlalchemy import (
    create_engine, Column, Integer, String, ForeignKey,
    DateTime, Boolean, Table, CheckConstraint, Index,
    text, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Импортируем из shared.utils, а не из school_sync
from shared.utils import get_db_time

Base = declarative_base()

# ==================== ТАБЛИЦЫ СВЯЗЕЙ ====================

class_staff = Table(
    'class_staff',
    Base.metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('class_unit_id', Integer, ForeignKey('class_units.id', ondelete='CASCADE')),
    Column('staff_id', Integer, ForeignKey('staff.id', ondelete='CASCADE')),
    Column('is_leader', Boolean, default=False, server_default=text('0')),
    Column('subject', String(100), nullable=True),
    Column('created_at', DateTime, default=get_db_time),
    UniqueConstraint('class_unit_id', 'staff_id', name='uq_class_staff'),
    Index('ix_class_staff_class', 'class_unit_id'),
    Index('ix_class_staff_staff', 'staff_id'),
)

parent_student = Table(
    'parent_student',
    Base.metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('parent_id', Integer, ForeignKey('parents.id', ondelete='CASCADE')),
    Column('student_id', Integer, ForeignKey('students.id', ondelete='CASCADE')),
    Column('relationship_type', String(50), nullable=True),
    Column('created_at', DateTime, default=get_db_time),
    UniqueConstraint('parent_id', 'student_id', name='uq_parent_student'),
    Index('ix_parent_student_parent', 'parent_id'),
    Index('ix_parent_student_student', 'student_id'),
)

# ==================== ОСНОВНЫЕ МОДЕЛИ ====================

class Staff(Base):
    """Модель персонала (учителя, менторы, и т.д.)"""
    __tablename__ = 'staff'

    __table_args__ = (
        CheckConstraint('length(phone) = 11 OR phone IS NULL', name='check_phone_length'),
        Index('ix_staff_person_id', 'person_id', unique=True),
        Index('ix_staff_user_id', 'user_id'),
        Index('ix_staff_is_active', 'is_active'),
        Index('ix_staff_active_type', 'is_active', 'type'),
        Index('ix_staff_updated_api', 'updated_at_api'),
        Index('ix_staff_max_user_id', 'max_user_id'),  # Новый индекс
    )

    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=True)

    # Новое поле для MAX ID
    max_user_id = Column(String(255), nullable=True,
                         unique=True)  # unique=True если ID уникальны для всех пользователей

    name = Column(String(200), nullable=True)
    last_name = Column(String(100), nullable=True)
    first_name = Column(String(100), nullable=True)
    middle_name = Column(String(100), nullable=True)

    email = Column(String(100), nullable=True)
    phone = Column(String(11), nullable=True)

    type = Column(String(50), nullable=True)
    updated_at_api = Column(DateTime, nullable=True)

    classes = relationship("ClassUnit", secondary=class_staff, back_populates="staff")

    is_active = Column(Boolean, default=True, server_default=text('1'))
    deactivated_at = Column(DateTime, nullable=True)
    last_seen_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    def __repr__(self):
        return f"<Staff {self.last_name or self.name} (ID: {self.person_id})>"

    @property
    def full_name(self):
        if self.last_name and self.first_name:
            return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()
        return self.name or f"ID:{self.person_id}"


class ClassUnit(Base):
    """Модель классного подразделения"""
    __tablename__ = 'class_units'

    __table_args__ = (
        Index('ix_class_name', 'name'),
        Index('ix_class_parallel', 'parallel'),
    )

    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, nullable=True)
    class_level_id = Column(Integer, nullable=True)
    name = Column(String(50), nullable=False)
    parallel = Column(String(10), nullable=True)
    literal = Column(String(10), nullable=True)
    max_user_id = Column(String(11), nullable=True)
    max_link = Column(String(11), nullable=True)
    students = relationship("Student", back_populates="class_unit", cascade="all, delete-orphan")
    staff = relationship("Staff", secondary=class_staff, back_populates="classes")

    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    def __repr__(self):
        return f"<ClassUnit {self.name}>"

    @property
    def student_count(self):
        return len([s for s in self.students if s.is_active])


class Student(Base):
    """Модель ученика"""
    __tablename__ = 'students'

    __table_args__ = (
        CheckConstraint('length(phone) = 11 OR phone IS NULL', name='check_student_phone_length'),
        Index('ix_student_person_id', 'person_id', unique=True),
        Index('ix_student_class', 'class_unit_id'),
        Index('ix_student_is_active', 'is_active'),
        Index('ix_student_name', 'last_name', 'first_name'),
        Index('ix_student_active_class', 'is_active', 'class_unit_id'),
        Index('ix_student_max_user_id', 'max_user_id'),  # Новый индекс
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, nullable=False)
    user_name = Column(String(100), nullable=True)

    # Новое поле для MAX ID
    max_user_id = Column(String(255), nullable=True, unique=True)

    last_name = Column(String(100), nullable=False)
    first_name = Column(String(100), nullable=False)
    middle_name = Column(String(100), nullable=True)

    email = Column(String(100), nullable=True)
    phone = Column(String(11), nullable=True)

    is_active = Column(Boolean, default=True, server_default=text('1'))
    deactivated_at = Column(DateTime, nullable=True)

    class_unit_id = Column(Integer, ForeignKey('class_units.id', ondelete='SET NULL'))
    class_unit = relationship("ClassUnit", back_populates="students")
    parents = relationship("Parent", secondary=parent_student, back_populates="children")

    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    def __repr__(self):
        return f"<Student {self.last_name} {self.first_name} (ID: {self.person_id})>"

    @property
    def full_name(self):
        return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()

    @property
    def parent_count(self):
        return len([p for p in self.parents if p.is_active])


class Parent(Base):
    """Модель родителя"""
    __tablename__ = 'parents'

    __table_args__ = (
        CheckConstraint('length(phone) = 11 OR phone IS NULL', name='check_parent_phone_length'),
        Index('ix_parent_person_id', 'person_id', unique=True),
        Index('ix_parent_is_active', 'is_active'),
        Index('ix_parent_name', 'last_name', 'first_name'),
        Index('ix_parent_max_user_id', 'max_user_id'),  # Новый индекс
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, nullable=False)

    # Новое поле для MAX ID
    max_user_id = Column(String(255), nullable=True, unique=True)

    name = Column(String(200), nullable=True)
    last_name = Column(String(100), nullable=True)
    first_name = Column(String(100), nullable=True)
    middle_name = Column(String(100), nullable=True)

    email = Column(String(100), nullable=True)
    phone = Column(String(11), nullable=True)

    is_active = Column(Boolean, default=True, server_default=text('1'))
    deactivated_at = Column(DateTime, nullable=True)

    children = relationship("Student", secondary=parent_student, back_populates="parents")

    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    def __repr__(self):
        return f"<Parent {self.name or f'{self.last_name} {self.first_name}'} (ID: {self.person_id})>"

    @property
    def full_name(self):
        if self.last_name and self.first_name:
            return f"{self.last_name} {self.first_name} {self.middle_name or ''}".strip()
        return self.name or f"ID:{self.person_id}"

    @property
    def children_count(self):
        return len([c for c in self.children if c.is_active])