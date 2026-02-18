"""Association tables for many-to-many relationships."""
from sqlalchemy import Table, Column, Integer, String, Boolean, ForeignKey, Index, UniqueConstraint,DateTime
from sqlalchemy.sql import text
from .base import Base
from ..utils import get_db_time

# Таблица связи классов и персонала
class_staff = Table(
    'class_staff',
    Base.metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('class_unit_id', Integer, ForeignKey('class_units.id', ondelete='CASCADE')),
    Column('staff_id', Integer, ForeignKey('staff.id', ondelete='CASCADE')),
    Column('is_leader', Boolean, default=False, server_default='false'),
    Column('subject', String(100), nullable=True),
    Column('created_at', DateTime, default=get_db_time),
    UniqueConstraint('class_unit_id', 'staff_id', name='uq_class_staff'),
    Index('ix_class_staff_class', 'class_unit_id'),
    Index('ix_class_staff_staff', 'staff_id'),
)

# Таблица связи родителей и учеников
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