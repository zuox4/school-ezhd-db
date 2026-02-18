"""ClassUnit model."""
from sqlalchemy import Column, Integer, String, DateTime, Index
from sqlalchemy.orm import relationship
from .base import Base
from .tables import class_staff
from ..utils import get_db_time

class ClassUnit(Base):
    """Модель класса."""
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

    # Relationships
    students = relationship("Student", back_populates="class_unit", cascade="all, delete-orphan")
    staff = relationship("Staff", secondary=class_staff, back_populates="classes")

    # Временные метки
    created_at = Column(DateTime, default=get_db_time, nullable=False)
    updated_at = Column(DateTime, default=get_db_time, onupdate=get_db_time, nullable=False)

    @property
    def student_count(self):
        """Количество активных учеников."""
        return len([s for s in self.students if s.is_active])

    @property
    def class_teacher(self):
        """Классный руководитель."""
        # Этот метод требует доступа к is_leader из class_staff
        # В упрощенной версии можно сделать отдельный запрос
        from sqlalchemy import select
        from .tables import class_staff

        stmt = select(class_staff.c.staff_id).where(
            class_staff.c.class_unit_id == self.id,
            class_staff.c.is_leader == True
        )
        result = self.class_staff.execute(stmt).first()
        if result:
            from .staff import Staff
            return Staff.query.get(result[0])
        return None

    def __repr__(self):
        return f"<ClassUnit {self.name}>"