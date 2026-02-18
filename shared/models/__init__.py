"""Models package."""
from .staff import Staff
from .class_unit import ClassUnit
from .student import Student
from .parent import Parent
from .tables import class_staff, parent_student

__all__ = [
    'Staff', 'ClassUnit', 'Student', 'Parent',
    'class_staff', 'parent_student'
]