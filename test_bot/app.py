# web_app/app_fastapi.py
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database import init_db, get_session
from shared.models import Staff, Student, Parent, ClassUnit
from config import config
from pydantic import BaseModel, ConfigDict

# Инициализация при старте
init_db(
    db_path=config.DB_PATH,
    pool_size=config.DB_POOL_SIZE,
    max_overflow=config.DB_MAX_OVERFLOW
)

app = FastAPI(title="School API")


# Pydantic модели для ответов
class StudentResponse(BaseModel):
    id: int
    full_name: str
    class_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None

    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True
    )


class StaffResponse(BaseModel):
    id: int
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    type: Optional[str] = None
    classes: List[str] = []


# Зависимость для получения сессии
def get_db():
    db = get_session()
    try:
        yield db
    finally:
        db.close()


@app.get("/api/students", response_model=List[StudentResponse])
def get_students(
        class_id: Optional[int] = Query(None),
        name: Optional[str] = Query(None),
        db: Session = Depends(get_db)
):
    """Получение списка учеников"""
    query = db.query(Student).filter(Student.is_active == True)

    if class_id:
        query = query.filter(Student.class_unit_id == class_id)
    if name:
        query = query.filter(
            (Student.last_name.ilike(f'%{name}%')) |
            (Student.first_name.ilike(f'%{name}%'))
        )

    students = query.all()

    return [
        StudentResponse(
            id=s.person_id,
            full_name=s.full_name,
            class_name=s.class_unit.name if s.class_unit else None,
            email=s.email,
            phone=s.phone
        ) for s in students
    ]


@app.get("/api/staff", response_model=StaffResponse)
def get_staff(staff_id: int, db: Session = Depends(get_db)):
    """Получение информации о сотруднике"""
    staff = db.query(Staff).first()

    if not staff:
        raise HTTPException(status_code=404, detail="Staff not found")

    return StaffResponse(
        id=staff.person_id,
        name=staff.full_name,
        email=staff.email,
        phone=staff.phone,
        type=staff.type,
        classes=[c.name for c in staff.classes]
    )

# Запуск: uvicorn web_app.app_fastapi:app --reload