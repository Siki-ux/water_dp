"""
Base model with common fields and functionality.
"""

from datetime import datetime, timezone
from typing import Any, Dict

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Session


class BaseModel:
    """Base model with common fields."""

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    created_by = Column(String(100), nullable=True)
    updated_by = Column(String(100), nullable=True)
    metadata_json = Column(
        Text, nullable=True
    )  # For storing additional metadata as JSON

    @declared_attr
    def __tablename__(cls) -> str:
        """Generate table name from class name."""
        # SQLAlchemy's @declared_attr passes the class as the first argument (cls),
        # even though it's defined as an instance method in the mixin.
        return cls.__name__.lower()

    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary."""
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }

    @classmethod
    def get_by_id(cls, db: Session, id: int):
        """Get model instance by ID."""
        return db.query(cls).filter(cls.id == id).first()

    @classmethod
    def get_all(cls, db: Session, skip: int = 0, limit: int = 100):
        """Get all model instances with pagination."""
        return db.query(cls).offset(skip).limit(limit).all()


class PydanticBase(PydanticBaseModel):
    """Base Pydantic model with common configuration."""

    model_config = ConfigDict(from_attributes=True)
