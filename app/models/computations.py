import uuid

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel


class ComputationScript(Base, BaseModel):
    __tablename__ = "computation_scripts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    filename = Column(String, nullable=False)

    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
    )
    uploaded_by = Column(String, nullable=False)  # User ID


class ComputationJob(Base, BaseModel):
    __tablename__ = "computation_jobs"

    # Use the Celery Task ID as primary key
    id = Column(String, primary_key=True, nullable=False)

    script_id = Column(
        UUID(as_uuid=True),
        ForeignKey("computation_scripts.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id = Column(String, nullable=False)  # User who started it
    status = Column(String, nullable=False, default="PENDING")
    start_time = Column(
        String, nullable=True
    )  # ISO format or DateTime? Let's use DateTime if possible, but String is safer if flexible. Let's use DateTime with func.now()

    script = relationship("ComputationScript", backref="jobs")
