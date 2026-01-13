import uuid

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel


class DataSource(Base, BaseModel):
    """
    DataSource model to store external database connections.
    """

    __tablename__ = "datasources"

    # Override ID to use UUID
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)

    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
    )
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)  # POSTGRES, GEOSERVER, TIMEIO

    # Store connection details (host, port, db, user, password_encrypted)
    connection_details = Column(JSONB, nullable=False)

    project = relationship("Project", backref="datasources")
