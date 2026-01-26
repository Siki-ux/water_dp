from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String
from sqlalchemy.sql import func

from app.models.base import Base


class Simulation(Base):
    __tablename__ = "simulations"

    id = Column(String, primary_key=True, index=True)  # UUID
    thing_uuid = Column(
        String, unique=True, index=True, nullable=False
    )  # Refers to config_db/TSM thing

    # Configuration: datastreams, patterns, limits
    # [ {name, pattern: "sine", min, max, period}, ... ]
    config = Column(JSON, nullable=False)

    is_enabled = Column(Boolean, default=True)

    # Execution Tracking
    last_run = Column(DateTime(timezone=True), nullable=True)
    interval_seconds = Column(Integer, default=60)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
