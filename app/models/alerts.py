import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String, Text, Float
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel


class AlertDefinition(Base, BaseModel):
    """
    Defines a rule for triggering alerts.
    """
    __tablename__ = "alert_definitions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    
    project_id = Column(UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    
    # Configuration
    # Type: "threshold", "nodata", "computation_failure", "computation_result"
    alert_type = Column(String, nullable=False)
    
    # Target
    # For sensors: 'station_id' or Datastream ID
    # For computations: 'script_id' or 'job_id'
    target_id = Column(String, nullable=True) 
    
    # Thresholds / Conditions (stored as JSON for flexibility)
    # e.g. {"operator": ">", "value": 50.0} or {"duration_minutes": 60}
    conditions = Column(JSONB, nullable=False, default={})
    
    is_active = Column(Boolean, default=True)
    severity = Column(String, default="warning") # info, warning, critical
    
    # Metadata
    created_by = Column(String, nullable=True)
    
    project = relationship("Project", backref="alert_definitions")
    alerts = relationship("Alert", backref="definition", cascade="all, delete-orphan")


class Alert(Base, BaseModel):
    """
    Represents an actual triggered alert instance.
    """
    __tablename__ = "alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    definition_id = Column(UUID(as_uuid=True), ForeignKey("alert_definitions.id", ondelete="CASCADE"), nullable=False)
    
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    status = Column(String, default="active") # active, acknowledged, resolved
    
    message = Column(Text, nullable=False)
    details = Column(JSONB, nullable=True) # Snapshot of value, etc.
    
    acknowledged_by = Column(String, nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)
