"""
User Context models for Projects and Dashboards.
"""

import uuid

from sqlalchemy import Boolean, Column, ForeignKey, String, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.models.base import BaseModel

# Association table for Project <-> Sensor (TimeIO Thing)
# Since Sensors are external (TimeIO), we just store the ID string.
project_sensors = Table(
    "project_sensors",
    Base.metadata,
    Column(
        "project_id",
        UUID(as_uuid=True),
        ForeignKey("projects.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("sensor_id", String, primary_key=True),  # TimeIO/Frost Thing ID
)


class Project(Base, BaseModel):
    """
    Project model to group sensors and dashboards.
    Owned by a Keycloak User.
    """

    __tablename__ = "projects"

    # Override ID to use UUID
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)

    name = Column(String(255), nullable=False)
    description = Column(String, nullable=True)

    # Keycloak User ID (Subject) - String or UUID depending on Keycloak config, usually UUID string
    owner_id = Column(String(255), nullable=False, index=True)

    # Additional properties (e.g. external IDs)
    properties = Column(JSONB, nullable=True)

    # Keycloak Group mapping for authorization
    authorization_provider_group_id = Column(String(255), nullable=True, index=True)
    # authorization_group_ids = Column(JSONB, nullable=True, default=list) # Removed

    # TimeIO Project ID
    schema_name = Column(String(255), nullable=True, index=True)

    # Relationships
    dashboards = relationship(
        "Dashboard", back_populates="project", cascade="all, delete-orphan"
    )
    members = relationship(
        "ProjectMember", back_populates="project", cascade="all, delete-orphan"
    )

    # We can't use a standard relationship for sensors easily because they aren't in this DB (conceptually),
    # but we can store the association. If we want to query them, we'd use the association table.
    # To make it accessible as a list of IDs:
    # We will just map the association table to a property or use a slightly different approach
    # if we want to store extra data. For now, simple association is fine.
    # But since 'Sensor' isn't a widely known model class here (it's TimeIO), we won't define a relationship
    # to a 'Sensor' class. Instead, we can query the association table directly or use a model for it
    # if we want more control.
    # Let's stick to the Table 'project_sensors' defined above.

    # To actually access the sensor_ids, we might not need a relationship property
    # unless we map 'sensor_id' to a class.
    # Let's just use the table for joins.


class ProjectMember(Base, BaseModel):
    """
    Members of a project with specific roles.
    """

    __tablename__ = "project_members"

    # Override ID to use UUID
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)

    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id = Column(String(255), nullable=False)  # Keycloak ID
    role = Column(
        String(50), nullable=False, default="viewer"
    )  # 'viewer', 'editor', 'admin'

    project = relationship("Project", back_populates="members")

    __table_args__ = (
        UniqueConstraint("project_id", "user_id", name="uq_project_member"),
    )


class Dashboard(Base, BaseModel):
    """
    Dashboard configuration for visualizing data.
    """

    __tablename__ = "dashboards"

    # Override ID to use UUID
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)

    project_id = Column(
        UUID(as_uuid=True),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
    )
    name = Column(String(255), nullable=False)

    layout_config = Column(JSONB, nullable=True)  # Grid layout positions
    widgets = Column(JSONB, nullable=True)  # Widget definitions

    is_public = Column(Boolean, default=False, nullable=False)

    project = relationship("Project", back_populates="dashboards")
