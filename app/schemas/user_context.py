from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.base import PydanticBase

# --- Project Member Schemas ---


class ProjectMemberBase(PydanticBase):
    role: str = Field(default="viewer", pattern="^(viewer|editor)$")


class ProjectMemberCreate(ProjectMemberBase):
    user_id: str


class ProjectMemberResponse(ProjectMemberBase):
    id: UUID
    project_id: UUID
    user_id: str
    created_at: datetime
    updated_at: datetime


# --- Dashboard Schemas ---


class DashboardBase(PydanticBase):
    name: str = Field(min_length=1, max_length=255)
    layout_config: Optional[Dict[str, Any]] = None
    widgets: Optional[List[Dict[str, Any]]] = None
    is_public: bool = False


class DashboardCreate(DashboardBase):
    project_id: Optional[UUID] = None


class DashboardUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    layout_config: Optional[Dict[str, Any]] = None
    widgets: Optional[List[Dict[str, Any]]] = None
    is_public: Optional[bool] = None


class DashboardResponse(DashboardBase):
    id: UUID
    project_id: UUID
    created_at: datetime
    updated_at: datetime


# --- Project Schemas ---


class ProjectBase(PydanticBase):
    name: str = Field(min_length=1, max_length=255)
    description: Optional[str] = None


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None


class ProjectResponse(ProjectBase):
    id: UUID
    owner_id: str
    created_at: datetime
    updated_at: datetime

    # Optional fields to include linked resources?
    # For now, keep it simple. Members and Dashboards fetched separately or via include param.


class ProjectSensorResponse(BaseModel):
    project_id: UUID
    sensor_id: str
