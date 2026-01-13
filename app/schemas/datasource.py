from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class DataSourceBase(BaseModel):
    name: str
    type: str = Field(..., description="POSTGRES, GEOSERVER, TIMEIO")
    connection_details: Dict[str, Any]


class DataSourceCreate(DataSourceBase):
    pass


class DataSourceUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    connection_details: Optional[Dict[str, Any]] = None


class QueryRequest(BaseModel):
    sql: str


class DataSourceResponse(DataSourceBase):
    id: UUID
    project_id: UUID

    class Config:
        from_attributes = True

    @field_validator("connection_details")
    @classmethod
    def mask_password(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if v and "password" in v:
            v_copy = v.copy()
            v_copy["password"] = "********"
            return v_copy
        return v
