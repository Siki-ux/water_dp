import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import UUID4, BaseModel
from sqlalchemy.orm import Session

from app.api import deps
from app.core.database import get_db
from app.models.alerts import Alert, AlertDefinition
from app.services.project_service import ProjectService

logger = logging.getLogger(__name__)

router = APIRouter()

# --- Pydantic Schemas ---


class AlertDefinitionBase(BaseModel):
    name: str
    description: Optional[str] = None
    alert_type: str  # threshold, nodata, etc.
    target_id: Optional[str] = None
    conditions: Dict[str, Any] = {}
    severity: str = "warning"
    is_active: bool = True


class AlertDefinitionCreate(AlertDefinitionBase):
    project_id: UUID4


class AlertDefinitionRead(AlertDefinitionBase):
    id: UUID4
    project_id: UUID4
    created_by: Optional[str]

    class ConfigDict:
        from_attributes = True


class AlertRead(BaseModel):
    id: UUID4
    definition_id: UUID4
    timestamp: datetime
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    definition: AlertDefinitionRead  # Include nested details

    class ConfigDict:
        from_attributes = True


# --- Endpoints ---


@router.get("/definitions/{project_id}", response_model=List[AlertDefinitionRead])
def get_alert_definitions(
    project_id: UUID4,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    List alert definitions for a project.
    """
    ProjectService._check_access(db, project_id, current_user, required_role="viewer")

    definitions = (
        db.query(AlertDefinition).filter(AlertDefinition.project_id == project_id).all()
    )
    return definitions


@router.post("/definitions", response_model=AlertDefinitionRead)
def create_alert_definition(
    definition: AlertDefinitionCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Create a new alert definition.
    """
    ProjectService._check_access(
        db, definition.project_id, current_user, required_role="editor"
    )

    db_def = AlertDefinition(
        name=definition.name,
        description=definition.description,
        project_id=definition.project_id,
        alert_type=definition.alert_type,
        target_id=definition.target_id,
        conditions=definition.conditions,
        severity=definition.severity,
        is_active=definition.is_active,
        created_by=current_user.get("sub"),
    )
    db.add(db_def)
    db.commit()
    db.refresh(db_def)
    return db_def


class AlertDefinitionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    conditions: Optional[Dict[str, Any]] = None
    severity: Optional[str] = None
    is_active: Optional[bool] = None


@router.put("/definitions/{definition_id}", response_model=AlertDefinitionRead)
def update_alert_definition(
    definition_id: UUID4,
    update_data: AlertDefinitionUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Update an alert definition.
    """
    db_def = (
        db.query(AlertDefinition).filter(AlertDefinition.id == definition_id).first()
    )
    if not db_def:
        raise HTTPException(status_code=404, detail="Alert definition not found")

    ProjectService._check_access(
        db, db_def.project_id, current_user, required_role="editor"
    )

    if update_data.name is not None:
        db_def.name = update_data.name
    if update_data.description is not None:
        db_def.description = update_data.description
    if update_data.conditions is not None:
        db_def.conditions = update_data.conditions
    if update_data.severity is not None:
        db_def.severity = update_data.severity
    if update_data.is_active is not None:
        db_def.is_active = update_data.is_active

    db.commit()
    db.refresh(db_def)
    return db_def


@router.delete("/definitions/{definition_id}")
def delete_alert_definition(
    definition_id: UUID4,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Delete an alert definition.
    """
    db_def = (
        db.query(AlertDefinition).filter(AlertDefinition.id == definition_id).first()
    )
    if not db_def:
        raise HTTPException(status_code=404, detail="Alert definition not found")

    ProjectService._check_access(
        db, db_def.project_id, current_user, required_role="editor"
    )

    db.delete(db_def)
    db.commit()
    return {"ok": True}


@router.get("/history/{project_id}", response_model=List[AlertRead])
def get_alert_history(
    project_id: UUID4,
    status: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
    limit: int = 100,
):
    """
    Get history of triggered alerts for a project.
    """
    ProjectService._check_access(db, project_id, current_user, required_role="viewer")

    # Join with definition to filter by project
    query = (
        db.query(Alert)
        .join(AlertDefinition)
        .filter(AlertDefinition.project_id == project_id)
    )

    if status:
        query = query.filter(Alert.status == status)

    alerts = query.order_by(Alert.timestamp.desc()).limit(limit).all()
    return alerts


@router.post("/history/{alert_id}/acknowledge")
def acknowledge_alert(
    alert_id: UUID4,
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Acknowledge an alert.
    """
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    # Check access to the project owning the alert's definition
    definition = (
        db.query(AlertDefinition)
        .filter(AlertDefinition.id == alert.definition_id)
        .first()
    )
    if definition:
        ProjectService._check_access(
            db, definition.project_id, current_user, required_role="viewer"
        )

    alert.status = "acknowledged"
    alert.acknowledged_by = current_user.get("sub")
    alert.acknowledged_at = datetime.utcnow()
    db.commit()
    return {"status": "acknowledged"}


@router.post("/test-trigger", response_model=AlertRead)
def trigger_test_alert(
    definition_id: UUID4 = Body(..., embed=True),
    message: str = Body(..., embed=True),
    db: Session = Depends(get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Manually trigger an alert for testing/demo purposes.
    """
    db_def = (
        db.query(AlertDefinition).filter(AlertDefinition.id == definition_id).first()
    )
    if not db_def:
        raise HTTPException(status_code=404, detail="Alert definition not found")

    ProjectService._check_access(
        db, db_def.project_id, current_user, required_role="editor"
    )

    # Create Alert
    alert = Alert(
        definition_id=db_def.id,
        status="active",
        message=message,
        details={"executor": "manual_test", "triggered_by": current_user.get("sub")},
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert
