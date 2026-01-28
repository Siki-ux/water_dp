from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.api import deps
from app.models.user_context import Project
from app.schemas.parser import ParserCreate, ParserResponse
from app.services.timeio.orchestrator_v3 import orchestrator_v3
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("", response_model=List[ParserResponse])
def list_parsers(
    group_id: Optional[str] = Query(None, description="Filter by Keycloak Group ID"),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    List available parsers.
    Use group_id to filter by project visibility.
    """
    if not group_id:
        pass
        
    parsers = orchestrator_v3.db.get_parsers_by_group(group_id or "")
    
    results = []
    for p in parsers:
        results.append(ParserResponse(
            id=p["id"],
            name=p["name"],
            group_id=p["group_id"],
            type=p["type"],
            settings=p["settings"]
        ))
    return results

@router.post("", response_model=ParserResponse)
def create_parser(
    payload: ParserCreate,
    database: Session = Depends(deps.get_db),
    current_user: dict = Depends(deps.get_current_user),
):
    """
    Create a new CSV Parser configuration.
    """
    # Resolve Project UUID to Group ID
    project = database.query(Project).filter(Project.id == payload.project_uuid).first()
    if not project:
        raise HTTPException(status_code=404, detail=f"Project {payload.project_uuid} not found")
        
    group_id = project.authorization_provider_group_id
    if not group_id:
        # Fallback to UUID if group ID is missing (v3 pure project?)
        # But usually every project has a group link.
        logger.warning(f"Project {payload.project_uuid} has no auth group ID. Using UUID as group ID.")
        group_id = payload.project_uuid

    try:
        parser_id = orchestrator_v3.db.create_parser(
            name=payload.name,
            group_id=group_id,
            settings=payload.settings.dict(),
            type_name=payload.type
        )
        
        return ParserResponse(
            id=parser_id,
            name=payload.name,
            group_id=group_id,
            type=payload.type,
            settings=payload.settings
        )
    except Exception as e:
        logger.error(f"Error creating parser: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create parser: {str(e)}")
