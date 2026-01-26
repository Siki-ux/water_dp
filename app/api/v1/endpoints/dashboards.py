import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request, status
from jose import JWTError
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.api import deps
from app.core.database import get_db
from app.core.security import verify_token
from app.schemas.user_context import DashboardResponse, DashboardUpdate
from app.services.dashboard_service import DashboardService

logger = logging.getLogger(__name__)

router = APIRouter()


async def get_optional_current_user(request: Request) -> dict | None:
    """
    Get user from token if present, else None.
    Used for public/private dashboard access.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token = auth_header.split(" ")[1]
    try:
        return await verify_token(token)
    except (JWTError, ValidationError):
        return None
    except Exception as error:
        logger.error(f"Token verification unexpected error: {error}")
        return None


@router.get("/{dashboard_id}", response_model=DashboardResponse)
async def get_dashboard(
    dashboard_id: UUID,
    database: Session = Depends(get_db),
    user: dict | None = Depends(get_optional_current_user),
) -> Any:
    """
    Get dashboard details.
    Public dashboards are accessible without auth.
    Private dashboards require auth.
    """
    return DashboardService.get_dashboard(database, dashboard_id, user)


@router.put("/{dashboard_id}", response_model=DashboardResponse)
def update_dashboard(
    dashboard_id: UUID,
    dashboard_in: DashboardUpdate,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> Any:
    """Update dashboard."""
    return DashboardService.update_dashboard(
        database, dashboard_id, dashboard_in, user
    )


@router.delete("/{dashboard_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dashboard(
    dashboard_id: UUID,
    database: Session = Depends(get_db),
    user: dict = Depends(deps.get_current_user),
) -> None:
    """Delete dashboard."""
    DashboardService.delete_dashboard(database, dashboard_id, user)
    return
