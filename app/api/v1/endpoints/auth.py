from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm

from app.api import deps
from app.schemas.auth import LoginRequest, TokenRefreshRequest, TokenSchema
from app.services.keycloak_service import KeycloakService

router = APIRouter()


@router.post("/token", response_model=TokenSchema)
def login_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Dict[str, Any]:
    """
    OAuth2 compatible token login, get an access token for future requests.
    """
    return KeycloakService.login_user(form_data.username, form_data.password)


@router.post("/login", response_model=TokenSchema)
def login(request: LoginRequest) -> Dict[str, Any]:
    """
    Login with username and password to obtain access and refresh tokens.
    """
    return KeycloakService.login_user(request.username, request.password)


@router.post("/refresh", response_model=TokenSchema)
def refresh_token(request: TokenRefreshRequest) -> Dict[str, Any]:
    """
    Refresh an access token using a valid refresh token.
    """
    return KeycloakService.refresh_user_token(request.refresh_token)


@router.get("/me", response_model=Dict[str, Any])
def check_session(current_user: Dict[str, Any] = Depends(deps.get_current_user)):
    """
    Check current session validity and return user details.
    """
    return current_user
