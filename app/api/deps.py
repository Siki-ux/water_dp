"""
API Dependencies for Authentication and Authorization.
"""

from typing import Any, Callable, Dict

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.core.config import settings
from app.core.security import verify_token

oauth2_scheme = OAuth2PasswordBearer(
    # auto_error=False prevents FastAPI from automatically raising 401.
    # We handle this manually in get_current_user to return a consistent error structure.
    tokenUrl=f"{settings.api_prefix}/auth/token",
    auto_error=False,
)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Validate the Bearer token and return the user payload.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    payload = await verify_token(token)
    return payload


def has_role(required_role: str) -> Callable:
    """
    Dependency factory to check if the user has a specific role.
    Keycloak roles are typically in 'realm_access' -> 'roles'.
    """

    async def role_checker(current_user: Dict[str, Any] = Depends(get_current_user)):
        realm_access = current_user.get("realm_access", {})
        roles = realm_access.get("roles", [])

        if required_role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Operation requires '{required_role}' role",
            )
        return current_user

    return role_checker


async def get_current_active_superuser(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Check if the user is a superuser (admin).
    """
    realm_access = current_user.get("realm_access", {})
    roles = realm_access.get("roles", [])
    if "admin" not in roles and "admin-siki" not in roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The user doesn't have enough privileges",
        )
    return current_user
