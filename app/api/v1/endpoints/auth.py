"""
Authentication and token management endpoints.
"""

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/token")
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Any:
    """
    Proxy token request to Keycloak.
    Enables Swagger UI to authenticate directly with Keycloak via the API.
    """
    token_url = f"{settings.keycloak_url}/realms/{settings.keycloak_realm}/protocol/openid-connect/token"

    payload = {
        "grant_type": "password",
        "client_id": settings.keycloak_client_id,
        "username": form_data.username,
        "password": form_data.password,
        "scope": "openid profile email",
    }

    # Optional: Add client_secret if your client is not public
    logger.info(f"Proxying token request to: {token_url}")
    logger.info("Payload (no pass): {k:v for k,v in payload.items() if k!='password'}")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data=payload,
                timeout=10.0,
            )
            logger.info(f"Keycloak response: {response.status_code}")

            if response.status_code != 200:
                logger.error(
                    f"Keycloak token exchange failed: {response.status_code} - {response.text}"
                )
                try:
                    error_data = response.json()
                    detail = error_data.get(
                        "error_description", "Authentication failed"
                    )
                except Exception:
                    detail = "Authentication service returned an error"

                raise HTTPException(
                    status_code=response.status_code,
                    detail=detail,
                    headers={"WWW-Authenticate": "Bearer"},
                )

            return response.json()

    except httpx.RequestError as exc:
        logger.error(f"Connection error to Keycloak: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is currently unavailable",
        )
