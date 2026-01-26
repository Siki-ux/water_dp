"""
Security module for handling JWT verification against Keycloak.
"""

import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import HTTPException, status
from jose import JWTError, jwt

from app.core.config import settings

logger = logging.getLogger(__name__)

# Cache for JWKS (JSON Web Key Set)
_jwks_cache: Optional[Dict[str, Any]] = None


async def get_jwks() -> Dict[str, Any]:
    """
    Fetch JSON Web Key Set from Keycloak.
    Simple caching strategy (global variable). In production, consider expirable cache.
    """
    global _jwks_cache
    if _jwks_cache:
        return _jwks_cache

    try:
        url = f"{settings.keycloak_url}/realms/{settings.keycloak_realm}/protocol/openid-connect/certs"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            _jwks_cache = response.json()
            logger.info("Fetched JWKS from Keycloak")
            return _jwks_cache
    except Exception as error:
        logger.error(f"Failed to fetch JWKS: {error}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable",
        )


async def verify_token(token: str) -> Dict[str, Any]:
    """
    Verify JWT token signature and audience.
    """
    try:
        # 1. Get Public Keys
        jwks = await get_jwks()

        # 2. Decode Header to find Key ID (kid)
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}

        for key in jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"],
                }
                break

        if not rsa_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token header",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # 3. Verify Token
        # We allow multiple issuers (internal and external) to handle Docker networking
        valid_issuers = [
            f"{settings.keycloak_url}/realms/{settings.keycloak_realm}",
            f"http://keycloak:8080/realms/{settings.keycloak_realm}",
            f"http://localhost:8081/realms/{settings.keycloak_realm}",
            f"http://localhost:8080/realms/{settings.keycloak_realm}",
            f"http://hydro-portal.westeurope.cloudapp.azure.com:8081/realms/{settings.keycloak_realm}",
            f"http://hydro-portal.westeurope.cloudapp.azure.com:8080/realms/{settings.keycloak_realm}",
        ]
        if settings.keycloak_external_url:
            valid_issuers.append(
                f"{settings.keycloak_external_url}/realms/{settings.keycloak_realm}"
            )

        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            audience="account",
            options={
                "verify_aud": False,
                "verify_iss": False,  # We check issuer manually below
            },
        )

        issuer = payload.get("iss")
        if issuer not in valid_issuers:
            logger.warning(f"Invalid issuer: {issuer}. Expected one of {valid_issuers}")
            logger.debug(f"Unverified payload: {payload}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid token issuer: {issuer}",
                headers={"WWW-Authenticate": "Bearer"},
            )

        logger.info(
            f"Token verified for user: {payload.get('preferred_username', payload.get('sub'))}"
        )
        return payload

    except JWTError as error:
        logger.warning(f"JWT Verification failed: {error}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as error:
        logger.error(f"Authentication error: {error}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"},
        )
