"""
Tests for security core module and API dependencies.
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.api.deps import get_current_user, has_role
from app.core.security import verify_token


@pytest.fixture
def mock_jwks():
    return {
        "keys": [
            {
                "kid": "test-kid",
                "kty": "RSA",
                "use": "sig",
                "n": "test-n",
                "e": "test-e",
            }
        ]
    }


@pytest.mark.asyncio
async def test_verify_token_valid(mock_jwks):
    """Test verification of a valid token."""
    # Mock get_jwks to be an async function returning mock_jwks
    with patch("app.core.security.get_jwks", new_callable=AsyncMock) as mock_get_jwks:
        mock_get_jwks.return_value = mock_jwks
        with patch("jose.jwt.get_unverified_header", return_value={"kid": "test-kid"}):
            with patch(
                "jose.jwt.decode",
                return_value={
                    "sub": "user-123",
                    "realm_access": {"roles": ["user"]},
                    "iss": "http://localhost:8081/realms/timeio",
                },
            ):
                payload = await verify_token("valid-token")
                assert payload["sub"] == "user-123"


@pytest.mark.asyncio
async def test_get_current_user_valid():
    """Test dependency returns user payload on valid token."""
    # Mock verify_token to be an async function returning user payload
    with patch("app.api.deps.verify_token", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = {"sub": "user-123"}
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="valid-token")
        user = await get_current_user(creds)
        assert user["sub"] == "user-123"


@pytest.mark.asyncio
async def test_has_role_allowed():
    """Test RBAC allows access when role is present."""
    mock_user = {"realm_access": {"roles": ["admin", "editor"]}}

    checker = has_role("admin")
    # The dependency itself is async if it depends on async get_current_user,
    # but here we are testing the return value of has_role (the callable `role_checker`).
    # `role_checker` depends on `current_user`. In integration it's resolved.
    # In unit test, we call `role_checker(current_user=mock_user)`.
    # `role_checker` is defined as `async def role_checker`.
    result = await checker(current_user=mock_user)
    assert result == mock_user


@pytest.mark.asyncio
async def test_has_role_forbidden():
    """Test RBAC denies access when role is missing."""
    mock_user = {"realm_access": {"roles": ["editor"]}}

    checker = has_role("admin")
    with pytest.raises(HTTPException) as exc:
        await checker(current_user=mock_user)
    assert exc.value.status_code == 403
