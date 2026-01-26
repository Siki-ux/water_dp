from unittest.mock import patch
from fastapi.testclient import TestClient
from app.main import app
from app.api.deps import get_current_user

client = TestClient(app)

@patch("app.services.keycloak_service.KeycloakService.login_user")
def test_login_access_token(mock_login):
    mock_login.return_value = {
        "access_token": "token",
        "refresh_token": "ref",
        "token_type": "bearer",
        "expires_in": 300
    }
    
    response = client.post("/api/v1/auth/token", data={"username": "u", "password": "p"})
    assert response.status_code == 200
    assert response.json()["access_token"] == "token"

@patch("app.services.keycloak_service.KeycloakService.login_user")
def test_login_json(mock_login):
    mock_login.return_value = {
        "access_token": "token",
        "refresh_token": "ref",
        "token_type": "bearer",
        "expires_in": 300
    }
    
    response = client.post("/api/v1/auth/login", json={"username": "u", "password": "p"})
    assert response.status_code == 200
    assert response.json()["access_token"] == "token"

@patch("app.services.keycloak_service.KeycloakService.refresh_user_token")
def test_refresh_token(mock_refresh):
    mock_refresh.return_value = {
        "access_token": "new",
        "refresh_token": "ref",
        "token_type": "bearer",
        "expires_in": 300
    }
    
    response = client.post("/api/v1/auth/refresh", json={"refresh_token": "old"})
    assert response.status_code == 200
    assert response.json()["access_token"] == "new"

def test_check_session():
    # Use dependency override
    app.dependency_overrides[get_current_user] = lambda: {"sub": "uid", "username": "u"}
    
    response = client.get("/api/v1/auth/me")
    assert response.status_code == 200
    assert response.json()["sub"] == "uid"
    app.dependency_overrides = {}
