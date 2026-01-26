from unittest.mock import MagicMock, patch
import pytest
from app.services.keycloak_service import KeycloakService
from app.core.exceptions import AuthenticationException, ConfigurationException

@pytest.fixture
def mock_settings():
    with patch("app.services.keycloak_service.settings") as mock:
        mock.keycloak_url = "http://localhost:8080"
        mock.keycloak_realm = "test-realm"
        mock.keycloak_client_id = "test-client"
        mock.keycloak_admin_client_id = "admin-cli"
        mock.keycloak_admin_client_secret = "secret"
        yield mock

@pytest.fixture
def mock_admin_client():
    with patch("app.services.keycloak_service.KeycloakAdmin") as mock:
        yield mock

def test_get_token_url(mock_settings):
    url = KeycloakService._get_token_url()
    assert url == "http://localhost:8080/realms/test-realm/protocol/openid-connect/token"

@patch("app.services.keycloak_service.requests")
def test_login_user_success(mock_requests, mock_settings):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "token", "refresh_token": "refresh"}
    mock_requests.post.return_value = mock_response

    result = KeycloakService.login_user("user", "pass")
    assert result["access_token"] == "token"
    mock_requests.post.assert_called_once()

@patch("app.services.keycloak_service.requests")
def test_login_user_failure(mock_requests, mock_settings):
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_requests.post.return_value = mock_response

    with pytest.raises(AuthenticationException):
        KeycloakService.login_user("user", "pass")

@patch("app.services.keycloak_service.requests")
def test_refresh_user_token_success(mock_requests, mock_settings):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "new_token"}
    mock_requests.post.return_value = mock_response

    result = KeycloakService.refresh_user_token("refresh_token")
    assert result["access_token"] == "new_token"

def test_get_admin_client_initialization(mock_settings, mock_admin_client):
    KeycloakService._admin_client = None
    client = KeycloakService.get_admin_client()
    assert client is not None
    mock_admin_client.assert_called_once()

def test_get_user_by_username(mock_settings, mock_admin_client):
    KeycloakService._admin_client = MagicMock()
    KeycloakService._admin_client.get_users.return_value = [{"id": "1", "username": "test"}]
    
    user = KeycloakService.get_user_by_username("test")
    assert user["username"] == "test"
    KeycloakService._admin_client.get_users.assert_called_with(query={"username": "test", "exact": True})

def test_create_group(mock_settings, mock_admin_client):
    KeycloakService._admin_client = MagicMock()
    KeycloakService._admin_client.create_group.return_value = "group-id"
    
    group_id = KeycloakService.create_group("new-group")
    assert group_id == "group-id"
    KeycloakService._admin_client.create_group.assert_called_with({"name": "new-group"})
