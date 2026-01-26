import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.exceptions import (
    DatabaseException,
    GeoServerException,
    ResourceNotFoundException,
    TimeSeriesException,
    ValidationException,
)
from app.core.middleware import ErrorHandlingMiddleware


@pytest.fixture
def client():
    app = FastAPI()
    app.add_middleware(ErrorHandlingMiddleware)

    @app.get("/success")
    async def success():
        return {"message": "success"}

    @app.get("/not-found")
    async def not_found():
        raise ResourceNotFoundException("Item not found", {"id": "123"})

    @app.get("/validation-error")
    async def validation_error():
        raise ValidationException("Invalid input")

    @app.get("/service-error")
    async def service_error():
        raise TimeSeriesException("Service failure")

    @app.get("/database-error")
    async def database_error():
        raise DatabaseException("DB failure")

    @app.get("/geoserver-error")
    async def geoserver_error():
        raise GeoServerException("GeoServer failure")

    @app.get("/unknown-error")
    async def unknown_error():
        raise Exception("Unexpected error")

    with TestClient(app) as client:
        yield client


def test_success(client):
    response = client.get("/success")
    assert response.status_code == 200
    assert response.json() == {"message": "success"}


def test_resource_not_found(client):
    response = client.get("/not-found")
    assert response.status_code == 404
    data = response.json()
    assert data["error"]["message"] == "Item not found"
    assert data["error"]["code"] == "ResourceNotFoundException"
    assert "X-Error-Details" in response.headers

    assert "'id': '123'" in response.headers["X-Error-Details"]


def test_validation_error(client):
    response = client.get("/validation-error")
    assert response.status_code == 422
    data = response.json()
    assert data["error"]["message"] == "Invalid input"


def test_service_error_timeseries(client):
    response = client.get("/service-error")
    assert response.status_code == 500
    data = response.json()
    assert data["error"]["message"] == "Time series processing failed"


def test_service_error_database(client):
    response = client.get("/database-error")
    assert response.status_code == 500
    data = response.json()
    assert data["error"]["message"] == "Database operation failed"


def test_service_error_geoserver(client):
    response = client.get("/geoserver-error")
    assert response.status_code == 503
    data = response.json()
    assert data["error"]["message"] == "GeoServer operation failed"


def test_unknown_error(client):
    response = client.get("/unknown-error")
    assert response.status_code == 500
    data = response.json()
    assert data["error"]["message"] == "An unexpected error occurred."
    assert data["error"]["code"] == "InternalServerException"
