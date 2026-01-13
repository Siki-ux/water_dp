from unittest.mock import patch

from app.core.exceptions import ResourceNotFoundException, TimeSeriesException


def test_get_stations_success(client):
    with patch("app.api.v1.endpoints.water_data.TimeSeriesService") as MockService:
        MockService.return_value.get_stations.return_value = []

        response = client.get("/api/v1/water-data/stations")
        assert response.status_code == 200
        assert response.json()["total"] == 0


def test_get_station_success(client):
    from datetime import datetime

    mock_station = {
        "id": 1,
        "name": "S1",
        "station_id": "ST_1",
        "latitude": 0,
        "longitude": 0,
        "station_type": "river",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    with patch("app.api.v1.endpoints.water_data.TimeSeriesService") as MockService:
        MockService.return_value.get_station.return_value = mock_station

        response = client.get("/api/v1/water-data/stations/ST_1")
        assert response.status_code == 200
        assert response.json()["station_id"] == "ST_1"


def test_get_station_not_found(client):
    with patch("app.api.v1.endpoints.water_data.TimeSeriesService") as MockService:
        MockService.return_value.get_station.side_effect = ResourceNotFoundException(
            "Not found"
        )

        response = client.get("/api/v1/water-data/stations/ST_MISSING")
        assert response.status_code == 404


def test_create_data_point_error(client):
    # Use valid enum for parameter: water_level
    data = {
        "station_id": 1,
        "timestamp": "2023-01-01T00:00:00Z",
        "value": 10.0,
        "parameter": "water_level",
        "unit": "m",
    }
    with patch("app.api.v1.endpoints.water_data.TimeSeriesService") as MockService:
        MockService.return_value.create_data_point.side_effect = TimeSeriesException(
            "Fail"
        )

        response = client.post("/api/v1/water-data/data-points", json=data)
        assert response.status_code == 500
