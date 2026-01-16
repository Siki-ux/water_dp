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
        "id": "ST_1",
        "name": "S1",
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
        assert response.json()["id"] == "ST_1"


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
        "timestamp": "2023-01-01T00:00:00Z",
        "value": 10.0,
        "parameter": "water_level",
        "unit": "m",
    }
    with patch("app.api.v1.endpoints.water_data.TimeSeriesService") as MockService:
        MockService.return_value.create_data_point.side_effect = TimeSeriesException(
            "Fail"
        )

        response = client.post("/api/v1/water-data/stations/1/data-points", json=data)
        assert response.status_code == 500


def test_get_data_points_pagination(client):
    """Test data points retrieval with pagination (limit/offset)."""
    with patch("app.api.v1.endpoints.water_data.TimeSeriesService") as MockService:
        # Reset and mock real flow
        ds = [
            {
                "name": "DS_1",
                "ObservedProperty": {"name": "Water Level"},
                "unitOfMeasurement": {"name": "m"},
            }
        ]
        MockService.return_value.get_datastreams_for_station.return_value = ds
        MockService.return_value.get_time_series_data.return_value = []

        response = client.get(
            "/api/v1/water-data/data-points?id=ST_1&limit=10&offset=5"
        )
        assert response.status_code == 200

        # Check call arguments
        # The second call to service (get_time_series_data) should have the query
        call_args = MockService.return_value.get_time_series_data.call_args
        query_obj = call_args[0][0]
        assert query_obj.limit == 10
        assert query_obj.offset == 5
