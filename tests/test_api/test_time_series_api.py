from unittest.mock import patch

from app.core.exceptions import ResourceNotFoundException, TimeSeriesException

# Override get_db fixture


def test_get_time_series_metadata_success(client, mock_db_session):
    # Mock Service Response
    from datetime import datetime

    from app.schemas.time_series import DataType, QualityLevel, SourceType

    mock_meta = [
        {
            "id": "1",
            "series_id": "DS_1",
            "name": "DS_1",
            "description": "Desc",
            "parameter": "Water Level",
            "unit": "m",
            "source_type": SourceType.SENSOR,
            "source_id": None,
            "data_type": DataType.CONTINUOUS,
            "start_time": datetime.now(),
            "end_time": None,
            "time_zone": "UTC",
            "sampling_rate": None,
            "quality_level": QualityLevel.RAW,
            "processing_notes": None,
            "interval": "variable",
            "is_active": True,
            "data_retention_days": 365,
            "properties": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
    ]

    with patch("app.api.v1.endpoints.time_series.TimeSeriesService") as MockService:
        MockService.return_value.get_time_series_metadata.return_value = mock_meta

        response = client.get("/api/v1/time-series/metadata")
        assert response.status_code == 200
        data = response.json()
        assert len(data["series"]) == 1
        assert data["series"][0]["series_id"] == "DS_1"


def test_get_time_series_metadata_error(client):
    with patch("app.api.v1.endpoints.time_series.TimeSeriesService") as MockService:
        MockService.return_value.get_time_series_metadata.side_effect = (
            TimeSeriesException("Fail")
        )

        response = client.get("/api/v1/time-series/metadata")
        assert response.status_code == 500
        assert response.json()["detail"] == "Time series processing failed"


def test_get_metadata_by_id_success(client):
    from datetime import datetime

    from app.schemas.time_series import DataType, QualityLevel, SourceType

    mock_meta = {
        "id": "1",
        "series_id": "DS_1",
        "name": "DS_1",
        "description": "Desc",
        "parameter": "Water Level",
        "unit": "m",
        "source_type": SourceType.SENSOR,
        "source_id": None,
        "data_type": DataType.CONTINUOUS,
        "start_time": datetime.now(),
        "end_time": None,
        "time_zone": "UTC",
        "sampling_rate": None,
        "quality_level": QualityLevel.RAW,
        "processing_notes": None,
        "interval": "variable",
        "is_active": True,
        "data_retention_days": 365,
        "properties": None,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    with patch("app.api.v1.endpoints.time_series.TimeSeriesService") as MockService:
        MockService.return_value.get_time_series_metadata_by_id.return_value = mock_meta

        response = client.get("/api/v1/time-series/metadata/DS_1")
        assert response.status_code == 200
        assert response.json()["series_id"] == "DS_1"


def test_get_metadata_by_id_not_found(client):
    with patch("app.api.v1.endpoints.time_series.TimeSeriesService") as MockService:
        MockService.return_value.get_time_series_metadata_by_id.side_effect = (
            ResourceNotFoundException("Not found")
        )

        response = client.get("/api/v1/time-series/metadata/DS_MISSING")
        assert response.status_code == 404
        assert response.json()["detail"] == "Not found"


def test_get_time_series_data_success(client):
    with patch("app.api.v1.endpoints.time_series.TimeSeriesService") as MockService:
        MockService.return_value.get_time_series_data.return_value = []

        # Correct path: /api/v1/time-series/data?series_id=DS_1
        response = client.get("/api/v1/time-series/data?series_id=DS_1")
        assert response.status_code == 200
        assert response.json()["total"] == 0


def test_get_time_series_data_validation_error(client):
    # Test invalid datetime format which raises error in endpoint or pydantic/parsing
    response = client.get("/api/v1/time-series/data?series_id=DS_1&start_time=INVALID")
    assert response.status_code == 400
    # Note: We kept `except ValueError` in endpoint for datetime parsing in Step 95
