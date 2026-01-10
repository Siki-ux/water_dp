import json
from unittest.mock import patch
from uuid import uuid4


def test_import_json_file_upload(client):
    """Test importing data via JSON file upload (multipart)."""
    project_id = uuid4()
    station_id = "test_station"

    # Patch ProjectService used in the endpoint
    with patch(
        "app.api.v1.endpoints.project_data.ProjectService"
    ) as MockProjectService, patch(
        "app.api.v1.endpoints.project_data.TimeSeriesService"
    ) as MockTimeSeriesService:

        # Setup ProjectService Mock
        MockProjectService.list_sensors.return_value = [station_id]

        # Setup TimeSeriesService Mock (instantiated inside function)
        mock_ts_instance = MockTimeSeriesService.return_value
        mock_ts_instance.add_bulk_data.return_value = 2

        # Test Data
        data = [
            {
                "timestamp": "2026-01-01T10:00:00Z",
                "value": 10.0,
                "quality_flag": "good",
            },
            {"timestamp": "2026-01-01T11:00:00Z", "value": 11.0},
        ]

        # Prepare file
        file_content = json.dumps(data).encode("utf-8")
        files = {"file": ("dataset_001.json", file_content, "application/json")}

        response = client.post(
            f"/api/v1/projects/{project_id}/things/{station_id}/import?parameter=Level",
            files=files,
        )

        assert response.status_code == 200
        assert response.json()["imported"] == 2
        assert response.json()["status"] == "success"

        # Verify add_bulk_data called
        mock_ts_instance.add_bulk_data.assert_called_once()
        args = mock_ts_instance.add_bulk_data.call_args
        assert args[0][0] == f"DS_{station_id}_Level"  # series_id
        assert len(args[0][1]) == 2  # 2 points


def test_import_csv_file_upload(client):
    """Test importing data via CSV file upload (multipart)."""
    project_id = uuid4()
    station_id = "test_station"

    with patch(
        "app.api.v1.endpoints.project_data.ProjectService"
    ) as MockProjectService, patch(
        "app.api.v1.endpoints.project_data.TimeSeriesService"
    ) as MockTimeSeriesService:

        MockProjectService.list_sensors.return_value = [station_id]
        mock_ts_instance = MockTimeSeriesService.return_value
        mock_ts_instance.add_bulk_data.return_value = 2

        # CSV Content
        csv_content = "timestamp,value,quality_flag\n2026-01-01T10:00:00Z,10.0,good\n2026-01-01T11:00:00Z,11.0,good"
        files = {"file": ("data.csv", csv_content, "text/csv")}

        response = client.post(
            f"/api/v1/projects/{project_id}/things/{station_id}/import?parameter=Level",
            files=files,
        )

        assert response.status_code == 200
        assert response.json()["imported"] == 2


def test_import_json_body_raw(client):
    """Test importing data via JSON Body (application/json) on dedicated endpoint."""
    project_id = uuid4()
    station_id = "test_station"

    with patch(
        "app.api.v1.endpoints.project_data.ProjectService"
    ) as MockProjectService, patch(
        "app.api.v1.endpoints.project_data.TimeSeriesService"
    ) as MockTimeSeriesService:

        MockProjectService.list_sensors.return_value = [station_id]
        mock_ts_instance = MockTimeSeriesService.return_value
        mock_ts_instance.add_bulk_data.return_value = 2

        data = [
            {"timestamp": "2026-01-01T10:00:00Z", "value": 10.0},
            {"timestamp": "2026-01-01T11:00:00Z", "value": 11.0},
        ]

        # Use json= parameter and target correct endpoint
        response = client.post(
            f"/api/v1/projects/{project_id}/things/{station_id}/import-json?parameter=Level",
            json=data,
        )

        assert response.status_code == 200
        assert response.json()["imported"] == 2
