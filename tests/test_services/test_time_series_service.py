from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from app.schemas.time_series import (
    InterpolationRequest,
    TimeSeriesAggregation,
)
from app.services.time_series_service import TimeSeriesService


class MockTimeSeriesData:
    def __init__(
        self,
        series_id,
        timestamp,
        value,
        quality_flag="good",
        is_interpolated=False,
        is_aggregated=False,
    ):
        self.series_id = series_id
        self.timestamp = timestamp
        self.value = value
        self.quality_flag = quality_flag
        self.is_interpolated = is_interpolated
        self.is_aggregated = is_aggregated


class TestTimeSeriesService:
    @pytest.fixture
    def service(self, mock_db_session):
        return TimeSeriesService(mock_db_session)

    @pytest.fixture
    def sample_data(self):
        start = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
        data = []
        for i in range(10):
            data.append(
                MockTimeSeriesData(
                    series_id="TS1",
                    timestamp=start + timedelta(hours=i),
                    value=float(i),
                    quality_flag="good",
                    is_interpolated=False,
                    is_aggregated=False,
                )
            )
        return data

    def test_aggregate_time_series_mean(self, service, sample_data, monkeypatch):
        req = TimeSeriesAggregation(
            series_id="TS1",
            start_time=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
            end_time=datetime(2023, 1, 1, 22, 0, tzinfo=timezone.utc),
            aggregation_interval="1h",
            aggregation_method="mean",
        )

        # Monkeypatch the instance method
        monkeypatch.setattr(service, "get_time_series_data", lambda query: sample_data)

        result = service.aggregate_time_series(req)

        # With 1hour interval, we expect 10 points (for 10 hours of data)
        # Values are 0.0, 1.0, 2.0...
        assert len(result) == 10
        assert result[0].value == 0.0
        assert result[-1].value == 9.0
        assert result[0].count == 1

    def test_calculate_statistics(self, service, sample_data, monkeypatch):
        monkeypatch.setattr(service, "get_time_series_data", lambda query: sample_data)

        start = datetime.now()
        end = start + timedelta(hours=1)
        stats = service.calculate_statistics("TS1", start, end)

        assert stats.statistics["count"] == 10
        assert stats.statistics["min"] == 0.0

    def test_detect_anomalies_zscore(self, service, monkeypatch):
        start = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
        data = []
        for i in range(15):
            val = 100.0 if i == 3 else 10.0  # Anomaly at index 3
            data.append(
                MockTimeSeriesData(
                    series_id="TS1", timestamp=start + timedelta(hours=i), value=val
                )
            )

        monkeypatch.setattr(service, "get_time_series_data", lambda query: data)

        queries_start = datetime.now()
        queries_end = queries_start + timedelta(hours=24)
        anomalies = service.detect_anomalies(
            "TS1", queries_start, queries_end, method="statistical", threshold=1.5
        )

        assert len(anomalies) > 0
        assert anomalies[0]["value"] == 100.0

    def test_interpolate_time_series(self, service, monkeypatch):
        start = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
        data = [
            MockTimeSeriesData(
                series_id="TS1", timestamp=start, value=10.0, quality_flag="good"
            ),
            MockTimeSeriesData(
                series_id="TS1",
                timestamp=start + timedelta(hours=2),
                value=20.0,
                quality_flag="good",
            ),
        ]

        req = InterpolationRequest(
            series_id="TS1",
            start_time=start,
            end_time=start + timedelta(hours=2),
            interval="1h",
            method="linear",
        )

        monkeypatch.setattr(service, "get_time_series_data", lambda query: data)

        result = service.interpolate_time_series(req)

        interpolated = [p for p in result if p.is_interpolated]
        assert len(interpolated) == 1
        assert interpolated[0].value == 15.0

    # --- FROST API Interaction Tests ---

    def test_get_time_series_data_frost(self, service):
        """Test fetching data from FROST API (mocked)."""
        mock_response = {
            "value": [
                {
                    "@iot.id": 1,
                    "phenomenonTime": "2023-01-01T12:00:00Z",
                    "result": 10.5,
                },
                {
                    "@iot.id": 2,
                    "phenomenonTime": "2023-01-02T12:00:00Z",
                    "result": 11.0,
                },
            ]
        }

        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = mock_response

            from app.schemas.time_series import TimeSeriesQuery

            query = TimeSeriesQuery(
                series_id="DS_1",
                start_time="2023-01-01T00:00:00Z",
                end_time="2023-01-02T00:00:00Z",
            )

            data = service.get_time_series_data(query)

            assert len(data) == 2
            assert data[0].value == 10.5
            assert data[0].timestamp == datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
            mock_get.assert_called_once()

    def test_get_stations(self, service):
        """Test fetching stations from FROST (mocked)."""
        mock_response = {
            "value": [
                {
                    "@iot.id": 1,
                    "name": "Test Station",
                    "description": "A test station",
                    "properties": {"station_id": "ST_1", "type": "river"},
                    "Locations": [{"location": {"coordinates": [10.0, 50.0]}}],
                }
            ]
        }

        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = mock_response

            stations = service.get_stations()

            assert len(stations) == 1
            assert stations[0]["name"] == "Test Station"
            assert stations[0]["latitude"] == 50.0
            assert stations[0]["longitude"] == 10.0

    def test_get_station(self, service):
        """Test fetching a single station."""
        # 1. ID Lookup fails (404)
        # 2. Filter Lookup succeeds (200)

        mock_list_response = {
            "value": [
                {
                    "@iot.id": 1,
                    "name": "Test Station",
                    "properties": {"station_id": "ST_1"},
                }
            ]
        }

        # Responses for the two calls
        resp_404 = MagicMock()
        resp_404.status_code = 404

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = mock_list_response

        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.side_effect = [resp_404, resp_200]

            station = service.get_station("ST_1")
            assert station is not None
            assert station["station_id"] == "ST_1"
