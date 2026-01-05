from datetime import datetime, timedelta, timezone

import pytest

from app.models.time_series import TimeSeriesData
from app.schemas.time_series import (
    InterpolationRequest,
    TimeSeriesAggregation,
)
from app.services.time_series_service import TimeSeriesService


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
                TimeSeriesData(
                    series_id="TS1",
                    timestamp=start + timedelta(hours=i),
                    value=float(i),
                    quality_flag="good",
                    is_interpolated="false",
                    is_aggregated="false",
                )
            )
        return data

    def test_aggregate_time_series_mean(self, service, sample_data, monkeypatch):
        req = TimeSeriesAggregation(
            series_id="TS1",
            start_time=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
            end_time=datetime(2023, 1, 1, 22, 0, tzinfo=timezone.utc),
            aggregation_interval="1hour",
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
                TimeSeriesData(
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
            TimeSeriesData(
                series_id="TS1", timestamp=start, value=10.0, quality_flag="good"
            ),
            TimeSeriesData(
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
            interval="1hour",
            method="linear",
        )

        monkeypatch.setattr(service, "get_time_series_data", lambda query: data)

        result = service.interpolate_time_series(req)

        interpolated = [p for p in result if p.is_interpolated]
        assert len(interpolated) == 1
        assert interpolated[0].value == 15.0
