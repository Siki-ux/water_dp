from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from app.core.exceptions import ResourceNotFoundException, TimeSeriesException
from app.schemas.water_data import ParameterType, QualityFlag, WaterDataPointCreate
from app.services.time_series_service import TimeSeriesService


class TestTimeSeriesServiceCoverage:
    @pytest.fixture
    def service(self, mock_db_session):
        return TimeSeriesService(mock_db_session)

    def test_get_int_id_coverage(self, service):
        """Test _get_int_id with various inputs."""
        # Integer string
        assert service._get_int_id("123") == 123
        assert service._get_int_id(123) == 123

        # Non-integer string (hash)
        val = service._get_int_id("test_string")
        assert isinstance(val, int)
        assert val > 0

        # Same string same hash
        assert service._get_int_id("test_string") == val

        # Different string different hash
        assert service._get_int_id("other_string") != val

    def test_get_datastreams_for_station_coverage(self, service):
        """Test get_datastreams_for_station with filters."""
        mock_response = {"value": [{"@iot.id": 1, "name": "DS1"}]}
        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = mock_response

            # Basic call
            ds = service.get_datastreams_for_station(100)
            assert len(ds) == 1
            # URL should contain Things(100)
            assert "Things(100)" in mock_get.call_args[0][0]
            # Check expand params
            expand_str = mock_get.call_args[1]["params"]["$expand"]
            assert "Thing/Locations" in expand_str

            # With parameter filter
            ds_param = service.get_datastreams_for_station(100, parameter="Water Temp")
            assert len(ds_param) == 1
            filter_str = mock_get.call_args[1]["params"]["$filter"]
            assert "ObservedProperty/name eq 'Water Temp'" in filter_str

    def test_get_time_series_metadata_by_id_coverage(self, service):
        """Test get_time_series_metadata_by_id success and failure."""
        mock_val = {
            "@iot.id": 55,
            "name": "TargetDS",
            "description": "Desc",
            "Thing": {"name": "St1"},
            "ObservedProperty": {"name": "Param1"},
            "unitOfMeasurement": {"name": "Unit1"},
            "phenomenonTime": "2023-01-01T00:00:00Z/2023-01-02T00:00:00Z",
        }

        with patch("app.services.time_series_service.requests.get") as mock_get:
            # 1. Success
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"value": [mock_val]}

            meta = service.get_time_series_metadata_by_id("TargetDS")
            assert meta.series_id == "TargetDS"
            assert meta.station_id == "St1"
            assert meta.start_time.year == 2023

            # 2. Not Found (Empty value)
            mock_get.return_value.json.return_value = {"value": []}
            with pytest.raises(ResourceNotFoundException):
                service.get_time_series_metadata_by_id("MissingDS")

            # 3. JSON Error
            mock_get.return_value.json.side_effect = ValueError("Bad JSON")
            assert service.get_time_series_metadata_by_id("BadJsonDS") is None

            # 4. Request Exception
            mock_get.return_value.json.side_effect = None
            mock_get.side_effect = requests.exceptions.RequestException("Net Error")
            with pytest.raises(TimeSeriesException):
                service.get_time_series_metadata_by_id("NetError")

    def test_create_data_point_coverage(self, service):
        """Test create_data_point including Datastream lookup."""
        data_point = WaterDataPointCreate(
            timestamp=datetime(2023, 1, 1, 12, 0),
            value=42.0,
            parameter=ParameterType.TEMPERATURE,  # Ensure using Enum if required, or string
            quality_flag=QualityFlag.GOOD,
            unit="C",
        )

        with patch("app.services.time_series_service.requests.get") as mock_get, patch(
            "app.services.time_series_service.requests.post"
        ) as mock_post:

            # 1. Datastream Lookup Success
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"value": [{"@iot.id": 999}]}

            # 2. Observation Create Success
            mock_post.return_value.status_code = 201
            mock_post.return_value.headers = {
                "Location": "http://frost/Observations(888)"
            }

            res = service.create_data_point("10", data_point)

            assert res["id"] == "888"
            assert res["value"] == 42.0

            # Verify DS lookup used correct name format
            # DS_{station_id}_{parameter} -> DS_10_temperature
            filter_arg = mock_get.call_args[1]["params"]["$filter"]
            assert "DS_10_temperature" in filter_arg

    def test_create_data_point_no_datastream(self, service):
        """Test create_data_point when datastream doesn't exist."""
        data_point = WaterDataPointCreate(
            timestamp=datetime.now(),
            value=1.0,
            parameter=ParameterType.WATER_LEVEL,
            quality_flag=QualityFlag.GOOD,
            unit="m",
        )
        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"value": []}  # Empty

            with pytest.raises(TimeSeriesException) as exc:
                service.create_data_point("10", data_point)
            assert "Datastream" in str(exc.value)

    def test_get_latest_data_coverage(self, service):
        """Test get_latest_data logic."""

        # Mock Datastreams response
        ds_resp = {
            "value": [
                {
                    "@iot.id": 101,
                    "ObservedProperty": {"name": "Temp"},
                    "unitOfMeasurement": {"name": "C"},
                },
                {
                    "@iot.id": 102,
                    "ObservedProperty": {"name": "Level"},
                    "unitOfMeasurement": {"name": "m"},
                },
            ]
        }

        # Mock Observations response (latest)
        obs_resp_1 = {
            "value": [
                {
                    "@iot.id": 5001,
                    "phenomenonTime": "2023-01-01T12:00:00Z",
                    "result": 25.0,
                }
            ]
        }
        obs_resp_2 = {"value": []}  # No data for second DS

        with patch("app.services.time_series_service.requests.get") as mock_get:
            # Sequence:
            # 1. Get Datastreams
            # 2. Get Obs for DS 101
            # 3. Get Obs for DS 102

            mock_get.side_effect = [
                MagicMock(
                    status_code=200, json=lambda: ds_resp, raise_for_status=lambda: None
                ),
                MagicMock(
                    status_code=200,
                    json=lambda: obs_resp_1,
                    raise_for_status=lambda: None,
                ),
                MagicMock(
                    status_code=200,
                    json=lambda: obs_resp_2,
                    raise_for_status=lambda: None,
                ),
            ]

            results = service.get_latest_data(station_id=50)

            assert len(results) == 1
            assert results[0]["parameter"] == "temp"
            assert results[0]["value"] == 25.0
            assert "station_id" not in results[0]
            assert mock_get.call_count == 3

    def test_unexpected_json_errors(self, service):
        """Test handling of malformed JSON from FROST."""
        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.side_effect = ValueError("Invalid JSON")

            # Test get_stations failure
            with pytest.raises(TimeSeriesException) as exc:
                service.get_stations()
            assert "invalid JSON" in str(exc.value)

    def test_get_time_series_data_filtering(self, service):
        """Test get_time_series_data with time filters and limit."""
        from app.schemas.time_series import TimeSeriesQuery

        query = TimeSeriesQuery(
            series_id="DS1",
            start_time=datetime(2023, 1, 1, 10, 0),
            end_time=datetime(2023, 1, 1, 12, 0),
            limit=50,
        )

        mock_resp = {
            "value": [
                {"@iot.id": 1, "phenomenonTime": "2023-01-01T10:00:00Z", "result": 10.0}
            ]
        }

        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = mock_resp

            results = service.get_time_series_data(query)

            assert len(results) == 1
            # Check params
            params = mock_get.call_args[1]["params"]
            assert params["$top"] == 50
            assert "phenomenonTime ge" in params["$filter"]
            assert "le" in params["$filter"]

    def test_aggregate_time_series_coverage(self, service):
        """Test aggregation logic branches."""
        from app.schemas.time_series import TimeSeriesAggregation

        # We mock get_time_series_data to return raw data
        mock_data = [
            MagicMock(timestamp=datetime(2023, 1, 1, 10, 0), value=10.0),
            MagicMock(timestamp=datetime(2023, 1, 1, 10, 30), value=20.0),
            MagicMock(timestamp=datetime(2023, 1, 1, 11, 0), value=5.0),
        ]

        with patch.object(service, "get_time_series_data", return_value=mock_data):
            # Test MAX aggregation
            agg_req = TimeSeriesAggregation(
                series_id="DS1",
                start_time=datetime(2023, 1, 1, 10, 0),
                end_time=datetime(2023, 1, 1, 12, 0),
                aggregation_interval="1h",
                aggregation_method="max",
            )

            res = service.aggregate_time_series(agg_req)
            # 10:00-11:00 bucket -> max(10, 20) = 20
            # 11:00-12:00 bucket -> max(5) = 5

            assert len(res) >= 2
            vals = [p.value for p in res]
            assert 20.0 in vals
            assert 5.0 in vals
            assert res[0].aggregation_method == "max"

    def test_detect_anomalies_coverage(self, service):
        """Test anomaly detection logic."""
        # Mock data: 10 points usually 10.0, one point 100.0
        data = []
        base_time = datetime(2023, 1, 1, 12, 0)
        from datetime import timedelta

        for i in range(10):
            val = 10.0
            if i == 5:
                val = 100.0  # Anomaly
            data.append(MagicMock(timestamp=base_time + timedelta(hours=i), value=val))

        with patch.object(service, "get_time_series_data", return_value=data):
            # Statistical method (Z-score)
            anomalies = service.detect_anomalies(
                series_id="DS1",
                start=base_time,
                end=base_time + timedelta(hours=10),
                method="statistical",
                threshold=2.0,
            )
            assert len(anomalies) == 1
            assert anomalies[0]["value"] == 100.0
            assert anomalies[0]["score"] > 2.0

    def test_calculate_statistics_coverage(self, service):
        """Test statistics calculation."""
        # Mock data
        data = [MagicMock(value=10.0), MagicMock(value=20.0), MagicMock(value=30.0)]

        with patch.object(service, "get_time_series_data", return_value=data):
            stats = service.calculate_statistics(
                series_id="DS1",
                start_time=datetime(2023, 1, 1),
                end_time=datetime(2023, 1, 2),
            )

            s = stats.statistics
            assert s["count"] == 3
            assert s["min"] == 10.0
            assert s["max"] == 30.0
            assert s["mean"] == 20.0

    def test_get_station_statistics_coverage(self, service):
        """Test get_station_statistics with the new count/min/max implementation."""
        # Mock responses for:
        # 1. Datastreams lookup
        # 2. Observation count ($count=true)
        # 3. Observation min ($orderby=result asc)
        # 4. Observation max ($orderby=result desc)

        ds_resp = {"value": [{"@iot.id": "DS_1", "ObservedProperty": {"name": "Temp"}}]}
        count_resp = {"@iot.count": 100, "value": []}
        min_resp = {"value": [{"result": 5.0}]}
        max_resp = {"value": [{"result": 25.0}]}

        with patch("app.services.time_series_service.requests.get") as mock_get:
            mock_get.side_effect = [
                MagicMock(
                    status_code=200, json=lambda: ds_resp, raise_for_status=lambda: None
                ),
                MagicMock(status_code=200, json=lambda: count_resp),
                MagicMock(status_code=200, json=lambda: min_resp),
                MagicMock(status_code=200, json=lambda: max_resp),
            ]

            result = service.get_station_statistics(
                station_id="ST1", start_time=None, end_time=None
            )

            assert result["id"] == "ST1"
            assert result["total_measurements"] == 100
            assert result["statistics"]["min"] == 5.0
            assert result["statistics"]["max"] == 25.0
            assert "station_id" not in result
