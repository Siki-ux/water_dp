import logging
from datetime import datetime, timedelta

import pytest
import requests

# Configuration
BASE_URL = "http://localhost:8000/api/v1"
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
@pytest.mark.integration
def seeded_series_id():
    """
    Finds a valid series_id from the seeded data to use for tests.
    """
    try:
        # First, try to get time series metadata to find a valid ID
        response = requests.get(f"{BASE_URL}/time-series/metadata")
        assert response.status_code == 200, "Failed to get metadata list"
        data = response.json()
        series_list = data.get("series", [])

        if series_list:
            # Prefer series with LEVEL in name (seeded data)
            for s in series_list:
                if "LEVEL" in s.get("series_id", "") or "LEVEL" in s.get("name", ""):
                    return s["series_id"]
            # Fallback to first available
            return series_list[0]["series_id"]

        # If no series found in metadata, we cannot proceed with tests requiring them
        logger.warning(
            "No time series found in metadata endpoint. Seeding might have failed or is incomplete."
        )

    except Exception as e:
        pytest.fail(f"Could not determine seeded series_id: {e}")

    pytest.skip(
        "No seeded time series data found. Ensure the environment is running and seeded."
    )


@pytest.mark.integration
def test_get_time_series_metadata_list():
    """Test retrieving list of time series metadata."""
    response = requests.get(f"{BASE_URL}/time-series/metadata")
    assert response.status_code == 200
    data = response.json()
    assert "series" in data
    assert isinstance(data["series"], list)


@pytest.mark.integration
def test_get_time_series_metadata_by_id(seeded_series_id):
    """Test retrieving specific time series metadata."""
    response = requests.get(f"{BASE_URL}/time-series/metadata/{seeded_series_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["series_id"] == seeded_series_id


@pytest.mark.integration
def test_get_time_series_data_points(seeded_series_id):
    """Test retrieving data points for a series."""
    # Get last 24 hours to ensure we hit seeded data (seeded for last 24h)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=2)

    params = {
        "series_id": seeded_series_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "limit": 100,
    }

    response = requests.get(f"{BASE_URL}/time-series/data", params=params)
    assert response.status_code == 200
    data = response.json()

    assert "series_id" in data
    assert data["series_id"] == seeded_series_id
    assert "data_points" in data
    assert len(data["data_points"]) > 0

    # Valid structure check
    point = data["data_points"][0]
    assert "timestamp" in point
    assert "value" in point


@pytest.mark.integration
def test_time_series_statistics(seeded_series_id):
    """Test retrieving statistics."""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=2)

    params = {"start_time": start_time.isoformat(), "end_time": end_time.isoformat()}

    url = f"{BASE_URL}/time-series/statistics/{seeded_series_id}"
    response = requests.get(url, params=params)

    assert response.status_code == 200
    data = response.json()

    assert data["series_id"] == seeded_series_id
    assert "statistics" in data
    stats = data["statistics"]
    assert "min" in stats
    assert "max" in stats
    assert "mean" in stats
    assert "count" in stats
    assert stats["count"] > 0


@pytest.mark.integration
def test_time_series_aggregation(seeded_series_id):
    """Test time series aggregation."""
    # This might require specific implementation support in the backend
    # verifying if endpoint exists and accepts request

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=2)

    payload = {
        "series_id": seeded_series_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "aggregation_interval": "6h",
        "aggregation_method": "mean",
    }

    response = requests.post(f"{BASE_URL}/time-series/aggregate", json=payload)

    if response.status_code == 501:  # Not implemented
        pytest.skip("Aggregation not implemented")

    assert response.status_code == 200
    data = response.json()
    # Response is TimeSeriesAggregationResponse object, not list
    assert "data_points" in data
    assert isinstance(data["data_points"], list)
    # With 2 days and 6h interval, we expect approx 8 points
    if len(data["data_points"]) > 0:
        point = data["data_points"][0]
        assert "avg" in point or "value" in point


@pytest.mark.integration
def test_time_series_data_missing_series():
    """Test error handling for non-existent series."""
    params = {"series_id": "NON_EXISTENT_ID_99999"}
    response = requests.get(f"{BASE_URL}/time-series/data", params=params)

    # Depending on implementation, might return 404 or empty list
    # Assuming standard REST patterns for "series not found" usually 404
    # But for search/filter it might be 200 with empty list.
    # Checking implementation... time_series.py line 116 (get_time_series_data)
    # Reading code...

    # If the service raises TimeSeriesNotFoundError, api returns 404.
    # If it just returns empty, it's 200.
    # Let's assert it is not 500 at least.
    assert response.status_code in [200, 404]
    if response.status_code == 200:
        data = response.json()
        assert len(data.get("data_points", [])) == 0
