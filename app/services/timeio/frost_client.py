"""
FROST Client
Wrapper for OGC SensorThings API interactions.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class FrostClient:
    """Client for interacting with a specific Project's FROST endpoint."""

    def __init__(self, base_url: str, timeout: int = 20):
        """
        Initialize FROST client.

        Args:
            base_url: The root URL for the project's FROST instance (e.g. http://frost:8080/project_x/v1.1)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, params: Dict = None) -> Any:
        url = self._url(path)
        try:
            resp = self._session.request(
                method, url, params=params, timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            logger.error(f"FROST request failed: {method} {url} - {e}")
            raise
        except Exception as e:
            logger.error(f"FROST request error: {method} {url} - {e}")
            raise

    def list_datastreams(self, thing_id: Any = None) -> List[Dict]:
        """
        List Datastreams, optionally filtered by Thing ID.
        """
        params = {}
        if thing_id:
            # Filter by Thing ID (navigation link or filter)
            # OGC STA: /Things(id)/Datastreams is efficient
            path = f"Things({thing_id})/Datastreams"
        else:
            path = "Datastreams"

        # Expand for context (ObservedProperty, Sensor, Unit)
        params["$expand"] = "ObservedProperty,Sensor,Thing"

        # Handle pagination manually? Or just get top X?
        # For now, get top 1000.
        params["$top"] = 1000

        data = self._request("GET", path, params=params)
        if not data:
            return []

        return data.get("value", [])

    def get_datastream(self, datastream_id: Any) -> Optional[Dict]:
        """Get Datastream details."""
        path = f"Datastreams({datastream_id})"
        params = {"$expand": "ObservedProperty,Sensor,Thing"}
        return self._request("GET", path, params=params)

    def get_thing(self, thing_id: Any) -> Optional[Dict]:
        """Get Thing details."""
        path = f"Things({thing_id})"
        return self._request("GET", path)

    def get_locations(self, thing_id: Any) -> List[Dict]:
        """Get Locations for a Thing."""
        path = f"Things({thing_id})/Locations"
        data = self._request("GET", path)
        if not data:
            return []
        return data.get("value", [])

    def get_observations(
        self,
        datastream_id: Any,
        start_time: str = None,
        end_time: str = None,
        limit: int = 1000,
    ) -> List[Dict]:
        """
        Get Observations for a Datastream.

        Args:
            datastream_id: ID of the Datastream
            start_time: ISO timestamp string
            end_time: ISO timestamp string
            limit: Max records
        """
        path = f"Datastreams({datastream_id})/Observations"
        params = {
            "$top": limit,
            "$orderby": "phenomenonTime desc",
            "$select": "phenomenonTime,result,resultTime",  # Optimize payload
        }

        # Time filter
        if start_time or end_time:
            # Format: phenomenonTime ge 2023-01-01T00:00:00Z and ...
            # STA supports ISO intervals too: min/max
            criteria = []
            if start_time:
                criteria.append(f"phenomenonTime ge {start_time}")
            if end_time:
                criteria.append(f"phenomenonTime le {end_time}")

            if criteria:
                params["$filter"] = " and ".join(criteria)

        data = self._request("GET", path, params=params)
        if not data:
            return []

        return data.get("value", [])
