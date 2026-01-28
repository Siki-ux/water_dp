"""
FROST Client
Wrapper for OGC SensorThings API interactions.
"""

import logging
from typing import Any, Dict, List, Optional
from functools import lru_cache

import requests

logger = logging.getLogger(__name__)


class FrostClient:
    """Client for interacting with a specific Project's FROST endpoint."""

    def __init__(self, base_url: str, project_name: str, version: str, frost_server: str, timeout: int = 20):
        """
        Initialize FROST client.

        Args:
            base_url: The root URL for the project's FROST instance (e.g. http://frost:8080)
            project_name: The name of the project (e.g. project_x)
            version: The version of the FROST API (e.g. v1.1)
            frost_server: The FROST server (e.g. sta)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.project_name = project_name
        self.version = version
        self.frost_server = frost_server
        self.timeout = timeout
        self._session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{self.project_name}/{self.version}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, params: Dict = None) -> Any:
        url = self._url(path)
        try:
            logger.info(f"FROST request: {method} {url} with params {params}")
            resp = self._session.request(
                method, url, params=params, timeout=self.timeout
            )
            logger.info(f"FROST response: {resp.status_code} {resp.text}")
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

    def list_datastreams(self, thing_id: Any, expand: str = "Thing") -> List[Dict]:
        """
        List Datastreams for a specific Thing.
        """
        path = f"Things({thing_id})/Datastreams"
        params = {"$expand": expand} if expand else None
        data = self._request("GET", path, params=params)
        if not data:
            logger.error(f"No datastreams found for thing {thing_id}")
            return []
        return data.get("value", [])

    def get_datastream(self, datastream_id: Any, expand:str = "Thing") -> Optional[Dict]:
        """Get Datastream details."""
        path = f"Datastreams({datastream_id})"
        params = {"$expand": expand} if expand else None
        data = self._request("GET", path, params=params)
        if not data:
            logger.error(f"No datastream found for datastream {datastream_id}")
            return []
        return data.get("value", [])

    def get_thing(self, thing_id: Any,  expand:str = None) -> Optional[Dict]:
        """Get Thing details."""
        params = {"$expand": expand} if expand else None
        path = f"Things({thing_id})"
        data = self._request("GET", path, params=params)
        if not data:
            logger.error(f"No thing found for thing {thing_id}")
            return None
        return data

    def get_observations(
        self,
        datastream_id: Any,
        start_time: str = None,
        end_time: str = None,
        limit: int = 1000,
        order_by: str = "phenomenonTime desc",
        select: str = "phenomenonTime,result,resultTime",
    ) -> List[Dict]:
        """
        Get Observations for a Datastream.

        Args:
            datastream_id: ID of the Datastream
            start_time: ISO timestamp string
            end_time: ISO timestamp string
            limit: Max records
            order_by: Order by
            select: Select fields
        """
        path = f"Datastreams({datastream_id})/Observations"
        params = {
            "$top": limit,
            "$orderby": order_by,
            "$select": select,
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

    def get_locations(self,expand: str = None) -> List[Dict[str, Any]]:
        """
        Get all locations of a things.
        """
        path = f"Locations"
        params = {"$expand": expand} if expand else None
        data = self._request("GET", path, params=params)
        if not data:
            return []
        return data.get("value", [])

    def get_things(self, expand: str = None, filter: str = None, top: int = None) -> List[Dict[str, Any]]:
        """
        Get all things with optional filtering and expansion.
        """
        path = "Things"
        params = {}
        if expand:
            params["$expand"] = expand
        if filter:
            params["$filter"] = filter
        if top:
            params["$top"] = top
            
        data = self._request("GET", path, params=params)
        if not data:
            return []
        return data.get("value", [])

@lru_cache(maxsize=128)
def get_cached_frost_client(base_url: str, project_name: str, version: str, frost_server: str, timeout: int = 20) -> FrostClient:
    """
    Returns a cached instance of FrostClient.
    Arguments must be hashable.
    """
    logger.info(f"Creating new FrostClient for {project_name} at {base_url}")
    return FrostClient(base_url=base_url, project_name=project_name, version=version, frost_server=frost_server, timeout=timeout)
