"""
Time series data processing and analysis service.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from sqlalchemy.orm import Session

from app.core.exceptions import (
    ResourceNotFoundException,
    TimeSeriesException,
)
from app.schemas.time_series import (
    AggregatedDataPoint,
    DataType,
    InterpolatedDataPoint,
    InterpolationRequest,
    SourceType,
    TimeSeriesAggregation,
    TimeSeriesMetadataResponse,
    TimeSeriesQuery,
    TimeSeriesStatistics,
)
from app.schemas.user_context import SensorCreate

logger = logging.getLogger(__name__)


class TimeSeriesService:
    """Service for time series data processing and analysis."""

    def __init__(self, db: Session):
        self.db = db

    def _get_frost_url(self):
        from app.core.config import settings

        return settings.frost_url

    def _get_timeout(self):
        from app.core.config import settings

        return settings.frost_timeout

    def _escape_odata_string(self, s: str) -> str:
        """Escape single quotes in OData string literals."""
        return s.replace("'", "''")

    def _get_int_id(self, iot_id) -> int:
        """
        Convert a FROST @iot.id to an integer.

        Uses BLAKE2b 64-bit hash for non-integer IDs to minimize collisions
        while maintaining compatibility with integer-based schemas.
        """
        try:
            return int(iot_id)
        except (ValueError, TypeError):
            # Use a deterministic 64-bit hash to minimize collisions
            import hashlib

            hash_bytes = hashlib.blake2b(
                str(iot_id).encode("utf-8"), digest_size=8
            ).digest()
            return int.from_bytes(hash_bytes, byteorder="big", signed=False)

    # --- Station (Thing) Maps ---
    def _map_thing_to_station(self, thing: Dict) -> Dict:
        props = thing.get("properties", {})
        locs = thing.get("Locations", [])
        lat, lon = None, None
        if locs:
            # Assuming first location is Point
            coords = locs[0].get("location", {}).get("coordinates", [])
            if len(coords) >= 2:
                lon, lat = coords[0], coords[1]

        iot_id = thing.get("@iot.id")
        # Consolidate: use the raw @iot.id (string or int as string) as 'id'
        str_id = str(iot_id)

        current_time = datetime.now()

        return {
            "id": str_id,
            "name": thing.get("name"),
            "description": thing.get("description"),
            "latitude": lat,
            "longitude": lon,
            "elevation": props.get("elevation"),
            "station_type": props.get("type", props.get("station_type", "unknown")),
            "status": props.get("status", "unknown"),
            "organization": props.get("organization"),
            "properties": props,
            "created_at": current_time,
            "updated_at": current_time,
        }

    # --- CRUD for Stations (Things) ---

    def get_stations(self, skip: int = 0, limit: int = 100, **filters) -> List[Dict]:
        url = f"{self._get_frost_url()}/Things"
        params = {"$expand": "Locations", "$top": limit, "$skip": skip}
        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()

            try:
                things = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                raise TimeSeriesException("Received invalid JSON from FROST server.")

            return [self._map_thing_to_station(t) for t in things]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch stations from FROST: {e}. URL: {url}")
            raise TimeSeriesException(f"Failed to fetch stations: {e}")

    def get_station(self, station_id: str) -> Optional[Dict]:
        """
        Get station (Thing) by ID or property lookup.
        Tries to fetch by @iot.id first, then by station_id property.
        """
        # 1. Try Fetching by Direct ID (FROST @iot.id)
        # FROST IDs are usually integers.
        url_id = f"{self._get_frost_url()}/Things({station_id})"
        params_id = {"$expand": "Locations"}

        try:
            resp = requests.get(url_id, params=params_id, timeout=self._get_timeout())
            if resp.status_code == 200:
                try:
                    return self._map_thing_to_station(resp.json())
                except Exception as e:
                    logger.warning(f"Failed to map station from ID lookup: {e}")
        except Exception as e:
            # Ignore errors (e.g. 404, 400) and proceed to filter lookup
            logger.debug(
                f"Direct lookup for station {station_id} by ID failed, will fallback: {e}"
            )

        # 2. Fallback: Filter by property 'station_id'
        url = f"{self._get_frost_url()}/Things"
        escaped_id = self._escape_odata_string(station_id)
        params = {
            "$expand": "Locations",
            "$filter": f"properties/station_id eq '{escaped_id}'",
        }
        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            try:
                val = resp.json().get("value")
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                raise TimeSeriesException("Received invalid JSON from FROST server.")

            if val:
                return self._map_thing_to_station(val[0])

            # If we get here, station was not found
            raise ResourceNotFoundException(f"Station '{station_id}' not found.")

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to fetch station '{station_id}' from FROST: {e}. URL: {url}"
            )
            raise TimeSeriesException(f"Failed to fetch station details: {e}")

    def get_datastreams_for_station(
        self, station_id: int | str, parameter: Optional[str] = None
    ) -> List[Dict]:
        """Get all datastreams for a station (Thing)."""
        # Use Navigation Path: Things({id})/Datastreams
        # This is more robust than filtering by Thing/id

        # Handle OData quoting for String IDs
        # If it's a string of digits, assume it's an Integer ID (unquoted)
        if isinstance(station_id, str) and not station_id.isdigit():
            safe_id = self._escape_odata_string(station_id)
            url_part = f"Things('{safe_id}')"
        else:
            url_part = f"Things({station_id})"

        url = f"{self._get_frost_url()}/{url_part}/Datastreams"

        params = {"$expand": "ObservedProperty,Thing/Locations"}

        if parameter:
            escaped_param = self._escape_odata_string(parameter)
            params["$filter"] = f"ObservedProperty/name eq '{escaped_param}'"

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            return resp.json().get("value", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch datastreams for station {station_id}: {e}")
            # Fallback (e.g. if Thing not found 404) -> return empty list?
            if (
                hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 404
            ):
                return []
            raise TimeSeriesException(f"Failed to fetch datastreams: {e}")

    def update_station(self, station_id: str, data: Dict) -> Optional[Dict]:
        """
        Update station (Thing) properties in FROST.
        """
        iot_id = None

        # 1. Try fetching by Direct ID first
        url_id = f"{self._get_frost_url()}/Things({station_id})"
        try:
            resp = requests.get(url_id, timeout=self._get_timeout())
            if resp.status_code == 200:
                iot_id = station_id
        except Exception as e:
            logger.debug(f"Direct ID lookup for station update failed: {e}")

        # 2. If not found, try filter by station_id property
        if not iot_id:
            url = f"{self._get_frost_url()}/Things"
            escaped_id = self._escape_odata_string(station_id)
            params = {"$filter": f"properties/station_id eq '{escaped_id}'"}
            try:
                resp = requests.get(url, params=params, timeout=self._get_timeout())
                resp.raise_for_status()
                val = resp.json().get("value", [])
                if val:
                    iot_id = val[0].get("@iot.id")
            except Exception as e:
                logger.error(f"Error lookup station for update: {e}")

        if not iot_id:
            return None

        # 3. Construct Payload
        # Support updating top-level fields and properties
        payload = {}
        if "name" in data:
            payload["name"] = data["name"]
        if "description" in data:
            payload["description"] = data["description"]

        # properties map
        props_to_update = {}
        if "station_id" in data:
            props_to_update["station_id"] = data["station_id"]

        # Handle Enum or value for status/type
        if "status" in data:
            val = data["status"]
            props_to_update["status"] = getattr(val, "value", str(val))

        if "station_type" in data:
            val = data["station_type"]
            props_to_update["station_type"] = getattr(val, "value", str(val))

        if "organization" in data:
            props_to_update["organization"] = data["organization"]

        if "properties" in data and isinstance(data["properties"], dict):
            props_to_update.update(data["properties"])

        if props_to_update:
            payload["properties"] = props_to_update

        if not payload:
            return self.get_station(str(iot_id))

        # 4. Execute PATCH
        patch_url = f"{self._get_frost_url()}/Things({iot_id})"
        try:
            patch_resp = requests.patch(
                patch_url, json=payload, timeout=self._get_timeout()
            )
            patch_resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to patch station {iot_id}: {e}")
            raise TimeSeriesException(f"Failed to update station: {e}")

        return self.get_station(str(iot_id))

    def delete_station(self, station_id: str) -> bool:
        iot_id = None

        # 1. Try fetching by Direct ID first
        url_id = f"{self._get_frost_url()}/Things({station_id})"
        try:
            resp = requests.get(url_id, timeout=self._get_timeout())
            if resp.status_code == 200:
                # Found by ID
                iot_id = station_id  # It is the ID
        except Exception as e:
            # Fallback
            logger.debug(f"Direct ID lookup for station deletion failed: {e}")

        # 2. If not found, try filter by station_id property
        if not iot_id:
            url = f"{self._get_frost_url()}/Things"
            escaped_id = self._escape_odata_string(station_id)
            params = {"$filter": f"properties/station_id eq '{escaped_id}'"}
            try:
                resp = requests.get(url, params=params, timeout=self._get_timeout())
                resp.raise_for_status()

                try:
                    val = resp.json().get("value", [])
                except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                    logger.error(
                        f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                    )
                    raise
                if val:
                    thing = val[0]
                    iot_id = thing.get("@iot.id")
            except Exception as e:
                logger.error(f"Error finding station {station_id} for deletion: {e}")
                # Fall through to check if we found anything

        if not iot_id:
            # Station not found
            raise ResourceNotFoundException(f"Station '{station_id}' not found.")

        # Execute DELETE
        del_url = f"{self._get_frost_url()}/Things({iot_id})"
        try:
            del_resp = requests.delete(del_url, timeout=self._get_timeout())
            if del_resp.status_code in [200, 204]:
                return True
            else:
                logger.error(
                    f"Failed to delete station {station_id} (IoT ID: {iot_id}): {del_resp.status_code} - {del_resp.text}"
                )
                del_resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error processing delete request for {station_id}: {e}")
            raise

        except Exception as e:
            logger.error(f"Error processing delete_station for {station_id}: {e}")
            raise

    def create_project_thing(
        self, name: str, description: str, project_id: str
    ) -> Optional[str]:
        """Create a Thing representing a Project in FROST."""
        payload = {
            "name": name,
            "description": description,
            "properties": {
                "type": "project",
                "project_id": str(project_id),
                "status": "active",
            },
        }
        url = f"{self._get_frost_url()}/Things"
        try:
            resp = requests.post(url, json=payload, timeout=self._get_timeout())
            if resp.status_code == 201:
                loc = resp.headers.get("Location")
                if loc:
                    # Parse ID from location URL: .../Things(123) or .../Things('123')
                    import re

                    # match (numbers) or ('string')
                    m = re.search(r"Things\((.+)\)", loc)
                    if m:
                        return m.group(1).strip("'")
            logger.error(
                f"Failed to create project thing: {resp.status_code} - {resp.text}"
            )
        except Exception as e:
            logger.error(f"Error creating project thing: {e}")
        return None

    def create_sensor_thing(self, data: SensorCreate) -> Optional[str]:
        """Create a Thing representing a Sensor in FROST."""
        payload = {
            "name": data.name,
            "description": data.description or "",
            "properties": {"station_type": data.station_type, "status": "active"},
            "Locations": [
                {
                    "name": "Location",
                    "encodingType": "application/vnd.geo+json",
                    "location": {"type": "Point", "coordinates": [data.lng, data.lat]},
                }
            ],
        }
        url = f"{self._get_frost_url()}/Things"
        try:
            resp = requests.post(url, json=payload, timeout=self._get_timeout())
            if resp.status_code == 201:
                loc = resp.headers.get("Location")
                if loc:
                    import re

                    m = re.search(r"Things\((.+)\)", loc)
                    if m:
                        return m.group(1).strip("'")
            logger.error(
                f"Failed to create sensor thing: {resp.status_code} - {resp.text}"
            )
        except Exception as e:
            logger.error(f"Error creating sensor thing: {e}")
        return None

    # --- Metadata (Datastreams) ---
    def get_time_series_metadata(
        self,
        skip: int = 0,
        limit: int = 100,
        parameter: Optional[str] = None,
        source_type: Optional[str] = None,
        station_id: Optional[str] = None,
    ) -> List[TimeSeriesMetadataResponse]:
        """Get time series metadata (Datastreams) from FROST Server."""

        # Build URL
        url = f"{self._get_frost_url()}/Datastreams"
        params = {
            "$top": limit,
            "$skip": skip,
            "$expand": "Thing,Sensor,ObservedProperty",
        }

        # Add filters
        filter_list = []
        if parameter:
            escaped_param = self._escape_odata_string(parameter)
            filter_list.append(f"ObservedProperty/name eq '{escaped_param}'")
        if station_id:
            # Match Thing name or properties/station_id
            escaped_sid = self._escape_odata_string(station_id)
            filter_list.append(
                f"(Thing/name eq '{escaped_sid}' or Thing/properties/station_id eq '{escaped_sid}')"
            )

        if filter_list:
            params["$filter"] = " and ".join(filter_list)

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            try:
                data = resp.json()
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                return []
            items = data.get("value", [])

            results = []
            for item in items:
                # Extract details
                thing = item.get("Thing", {})
                op = item.get("ObservedProperty", {})
                uom = item.get("unitOfMeasurement", {})  # camelCase

                # Parsing phenomenonTime
                pt = item.get("phenomenonTime")
                start_t = datetime.now()
                end_t = None
                if pt:
                    parts = pt.split("/")
                    try:
                        start_t = datetime.fromisoformat(
                            parts[0].replace("Z", "+00:00")
                        )
                        if len(parts) > 1:
                            end_t = datetime.fromisoformat(
                                parts[1].replace("Z", "+00:00")
                            )
                    except ValueError:
                        logger.warning(f"Failed to parse phenomenonTime: {pt}")

                results.append(
                    TimeSeriesMetadataResponse(
                        id=item.get("@iot.id"),
                        series_id=item.get("name"),
                        name=item.get("name"),  # Required
                        description=item.get("description"),
                        parameter=op.get("name", "unknown"),
                        unit=uom.get("name", "unknown"),
                        station_id=thing.get("name", "unknown"),
                        source_type=SourceType.SENSOR,  # Required
                        data_type=DataType.CONTINUOUS,  # Required
                        start_time=start_t,  # Required
                        end_time=end_t,
                        interval="variable",
                        is_active=True,
                        data_retention_days=365,
                        created_at=datetime.now(),  # Dummy
                        updated_at=datetime.now(),  # Dummy
                    )
                )
            return results
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request failure fetching metadata from FROST: {e} URL: {url}"
            )
            raise TimeSeriesException(f"Failed to fetch metadata: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata from FROST: {e}")
            raise TimeSeriesException(f"Unexpected error: {e}")

    def get_time_series_metadata_by_id(
        self, series_id: str
    ) -> Optional[TimeSeriesMetadataResponse]:

        # Find by Name
        url = f"{self._get_frost_url()}/Datastreams"
        escaped_id = self._escape_odata_string(series_id)
        params = {
            "$filter": f"name eq '{escaped_id}'",
            "$expand": "Thing,Sensor,ObservedProperty",
        }

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            try:
                val = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                return None
            if not val:
                raise ResourceNotFoundException(f"Time series '{series_id}' not found.")

            item = val[0]
            thing = item.get("Thing", {})
            op = item.get("ObservedProperty", {})
            uom = item.get("unitOfMeasurement", {})

            # Parsing phenomenonTime
            pt = item.get("phenomenonTime")
            start_t = datetime.now()
            end_t = None
            if pt:
                parts = pt.split("/")
                try:
                    start_t = datetime.fromisoformat(parts[0].replace("Z", "+00:00"))
                    if len(parts) > 1:
                        end_t = datetime.fromisoformat(parts[1].replace("Z", "+00:00"))
                except ValueError:
                    logger.warning(f"Failed to parse phenomenonTime: {pt}")

            return TimeSeriesMetadataResponse(
                id=item.get("@iot.id"),
                series_id=item.get("name"),
                name=item.get("name"),  # Required
                description=item.get("description"),
                parameter=op.get("name", "unknown"),
                unit=uom.get("name", "unknown"),
                station_id=thing.get("name", "unknown"),
                source_type=SourceType.SENSOR,  # Required
                data_type=DataType.CONTINUOUS,  # Required
                start_time=start_t,  # Required
                end_time=end_t,
                interval="variable",
                is_active=True,
                data_retention_days=365,
                created_at=datetime.now(),  # Dummy
                updated_at=datetime.now(),  # Dummy
            )
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request failure fetching metadata by ID '{series_id}' from FROST: {e}"
            )
            raise TimeSeriesException(
                f"Failed to fetch metadata for '{series_id}': {e}"
            )
        except ResourceNotFoundException:
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error fetching metadata by ID '{series_id}' from FROST: {e}"
            )
            raise TimeSeriesException(f"Unexpected error for '{series_id}': {e}")

    # --- Time Series Data ---

    def add_bulk_data(self, series_id: str, data_points: List[Any]) -> int:
        """
        Bulk add data points to a datastream (identified by name/series_id).
        """
        # 1. Resolve Datastream ID
        # 1. Resolve Datastream ID and Check Thing Location
        url = f"{self._get_frost_url()}/Datastreams"
        escaped_name = self._escape_odata_string(series_id)
        # Expand Thing/Locations to check if location exists
        params = {
            "$filter": f"name eq '{escaped_name}'",
            "$select": "id,Thing",
            "$expand": "Thing/Locations($select=id)",
        }

        ds_id = None
        thing_id = None
        has_location = False

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            vals = resp.json().get("value", [])
            if vals:
                ds = vals[0]
                ds_id = ds.get("@iot.id")
                thing = ds.get("Thing", {})
                thing_id = thing.get("@iot.id")
                locs = thing.get("Locations", [])
                if locs:
                    has_location = True
        except Exception as e:
            logger.error(f"Failed to lookup datastream {series_id}: {e}")
            raise TimeSeriesException(f"Failed to verify datastream: {e}")

        if not ds_id:
            raise ResourceNotFoundException(f"Datastream '{series_id}' not found.")

        # Ensure Thing has a location (Required for FoI generation)
        if thing_id and not has_location:
            try:
                self._ensure_thing_location(thing_id)
            except Exception as e:
                logger.warning(f"Failed to ensure location for Thing {thing_id}: {e}")
                # We proceed, but import might fail if FoI cannot be generated.

        # 2. Prepare Payloads
        # FROST Server might support batch operations, but standard OGC SensorThings API
        # uses MQTT or individual POSTs. Some implementations have extensions.
        # We will use individual POSTs for now, but in parallel or loop.
        # Ideally: Create Observations in bulk?
        # FROST supports batch requests via $batch, but simpler to just loop for MVP.

        count = 0
        # TODO: Use $batch endpoint if available for performance.
        post_url = f"{self._get_frost_url()}/Observations"

        errors = []
        for dp in data_points:
            try:
                payload = {
                    "phenomenonTime": dp.timestamp.isoformat(),
                    "result": dp.value,
                    "Datastream": {"@iot.id": ds_id},
                    "parameters": {"quality_flag": dp.quality_flag},
                }

                r = requests.post(post_url, json=payload, timeout=self._get_timeout())
                if r.status_code in [200, 201]:
                    count += 1
                else:
                    logger.error(
                        f"FROST Error ({r.status_code}): {r.text} - Payload: {payload}"
                    )
                    errors.append(f"{r.status_code}: {r.text}")
            except Exception as e:
                logger.error(f"Failed to post observation for {series_id}: {e}")
                errors.append(str(e))

        if count == 0 and errors:
            # If completely failed, raise
            raise TimeSeriesException(
                f"Failed to import data. Errors: {'; '.join(errors[:3])}..."
            )

        return count

    def create_data_point(self, station_id: str, data_point) -> Dict:
        """Create a new data point (Observation) in FROST."""

        # Determine Datastream Name: DS_{station_id}_{parameter}
        # data_point.parameter is likely an Enum, get value
        param_val = (
            data_point.parameter.value
            if hasattr(data_point.parameter, "value")
            else data_point.parameter
        )
        datastream_name = f"DS_{station_id}_{param_val}"

        # Find Datastream ID
        url = f"{self._get_frost_url()}/Datastreams"
        escaped_ds_name = self._escape_odata_string(datastream_name)
        # Expand Thing to get its ID for Alert Evaluation
        params = {
            "$filter": f"name eq '{escaped_ds_name}'",
            "$select": "id",
            "$expand": "Thing",
        }

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            ds_id = None
            thing_id = None
            if resp.status_code == 200:
                try:
                    vals = resp.json().get("value", [])
                except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                    logger.error(
                        f"Failed to parse JSON for datastream lookup: {json_err}. URL: {url}"
                    )
                    raise  # Re-raise as we need the datastream ID to proceed
                if vals:
                    ds_id = vals[0].get("@iot.id")
                    if vals[0].get("Thing"):
                        thing_id = vals[0]["Thing"].get("@iot.id")

            if not ds_id:
                # auto-creation could happen here, but for now specific error
                raise ValueError(
                    f"Datastream {datastream_name} not found. Please ensure station and parameter exist."
                )

            # Create Observation
            obs_payload = {
                "phenomenonTime": data_point.timestamp.isoformat(),
                "result": data_point.value,
                "Datastream": {"@iot.id": ds_id},
                "parameters": {
                    "quality_flag": (
                        data_point.quality_flag.value
                        if hasattr(data_point.quality_flag, "value")
                        else data_point.quality_flag
                    )
                    # Add other properties if needed
                },
            }

            post_url = f"{self._get_frost_url()}/Observations"
            post_resp = requests.post(
                post_url, json=obs_payload, timeout=self._get_timeout()
            )
            post_resp.raise_for_status()

            # Extract ID
            new_id = "0"
            loc = post_resp.headers.get("Location")
            if loc:
                try:
                    # Parse ID from Location header: .../Observations(123)
                    import re

                    m = re.search(r"Observations\((.+)\)", loc)
                    if m:
                        new_id = m.group(1).strip("'")
                except Exception as ex:
                    logger.warning(
                        f"Failed to parse new ID from Location header '{loc}': {ex}"
                    )

            # Trigger Alert Evaluation
            try:
                from app.services.alert_evaluator import AlertEvaluator

                evaluator = AlertEvaluator(self.db)
                # Pass the internal Thing ID if available, otherwise fallback to station_id string
                target_id = str(thing_id) if thing_id else str(station_id)
                evaluator.evaluate_sensor_data(target_id, data_point.value, param_val)
            except Exception as e:
                logger.error(f"Failed to trigger alert evaluation: {e}")

            return {
                "id": new_id,
                "timestamp": data_point.timestamp,
                "parameter": data_point.parameter,
                "value": data_point.value,
                "unit": data_point.unit,
                "quality_flag": data_point.quality_flag,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }

        except Exception as e:
            logger.error(f"Failed to create data point: {e}")
            raise TimeSeriesException(f"Failed to create data point: {e}")

    def get_latest_data(
        self, station_id: int | str, parameter: Optional[str] = None
    ) -> List[Dict]:
        """Get latest data points for a station (Thing). station_id should be the FROST @iot.id (string or int)."""

        # 1. Find Datastreams via Navigation
        # Handle OData quoting for String IDs
        # If it's a string of digits, assume it's an Integer ID (unquoted)
        if isinstance(station_id, str) and not station_id.isdigit():
            # Escape valid OData string if needed, but simple quoting is primary requirement
            # Assuming ID doesn't contain single quotes for now or use escape helper
            safe_id = self._escape_odata_string(station_id)
            url_part = f"Things('{safe_id}')"
        else:
            url_part = f"Things({station_id})"

        url = f"{self._get_frost_url()}/{url_part}/Datastreams"

        params = {"$expand": "ObservedProperty"}
        if parameter:
            escaped_param = self._escape_odata_string(parameter)
            params["$filter"] = f"ObservedProperty/name eq '{escaped_param}'"

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            try:
                datastreams = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                return []

            results = []
            for ds in datastreams:
                ds_id = ds.get("@iot.id")
                op_name = ds.get("ObservedProperty", {}).get("name", "unknown")
                uom = ds.get("unitOfMeasurement", {}).get("name", "unknown")

                # Get latest observation
                # Get latest observation
                if isinstance(ds_id, str) and not str(ds_id).isdigit():
                    quot_ds_id = f"'{ds_id}'"
                else:
                    quot_ds_id = ds_id

                obs_url = f"{self._get_frost_url()}/Datastreams({quot_ds_id})/Observations?$top=1&$orderby=phenomenonTime desc"
                try:
                    obs_resp = requests.get(obs_url, timeout=self._get_timeout())
                    obs_resp.raise_for_status()
                    try:
                        obs_vals = obs_resp.json().get("value", [])
                    except (
                        ValueError,
                        requests.exceptions.JSONDecodeError,
                    ) as json_err:
                        logger.error(f"Failed to parse observation JSON: {json_err}")
                        continue
                    if obs_vals:
                        obs = obs_vals[0]

                        # Parse time
                        t_str = obs.get("phenomenonTime")
                        try:
                            t = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
                        except ValueError:
                            t = datetime.now()  # Fallback

                        # Normalize parameter (slugify)
                        param_slug = op_name.lower().replace(" ", "_").replace("-", "_")
                        if param_slug == "water_temperature":
                            param_slug = "temperature"
                        elif param_slug == "level":
                            param_slug = "water_level"

                        results.append(
                            {
                                "id": str(obs.get("@iot.id")),
                                "timestamp": t,
                                "parameter": param_slug,
                                "value": obs.get("result"),
                                "unit": uom,
                                "quality_flag": obs.get("parameters", {}).get(
                                    "quality_flag", "good"
                                ),
                                "created_at": datetime.now(),
                                "updated_at": datetime.now(),
                            }
                        )
                except requests.exceptions.RequestException as e:
                    logger.warning(
                        f"Failed to fetch latest observation for Datastream {ds_id}: {e}"
                    )
                    continue
            return results

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get datastreams for station {station_id}: {e}")
            raise TimeSeriesException(
                f"Failed to get latest data for station {station_id}: {e}"
            )

    def get_time_series_data(self, query: TimeSeriesQuery) -> List[Any]:
        """Get time series data with filtering from FROST."""

        try:
            # Build Params
            params = {
                "$orderby": f"phenomenonTime {query.sort_order}",
                "$select": "phenomenonTime,result",
                "$top": query.limit,
                "$skip": query.offset,
            }

            # Filter
            escaped_series_id = self._escape_odata_string(query.series_id)
            filters = [f"Datastream/name eq '{escaped_series_id}'"]

            # Time Filter
            if query.start_time or query.end_time:
                # phenomenonTime=start/end or filter
                start = query.start_time or "1900-01-01T00:00:00Z"
                end = query.end_time or "2100-01-01T00:00:00Z"

                # Ensure ISO strings with Timezone
                from datetime import timezone

                def format_time_param(t):
                    if hasattr(t, "isoformat"):
                        # If naive datetime, assume UTC per project requirements
                        if t.tzinfo is None:
                            t = t.replace(tzinfo=timezone.utc)
                        return t.isoformat()
                    return t

                start = format_time_param(start)
                end = format_time_param(end)

                # Legacy fallback for string inputs (e.g. defaults)
                if (
                    isinstance(start, str)
                    and not start.endswith("Z")
                    and "+" not in start
                ):
                    start += "Z"
                if isinstance(end, str) and not end.endswith("Z") and "+" not in end:
                    end += "Z"

                filters.append(f"phenomenonTime ge {start} and phenomenonTime le {end}")

            params["$filter"] = " and ".join(filters)

            # Limit
            if query.limit:
                params["$top"] = query.limit

            resp = requests.get(
                f"{self._get_frost_url()}/Observations",
                params=params,
                timeout=self._get_timeout(),
            )
            resp.raise_for_status()

            try:
                items = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(f"Failed to parse JSON response from FROST: {json_err}")
                return []

            # Inline class removed in favor of Pydantic schema
            from app.schemas.time_series import TimeSeriesDataResponse

            result_points = []
            for idx, item in enumerate(items):
                t_str = item.get("phenomenonTime")
                try:
                    # Parse ISO
                    t = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
                except ValueError:
                    t = t_str
                # ID from FROST Observation ID? @iot.id
                obs_id = str(item.get("@iot.id", idx))

                # Create instance using schema
                point = TimeSeriesDataResponse(
                    id=obs_id,
                    series_id=query.series_id,
                    timestamp=t,
                    value=item.get("result"),
                    quality_flag="good",
                    is_interpolated=False,
                    is_aggregated=False,
                    uncertainty=None,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    properties={},
                )
                result_points.append(point)

            return result_points

        except Exception as e:
            logger.error(f"Failed to get time series data from FROST: {e}")
            raise TimeSeriesException(f"Failed to get time series data: {e}")

    def aggregate_time_series(
        self, aggregation: TimeSeriesAggregation
    ) -> List[AggregatedDataPoint]:
        """Aggregate time series data."""
        try:
            # Get raw data
            query = TimeSeriesQuery(
                series_id=aggregation.series_id,
                start_time=aggregation.start_time,
                end_time=aggregation.end_time,
            )
            data_points = self.get_time_series_data(query)

            if not data_points:
                return []

            # Convert to pandas DataFrame
            df = pd.DataFrame(
                [{"timestamp": dp.timestamp, "value": dp.value} for dp in data_points]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)

            # Resample and aggregate
            resampler = df.resample(aggregation.aggregation_interval)["value"]
            # Calculate all required statistics
            aggregated = resampler.agg(["count", "min", "max", "mean", "sum"])

            # Format result
            result = []
            for timestamp, row in aggregated.iterrows():
                # Check for NaN in 'mean' which indicates no data for that interval (unless using method 'count' where 0 is valid, but here we likely want non-empty)
                # If count is 0, we skip
                if row["count"] == 0:
                    continue

                # Determine the primary 'value' based on requested method
                # Handle 'avg' as an alias for 'mean'
                method = aggregation.aggregation_method
                if method in ["mean", "avg"]:
                    val = row["mean"]
                elif method == "min":
                    val = row["min"]
                elif method == "max":
                    val = row["max"]
                elif method == "sum":
                    val = row["sum"]
                elif method == "count":
                    val = row["count"]
                else:
                    # Fallback to mean if unknown (should be blocked by Enum validation)
                    val = row["mean"]

                result.append(
                    AggregatedDataPoint(
                        timestamp=timestamp,
                        value=float(val),
                        count=int(row["count"]),
                        min=float(row["min"]),
                        max=float(row["max"]),
                        avg=float(row["mean"]),
                        aggregation_method=aggregation.aggregation_method,  # Required
                        aggregation_interval=aggregation.aggregation_interval,  # Required
                        quality_flags=["good"],  # Required
                    )
                )

            return result

        except ValueError as e:
            logger.error(f"Validation error during aggregation: {e}")
            raise ValueError(
                f"Invalid aggregation parameters or unsupported interval: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to aggregate time series: {e}")
            raise TimeSeriesException(f"Failed to aggregate time series: {e}")

    def interpolate_time_series(self, request: InterpolationRequest) -> List[Any]:
        # Fetch data first
        query = TimeSeriesQuery(
            series_id=request.series_id,
            start_time=request.start_time,
            end_time=request.end_time,
        )
        data = self.get_time_series_data(query)
        if not data:
            return []

        # Pandas logic
        df = pd.DataFrame([{"timestamp": d.timestamp, "value": d.value} for d in data])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        try:
            resampled = df.resample(request.interval).asfreq()
            interpolated = resampled.interpolate(method=request.method)

            result = []
            for ts, row in interpolated.iterrows():
                is_interpolated = False
                # Check if original had this timestamp
                if ts not in df.index:
                    is_interpolated = True

                result.append(
                    InterpolatedDataPoint(
                        timestamp=ts,
                        value=float(row["value"]),
                        is_interpolated=is_interpolated,
                        quality_flag="good" if not is_interpolated else "interpolated",
                    )
                )

            return result
        except ValueError as e:
            logger.error(f"Validation error during interpolation: {e}")
            raise ValueError(
                f"Invalid interpolation parameters or unsupported interval: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error during interpolation: {e}")
            raise

    def calculate_statistics(
        self,
        series_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> TimeSeriesStatistics:
        # Fetch data first
        query = TimeSeriesQuery(
            series_id=series_id,
            start_time=start_time.isoformat() if start_time else None,
            end_time=end_time.isoformat() if end_time else None,
        )
        data = self.get_time_series_data(query)

        # Default empty
        if not data:
            return TimeSeriesStatistics(
                series_id=series_id,
                total_points=0,
                statistics={"min": 0, "max": 0, "mean": 0, "count": 0},
                time_range={},
                quality_summary={},
                gaps=[],
            )

        values = [d.value for d in data if d.value is not None]
        if not values:
            return TimeSeriesStatistics(
                series_id=series_id,
                total_points=0,
                statistics={"min": 0, "max": 0, "mean": 0, "count": 0},
                time_range={},
                quality_summary={},
                gaps=[],
            )

        return TimeSeriesStatistics(
            series_id=series_id,
            total_points=len(values),
            statistics={
                "min": min(values),
                "max": max(values),
                "mean": sum(values) / len(values),
                "std": float(np.std(values)),
                "count": len(values),
            },
            time_range={"start": data[0].timestamp, "end": data[-1].timestamp},
            quality_summary={"good": len(values)},
            gaps=[],
        )

    def get_station_statistics(
        self,
        station_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Dict:
        """Get aggregated statistics for a station."""

        # 1. Get all Datastreams for the station
        url = f"{self._get_frost_url()}/Datastreams"

        # Handle ID quoting
        if isinstance(station_id, str) and not station_id.isdigit():
            safe_id = self._escape_odata_string(station_id)
            filter_str = f"Thing/id eq '{safe_id}'"
        else:
            filter_str = f"Thing/id eq {station_id}"

        params = {"$filter": filter_str, "$expand": "ObservedProperty"}

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            try:
                datastreams = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(f"Failed to parse JSON response from FROST: {json_err}")
                datastreams = []
        except Exception as e:
            logger.error(f"Failed to fetch datastreams for station stats: {e}")
            datastreams = []

        total_measurements = 0
        global_min = None
        global_max = None

        # Date filters
        time_filter = ""
        if start_time or end_time:
            filters = []
            if start_time:
                s_iso = start_time.isoformat()
                if not s_iso.endswith("Z") and "+" not in s_iso:
                    s_iso += "Z"
                filters.append(f"phenomenonTime ge {s_iso}")
            if end_time:
                e_iso = end_time.isoformat()
                if not e_iso.endswith("Z") and "+" not in e_iso:
                    e_iso += "Z"
                filters.append(f"phenomenonTime le {e_iso}")
            time_filter = " and ".join(filters)

        for ds in datastreams:
            ds_id = ds.get("@iot.id")
            if not ds_id:
                continue

            # Determine ID for URL (quote if string)
            if isinstance(ds_id, str) and not str(ds_id).isdigit():
                safe_ds_id = f"'{ds_id}'"
            else:
                safe_ds_id = ds_id

            obs_base_url = (
                f"{self._get_frost_url()}/Datastreams({safe_ds_id})/Observations"
            )

            # 2. Get Count
            try:
                # Use $count=true and $top=0 to just get the count
                count_params = {"$count": "true", "$top": 0}
                if time_filter:
                    count_params["$filter"] = time_filter

                c_resp = requests.get(
                    obs_base_url, params=count_params, timeout=self._get_timeout()
                )
                if c_resp.status_code == 200:
                    data = c_resp.json()
                    ds_count = data.get(
                        "@iot.count", 0
                    )  # Standard OGC SensorThings uses @iot.count
                    total_measurements += ds_count
            except Exception as e:
                logger.warning(f"Failed to get count for DS {ds_id}: {e}")

            # 3. Get Min (OrderBy Result Asc, Top 1)
            try:
                min_params = {"$orderby": "result asc", "$top": 1, "$select": "result"}
                if time_filter:
                    min_params["$filter"] = time_filter

                min_resp = requests.get(
                    obs_base_url, params=min_params, timeout=self._get_timeout()
                )
                if min_resp.status_code == 200:
                    vals = min_resp.json().get("value", [])
                    if vals:
                        val = vals[0].get("result")
                        if isinstance(val, (int, float)):
                            if global_min is None or val < global_min:
                                global_min = val
            except Exception as e:
                logger.warning(f"Failed to get min for DS {ds_id}: {e}")

            # 4. Get Max (OrderBy Result Desc, Top 1)
            try:
                max_params = {"$orderby": "result desc", "$top": 1, "$select": "result"}
                if time_filter:
                    max_params["$filter"] = time_filter

                max_resp = requests.get(
                    obs_base_url, params=max_params, timeout=self._get_timeout()
                )
                if max_resp.status_code == 200:
                    vals = max_resp.json().get("value", [])
                    if vals:
                        val = vals[0].get("result")
                        if isinstance(val, (int, float)):
                            if global_max is None or val > global_max:
                                global_max = val
            except Exception as e:
                logger.warning(f"Failed to get max for DS {ds_id}: {e}")

        return {
            "id": station_id,
            "total_measurements": total_measurements,
            "statistics": {
                "min": global_min if global_min is not None else 0,
                "max": global_max if global_max is not None else 0,
                "count": total_measurements,
            },
            "time_range": {},  # Not calculating simplified global time range for now
            "parameters": [],  # Required by schema, returning empty for performance
            "data_quality_summary": {},  # Required by schema (note key name change)
        }

    def detect_anomalies(self, series_id, start, end, method, threshold):
        # Fetch data
        query = TimeSeriesQuery(
            series_id=series_id,
            start_time=start.isoformat() if start else None,
            end_time=end.isoformat() if end else None,
        )
        data = self.get_time_series_data(query)
        if not data:
            return []

        # Convert to arrays
        values = np.array([d.value for d in data if d.value is not None])
        if len(values) < 2:
            return []

        anomalies = []
        if method == "statistical" or method == "zscore":
            mean = np.mean(values)
            std = np.std(values)
            if std == 0:
                return []

            z_scores = np.abs((values - mean) / std)

            # Map back to data points
            # Assuming values are aligned with data (filtered none above)
            valid_indices = [i for i, d in enumerate(data) if d.value is not None]

            for idx, z in enumerate(z_scores):
                if z > threshold:
                    orig_idx = valid_indices[idx]
                    anomalies.append(
                        {
                            "timestamp": data[orig_idx].timestamp,
                            "value": data[orig_idx].value,
                            "score": float(z),
                            "type": "statistical",
                        }
                    )

        return anomalies

    def export_time_series(self, series_id, start, end, format):
        raise NotImplementedError("Export functionality not yet implemented.")

    # --- Helper: Ensure Entities ---
    def _ensure_observed_property(self, name: str) -> Any:
        # Check if exists
        url = f"{self._get_frost_url()}/ObservedProperties"
        escaped = self._escape_odata_string(name)
        params = {"$filter": f"name eq '{escaped}'"}

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            if resp.status_code == 200:
                vals = resp.json().get("value", [])
                if vals:
                    return vals[0]["@iot.id"]
        except Exception as e:
            logger.debug(f"Failed to fetch property {name} (will create): {e}")

        # Create
        payload = {
            "name": name,
            "definition": "http://www.opengis.net/def/nil/OGC/0/unknown",
            "description": f"Observed Property: {name}",
        }
        resp = requests.post(url, json=payload, timeout=self._get_timeout())
        resp.raise_for_status()
        loc = resp.headers["Location"]
        return loc.split("(")[1].split(")")[0]

    def _ensure_sensor(self, name: str) -> Any:
        url = f"{self._get_frost_url()}/Sensors"
        escaped = self._escape_odata_string(name)
        params = {"$filter": f"name eq '{escaped}'"}

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            if resp.status_code == 200:
                vals = resp.json().get("value", [])
                if vals:
                    return vals[0]["@iot.id"]
        except Exception as e:
            logger.debug(f"Failed to fetch sensor {name} (will create): {e}")

        payload = {
            "name": name,
            "description": "Auto-generated sensor for data import",
            "encodingType": "application/pdf",
            "metadata": "http://example.org/sensor.pdf",
        }
        resp = requests.post(url, json=payload, timeout=self._get_timeout())
        resp.raise_for_status()
        loc = resp.headers["Location"]
        return loc.split("(")[1].split(")")[0]

    def _ensure_thing_location(self, thing_id: Any) -> None:
        """Ensure a Thing has at least one Location linked."""
        url = f"{self._get_frost_url()}/Things({thing_id})/Locations"
        payload = {
            "name": "Default Location",
            "description": "Auto-generated default location",
            "encodingType": "application/vnd.geo+json",
            "location": {"type": "Point", "coordinates": [0, 0]},
        }
        try:
            resp = requests.post(url, json=payload, timeout=self._get_timeout())
            if resp.status_code not in [200, 201]:
                logger.warning(
                    f"Failed to add location to Thing {thing_id}: {resp.text}"
                )
        except Exception as e:
            logger.error(f"Error adding location to Thing {thing_id}: {e}")

    def ensure_datastream(self, station_id_str: str, parameter: str) -> str:
        """
        Ensure Datastream exists for station and parameter.
        Creates generic Datastream if missing.
        Returns series_id.
        """
        series_id = f"DS_{station_id_str}_{parameter}"

        # Check existence
        try:
            if self.get_time_series_metadata_by_id(series_id):
                return series_id
        except (ResourceNotFoundException, TimeSeriesException):
            # Not found, proceed to create
            pass

        # Need Thing ID
        thing_id = None

        # 1. Try direct navigation via Datastreams URL first (sometimes easier)
        # But we need Thing ID to create Datastream.

        # 2. Lookup Thing ID assuming station_id_str matches 'properties/station_id' OR '@iot.id'
        # Try as IoT ID first (if integer-like)
        if station_id_str.isdigit():
            url_id = f"{self._get_frost_url()}/Things({station_id_str})"
            try:
                r = requests.get(url_id, timeout=self._get_timeout())
                if r.status_code == 200:
                    thing_id = r.json().get("@iot.id")
            except Exception as e:
                logger.debug(f"Failed to get thing by ID {station_id_str}: {e}")

        if not thing_id:
            # Try as property station_id
            url = f"{self._get_frost_url()}/Things"
            escaped_sid = self._escape_odata_string(station_id_str)
            params = {
                "$filter": f"properties/station_id eq '{escaped_sid}'",
                "$select": "id",
            }
            try:
                resp = requests.get(url, params=params, timeout=self._get_timeout())
                vals = resp.json().get("value", [])
                if vals:
                    thing_id = vals[0]["@iot.id"]
            except Exception as e:
                logger.debug(
                    f"Failed to lookup thing by property {station_id_str}: {e}"
                )

        if not thing_id:
            raise ResourceNotFoundException(
                f"Station '{station_id_str}' not found in Time Series store."
            )

        # Ensure dependencies
        op_id = self._ensure_observed_property(parameter)
        sensor_id = self._ensure_sensor("DataImportSensor")

        # Create Datastream
        payload = {
            "name": series_id,
            "description": f"Datastream for {parameter} at {station_id_str}",
            "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            "unitOfMeasurement": {
                "name": "Unknown",
                "symbol": "",
                "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Unknown",
            },
            "Thing": {"@iot.id": thing_id},
            "ObservedProperty": {"@iot.id": op_id},
            "Sensor": {"@iot.id": sensor_id},
        }

        ds_url = f"{self._get_frost_url()}/Datastreams"
        resp = requests.post(ds_url, json=payload, timeout=self._get_timeout())
        resp.raise_for_status()

        return series_id
