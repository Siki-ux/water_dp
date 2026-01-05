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
        int_id = self._get_int_id(iot_id)

        current_time = datetime.now()

        return {
            "id": int_id,
            "station_id": props.get("station_id", str(iot_id)),
            "name": thing.get("name"),
            "description": thing.get("description"),
            "latitude": lat,
            "longitude": lon,
            "elevation": props.get("elevation"),
            "station_type": props.get("type", "unknown"),
            "status": props.get("status", "unknown"),
            "organization": props.get("organization"),
            "properties": props,
            "created_at": current_time,
            "updated_at": current_time,
        }

    # --- CRUD for Stations (Things) ---
    def create_station(self, station_data) -> Dict:
        # Create Thing
        thing_payload = {
            "name": station_data.name,
            "description": station_data.description or "Water Station",
            "properties": {
                "station_id": station_data.station_id,
                "station_type": station_data.station_type,
                "status": station_data.status,
                "organization": station_data.organization,
                **(station_data.properties or {}),
            },
        }

        url = f"{self._get_frost_url()}/Things"
        try:
            resp = requests.post(url, json=thing_payload, timeout=self._get_timeout())
            resp.raise_for_status()
            thing_loc = resp.headers["Location"]
            thing_id = thing_loc.split("(")[1].split(")")[0]

            # Create Location
            if station_data.latitude and station_data.longitude:
                loc_payload = {
                    "name": f"Loc {station_data.name}",
                    "encodingType": "application/vnd.geo+json",
                    "location": {
                        "type": "Point",
                        "coordinates": [station_data.longitude, station_data.latitude],
                    },
                }
                loc_resp = requests.post(
                    f"{self._get_frost_url()}/Locations",
                    json=loc_payload,
                    timeout=self._get_timeout(),
                )
                loc_resp.raise_for_status()
                loc_id = loc_resp.headers["Location"].split("(")[1].split(")")[0]
                # Link
                link_resp = requests.post(
                    f"{self._get_frost_url()}/Things({thing_id})/Locations/$ref",
                    json={"@iot.id": loc_id},
                    timeout=self._get_timeout(),
                )
                link_resp.raise_for_status()

            return self._map_thing_to_station(
                {
                    "@iot.id": thing_id,
                    **thing_payload,
                    "Locations": (
                        [
                            {
                                "location": {
                                    "coordinates": [
                                        station_data.longitude,
                                        station_data.latitude,
                                    ]
                                }
                            }
                        ]
                        if station_data.latitude
                        else []
                    ),
                }
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create station in FROST: {e}")
            raise

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
                return []
            return [self._map_thing_to_station(t) for t in things]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch stations from FROST: {e}. URL: {url}")
            return []

    def get_station(self, station_id: str) -> Optional[Dict]:
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
                return None
            if val:
                return self._map_thing_to_station(val[0])
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to fetch station '{station_id}' from FROST: {e}. URL: {url}"
            )
        return None

    def get_datastreams_for_station(
        self, station_id: int, parameter: Optional[str] = None
    ) -> List[Dict]:
        """Get all datastreams for a station (Thing)."""
        url = f"{self._get_frost_url()}/Datastreams"
        filter_parts = [f"Thing/id eq {station_id}"]
        if parameter:
            escaped_param = self._escape_odata_string(parameter)
            filter_parts.append(f"ObservedProperty/name eq '{escaped_param}'")

        params = {
            "$filter": " and ".join(filter_parts),
            "$expand": "ObservedProperty,unitOfMeasurement",
        }
        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            return resp.json().get("value", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch datastreams for station {station_id}: {e}")
            return []

    def update_station(self, station_id: str, data: Dict) -> Optional[Dict]:
        # Not fully implemented for deep patch
        return self.get_station(station_id)

    def delete_station(self, station_id: str) -> bool:
        try:
            # Fetch ID first using the property station_id
            url = f"{self._get_frost_url()}/Things"
            escaped_id = self._escape_odata_string(station_id)
            params = {"$filter": f"properties/station_id eq '{escaped_id}'"}
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()

            try:
                val = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                raise
            if not val:
                # Station not found
                return False

            # Assuming the first match is the correct one
            thing = val[0]
            iot_id = thing.get("@iot.id")

            # Execute DELETE
            del_url = f"{self._get_frost_url()}/Things({iot_id})"
            del_resp = requests.delete(del_url, timeout=self._get_timeout())

            if del_resp.status_code in [200, 204]:
                return True
            else:
                logger.error(
                    f"Failed to delete station {station_id} (IoT ID: {iot_id}): {del_resp.status_code} - {del_resp.text}"
                )
                del_resp.raise_for_status()

        except Exception as e:
            logger.error(f"Error processing delete_station for {station_id}: {e}")
            raise

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
            # Try matching Thing name or properties/station_id
            pass

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
                f"Request failure fetching metadata from FROST: {e} " f"URL: {url}"
            )
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata from FROST: {e}")
            raise

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
                return None

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
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error fetching metadata by ID '{series_id}' from FROST: {e}"
            )
            raise

    # --- Time Series Data ---

    def create_data_point(self, data_point) -> Dict:
        """Create a new data point (Observation) in FROST."""

        # Determine Datastream Name: DS_{station_id}_{parameter}
        # data_point.parameter is likely an Enum, get value
        param_val = (
            data_point.parameter.value
            if hasattr(data_point.parameter, "value")
            else data_point.parameter
        )
        datastream_name = f"DS_{data_point.station_id}_{param_val}"

        # Find Datastream ID
        url = f"{self._get_frost_url()}/Datastreams"
        escaped_ds_name = self._escape_odata_string(datastream_name)
        params = {"$filter": f"name eq '{escaped_ds_name}'", "$select": "id"}

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            ds_id = None
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
            new_id = 0
            loc = post_resp.headers.get("Location")
            if loc:
                try:
                    new_id = int(loc.split("(")[1].split(")")[0])
                except Exception as ex:
                    logger.warning(
                        f"Failed to parse new ID from Location header '{loc}': {ex}"
                    )

            return {
                "id": new_id,
                "station_id": data_point.station_id,
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
            raise

    def get_latest_data(
        self, station_id: int, parameter: Optional[str] = None
    ) -> List[Dict]:
        """Get latest data points for a station (Thing)."""

        # 1. Find Datastreams
        url = f"{self._get_frost_url()}/Datastreams"
        filter_parts = [f"Thing/id eq {station_id}"]
        if parameter:
            escaped_param = self._escape_odata_string(parameter)
            filter_parts.append(f"ObservedProperty/name eq '{escaped_param}'")

        params = {
            "$filter": " and ".join(filter_parts),
            "$expand": "ObservedProperty",
        }

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
                obs_url = f"{self._get_frost_url()}/Datastreams({ds_id})/Observations?$top=1&$orderby=phenomenonTime desc"
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

                        results.append(
                            {
                                "id": obs.get("@iot.id"),
                                "station_id": station_id,
                                "timestamp": t,
                                "parameter": op_name,
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
            return []

    def get_time_series_data(self, query: TimeSeriesQuery) -> List[Any]:
        """Get time series data with filtering from FROST."""

        try:
            # Build Params
            params = {
                "$orderby": "phenomenonTime asc",
                "$select": "phenomenonTime,result",
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
                obs_id = item.get("@iot.id", idx)

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
            raise

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
            raise

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
        station_id: int,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Dict:
        """Get aggregated statistics for a station."""

        # 1. Get all Datastreams for the station
        url = f"{self._get_frost_url()}/Datastreams"
        params = {"$filter": f"Thing/id eq {station_id}", "$expand": "ObservedProperty"}

        try:
            resp = requests.get(url, params=params, timeout=self._get_timeout())
            resp.raise_for_status()
            try:
                datastreams = resp.json().get("value", [])
            except (ValueError, requests.exceptions.JSONDecodeError) as json_err:
                logger.error(
                    f"Failed to parse JSON response from FROST: {json_err}. URL: {url}"
                )
                datastreams = []  # Fallback to empty list on JSON error
        except Exception as e:
            logger.error(f"Failed to fetch datastreams for station stats: {e}")
            datastreams = []

        total_measurements = 0
        quality_summary = {"good": 0, "questionable": 0, "bad": 0, "missing": 0}
        parameters_stats = []

        global_start = None
        global_end = None

        for ds in datastreams:
            series_id = ds.get("name")
            op_name = ds.get("ObservedProperty", {}).get("name", "unknown")

            # Calculate stats for this series
            stats = self.calculate_statistics(series_id, start_time, end_time)

            # Extract relevant info
            ds_count = stats.statistics.get("count", 0)
            total_measurements += ds_count

            ds_quality = stats.quality_summary
            for k, v in ds_quality.items():
                if k in quality_summary:
                    quality_summary[k] += v
                else:
                    # Handle custom flags if necessary
                    quality_summary["good"] += v  # Fallback or just ignore

            # Time range
            ts_range = stats.time_range
            if ts_range.get("start"):
                s = ts_range["start"]
                if global_start is None or s < global_start:
                    global_start = s
            if ts_range.get("end"):
                e = ts_range["end"]
                if global_end is None or e > global_end:
                    global_end = e

            parameters_stats.append(
                {
                    "parameter": op_name,
                    "series_id": series_id,
                    "count": ds_count,
                    "min": stats.statistics.get("min"),
                    "max": stats.statistics.get("max"),
                    "avg": stats.statistics.get("mean"),
                    "unit": ds.get("unitOfMeasurement", {}).get("name", "unknown"),
                }
            )

        return {
            "station_id": station_id,
            "time_range": {"start": global_start, "end": global_end},
            "parameters": parameters_stats,
            "total_measurements": total_measurements,
            "data_quality_summary": quality_summary,
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
