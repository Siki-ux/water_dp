from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional


from app.services.timeio.frost_client import get_cached_frost_client
from app.core.config import settings
from app.services.timeio.timeio_db import TimeIODatabase
from app.schemas.frost.thing import Thing, Location
from app.schemas.frost.datastream import Datastream, Observation
from app.core.exceptions import (
    AuthorizationException,
    ResourceNotFoundException,
    ValidationException,
)
from app.services.timeio.mqtt_client import MQTTClient

logger = logging.getLogger(__name__)

class ThingService:
    schema_name: str
    def __init__(self, schema_name: str):
        self.schema_name = schema_name
        self.timeio_db = TimeIODatabase()
        self.frost_client = get_cached_frost_client(
            base_url=settings.frost_url, 
            project_name=self.schema_name, 
            version=settings.frost_version, 
            frost_server=settings.frost_server
        )
    @property
    def mqtt(self) -> MQTTClient:
        if not self._mqtt_client:
            self._mqtt_client = MQTTClient()
        return self._mqtt_client

    def get_thing_id_from_uuid(self, sensor_uuid: str) -> Optional[str]:
        return self.timeio_db.get_thing_id_from_uuid(sensor_uuid)

    def get_thing(self, sensor_uuid: str, expand: List[str] = ["Locations","Datastreams"]) -> Thing:
        """
        Fetch a sensor for a project with full human-readable details.
        """
        thing_id = self.get_thing_id_from_uuid(sensor_uuid)
        if thing_id is None:
            raise ResourceNotFoundException("Thing not found")
        thing_data = self.frost_client.get_thing(thing_id,expand=",".join(expand))
        if not thing_data:
            return None
        return Thing.from_frost(thing_data)

    @staticmethod
    def get_all_things(schema_name: str, expand: List[str] = ["Locations","Datastreams"], filter: str = None, top: int = None) -> List[Thing]:
        """
        Fetch all sensors for a project with full human-readable details.
        """
        logger.info(f"Fetching things for project {schema_name} with expand {expand}, filter {filter}")
        frost_client = get_cached_frost_client(
            base_url=settings.frost_url, 
            project_name=schema_name, 
            version=settings.frost_version, 
            frost_server=settings.frost_server
        )
        things_data = frost_client.get_things(expand=",".join(expand), filter=filter, top=top)
        logger.info(f"Fetched {len(things_data)} things for project {schema_name}")
        if not things_data:
            return []
            
        return [Thing.from_frost(t) for t in things_data]

    def get_things(self, expand: List[str] = ["Locations","Datastreams"], filter: str = None, top: int = None) -> List[Thing]:
        """
        Fetch all sensors for a project with full human-readable details.
        """
        logger.info(f"Fetching things for project {self.schema_name} with expand {expand}, filter {filter}")
        things_data = self.frost_client.get_things(expand=",".join(expand), filter=filter, top=top)
        logger.info(f"Fetched {len(things_data)} things for project {self.schema_name}")
        if not things_data:
            return []
            
        return [Thing.from_frost(t) for t in things_data]


    def get_datastreams(self) -> List[Datastream]:
        """
        Get datastreams for a project via FROST.
        """
        datastreams = self.frost_client.get_datastreams()
        if not datastreams:
            return []
        return [Datastream.from_frost(ds) for ds in datastreams]

    def get_sensor_datastreams(self, sensor_uuid: str) -> List[Datastream]:
        """
        Get datastreams for a sensor via FROST.
        """
        thing_id = self.get_thing_id_from_uuid(sensor_uuid)
        if thing_id is None:
            raise ResourceNotFoundException("Thing not found")
        datastreams = self.frost_client.list_datastreams(thing_id=thing_id)
        if not datastreams:
            return []
        return [Datastream.from_frost(ds) for ds in datastreams]

    def get_sensor_datastream(self, sensor_uuid: str, datastream_name: str) -> Datastream:
        """
        Get a specific datastream for a sensor via FROST.
        """
        thing_id = self.get_thing_id_from_uuid(sensor_uuid)
        if thing_id is None:
            raise ResourceNotFoundException("Thing not found")

        datastreams = self.frost_client.list_datastreams(thing_id=thing_id)
        if not datastreams:
            return None
        for datastream in datastreams:
            if datastream.get("name") == datastream_name:
                return Datastream.from_frost(datastream)
        return None

    def get_observations(self,
        datastream_uuid: str,
        start_time: str = None,
        end_time: str = None,
        limit: int = 1000,
        order_by: str = "phenomenonTime desc",
        select: str = "phenomenonTime,result,resultTime") -> List[Observation]:
        """
        Get observations for a specific datastream for Datastream
        """
        observations = self.frost_client.get_observations(datastream_uuid,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        order_by=order_by,
        select=select)
        if not observations:
            return []
        return [Observation.from_frost(obs) for obs in observations]

    def get_observations_by_name_from_sensor_uuid(self,
        sensor_uuid: str,
        datastream_name: str,
        start_time: str = None,
        end_time: str = None,
        limit: int = 1000,
        order_by: str = "phenomenonTime desc",
        select: str = "phenomenonTime,result,resultTime") -> List[Observation]:
        """
        Get observations for a specific datastream observations for a sensor via FROST.
        """

        datastreams = self.get_sensor_datastreams(sensor_uuid)
        if not datastreams:
            return []
        for datastream in datastreams:
            if datastream.name == datastream_name:
                return self.get_observations(
                    datastream_uuid=datastream.datastream_id,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                    order_by=order_by,
                    select=select)
        return []

    def get_locations(self) -> List[Location]:
        """
        Get locations for a project via FROST.
        """
        locations = self.frost_client.get_locations()
        if not locations:
            return []
        return [Location.from_frost(loc) for loc in locations]

