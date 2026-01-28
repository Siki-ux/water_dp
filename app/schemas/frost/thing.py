"""
Thing (Sensor) models.
"""

from typing import Any, Dict, List, Optional
from pydantic import Field
from pydantic import BaseModel


class Coordinate(BaseModel):
    """
    Represents a coordinate of a Location.
    """
    latitude: Optional[float] = Field(default=None, description="Latitude coordinate")
    longitude: Optional[float] = Field(default=None, description="Longitude coordinate")

class Location(BaseModel):
    """
    Represents a Location of a Thing.
    """
    coordinates: Optional[Coordinate] = Field(default=None, description="Coordinates of the Location")
    type: str = Field(..., description="Type of the Location")

    @classmethod
    def from_frost(cls, data: Dict[str, Any]) -> "Location":
        loc = data.get("location", {})
        if loc.get("type") == "Point":
            coords = loc.get("coordinates")
            if coords and len(coords) >= 2:
                return cls(
                    type="Point",
                    coordinates=Coordinate(longitude=coords[0], latitude=coords[1])
                )
        return cls(type="Unknown")

class Datastream(BaseModel):
    """
    Represents a Datastream capability of a Thing.
    """
    name: str = Field(..., description="Name of the datastream (e.g. water_level)")
    unit: str = Field(..., description="Unit of measurement symbol (e.g. m)")
    label: str = Field(..., description="Human-readable label (e.g. Water Level)")
    properties: Optional[Dict[str, Any]] = Field(default=None, description="Additional properties")

    @classmethod
    def from_frost(cls, data: Dict[str, Any]) -> "Datastream":
        name = data.get("name", "Unknown")
        props = data.get("properties", {}) or {}
        
        # Unit Logic: Top-level UoM > Properties UoM > ?
        uom = data.get("unitOfMeasurement", {})
        unit = uom.get("symbol")
        
        if not unit or unit == "?":
            # Fallback to properties
            prop_uom = props.get("unitOfMeasurement", {})
            if isinstance(prop_uom, dict):
                unit = prop_uom.get("symbol")
        
        if not unit:
            unit = "?"

        # Label Logic: Properties label > Name
        label = props.get("label", name)

        return cls(
            name=name,
            unit=unit,
            label=label,
            properties=props
        )


class Thing(BaseModel):
    """
    Represents a Thing (Sensor) in the system.
    """
    thing_id: Optional[str] = Field(default=None, description="Unique identifier of the Thing")
    sensor_uuid: str = Field(..., description="Unique identifier of the Sensor")
    name: str = Field(..., description="Name of the Thing")
    description: Optional[str] = Field(default="", description="Description of the Thing")
    
    # Location
    location: Optional[Location] = Field(default=None, description="Location of the Thing")
    
    # Metadata
    properties: Optional[Dict[str, Any]] = Field(default=None, description="Custom properties")
    
    # Relations
    datastreams: List[Datastream] = Field(default_factory=list, description="List of associated datastreams")

    @classmethod
    def from_frost(cls, data: Dict[str, Any]) -> "Thing":
        props = data.get("properties", {}) or {}
        
        # 1. UUID
        sensor_uuid = props.get("uuid")
        if not sensor_uuid:
            sensor_uuid = str(data.get("@iot.id", ""))

        # 2. Location
        # FROST locations are usually geojson
        lat = None
        lon = None
        
        # TODO: This needs to be fixed we should use the expanded locations
        # Prioritize properties.location
        if "location" in props:
            ploc = props["location"]
            if ploc.get("type") == "Point":
                 pcoords = ploc.get("coordinates")
                 if pcoords and len(pcoords) >= 2:
                    lon = pcoords[0]
                    lat = pcoords[1]
        
        # Fallback to expanded Locations
        if lat is None:
            locations = data.get("Locations", [])
            if locations and isinstance(locations, list):
                loc_obj = locations[0]
                geo = loc_obj.get("location", {})
                # Check GeoJSON Point
                if geo and geo.get("type") == "Point":
                    coords = geo.get("coordinates")
                    if coords and len(coords) >= 2:
                        lon = coords[0]
                        lat = coords[1]

        location_obj = None
        if lat is not None and lon is not None:
             location_obj = Location(
                 type="Point",
                 coordinates=Coordinate(latitude=lat, longitude=lon)
             )

        # 3. Datastreams
        datastreams_data = data.get("Datastreams", [])
        parsed_datastreams = [
            Datastream.from_frost(ds) for ds in datastreams_data
        ]

        return cls(
            thing_id=str(data.get("@iot.id", "")),
            sensor_uuid=sensor_uuid,
            name=data.get("name", "Unknown"),
            description=data.get("description", ""),
            location=location_obj,
            properties=props,
            datastreams=parsed_datastreams
        )
