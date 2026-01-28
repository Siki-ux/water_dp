from pydantic import BaseModel
from typing import Optional

class UnitOfMeasurement(BaseModel):
    definition: str
    symbol: str
    label: str

class Datastream(BaseModel):
    datastream_id: str
    thing_id: str
    sensor_uuid: Optional[str]
    name: str
    unit_of_measurement: UnitOfMeasurement

    @classmethod
    def from_frost(cls, data: dict) -> "Datastream":
        properties = data.get("properties", {}) or {}
        
        # Unit Logic
        # 1. Try properties.unitOfMeasurement (Custom overrides)
        uom_data = properties.get("unitOfMeasurement")
        
        # 2. Fallback to root unitOfMeasurement
        if not uom_data:
            uom_data = data.get("unitOfMeasurement", {})

        # Map 'name' to 'label' if label is missing (common FROST pattern)
        # Schema requires: definition, symbol, label
        unit = UnitOfMeasurement(
            definition=uom_data.get("definition", "http://unknown"),
            symbol=uom_data.get("symbol", "?"),
            label=uom_data.get("label") or uom_data.get("name", "Unknown")
        )

        # IDs
        datastream_id = str(data.get("@iot.id", ""))
        
        # Thing Relation
        thing_data = data.get("Thing", {})
        thing_id = str(thing_data.get("@iot.id", ""))
        
        # Sensor UUID from Thing properties
        thing_props = thing_data.get("properties", {})
        sensor_uuid = thing_props.get("uuid")

        return cls(
            datastream_id=datastream_id,
            thing_id=thing_id,
            sensor_uuid=sensor_uuid,
            name=data.get("name", "Unknown"),
            unit_of_measurement=unit
        )


class Observation(BaseModel):
    observation_id: str
    phenomenon_time: str
    result_time: str
    result: float

    @classmethod
    def from_frost(cls, data: dict) -> "Observation":
        return cls(
            observation_id=str(data.get("@iot.id", "")),
            phenomenon_time=data.get("phenomenonTime", ""),
            result_time=data.get("resultTime", ""),
            result=data.get("result", 0.0),
        )