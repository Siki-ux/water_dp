from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class TimestampColumn(BaseModel):
    column: int = Field(..., description="0-based column index")
    format: str = Field(..., description="Datetime format string (e.g. %Y-%m-%d %H:%M:%S)")

class CsvParserSettings(BaseModel):
    delimiter: str = Field(default=",", description="Column delimiter")
    exclude_headlines: int = Field(default=0, description="Number of lines to skip at start")
    exclude_footlines: int = Field(default=0, description="Number of lines to skip at end")
    timestamp_columns: List[TimestampColumn]
    # pandas_read_csv allows storing arbitrary params passed to pd.read_csv
    pandas_read_csv: Optional[Dict[str, Any]] = Field(default=None, description="Advanced pandas options")

class ParserCreate(BaseModel):
    name: str = Field(..., min_length=1, description="Parser Name")
    project_uuid: str = Field(..., description="Project UUID from water_dp-api")
    type: str = Field(default="CsvParser", description="Parser Type (currently only CsvParser)")
    settings: CsvParserSettings

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "my-csv-parser",
                "project_uuid": "1bfde64c-a785-416a-a513-6be718055ce1",
                "type": "CsvParser",
                "settings": {
                    "delimiter": ",",
                    "exclude_headlines": 1, 
                    "exclude_footlines": 0,
                    "timestamp_columns": [
                        {"column": 0, "format": "%Y-%m-%dT%H:%M:%S.%fZ"}
                    ],
                    "pandas_read_csv": {"header": 0}
                }
            }
        }
    }

class ParserResponse(BaseModel):
    id: int
    name: str
    group_id: str
    type: str
    settings: CsvParserSettings

    class Config:
        from_attributes = True
