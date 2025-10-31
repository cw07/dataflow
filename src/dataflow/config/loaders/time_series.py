import json
from typing import Any
from pydantic import BaseModel, Field, field_validator, computed_field

from datacore.models.assets import BaseAsset
from dataflow.utils.common import DataOutput
from datacore.models.mktdata.schema import MktDataSchema


class TimeSeriesConfig(BaseModel):
    """Model for each time series configuration entry"""
    service_id: int
    asset: BaseAsset
    data_schema: MktDataSchema
    data_source: str
    destination: list[str]
    extractor: str
    additional_params: dict = Field(default_factory=dict)
    active: bool

    @field_validator('destination', mode='before')
    @classmethod
    def parse_destination(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            return [d for d in v.split(',')]
        elif isinstance(v, list):
            return v
        else:
            return []

    @field_validator('additional_params', mode='before')
    @classmethod
    def parse_json_params(cls, v: Any) -> dict:
        if isinstance(v, dict):
            return v
        elif isinstance(v, str):
            try:
                return json.loads(v) if v else {}
            except json.JSONDecodeError:
                return {}
        else:
            raise TypeError(f"additional_params must be a str or dict, not {type(v)}")

    @field_validator('active', mode='before')
    @classmethod
    def parse_active(cls, v: Any) -> bool:
        if isinstance(v, str):
            return v.lower() in ('true', '1', 'yes', 'on')
        return bool(v)

    @staticmethod
    def output_type(destination: str):
        if "database" in destination or "db" in destination:
            output_type = DataOutput.database
        elif "redis" in destination:
            output_type = DataOutput.redis
        elif "file" in destination:
            output_type = DataOutput.file
        else:
            raise ValueError("Invalid destination type")
        return output_type

    @computed_field
    @property
    def description(self) -> str:
        """Automatically composed description from asset and pipeline components."""
        extractor_map = {
            "realtime": "rt",
            "historical": "hist"
        }
        parts = [
            self.asset.dflow_id,
            self.asset.description,
            f"{extractor_map[self.extractor]}",
            f"{self.data_source}",
            f"schema:{self.data_schema.value}"
        ]
        dest_str = ", ".join(self.destination)
        parts.append(f"to:{dest_str}")
        return " | ".join(parts)

    def __str__(self) -> str:
       return self.description

    def __repr__(self) -> str:
        return self.description
