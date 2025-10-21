import json
import pandas as pd
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

from datacore.models.mktdata.schema import MktDataSchema

from dataflow.config.settings import settings
from dataflow.utils.common import DataOutput


class TimeSeriesConfig(BaseModel):
    """Model for each time series configuration entry"""
    service_id: int
    series_id: str
    series_type: str
    root_id: str
    venue: str
    data_schema: MktDataSchema
    data_source: str
    destination: list[str]
    extractor: str
    description: Optional[str] = None
    additional_params: dict = Field(default_factory=dict)
    symbol: Optional[str] = None
    active: bool

    @field_validator('destination', mode='before')
    @classmethod
    def parse_destination(cls, v: Any) -> list[str]:
        if isinstance(v, str):
            return [d for d in v.split(',')]
        else:
            return []

    @field_validator('symbol', mode='before')
    @classmethod
    def parse_symbol(cls, v: Any) -> Optional[str]:
        if isinstance(v, str) and v:
            return v
        else:
            return None

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

    @model_validator(mode='after')
    def resolve_fut_description(self) -> 'TimeSeriesConfig':
        from dataflow.config.loaders.fut_spec import futures_specs
        if self.series_type == 'fut' and self.description is None:
            term = int(self.series_id.split(".")[2])
            term_in_word = {1: "1st", 2: "2nd", 3: "3rd"}
            fut_contract = futures_specs.get_spec(self.root_id)
            if fut_contract:
                self.description = term_in_word.get(term, str(term)+"th") + " " + fut_contract.description
            else:
                self.description = term_in_word.get(term, str(term)+"th") + " " + self.root_id
            self.description = self.description + f" ({self.venue})"
        return self

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


    def __str__(self) -> str:
        """String representation showing all fields except additional_params"""
        dest_str = ','.join(self.destination) if self.destination else ''
        desc_str = f"('{self.description}')" if self.description else 'None'

        return (
            f"TimeSeriesConfig("
            f"service_id={self.service_id}, "
            f"series_id='{self.series_id}', "
            f"series_type='{self.series_type}', "
            f"root_id='{self.root_id}', "
            f"venue='{self.venue}', "
            f"data_schema='{self.data_schema}', "
            f"data_source='{self.data_source}', "
            f"destination=[{dest_str}], "
            f"extractor='{self.extractor}', "
            f"description={desc_str}, "
            f"active={self.active})"
        )

    def __repr__(self) -> str:
        return self.__str__()

class TimeSeriesFilterMixin:
    """Mixin class providing common filtering methods"""

    # Subclasses must implement this property
    @property
    def time_series(self) -> list[TimeSeriesConfig]:
        raise NotImplementedError("Subclass must provide time_series property")

    def get_extractors(self):
        return set(s.extractor for s in self.time_series)

    def get_active_ts(self):
        """Get all time series (no filtering)"""
        filtered = [s for s in self.time_series if s.active]
        return self._wrap_result(filtered)

    def get_ts_by_source(self, source: str):
        """Filter by data source"""
        filtered = [s for s in self.time_series if s.data_source == source]
        return self._wrap_result(filtered)

    def get_ts_by_asset_type(self, asset_type: str):
        """Filter by asset type"""
        filtered = [s for s in self.time_series if s.series_type == asset_type]
        return self._wrap_result(filtered)

    def get_ts_by_schema(self, schema: str):
        """Filter by schema"""
        filtered = [s for s in self.time_series if s.data_schema == schema]
        return self._wrap_result(filtered)

    def get_realtime_ts(self):
        """Filter for realtime series"""
        filtered = [s for s in self.time_series if s.extractor == "realtime"]
        return self._wrap_result(filtered)

    def get_historical_ts(self):
        """Filter for historical series"""
        filtered = [s for s in self.time_series if s.extractor == "historical"]
        return self._wrap_result(filtered)

    def _wrap_result(self, filtered: list[TimeSeriesConfig]):
        """Wrap filtered results - subclasses override this"""
        raise NotImplementedError("Subclass must implement _wrap_result")


class TimeSeriesQueryResult(TimeSeriesFilterMixin):
    """Wrapper class to enable method chaining on query results"""

    def __init__(self, time_series: list[TimeSeriesConfig]):
        self._time_series = time_series

    @property
    def time_series(self) -> list[TimeSeriesConfig]:
        return self._time_series

    def _wrap_result(self, filtered: list[TimeSeriesConfig]) -> 'TimeSeriesQueryResult':
        """Wrap filtered results in a new QueryResult for chaining"""
        return TimeSeriesQueryResult(filtered)

    def to_list(self) -> list[TimeSeriesConfig]:
        """Get the underlying list"""
        return self._time_series

    def __repr__(self) -> str:
        """String representation of the query result"""
        count = len(self._time_series)
        if count == 0:
            return "TimeSeriesQueryResult(0 time series)"
        configs_repr = '\n  '.join(repr(ts) for ts in self._time_series)
        if count == 1:
            return f"TimeSeriesQueryResult(1 time series):\n {configs_repr}"
        else:
            return f"TimeSeriesQueryResult({count} time series):\n{configs_repr}"

    def __iter__(self):
        """Make it iterable"""
        return iter(self._time_series)

    def __len__(self):
        """Get count"""
        return len(self._time_series)

    def __bool__(self):
        return len(self._time_series) > 0


class TimeSeriesConfigManager(TimeSeriesFilterMixin):
    """Manages loading and querying service configurations"""

    def __init__(self, active_only: bool = True):
        self.active_only: bool = active_only
        self._time_series: list[TimeSeriesConfig] = []
        self.load_configurations()

    @property
    def time_series(self) -> TimeSeriesQueryResult:
        return self._wrap_result(self._time_series)

    def load_configurations(self):
        """Load service configurations from CSV"""
        if settings.time_series_config_type == "file":
            df = pd.read_csv(settings.time_series_config_path)
        else:
            df = pd.DataFrame()
        df = df.where(pd.notnull(df), None)
        for _, row in df.iterrows():
            ts = TimeSeriesConfig(**row.to_dict())
            if self.active_only:
                if ts.active:
                    self._time_series.append(ts)
            else:
                self._time_series.append(ts)

    def _wrap_result(self, filtered: list[TimeSeriesConfig]) -> TimeSeriesQueryResult:
        """Wrap filtered results in a QueryResult for chaining"""
        return TimeSeriesQueryResult(filtered)


time_series_config = TimeSeriesConfigManager(active_only=True).time_series

if __name__ == "__main__":
    ts_config = TimeSeriesConfigManager(active_only=True).time_series
    result = ts_config.get_realtime_ts().get_ts_by_source("bbg")
    print(result)