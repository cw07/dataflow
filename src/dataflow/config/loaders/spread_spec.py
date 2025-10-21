import yaml
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from dataflow.config.loaders.base import BaseSpecReader


@dataclass
class SpreadSpec:
    """Data class representing the specification of a spread instrument."""
    root_id: str
    description: str = ""
    time_zone: str = "UTC"
    trading_days: List[int] = field(default_factory=list)
    open_time_local: Optional[str] = None
    close_time_local: Optional[str] = None
    active: bool = True
    legs: List[Dict[str, Any]] = field(default_factory=list)


class SpreadSpecReader(BaseSpecReader[SpreadSpec]):
    """Reads and parses the spread specifications from a YAML file."""

    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path, default_filename="spread_spec.yaml")

    def _load_config(self) -> Dict[str, SpreadSpec]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f) or {}

        if "spread_spec" not in self.raw_data:
            raise ValueError("YAML must contain a top-level 'spread_spec' key")

        spread_spec_data = self.raw_data.get("spread_spec") or {}
        specs: Dict[str, SpreadSpec] = {}

        for root_id, data in spread_spec_data.items():
            if not isinstance(data, dict):
                raise ValueError(f"Spec for root_id '{root_id}' is not a mapping")

            trading_hours = data.get("trading_hours", {}) or {}
            legs = data.get("legs", []) or []
            if not isinstance(legs, list):
                raise ValueError(f"'legs' for root_id '{root_id}' must be a list")

            spec = SpreadSpec(
                root_id=root_id,
                description=data.get("description", ""),
                time_zone=data.get("time_zone", "UTC"),
                open_time_local=trading_hours.get("open_time_local"),
                close_time_local=trading_hours.get("close_time_local"),
                trading_days=trading_hours.get("trading_days", []),
                active=trading_hours.get("active", True),
                legs=legs,
            )
            specs[root_id] = spec

        return specs

    def get_spec(self, root_id: str) -> SpreadSpec:
        """Unified interface: return the spec for a given root_id."""
        return super().get_spec(root_id)


spread_specs = SpreadSpecReader()