import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from dataflow.config.loaders.base import BaseSpecReader


@dataclass
class FxSpec:
    """Data class representing the specification of an FX pair."""
    root_id: str
    description: str = ""
    time_zone: str = "UTC"
    trading_days: List[int] = field(default_factory=list)
    open_time_local: Optional[str] = None
    close_time_local: Optional[str] = None
    active: bool = True


class FxSpecReader(BaseSpecReader):
    """Reads and parses the FX specifications from a YAML file."""

    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path, default_filename="fx_spec.yaml")

    def _load_config(self) -> Dict[str, FxSpec]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f) or {}

        if "fx_spec" not in self.raw_data :
            raise ValueError("YAML must contain a top-level 'fx_spec' key")

        fx_spec_data = self.raw_data["fx_spec"] or {}
        specs: Dict[str, FxSpec] = {}

        for dataflow_id, fx_spec in fx_spec_data.items():
            if not isinstance(fx_spec, dict):
                raise ValueError(f"Spec for dataflow_id '{dataflow_id}' is not a mapping")

            trading_hours = fx_spec.get("trading_hours", {}) or {}
            fx_spec = FxSpec(
                root_id=fx_spec["root_id"],
                description=fx_spec.get("description", ""),
                time_zone=fx_spec.get("time_zone", "UTC"),
                open_time_local=trading_hours.get("open_time_local"),
                close_time_local=trading_hours.get("close_time_local"),
                trading_days=trading_hours.get("trading_days", []),
                active=trading_hours.get("active", True),
            )
            specs[dataflow_id] = fx_spec

        return specs


fx_specs = FxSpecReader()