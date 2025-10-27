import yaml
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from dataflow.config.loaders.base import BaseSpecReader


@dataclass
class EquitySpec:
    """Data class representing the specification of an Equity."""
    root_id: str
    description: str = ""
    time_zone: str = "UTC"
    trading_days: List[int] = field(default_factory=list)
    open_time_local: Optional[str] = None
    close_time_local: Optional[str] = None
    active: bool = True


class EquitySpecReader(BaseSpecReader):
    """Reads and parses the equity specifications from a YAML file."""

    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path, default_filename="equity_spec.yaml")

    def _load_config(self) -> Dict[str, EquitySpec]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            raw_data = yaml.safe_load(f) or {}

        equity_spec_data = raw_data["equity_spec"] or {}
        specs: Dict[str, EquitySpec] = {}

        for dataflow_id, equity_spec in equity_spec_data.items():
            if not isinstance(equity_spec, dict):
                raise ValueError(f"Spec for dataflow_id '{dataflow_id}' is not a mapping")

            trading_hours = equity_spec.get("trading_hours", {}) or {}
            equity_spec = EquitySpec(
                root_id=equity_spec["root_id"],
                description=equity_spec.get("description", ""),
                time_zone=equity_spec.get("time_zone", "UTC"),
                open_time_local=trading_hours.get("open_time_local"),
                close_time_local=trading_hours.get("close_time_local"),
                trading_days=trading_hours.get("trading_days", []),
                active=trading_hours.get("active", True),
            )
            specs[dataflow_id] = equity_spec

        return specs

equity_specs = EquitySpecReader()  # instantiate to validate file existence at import time