import yaml
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from dataflow.config.loaders.base import BaseSpecReader


@dataclass
class ForwardSpec:
    """Data class representing the specification of a Forward contract."""
    root_id: str
    time_zone: str
    description: str = ""
    trading_days: List[int] = field(default_factory=list)
    open_time_local: Optional[str] = None
    close_time_local: Optional[str] = None
    active: bool = True


class ForwardSpecReader(BaseSpecReader):
    """Reads and parses the forward specifications from a YAML file."""

    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path, default_filename="fwd_spec.yaml")

    def _load_config(self) -> Dict[str, ForwardSpec]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f) or {}

        if "forward_spec" not in self.raw_data:
            raise ValueError("YAML must contain a top-level 'forward_spec' key")

        forward_spec_data = self.raw_data["forward_spec"] or {}
        specs: Dict[str, ForwardSpec] = {}

        for root_id, data in forward_spec_data.items():
            if not isinstance(data, dict):
                raise ValueError(f"Spec for root_id '{root_id}' is not a mapping")

            trading_hours = data.get("trading_hours", {}) or {}
            spec = ForwardSpec(
                root_id=root_id,
                description=data["description"],
                time_zone=data["time_zone"],
                open_time_local=trading_hours["open_time_local"],
                close_time_local=trading_hours["close_time_local"],
                trading_days=trading_hours["trading_days"],
                active=trading_hours["active"]
            )
            specs[root_id] = spec

        return specs


fwd_specs = ForwardSpecReader()