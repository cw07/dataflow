import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from dataflow.config.loaders.base import BaseSpecReader


@dataclass
class IndexSpec:
    """
    Data class representing the specification of Index.
    """
    root_id: str
    venue: str
    description: str
    time_zone: str
    trading_days: List[int] = field(default_factory=list)
    open_time_local: Optional[str] = None
    close_time_local: Optional[str] = None
    active: bool = True


class IndexSpecReader(BaseSpecReader):
    """
    Reads and parses the index specifications from a YAML file.
    """

    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path, default_filename="index_spec.yaml")

    def _load_config(self) -> Dict[str, IndexSpec]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_data = yaml.safe_load(f)

        if "index_spec" not in self.raw_data:
            raise ValueError("YAML must contain a top-level 'index_spec' key")

        index_spec_data = self.raw_data["index_spec"]
        specs = {}

        for root_id, idx_spec in index_spec_data.items():
            trading_hours = idx_spec.get("trading_hours", {}) or {}
            idx_spec = IndexSpec(
                root_id=idx_spec["root_id"],
                venue=root_id.split('.')[0],
                description=idx_spec.get("description", ""),
                time_zone=idx_spec.get("time_zone", "UTC"),
                open_time_local=trading_hours.get("open_time_local"),
                close_time_local=trading_hours.get("close_time_local"),
                trading_days=trading_hours.get("trading_days", []),
                active=trading_hours.get("active", True),
            )
            specs[root_id] = idx_spec
        return specs

    def get_spec(self, root_id: str) -> IndexSpec:
        """Unified interface: return the spec for a given root_id."""
        if root_id not in self.specs:
            raise KeyError(f"No index spec found for root_id: '{root_id}'")
        return self.specs[root_id]

    def all(self) -> Dict[str, IndexSpec]:
        return self.specs.copy()


index_specs = IndexSpecReader()


if __name__ == "__main__":
    index_specs = IndexSpecReader()
