from zoneinfo import ZoneInfo
import yaml
import datetime as dt
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional
from dataflow.config.loaders.base import BaseSpecReader

@dataclass
class FuturesOptSpec:
    """Data class for a futures option contract specification"""
    root_id: str
    terms: int
    contract_size: int
    description: str
    venue: str
    time_zone: str
    open_time_local: str
    close_time_local: str
    trading_days: List[int]
    contract_month_code: List[str]
    contract_months: List[str]
    active: bool

    def is_trading_now(self) -> bool:
        pass


class FuturesOptSpecReader(BaseSpecReader):
    """Reader class for futures specification YAML configuration"""

    def __init__(self, config_path: Optional[str] = None):
        self.exchanges = {}
        self.categories = {}
        super().__init__(config_path, default_filename="futopt_spec.yaml")

    def _load_config(self) -> Dict[str, FuturesOptSpec]:
        """Load and parse the YAML configuration file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            self.raw_data = yaml.safe_load(f)

        fut_option_spec_data = self.raw_data["futures_option_spec"] or {}
        specs: Dict[str, FuturesOptSpec] = {}

        for dataflow_id, futopt_spec in fut_option_spec_data.items():
            contract = FuturesOptSpec(
                root_id=futopt_spec["root_id"],
                terms=int(futopt_spec["terms"]),
                contract_size=int(futopt_spec["contract_size"]),
                description=futopt_spec["description"],
                venue=dataflow_id.split('.')[1],
                time_zone=futopt_spec["time_zone"],
                open_time_local=futopt_spec["trading_hours"]["open_time_local"],
                close_time_local=futopt_spec["trading_hours"]["close_time_local"],
                trading_days=futopt_spec["trading_hours"]["trading_days"],
                contract_month_code=[self.raw_data["contract_month_codes"][des] for des in futopt_spec["contract_months"]],
                contract_months=futopt_spec["contract_months"],
                active=bool(futopt_spec["active"])
            )
            specs[dataflow_id] = contract
        return specs

    def get_contract(self, root_id: str) -> Optional[FuturesOptSpec]:
        """Backward-compatible alias for get_spec."""
        return self.get_spec(root_id)

    def get_by_exchange(self, exchange: str) -> List[FuturesOptSpec]:
        return [c for c in self.specs.values() if c.venue == exchange]

    def get_by_category(self, category: str) -> List[FuturesOptSpec]:
        return self.categories.get(category, [])

    def all(self) -> Dict[str, FuturesOptSpec]:
        return self.specs


futures_opt_specs = FuturesOptSpecReader()

if __name__ == "__main__":
    futures_opt_specs = FuturesOptSpecReader()
    print(futures_opt_specs)