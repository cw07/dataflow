from zoneinfo import ZoneInfo

import yaml
import datetime as dt
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from dataflow.config.loaders.base import BaseSpecReader


@dataclass
class FuturesSpec:
    """Data class for a futures contract specification"""
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
    category: Optional[str] = None


class FuturesSpecReader(BaseSpecReader):
    """Reader class for futures specification YAML configuration"""

    def __init__(self, config_path: Optional[str] = None):
        self.exchanges = {}
        self.categories = {}
        super().__init__(config_path, default_filename="fut_spec.yaml")

    def _load_config(self) -> Dict[str, FuturesSpec]:
        """Load and parse the YAML configuration file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            self.raw_data = yaml.safe_load(f)

        fut_spec_data = self.raw_data["futures_spec"] or {}
        specs: Dict[str, FuturesSpec] = {}
        categories: Dict[str, List[FuturesSpec]] = {}

        for category, contracts in fut_spec_data.items():
            categories[category] = []
            for dataflow_id, fut_spec in contracts.items():
                contract = FuturesSpec(
                    root_id=dataflow_id,
                    terms=int(fut_spec["terms"]),
                    contract_size=int(fut_spec["contract_size"]),
                    description=fut_spec['description'],
                    venue=dataflow_id.split('.')[0],
                    time_zone=fut_spec['time_zone'],
                    open_time_local=fut_spec['trading_hours']['open_time_local'],
                    close_time_local=fut_spec['trading_hours']['close_time_local'],
                    trading_days=fut_spec["trading_hours"]["trading_days"],
                    contract_month_code=[self.raw_data["contract_month_codes"][des] for des in fut_spec["contract_months"]],
                    contract_months=fut_spec['contract_months'],
                    category=category,
                    active=bool(fut_spec["active"])
                )
                specs[dataflow_id] = contract
                categories[category].append(contract)

        self.categories = categories

        if 'exchanges' in self.raw_data:
            self.exchanges = self.raw_data['exchanges']

        return specs

    def get_contract(self, root_id: str) -> Optional[FuturesSpec]:
        """Backward-compatible alias for get_spec."""
        return self.get_spec(root_id)

    def get_by_exchange(self, exchange: str) -> List[FuturesSpec]:
        return [c for c in self.specs.values() if c.venue == exchange]

    def get_by_category(self, category: str) -> List[FuturesSpec]:
        return self.categories.get(category, [])

    def all(self) -> Dict[str, FuturesSpec]:
        return self.specs

futures_specs = FuturesSpecReader()


if __name__ == "__main__":
    futures_specs = FuturesSpecReader()