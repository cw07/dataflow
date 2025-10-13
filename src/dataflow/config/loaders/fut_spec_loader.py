import yaml
import datetime as dt
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class FuturesContract:
    """Data class for a futures contract specification"""
    root_id: str
    description: str
    exchange: str
    time_zone: str
    open_time_utc: str
    close_time_utc: str
    contract_months: List[str]
    contract_months_desc: List[str]
    category: Optional[str] = None

    def get_active_contracts(self, year: int, month: Optional[int] = None) -> List[str]:
        """
        Generate active contract symbols for a given year/month

        Args:
            year: Year for contracts (e.g., 2025)
            month: Optional month to filter contracts after

        Returns:
            List of contract symbols (e.g., ['CLF25', 'CLG25', ...])
        """
        year_suffix = str(year)[-2:]  # Get last 2 digits of year
        contracts = []

        month_map = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                     'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}

        for contract_month in self.contract_months:
            contract_month_num = month_map[contract_month]
            if month is None or contract_month_num >= month:
                contracts.append(f"{self.root_id}{contract_month}{year_suffix}")

        return contracts

    def is_trading_now(self, current_time: dt.datetime = None) -> bool:
        """
        Check if the contract is currently in trading hours

        Args:
            current_time: Time to check (defaults to current UTC time)

        Returns:
            Boolean indicating if market is open
        """
        if current_time is None:
            current_time = dt.datetime.utcnow()

        # Parse trading hours
        open_hour, open_min, open_sec = map(int, self.open_time_utc.split(':'))
        close_hour, close_min, close_sec = map(int, self.close_time_utc.split(':'))

        current_time_minutes = current_time.hour * 60 + current_time.minute
        open_minutes = open_hour * 60 + open_min
        close_minutes = close_hour * 60 + close_min

        # Handle overnight trading (open > close means crosses midnight)
        if open_minutes > close_minutes:
            return current_time_minutes >= open_minutes or current_time_minutes < close_minutes
        else:
            return open_minutes <= current_time_minutes < close_minutes


class FuturesSpecReader:
    """Reader class for futures specification YAML configuration"""

    def __init__(self, config_path: str = "fut_spec.yaml"):
        """
        Initialize the futures specification reader

        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = Path(config_path)
        self.data = None
        self.contracts = {}
        self.exchanges = {}
        self.categories = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load and parse the YAML configuration file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            self.data = yaml.safe_load(f)

        # Parse futures specifications
        for category, contracts in self.data['futures_specifications'].items():
            self.categories[category] = []
            for root_id, spec in contracts.items():
                contract = FuturesContract(
                    root_id=spec['root_id'],
                    description=spec['description'],
                    exchange=spec['exchange'],
                    time_zone=spec['time_zone'],
                    open_time_utc=spec['trading_hours']['open_time_utc'],
                    close_time_utc=spec['trading_hours']['close_time_utc'],
                    contract_months=spec['contract_months'],
                    contract_months_desc=spec['contract_months_desc'],
                    category=category
                )
                self.contracts[root_id] = contract
                self.categories[category].append(contract)

        # Parse exchange information
        if 'exchanges' in self.data:
            self.exchanges = self.data['exchanges']

    def get_contract(self, root_id: str) -> Optional[FuturesContract]:
        """
        Get a specific futures contract by root ID

        Args:
            root_id: Root symbol (e.g., 'CL', 'GC')

        Returns:
            FuturesContract object or None if not found
        """
        return self.contracts.get(root_id)

    def get_contracts_by_exchange(self, exchange: str) -> List[FuturesContract]:
        """
        Get all contracts traded on a specific exchange

        Args:
            exchange: Exchange name (e.g., 'NYMEX', 'CME')

        Returns:
            List of FuturesContract objects
        """
        return [c for c in self.contracts.values() if c.exchange == exchange]

    def get_contracts_by_category(self, category: str) -> List[FuturesContract]:
        """
        Get all contracts in a specific category

        Args:
            category: Category name (e.g., 'energy', 'precious_metals')

        Returns:
            List of FuturesContract objects
        """
        return self.categories.get(category, [])

    def get_all_contracts(self) -> Dict[str, FuturesContract]:
        """
        Get all futures contracts

        Returns:
            Dictionary of root_id to FuturesContract objects
        """
        return self.contracts

    def get_active_symbols(self,
                           root_ids: Optional[List[str]] = None,
                           year: int = None,
                           month: int = None,
                           count: Optional[int] = None) -> List[str]:
        """
        Generate active contract symbols

        Args:
            root_ids: List of root symbols (uses all if None)
            year: Year for contracts (defaults to current year)
            month: Starting month (defaults to current month)
            count: Number of contracts per root (gets all if None)

        Returns:
            List of contract symbols
        """
        if year is None:
            year = dt.datetime.now().year
        if month is None:
            month = dt.datetime.now().month

        if root_ids is None:
            root_ids = list(self.contracts.keys())

        all_symbols = []
        for root_id in root_ids:
            contract = self.get_contract(root_id)
            if contract:
                symbols = contract.get_active_contracts(year, month)
                if count:
                    symbols = symbols[:count]
                all_symbols.extend(symbols)

        return all_symbols

    def get_trading_contracts(self, current_time: dt.datetime = None) -> List[FuturesContract]:
        """
        Get contracts that are currently in trading hours

        Args:
            current_time: Time to check (defaults to current UTC time)

        Returns:
            List of contracts currently trading
        """
        if current_time is None:
            current_time = dt.datetime.utcnow()

        trading_contracts = []
        for contract in self.contracts.values():
            if contract.is_trading_now(current_time):
                trading_contracts.append(contract)

        return trading_contracts

    def get_front_month_symbols(self,
                                root_ids: Optional[List[str]] = None,
                                year: int = None) -> Dict[str, str]:
        """
        Get front month contract for each root

        Args:
            root_ids: List of root symbols (uses all if None)
            year: Year for contracts (defaults to current year)

        Returns:
            Dictionary of root_id to front month symbol
        """
        if year is None:
            year = dt.datetime.now().year
        current_month = dt.datetime.now().month

        if root_ids is None:
            root_ids = list(self.contracts.keys())

        front_months = {}
        for root_id in root_ids:
            contract = self.get_contract(root_id)
            if contract:
                symbols = contract.get_active_contracts(year, current_month)
                if symbols:
                    front_months[root_id] = symbols[0]

        return front_months

    def validate_symbol(self, symbol: str) -> Tuple[bool, Optional[str]]:
        """
        Validate if a symbol follows futures naming convention

        Args:
            symbol: Contract symbol (e.g., 'CLZ25')

        Returns:
            Tuple of (is_valid, error_message)
        """
        if len(symbol) < 4:
            return False, "Symbol too short"

        # Extract components
        root_id = symbol[:-3]
        month_code = symbol[-3]
        year = symbol[-2:]

        # Check root exists
        if root_id not in self.contracts:
            return False, f"Unknown root symbol: {root_id}"

        # Check month code
        contract = self.contracts[root_id]
        if month_code not in contract.contract_months:
            return False, f"Invalid month code {month_code} for {root_id}"

        # Check year is numeric
        if not year.isdigit():
            return False, f"Invalid year format: {year}"

        return True, None


# Example usage functions
def example_usage():
    """Example of how to use the FuturesSpecReader"""

    # Initialize reader
    reader = FuturesSpecReader("../specs/fut_spec.yaml")

    # Get specific contract
    cl_contract = reader.get_contract("CL")
    if cl_contract:
        print(f"WTI Crude: {cl_contract.description}")
        print(f"Exchange: {cl_contract.exchange}")
        print(f"Trading hours: {cl_contract.open_time_utc} - {cl_contract.close_time_utc} UTC")

        # Get active contracts for 2025
        active_2025 = cl_contract.get_active_contracts(2025)
        print(f"Active 2025 contracts: {active_2025}")

    # Get all energy contracts
    energy_contracts = reader.get_contracts_by_category("energy")
    print(f"\nEnergy contracts: {[c.root_id for c in energy_contracts]}")

    # Get all NYMEX contracts
    nymex_contracts = reader.get_contracts_by_exchange("NYMEX")
    print(f"\nNYMEX contracts: {[c.root_id for c in nymex_contracts]}")

    # Generate symbols for current year, next 3 months
    symbols = reader.get_active_symbols(
        root_ids=["CL", "GC"],
        year=2025,
        month=1,
        count=3
    )
    print(f"\nNext 3 months symbols for CL and GC: {symbols}")

    # Get front month contracts
    front_months = reader.get_front_month_symbols(["CL", "GC", "SI"])
    print(f"\nFront month contracts: {front_months}")

    # Check what's trading now
    trading_now = reader.get_trading_contracts()
    print(f"\nCurrently trading: {[c.root_id for c in trading_now]}")

    # Validate a symbol
    is_valid, error = reader.validate_symbol("CLZ25")
    print(f"\nCLZ25 valid: {is_valid}")

    is_valid, error = reader.validate_symbol("XXZ25")
    print(f"XXZ25 valid: {is_valid}, error: {error}")


# Integration with dataflow framework
def load_futures_config_for_dataflow(config_path: str = "fut_spec.yaml") -> Dict[str, List[str]]:
    """
    Load futures configuration for dataflow framework integration

    Args:
        config_path: Path to YAML configuration

    Returns:
        Dictionary mapping extractors to symbol lists
    """
    reader = FuturesSpecReader(config_path)

    # Example: Configure symbols for different data sources
    config = {
        "databento_realtime": [],
        "databento_historical": [],
        "bloomberg_historical": []
    }

    # Get front 5 contracts for energy futures for real-time
    energy_contracts = reader.get_contracts_by_category("energy")
    for contract in energy_contracts:
        symbols = contract.get_active_contracts(2025, 1)[:5]
        config["databento_realtime"].extend(symbols)

    # Get all precious metals for historical
    metals_contracts = reader.get_contracts_by_category("precious_metals")
    for contract in metals_contracts:
        symbols = contract.get_active_contracts(2025)
        config["databento_historical"].extend(symbols)

    # Get base metals for Bloomberg
    base_metals = reader.get_contracts_by_category("base_metals")
    for contract in base_metals:
        symbols = contract.get_active_contracts(2025)[:3]
        config["bloomberg_historical"].extend(symbols)

    return config


if __name__ == "__main__":
    example_usage()