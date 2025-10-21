from pathlib import Path
from typing import Dict, Generic, TypeVar, Optional

T = TypeVar("T")


class BaseSpecReader(Generic[T]):
    """
    Base class for YAML spec readers.
    Subclasses must implement `_load_config` to parse the YAML into `self.specs`.
    """

    def __init__(self, config_path: Optional[str] = None, default_filename: Optional[str] = None):
        if config_path is not None:
            self.config_path = Path(config_path)
        else:
            if default_filename is None:
                raise ValueError("Either 'config_path' or 'default_filename' must be provided.")
            self.config_path = Path(__file__).resolve().parent.parent / "specs" / default_filename
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path.resolve()}")
        self.raw_data: Dict[str, T] = {}
        self.specs: Dict[str, T] = self._load_config()

    def __iter__(self):
        return iter(self.all())

    def _load_config(self) -> Dict[str, T]:
        """Load and parse the YAML file, returning a map of root_id -> spec.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("_load_config must be implemented by subclasses.")

    def get_spec(self, root_id: str) -> T:
        """Return the spec for a given `root_id`. Raises KeyError if missing."""
        return self.specs.get(root_id)

    def all(self) -> Dict[str, T]:
        """Return a shallow copy of all specs."""
        return self.specs.copy()