import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseExtractor(ABC):
    """Base class for all data extractors"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validate_config()
        self._session = None
        self._is_connected = False

    @abstractmethod
    def connect(self):
        """Initialize connection/session"""
        pass

    @abstractmethod
    def disconnect(self):
        """Close connection/session"""
        pass

    @abstractmethod
    def validate_config(self):
        """Validate configuration parameters"""
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        return self._is_connected