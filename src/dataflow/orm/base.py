import logging
from abc import ABC, abstractmethod
from typing import Any, Type, Optional

from dataflow.config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


class BaseORMAdapter(ABC):

    def __init__(self, config: DatabaseConfig):
        self.config = config

    @abstractmethod
    def create_engine(self):
        pass

    @abstractmethod
    def create_model(self, name: str, fields: dict[str, Any]) -> Type:
        """Dynamically create ORM model"""
        pass

    @abstractmethod
    def create_table(self, models: list[Type]) -> None:
        """Create database tables for models"""
        pass

    @abstractmethod
    def save_data(self, data_obj):
        pass

    @abstractmethod
    def bulk_insert(self, model: Type, data: list[dict]) -> None:
        """Bulk insert data into table"""
        pass

    @abstractmethod
    def insert_one(self, model: Type, data: dict) -> Any:
        """Insert single record"""
        pass

    @abstractmethod
    def connect(self, config: dict[str, Any]) -> None:
        """Establish database connection"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close database connection"""
        pass


class LazyDB:
    def __init__(self, db_class: Type):
        self.db_class: Type = db_class
        self.config: Optional[DatabaseConfig] = None
        self._instance = None

    def __call__(self, config: DatabaseConfig):
        self.config = config

    def __getattr__(self, method):
        return getattr(self.get_instance(), method)

    def __repr__(self):
        return repr(self.get_instance())

    def __str__(self):
        return str(self.get_instance())

    def _get_instance(self):
        if self._instance is None:
            self._instance = self.db_class(self.config)
        return self._instance


