# orm/base.py
from abc import ABC, abstractmethod
from typing import Any, Type, List, Dict


class BaseORMAdapter(ABC):
    @abstractmethod
    def create_model(self, name: str, fields: Dict[str, Any]) -> Type:
        """Dynamically create ORM model"""
        pass

    @abstractmethod
    def create_tables(self, models: List[Type]) -> None:
        """Create database tables for models"""
        pass

    @abstractmethod
    def bulk_insert(self, model: Type, data: List[Dict]) -> None:
        """Bulk insert data into table"""
        pass

    @abstractmethod
    def insert_one(self, model: Type, data: Dict) -> Any:
        """Insert single record"""
        pass

    @abstractmethod
    def connect(self, config: Dict[str, Any]) -> None:
        """Establish database connection"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close database connection"""
        pass