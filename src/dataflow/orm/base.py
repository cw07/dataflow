from abc import ABC, abstractmethod
from typing import Any, Type, List, Dict


class BaseORMAdapter(ABC):

    def __init__(self, config: dict):
        self.config = config


    @abstractmethod
    def create_engine(self):
        pass

    @abstractmethod
    def create_session(self):
        pass

    @abstractmethod
    def create_model(self, name: str, fields: Dict[str, Any]) -> Type:
        """Dynamically create ORM model"""
        pass

    @abstractmethod
    def create_table(self, models: List[Type]) -> None:
        """Create database tables for models"""
        pass

    @abstractmethod
    def save_data(self, data_obj):
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