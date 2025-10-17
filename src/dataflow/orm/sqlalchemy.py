import logging

from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, Type, List
from sqlalchemy import URL, Engine, create_engine, text

from dataflow.config.settings import DatabaseConfig
from dataflow.orm.base import BaseORMAdapter, LazyDB


logger = logging.getLogger(__name__)


class SQLAlchemyDB(BaseORMAdapter):

    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.engine = self.create_engine()
        self.session = self.create_session()

    def create_model(self, name: str, fields: Dict[str, Any]) -> Type:
        pass

    def create_table(self, models: List[Type]) -> None:
        pass

    def save_data(self, data_obj):
        pass

    def bulk_insert(self, model: Type, data: List[Dict]) -> None:
        pass

    def insert_one(self, model: Type, data: Dict) -> Any:
        pass

    def connect(self, config: Dict[str, Any]) -> None:
        pass

    def close(self) -> None:
        pass

    def create_engine(self):
        conn_str = self.config.connection_params()
        engine = create_engine(url=conn_str,
                               pool_size=self.config.connection_pool_max_size,
                               pool_recycle=self.config.connection_pool_recycle)
        logger.info(f"Connected to {self.config.id}")
        return engine

    def create_session(self):
        if self.engine:
            return sessionmaker(self.engine)
        else:
            raise ValueError("Must create engine before create session")


sqlalchemy_db = LazyDB(SQLAlchemyDB)