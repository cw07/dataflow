from typing import Dict, Any, Type, List

from dataflow.orm.base import BaseORMAdapter

def create_sqlalchemy_model(instance):
    pass


def sqlalchemy_database(db_type, **config):
    pass


class SQLAlchemyDB(BaseORMAdapter):

    def __init__(self, config):
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
        pass

    def create_session(self):
        pass