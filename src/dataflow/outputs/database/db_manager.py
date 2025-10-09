from datacore.models.orm import ORM
from datacore.models.mktdata.realtime import MarketByPrice1

from ..base import BaseOutputManager
from ...orm.peewee import create_peewee_model, peewee_database
from ...orm.sqlalchemy import create_sqlalchemy_model, sqlalchemy_database


schema_dataclass_map = {
    "mbp1": MarketByPrice1
}


class DatabaseManager(BaseOutputManager):
    """Manage database connections and models"""
    def __init__(self):
        super().__init__()
        self.db_instance = None

    def save(self, message, config: dict):
        orm = config["orm"]
        schema = config["schema"]
        market_data = schema_dataclass_map[schema].from_dict(message)

        if orm == ORM.PEEWEE:
            self.db_instance = peewee_database(config)
            orm_model = create_peewee_model(instance=market_data)
            self.db_instance.create_tables([orm_model], safe=True)
            data = market_data.to_dict()
            record = orm_model.create(**data)


