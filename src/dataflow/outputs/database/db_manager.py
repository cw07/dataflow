import logging

from dataflow.utils.common import ORM
from dataflow.orm.peewee import LazyPeewee
from dataflow.orm.base import BaseORMAdapter
from dataflow.orm.sqlalchemy import LazySQLAlchemy
from dataflow.utils.schema_map import SCHEMA_MAP
from dataflow.config.settings import DatabaseConfig
from dataflow.outputs.base import BaseOutputManager
from dataflow.config.loaders.time_series_loader import TimeSeriesConfig

logger = logging.getLogger(__name__)


class DatabaseManager(BaseOutputManager):
    """Manage database connections and models"""
    def __init__(self, config: dict[str, DatabaseConfig]):
        super().__init__(config)
        self.db_instance: dict[str, BaseORMAdapter] = {}
        self.init_db()

    def init_db(self):
        for db_id, db_cfg in self.config.items():
            if db_cfg.orm == ORM.SQLALCHEMY:
                self.db_instance[db_id] = LazySQLAlchemy(db_cfg)
            elif db_cfg.orm == ORM.PEEWEE:
                self.db_instance[db_id] = LazyPeewee(db_cfg)
            else:
                raise ValueError(f"Unknown database orm: {db_cfg.orm}")

    def save(self, message, time_series: TimeSeriesConfig):
        market_data_obj = SCHEMA_MAP[time_series.data_schema].from_dict(message)
        for output_name in time_series.destination:
            if output_name in self.db_instance:
                self.db_instance[output_name].save_data(market_data_obj)
