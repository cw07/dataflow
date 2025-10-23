import logging

from dataflow.orm.base import BaseORMAdapter
from dataflow.utils.schema_map import SCHEMA_MAP
from dataflow.outputs.base import BaseOutputManager
from dataflow.config.loaders.time_series import TimeSeriesConfig

logger = logging.getLogger(__name__)


class FileManager(BaseOutputManager):
    def __init__(self, config: dict):
        super().__init__(config)

    def save(self, message, orm: str, data_model):
        pass