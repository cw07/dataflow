import logging

from datacore.models.mktdata.outputs import DataOutput

from dataflow.config.settings import settings
from dataflow.outputs.file.file_manager import FileManager
from dataflow.outputs.redis.redis_manager import RedisManager
from dataflow.outputs.database.db_manager import DatabaseManager
from dataflow.config.loaders.time_series import TimeSeriesConfig

logger = logging.getLogger(__name__)


class OutputRouter:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not OutputRouter._initialized:
            OutputRouter._initialized = True
            db_manager = DatabaseManager(settings.all_databases())
            redis_manager = RedisManager(settings.all_redis())
            file_manager = FileManager(settings.all_files())
            self.outputs = {
                DataOutput.database: db_manager,
                DataOutput.redis: redis_manager,
                DataOutput.file: file_manager
            }

    def execute(self, data):
        pass

    @staticmethod
    def decorate(message: dict, time_series):
        return message

    def route(self, message, time_series: TimeSeriesConfig):
        for output in time_series.destination:
            if "database" in output or "db" in output:
                output_type = DataOutput.database
                message = self.decorate(message, time_series)
            elif "redis" in output:
                output_type = DataOutput.redis
            elif "file" in output:
                output_type = DataOutput.file
            else:
                output_type = None
                logger.info(f"Log only: {message}")
            if output_type:
                output_model = self.outputs.get(output_type)
                output_model.save(message, time_series)
            else:
                if output_type is not None:
                    logger.info(f"Cannot find output model for the output type: {output_type}")


output_router = OutputRouter()