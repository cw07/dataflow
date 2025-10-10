from datacore.models.mktdata.outputs import DataOutput

from .database.db_manager import DatabaseManager
from .file.file_manager import FileManager
from .redis.redis_manager import RedisManager


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
            db_manager = DatabaseManager()
            file_manager = FileManager()
            redis_manager = RedisManager()
            self.outputs = {
                DataOutput.database: db_manager,
                DataOutput.redis: redis_manager,
                DataOutput.file: file_manager
            }

    def execute(self, data):
        pass

    @staticmethod
    def decorate(message: dict, output, config):
        if output == DataOutput.database:
            message["vendor"] = config["vendor"]
            message["asset_type"] = config["asset_type"]
        return message


    def route(self, message, output: DataOutput, config):
        decorated_message = self.decorate(message, output, config)
        output_model = self.outputs[output]
        output_model.save(decorated_message, config)


output_router = OutputRouter()