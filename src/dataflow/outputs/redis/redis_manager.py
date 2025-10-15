from dataflow.config.settings import RedisConfig
from dataflow.outputs.base import BaseOutputManager
from dataflow.config.loaders.time_series_loader import TimeSeriesConfig
from dataflow.utils.schema_map import SCHEMA_MAP


class RedisWrapper:

    def __init__(self, redis_client):
        self.redis = redis_client

    def save_data(self, data_object):
        redis_key = data_object.redis_name()
        self.redis.add_to_stream(data_object.to_dict(), redis_key)


class RedisManager(BaseOutputManager):
    def __init__(self, config: dict[str, RedisConfig]):
        super().__init__(config)
        self.redis_instance: dict = {}

    def init_redis(self):
        for redis_id, redis_cfg in self.config.items():
            self.redis_instance[redis_id] = RedisWrapper(1)

    def save(self, message, time_series: TimeSeriesConfig):
        market_data_obj = SCHEMA_MAP[time_series.data_schema].from_dict(message)
        for output_name in time_series.destination:
            if output_name in self.redis_instance:
                self.redis_instance[output_name].save_data(market_data_obj)