import logging

from dataflow.config.settings import RedisConfig
from dataflow.outputs.base import BaseOutputManager
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.utils.schema_map import SCHEMA_MAP

logger = logging.getLogger(__name__)


class RedisWrapper:

    def __init__(self, redis_client):
        self.redis = redis_client

    def save_data(self, data_object, time_series: TimeSeriesConfig):
        redis_key = data_object.redis_name()
        store_config = time_series.additional_params.get("redis")
        if not store_config:
            raise KeyError(f"Redis store config missing: {time_series}")
        store_type = store_config.get("type")

        if store_type == "stream":
            self.redis.add_to_stream(data_object.to_dict(), redis_key)
        elif store_type == "kv":
            value_field = store_config.get("field")
            if value_field is None:
                raise KeyError(f"store type is kv, but field is missing: {store_config}")
            else:
                value = data_object.to_dict()[value_field]
                self.redis.set_value(redis_key, value)
        else:
            raise ValueError(f"Unknown redis store type: {store_type}")


class RedisManager(BaseOutputManager):
    def __init__(self, config: dict[str, RedisConfig]):
        super().__init__(config)
        self.redis_instance: dict = {}
        self.init_redis()

    def init_redis(self):
        for redis_id, redis_cfg in self.config.items():
            self.redis_instance[redis_id] = RedisWrapper(1)

    def save(self, message, time_series: TimeSeriesConfig):
        market_data_obj = SCHEMA_MAP[time_series.data_schema].from_dict(message)
        for output_name in time_series.destination:
            if output_name in self.redis_instance:
                logger.info(f"Saving {market_data_obj} for {time_series}")
                self.redis_instance[output_name].save_data(market_data_obj, time_series)