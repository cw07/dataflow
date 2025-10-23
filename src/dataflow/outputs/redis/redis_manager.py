import redis
import logging

from dataflow.utils.schema_map import SCHEMA_MAP
from dataflow.config.settings import RedisConfig
from dataflow.outputs.base import BaseOutputManager
from dataflow.config.loaders.time_series import TimeSeriesConfig

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
            self.redis.xadd(name=redis_key,
                            fields=data_object.to_dict_redis(),
                            id="*",
                            maxlen=20,
                            approximate=True
                            )
        elif store_type == "kv":
            value_field = store_config.get("field")
            if value_field is None:
                raise KeyError(f"store type is kv, but field is missing: {store_config}")
            else:
                value = data_object.to_dict()[value_field]
                self.redis.set(redis_key, value)
        else:
            raise ValueError(f"Unknown redis store type: {store_type}")


class RedisManager(BaseOutputManager):
    def __init__(self, config: dict[str, RedisConfig]):
        super().__init__(config)
        self.redis_instance: dict = {}
        self.init_redis()

    def init_redis(self):
        for redis_id, redis_cfg in self.config.items():
            scheme = "rediss" if redis_cfg.ssl else "redis"
            if redis_cfg.username:
                auth = f"{redis_cfg.username}:{redis_cfg.password}"
            else:
                auth = f":{redis_cfg.password}"
            redis_url = f"{scheme}://{auth}@{redis_cfg.host}:{redis_cfg.port}/{redis_cfg.db}"
            pool = redis.ConnectionPool.from_url(url=redis_url, decode_responses=True)
            redis_instance = redis.Redis(connection_pool=pool)
            if redis_instance.ping():
                logger.info(f"{redis_id} connect success")
                self.redis_instance[redis_id] = RedisWrapper(redis_instance)
            else:
                logger.error(f"{redis_id} connect failed")

    def save(self, message, time_series: TimeSeriesConfig):
        market_data_obj = SCHEMA_MAP[time_series.data_schema].from_dict(message)
        for destination_name in time_series.destination:
            if destination_name in self.redis_instance:
                logger.info(f"Saving {destination_name} {market_data_obj} for {time_series}")
                self.redis_instance[destination_name].save_data(market_data_obj, time_series)
            else:
                logger.error(f"No active instance for {destination_name}")