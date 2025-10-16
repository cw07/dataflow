import os
import logging
import databento as db
from typing import Callable, Dict, Any, Optional

from dataflow.outputs import output_router
from dataflow.utils.loop_control import RealTimeLoopControl
from dataflow.config.loaders.time_series_loader import TimeSeriesConfig
from dataflow.extractors.realtime.base_realtime import BaseRealtimeExtractor

logger = logging.getLogger(__name__)

loop_control = RealTimeLoopControl(start=os.environ["EXTRACT_START_TIME"],
                                   end=os.environ["EXTRACT_END_TIME"])


class DatabentoRealtimeExtractor(BaseRealtimeExtractor):
    vendor = "databento"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]
        self.schemas = set(ts.data_schema for ts in self.time_series)
        self.mapping: dict[str, TimeSeriesConfig] = {}
        self.dbento_client = None
        self.error_handler = None

    def validate_config(self) -> None:
        assert "api_key" in self.config
        assert "dataset" in self.config
        assert "realtime_schema" in self.config
        assert "stype_in" in self.config
        assert "symbols" in self.config
        assert "output" in self.config
        assert "asset_type" in self.config
        assert "vendor" in self.config

    def connect(self):
        api_key = self.config.get("api_key")
        try:
            self.dbento_client = db.Live(key=api_key)
            self._is_connected = True
        except Exception as e:
            logger.error(e)

    def disconnect(self):
        pass

    def subscribe(self, symbols: list):
        for schema in self.schemas:
            symbols = [s.symbol for s in self.time_series]
            self.dbento_client.subscribe(
                dataset=self.config["dataset"],
                schema=schema,
                stype_in=self.config["stype_in"],
                symbols=symbols
            )

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    @loop_control
    def start_extract(self):
        self.subscribe(self.config.get('symbols'))
        self.dbento_client.add_callback(self.on_message)
        self.dbento_client.start()

    def stop_extract(self):
        self.unsubscribe()
        self.dbento_client.block_for_close(timeout=10)

    def on_message(self, message) -> Callable:
        symbol = message["symbol"]
        time_series = self.mapping[symbol]
        return output_router.route(message=message, time_series=time_series)
