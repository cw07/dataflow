import os
import logging
import databento as db
from functools import partial
from typing import Callable, Dict, Any, Optional

from datacore.models.mktdata.outputs import DataOutput
from datacore.models.mktdata.realtime import RealtimeSchema

from dataflow.outputs import output_router
from dataflow.utils.loop_control import RealTimeLoopControl
from dataflow.extractors.realtime.base_realtime import BaseRealtimeExtractor


logger = logging.getLogger(__name__)

DatabentoSchemaMap = {
    RealtimeSchema.MBO: 'mbo',
    RealtimeSchema.MBP_1: 'mbp-1'
}

class DatabentoRealtimeExtractor(BaseRealtimeExtractor):
    vendor = "databento"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
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
        self.dbento_client.subscribe(
            dataset=self.config["dataset"],
            schema=DatabentoSchemaMap[self.config["realtime_schema"]],
            stype_in=self.config["stype_in"],
            symbols=symbols
        )

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    def start_extract(self):
        self.subscribe(self.config.get('symbols'))
        self.dbento_client.add_callback(self.set_handler())
        self.dbento_client.start()

    def stop_extract(self):
        self.unsubscribe()
        self.dbento_client.block_for_close(timeout=10)

    def set_handler(self) -> Callable:
        output = self.config["output"]
        return partial(output_router.route, output=output, config=self.config)
