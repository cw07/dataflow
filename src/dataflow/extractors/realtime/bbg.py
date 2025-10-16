import os

from functools import partial
from typing import Any, Optional

from dataflow.outputs import output_router
from dataflow.utils.loop_control import RealTimeLoopControl
from dataflow.config.loaders.time_series_loader import TimeSeriesQueryResult
from dataflow.extractors.realtime.base_realtime import BaseRealtimeExtractor


class BBGRealtimeExtractor(BaseRealtimeExtractor):
    vendor = "bbg"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series: TimeSeriesQueryResult = self.config["time_series"]
        self.bbg_client = None

    def validate_config(self) -> None:
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self):
        pass

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    def start_extract(self):
        pass

    def stop_extract(self):
        pass

    def set_handler(self):
        output = self.config["output"]
        return partial(output_router.route, output=output, config=self.config)