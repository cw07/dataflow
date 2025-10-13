import os
from functools import partial
from typing import Any, Optional

from ...outputs import output_router
from ...utils.loop_control import RealTimeLoopControl
from ...extractors.realtime.base_realtime import BaseRealtimeExtractor


class FluxRealtimeExtractor(BaseRealtimeExtractor):

    vendor = "flux"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series = self.config["time_series"]
        self.bbg_client = None

    def validate_config(self) -> None:
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, symbols: list):
        pass

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    @RealTimeLoopControl(start=os.environ["EXTRACT_START_TIME"], end=os.environ["EXTRACT_END_TIME"])
    def start_extract(self):
        pass

    def stop_extract(self):
        pass

    def set_handler(self):
        output = self.config["output"]
        return partial(output_router.route, output=output, config=self.config)