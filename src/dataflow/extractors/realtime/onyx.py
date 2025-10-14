import os
import time
import logging
import requests
from typing import Any, Optional

from dataflow.config.settings import settings

from ...outputs import output_router
from ...utils.loop_control import RealTimeLoopControl
from ...config.loaders.time_series_loader import TimeSeriesConfig
from ...extractors.realtime.base_realtime import BaseRealtimeExtractor


logger = logging.getLogger(__name__)


class OnyxRealtimeExtractor(BaseRealtimeExtractor):

    vendor = "onyx"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]
        self.mapping: dict[str, TimeSeriesConfig] = {}
        self.headers = None
        self.root_ids = None

    def validate_config(self) -> None:
        pass

    def connect(self):
        self.headers = {
            "Authorization =": f"Bearer {settings.onyx_api_key}",
        }
        self.mapping = {s.symbol: s for s in self.time_series}
        self.root_ids = {s.root_id for s in self.time_series}

    def disconnect(self):
        pass

    def subscribe(self, symbols: list):
        pass

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    @RealTimeLoopControl(start=os.environ["EXTRACT_START_TIME"],
                         end=os.environ["EXTRACT_END_TIME"]
                         )
    def start_extract(self):
        self.connect()
        while True:
            for root_id in self.root_ids:
                try:
                    url = f"https://api.onyxhub.com/v1/tickers/live/{root_id}"
                    response = requests.get(url, headers=self.headers)
                    data = response.json()
                    for d in data:
                        if d["symbol"] in self.mapping:
                            self.on_message(d)
                except Exception as e:
                    logger.error(f"Error fetching {root_id} from Onyx: {e}")
                time.sleep(1)

    def stop_extract(self):
        pass

    def on_message(self, message):
        symbol = message["symbol"]
        time_series = self.mapping[symbol]
        output_router.route(message=message, time_series=time_series)
