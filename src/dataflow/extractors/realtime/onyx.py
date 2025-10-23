import os
import time
import logging
import requests
from typing import Any, Optional

from dataflow.config.settings import settings
from dataflow.outputs import output_router
from dataflow.utils.loop_control import RuntimeControl
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.extractors.realtime.base_realtime import BaseRealtimeExtractor

logger = logging.getLogger(__name__)

runtime_control = RuntimeControl(start=os.environ["EXTRACT_START_TIME"],
                                 end=os.environ["EXTRACT_END_TIME"],
                                 poll_seconds=1.0)


class OnyxRealtimeExtractor(BaseRealtimeExtractor):

    vendor = "onyx"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]
        self.mapping: dict[str, TimeSeriesConfig] = {s.symbol: s for s in self.time_series}
        self.root_ids = {s.root_id for s in self.time_series}
        self.headers = {
            "Authorization": f"Bearer {settings.onyx_api_key}",
        }

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

    @runtime_control
    def start_extract(self):
        for root_id in self.root_ids:
            try:
                url = f"{settings.onyx_url}/tickers/live/{root_id}"
                response = requests.get(url, headers=self.headers)
                data = response.json()
                for d in data:
                    if d["symbol"] in self.mapping:
                        self.on_message(d)
            except Exception as e:
                logger.error(f"Error fetching {root_id} from Onyx: {e}")

    def stop_extract(self):
        logger.info(f"Onyx realtime extractor stopped gracefully")

    def on_message(self, message):
        symbol = message["symbol"]
        time_series: TimeSeriesConfig = self.mapping[symbol]
        new_message = {
            "asset_type": time_series.series_type,
            "vendor": time_series.data_source,
            "symbol": time_series.symbol,
            "price": message["mid"],
            "ts_event": message["timestamp"]
        }
        output_router.route(message=new_message, time_series=time_series)
        return 1
