import os
import time
import logging
import requests
from typing import Any, Optional

from dataflow.outputs import output_router
from dataflow.config.settings import settings
from dataflow.utils.common import parse_web_response
from dataflow.utils.loop_control import RuntimeControl
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.symbology.onyx_resolver import onyx_symbol_resolver
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
        self.headers = {
            "Authorization": f"Bearer {settings.onyx_api_key}",
        }
        self.resolve_raw_symbols()
        self.raw_sym_to_ts: dict[str, TimeSeriesConfig] = {s.symbol: s for s in self.time_series}

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

    def resolve_raw_symbols(self):
        series_ids = [s.series_id for s in self.time_series]
        series_id_to_raw_symbol = onyx_symbol_resolver.resolve(series_ids)
        for s in self.time_series:
            if s.series_id in series_id_to_raw_symbol:
                s.symbol = series_id_to_raw_symbol[s.series_id]

    @runtime_control
    def start_extract(self):
        for time_series in self.time_series:
            try:
                root_id = time_series.root_id
                id_without_venue = root_id.split(".")[1]
                url = f"{settings.onyx_url}/tickers/live/{id_without_venue}"
                resp = requests.get(url, headers=self.headers)
                data, error = parse_web_response(resp)
                if error:
                    logger.error(f"Failed to fetch data for {time_series.series_id}: {error}")
                else:
                    for d in data:
                        self.on_message(d)
            except Exception as e:
                logger.error(f"Error fetching realtime {time_series.series_id} from Onyx: {e}")

    def stop_extract(self):
        logger.info(f"Onyx realtime extractor stopped gracefully")

    def on_message(self, message):
        symbol = message["symbol"]
        time_series: TimeSeriesConfig = self.raw_sym_to_ts[symbol]
        new_message = {
            "asset_type": time_series.series_type,
            "vendor": time_series.data_source,
            "symbol": time_series.symbol,
            "price": message["mid"],
            "ts_event": message["timestamp"]
        }
        output_router.route(message=new_message, time_series=time_series)
        return 1
