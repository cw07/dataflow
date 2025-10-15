import logging
from typing import Any

import requests
from datacore.models.mktdata.historical import OHLCV1D
from datacore.models.mktdata.schema import MktDataSchema

from dataflow.config.settings import settings
from dataflow.outputs import output_router
from dataflow.config.loaders.time_series_loader import TimeSeriesConfig
from dataflow.extractors.historical.base_historical import BaseHistoricalExtractor

logger = logging.getLogger(__name__)


class OnyxHistoricalExtractor(BaseHistoricalExtractor):
    vendor = "onyx"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]

    def connect(self):
        pass

    def disconnect(self):
        pass

    def validate_config(self):
        pass

    def start_extract(self):
        headers = {
            "Authorization": f"Bearer {settings.onyx_api_key}"
        }
        start = self.config["start"]
        end = self.config["end"]

        onyx_period_map = {
            MktDataSchema.OHLCV_1D: "1d"
        }

        params = {
            "start": start,
            "end": end,
        }

        for time_series in self.time_series:
            try:
                symbol = time_series.symbol
                period = onyx_period_map[time_series.data_schema]
                url = f"{settings.onyx_url}/tickers/ohlc/{symbol}/{period}"
                response = requests.get(url, headers=headers, params=params)
                data = response.json()
                for d in data:
                    self.on_message(d, time_series)
            except Exception as e:
                logger.error(f"Error fetching historical {time_series.symbol} from Onyx: {e}")

    def on_message(self, data, time_series):
        new_data = {
            "asset_type": time_series.asset_type,
            "vendor": time_series.data_source,
            "symbol": time_series.symbol,
            "ts_event": data["timestamp"],
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"]
        }
        output_router.route(message=new_data, time_series=time_series)

