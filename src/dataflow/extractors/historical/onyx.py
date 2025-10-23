import os
import logging
from typing import Any

import requests
from datacore.models.mktdata.historical import OHLCV1D
from datacore.models.mktdata.schema import MktDataSchema

from dataflow.outputs import output_router
from dataflow.config.settings import settings
from dataflow.utils.common import parse_web_response
from dataflow.utils.loop_control import RuntimeControl
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.symbology.onyx_resolver import onyx_symbol_resolver
from dataflow.extractors.historical.base_historical import BaseHistoricalExtractor
from tradetools.bdate import BDate

logger = logging.getLogger(__name__)


runtime_control = RuntimeControl(start=os.environ["EXTRACT_START_TIME"],
                                 end=os.environ["EXTRACT_END_TIME"]
                                 )


class OnyxHistoricalExtractor(BaseHistoricalExtractor):
    vendor = "onyx"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]
        runtime_control.set_max_job(len(self.time_series))
        self.resolve_raw_symbols()

    def connect(self):
        pass

    def disconnect(self):
        pass

    def validate_config(self):
        pass

    def stop_extract(self) -> None:
        pass

    def resolve_raw_symbols(self):
        series_ids = [s.series_id for s in self.time_series]
        series_id_to_raw_symbol = onyx_symbol_resolver.resolve(series_ids)
        for s in self.time_series:
            if s.series_id in series_id_to_raw_symbol:
                s.symbol = series_id_to_raw_symbol[s.series_id]

    @runtime_control
    def start_extract(self):
        total_done = 0
        headers = {
            "Authorization": f"Bearer {settings.onyx_api_key}"
        }

        start = self.config["start"] if self.config["start"] else BDate("T-1").date.isoformat()
        end = self.config["end"] if self.config["start"] else BDate("T-1").date.isoformat()

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
                if not symbol:
                    logger.error(f"No symbol found for {time_series.series_id}")
                    continue
                period = onyx_period_map[time_series.data_schema]
                url = f"{settings.onyx_url}/tickers/ohlc/{symbol}/{period}"
                resp = requests.get(url, headers=headers, params=params)
                data, error = parse_web_response(resp)
                if error:
                    logger.error(f"Failed to fetch data for {time_series.series_id}: {error}")
                else:
                    for d in data:
                        total_done += self.on_message(d, time_series)
            except Exception as e:
                logger.error(f"Error fetching historical {time_series.symbol} from Onyx: {e}")
        return total_done

    def on_message(self, data, time_series: TimeSeriesConfig):
        new_data = {
            "asset_type": time_series.series_type,
            "vendor": time_series.data_source,
            "symbol": time_series.symbol,
            "ts_event": data["timestamp"],
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"]
        }
        output_router.route(message=new_data, time_series=time_series)
        runtime_control.add_job_done()
        return 1

