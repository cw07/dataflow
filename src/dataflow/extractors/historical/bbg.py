import os
import logging
from typing import Any, Optional

from datacore.models.assets import AssetType
from tradetools.bdate import BDate
from datacore.models.mktdata.historical import OHLCV1D
from datacore.models.mktdata.schema import MktDataSchema

from dataflow.outputs import output_router
from dataflow.config.settings import settings
from dataflow.utils.common import parse_web_response
from dataflow.utils.loop_control import RuntimeControl
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.symbology.bbg_resolver import bbg_symbol_resolver
from dataflow.extractors.historical.base_historical import BaseHistoricalExtractor


logger = logging.getLogger(__name__)


runtime_control = RuntimeControl(start=os.environ["EXTRACT_START_TIME"],
                                 end=os.environ["EXTRACT_END_TIME"],
                                 poll_seconds=1
                                 )


class BBGHistoricalExtractor(BaseHistoricalExtractor):
    vendor = "bbg"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]
        runtime_control.set_max_job(len(self.time_series))

    def stop_extract(self) -> None:
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def validate_config(self):
        pass

    def start_extract(self) -> Optional[int]:

        start = self.config["start_range"] if self.config["start_range"] else BDate("T-1").date.isoformat()
        end = self.config["end_range"] if self.config["end_range"] else BDate("T-1").date.isoformat()
        if self.config["asset_type"] == AssetType.FUT_OPTION:
            underlyings = {}

            for ts in self.time_series:
                underlyings[ts.root_id] = ts

            all_futures = list(underlyings.keys())

            all_options = bbg_symbol_resolver.resolve(input_symbols=all_futures, market="LME", resolve_type=AssetType.FUT_OPTION)
            for series_id, option_symbols in all_options.items():
                data = self.extract_price(option_symbols)
                self.on_message(data, underlyings[series_id])

        else:
            raise NotImplementedError(f"{self.config['asset_type']} not implemented")

    def extract_price(self, symbols: list[str]):
        pass


    def on_message(self, data, time_series: TimeSeriesConfig):

        new_data = {

        }

        output_router.route(message=new_data, time_series=time_series)
