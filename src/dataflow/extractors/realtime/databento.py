import os
import logging
import databento as db
from collections import defaultdict
from typing import Callable, Dict, Any, Optional

from dataflow.outputs import output_router
from dataflow.config.settings import settings
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
        self.data_sets: dict = self.get_data_sets()
        self.dbento_mapping: dict[int, str] = {}
        self.mapping: dict[str, TimeSeriesConfig] = {s.symbol: s for s in self.time_series}
        self.dbento_client = None
        self.error_handler = None

    def validate_config(self) -> None:
        pass

    def connect(self):
        api_key = settings.databento_api_key
        try:
            self.dbento_client = db.Live(key=api_key)
            self._is_connected = True
        except Exception as e:
            logger.error(e)

    def disconnect(self):
        pass

    def subscribe(self):
        for data_set, schema_to_symbols in self.data_sets.items():
            for schema, symbols in schema_to_symbols.items():
                self.dbento_client.subscribe(
                    dataset=data_set,
                    schema=schema,
                    stype_in="raw_symbol",
                    symbols=symbols
                )

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    @loop_control
    def start_extract(self):
        self.connect()
        self.subscribe()
        self.dbento_client.add_callback(record_callback=self.on_message,
                                        exception_callback=self.error_handler)
        self.dbento_client.start()

    def stop_extract(self):
        self.unsubscribe()
        self.dbento_client.block_for_close(timeout=10)

    def get_data_sets(self):
        data_sets = defaultdict(lambda: defaultdict(list))
        for ts in self.time_series:
            data_set = ts.additional_params.get("dataset")
            schema = ts.data_schema
            symbol = ts.symbol
            if not data_set:
                raise ValueError(f"Databento time series must have dataset in parameters: {ts.additional_params}")
            data_sets[data_set][schema].append(symbol)
        return data_sets

    def on_message(self, message: db.DBNRecord) -> None:
        if isinstance(message, db.SystemMsg):
            self.handle_system_msg(message)
        elif isinstance(message, db.SymbolMappingMsg):
            self.handle_symbol_mapping(message)
        elif isinstance(message, db.MBP1Msg):
            self.handle_mbp1(message)
        else:
            pass

    def on_failure(self, exception: Exception) -> None:
        logger.error(exception)

    def on_reconnect(self, start, end) -> None:
        pass

    def handle_mbp1(self, msg: db.MBP1Msg) -> None:
        symbol = self.dbento_mapping[msg.instrument_id]
        time_series = self.mapping[symbol]
        return output_router.route(message=msg, time_series=time_series)

    def handle_symbol_mapping(self, msg: db.SymbolMappingMsg):
        self.dbento_mapping[msg.instrument_id] = msg.stype_in_symbol

    @staticmethod
    def handle_system_msg(msg: db.SystemMsg):
        logger.info(f"Received system msg: {msg}")
