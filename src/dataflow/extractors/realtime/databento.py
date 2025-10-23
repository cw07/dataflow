import os
import logging
import databento as db
from collections import defaultdict
from typing import Callable, Dict, Any, Optional

from dataflow.outputs import output_router
from dataflow.config.settings import settings
from dataflow.utils.databento import VENUE_DATASET_MAP
from dataflow.utils.loop_control import RuntimeControl
from dataflow.config.loaders.time_series import TimeSeriesConfig
from dataflow.symbology.databento_resolver import db_symbol_resolver
from dataflow.extractors.realtime.base_realtime import BaseRealtimeExtractor

logger = logging.getLogger(__name__)

loop_control = RuntimeControl(start=os.environ["EXTRACT_START_TIME"],
                              end=os.environ["EXTRACT_END_TIME"],
                              run_in_thread=True
                              )


class DatabentoRealtimeExtractor(BaseRealtimeExtractor):
    vendor = "databento"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.time_series: list[TimeSeriesConfig] = self.config["time_series"]
        self.data_sets: dict = self.get_data_sets()
        self.instrument_id_to_raw_sym: dict[int, str] = {}
        self.resolve_raw_symbols()
        self.raw_sym_to_ts: dict[str, TimeSeriesConfig] = {s.symbol: s for s in self.time_series}
        self.dbento_client = None

    def validate_config(self) -> None:
        pass

    def connect(self):
        api_key = settings.databento_api_key
        try:
            self.dbento_client = db.Live(key=api_key,
                                         reconnect_policy="reconnect"
                                         )
            self._is_connected = True
        except Exception as e:
            logger.error(e)

    def disconnect(self):
        pass

    def subscribe(self):
        for data_set, schema_to_all_ts in self.data_sets.items():
            for schema, all_ts in schema_to_all_ts.items():
                db_raw_symbols = [ts.symbol for ts in all_ts]
                logger.info(f"Subscribing to {data_set} {schema} for {len(db_raw_symbols)} symbols")
                if db_raw_symbols:
                    self.dbento_client.subscribe(
                        dataset=data_set,
                        schema=schema,
                        stype_in="raw_symbol",
                        symbols=db_raw_symbols,
                    )
                else:
                    logger.error(f"No symbol found for {data_set} {schema}, skip subscription")

    def resubscribe(self, symbols: Optional[list] = None):
        pass

    def unsubscribe(self, symbols: Optional[list] = None):
        pass

    @loop_control
    def start_extract(self):
        try:
            self.connect()
            self.subscribe()
            self.dbento_client.add_callback(record_callback=self.on_message,
                                            exception_callback=self.on_error)
            self.dbento_client.add_reconnect_callback(reconnect_callback=self.on_reconnect,
                                                      exception_callback=self.on_error)
            self.dbento_client.start()
        except Exception as e:
            logger.error(f"Error when extracting databento: {e}, shutting down")
            if self._is_connected and self.dbento_client is not None:
                self.dbento_client.block_for_close(timeout=30)

    def stop_extract(self):
        if self.dbento_client is not None:
            self.dbento_client.stop()
            res = self.dbento_client.block_for_close(timeout=60)
            if res is None:
                logger.info(f"Databento realtime extractor stopped gracefully")
            else:
                logger.error(f"Databento realtime extractor stopped with error: {res}")

    def resolve_raw_symbols(self):
        mapping = {}  # {series_id: raw_symbol}
        for data_set, schema_to_ts in self.data_sets.items():
            series_id_in_data_set = []
            for schema, all_ts in schema_to_ts.items():
                series_id_in_data_set.extend([ts.series_id for ts in all_ts])
            mapping.update(db_symbol_resolver.resolve(series_id_in_data_set, data_set))
        for ts in self.time_series:
            ts.symbol = mapping.get(ts.series_id, ts.symbol)

    def get_data_sets(self) -> dict[str, dict[str, list[TimeSeriesConfig]]]:
        data_sets = defaultdict(lambda: defaultdict(list))
        for ts in self.time_series:
            data_set = VENUE_DATASET_MAP.get(ts.venue)
            if data_set is None:
                logger.error(
                    f"Cannot find the dataset for {ts.venue}, "
                    f"please make sure the dataset is configured in VENUE_DATASET_MAP")
            schema = ts.data_schema
            data_sets[data_set][schema].append(ts)
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

    def on_error(self, exception: Exception) -> None:
        logger.error(exception)

    def on_reconnect(self, start, end):
        logger.info(f"reconnection gap from {start} to {end}")

    def handle_mbp1(self, msg: db.MBP1Msg) -> None:
        try:
            symbol = self.instrument_id_to_raw_sym[msg.instrument_id]
            time_series = self.raw_sym_to_ts[symbol]

            new_msg = {
                "asset_type": time_series.series_type,
                "vendor":  time_series.data_source,
                "symbol": symbol,
                "price": msg.pretty_price,
                "ts_event": msg.pretty_ts_event.isoformat(),
                "ts_recv": msg.pretty_ts_recv.isoformat(),
                "ts_in_delta": msg.ts_in_delta,
                "action": str(msg.action),
                "side": str(msg.side),
                "size": msg.size,
                "instrument_id": msg.instrument_id,
                "publisher_id": msg.publisher_id,
                "rtype": str(msg.rtype),
                "sequence": msg.sequence,
                "flags": msg.flags,
                "bid_px_00": msg.levels[0].bid_px * 0.000000001,
                "bid_sz_00": msg.levels[0].bid_sz,
                "bid_ct_00": msg.levels[0].bid_ct,
                "ask_px_00": msg.levels[0].ask_px * 0.000000001,
                "ask_sz_00": msg.levels[0].ask_sz,
                "ask_ct_00": msg.levels[0].ask_ct,
                "mid_px_00": (msg.levels[0].bid_px * 0.000000001 + msg.levels[0].ask_px * 0.000000001) / 2,
            }
            output_router.route(message=new_msg, time_series=time_series)
        except Exception as e:
            logger.error(f"Error handle mbp1: {e}")

    def handle_symbol_mapping(self, msg: db.SymbolMappingMsg):
        self.instrument_id_to_raw_sym[msg.instrument_id] = msg.stype_in_symbol

    @staticmethod
    def handle_system_msg(msg: db.SystemMsg):
        logger.info(f"Received system msg: {msg}")
