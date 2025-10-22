import logging
import databento as db

from tradetools.bdate import BDate

from dataflow.config.settings import settings
from dataflow.symbology.base import BaseSymbolResolver

logger = logging.getLogger(__name__)


class DatabentoSymbolResolve(BaseSymbolResolver):

    def __init__(self,
                 symbol_type_in: str = "continuous",
                 symbol_type_out: str = "raw_symbol",
                 continuous_type: str = "c"  # n: open interest, v: volume
                 ):
        self.client = db.Historical(settings.databento_api_key)
        self.mapping = {}
        self.symbol_type_in = symbol_type_in
        self.symbol_type_out = symbol_type_out
        self.continuous_type = continuous_type

    def resolve(self, input_symbols: list[str], data_set: str) -> dict[str, str]:
        if data_set == "GLBX.MDP3":
            return self.resolve_cme(input_symbols)
        else:
            logger.error(f"{data_set} resolver not implemented")
            return {}

    def resolve_cme(self, input_symbols: list) -> dict[str, str]:
        start_date = BDate("T").delta_calendar_day(-1).date_str
        end_date = BDate("T").date_str

        # Step 1: Build series_id → db_id mapping
        series_id_to_db_id = {
            symbol: f"{symbol.split('.')[1]}.{self.continuous_type}.{int(symbol.split('.')[2]) - 1}"
            for symbol in input_symbols
        }

        # Step 2: Resolve databento_id → instrument_id
        instrument_id_res = self.client.symbology.resolve(
            dataset="GLBX.MDP3",
            symbols=list(series_id_to_db_id.values()),
            stype_in=self.symbol_type_in,
            stype_out="instrument_id",  # Must be instrument_id here, db don't support to raw_symbol diretly
            start_date=start_date,
            end_date=end_date,
        )
        if instrument_id_res["message"] != "OK":
            raise ValueError(f"Failed to resolve instrument ID: {instrument_id_res['message']}")

        db_id_to_db_instrument_id = {}
        for db_id, instrument_id in instrument_id_res["result"].items():
            db_id_to_db_instrument_id[db_id] = instrument_id[0]["s"]

        # Step 3: Resolve instrument_id → raw_symbol
        raw_symbols_res = self.client.symbology.resolve(
            dataset="GLBX.MDP3",
            symbols=list(db_id_to_db_instrument_id.values()),
            stype_in="instrument_id",
            stype_out="raw_symbol",
            start_date=start_date,
            end_date=end_date,
        )
        if raw_symbols_res["message"] != "OK":
            raise ValueError(f"Failed to resolve raw symbol: {raw_symbols_res['message']}")

        instrument_id_to_raw_symbol ={}
        for instrument_id, raw_symbol in raw_symbols_res["result"].items():
            instrument_id_to_raw_symbol[instrument_id] = raw_symbol[0]["s"]

        # Step 4: Chain mappings: series_id → raw_symbol
        series_id_to_raw_symbol = {}
        for series_id in input_symbols:
            db_id = series_id_to_db_id[series_id]
            inst_id = db_id_to_db_instrument_id.get(db_id)
            if inst_id is None:
                raise KeyError(f"No instrument_id found for db_id: {db_id}")
            raw_sym = instrument_id_to_raw_symbol.get(inst_id)
            if raw_sym is None:
                raise KeyError(f"No raw_symbol found for instrument_id: {inst_id}")
            series_id_to_raw_symbol[series_id] = raw_sym
        return series_id_to_raw_symbol

db_symbol_resolver = DatabentoSymbolResolve()









