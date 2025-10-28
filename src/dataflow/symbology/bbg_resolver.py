import time
import logging
from bamdata import bloomberg
from bamdata.api import get

from datacore.models.assets import AssetType
from dataflow.config.settings import settings
from dataflow.utils.bbg import DATAFLOW_ID_TO_BBG, BBG_SYMBOL_SPEC
from dataflow.symbology.base import BaseSymbolResolver

bloomberg.FORCE_DELAY_REPOS = set()
logger = logging.getLogger(__name__)


class BBGSymbolResolver(BaseSymbolResolver):
    def resolve(self, input_symbols: list[str], market: str, resolve_type: str) -> dict[str, str]:
        if market == "LME" and resolve_type == AssetType.FUT:
            mapping = self.resolve_lme_futures(input_symbols)
        elif market == "LME" and resolve_type == AssetType.FUT_OPTION:
            mapping = self.resolve_lme_futures_option(input_symbols)
        else:
            mapping = {}
        return mapping

    def resolve_lme_futures(self, symbols: list[str]) -> dict[str, str]:
        futures_mapping = {}
        all_futures = []
        for sym in symbols:
            venue, fut_root, term = sym.split('.')
            dataflow_fut_root = f"{venue}.{fut_root}"
            bbg_future = DATAFLOW_ID_TO_BBG[dataflow_fut_root] + str(term) + BBG_SYMBOL_SPEC[DATAFLOW_ID_TO_BBG[dataflow_fut_root]]["suffix"]
            all_futures.append(bbg_future)

        bbg_fut_mapping = get(
            "BloombergApi.BloombergReferenceData",
            Auth=dict(Type='App', Name=settings.bpipe_app_name),
            Symbols=dict(Symbology="ticker", IDs=all_futures),
            Fields=['FUT_CUR_GEN_TICKER']
        )
        bbg_fut_mapping = dict(zip(bbg_fut_mapping["SYMBOL"], bbg_fut_mapping["FUT_CUR_GEN_TICKER"]))

        for sym in symbols:
            venue, fut_root, term = sym.split('.')
            dataflow_fut_root = f"{venue}.{fut_root}"
            bbg_future = DATAFLOW_ID_TO_BBG[dataflow_fut_root] + str(term) + \
                         BBG_SYMBOL_SPEC[DATAFLOW_ID_TO_BBG[dataflow_fut_root]]["suffix"]
            bbg_id = bbg_fut_mapping[bbg_future] + BBG_SYMBOL_SPEC[DATAFLOW_ID_TO_BBG[dataflow_fut_root]]["suffix"]
            futures_mapping[sym] = bbg_id
            logger.info(f"contract mapping {sym} -> {bbg_id}")
        return futures_mapping

    def resolve_lme_futures_option(self, symbols: list[str]) -> dict[str, str]:
        futures_mapping = self.resolve_lme_futures(symbols)
        options_mapping = {}
        for dataflow_id, bbg_id in futures_mapping.items():
            opt_chain = get(
                "BloombergApi.BloombergReferenceData",
                Auth=dict(Type='App', Name=settings.bpipe_app_name),
                Symbols=dict(Symbology="ticker", IDs=bbg_id),
                Fields=['OPT_CHAIN']
            )
            logger.info(f"Got {len(opt_chain)} options for {bbg_id}")
            options_mapping[dataflow_id] = [" ".join(ticker.split()) for ticker in list(opt_chain['SECURITY_DESCRIPTION'])]
            time.sleep(5)
        return options_mapping

    def resolve_cme_futures(self, symbols: list[str]) -> dict[str, str]:
        pass

    def resolve_cme_futures_option(self, symbols: list[str]) -> dict[str, str]:
        pass


bbg_symbol_resolver = BBGSymbolResolver()