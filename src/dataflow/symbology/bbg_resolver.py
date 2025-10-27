import logging

from tradetools.bdate import BDate

from dataflow.config.settings import settings
from dataflow.symbology.base import BaseSymbolResolver

logger = logging.getLogger(__name__)

class BBGSymbolResolve(BaseSymbolResolver):
    def resolve(self, input_symbols: list[str], market: str, resolve_type: str) -> dict[str, str]:
        if market == "LME" and resolve_type == "fut":
            mapping = self.resolve_lme_futures(input_symbols)
        elif market == "LME" and resolve_type == "futopt":
            mapping = self.resolve_lme_futures_option(input_symbols)
        else:
            mapping = {}
        return mapping

    def resolve_lme_futures(self, symbols: list[str]) -> dict[str, str]:
        pass

    def resolve_lme_futures_option(self, symbols: list[str]) -> dict[str, str]:
        pass

    def resolve_cme_futures(self, symbols: list[str]) -> dict[str, str]:
        pass

    def resolve_cme_futures_option(self, symbols: list[str]) -> dict[str, str]:
        pass

bbg_symbol_resolver = BBGSymbolResolve()