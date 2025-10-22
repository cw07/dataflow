import logging
import databento as db

from tradetools.bdate import BDate

from dataflow.config.settings import settings
from dataflow.symbology.base import BaseSymbolResolver


logger = logging.getLogger(__name__)


class OnyxSymbolResolve(BaseSymbolResolver):

    def resolve(self, input_symbols: list):
        pass