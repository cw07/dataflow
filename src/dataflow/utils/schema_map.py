from datacore.models.mktdata.realtime import MarketByPrice1
from datacore.models.mktdata.historical import OHLCV1D

SCHEMA_MAP = {
    "mbp-1": MarketByPrice1,
    "ohlcv-1d": OHLCV1D
}