from datacore.models.mktdata.realtime import MarketByPrice1
from datacore.models.mktdata.historical import OHLCV1D
from datacore.models.mktdata.schema import MktDataSchema

SCHEMA_MAP = {
    MktDataSchema.MBP_1: MarketByPrice1,
    MktDataSchema.OHLCV_1D: OHLCV1D
}