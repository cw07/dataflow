import datetime as dt
from typing import Optional
from zoneinfo import ZoneInfo
from pydantic import BaseModel, Field, field_validator, computed_field

from datacore.models.assets import AssetType

from dataflow.utils.common import CONTRACT_MONTH_CODE

class BaseFutures(BaseModel):
    dflow_id: str
    terms: int
    contract_size: int
    venue: str
    time_zone: str
    open_time_local: str
    close_time_local: str
    trading_days: list[int]
    contract_months: list[str]
    description: Optional[str] = None
    category: Optional[str] = None

    @computed_field
    @property
    def contract_month_code(self) -> list[str]:
        return [CONTRACT_MONTH_CODE[m] for m in self.contract_months]

    def is_trading_now(self) -> bool:
        """
        Returns True if the market is currently trading.
        Uses string comparison to detect overnight sessions (e.g. '18:00:00' > '17:00:00').
        """
        tz = ZoneInfo(self.time_zone)
        now_local = dt.datetime.now(tz)
        now_local_weekday = now_local.weekday()
        now_local_time_str = now_local.strftime("%H:%M:%S")

        is_overnight = self.open_time_local > self.close_time_local

        if not is_overnight:
            if now_local_weekday not in self.trading_days:
                return False
            return self.open_time_local <= now_local_time_str < self.close_time_local
        else:
            if self.open_time_local <= now_local_time_str:
                # Session started today (e.g. 18:00–23:59)
                session_start_weekday = now_local_weekday
            elif now_local_time_str < self.close_time_local:
                # Session started yesterday (e.g. 00:00–17:00)
                session_start_weekday = (now_local - dt.timedelta(days=1)).weekday()
            else:
                # Time is in the gap: close_time <= now < open_time → market closed
                return False

            return session_start_weekday in self.trading_days


class Futures(BaseModel):
    dflow_id: str
    venue: str
    parent: BaseFutures
    description: Optional[str] = None
    symbol: Optional[str] = None
    asset_type: AssetType = AssetType.FUT


class FuturesOptions(BaseModel):
    dflow_id: str
    venue: str
    parent: Futures
    description: Optional[str] = None
    symbol: Optional[str] = None
    asset_type: AssetType = AssetType.FUT_OPTION



if __name__ == "__main__":
    cme_cl = BaseFutures(dflow_id="CME.CL",
                         terms=12,
                         contract_size=1000,
                         venue="CME",
                         time_zone="America/New_York",
                         open_time_local="18:00:00",
                         close_time_local="17:00:00",
                         trading_days=[6,0,1,2,3,4],
                         contract_months=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                         )

    cme_cl_1 = Futures(dflow_id="CME.CL.1",
                       parent=cme_cl,
                       venue="CME",
                       description="CME CL 1"
                       )

    cme_cl_1_opt = FuturesOptions(dflow_id="12",
                                  parent=cme_cl_1,
                                  venue="CME",
                                  description="CME CL 1"
                                  )

    print(cme_cl_1_opt)


