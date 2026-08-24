from typing import Optional
from src.config import FetcherConfig
import logging
import tushare as ts
import pandas as pd
import time
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class StockDataFetcher:
    """Fetch US stock, index, calendar, and metadata data."""

    # stock list cache
    # df_stock_list:pd.DataFrame | None = None

    def __init__(self, config: FetcherConfig, stock_list_df: pd.DataFrame):
        tushare_token = config.tushare_token
        ts.set_token(tushare_token)
        self.tushare_pro = ts.pro_api()
        self.stock_list_df = stock_list_df

    def get_us_daily(
        self, ts_code: str, start_date: str = "20050101", end_date: str = ""
    ) -> Optional[pd.DataFrame]:
        """Fetch daily US stock data from Tushare.

        Returns:
            DataFrame of us stock data
        """
        end_date = (
            datetime.now(tz=ZoneInfo("America/New_York")).strftime("%Y%m%d")
            if not end_date
            else end_date
        ) # end date defaults to "today" if not provided

        # if no ts_code provided, only fetch one day data
        if not ts_code and start_date != end_date:
            raise ValueError(
                "tushare us_daily(): either provide a ts_code or set the start/end date the same day"
            )
        if (int(end_date) - int(start_date)) // 10000 > 23:
            raise ValueError(
                "tushare us_daily(): time span should not more than 23 years"
            )
        try_times = 0
        success = False
        exception = None

        # there can be ts_code reuse issue, so we need to refer to the stock's list date
        if ts_code != "":
            if ts_code not in self.stock_list_df.index:
                raise KeyError(
                    f"{ts_code} not in the stock list, check either the stock list or ts_code"
                )
            else:
                stock_list_date = str(self.stock_list_df.loc[ts_code, "list_date"])
                start_date = max(start_date, stock_list_date)

        while try_times < 3:
            try:
                # this api returns 8000 rows in maximum in one call
                # can retrieve full data through pagination in the future
                df = self.tushare_pro.us_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=[
                        "ts_code",
                        "trade_date",
                        "open",
                        "close",
                        "high",
                        "low",
                        "turnover_ratio",
                        "pe",
                        "pb",
                        "total_mv",
                        "amount",
                    ],
                )
            except Exception as e:
                exception = e
                try_times += 1
                logger.warning(f"Fetch us_daily exception {ts_code}:{str(exception)}, retry time ${try_times}")
                time.sleep(30)
                continue
            else:
                success = True
                break
        if not success:
            logger.error(f"Failed to fetch us_daily of {ts_code}: {str(exception)}")
            return None
        if df.empty:
            logger.warning(f"Got empty data {ts_code}")
            return None
        df["roe"] = df["pb"] / df["pe"]
        df["total_share"] = df["total_mv"] / df["close"]
        df = df.rename(columns={"turnover_ratio": "turnover"})
        df = df.sort_values("trade_date").reset_index(drop=True)
        return df
