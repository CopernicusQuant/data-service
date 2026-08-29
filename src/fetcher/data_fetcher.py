import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts

from src.config import FetcherConfig

logger = logging.getLogger(__name__)


DEFAULT_DATE_START = (
    "20200101"  # we only fetch 6 years data for learning and experimenting
)


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
        self, ts_code: str, start_date: str = DEFAULT_DATE_START, end_date: str = ""
    ) -> pd.DataFrame | None:
        """Fetch daily US stock data from Tushare.
        we only get the most recent 20 years stock data

        Returns:
            DataFrame of us stock data
        """
        end_date = (
            datetime.now(tz=ZoneInfo("America/New_York")).strftime("%Y%m%d")
            if not end_date
            else end_date
        )  # end date defaults to "today" if not provided

        # if no ts_code provided, only fetch one day data
        if not ts_code and start_date != end_date:
            raise ValueError(
                "tushare us_daily(): either provide a ts_code or set the start/end date the same day"
            )
        # there can be ts_code reuse issue, so we need to refer to the stock's list date
        if ts_code != "":
            if ts_code not in self.stock_list_df.index:
                raise KeyError(
                    f"{ts_code} not in the stock list, check either the stock list or ts_code"
                )
            else:
                stock_list_date = str(self.stock_list_df.loc[ts_code, "list_date"])
                start_date = max(start_date, stock_list_date)

        # get us daily data
        df_us_daily = self._get_us_daily_with_retry(
            ts_code=ts_code, start_date=start_date, end_date=end_date
        )
        if df_us_daily is None:
            return None
        # get us accumulated adjust factor data
        df_us_adj = self._get_us_adj_factor_with_retry(
            ts_code=ts_code, start_date=start_date, end_date=end_date
        )
        if df_us_adj is None:
            return None
        # tushare missing some us_adj data, so we need to fill them in the backward manner
        df = pd.merge(
            df_us_daily,
            df_us_adj,
            on=["ts_code", "trade_date"],
            how="left",
            suffixes=("", "_adj"),
        )
        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
        df["cum_adjfactor"] = df.groupby("ts_code")["cum_adjfactor"].ffill().bfill()
        if df["cum_adjfactor"].isna().any():
            raise ValueError(f"found missing adj factors in stock {ts_code}")
        # compute adjusted prices
        for col in ["high", "low", "open", "close", "vwap"]:
            df[f"adj_{col}"] = df[col] * df["cum_adjfactor"]
        # compute roe and total share
        df["roe"] = df["pb"] / df["pe"]
        df["total_share"] = df["total_mv"] / df["close"]
        df = df.rename(columns={"turnover_ratio": "turnover"})
        return df

    def get_us_index(
        self, ts_code: str, start_date: str = DEFAULT_DATE_START, end_date: str = ""
    ) -> pd.DataFrame | None:
        """
        Get US market-specific index data
        The index data were fetched through pagination, so if any page failed, we consider
        the index fetching failed to ensure the data integrity

        Returns:
            pd.DataFrame if fetch succeeded, otherwise None
        """
        if not ts_code:
            raise ValueError("ts_code is required for us index data")
        all_dfs = []
        limit = 4000
        offset = 0
        while True:
            success, df = self._get_us_index_with_retry(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset,
            )
            # if any page failed, we break the fetching and return None, to ensure
            # the data integrity
            if not success:
                return None
            if df is None:
                break
            all_dfs.append(df)
            if len(df) < limit:
                break
            offset += limit
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.sort_values("trade_date").reset_index(drop=True)
        return final_df

    def _get_us_daily_with_retry(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """
        Fetch us daily stock data from Tushare, with retry

        Returns:
            pd.DataFrame if fetched successfully, None otherwise
        """
        try_times = 0
        success = False
        exception = None
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
                        "close",
                        "open",
                        "high",
                        "low",
                        "vol",
                        "amount",
                        "vwap",
                        "turnover_ratio",
                        "total_mv",
                        "pe",
                        "pb",
                    ],
                )
            except Exception as e:  # noqa: BLE001
                exception = e
                try_times += 1
                logger.warning(
                    f"Fetch us_daily exception {ts_code}:{exception!s}, retry time {try_times}"
                )
                time.sleep(30)
                continue
            else:
                success = True
                break
        if not success:
            logger.error(f"Failed to fetch us_daily of {ts_code}: {exception!s}")
            return None
        if df.empty:
            logger.warning(f"Got empty data {ts_code}")
            return None
        return df

    def _get_us_adj_factor_with_retry(
        self, ts_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """
        Fetch us stock accumulated adjust factor from Tushare, with retry

        Returns:
            pd.DataFrame if fetched successfully, None otherwise
        """
        try_times = 0
        success = False
        exception = None
        while try_times < 3:
            try:
                # this api returns 8000 rows in maximum in one call
                # can retrieve full data through pagination in the future
                df = self.tushare_pro.us_adjfactor(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=[
                        "ts_code",
                        "trade_date",
                        "cum_adjfactor",
                    ],
                )
            except Exception as e:  # noqa: BLE001
                exception = e
                try_times += 1
                logger.warning(
                    f"Fetch us_adjfactor exception {ts_code}:{exception!s}, retry time {try_times}"
                )
                time.sleep(30)
                continue
            else:
                success = True
                break
        if not success:
            logger.error(f"Failed to fetch us_adjfactor of {ts_code}: {exception!s}")
            return None
        if df.empty:
            logger.warning(f"Got empty data {ts_code}")
            return None
        return df

    def _get_us_index_with_retry(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        limit: int | None = None,
        offset: int | None = None,
    ) -> tuple[bool, pd.DataFrame | None]:
        """
        Fetch US index data from Tushare, with retry. Different from the stock data fetching method,
        the index data needs to be fetched through pagination, so we should return the `success` value
        for the other function to check if every single page were successfully fetched

        Returns:
            success True if the fetching is success False other wise.
            pd.DataFrame if fetched successfully, otherwise None
        """
        try_times = 0
        exception = None
        success = False
        while try_times < 3:
            try:
                index_df = self.tushare_pro.index_global(
                    ts_code=ts_code,
                    offset=offset,
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date,
                    fields=[
                        "ts_code",
                        "trade_date",
                        "open",
                        "close",
                        "high",
                        "low",
                        "pre_close",
                        "change",
                        "pct_chg",
                        "swing",
                        "vol",
                    ],
                )
            except Exception as e:  # noqa: BLE001
                exception = e
                try_times += 1
                logger.warning(
                    f"Fetch index_global exception {ts_code}:{exception!s}, retry time {try_times}"
                )
                time.sleep(30)
                continue
            else:
                success = True
                break
        if not success:
            logger.error(f"Failed to fetch index_global of {ts_code}: {exception!s}")
            return success, None
        if index_df.empty:
            logger.warning(f"Got empty data {ts_code}, offset {offset}, limit {limit}")
            return success, None
        return success, index_df
