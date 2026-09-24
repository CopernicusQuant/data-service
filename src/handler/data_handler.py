import logging
import time
from enum import StrEnum

import pandas as pd

from src.feature import FeatureCalculator
from src.fetcher import StockDataFetcher
from src.store import StockDataStore

logger = logging.getLogger(__name__)


class DataWindow(StrEnum):
    days_60 = "60days"
    days_180 = "180days"
    year_1 = "1year"
    years_3 = "3years"
    all = "all"


class DataHandler:
    def __init__(
        self,
        fetcher: StockDataFetcher,
        calculator: FeatureCalculator,
        data_store: StockDataStore,
    ):
        self.fetcher = fetcher
        self.data_store = data_store
        self.calculator = calculator

    # ======== Job Runners ========

    def run_update_stocks(self):
        """
        Run get stock data job, to get the complete stock data from 20050101 till the current day
        """
        time_spent = self._update_stocks()
        logger.info(f"stock data updated, time spent: {time_spent}s")

    def run_update_indices(self):
        time_spent = self._update_indices()
        logger.info(f"index data updated, time spent: {time_spent}s")

    def run_update_features(self):
        time_spent = self._update_features()
        logger.info(f"feature data updated, time spent: {time_spent}s")

    def _update_features(self) -> float:
        start_time = time.perf_counter()
        try:
            all_stocks = self.data_store.load_all_stocks()
            stocks_info = self.data_store.stock_list_df
            all_features = self.calculator.compute_all_stock_features(
                combined_stock_df=all_stocks, combined_info_df=stocks_info
            )
            self.data_store.save_features(all_features)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to update features: {exc!s}")
        self.data_store.clear_feature_cache()  # clear features lru cache
        end_time = time.perf_counter()
        time_spent = end_time - start_time
        return time_spent

    def _update_stocks(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> float:
        """
        Fetch stock data from Tushare and save fetched data to Cloudflare R2 storage

        Args:
            start_date: YYYYMMDD, exp 20250610, should not early than 20060101
            end_date: YYMMDD, exp 20250611, should not early than start_date

        Returns:
            UpdateDataResult
        """
        tickers = self.data_store.list_stock_tickers()
        kwargs: dict[str, str] = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        if start_date and end_date and kwargs["end_date"] < kwargs["start_date"]:
            kwargs["end_date"] = start_date

        start_time = time.perf_counter()

        for i, ticker in enumerate(tickers):
            try:
                stock_df = self.fetcher.get_us_daily(ticker, **kwargs)
                if stock_df is None:
                    raise RuntimeError(f"Fetcher returned no data: {ticker}")
                self.data_store.save_stock(
                    stock_df=stock_df,
                    refresh=True,  # we should replace the existing data in this mode
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"failed to fetch data for {ticker}: {exc!s}")

            if (i + 1) % 10 == 0:
                logger.info(f"{(i + 1)}/{len(tickers)} task executed")
        self.data_store.clear_stock_cache()  # clear stock lru cache
        end_time = time.perf_counter()
        time_spent = end_time - start_time
        return time_spent

    def _update_indices(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> float:
        """
        Fetch index data from Tushare and save fetched data to Cloudflare R2 storage

        Args:
            start_date: YYYYMMDD, exp 20250610, should not early than 20060101
            end_date: YYMMDD, exp 20250611, should not early than start_date

        Returns:
            UpdateDataResult
        """
        indices = self.data_store.index_list

        kwargs: dict[str, str] = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        if start_date and end_date and end_date < start_date:
            kwargs["end_date"] = start_date

        start_time = time.perf_counter()
        for index in indices:
            try:
                index_df = self.fetcher.get_us_index(index, **kwargs)
                if index_df is None:
                    raise RuntimeError(f"Fetcher returned no data: {index}")
                self.data_store.save_index(index_df=index_df, refresh=True)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"failed to fetch data for {index}: {exc!s}")
        end_time = time.perf_counter()
        time_spent = end_time - start_time
        return time_spent

    # ======== Stock data retrievers ========
    def get_stock_list(self) -> dict[str]:
        """
        Get the full list of stock tickers and company names
        format: [{ts_code: company_name}, {ts_code: company_name}, ...]
        """
        stock_list = self.data_store.stock_list_df[
            ["company_name", "sector", "sub_industry"]
        ].to_dict(orient="index")
        return stock_list

    def get_stock_info(self, ts_code: str) -> dict[str]:
        """
        Get given ts_code's stock info
        """
        ts_code = ts_code.upper()
        if ts_code not in self.data_store.stock_list_df.index:
            raise KeyError("Invalid stock ticker")
        stock_info = self.data_store.stock_list_df.loc[ts_code][
            [
                "ts_code",
                "company_name",
                "sector",
                "sub_industry",
                "headquarters",
                "founded",
            ]
        ].to_dict()
        return stock_info

    def get_stock_and_feature(
        self, ts_code: str, window: DataWindow = DataWindow.days_60
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        get a single stock's price data and feature data within a given period
        (
            NOTE: this part needs further optimization to remove the unnecessary columns
            to reduce the response size (and time)
        )
        Args:
            ts_code: str, the stock's ticker
            window: DataWindow, the date range
        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: [stock_df, feature_df]
        """
        ts_code = ts_code.upper()
        if ts_code not in self.data_store.stock_list_df.index:
            raise KeyError("Invalid stock ticker")
        stock_df = self.slice_df(
            self.data_store.read_stock(ts_code=ts_code), window=window
        )
        feature_df = self.slice_df(
            self.data_store.read_feature(ts_code=ts_code), window=window
        )
        return stock_df, feature_df

    def slice_df(self, df: pd.DataFrame, window: DataWindow) -> pd.DataFrame:
        """
        slice dataframe based on the given date window
        """
        approx_days_year = 252  # approximate trading days a year
        match window:
            case DataWindow.days_60:
                return df.tail(60)
            case DataWindow.days_180:
                return df.tail(180)
            case DataWindow.year_1:
                return df.tail(approx_days_year)
            case DataWindow.years_3:
                return df.tail(approx_days_year * 3)
            case DataWindow.all:
                return df
            case _:
                raise ValueError(f"Unsupported data window {window}")
