import logging
import time

from pydantic import BaseModel

from src.fetcher import StockDataFetcher
from src.store import JobResult, MetaStore, StockDataStore
from src.store.meta_store import JobType

logger = logging.getLogger(__name__)


class GetDataResult(BaseModel):
    requested_num: int = 0
    failed_num: int = 0
    records_fetched: int = 0
    failed_codes: list[str] = []
    total_rows: int = 0
    total_records: int = 0
    time_spent: float = 0.0  # in seconds


class DataHandler:
    def __init__(
        self,
        fetcher: StockDataFetcher,
        data_store: StockDataStore,
        meta_store: MetaStore,
    ):
        self.fetcher = fetcher
        self.data_store = data_store
        self.meta_store = meta_store

    def _get_stock_data(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> GetDataResult:
        """
        Fetch stock data from Tushare and save fetched data to Cloudflare R2 storage

        Args:
            start_date: YYYYMMDD, exp 20250610, should not early than 20050101
            end_date: YYMMDD, exp 20250611, should not early than start_date

        Returns:
            GetDataResult
        """
        tickers = self.data_store.list_stock_tickers()
        result = GetDataResult()

        kwargs: dict[str, str] = {}
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date

        if kwargs["start_date"] and kwargs["end_date"] < kwargs["start_date"]:
            kwargs["end_date"] = kwargs["start_date"]

        start_time = time.perf_counter()

        for ticker in tickers:
            try:
                result.requested_num += 1
                stock_df = self.fetcher.get_us_daily(ticker, **kwargs)
                if stock_df is None:
                    raise RuntimeError(f"Fetcher returned no data: {ticker}")
                result.records_fetched += len(stock_df)
                _, num_rows = self.data_store.save_stock(stock_df=stock_df)
                result.total_rows += num_rows
                result.total_records += 1

            except Exception as exc:
                logger.warning(f"failed to fetch data for {ticker}: {exc!s}")
                result.failed_codes.append(ticker)
                result.failed_num += 1

        end_time = time.perf_counter()
        result.time_spent = end_time - start_time
        return result

    def run_get_stock_data(self):
        """
        Run get stock data job, to get the complete stock data from 20050101 till the current day
        """

        new_job = self.meta_store.create_job(job_type=JobType.GET_STOCKS)
        if not new_job:
            logger.error("failed to create new job")
            return
        result = self._get_stock_data()
        job_result = JobResult(
            requested_num=result.requested_num,
            failed_num=result.failed_num,
            records_fetched=result.records_fetched,
            failed_codes=result.failed_codes,
            time_spent=result.time_spent,
        )
        failed = result.failed_num == result.requested_num
        self.meta_store.complete_job(
            job_type=JobType.GET_STOCKS,
            success=(not failed),
            job_result=job_result,
            total_rows=result.total_rows,
            total_records=result.total_records,
        )
        logger.info(f"{JobType.GET_STOCKS} completed.")
