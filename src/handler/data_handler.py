import logging
import time

from pydantic import BaseModel

from src.fetcher import StockDataFetcher
from src.store import JobResult, MetaStore, StockDataStore
from src.store.meta_store import JobRecord, JobType

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

    def run_get_stock_data(self, job: JobRecord):
        """
        Run get stock data job, to get the complete stock data from 20050101 till the current day
        """

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
            job_type=job.type,
            success=(not failed),
            job_result=job_result,
            total_rows=result.total_rows,
            total_records=result.total_records,
        )
        logger.info(f"{JobType.GET_STOCKS}: {job.id} completed.")

    def run_get_index_data(self, job: JobRecord):
        result = self._get_index_data()
        job_result = JobResult(
            requested_num=result.requested_num,
            failed_num=result.failed_num,
            records_fetched=result.records_fetched,
            failed_codes=result.failed_codes,
            time_spent=result.time_spent,
        )
        failed = result.failed_num == result.requested_num
        self.meta_store.complete_job(
            job_type=job.type,
            success=(not failed),
            job_result=job_result,
            total_rows=result.total_rows,
            total_records=result.total_records,
        )
        logger.info(f"{JobType.GET_INDEX}: {job.id} completed")

    def _get_stock_data(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> GetDataResult:
        """
        Fetch stock data from Tushare and save fetched data to Cloudflare R2 storage

        Args:
            start_date: YYYYMMDD, exp 20250610, should not early than 20060101
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

        if start_date and end_date and kwargs["end_date"] < kwargs["start_date"]:
            kwargs["end_date"] = start_date

        start_time = time.perf_counter()

        for i, ticker in enumerate(tickers):
            try:
                result.requested_num += 1
                stock_df = self.fetcher.get_us_daily(ticker, **kwargs)
                if stock_df is None:
                    raise RuntimeError(f"Fetcher returned no data: {ticker}")
                result.records_fetched += len(stock_df)
                _, num_rows = self.data_store.save_stock(
                    stock_df=stock_df,
                    refresh=True,  # we should replace the existing data in this mode
                )
                result.total_rows += num_rows
                result.total_records += 1

            except Exception as exc:  # noqa: BLE001
                logger.warning(f"failed to fetch data for {ticker}: {exc!s}")
                result.failed_codes.append(ticker)
                result.failed_num += 1

            if (i + 1) % 10 == 0:
                logger.info(f"{(i + 1)}/{len(tickers)} task executed")
        end_time = time.perf_counter()
        result.time_spent = end_time - start_time
        return result

    def _get_index_data(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> GetDataResult:
        """
        Fetch index data from Tushare and save fetched data to Cloudflare R2 storage

        Args:
            start_date: YYYYMMDD, exp 20250610, should not early than 20060101
            end_date: YYMMDD, exp 20250611, should not early than start_date

        Returns:
            GetDataResult
        """
        indices = self.data_store.index_list
        result = GetDataResult()

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
                result.requested_num += 1
                index_df = self.fetcher.get_us_index(index, **kwargs)
                if index_df is None:
                    raise RuntimeError(f"Fetcher returned no data: {index}")
                result.records_fetched += len(index_df)
                _, num_rows = self.data_store.save_index(
                    index_df=index_df, refresh=True
                )
                result.total_rows += num_rows
                result.total_records += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"failed to fetch data for {index}: {exc!s}")
                result.failed_codes.append(index)
                result.failed_num += 1
        end_time = time.perf_counter()
        result.time_spent = end_time - start_time
        return result
