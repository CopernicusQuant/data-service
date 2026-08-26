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
        tickers = self.data_store.list_stock_tickers()
        result = GetDataResult()

        args: dict[str, str] = {}
        if start_date:
            args["start_date"] = start_date
        if end_date:
            args["end_date"] = end_date

        start_time = time.perf_counter()

        for ticker in tickers:
            try:
                result.requested_num += 1
                stock_df = self.fetcher.get_us_daily(ticker, *args)
                if stock_df is None:
                    raise RuntimeError(f"Fetcher returned no data: {ticker}")
                result.records_fetched += len(stock_df)
                _, num_rows = self.data_store.save_stock(stock_df=stock_df)
                result.total_rows += num_rows
                result.total_records += 1
                logger.info(f"Saved data for {ticker}, total_rows: {num_rows}")

            except Exception as exc:
                logger.warning(f"failed to fetch data for {ticker}: {exc!s}")
                result.failed_codes.append(ticker)
                result.failed_num += 1

        end_time = time.perf_counter()
        result.time_spent = end_time - start_time
        return result

    def run_get_stock_data(self):
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
        self.meta_store.complete_job(
            job_type=JobType.GET_STOCKS,
            success=True,
            job_result=job_result,
            total_rows=result.total_rows,
            total_records=result.total_records,
        )
        logger.info("job completed")
