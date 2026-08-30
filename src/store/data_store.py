from io import StringIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pyarrow import csv, fs

from src.config import StoreConfig

INDEX_DIR = "index"
STOCK_DIR = "stock"
META_DIR = "meta"

STOCK_LIST_FILENAME_PREFIX = "stock_list"

# we just hardcode these four index here
INDEX_LIST = [
    "RUT",  # Russell 2000 Index
    "SPX",  # S&P 500 Index
    "DJI",  # Dow Jones Industrial Average
    "IXIC",  # NASDAQ Composite Index
]


class StockDataStore:
    def __init__(self, config: StoreConfig):
        # initialize Cloudflare r2 filesystem
        endpoint = config.bucket_endpoint.replace("https://", "").replace("http://", "")
        self.fs = fs.S3FileSystem(
            access_key=config.access_key_id,
            secret_key=config.secret_access_key,
            region="auto",
            scheme="https",
            endpoint_override=endpoint,
        )
        # load stock list to the system
        self.bucket_name = config.bucket_name
        if config.runtime_env not in ["prod", "dev"]:
            raise ValueError("runtime_env variable should be 'prod' or 'dev'")
        self.runtime_env = config.runtime_env
        self.index_list = INDEX_LIST
        try:
            self.stock_list_df = self.load_stock_list()
        except Exception as load_exc:  # noqa BLE001
            try:
                self.stock_list_df = self._refresh_sp_500_list()
            except Exception as refresh_exc:
                raise RuntimeError(
                    f"failed to load stock list {load_exc!s}"
                    f"failed to refresh stock list {refresh_exc!s}"
                ) from refresh_exc

    def load_stock_list(self) -> pd.DataFrame:
        """
        load stock list from r2 storage. The stock list is a relatively static file
        will only be updated once or twice a year.

        Returns:
            pd.DataFrame if loaded successfully, otherwise raises
        """
        with self.fs.open_input_file(self._stock_list_path()) as source:
            stock_list = (
                csv.read_csv(source).to_pandas().set_index("ts_code", drop=False)
            )
        if len(stock_list) == 0:
            raise ValueError(f"stock list file is empty: ${self._stock_list_path()}")
        return stock_list

    def list_stock_tickers(self, num: int | None = None) -> list[str]:
        """
        Get stock tickers from the memory stock list data

        Args:
            num: number of stock tickers to retrieve
        """
        stocks = self.stock_list_df["ts_code"].dropna().astype(str).tolist()
        if num != None and num > 0:
            return stocks[:num]
        return stocks

    def save_stock(
        self, stock_df: pd.DataFrame, refresh: bool = False
    ) -> tuple[str, int]:
        """
        Merge daily data into one Parquet object for a single ticker
        and upload it to Cloudflare R2

        Args:
            stock_df: the fetched stock dataframe
            refresh: if `True`, replace the existing data

        Returns:
            The R2 object path written
            The number of record
        """
        required_columns = {"ts_code", "trade_date"}
        missing_columns = required_columns - set(stock_df.columns)
        if missing_columns:
            raise ValueError(
                f"Stock data is missing required columns: {missing_columns}"
            )
        if stock_df.empty:
            raise ValueError("Empty df received")

        # ensure there's only one ticker in the given df
        tickers = stock_df["ts_code"].dropna().unique()
        if len(tickers) != 1:
            raise ValueError(
                f"save_stock expects data exactly one ticker, received {len(tickers)} tickers"
            )
        ts_code = tickers[0]
        path = self._stock_path(ts_code)

        data = stock_df.copy()
        if refresh == False:
            # Merge existing R2 data with the latest fetched data
            file_info = self.fs.get_file_info(path)
            if file_info.type == fs.FileType.File:
                with self.fs.open_input_file(path) as source:
                    old_data = pq.read_table(source).to_pandas()
                data = pd.concat(
                    [old_data, data], ignore_index=True
                )  # old data first, new data second
        data = (
            data.drop_duplicates(subset="trade_date", keep="last")
            .sort_values("trade_date")
            .reset_index(drop=True)
        )

        # save data to r2 storage
        table = pa.Table.from_pandas(data, preserve_index=False)
        with self.fs.open_output_stream(path) as sink:
            pq.write_table(
                table,
                sink,
                compression="zstd",
                compression_level=3,
            )
        return path, len(data)

    def read_stock(
        self,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Read Stock DataFrom from the R2 Storage

        Returns:
            pd.DataFrame
        """
        path = self._stock_path(ts_code)
        file_info = self.fs.get_file_info(path)
        if file_info.type != fs.FileType.File:
            raise FileNotFoundError(f"Stock data does not exist: {path}")
        with self.fs.open_input_file(path) as source:
            table = pq.read_table(source)
        data = table.to_pandas()
        if start_date:
            data = data[data["trade_date"] >= start_date]
        if end_date:
            data = data[data["trade_date"] <= end_date]
        return data.sort_values("trade_date").reset_index(drop=True)

    def save_index(
        self, index_df: pd.DataFrame, refresh: bool = False
    ) -> tuple[str, int]:
        """
        Merge daily data into one Parquet object for a single index
        and upload it to Cloudflare R2

        Args:
            stock_df: the fetched index dataframe
            refresh: if `True`, replace the existing data

        Returns:
            The R2 object path written
            The number of record
        """
        required_columns = {"ts_code", "trade_date"}
        missing_columns = required_columns - set(index_df.columns)
        if missing_columns:
            raise ValueError(
                f"Stock data is missing required columns: {missing_columns}"
            )
        if index_df.empty:
            raise ValueError("Empty df received")
        # ensure there's only one index code presenting
        index_codes = index_df["ts_code"].dropna().unique()
        if len(index_codes) != 1:
            raise ValueError(
                f"save_index expects data has exactly one index, received {len(index_codes)} codes"
            )
        code = index_codes[0]
        path = self._index_path(code)

        data = index_df.copy()
        if refresh == False:
            # Merge existing R2 data with the latest fetched data
            file_info = self.fs.get_file_info(path)
            if file_info.type == fs.FileType.File:
                with self.fs.open_input_file(path) as source:
                    old_data = pq.read_table(source).to_pandas()
                data = pd.concat([old_data, data], ignore_index=True)
        data = (
            data.drop_duplicates(subset="trade_date", keep="last")
            .sort_values("trade_date")
            .reset_index(drop=True)
        )
        # save data to r2 storage
        table = pa.Table.from_pandas(data, preserve_index=False)
        with self.fs.open_output_stream(path) as sink:
            pq.write_table(table, sink, compression="zstd", compression_level=3)
        return path, len(data)

    def _stock_path(self, ts_code: str) -> str:
        return f"{self.bucket_name}/{STOCK_DIR}/{ts_code}.parquet"

    def _index_path(self, ts_code: str) -> str:
        return f"{self.bucket_name}/{INDEX_DIR}/{ts_code}.parquet"

    def _stock_list_path(self) -> str:
        return f"{self.bucket_name}/{META_DIR}/{STOCK_LIST_FILENAME_PREFIX}_{self.runtime_env}.csv"

    def _refresh_sp_500_list(self) -> pd.DataFrame:
        """
        Get S&P 500 stock list from wikipedia and rename columns

        Returns
            pd.DataFrame
        """
        sp_500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {"User-Agent": "data-service/1.0 (contact: email@example.com)"}
        response = requests.get(sp_500_url, headers=headers, timeout=5)
        response.raise_for_status()
        sp_500 = pd.read_html(StringIO(response.text))[0]
        # rename columns
        sp_500.columns = [
            "ts_code",
            "company_name",
            "sector",
            "sub_industry",
            "headquarters",
            "date_added",
            "cik",
            "founded",
        ]
        sp_500["date_added"] = pd.to_datetime(sp_500["date_added"]).dt.strftime(
            "%Y%m%d"
        )
        if self.runtime_env == "dev":
            sp_500 = sp_500[:50]
        table = pa.Table.from_pandas(sp_500, preserve_index=False)
        with self.fs.open_output_stream(self._stock_list_path()) as sink:
            csv.write_csv(table, sink)
        return sp_500.set_index("ts_code", drop=False)
