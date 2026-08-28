import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv, fs

from src.config import StoreConfig

INDEX_DIR = "index"
STOCK_DIR = "stock"
META_DIR = "meta"

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
        try:
            self.stock_list_df = self.load_stock_list()
        except Exception as exc:
            raise RuntimeError("Required stock list file could not be loaded") from exc
        self.index_list = INDEX_LIST

    def load_stock_list(self) -> pd.DataFrame:
        """
        load stock list from r2 storage. The stock list is a relatively static file
        will only be updated once or twice a year.

        Returns:
            pd.DataFrame if loaded successfully, otherwise raises
        """
        stock_list_path = f"{self.bucket_name}/{META_DIR}/stock_list_small.csv"
        with self.fs.open_input_file(stock_list_path) as source:
            stock_list = csv.read_csv(source)
        if stock_list.num_rows == 0:
            raise ValueError(f"stock list file is empty: ${stock_list_path}")
        return stock_list.to_pandas().set_index("ts_code", drop=False)

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
