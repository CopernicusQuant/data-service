import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv, fs

from src.config import StoreConfig

INDEX_DIR = "index"
STOCK_DIR = "stock"
META_DIR = "meta"


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

    def save_stock(self, stock_df: pd.DataFrame) -> tuple[str, int]:
        """
        Merge daily data into one Parquet object for a single ticker
        and upload it to Cloudflare R2

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

        # Merge existing R2 data with the latest fetched data
        data = stock_df.copy()
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

    def _stock_path(self, ts_code: str) -> str:
        return f"{self.bucket_name}/{STOCK_DIR}/{ts_code}.parquet"
