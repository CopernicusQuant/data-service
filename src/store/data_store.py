from typing import List, Optional
from src.config import StoreConfig
import pyarrow.fs as fs
import pyarrow.csv as csv
import pandas as pd

INDEX_DIR = "index"
STOCK_DIR = "stock"
META_DIR = "meta"

class StockDataStore:
    def __init__(self, config: StoreConfig):
        # initialize Cloudflare r2 filesystem
        endpoint = config.bucket_endpoint.replace(
            "https://", "").replace("http://", "")
        self.fs = fs.S3FileSystem(
            access_key=config.access_key_id,
            secret_key=config.secret_access_key,
            region="auto",
            scheme="https",
            endpoint_override=endpoint
        )
        # load stock list to the system
        self.bucket_name = config.bucket_name
        try:
            self.stock_list_df = self.load_stock_list()
        except Exception as exc:
            raise RuntimeError(
                f"Required stock list file could not be loaded"
            ) from exc

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
            raise ValueError(
                f"stock list file is empty: ${stock_list_path}"
            )
        return stock_list.to_pandas().set_index("ts_code", drop=False)

    def list_stock_codes(self, num: Optional[int] = None) -> List[str]:
        """
        Get stock codes from the memory stock list data

        Args:
            num: number of stock codes to retrieve
        """
        stocks = self.stock_list_df["ts_code"].dropna().astype(str).tolist()
        if num != None and num > 0:
            return stocks[:num]
        return stocks
