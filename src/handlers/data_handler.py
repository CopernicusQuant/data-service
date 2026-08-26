from src.fetcher import StockDataFetcher
from src.store import MetaStore, StockDataStore


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
