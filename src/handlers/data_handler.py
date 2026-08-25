from src.fetcher import StockDataFetcher
from src.store import StockDataStore


class DataHandler:
    def __init__(self, fetcher: StockDataFetcher, store: StockDataStore):
        self.fetcher = fetcher
        self.store = store
