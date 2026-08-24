import logging
import tushare as ts

logger = logging.getLogger(__name__)

class StockDataFetcher:
    """Fetch US stock, index, calendar, and metadata data."""

    # stock list cache
    # df_stock_list:pd.DataFrame | None = None

    def __init__(self, config: dict, load_stock_list=False):
        tushare_token = config["tushare_token"]
        ts.set_token(tushare_token)
        self.tushare_pro = ts.pro_api()
        logger.info("StockDataFetcher initialized")
