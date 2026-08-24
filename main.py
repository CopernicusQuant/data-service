from src.fetcher.data_fetcher import StockDataFetcher
from src.store.data_store import StockDataStore
import sys
import json
from datetime import datetime, timezone
import logging
from src.config import load_config

# Format python logger to Google CloudRun-compatible log format
class CloudRunLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)

def configure_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CloudRunLogFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel("INFO")
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    return logging.getLogger(__name__)


def main():
    logger = configure_logging()
    logger.info("Starting application")
    try:
        config = load_config()
        store = StockDataStore(config=config.store)
    except Exception:
        logger.exception("Failed to start the service")
        sys.exit(1)

    fetcher = StockDataFetcher(
        config=config.fetcher, stock_list_df=store.stock_list_df)

    # test_data = store.read_stock("AAPL", start_date="20260813", end_date="20260814")
    # print(test_data)

    # for ticker in ["AAPL", "AVGO", "SPCX"]:
    #     try:
    #         sample_data = fetcher.get_us_daily(ticker, "20260810", "20260819")
    #     except Exception as exc:
    #         logger.warning(f"failed to get stock {ticker}: {exc}")
    #         continue
    #     if sample_data is None:
    #         logger.warning(f"failed to get stock {ticker}")
    #         continue
    #     store.save_stock(sample_data)
    #     logger.info(f"successfully uploaded {ticker}")

if __name__ == "__main__":
    main()
