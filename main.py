import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI

from src.config import load_config
from src.fetcher import StockDataFetcher
from src.handler import DataHandler
from src.store import MetaStore, StockDataStore


# Format python logger to Google CloudRun-compatible log format
class CloudRunLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "timestamp": datetime.now(ZoneInfo("America/Los_Angeles")).isoformat(),
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


app = FastAPI(title="Copernicus Quant Data Service", version="0.1.0")

logger = configure_logging()
logger.info("Starting application")
try:
    config = load_config()
    data_store = StockDataStore(config=config.store)
    meta_store = MetaStore(config=config.meta)
except Exception:
    logger.exception("Failed to start the service")
    sys.exit(1)
fetcher = StockDataFetcher(
    config=config.fetcher, stock_list_df=data_store.stock_list_df
)
data_handler = DataHandler(
    fetcher=fetcher,
    data_store=data_store,
    meta_store=meta_store,
)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


# @app.get("/get-stocks")
# async def get_stocks():
#     data_handler.run_get_stock_data()
#     return {"status": "ok"}
