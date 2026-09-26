import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.config import load_config
from src.feature import FeatureCalculator
from src.fetcher import StockDataFetcher
from src.handler import DataHandler, DataWindow
from src.store import StockDataStore


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

app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"])

logger = configure_logging()
logger.info("Starting application")
try:
    config = load_config()
    data_store = StockDataStore(config=config.store)
except Exception:
    logger.exception("Failed to start the service")
    sys.exit(1)
fetcher = StockDataFetcher(
    config=config.fetcher, stock_list_df=data_store.stock_list_df
)
feature_calculator = FeatureCalculator()
data_handler = DataHandler(
    fetcher=fetcher,
    calculator=feature_calculator,
    data_store=data_store,
)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/stocklist")
def get_stock_list():
    stock_list = data_handler.get_stock_list()
    return {"data": stock_list}


@app.get("/stockinfo")
def get_stock_info(ticker: str):
    try:
        stock_info = data_handler.get_stock_info(ts_code=ticker)
        return {"data": stock_info}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/stock/{ts_code}")
def get_stock_and_features(
    ts_code: str, window: DataWindow = DataWindow.days_60, group="pricemomentum"
) -> dict:
    try:
        stock_df, feature_df = data_handler.get_stock_and_feature(
            ts_code, window, group
        )
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "data": {
            "ts_code": ts_code,
            "stock": json.loads(stock_df.to_json(orient="records")),
            "features": json.loads(feature_df.to_json(orient="records")),
        }
    }


class JobAcceptedResponse(BaseModel):
    job_type: str


@app.get("/jobs/stocks", status_code=202, response_model=JobAcceptedResponse)
async def create_stock_job(background_tasks: BackgroundTasks):
    background_tasks.add_task(data_handler.run_update_stocks)
    return JobAcceptedResponse(job_type="update stocks")


@app.get("/jobs/indices", status_code=202, response_model=JobAcceptedResponse)
async def create_index_job(background_task: BackgroundTasks):
    background_task.add_task(data_handler.run_update_indices)
    return JobAcceptedResponse(job_type="update index")


@app.get("/jobs/features", status_code=202, response_model=JobAcceptedResponse)
async def create_features_job(background_task: BackgroundTasks):
    background_task.add_task(data_handler.run_update_features)
    return JobAcceptedResponse(job_type="update features")
