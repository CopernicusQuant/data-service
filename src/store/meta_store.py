from datetime import datetime
from enum import StrEnum
from zoneinfo import ZoneInfo

from google.cloud import firestore
from pydantic import BaseModel, Field

from src.config.config import MetaConfig


def _server_time_now() -> datetime:
    return datetime.now(ZoneInfo("America/Los_Angeles"))


JOB_DOC = "job"
DATA_DOC = "data"

# ========== Data-fetching job schemas ==========


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class JobType(StrEnum):
    GET_STOCKS = "get_stocks"
    UPDATE_STOCKS = "update_stocks"
    GET_INDEX = "get_index"
    UPDATE_INDEX = "update_index"


class JobResult(BaseModel):
    requested_num: int = 0
    failed_num: int = 0
    records_fetched: int = 0
    failed_codes: list[str] = []
    total_time: int = 0  # this is suppose to be in seconds


class JobRecord(BaseModel):
    id: str
    type: JobType
    status: JobStatus = JobStatus.QUEUED

    requested_at: datetime = Field(default_factory=_server_time_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: JobResult | None


# ========== DB record schemas ==========
class DataRecord(BaseModel):
    stock_count: int = 0
    index_count: int = 0
    updated_at: datetime = Field(default_factory=_server_time_now)


class ServiceMetadata(BaseModel):
    job: JobRecord | None
    data: DataRecord


class MetaStore:
    def __init__(self, config: MetaConfig):
        self.db = firestore.Client()
        self.data_ref = self.db.collection(config.meta_collection_name).document(
            DATA_DOC
        )
        self.job_ref = self.db.collection(config.meta_collection_name).document(JOB_DOC)

    # def ensure_metadata(self) -> ServiceMetadata:
    #     job_snapshot = self.job_ref.get()
    #     if job_snapshot.exists:
    #         job = JobRecord.model_validate(job_snapshot.to_dict())
    #     else:
