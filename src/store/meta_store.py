import logging
import uuid
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
    time_spent: float = 0.0  # in seconds


class JobRecord(BaseModel):
    id: str
    type: JobType
    status: JobStatus = JobStatus.RUNNING

    started_at: datetime = Field(default_factory=_server_time_now)
    ended_at: datetime | None = None
    result: JobResult | None = None


# ========== DB record schemas ==========
class DataRecord(BaseModel):
    stock_count: int = 0
    index_count: int = 0
    total_stock_rows: int = 0
    total_index_rows: int = 0
    updated_at: datetime = Field(default_factory=_server_time_now)


class ServiceMetadata(BaseModel):
    job: JobRecord | None
    data: DataRecord


class MetaStore:
    def __init__(self, config: MetaConfig):

        self.db = firestore.Client()
        self.job_record_ref = self.db.collection(config.meta_collection_name).document(
            JOB_DOC
        )
        self.data_record_ref = self.db.collection(config.meta_collection_name).document(
            DATA_DOC
        )
        self._ensure_metadata()

    def get_metadata(self) -> ServiceMetadata:
        """
        Get metadata of the current database

        Returns:
            ServiceMetadata
        """
        return self._ensure_metadata()

    def create_job(self, job_type: JobType) -> JobRecord | None:
        """
        Create a new job when there's no running job. Otherwise just early return

        Returns:
            JobRecord if no current job running, otherwise None
        """
        new_job = JobRecord(
            id=uuid.uuid4().hex,
            type=job_type,
        )
        transaction = self.db.transaction()

        @firestore.transactional
        def create(transaction) -> JobRecord | None:
            job_snapshot = self.job_record_ref.get(transaction=transaction)
            job_data = (
                JobRecord.model_validate(job_snapshot.to_dict())
                if job_snapshot.exists
                else None
            )
            if job_data and job_data.status == JobStatus.RUNNING:
                return None
            transaction.set(self.job_record_ref, new_job.model_dump(mode="python"))
            return new_job

        return create(transaction=transaction)

    def complete_job(
        self,
        job_type: JobType,
        success: bool,
        job_result: JobResult,
        total_rows: int,
        total_records: int,
    ):
        """
        Update job result info to the firestore database
        """

        transaction = self.db.transaction()

        @firestore.transactional
        def update(transaction):
            job_snapshot = self.job_record_ref.get(transaction=transaction)
            record_snapshot = self.data_record_ref.get(transaction=transaction)

            job_data = JobRecord.model_validate(job_snapshot.to_dict())
            job_data.status = JobStatus.SUCCEEDED if success else JobStatus.FAILED
            job_data.result = job_result
            job_data.ended_at = _server_time_now()

            record_data = DataRecord.model_validate(record_snapshot.to_dict())
            if job_type in [JobType.UPDATE_STOCKS, JobType.GET_STOCKS]:
                record_data.stock_count = total_records
                record_data.total_stock_rows = total_rows
            elif job_type in [JobType.GET_INDEX, JobType.UPDATE_INDEX]:
                record_data.index_count = total_records
                record_data.total_index_rows = total_rows
            record_data.updated_at = _server_time_now()

            transaction.set(self.job_record_ref, job_data.model_dump(mode="python"))
            transaction.set(self.data_record_ref, record_data.model_dump(mode="python"))

        update(transaction=transaction)

    def _ensure_metadata(self) -> ServiceMetadata:
        """
        Retrieve database metadata from Firebase. For the data metadata, if the record does not exist,
        system will initialize an default record. For the job metadata, if it does not exists, just ignore the data

        Returns:
            ServiceMetadata
        """
        job_snapshot = self.job_record_ref.get()
        data_snapshot = self.data_record_ref.get()

        job = (
            JobRecord.model_validate(job_snapshot.to_dict())
            if job_snapshot.exists
            else None
        )

        if job is not None and job.status == JobStatus.RUNNING:
            job = self._set_job_interrupted()

        if data_snapshot.exists:
            data = DataRecord.model_validate(data_snapshot.to_dict())
        else:
            data = DataRecord()
            self.data_record_ref.set(
                data.model_dump(mode="python")
            )  # use python to make datetime compatible with db date time format
        return ServiceMetadata(job=job, data=data)

    def _set_job_interrupted(self) -> JobRecord:
        transaction = self.db.transaction()

        @firestore.transactional
        def interrupt(transaction) -> JobRecord:
            job_snapshot = self.job_record_ref.get(transaction=transaction)

            job_data = JobRecord.model_validate(job_snapshot.to_dict())
            job_data.status = JobStatus.INTERRUPTED
            job_data.ended_at = _server_time_now()

            transaction.set(self.job_record_ref, job_data.model_dump(mode="python"))
            return job_data

        return interrupt(transaction=transaction)
