from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, field_validator, model_validator


class SyncRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    order_id_gt: Optional[int] = None
    limit: Optional[int] = None

    @model_validator(mode="after")
    def validate_dates(self) -> "SyncRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")
        return self

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value <= 0:
            raise ValueError("limit must be positive")
        return value

    @field_validator("order_id_gt")
    @classmethod
    def validate_order_floor(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 0:
            raise ValueError("order_id_gt must be non-negative")
        return value


class SyncSummary(BaseModel):
    orders_fetched: int
    sales_upserted: int
    payments_upserted: int
    line_items_upserted: int
    sales_report: "TableSyncReport"
    payments_report: "TableSyncReport"
    line_items_report: "TableSyncReport"
    data_integrity_ok: bool
    oracle: "ConnectionReport"


class HealthResponse(BaseModel):
    status: str
    oracle_connected: bool
    oracle_target: str
    oracle_user: str
    odoo_ready: bool


class RetryBatch(BaseModel):
    row_ids: List[int]
    reason: str


class TableSyncReport(BaseModel):
    attempted: int
    upserted: int
    missing_row_ids: List[int]
    retry_batches: List[RetryBatch]
    errors: List[str]


class ConnectionReport(BaseModel):
    connected: bool
    target: str
    user: str


SyncSummary.model_rebuild()
