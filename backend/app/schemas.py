from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, field_validator, model_validator


class SyncRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    order_id_gt: Optional[int] = None
    limit: Optional[int] = None
    pos_id: Optional[int] = None
    company_id: Optional[int] = None

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


# ── Local data query / response ──────────────────────────────────────────────

class LocalDataQuery(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    invoice_number: Optional[str] = None
    outlet_name: Optional[str] = None
    synced: Optional[bool] = None
    limit: int = 500
    offset: int = 0

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("limit must be positive")
        if value > 2000:
            raise ValueError("limit must not exceed 2000")
        return value

    @field_validator("offset")
    @classmethod
    def validate_offset(cls, value: int) -> int:
        if value < 0:
            raise ValueError("offset must be non-negative")
        return value


class LocalDataResponse(BaseModel):
    total: int
    rows: List[Dict[str, Any]]


class UnsyncedCount(BaseModel):
    sales: int
    payments: int
    line_items: int


# ── Oracle push ───────────────────────────────────────────────────────────────

class PushRequest(BaseModel):
    tables: Optional[List[str]] = None
    batch_size: int = 500

    @field_validator("tables")
    @classmethod
    def validate_tables(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return value
        valid = {"sales", "payments", "line_items"}
        for t in value:
            if t not in valid:
                raise ValueError(f"Invalid table '{t}'. Must be one of: {sorted(valid)}")
        return value

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("batch_size must be positive")
        if value > 5000:
            raise ValueError("batch_size must not exceed 5000")
        return value


class PushSummary(BaseModel):
    sales_pushed: int
    payments_pushed: int
    line_items_pushed: int
    sales_report: "TableSyncReport"
    payments_report: "TableSyncReport"
    line_items_report: "TableSyncReport"
    data_integrity_ok: bool
    oracle: "ConnectionReport"


class SyncProgress(BaseModel):
    status: str           # idle | fetching | storing | done | error
    fetched: int
    total: Optional[int]  # None if not yet known
    error: Optional[str]


class ClearRequest(BaseModel):
    tables: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @field_validator("tables")
    @classmethod
    def validate_tables(cls, value: List[str]) -> List[str]:
        valid = {"sales", "payments", "line_items"}
        for t in value:
            if t not in valid:
                raise ValueError(f"Invalid table '{t}'. Must be one of: {sorted(valid)}")
        return value


class ClearResponse(BaseModel):
    deleted: Dict[str, int]


SyncSummary.model_rebuild()
PushSummary.model_rebuild()
