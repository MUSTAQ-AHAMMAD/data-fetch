"""Push staged rows from local SQLite database to Oracle."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status

from .config import Settings
from .db import get_connection
from .local_db import (
    get_unsynced_line_items,
    get_unsynced_payments,
    get_unsynced_sales,
    mark_synced,
)
from .schemas import ConnectionReport, PushSummary, RetryBatch, TableSyncReport

RETRY_BATCH_SIZE = 50


def _to_datetime(value: Any) -> Optional[datetime]:
    """Convert an ISO string (or passthrough datetime/None) to a Python datetime.

    python-oracledb maps Python ``datetime`` objects to Oracle DATE/TIMESTAMP
    natively. Passing a bare string causes Oracle to attempt an implicit
    conversion using the session NLS_DATE_FORMAT which typically does not
    support the ISO-8601 'T' separator stored by SQLite, leading to an
    ORA-01858 / ORA-01843 error and a 500 response.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def _normalize_sales_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Only include the columns that are bind variables in the Oracle MERGE SQL.
    # Extra SQLite-only columns (e.g. SYNCED_TO_ORACLE, FETCHED_AT) must be
    # stripped out; python-oracledb passes ALL dict keys as bind variable
    # metadata to Oracle, which rejects unknown variable names with ORA-01036.
    return [
        {
            "ROW_ID": r["ROW_ID"],
            "INVOICE_NUMBER": r["INVOICE_NUMBER"],
            "REGISTER_NAME": r["REGISTER_NAME"],
            "SALE_DATE": _to_datetime(r.get("SALE_DATE")),
            "TOTAL_PRICE": r["TOTAL_PRICE"],
            "TOTAL_TAX": r["TOTAL_TAX"],
            "TOTAL_LOYALTY": r["TOTAL_LOYALTY"],
            "TOTAL_PRICE_INCL_TAX": r["TOTAL_PRICE_INCL_TAX"],
            "VERSION": r["VERSION"],
            "REGION": r["REGION"],
            "CUSTOMER_TYPE": r["CUSTOMER_TYPE"],
        }
        for r in rows
    ]


def _normalize_payment_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "ROW_ID": r["ROW_ID"],
            "INVOICE_NUMBER": r["INVOICE_NUMBER"],
            "REGISTER_NAME": r["REGISTER_NAME"],
            "AMOUNT": r["AMOUNT"],
            "CURRENCY": r["CURRENCY"],
            "PAYMENT_TYPE": r["PAYMENT_TYPE"],
            "PAYMENT_DATE": _to_datetime(r.get("PAYMENT_DATE")),
            "DELETED_AT": _to_datetime(r.get("DELETED_AT")),
            "REGION": r["REGION"],
            "SALE_DATE": _to_datetime(r.get("SALE_DATE")),
        }
        for r in rows
    ]


def _normalize_line_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "ROW_ID": r["ROW_ID"],
            "INVOICE_NUMBER": r["INVOICE_NUMBER"],
            "LINE_NUMBER": r["LINE_NUMBER"],
            "ITEM_NUMBER": r["ITEM_NUMBER"],
            "ITEM_NAME": r["ITEM_NAME"],
            "QUANTITY": r["QUANTITY"],
            "LOYALTY_VALUE": r["LOYALTY_VALUE"],
            "TOTAL_PRICE": r["TOTAL_PRICE"],
            "TOTAL_TAX": r["TOTAL_TAX"],
            "TOTAL_DISCOUNT": r["TOTAL_DISCOUNT"],
            "TOTAL_LOYALTY": r["TOTAL_LOYALTY"],
            "REGION": r["REGION"],
            "SALE_DATE": _to_datetime(r.get("SALE_DATE")),
            "TAX_NAME": r["TAX_NAME"],
            "INV_UPLOAD_QNT_FLAG": r["INV_UPLOAD_QNT_FLAG"],
        }
        for r in rows
    ]


def _chunk_list(values: List[int], size: int) -> List[List[int]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _build_retry_batches(missing_ids: List[int]) -> List[RetryBatch]:
    if not missing_ids:
        return []
    return [
        RetryBatch(
            row_ids=batch,
            reason="Oracle merge did not accept these rows; safe to retry in a follow-up batch.",
        )
        for batch in _chunk_list(missing_ids, RETRY_BATCH_SIZE)
    ]


def _merge_rows_oracle(cursor, rows: List[Dict[str, Any]], sql: str) -> TableSyncReport:
    if not rows:
        return TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])
    cursor.executemany(sql, rows, batcherrors=True)
    errors = cursor.getbatcherrors() or []
    missing_ids: List[int] = []
    error_msgs: List[str] = []
    for error in errors:
        message = getattr(error, "message", "").strip() or "Oracle merge error"
        offset = getattr(error, "offset", None)
        if offset is not None and offset < len(rows):
            missing_ids.append(rows[offset]["ROW_ID"])
        error_msgs.append(message)
    return TableSyncReport(
        attempted=len(rows),
        upserted=len(rows) - len(errors),
        missing_row_ids=missing_ids,
        retry_batches=_build_retry_batches(missing_ids),
        errors=error_msgs,
    )


def _push_sales_oracle(cursor, rows: List[Dict[str, Any]]) -> TableSyncReport:
    if not rows:
        return TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])
    rows = _normalize_sales_rows(rows)
    sql = """
        MERGE INTO ODOO_INTEGRATION.BACKUP_VENDHQ_SALES_TEMP tgt
        USING (
            SELECT
                :ROW_ID AS ROW_ID,
                :INVOICE_NUMBER AS INVOICE_NUMBER,
                :REGISTER_NAME AS REGISTER_NAME,
                :SALE_DATE AS SALE_DATE,
                :TOTAL_PRICE AS TOTAL_PRICE,
                :TOTAL_TAX AS TOTAL_TAX,
                :TOTAL_LOYALTY AS TOTAL_LOYALTY,
                :TOTAL_PRICE_INCL_TAX AS TOTAL_PRICE_INCL_TAX,
                :VERSION AS VERSION,
                :REGION AS REGION,
                :CUSTOMER_TYPE AS CUSTOMER_TYPE
            FROM dual
        ) src
        ON (tgt.ROW_ID = src.ROW_ID)
        WHEN MATCHED THEN UPDATE SET
            tgt.INVOICE_NUMBER = src.INVOICE_NUMBER,
            tgt.REGISTER_NAME = src.REGISTER_NAME,
            tgt.SALE_DATE = src.SALE_DATE,
            tgt.TOTAL_PRICE = src.TOTAL_PRICE,
            tgt.TOTAL_TAX = src.TOTAL_TAX,
            tgt.TOTAL_LOYALTY = src.TOTAL_LOYALTY,
            tgt.TOTAL_PRICE_INCL_TAX = src.TOTAL_PRICE_INCL_TAX,
            tgt.VERSION = src.VERSION,
            tgt.REGION = src.REGION,
            tgt.CUSTOMER_TYPE = src.CUSTOMER_TYPE
        WHEN NOT MATCHED THEN INSERT (
            ROW_ID, INVOICE_NUMBER, REGISTER_NAME, SALE_DATE,
            TOTAL_PRICE, TOTAL_TAX, TOTAL_LOYALTY, TOTAL_PRICE_INCL_TAX,
            VERSION, REGION, CUSTOMER_TYPE
        ) VALUES (
            src.ROW_ID, src.INVOICE_NUMBER, src.REGISTER_NAME, src.SALE_DATE,
            src.TOTAL_PRICE, src.TOTAL_TAX, src.TOTAL_LOYALTY, src.TOTAL_PRICE_INCL_TAX,
            src.VERSION, src.REGION, src.CUSTOMER_TYPE
        )
    """
    return _merge_rows_oracle(cursor, rows, sql)


def _push_payments_oracle(cursor, rows: List[Dict[str, Any]]) -> TableSyncReport:
    if not rows:
        return TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])
    rows = _normalize_payment_rows(rows)
    sql = """
        MERGE INTO ODOO_INTEGRATION.BACKUP_VENDHQ_PAYMENTS_TEMP tgt
        USING (
            SELECT
                :ROW_ID AS ROW_ID,
                :INVOICE_NUMBER AS INVOICE_NUMBER,
                :REGISTER_NAME AS REGISTER_NAME,
                :AMOUNT AS AMOUNT,
                :CURRENCY AS CURRENCY,
                :PAYMENT_TYPE AS PAYMENT_TYPE,
                :PAYMENT_DATE AS PAYMENT_DATE,
                :DELETED_AT AS DELETED_AT,
                :REGION AS REGION,
                :SALE_DATE AS SALE_DATE
            FROM dual
        ) src
        ON (tgt.ROW_ID = src.ROW_ID)
        WHEN MATCHED THEN UPDATE SET
            tgt.INVOICE_NUMBER = src.INVOICE_NUMBER,
            tgt.REGISTER_NAME = src.REGISTER_NAME,
            tgt.AMOUNT = src.AMOUNT,
            tgt.CURRENCY = src.CURRENCY,
            tgt.PAYMENT_TYPE = src.PAYMENT_TYPE,
            tgt.PAYMENT_DATE = src.PAYMENT_DATE,
            tgt.DELETED_AT = src.DELETED_AT,
            tgt.REGION = src.REGION,
            tgt.SALE_DATE = src.SALE_DATE
        WHEN NOT MATCHED THEN INSERT (
            ROW_ID, INVOICE_NUMBER, REGISTER_NAME, AMOUNT, CURRENCY,
            PAYMENT_TYPE, PAYMENT_DATE, DELETED_AT, REGION, SALE_DATE
        ) VALUES (
            src.ROW_ID, src.INVOICE_NUMBER, src.REGISTER_NAME, src.AMOUNT, src.CURRENCY,
            src.PAYMENT_TYPE, src.PAYMENT_DATE, src.DELETED_AT, src.REGION, src.SALE_DATE
        )
    """
    return _merge_rows_oracle(cursor, rows, sql)


def _push_lines_oracle(cursor, rows: List[Dict[str, Any]]) -> TableSyncReport:
    if not rows:
        return TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])
    rows = _normalize_line_rows(rows)
    sql = """
        MERGE INTO ODOO_INTEGRATION.BACKUP_VENDHQ_LINE_ITEMS_TEMP tgt
        USING (
            SELECT
                :ROW_ID AS ROW_ID,
                :INVOICE_NUMBER AS INVOICE_NUMBER,
                :LINE_NUMBER AS LINE_NUMBER,
                :ITEM_NUMBER AS ITEM_NUMBER,
                :ITEM_NAME AS ITEM_NAME,
                :QUANTITY AS QUANTITY,
                :LOYALTY_VALUE AS LOYALTY_VALUE,
                :TOTAL_PRICE AS TOTAL_PRICE,
                :TOTAL_TAX AS TOTAL_TAX,
                :TOTAL_DISCOUNT AS TOTAL_DISCOUNT,
                :TOTAL_LOYALTY AS TOTAL_LOYALTY,
                :REGION AS REGION,
                :SALE_DATE AS SALE_DATE,
                :TAX_NAME AS TAX_NAME,
                :INV_UPLOAD_QNT_FLAG AS INV_UPLOAD_QNT_FLAG
            FROM dual
        ) src
        ON (tgt.ROW_ID = src.ROW_ID)
        WHEN MATCHED THEN UPDATE SET
            tgt.INVOICE_NUMBER = src.INVOICE_NUMBER,
            tgt.LINE_NUMBER = src.LINE_NUMBER,
            tgt.ITEM_NUMBER = src.ITEM_NUMBER,
            tgt.ITEM_NAME = src.ITEM_NAME,
            tgt.QUANTITY = src.QUANTITY,
            tgt.LOYALTY_VALUE = src.LOYALTY_VALUE,
            tgt.TOTAL_PRICE = src.TOTAL_PRICE,
            tgt.TOTAL_TAX = src.TOTAL_TAX,
            tgt.TOTAL_DISCOUNT = src.TOTAL_DISCOUNT,
            tgt.TOTAL_LOYALTY = src.TOTAL_LOYALTY,
            tgt.REGION = src.REGION,
            tgt.SALE_DATE = src.SALE_DATE,
            tgt.TAX_NAME = src.TAX_NAME,
            tgt.INV_UPLOAD_QNT_FLAG = src.INV_UPLOAD_QNT_FLAG
        WHEN NOT MATCHED THEN INSERT (
            ROW_ID, INVOICE_NUMBER, LINE_NUMBER, ITEM_NUMBER, ITEM_NAME, QUANTITY,
            LOYALTY_VALUE, TOTAL_PRICE, TOTAL_TAX, TOTAL_DISCOUNT, TOTAL_LOYALTY,
            REGION, SALE_DATE, TAX_NAME, INV_UPLOAD_QNT_FLAG
        ) VALUES (
            src.ROW_ID, src.INVOICE_NUMBER, src.LINE_NUMBER, src.ITEM_NUMBER, src.ITEM_NAME, src.QUANTITY,
            src.LOYALTY_VALUE, src.TOTAL_PRICE, src.TOTAL_TAX, src.TOTAL_DISCOUNT, src.TOTAL_LOYALTY,
            src.REGION, src.SALE_DATE, src.TAX_NAME, src.INV_UPLOAD_QNT_FLAG
        )
    """
    return _merge_rows_oracle(cursor, rows, sql)


async def push_to_oracle(
    settings: Settings,
    tables: Optional[List[str]] = None,
    batch_size: int = 500,
) -> PushSummary:
    """Push unsynced rows from local SQLite to Oracle.

    Args:
        settings: Application settings.
        tables: List of tables to push. If None, pushes all three tables.
                Valid values: 'sales', 'payments', 'line_items'.
        batch_size: Maximum rows per table to push in one call.
    """
    from .db import describe_target, test_connection

    if tables is None:
        tables = ["sales", "payments", "line_items"]

    oracle_target = describe_target(settings)
    oracle_connected = await test_connection(settings)

    sales_report = TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])
    payments_report = TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])
    lines_report = TableSyncReport(attempted=0, upserted=0, missing_row_ids=[], retry_batches=[], errors=[])

    if not oracle_connected:
        return PushSummary(
            sales_pushed=0,
            payments_pushed=0,
            line_items_pushed=0,
            sales_report=sales_report,
            payments_report=payments_report,
            line_items_report=lines_report,
            data_integrity_ok=True,
            oracle=ConnectionReport(connected=False, target=oracle_target, user=settings.oracle_user),
        )

    sales_rows: List[Dict[str, Any]] = []
    payment_rows: List[Dict[str, Any]] = []
    line_rows: List[Dict[str, Any]] = []

    if "sales" in tables:
        sales_rows = await get_unsynced_sales(settings, batch_size)
    if "payments" in tables:
        payment_rows = await get_unsynced_payments(settings, batch_size)
    if "line_items" in tables:
        line_rows = await get_unsynced_line_items(settings, batch_size)

    try:
        async with get_connection(settings) as conn:
            cursor = await asyncio.to_thread(conn.cursor)
            try:
                if sales_rows:
                    sales_report = await asyncio.to_thread(_push_sales_oracle, cursor, sales_rows)
                if payment_rows:
                    payments_report = await asyncio.to_thread(_push_payments_oracle, cursor, payment_rows)
                if line_rows:
                    lines_report = await asyncio.to_thread(_push_lines_oracle, cursor, line_rows)
                await asyncio.to_thread(conn.commit)
            finally:
                await asyncio.to_thread(cursor.close)
    except HTTPException:
        # HTTPExceptions are already properly formatted API responses
        # (correct status code + JSON body); let them pass through so
        # FastAPI's ExceptionMiddleware handles them and CORSMiddleware
        # can attach its headers normally.
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Oracle push failed: {exc}",
        ) from exc

    # Mark successfully pushed rows as synced
    if sales_rows:
        synced_ids = [r["ROW_ID"] for r in sales_rows if r["ROW_ID"] not in sales_report.missing_row_ids]
        await mark_synced(settings, "TEST_BACKUP_VENDHQ_SALES", synced_ids)
    if payment_rows:
        synced_ids = [r["ROW_ID"] for r in payment_rows if r["ROW_ID"] not in payments_report.missing_row_ids]
        await mark_synced(settings, "TEST_BACKUP_VENDHQ_PAYMENTS", synced_ids)
    if line_rows:
        synced_ids = [r["ROW_ID"] for r in line_rows if r["ROW_ID"] not in lines_report.missing_row_ids]
        await mark_synced(settings, "TEST_BACKUP_VENDHQ_LINE_ITEMS", synced_ids)

    data_integrity_ok = all(
        r.upserted == r.attempted and not r.missing_row_ids
        for r in [sales_report, payments_report, lines_report]
        if r.attempted > 0
    )

    return PushSummary(
        sales_pushed=sales_report.upserted,
        payments_pushed=payments_report.upserted,
        line_items_pushed=lines_report.upserted,
        sales_report=sales_report,
        payments_report=payments_report,
        line_items_report=lines_report,
        data_integrity_ok=data_integrity_ok,
        oracle=ConnectionReport(connected=True, target=oracle_target, user=settings.oracle_user),
    )
