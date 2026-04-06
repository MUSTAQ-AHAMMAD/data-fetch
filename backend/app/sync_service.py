import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status

from . import odoo_client
from .config import Settings
from .db import describe_target, get_connection, test_connection
from .schemas import (
    ConnectionReport,
    RetryBatch,
    SyncSummary,
    TableSyncReport,
)

RETRY_BATCH_SIZE = 50


def _parse_date(date_str: str) -> datetime:
    if not date_str:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Order missing date_order field",
        )
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")


def _customer_type(customer_name: str) -> str:
    if not customer_name:
        return "NORMAL"
    upper = customer_name.upper()
    if "WC-" in upper:
        return "WHOLESALE"
    if "VIP" in upper:
        return "VIP"
    return "NORMAL"


def _discount_amount(line: Dict[str, Any]) -> float:
    discount_percent = float(line.get("discount") or 0)
    subtotal = float(line.get("price_subtotal") or 0)
    if discount_percent <= 0:
        return 0.0
    if discount_percent >= 100:
        return subtotal
    try:
        pre_discount_total = subtotal / (1 - discount_percent / 100)
        return pre_discount_total - subtotal
    except ZeroDivisionError:
        return 0.0


def _line_tax(line: Dict[str, Any]) -> float:
    if line.get("price_tax") is not None:
        return float(line["price_tax"])
    subtotal_incl = line.get("price_subtotal_incl")
    subtotal = float(line.get("price_subtotal") or 0)
    if subtotal_incl is not None:
        return float(subtotal_incl) - subtotal
    return 0.0


def _inv_upload_flag(item_name: str) -> str:
    if item_name and "TOBACCO" in item_name.upper():
        return "Y"
    return "N"


def _chunk_list(values: List[int], size: int) -> List[List[int]]:
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


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


def _ensure_config(settings: Settings) -> None:
    missing = []
    for key in ("oracle_host", "oracle_service", "oracle_password"):
        if not getattr(settings, key):
            missing.append(key)
    if missing:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Missing Oracle configuration values: {', '.join(missing)}",
        )
    if not settings.odoo_api_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Missing Odoo API key configuration",
        )


def _build_sales_rows(
    orders: List[Dict[str, Any]], settings: Settings
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for order_wrapper in orders:
        order = order_wrapper.get("order") or {}
        sale_date = _parse_date(order["date_order"])
        total_tax = float(order.get("amount_tax") or 0)
        total_paid = float(order.get("amount_paid") or 0)

        rows.append(
            {
                "row_id": int(order["order_id"]),
                "invoice_number": order.get("name", ""),
                "outlet_name": order.get("pos_name", ""),
                "register_name": order.get("pos_name", ""),
                "sale_date": sale_date,
                "total_price": total_paid - total_tax,
                "total_tax": total_tax,
                "total_loyalty": 0,
                "total_price_incl_tax": total_paid,
                "version": int(datetime.utcnow().timestamp()),
                "region": settings.region,
                "customer_type": _customer_type(order.get("customer_name", "")),
            }
        )
    return rows


def _build_payment_rows(
    orders: List[Dict[str, Any]], settings: Settings
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for order_wrapper in orders:
        order = order_wrapper.get("order") or {}
        sale_date = _parse_date(order["date_order"])
        invoice_number = order.get("name", "")
        outlet = order.get("pos_name", "")
        payments = order_wrapper.get("payments") or []
        for payment in payments:
            method = payment.get("payment_method_id") or ["", ""]
            payment_type = method[1] if len(method) > 1 else ""
            rows.append(
                {
                    "row_id": int(payment["id"]),
                    "invoice_number": invoice_number,
                    "outlet_name": outlet,
                    "register_name": outlet,
                    "amount": float(payment.get("amount") or 0),
                    "currency": "SAR",
                    "payment_type": payment_type,
                    "payment_date": sale_date,
                    "deleted_at": None,
                    "region": settings.region,
                    "sale_date": sale_date,
                }
            )
    return rows


def _build_line_rows(
    orders: List[Dict[str, Any]], settings: Settings
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for order_wrapper in orders:
        order = order_wrapper.get("order") or {}
        lines = order_wrapper.get("lines") or []
        invoice_number = order.get("name", "")
        sale_date = _parse_date(order["date_order"])
        for idx, line in enumerate(lines, start=1):
            product = line.get("product_id") or ["", ""]
            item_number = str(product[0]) if product else ""
            item_name = (product[1] if len(product) > 1 else "") or "Discount Item"
            rows.append(
                {
                    "row_id": int(line["id"]),
                    "invoice_number": invoice_number,
                    "line_number": idx,
                    "item_number": item_number,
                    "item_name": item_name,
                    "quantity": float(line.get("qty") or 0),
                    "loyalty_value": 0,
                    "total_price": float(line.get("price_subtotal") or 0),
                    "total_tax": _line_tax(line),
                    "total_discount": _discount_amount(line),
                    "total_loyalty": 1 if line.get("is_program_reward") else 0,
                    "region": settings.region,
                    "sale_date": sale_date,
                    "tax_name": "OUTPUT-GOODS-DOM-15%",
                    "inv_upload_qnt_flag": _inv_upload_flag(item_name),
                }
            )
    return rows


def _empty_report() -> TableSyncReport:
    return TableSyncReport(
        attempted=0,
        upserted=0,
        missing_row_ids=[],
        retry_batches=[],
        errors=[],
    )


def _merge_rows(cursor, rows: List[Dict[str, Any]], sql: str) -> TableSyncReport:
    if not rows:
        return _empty_report()

    cursor.executemany(sql, rows, batcherrors=True)
    errors = cursor.getbatcherrors() or []
    missing_ids: List[int] = []
    error_messages: List[str] = []
    for error in errors:
        offset = getattr(error, "offset", None)
        message = getattr(error, "message", "").strip() or "Oracle merge error"
        if offset is not None:
            message = f"{message} (row offset {offset})"
        error_messages.append(message)
        if offset is None:
            continue
        if 0 <= offset < len(rows):
            row_id = rows[offset].get("row_id")
            if row_id is not None:
                missing_ids.append(int(row_id))

    return TableSyncReport(
        attempted=len(rows),
        upserted=len(rows) - len(errors),
        missing_row_ids=missing_ids,
        retry_batches=_build_retry_batches(missing_ids),
        errors=error_messages,
    )


def _data_integrity_ok(reports: List[TableSyncReport]) -> bool:
    return all(
        report.upserted == report.attempted and not report.missing_row_ids for report in reports
    )


def _merge_sales(cursor, rows: List[Dict[str, Any]]) -> TableSyncReport:
    if not rows:
        return _empty_report()
    sql = """
        MERGE INTO ODOO_INTEGRATION.TEST_BACKUP_VENDHQ_SALES tgt
        USING (
            SELECT
                :row_id AS ROW_ID,
                :invoice_number AS INVOICE_NUMBER,
                :outlet_name AS OUTLET_NAME,
                :register_name AS REGISTER_NAME,
                :sale_date AS SALE_DATE,
                :total_price AS TOTAL_PRICE,
                :total_tax AS TOTAL_TAX,
                :total_loyalty AS TOTAL_LOYALTY,
                :total_price_incl_tax AS TOTAL_PRICE_INCL_TAX,
                :version AS VERSION,
                :region AS REGION,
                :customer_type AS CUSTOMER_TYPE
            FROM dual
        ) src
        ON (tgt.ROW_ID = src.ROW_ID)
        WHEN MATCHED THEN UPDATE SET
            tgt.INVOICE_NUMBER = src.INVOICE_NUMBER,
            tgt.OUTLET_NAME = src.OUTLET_NAME,
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
            ROW_ID, INVOICE_NUMBER, OUTLET_NAME, REGISTER_NAME, SALE_DATE,
            TOTAL_PRICE, TOTAL_TAX, TOTAL_LOYALTY, TOTAL_PRICE_INCL_TAX,
            VERSION, REGION, CUSTOMER_TYPE
        ) VALUES (
            src.ROW_ID, src.INVOICE_NUMBER, src.OUTLET_NAME, src.REGISTER_NAME, src.SALE_DATE,
            src.TOTAL_PRICE, src.TOTAL_TAX, src.TOTAL_LOYALTY, src.TOTAL_PRICE_INCL_TAX,
            src.VERSION, src.REGION, src.CUSTOMER_TYPE
        )
    """
    return _merge_rows(cursor, rows, sql)


def _merge_payments(cursor, rows: List[Dict[str, Any]]) -> TableSyncReport:
    if not rows:
        return _empty_report()
    sql = """
        MERGE INTO ODOO_INTEGRATION.TEST_BACKUP_VENDHQ_PAYMENTS tgt
        USING (
            SELECT
                :row_id AS ROW_ID,
                :invoice_number AS INVOICE_NUMBER,
                :outlet_name AS OUTLET_NAME,
                :register_name AS REGISTER_NAME,
                :amount AS AMOUNT,
                :currency AS CURRENCY,
                :payment_type AS PAYMENT_TYPE,
                :payment_date AS PAYMENT_DATE,
                :deleted_at AS DELETED_AT,
                :region AS REGION,
                :sale_date AS SALE_DATE
            FROM dual
        ) src
        ON (tgt.ROW_ID = src.ROW_ID)
        WHEN MATCHED THEN UPDATE SET
            tgt.INVOICE_NUMBER = src.INVOICE_NUMBER,
            tgt.OUTLET_NAME = src.OUTLET_NAME,
            tgt.REGISTER_NAME = src.REGISTER_NAME,
            tgt.AMOUNT = src.AMOUNT,
            tgt.CURRENCY = src.CURRENCY,
            tgt.PAYMENT_TYPE = src.PAYMENT_TYPE,
            tgt.PAYMENT_DATE = src.PAYMENT_DATE,
            tgt.DELETED_AT = src.DELETED_AT,
            tgt.REGION = src.REGION,
            tgt.SALE_DATE = src.SALE_DATE
        WHEN NOT MATCHED THEN INSERT (
            ROW_ID, INVOICE_NUMBER, OUTLET_NAME, REGISTER_NAME, AMOUNT, CURRENCY,
            PAYMENT_TYPE, PAYMENT_DATE, DELETED_AT, REGION, SALE_DATE
        ) VALUES (
            src.ROW_ID, src.INVOICE_NUMBER, src.OUTLET_NAME, src.REGISTER_NAME, src.AMOUNT, src.CURRENCY,
            src.PAYMENT_TYPE, src.PAYMENT_DATE, src.DELETED_AT, src.REGION, src.SALE_DATE
        )
    """
    return _merge_rows(cursor, rows, sql)


def _merge_lines(cursor, rows: List[Dict[str, Any]]) -> TableSyncReport:
    if not rows:
        return _empty_report()
    sql = """
        MERGE INTO ODOO_INTEGRATION.TEST_BACKUP_VENDHQ_LINE_ITEMS tgt
        USING (
            SELECT
                :row_id AS ROW_ID,
                :invoice_number AS INVOICE_NUMBER,
                :line_number AS LINE_NUMBER,
                :item_number AS ITEM_NUMBER,
                :item_name AS ITEM_NAME,
                :quantity AS QUANTITY,
                :loyalty_value AS LOYALTY_VALUE,
                :total_price AS TOTAL_PRICE,
                :total_tax AS TOTAL_TAX,
                :total_discount AS TOTAL_DISCOUNT,
                :total_loyalty AS TOTAL_LOYALTY,
                :region AS REGION,
                :sale_date AS SALE_DATE,
                :tax_name AS TAX_NAME,
                :inv_upload_qnt_flag AS INV_UPLOAD_QNT_FLAG
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
    return _merge_rows(cursor, rows, sql)


async def _write_to_oracle(
    settings: Settings, orders: List[Dict[str, Any]]
) -> Tuple[TableSyncReport, TableSyncReport, TableSyncReport]:
    sales_rows = _build_sales_rows(orders, settings)
    payment_rows = _build_payment_rows(orders, settings)
    line_rows = _build_line_rows(orders, settings)

    async with get_connection(settings) as conn:
        cursor = await asyncio.to_thread(conn.cursor)
        try:
            sales_report = await asyncio.to_thread(_merge_sales, cursor, sales_rows)
            payments_report = await asyncio.to_thread(_merge_payments, cursor, payment_rows)
            lines_report = await asyncio.to_thread(_merge_lines, cursor, line_rows)
            await asyncio.to_thread(conn.commit)
            return sales_report, payments_report, lines_report
        finally:
            await asyncio.to_thread(cursor.close)


async def sync_orders(
    settings: Settings,
    start_date: datetime,
    end_date: datetime,
    order_id_gt: Optional[int],
    page_limit: int,
    pos_id: Optional[int] = None,
    company_id: Optional[int] = None,
) -> SyncSummary:
    _ensure_config(settings)
    oracle_target = describe_target(settings)
    oracle_connected = await test_connection(settings)
    orders = await odoo_client.fetch_orders(
        settings=settings,
        start_date=start_date,
        end_date=end_date,
        order_id_gt=order_id_gt,
        page_limit=page_limit,
        pos_id=pos_id,
        company_id=company_id,
    )

    if not orders:
        empty_sales = _empty_report()
        empty_payments = _empty_report()
        empty_lines = _empty_report()
        return SyncSummary(
            orders_fetched=0,
            sales_upserted=empty_sales.upserted,
            payments_upserted=empty_payments.upserted,
            line_items_upserted=empty_lines.upserted,
            sales_report=empty_sales,
            payments_report=empty_payments,
            line_items_report=empty_lines,
            data_integrity_ok=True,
            oracle=ConnectionReport(
                connected=oracle_connected,
                target=oracle_target,
                user=settings.oracle_user,
            ),
        )

    try:
        sales_report, payments_report, lines_report = await _write_to_oracle(settings, orders)
        oracle_connected = True
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to write to Oracle ({oracle_target}): {exc}",
        ) from exc

    reports = [sales_report, payments_report, lines_report]

    return SyncSummary(
        orders_fetched=len(orders),
        sales_upserted=sales_report.upserted,
        payments_upserted=payments_report.upserted,
        line_items_upserted=lines_report.upserted,
        sales_report=sales_report,
        payments_report=payments_report,
        line_items_report=lines_report,
        data_integrity_ok=_data_integrity_ok(reports),
        oracle=ConnectionReport(
            connected=oracle_connected,
            target=oracle_target,
            user=settings.oracle_user,
        ),
    )
