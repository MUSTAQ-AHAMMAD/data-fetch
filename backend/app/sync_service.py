from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status

from . import cancel as _cancel
from . import odoo_client
from .config import Settings
from .db import describe_target, test_connection
from .local_db import init_db, upsert_line_items, upsert_payments, upsert_sales
from .schemas import (
    ConnectionReport,
    SyncSummary,
    TableSyncReport,
)


def _parse_date(date_str: str) -> datetime:
    if not date_str:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Order missing date_order field",
        )
    # Normalise to "YYYY-MM-DD HH:MM:SS" before parsing so that both the
    # classic Odoo format ("2026-04-07 08:47:21") and ISO-8601 variants
    # ("2026-04-07T08:47:21", "2026-04-07T08:47:21.000Z", "...+03:00")
    # are all handled without error.
    normalized = date_str[:19].replace("T", " ")
    try:
        return datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cannot parse date from Odoo: {date_str!r}",
        ) from exc


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


def _ensure_config(settings: Settings) -> None:
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


def _data_integrity_ok(reports: List[TableSyncReport]) -> bool:
    return all(
        report.upserted == report.attempted and not report.missing_row_ids for report in reports
    )


async def _write_to_local(
    settings: Settings, orders: List[Dict[str, Any]]
) -> Tuple[TableSyncReport, TableSyncReport, TableSyncReport]:
    """Store fetched Odoo orders in the local SQLite database."""
    sales_rows = _build_sales_rows(orders, settings)
    payment_rows = _build_payment_rows(orders, settings)
    line_rows = _build_line_rows(orders, settings)

    await init_db(settings)
    sales_count = await upsert_sales(settings, sales_rows)
    payment_count = await upsert_payments(settings, payment_rows)
    line_count = await upsert_line_items(settings, line_rows)

    empty_report = lambda attempted, upserted: TableSyncReport(
        attempted=attempted,
        upserted=upserted,
        missing_row_ids=[],
        retry_batches=[],
        errors=[],
    )
    return (
        empty_report(len(sales_rows), sales_count),
        empty_report(len(payment_rows), payment_count),
        empty_report(len(line_rows), line_count),
    )


async def sync_orders(
    settings: Settings,
    start_date: datetime,
    end_date: datetime,
    order_id_gt: Optional[int],
    page_limit: int,
    pos_id: Optional[int] = None,
    company_id: Optional[int] = None,
) -> SyncSummary:
    _cancel.reset()
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

    if _cancel.is_cancelled():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Sync cancelled before writing to local database.",
        )

    try:
        sales_report, payments_report, lines_report = await _write_to_local(settings, orders)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to write to local database: {exc}",
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
