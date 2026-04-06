"""Local SQLite database for staging data fetched from Odoo before pushing to Oracle."""

import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from .config import Settings

_DB_PATH: Optional[Path] = None


def _get_db_path(settings: Settings) -> Path:
    global _DB_PATH
    if _DB_PATH is None:
        root = Path(__file__).resolve().parent.parent
        _DB_PATH = root / "local_data.db"
    return _DB_PATH


_DDL = """
CREATE TABLE IF NOT EXISTS TEST_BACKUP_VENDHQ_SALES (
    ROW_ID             INTEGER PRIMARY KEY,
    INVOICE_NUMBER     TEXT,
    OUTLET_NAME        TEXT,
    REGISTER_NAME      TEXT,
    SALE_DATE          TEXT,
    TOTAL_PRICE        REAL,
    TOTAL_TAX          REAL,
    TOTAL_LOYALTY      REAL,
    TOTAL_PRICE_INCL_TAX REAL,
    VERSION            INTEGER,
    REGION             TEXT,
    CUSTOMER_TYPE      TEXT,
    SYNCED_TO_ORACLE   INTEGER DEFAULT 0,
    FETCHED_AT         TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS TEST_BACKUP_VENDHQ_PAYMENTS (
    ROW_ID             INTEGER PRIMARY KEY,
    INVOICE_NUMBER     TEXT,
    OUTLET_NAME        TEXT,
    REGISTER_NAME      TEXT,
    AMOUNT             REAL,
    CURRENCY           TEXT,
    PAYMENT_TYPE       TEXT,
    PAYMENT_DATE       TEXT,
    DELETED_AT         TEXT,
    REGION             TEXT,
    SALE_DATE          TEXT,
    SYNCED_TO_ORACLE   INTEGER DEFAULT 0,
    FETCHED_AT         TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS TEST_BACKUP_VENDHQ_LINE_ITEMS (
    ROW_ID             INTEGER PRIMARY KEY,
    INVOICE_NUMBER     TEXT,
    LINE_NUMBER        INTEGER,
    ITEM_NUMBER        TEXT,
    ITEM_NAME          TEXT,
    QUANTITY           REAL,
    LOYALTY_VALUE      REAL,
    TOTAL_PRICE        REAL,
    TOTAL_TAX          REAL,
    TOTAL_DISCOUNT     REAL,
    TOTAL_LOYALTY      REAL,
    REGION             TEXT,
    SALE_DATE          TEXT,
    TAX_NAME           TEXT,
    INV_UPLOAD_QNT_FLAG TEXT,
    SYNCED_TO_ORACLE   INTEGER DEFAULT 0,
    FETCHED_AT         TEXT DEFAULT (datetime('now'))
);
"""


async def init_db(settings: Settings) -> None:
    """Create tables if they do not exist."""
    path = _get_db_path(settings)
    async with aiosqlite.connect(path) as db:
        await db.executescript(_DDL)
        await db.commit()


async def upsert_sales(settings: Settings, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    path = _get_db_path(settings)
    sql = """
        INSERT INTO TEST_BACKUP_VENDHQ_SALES
            (ROW_ID, INVOICE_NUMBER, OUTLET_NAME, REGISTER_NAME, SALE_DATE,
             TOTAL_PRICE, TOTAL_TAX, TOTAL_LOYALTY, TOTAL_PRICE_INCL_TAX,
             VERSION, REGION, CUSTOMER_TYPE)
        VALUES
            (:row_id, :invoice_number, :outlet_name, :register_name, :sale_date,
             :total_price, :total_tax, :total_loyalty, :total_price_incl_tax,
             :version, :region, :customer_type)
        ON CONFLICT(ROW_ID) DO UPDATE SET
            INVOICE_NUMBER = excluded.INVOICE_NUMBER,
            OUTLET_NAME = excluded.OUTLET_NAME,
            REGISTER_NAME = excluded.REGISTER_NAME,
            SALE_DATE = excluded.SALE_DATE,
            TOTAL_PRICE = excluded.TOTAL_PRICE,
            TOTAL_TAX = excluded.TOTAL_TAX,
            TOTAL_LOYALTY = excluded.TOTAL_LOYALTY,
            TOTAL_PRICE_INCL_TAX = excluded.TOTAL_PRICE_INCL_TAX,
            VERSION = excluded.VERSION,
            REGION = excluded.REGION,
            CUSTOMER_TYPE = excluded.CUSTOMER_TYPE,
            SYNCED_TO_ORACLE = 0
    """
    serialized = [
        {**r, "sale_date": r["sale_date"].isoformat() if hasattr(r["sale_date"], "isoformat") else r["sale_date"]}
        for r in rows
    ]
    async with aiosqlite.connect(path) as db:
        await db.executemany(sql, serialized)
        await db.commit()
    return len(rows)


async def upsert_payments(settings: Settings, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    path = _get_db_path(settings)
    sql = """
        INSERT INTO TEST_BACKUP_VENDHQ_PAYMENTS
            (ROW_ID, INVOICE_NUMBER, OUTLET_NAME, REGISTER_NAME, AMOUNT, CURRENCY,
             PAYMENT_TYPE, PAYMENT_DATE, DELETED_AT, REGION, SALE_DATE)
        VALUES
            (:row_id, :invoice_number, :outlet_name, :register_name, :amount, :currency,
             :payment_type, :payment_date, :deleted_at, :region, :sale_date)
        ON CONFLICT(ROW_ID) DO UPDATE SET
            INVOICE_NUMBER = excluded.INVOICE_NUMBER,
            OUTLET_NAME = excluded.OUTLET_NAME,
            REGISTER_NAME = excluded.REGISTER_NAME,
            AMOUNT = excluded.AMOUNT,
            CURRENCY = excluded.CURRENCY,
            PAYMENT_TYPE = excluded.PAYMENT_TYPE,
            PAYMENT_DATE = excluded.PAYMENT_DATE,
            DELETED_AT = excluded.DELETED_AT,
            REGION = excluded.REGION,
            SALE_DATE = excluded.SALE_DATE,
            SYNCED_TO_ORACLE = 0
    """
    serialized = [
        {
            **r,
            "payment_date": r["payment_date"].isoformat() if hasattr(r["payment_date"], "isoformat") else r["payment_date"],
            "sale_date": r["sale_date"].isoformat() if hasattr(r["sale_date"], "isoformat") else r["sale_date"],
        }
        for r in rows
    ]
    async with aiosqlite.connect(path) as db:
        await db.executemany(sql, serialized)
        await db.commit()
    return len(rows)


async def upsert_line_items(settings: Settings, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    path = _get_db_path(settings)
    sql = """
        INSERT INTO TEST_BACKUP_VENDHQ_LINE_ITEMS
            (ROW_ID, INVOICE_NUMBER, LINE_NUMBER, ITEM_NUMBER, ITEM_NAME, QUANTITY,
             LOYALTY_VALUE, TOTAL_PRICE, TOTAL_TAX, TOTAL_DISCOUNT, TOTAL_LOYALTY,
             REGION, SALE_DATE, TAX_NAME, INV_UPLOAD_QNT_FLAG)
        VALUES
            (:row_id, :invoice_number, :line_number, :item_number, :item_name, :quantity,
             :loyalty_value, :total_price, :total_tax, :total_discount, :total_loyalty,
             :region, :sale_date, :tax_name, :inv_upload_qnt_flag)
        ON CONFLICT(ROW_ID) DO UPDATE SET
            INVOICE_NUMBER = excluded.INVOICE_NUMBER,
            LINE_NUMBER = excluded.LINE_NUMBER,
            ITEM_NUMBER = excluded.ITEM_NUMBER,
            ITEM_NAME = excluded.ITEM_NAME,
            QUANTITY = excluded.QUANTITY,
            LOYALTY_VALUE = excluded.LOYALTY_VALUE,
            TOTAL_PRICE = excluded.TOTAL_PRICE,
            TOTAL_TAX = excluded.TOTAL_TAX,
            TOTAL_DISCOUNT = excluded.TOTAL_DISCOUNT,
            TOTAL_LOYALTY = excluded.TOTAL_LOYALTY,
            REGION = excluded.REGION,
            SALE_DATE = excluded.SALE_DATE,
            TAX_NAME = excluded.TAX_NAME,
            INV_UPLOAD_QNT_FLAG = excluded.INV_UPLOAD_QNT_FLAG,
            SYNCED_TO_ORACLE = 0
    """
    serialized = [
        {**r, "sale_date": r["sale_date"].isoformat() if hasattr(r["sale_date"], "isoformat") else r["sale_date"]}
        for r in rows
    ]
    async with aiosqlite.connect(path) as db:
        await db.executemany(sql, serialized)
        await db.commit()
    return len(rows)


_ALLOWED_TABLES = {
    "TEST_BACKUP_VENDHQ_SALES",
    "TEST_BACKUP_VENDHQ_PAYMENTS",
    "TEST_BACKUP_VENDHQ_LINE_ITEMS",
}

_ALLOWED_DATE_COLS = {"SALE_DATE", "PAYMENT_DATE"}


def _build_where(
    params: Dict[str, Any],
    start_date: Optional[str],
    end_date: Optional[str],
    invoice_number: Optional[str],
    outlet_name: Optional[str],
    synced: Optional[bool],
    date_col: str = "SALE_DATE",
) -> str:
    if date_col not in _ALLOWED_DATE_COLS:
        raise ValueError(f"Invalid date_col: {date_col!r}")
    clauses = []
    if start_date:
        clauses.append(f"{date_col} >= :start_date")
        params["start_date"] = start_date
    if end_date:
        clauses.append(f"{date_col} <= :end_date")
        params["end_date"] = end_date
    if invoice_number:
        clauses.append("INVOICE_NUMBER LIKE :invoice_number")
        params["invoice_number"] = f"%{invoice_number}%"
    if outlet_name:
        clauses.append("OUTLET_NAME LIKE :outlet_name")
        params["outlet_name"] = f"%{outlet_name}%"
    if synced is not None:
        clauses.append("SYNCED_TO_ORACLE = :synced")
        params["synced"] = 1 if synced else 0
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


async def query_sales(
    settings: Settings,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    invoice_number: Optional[str] = None,
    outlet_name: Optional[str] = None,
    synced: Optional[bool] = None,
    limit: int = 500,
    offset: int = 0,
) -> Dict[str, Any]:
    path = _get_db_path(settings)
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    where = _build_where(params, start_date, end_date, invoice_number, outlet_name, synced)
    sql = f"SELECT * FROM TEST_BACKUP_VENDHQ_SALES{where} ORDER BY ROW_ID DESC LIMIT :limit OFFSET :offset"
    count_sql = f"SELECT COUNT(*) FROM TEST_BACKUP_VENDHQ_SALES{where}"
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
        async with db.execute(count_sql, count_params) as cur:
            row = await cur.fetchone()
            total = row[0] if row else 0
        async with db.execute(sql, params) as cur:
            rows = await cur.fetchall()
    return {"total": total, "rows": [dict(r) for r in rows]}


async def query_payments(
    settings: Settings,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    invoice_number: Optional[str] = None,
    outlet_name: Optional[str] = None,
    synced: Optional[bool] = None,
    limit: int = 500,
    offset: int = 0,
) -> Dict[str, Any]:
    path = _get_db_path(settings)
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    where = _build_where(params, start_date, end_date, invoice_number, outlet_name, synced)
    sql = f"SELECT * FROM TEST_BACKUP_VENDHQ_PAYMENTS{where} ORDER BY ROW_ID DESC LIMIT :limit OFFSET :offset"
    count_sql = f"SELECT COUNT(*) FROM TEST_BACKUP_VENDHQ_PAYMENTS{where}"
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
        async with db.execute(count_sql, count_params) as cur:
            row = await cur.fetchone()
            total = row[0] if row else 0
        async with db.execute(sql, params) as cur:
            rows = await cur.fetchall()
    return {"total": total, "rows": [dict(r) for r in rows]}


async def query_line_items(
    settings: Settings,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    invoice_number: Optional[str] = None,
    outlet_name: Optional[str] = None,
    synced: Optional[bool] = None,
    limit: int = 500,
    offset: int = 0,
) -> Dict[str, Any]:
    path = _get_db_path(settings)
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    where = _build_where(params, start_date, end_date, invoice_number, outlet_name, synced, date_col="SALE_DATE")
    sql = f"SELECT * FROM TEST_BACKUP_VENDHQ_LINE_ITEMS{where} ORDER BY ROW_ID DESC LIMIT :limit OFFSET :offset"
    count_sql = f"SELECT COUNT(*) FROM TEST_BACKUP_VENDHQ_LINE_ITEMS{where}"
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
        async with db.execute(count_sql, count_params) as cur:
            row = await cur.fetchone()
            total = row[0] if row else 0
        async with db.execute(sql, params) as cur:
            rows = await cur.fetchall()
    return {"total": total, "rows": [dict(r) for r in rows]}


async def mark_synced(settings: Settings, table: str, row_ids: List[int]) -> None:
    if not row_ids:
        return
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table: {table!r}")
    path = _get_db_path(settings)
    placeholders = ",".join("?" * len(row_ids))
    sql = f"UPDATE {table} SET SYNCED_TO_ORACLE = 1 WHERE ROW_ID IN ({placeholders})"
    async with aiosqlite.connect(path) as db:
        await db.execute(sql, row_ids)
        await db.commit()


async def get_unsynced_sales(settings: Settings, batch_size: int = 500) -> List[Dict[str, Any]]:
    path = _get_db_path(settings)
    sql = "SELECT * FROM TEST_BACKUP_VENDHQ_SALES WHERE SYNCED_TO_ORACLE = 0 ORDER BY ROW_ID LIMIT ?"
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (batch_size,)) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_unsynced_payments(settings: Settings, batch_size: int = 500) -> List[Dict[str, Any]]:
    path = _get_db_path(settings)
    sql = "SELECT * FROM TEST_BACKUP_VENDHQ_PAYMENTS WHERE SYNCED_TO_ORACLE = 0 ORDER BY ROW_ID LIMIT ?"
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (batch_size,)) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_unsynced_line_items(settings: Settings, batch_size: int = 500) -> List[Dict[str, Any]]:
    path = _get_db_path(settings)
    sql = "SELECT * FROM TEST_BACKUP_VENDHQ_LINE_ITEMS WHERE SYNCED_TO_ORACLE = 0 ORDER BY ROW_ID LIMIT ?"
    async with aiosqlite.connect(path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, (batch_size,)) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def count_unsynced(settings: Settings) -> Dict[str, int]:
    path = _get_db_path(settings)
    async with aiosqlite.connect(path) as db:
        async with db.execute("SELECT COUNT(*) FROM TEST_BACKUP_VENDHQ_SALES WHERE SYNCED_TO_ORACLE = 0") as cur:
            row = await cur.fetchone()
            sales = row[0] if row else 0
        async with db.execute("SELECT COUNT(*) FROM TEST_BACKUP_VENDHQ_PAYMENTS WHERE SYNCED_TO_ORACLE = 0") as cur:
            row = await cur.fetchone()
            payments = row[0] if row else 0
        async with db.execute("SELECT COUNT(*) FROM TEST_BACKUP_VENDHQ_LINE_ITEMS WHERE SYNCED_TO_ORACLE = 0") as cur:
            row = await cur.fetchone()
            line_items = row[0] if row else 0
    return {"sales": sales, "payments": payments, "line_items": line_items}
