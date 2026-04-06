import asyncio
from contextlib import asynccontextmanager

import oracledb

from .config import Settings

pool_cache: dict[str, oracledb.ConnectionPool] = {}


def _auth_mode(mode_name: str) -> int:
    if mode_name and mode_name.upper() == "SYSDBA":
        return oracledb.AUTH_MODE_SYSDBA
    return oracledb.AUTH_MODE_DEFAULT


def _pool_key(settings: Settings) -> str:
    return f"{settings.oracle_user}@{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"


async def get_pool(settings: Settings) -> oracledb.ConnectionPool:
    key = _pool_key(settings)
    if key in pool_cache:
        return pool_cache[key]

    dsn = oracledb.makedsn(
        settings.oracle_host, settings.oracle_port, service_name=settings.oracle_service
    )

    pool = await asyncio.to_thread(
        oracledb.create_pool,
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=dsn,
        min=1,
        max=4,
        increment=1,
        encoding="UTF-8",
        nencoding="UTF-8",
        homogeneous=True,
        mode=_auth_mode(settings.oracle_mode),
    )
    pool_cache[key] = pool
    return pool


@asynccontextmanager
async def get_connection(settings: Settings) -> oracledb.Connection:
    pool = await get_pool(settings)
    conn = await asyncio.to_thread(pool.acquire)
    try:
        yield conn
    finally:
        await asyncio.to_thread(pool.release, conn)


async def test_connection(settings: Settings) -> bool:
    if not settings.oracle_host or not settings.oracle_service or not settings.oracle_password:
        return False
    try:
        async with get_connection(settings) as conn:
            cursor = await asyncio.to_thread(conn.cursor)
            await asyncio.to_thread(cursor.execute, "SELECT 1 FROM dual")
            await asyncio.to_thread(cursor.fetchone)
            await asyncio.to_thread(cursor.close)
        return True
    except Exception:
        return False
