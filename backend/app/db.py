import asyncio
import logging
import threading
from contextlib import asynccontextmanager

import oracledb

from .config import Settings

pool_cache: dict[str, oracledb.ConnectionPool] = {}
_thick_mode_initialized = False
_thick_mode_lock = threading.Lock()

logger = logging.getLogger(__name__)


def _ensure_thick_mode(client_lib: str) -> None:
    """Initialize oracledb Thick mode once if a client library path is provided.

    Thick mode requires Oracle Instant Client to be installed on the machine.
    When *client_lib* is an empty string the library stays in the default
    Thin mode (pure-Python, no client installation needed).
    """
    global _thick_mode_initialized
    if not client_lib:
        return
    with _thick_mode_lock:
        if _thick_mode_initialized:
            return
        try:
            oracledb.init_oracle_client(lib_dir=client_lib)
            _thick_mode_initialized = True
            logger.info("oracledb running in Thick mode using client at: %s", client_lib)
        except oracledb.ProgrammingError:
            # Already initialized (e.g. reloaded module) – that is fine.
            _thick_mode_initialized = True
        except Exception as exc:
            logger.warning(
                "Could not initialize Oracle Thick client from '%s': %s. "
                "Falling back to Thin mode.",
                client_lib,
                exc,
            )


def _auth_mode(mode_name: str) -> int:
    if mode_name and mode_name.upper() == "SYSDBA":
        return oracledb.AUTH_MODE_SYSDBA
    return oracledb.AUTH_MODE_DEFAULT


def _pool_key(settings: Settings) -> str:
    return f"{settings.oracle_user}@{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"


def describe_target(settings: Settings) -> str:
    if not settings.oracle_host or not settings.oracle_service:
        return "not configured"
    return f"{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"


async def get_pool(settings: Settings) -> oracledb.ConnectionPool:
    _ensure_thick_mode(settings.oracle_client_lib)

    key = _pool_key(settings)
    if key in pool_cache:
        return pool_cache[key]

    dsn = f"{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"

    create_pool_kwargs: dict = dict(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=dsn,
        min=1,
        max=4,
        increment=1,
        homogeneous=True,
        mode=_auth_mode(settings.oracle_mode),
    )

    # encoding / nencoding are only valid in Thick mode; omit them for Thin
    # mode to avoid a TypeError from python-oracledb.
    if settings.oracle_client_lib:
        create_pool_kwargs["encoding"] = "UTF-8"
        create_pool_kwargs["nencoding"] = "UTF-8"

    pool = await asyncio.to_thread(oracledb.create_pool, **create_pool_kwargs)
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
