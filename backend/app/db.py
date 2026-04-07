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
    if mode_name:
        upper = mode_name.upper()
        if upper == "SYSDBA":
            return oracledb.AUTH_MODE_SYSDBA
        if upper == "SYSOPER":
            return oracledb.AUTH_MODE_SYSOPER
    return oracledb.AUTH_MODE_DEFAULT


def _is_privileged_mode(mode_name: str) -> bool:
    return bool(mode_name) and mode_name.upper() in ("SYSDBA", "SYSOPER")


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
    mode_name = settings.oracle_mode or "DEFAULT"
    logger.info(
        "Creating Oracle connection pool: dsn=%s user=%s auth_mode=%s thick_mode=%s",
        dsn,
        settings.oracle_user,
        mode_name,
        bool(settings.oracle_client_lib),
    )

    create_pool_kwargs: dict = dict(
        user=settings.oracle_user,
        password=settings.oracle_password,
        dsn=dsn,
        # min=0 avoids eager connection at pool creation time; connections are
        # established on first acquire instead, preventing startup failures when
        # the DB is temporarily unreachable.
        min=0,
        max=4,
        increment=1,
        homogeneous=True,
        mode=_auth_mode(settings.oracle_mode),
    )

    try:
        pool = await asyncio.to_thread(oracledb.create_pool, **create_pool_kwargs)
        pool_cache[key] = pool
        logger.info("Oracle connection pool created successfully: %s", key)
        return pool
    except Exception as exc:
        logger.error(
            "Failed to create Oracle connection pool [dsn=%s user=%s mode=%s]: %s",
            dsn,
            settings.oracle_user,
            mode_name,
            exc,
            exc_info=True,
        )
        raise


@asynccontextmanager
async def get_connection(settings: Settings) -> oracledb.Connection:
    # SYSDBA / SYSOPER connections do not work reliably through a session pool in
    # Thick mode (the auth privilege is not always propagated to pooled sessions).
    # Use a direct (non-pooled) connection for privileged modes instead.
    if _is_privileged_mode(settings.oracle_mode):
        _ensure_thick_mode(settings.oracle_client_lib)
        dsn = f"{settings.oracle_host}:{settings.oracle_port}/{settings.oracle_service}"
        logger.debug(
            "Creating direct Oracle connection (privileged mode=%s): dsn=%s user=%s",
            settings.oracle_mode,
            dsn,
            settings.oracle_user,
        )
        conn = await asyncio.to_thread(
            oracledb.connect,
            user=settings.oracle_user,
            password=settings.oracle_password,
            dsn=dsn,
            mode=_auth_mode(settings.oracle_mode),
        )
        try:
            yield conn
        finally:
            await asyncio.to_thread(conn.close)
    else:
        pool = await get_pool(settings)
        conn = await asyncio.to_thread(pool.acquire)
        try:
            yield conn
        finally:
            await asyncio.to_thread(pool.release, conn)


async def test_connection(settings: Settings) -> bool:
    if not settings.oracle_host or not settings.oracle_service or not settings.oracle_password:
        logger.debug(
            "Oracle connection skipped – missing config: host=%r service=%r password_set=%s",
            settings.oracle_host,
            settings.oracle_service,
            bool(settings.oracle_password),
        )
        return False
    try:
        async with get_connection(settings) as conn:
            cursor = await asyncio.to_thread(conn.cursor)
            await asyncio.to_thread(cursor.execute, "SELECT 1 FROM dual")
            await asyncio.to_thread(cursor.fetchone)
            await asyncio.to_thread(cursor.close)
        logger.info("Oracle connection test succeeded: %s", _pool_key(settings))
        return True
    except Exception as exc:
        logger.error(
            "Oracle connection test FAILED [host=%s port=%s service=%s user=%s mode=%s]: %s",
            settings.oracle_host,
            settings.oracle_port,
            settings.oracle_service,
            settings.oracle_user,
            settings.oracle_mode,
            exc,
            exc_info=True,
        )
        return False
