"""Module-level fetch progress tracking (single-sync model)."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_state: dict = {
    "status": "idle",        # idle | fetching | storing | done | error
    "fetched": 0,
    "total": None,           # None means total not yet known
    "error": None,
    # DB insert progress (populated during 'storing' phase)
    "store_total": None,     # total rows to insert across all tables
    "store_completed": 0,    # rows inserted so far
    "store_current_table": None,  # 'sales' | 'payments' | 'line_items' | None
}


def reset() -> None:
    """Clear progress state at the start of a new sync."""
    _state.update(
        status="idle",
        fetched=0,
        total=None,
        error=None,
        store_total=None,
        store_completed=0,
        store_current_table=None,
    )


def start_fetch(total: Optional[int] = None) -> None:
    """Mark that the fetch phase has begun; optionally record expected total."""
    _state.update(status="fetching", fetched=0, total=total, error=None)


def update_fetched(count: int) -> None:
    """Update the running count of records fetched so far.

    If the API-reported total is known, the counter is capped at that value so
    the progress display never shows a fetched count that exceeds the total
    (which can happen when the server ignores the requested page-size limit and
    returns larger pages than expected, causing parallel pages to overlap before
    deduplication).
    """
    total = _state.get("total")
    if total is not None and count > total:
        logger.debug(
            "Capped fetched count from %d to total %d (API page size exceeds reported total).",
            count,
            total,
        )
        count = total
    _state["fetched"] = count


def set_total(total: int) -> None:
    """Update the expected total (e.g. once the API reports it)."""
    _state["total"] = total


def start_storing(store_total: Optional[int] = None) -> None:
    """Mark that all pages have been fetched and writing to DB has begun.

    Args:
        store_total: Total number of rows that will be inserted across all tables.
    """
    _state.update(
        status="storing",
        store_total=store_total,
        store_completed=0,
        store_current_table=None,
    )


def update_store_table(table_name: Optional[str]) -> None:
    """Set the table currently being inserted (e.g. 'sales', 'payments', 'line_items')."""
    _state["store_current_table"] = table_name


def update_store_completed(completed: int) -> None:
    """Update the cumulative count of rows already inserted into the local DB."""
    _state["store_completed"] = completed


def done() -> None:
    """Mark the sync as successfully completed."""
    _state["status"] = "done"
    _state["store_current_table"] = None


def error(message: str) -> None:
    """Mark the sync as failed with an error message."""
    _state["status"] = "error"
    _state["error"] = message
    _state["store_current_table"] = None


def get_state() -> dict:
    """Return a snapshot of the current progress state."""
    return dict(_state)
