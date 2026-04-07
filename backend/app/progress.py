"""Module-level fetch progress tracking (single-sync model)."""

from typing import Optional

_state: dict = {
    "status": "idle",   # idle | fetching | storing | done | error
    "fetched": 0,
    "total": None,      # None means total not yet known
    "error": None,
}


def reset() -> None:
    """Clear progress state at the start of a new sync."""
    _state.update(status="idle", fetched=0, total=None, error=None)


def start_fetch(total: Optional[int] = None) -> None:
    """Mark that the fetch phase has begun; optionally record expected total."""
    _state.update(status="fetching", fetched=0, total=total, error=None)


def update_fetched(count: int) -> None:
    """Update the running count of records fetched so far."""
    _state["fetched"] = count


def set_total(total: int) -> None:
    """Update the expected total (e.g. once the API reports it)."""
    _state["total"] = total


def start_storing() -> None:
    """Mark that all pages have been fetched and writing to DB has begun."""
    _state["status"] = "storing"


def done() -> None:
    """Mark the sync as successfully completed."""
    _state["status"] = "done"


def error(message: str) -> None:
    """Mark the sync as failed with an error message."""
    _state["status"] = "error"
    _state["error"] = message


def get_state() -> dict:
    """Return a snapshot of the current progress state."""
    return dict(_state)
