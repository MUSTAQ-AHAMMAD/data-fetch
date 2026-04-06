import asyncio

_cancel_event = asyncio.Event()


def request_cancel() -> None:
    """Signal that the running sync should stop at its next checkpoint."""
    _cancel_event.set()


def reset() -> None:
    """Clear the cancellation flag at the start of a new sync."""
    _cancel_event.clear()


def is_cancelled() -> bool:
    """Return True if a cancellation has been requested."""
    return _cancel_event.is_set()
