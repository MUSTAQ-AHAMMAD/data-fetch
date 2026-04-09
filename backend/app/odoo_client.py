import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import HTTPException, status

from . import cancel as _cancel
from . import progress as _progress
from .config import Settings

logger = logging.getLogger(__name__)


def _format_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _extract_results(payload: Any) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Extract the results list and total count from an Odoo API response.

    Handles multiple common response shapes:
    - {"results": [...], "total": N}            - original custom REST format
    - {"records": [...], "length": N}            - Odoo 17+ standard REST
    - {"result": {"results": [...], "total": N}} - JSON-RPC wrapper with object
    - {"result": [...]}                          - JSON-RPC wrapper with direct array
    - [...]                                      - direct array response
    """
    # Direct array response
    if isinstance(payload, list):
        return payload, len(payload)

    # Unwrap JSON-RPC {"result": ...} wrapper if present (and no top-level error)
    if "result" in payload and "error" not in payload:
        inner = payload["result"]
        if isinstance(inner, list):
            return inner, len(inner)
        if isinstance(inner, dict):
            payload = inner

    # Detect and surface API-level error messages
    if "error" in payload:
        error = payload["error"]
        if isinstance(error, dict):
            data = error.get("data") or {}
            msg = error.get("message") or data.get("message") or str(error)
        else:
            msg = str(error)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Odoo API returned an error: {msg}",
        )

    # Use explicit key presence checks so that an empty list ([]) or zero count
    # stored under the first key is not mistakenly skipped.
    results: List[Dict[str, Any]] = []
    for key in ("results", "records", "data"):
        if key in payload:
            results = payload[key] or []
            break

    total: Optional[int] = None
    for key in ("total", "length", "count"):
        if key in payload:
            raw = payload[key]
            try:
                total = int(raw)
            except (TypeError, ValueError):
                total = None
            break

    return results, total


async def _fetch_page(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    base_params: Dict[str, Any],
    offset: int,
    limit: int,
    max_retries: int = 3,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Fetch a single page from the Odoo API with retry-with-backoff.

    Returns (records, total) where *total* may be None if the API does not
    report it.  Raises HTTPException on unrecoverable errors.
    """
    if _cancel.is_cancelled():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Sync cancelled during order fetch.",
        )

    params = {**base_params, "offset": offset, "limit": limit}
    request = client.build_request("GET", url, headers=headers, params=params)
    logger.info("Fetching Odoo orders: url=%s", request.url)

    response: Optional[httpx.Response] = None
    for attempt in range(max_retries):
        try:
            response = await client.send(request)
            response.raise_for_status()
            break
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code >= 500 and attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "Odoo API returned %s on attempt %d/%d; retrying in %ds. URL: %s",
                    exc.response.status_code,
                    attempt + 1,
                    max_retries,
                    wait,
                    request.url,
                )
                await asyncio.sleep(wait)
                continue
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Odoo API error {exc.response.status_code}: {exc.response.text}",
            ) from exc
        except httpx.HTTPError as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning(
                    "Odoo API request failed on attempt %d/%d; retrying in %ds. Error: %s",
                    attempt + 1,
                    max_retries,
                    wait,
                    exc,
                )
                await asyncio.sleep(wait)
                continue
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Odoo API request failed: {exc}",
            ) from exc

    if response is None:  # pragma: no cover — all retries raised above
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Odoo API request failed: no response received.",
        )

    payload = response.json()
    try:
        page_results, total = _extract_results(payload)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(
            "Unexpected Odoo response structure. Keys: %s. Error: %s",
            list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Unexpected Odoo API response format: {exc}",
        ) from exc

    return page_results, total


async def fetch_orders(
    settings: Settings,
    start_date: datetime,
    end_date: datetime,
    order_id_gt: Optional[int],
    page_limit: Optional[int],
    pos_id: Optional[int] = None,
    company_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    headers = {"x-api-key": settings.odoo_api_key}
    limit = page_limit or settings.page_limit
    max_concurrent = settings.max_concurrent_pages

    base_params: Dict[str, Any] = {
        "start_date": _format_date(start_date),
        "end_date": _format_date(end_date),
        "order": "id asc",
    }
    if pos_id is not None:
        base_params["pos_id"] = pos_id
    if company_id is not None:
        base_params["company_id"] = company_id
    # Apply the order-ID floor so the API only returns orders above the given
    # threshold.  Use the explicit per-request value when provided; fall back to
    # the configured default minimum order ID so that stale/already-synced
    # orders are never re-fetched unless the caller explicitly asks for them.
    effective_order_id_gt = order_id_gt if order_id_gt is not None else settings.odoo_order_min_id
    if effective_order_id_gt:
        base_params["order_id_gt"] = effective_order_id_gt

    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds) as client:
        # ── Page 0: always fetched first so we can read the total count ──
        page0_results, total = await _fetch_page(
            client, settings.odoo_api_url, headers, base_params, offset=0, limit=limit
        )

        if not page0_results:
            # Log the zero-result case (mirrors old behaviour)
            logger.warning(
                "Odoo returned zero results for date range %s – %s (reported total=%s).",
                _format_date(start_date),
                _format_date(end_date),
                total,
            )
            return []

        # Initialise progress tracking now that we have the expected total.
        _progress.start_fetch(total)
        orders: List[Dict[str, Any]] = list(page0_results)
        _progress.update_fetched(len(orders))

        # The API may return more records per page than the requested `limit`
        # (e.g. it has a minimum or fixed page size of 1000).  Use the actual
        # number of records returned on page 0 as the effective page size for
        # all subsequent offset calculations so pages don't overlap.
        actual_page_size = len(page0_results)

        # ── Early exit: everything fit in the first page ──
        # When the API reports a total, trust it: only exit if we already
        # have all (or more) records.  The partial-page heuristic
        # (actual_page_size < limit) must NOT be used when total is known
        # because the first page can legitimately be partial even when
        # thousands of additional records exist on subsequent pages (e.g. the
        # API enforces a different internal page size or the first page
        # straddles a date-boundary resulting in fewer rows than the limit).
        if total is not None:
            should_stop = actual_page_size >= total
        else:
            # No total reported — a partial first page means we've hit the end.
            should_stop = actual_page_size < limit

        if should_stop:
            logger.info(
                "Odoo fetch complete (single page): %d orders (%s – %s)",
                len(orders),
                _format_date(start_date),
                _format_date(end_date),
            )
            return orders

        # ── Cursor-based sequential pagination ───────────────────────────────
        # The API does not reliably support offset-based pagination; requesting
        # offset=N often returns the same first page for any N.  Instead we
        # advance the `order_id_gt` cursor to the maximum order_id seen so far
        # and always request offset=0.  Because orders are returned with
        # `order=id asc`, the last record on every page has the highest id,
        # making it the correct cursor for the next page.
        cursor_params = {**base_params}

        # Seed the cursor from page 0 (already in orders).  We track it
        # incrementally so we never re-scan the whole list on each iteration.
        cursor_order_id = None
        for wrapper in reversed(page0_results):
            oid = (wrapper.get("order") or {}).get("order_id")
            if oid is not None:
                cursor_order_id = oid
                break

        while cursor_order_id is not None:
            if _cancel.is_cancelled():
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Sync cancelled during order fetch.",
                )

            cursor_params["order_id_gt"] = cursor_order_id
            logger.info(
                "Cursor-based fetch: order_id_gt=%s, collected=%d so far",
                cursor_order_id,
                len(orders),
            )

            page_results, _ = await _fetch_page(
                client, settings.odoo_api_url, headers, cursor_params,
                offset=0, limit=limit,
            )

            if not page_results:
                break

            # Advance cursor to the last (highest) order_id on this page.
            for wrapper in reversed(page_results):
                oid = (wrapper.get("order") or {}).get("order_id")
                if oid is not None:
                    cursor_order_id = oid
                    break

            orders.extend(page_results)
            _progress.update_fetched(len(orders))

            if len(page_results) < limit:
                break

        if cursor_order_id is None:
            logger.warning(
                "Cannot determine pagination cursor (no order_id found in page 0); "
                "only the first page of %d records will be stored.",
                len(orders),
            )

    # ── Deduplicate by order_id ──────────────────────────────────────────────
    # Offset-based parallel pagination can yield duplicate records when the
    # server-side total is inaccurate or new records are inserted mid-fetch,
    # shifting page boundaries.  De-duplicating here ensures (a) the reported
    # orders_fetched count is accurate and (b) we never inflate storage counts.
    # Orders without an order_id are malformed and skipped entirely.
    seen_ids: set = set()
    unique_orders: List[Dict[str, Any]] = []
    skipped_no_id = 0
    for order_wrapper in orders:
        order = order_wrapper.get("order") or {}
        order_id = order.get("order_id")
        if order_id is None:
            skipped_no_id += 1
            continue
        if order_id not in seen_ids:
            seen_ids.add(order_id)
            unique_orders.append(order_wrapper)
    duplicate_count = len(orders) - len(unique_orders) - skipped_no_id
    if skipped_no_id:
        logger.warning(
            "Skipped %d orders with missing order_id during deduplication.",
            skipped_no_id,
        )
    if duplicate_count:
        logger.warning(
            "Removed %d duplicate orders during deduplication (raw=%d, unique=%d).",
            duplicate_count,
            len(orders),
            len(unique_orders),
        )
    orders = unique_orders

    # ── Count verification ───────────────────────────────────────────────────
    # Compare the number of unique orders collected against the total that the
    # API reported on page 0.  A mismatch is logged as a warning so that it is
    # always visible in the logs, even when the sync otherwise succeeds.
    if total is not None and len(orders) != total:
        logger.warning(
            "Count mismatch after deduplication: API reported total=%d but collected %d unique orders "
            "(%s – %s). This may indicate records were added/removed mid-fetch or the API total was inaccurate.",
            total,
            len(orders),
            _format_date(start_date),
            _format_date(end_date),
        )
    elif total is not None:
        logger.info(
            "Count verified: collected %d unique orders matches API reported total=%d (%s – %s).",
            len(orders),
            total,
            _format_date(start_date),
            _format_date(end_date),
        )

    logger.info(
        "Odoo fetch complete: %d unique orders collected (%s – %s)",
        len(orders),
        _format_date(start_date),
        _format_date(end_date),
    )
    return orders

