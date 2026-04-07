import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import HTTPException, status

from . import cancel as _cancel
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
            total = payload[key]
            break

    return results, total


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
    offset = 0
    orders: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds) as client:
        while True:
            if _cancel.is_cancelled():
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Sync cancelled during order fetch.",
                )
            params: Dict[str, Any] = {
                "start_date": _format_date(start_date),
                "end_date": _format_date(end_date),
                "order_by": "id ASC",
                "limit": limit,
                "offset": offset,
            }
            if order_id_gt is not None:
                params["order_id"] = f">{order_id_gt}"
            if pos_id is not None:
                params["pos_id"] = pos_id
            if company_id is not None:
                params["company_id"] = company_id

            # Build a prepared request so we can log the final URL (with
            # percent-encoded query string) before sending it.
            request = client.build_request(
                "GET", settings.odoo_api_url, headers=headers, params=params
            )
            logger.info(
                "Fetching Odoo orders: url=%s",
                request.url,
            )

            try:
                response = await client.send(request)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Odoo API error {exc.response.status_code}: {exc.response.text}",
                ) from exc
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Odoo API request failed: {exc}",
                ) from exc

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

            if not page_results and offset == 0:
                # Log the full response body (truncated) so operators can see
                # exactly what Odoo returned and why no records came back.
                raw_body = response.text[:2000]
                logger.warning(
                    "Odoo returned zero results for date range %s – %s. "
                    "Request URL: %s | Response (first 2000 chars): %s",
                    _format_date(start_date),
                    _format_date(end_date),
                    request.url,
                    raw_body,
                )

            orders.extend(page_results)

            offset += len(page_results)
            if not page_results:
                break
            if total is not None and offset >= total:
                break
            if len(page_results) < limit:
                break

    logger.info(
        "Odoo fetch complete: %d orders collected (%s – %s)",
        len(orders),
        _format_date(start_date),
        _format_date(end_date),
    )
    return orders
