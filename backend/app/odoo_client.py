from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException, status

from .config import Settings


def _format_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


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

            try:
                response = await client.get(settings.odoo_api_url, headers=headers, params=params)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Odoo API error: {exc.response.text}",
                ) from exc
            except httpx.HTTPError as exc:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Odoo API request failed: {exc}",
                ) from exc

            payload = response.json()
            page_results = payload.get("results") or []
            orders.extend(page_results)

            total = payload.get("total")
            offset += len(page_results)
            if not page_results:
                break
            if total is not None and offset >= total:
                break
            if len(page_results) < limit:
                break

    return orders
