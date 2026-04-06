from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from . import cancel as _cancel
from .config import Settings
from .db import describe_target, test_connection
from .local_db import count_unsynced, init_db, query_line_items, query_payments, query_sales
from .push_service import push_to_oracle
from .schemas import (
    HealthResponse,
    LocalDataQuery,
    LocalDataResponse,
    PushRequest,
    PushSummary,
    SyncRequest,
    SyncSummary,
    UnsyncedCount,
)
from .sync_service import sync_orders


def get_settings() -> Settings:
    return Settings()


app = FastAPI(title="POS Order Sync Service", version="2.0.0")


@app.on_event("startup")
async def startup_event():
    settings = Settings()
    await init_db(settings)


def _setup_cors(app: FastAPI, settings: Settings) -> None:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


_setup_cors(app, get_settings())


@app.get("/health", response_model=HealthResponse)
async def health(settings: Settings = Depends(get_settings)) -> HealthResponse:
    oracle_ok = await test_connection(settings)
    return HealthResponse(
        status="ok",
        oracle_connected=oracle_ok,
        oracle_target=describe_target(settings),
        oracle_user=settings.oracle_user,
        odoo_ready=bool(settings.odoo_api_key),
    )


@app.post("/cancel")
async def cancel_sync() -> dict:
    _cancel.request_cancel()
    return {"cancelled": True}


@app.post("/sync", response_model=SyncSummary)
async def trigger_sync(request: SyncRequest, settings: Settings = Depends(get_settings)) -> SyncSummary:
    page_limit = request.limit or settings.page_limit
    summary = await sync_orders(
        settings=settings,
        start_date=request.start_date,
        end_date=request.end_date,
        order_id_gt=request.order_id_gt,
        page_limit=page_limit,
        pos_id=request.pos_id,
        company_id=request.company_id,
    )
    return summary


# ── Local data query endpoints ────────────────────────────────────────────────

@app.get("/local/sales", response_model=LocalDataResponse)
async def local_sales(
    start_date: str = Query(None),
    end_date: str = Query(None),
    invoice_number: str = Query(None),
    outlet_name: str = Query(None),
    synced: bool = Query(None),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
) -> LocalDataResponse:
    result = await query_sales(
        settings,
        start_date=start_date,
        end_date=end_date,
        invoice_number=invoice_number,
        outlet_name=outlet_name,
        synced=synced,
        limit=limit,
        offset=offset,
    )
    return LocalDataResponse(**result)


@app.get("/local/payments", response_model=LocalDataResponse)
async def local_payments(
    start_date: str = Query(None),
    end_date: str = Query(None),
    invoice_number: str = Query(None),
    outlet_name: str = Query(None),
    synced: bool = Query(None),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
) -> LocalDataResponse:
    result = await query_payments(
        settings,
        start_date=start_date,
        end_date=end_date,
        invoice_number=invoice_number,
        outlet_name=outlet_name,
        synced=synced,
        limit=limit,
        offset=offset,
    )
    return LocalDataResponse(**result)


@app.get("/local/line_items", response_model=LocalDataResponse)
async def local_line_items(
    start_date: str = Query(None),
    end_date: str = Query(None),
    invoice_number: str = Query(None),
    outlet_name: str = Query(None),
    synced: bool = Query(None),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    settings: Settings = Depends(get_settings),
) -> LocalDataResponse:
    result = await query_line_items(
        settings,
        start_date=start_date,
        end_date=end_date,
        invoice_number=invoice_number,
        outlet_name=outlet_name,
        synced=synced,
        limit=limit,
        offset=offset,
    )
    return LocalDataResponse(**result)


@app.get("/local/unsynced-count", response_model=UnsyncedCount)
async def local_unsynced_count(settings: Settings = Depends(get_settings)) -> UnsyncedCount:
    counts = await count_unsynced(settings)
    return UnsyncedCount(**counts)


# ── Oracle push endpoints ─────────────────────────────────────────────────────

@app.post("/push", response_model=PushSummary)
async def push_all(request: PushRequest, settings: Settings = Depends(get_settings)) -> PushSummary:
    return await push_to_oracle(settings, tables=request.tables, batch_size=request.batch_size)


@app.post("/push/sales", response_model=PushSummary)
async def push_sales(settings: Settings = Depends(get_settings)) -> PushSummary:
    return await push_to_oracle(settings, tables=["sales"])


@app.post("/push/payments", response_model=PushSummary)
async def push_payments(settings: Settings = Depends(get_settings)) -> PushSummary:
    return await push_to_oracle(settings, tables=["payments"])


@app.post("/push/line_items", response_model=PushSummary)
async def push_line_items(settings: Settings = Depends(get_settings)) -> PushSummary:
    return await push_to_oracle(settings, tables=["line_items"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
