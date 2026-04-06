from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import cancel as _cancel
from .config import Settings
from .db import describe_target, test_connection
from .schemas import HealthResponse, SyncRequest, SyncSummary
from .sync_service import sync_orders


def get_settings() -> Settings:
    return Settings()


app = FastAPI(title="POS Order Sync Service", version="1.0.0")


@app.on_event("startup")
async def startup_event():
    # Warm up settings and optionally test external systems
    Settings()


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
