from pathlib import Path
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    odoo_api_url: str = "https://ibrahimalquraishieu-26-2-26-29083802.dev.odoo.com/api/pos/order"
    odoo_api_key: str = ""
    odoo_order_min_id: int = 5525874
    oracle_host: str = ""
    oracle_port: int = 1521
    oracle_service: str = ""
    oracle_user: str = "SYS"
    oracle_password: str = ""
    oracle_mode: str = "SYSDBA"
    # Path to Oracle Instant Client directory (enables Thick mode).
    # Leave empty to use Thin mode (pure Python, no client install needed).
    # Example for Windows: C:\oracle\instantclient_21_14
    oracle_client_lib: str = ""
    allowed_origins: List[str] = ["http://localhost:5173"]
    region: str = "SA"
    request_timeout_seconds: float = 30.0
    page_limit: int = 100

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
