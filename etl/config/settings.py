"""
Pharma DR · ETL Configuration
All connection strings, paths, and parameters loaded from environment variables.
"""

import os
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── PostgreSQL ────────────────────────────────────────────
    postgres_host:     str = Field(default="localhost")
    postgres_port:     int = Field(default=5432)
    postgres_db:       str = Field(default="pharma_dr")
    postgres_user:     str = Field(default="pharma_admin")
    postgres_password: str = Field(default="")
    postgres_schema_dw:      str = Field(default="dw")
    postgres_schema_staging: str = Field(default="staging")
    postgres_schema_mart:    str = Field(default="mart")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_dsn_async(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # ── SAP HANA ──────────────────────────────────────────────
    sap_hana_host:          str   = Field(default="")
    sap_hana_port:          int   = Field(default=30015)
    sap_hana_user:          str   = Field(default="")
    sap_hana_password:      str   = Field(default="")
    sap_hana_schema:        str   = Field(default="SALES_SCHEMA")
    sap_hana_table_sales:   str   = Field(default="VBAP_ENRICHED")
    sap_hana_table_clients: str   = Field(default="KNA1_DR")
    sap_hana_table_products:str   = Field(default="MARA_DR")
    sap_hana_watermark_col: str   = Field(default="_CHANGED_AT")
    sap_hana_batch_size:    int   = Field(default=5000)

    # ── SQL Server ────────────────────────────────────────────
    sqlsrv_host:     str = Field(default="")
    sqlsrv_port:     int = Field(default=1433)
    sqlsrv_db:       str = Field(default="HistoricalSales")
    sqlsrv_user:     str = Field(default="")
    sqlsrv_password: str = Field(default="")
    sqlsrv_driver:   str = Field(default="{ODBC Driver 18 for SQL Server}")

    @property
    def sqlsrv_connection_string(self) -> str:
        return (
            f"DRIVER={self.sqlsrv_driver};"
            f"SERVER={self.sqlsrv_host},{self.sqlsrv_port};"
            f"DATABASE={self.sqlsrv_db};"
            f"UID={self.sqlsrv_user};"
            f"PWD={self.sqlsrv_password};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )

    # ── Excel ─────────────────────────────────────────────────
    excel_landing_path:   str = Field(default="data/excel_samples")
    excel_processed_path: str = Field(default="data/excel_processed")
    excel_rejected_path:  str = Field(default="data/excel_rejected")

    # ── ETL Runtime ───────────────────────────────────────────
    etl_log_level:          str = Field(default="INFO")
    etl_log_path:           str = Field(default="data/logs")
    etl_watermark_path:     str = Field(default="data/watermarks")
    etl_max_retry:          int = Field(default=3)
    etl_retry_delay_seconds:int = Field(default=30)

    # ── Notifications ─────────────────────────────────────────
    smtp_host:          str = Field(default="")
    smtp_port:          int = Field(default=587)
    smtp_user:          str = Field(default="")
    smtp_password:      str = Field(default="")
    alert_recipients:   str = Field(default="")

    @property
    def alert_recipient_list(self) -> list[str]:
        return [r.strip() for r in self.alert_recipients.split(",") if r.strip()]

    # ── Environment ───────────────────────────────────────────
    environment: str = Field(default="development")

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


# Singleton settings instance
settings = Settings()

# Ensure required directories exist
for path_attr in ("etl_log_path", "etl_watermark_path", "excel_processed_path", "excel_rejected_path"):
    Path(getattr(settings, path_attr)).mkdir(parents=True, exist_ok=True)
