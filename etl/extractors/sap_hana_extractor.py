"""
Pharma DR · SAP HANA Extractor
================================
Incremental extraction using timestamp-based CDC (_CHANGED_AT watermark).
Supports:
  - Sales transactions (VBAP_ENRICHED)
  - Client master data (KNA1_DR)
  - Product master data (MARA_DR)

In production: requires hdbcli (SAP HANA Client Python driver).
For demo/dev: returns synthetic data if SAP_HANA_HOST is not configured.
"""

import json
from datetime import datetime, timezone
from typing import Iterator, Optional

import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed

from etl.config.settings import settings


class SapHanaExtractor:
    """
    Incremental extractor for SAP HANA.

    Extraction strategy:
      1. Read current watermark from audit.watermarks (or file fallback)
      2. Query HANA with: WHERE _CHANGED_AT > :watermark ORDER BY _CHANGED_AT
      3. Yield batches of `batch_size` records
      4. After successful load, update watermark to max(_CHANGED_AT) in this run
    """

    def __init__(self):
        self._conn = None
        self._demo_mode = not bool(settings.sap_hana_host)

    def _get_connection(self):
        """Establish HANA connection (lazy). Uses hdbcli in production."""
        if self._conn is not None:
            return self._conn
        if self._demo_mode:
            logger.warning("SAP HANA host not configured — running in DEMO MODE (synthetic data)")
            return None
        try:
            import hdbcli.dbapi as hdbcli   # type: ignore  # noqa: PLC0415
            self._conn = hdbcli.connect(
                address=settings.sap_hana_host,
                port=settings.sap_hana_port,
                user=settings.sap_hana_user,
                password=settings.sap_hana_password,
                autocommit=True,
            )
            logger.info("SAP HANA connection established: {}:{}", settings.sap_hana_host, settings.sap_hana_port)
            return self._conn
        except ImportError:
            logger.error("hdbcli not installed. Install SAP HANA Client or run in demo mode.")
            raise
        except Exception as exc:
            logger.error("SAP HANA connection failed: {}", exc)
            raise

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Watermark Management ────────────────────────────────────

    def get_watermark(self, source_system: str = "SAP_HANA") -> datetime:
        """Read last successful watermark from PostgreSQL audit table."""
        import sqlalchemy as sa
        engine = sa.create_engine(settings.postgres_dsn)
        with engine.connect() as conn:
            result = conn.execute(
                sa.text("SELECT last_watermark FROM audit.watermarks WHERE source_system = :s"),
                {"s": source_system},
            ).fetchone()
        if result:
            return result[0]
        return datetime(2018, 1, 1, tzinfo=timezone.utc)

    def set_watermark(self, source_system: str, watermark: datetime) -> None:
        """Update watermark after successful extraction."""
        import sqlalchemy as sa
        engine = sa.create_engine(settings.postgres_dsn)
        with engine.begin() as conn:
            conn.execute(
                sa.text("""
                    INSERT INTO audit.watermarks(source_system, last_watermark, updated_at)
                    VALUES (:s, :w, NOW())
                    ON CONFLICT (source_system) DO UPDATE
                    SET last_watermark = :w, updated_at = NOW()
                """),
                {"s": source_system, "w": watermark},
            )
        logger.info("Watermark updated: {} → {}", source_system, watermark.isoformat())

    # ── Sales Extraction ────────────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
    def extract_sales(
        self,
        watermark_from: Optional[datetime] = None,
        batch_size: int = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Extract incremental sales from SAP HANA VBAP_ENRICHED view.

        Yields pandas DataFrames of batch_size rows each.

        SAP HANA query (production):
          SELECT
            VBELN, POSNR, ERDAT, MATNR, KUNNR, KWMENG, NETPR, NETWR,
            MWSBP, BONBA, WERKS, _CHANGED_AT
          FROM {schema}.{table}
          WHERE _CHANGED_AT > :watermark
          ORDER BY _CHANGED_AT ASC
          LIMIT :batch_size OFFSET :offset
        """
        if watermark_from is None:
            watermark_from = self.get_watermark("SAP_HANA")

        batch_size = batch_size or settings.sap_hana_batch_size
        logger.info("SAP HANA extraction: watermark={}", watermark_from.isoformat())

        if self._demo_mode:
            yield from self._extract_sales_demo(watermark_from)
            return

        conn = self._get_connection()
        sql = f"""
            SELECT
                VBELN           AS invoice_number,
                POSNR           AS invoice_line,
                ERDAT           AS sale_date,
                MATNR           AS sap_product_id,
                KUNNR           AS sap_client_id,
                KWMENG          AS quantity,
                NETPR           AS unit_price,
                NETWR           AS net_amount,
                MWSBP           AS tax_amount,
                WERKS           AS plant_code,
                VKORG           AS sales_org,
                VTWEG           AS dist_channel,
                SPART           AS division,
                KURSK           AS exchange_rate,
                _CHANGED_AT     AS changed_at
            FROM {settings.sap_hana_schema}.{settings.sap_hana_table_sales}
            WHERE _CHANGED_AT > '{watermark_from.isoformat()}'
            ORDER BY _CHANGED_AT ASC
        """
        offset = 0
        max_changed_at = watermark_from

        while True:
            paginated_sql = f"{sql} LIMIT {batch_size} OFFSET {offset}"
            cursor = conn.cursor()
            cursor.execute(paginated_sql)
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                break

            cols = [
                "invoice_number","invoice_line","sale_date","sap_product_id",
                "sap_client_id","quantity","unit_price","net_amount","tax_amount",
                "plant_code","sales_org","dist_channel","division","exchange_rate",
                "changed_at",
            ]
            df = pd.DataFrame(rows, columns=cols)
            df["source_system"] = "SAP_HANA"
            df["extracted_at"] = datetime.now(tz=timezone.utc)

            max_changed_at = max(max_changed_at, df["changed_at"].max())
            logger.info("SAP HANA batch: offset={}, rows={}", offset, len(df))
            yield df

            if len(rows) < batch_size:
                break
            offset += batch_size

        self.set_watermark("SAP_HANA", max_changed_at)

    def _extract_sales_demo(self, watermark_from: datetime) -> Iterator[pd.DataFrame]:
        """Demo mode: load from synthetic CSV file."""
        from pathlib import Path
        csv_path = Path("data/synthetic/sales.csv")
        if not csv_path.exists():
            logger.warning("Demo CSV not found: {}. Run generate_data.py first.", csv_path)
            return

        df = pd.read_csv(csv_path, parse_dates=["full_date"])
        df_filtered = df[
            (df["source_system"] == "SAP_HANA") &
            (pd.to_datetime(df["full_date"]) > pd.Timestamp(watermark_from))
        ]
        logger.info("SAP HANA DEMO: {} rows available after watermark", len(df_filtered))
        chunk_size = settings.sap_hana_batch_size
        for start in range(0, len(df_filtered), chunk_size):
            chunk = df_filtered.iloc[start:start + chunk_size].copy()
            chunk["extracted_at"] = datetime.now(tz=timezone.utc)
            yield chunk

    # ── Client Master Extraction ────────────────────────────────

    def extract_clients(self) -> pd.DataFrame:
        """Full extract of client master data (KNA1_DR)."""
        if self._demo_mode:
            from pathlib import Path
            csv_path = Path("data/synthetic/clients.csv")
            if csv_path.exists():
                return pd.read_csv(csv_path)
            logger.warning("Demo clients CSV not found.")
            return pd.DataFrame()

        conn = self._get_connection()
        sql = f"""
            SELECT
                KUNNR   AS sap_client_id,
                NAME1   AS client_name,
                ORT01   AS city_name,
                REGIO   AS region_code,
                TELF1   AS phone,
                SMTP_ADDR AS email,
                LOEVM   AS delete_flag
            FROM {settings.sap_hana_schema}.{settings.sap_hana_table_clients}
            WHERE LAND1 = 'DO'
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        cols = ["sap_client_id","client_name","city_name","region_code","phone","email","delete_flag"]
        return pd.DataFrame(rows, columns=cols)

    # ── Product Master Extraction ───────────────────────────────

    def extract_products(self) -> pd.DataFrame:
        """Full extract of product master data (MARA_DR)."""
        if self._demo_mode:
            import json
            from pathlib import Path
            # Build from embedded product list
            from etl.transformers.normalizer import PRODUCTS_REFERENCE
            return pd.DataFrame(PRODUCTS_REFERENCE)

        conn = self._get_connection()
        sql = f"""
            SELECT
                MATNR   AS sap_product_id,
                MAKTX   AS product_name,
                MATKL   AS material_group,
                MEINS   AS base_uom,
                NTGEW   AS net_weight,
                ZEINR   AS drawing_number,
                LVORM   AS delete_flag
            FROM {settings.sap_hana_schema}.{settings.sap_hana_table_products}
        """
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        cols = ["sap_product_id","product_name","material_group","base_uom","net_weight","drawing_number","delete_flag"]
        return pd.DataFrame(rows, columns=cols)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
