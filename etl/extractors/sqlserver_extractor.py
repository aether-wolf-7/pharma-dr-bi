"""
Pharma DR · SQL Server Historical Data Extractor
==================================================
One-time migration + optional monthly delta of historical sales data.
Requires: pyodbc + ODBC Driver 18 for SQL Server

Demo mode: loads from synthetic CSV when SQLSRV_HOST is not configured.
"""

from datetime import datetime, timezone
from typing import Iterator, Optional

import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed

from etl.config.settings import settings


class SqlServerExtractor:
    """
    Historical sales extractor from SQL Server.

    Production tables assumed:
      - dbo.VentasHistoricas      — main historical transactions
      - dbo.ClientesMaestro       — client master
      - dbo.ProductosMaestro      — product master (legacy codes)
    """

    def __init__(self):
        self._conn = None
        self._demo_mode = not bool(settings.sqlsrv_host)

    def _get_connection(self):
        if self._conn is not None:
            return self._conn
        if self._demo_mode:
            logger.warning("SQL Server not configured — DEMO MODE active")
            return None
        try:
            import pyodbc  # type: ignore  # noqa: PLC0415
            self._conn = pyodbc.connect(settings.sqlsrv_connection_string, timeout=30)
            self._conn.autocommit = True
            logger.info("SQL Server connected: {}:{}", settings.sqlsrv_host, settings.sqlsrv_port)
            return self._conn
        except ImportError:
            logger.error("pyodbc not installed. pip install pyodbc")
            raise
        except Exception as exc:
            logger.error("SQL Server connection failed: {}", exc)
            raise

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Full Historical Migration ────────────────────────────────

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
    def extract_historical_sales(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        batch_size: int = 10000,
    ) -> Iterator[pd.DataFrame]:
        """
        Extract historical sales in batches.

        SQL Server query pattern (production):
          SELECT TOP :batch_size
            VentaID, FechaVenta, ProductoCodigo, ClienteCodigo,
            Cantidad, PrecioUnitario, MontoNeto, DescuentoPct,
            CostoUnitario, CiudadNombre, ZonaCodigo, VendedorCodigo,
            FuenteSistema
          FROM dbo.VentasHistoricas
          WHERE FechaVenta >= :from_date
            AND FechaVenta < :to_date
            AND VentaID > :last_id
          ORDER BY VentaID ASC
        """
        if from_date is None:
            from_date = datetime(2018, 1, 1, tzinfo=timezone.utc)
        if to_date is None:
            to_date = datetime(2021, 1, 1, tzinfo=timezone.utc)   # Pre-SAP data

        logger.info(
            "SQL Server extraction: {} → {}",
            from_date.date().isoformat(),
            to_date.date().isoformat(),
        )

        if self._demo_mode:
            yield from self._extract_historical_demo(from_date, to_date)
            return

        conn = self._get_connection()
        cursor = conn.cursor()
        last_id = 0

        while True:
            sql = f"""
                SELECT TOP {batch_size}
                    VentaID            AS source_record_id,
                    FechaVenta         AS sale_date,
                    ProductoCodigo     AS product_code_legacy,
                    ProductoNombre     AS product_name_legacy,
                    ClienteCodigo      AS client_code_legacy,
                    ClienteNombre      AS client_name,
                    Cantidad           AS quantity,
                    PrecioUnitario     AS unit_price,
                    MontoNeto          AS net_amount,
                    MontoBruto         AS gross_amount,
                    DescuentoPct       AS discount_pct,
                    CostoUnitario      AS unit_cost,
                    CiudadNombre       AS city_name,
                    ZonaCodigo         AS zone_code,
                    VendedorCodigo     AS salesperson_code,
                    NumeroFactura      AS invoice_number,
                    LineaFactura       AS invoice_line
                FROM dbo.VentasHistoricas
                WHERE FechaVenta >= '{from_date.strftime('%Y-%m-%d')}'
                  AND FechaVenta <  '{to_date.strftime('%Y-%m-%d')}'
                  AND VentaID > {last_id}
                ORDER BY VentaID ASC
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                break

            cols = [
                "source_record_id","sale_date","product_code_legacy","product_name_legacy",
                "client_code_legacy","client_name","quantity","unit_price","net_amount",
                "gross_amount","discount_pct","unit_cost","city_name","zone_code",
                "salesperson_code","invoice_number","invoice_line",
            ]
            df = pd.DataFrame([list(r) for r in rows], columns=cols)
            df["source_system"] = "SQL_SERVER"
            df["extracted_at"] = datetime.now(tz=timezone.utc)

            last_id = int(df["source_record_id"].max())
            logger.info("SQL Server batch: last_id={}, rows={}", last_id, len(df))
            yield df

            if len(rows) < batch_size:
                break

        cursor.close()

    def _extract_historical_demo(
        self,
        from_date: datetime,
        to_date: datetime,
    ) -> Iterator[pd.DataFrame]:
        """Demo mode: return synthetic data labeled as SQL_SERVER source."""
        from pathlib import Path
        csv_path = Path("data/synthetic/sales.csv")
        if not csv_path.exists():
            logger.warning("Demo CSV not found. Run generate_data.py first.")
            return

        df = pd.read_csv(csv_path, parse_dates=["full_date"])
        df_filtered = df[
            (df["source_system"] == "SAP_HANA") &  # Relabel a slice as SQL Server
            (pd.to_datetime(df["full_date"]) >= pd.Timestamp(from_date)) &
            (pd.to_datetime(df["full_date"]) < pd.Timestamp(to_date))
        ].copy()
        df_filtered["source_system"] = "SQL_SERVER"
        df_filtered["source_record_id"] = "SQLSRV-" + df_filtered.index.astype(str)

        logger.info("SQL Server DEMO: {} historical rows", len(df_filtered))
        chunk_size = 10000
        for start in range(0, len(df_filtered), chunk_size):
            yield df_filtered.iloc[start:start + chunk_size].copy()

    # ── Client Master ───────────────────────────────────────────

    def extract_clients(self) -> pd.DataFrame:
        """Extract client master from SQL Server."""
        if self._demo_mode:
            from pathlib import Path
            csv_path = Path("data/synthetic/clients.csv")
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                df["source_system"] = "SQL_SERVER"
                return df
            return pd.DataFrame()

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                ClienteCodigo   AS client_code_legacy,
                ClienteNombre   AS client_name,
                TipoCliente     AS client_type,
                RNC             AS rnc,
                Direccion       AS address,
                Telefono        AS phone,
                Email           AS email,
                CiudadNombre    AS city_name,
                ZonaCodigo      AS zone_code,
                LimiteCredito   AS credit_limit,
                PlazoCredito    AS payment_terms
            FROM dbo.ClientesMaestro
            WHERE Activo = 1
        """)
        rows = cursor.fetchall()
        cursor.close()
        cols = [
            "client_code_legacy","client_name","client_type","rnc","address",
            "phone","email","city_name","zone_code","credit_limit","payment_terms",
        ]
        return pd.DataFrame([list(r) for r in rows], columns=cols)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
