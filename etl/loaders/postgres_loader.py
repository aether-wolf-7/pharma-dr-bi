"""
Pharma DR · PostgreSQL Loader
===============================
Handles:
  - Upsert (INSERT ... ON CONFLICT DO UPDATE) for fact_sales
  - Batch chunked loading for performance
  - Staging table → DW promotion pattern
  - Post-load materialized view refresh
  - ETL audit logging
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from loguru import logger
from sqlalchemy.dialects.postgresql import insert as pg_insert

from etl.config.settings import settings


class PostgresLoader:
    """
    Loader for the pharma_dr PostgreSQL data warehouse.
    Supports upsert, batch loading, and audit logging.
    """

    CHUNK_SIZE = 2000   # Rows per INSERT batch

    def __init__(self, run_id: Optional[int] = None):
        self._engine = sa.create_engine(
            settings.postgres_dsn,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        self.run_id = run_id

    # ── ETL Audit Logging ─────────────────────────────────────

    def start_run(self, pipeline_name: str, source_system: str) -> int:
        """Register ETL run in audit log. Returns run_id."""
        with self._engine.begin() as conn:
            result = conn.execute(
                sa.text("""
                    INSERT INTO audit.etl_run_log
                        (pipeline_name, source_system, start_time, status)
                    VALUES (:pn, :ss, NOW(), 'RUNNING')
                    RETURNING run_id
                """),
                {"pn": pipeline_name, "ss": source_system},
            )
            run_id = result.scalar()
        self.run_id = run_id
        logger.info("ETL run started: run_id={}, pipeline={}, source={}", run_id, pipeline_name, source_system)
        return run_id

    def complete_run(
        self,
        records_read: int = 0,
        records_loaded: int = 0,
        records_rejected: int = 0,
        watermark_to: Optional[datetime] = None,
        status: str = "SUCCESS",
    ) -> None:
        """Mark ETL run as complete."""
        if not self.run_id:
            return
        with self._engine.begin() as conn:
            conn.execute(
                sa.text("""
                    UPDATE audit.etl_run_log SET
                        end_time         = NOW(),
                        status           = :status,
                        records_read     = :rr,
                        records_loaded   = :rl,
                        records_rejected = :rx,
                        watermark_to     = :wt
                    WHERE run_id = :rid
                """),
                {
                    "status": status,
                    "rr": records_read,
                    "rl": records_loaded,
                    "rx": records_rejected,
                    "wt": watermark_to,
                    "rid": self.run_id,
                },
            )
        logger.info(
            "ETL run_id={} completed: status={}, loaded={}, rejected={}",
            self.run_id, status, records_loaded, records_rejected,
        )

    def fail_run(self, error_message: str) -> None:
        if not self.run_id:
            return
        with self._engine.begin() as conn:
            conn.execute(
                sa.text("""
                    UPDATE audit.etl_run_log SET
                        end_time = NOW(), status = 'FAILED', error_message = :em
                    WHERE run_id = :rid
                """),
                {"em": str(error_message)[:2000], "rid": self.run_id},
            )
        logger.error("ETL run_id={} FAILED: {}", self.run_id, error_message)

    # ── Staging Load ──────────────────────────────────────────

    def load_to_staging(
        self,
        df: pd.DataFrame,
        distributor_id: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> int:
        """Load raw DataFrame to staging.excel_raw for validation."""
        if df.empty:
            return 0

        rows = []
        for _, row in df.iterrows():
            rows.append({
                "distributor_id": distributor_id,
                "file_name": file_name,
                "row_num": _ + 1,
                "raw_data": row.to_json(),
                "extracted_at": datetime.now(tz=timezone.utc),
            })

        staging_df = pd.DataFrame(rows)
        with self._engine.begin() as conn:
            staging_df.to_sql(
                "excel_raw",
                conn,
                schema="staging",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=self.CHUNK_SIZE,
            )
        logger.debug("Staging: {} rows → staging.excel_raw", len(rows))
        return len(rows)

    # ── Fact Sales Upsert ─────────────────────────────────────

    def upsert_fact_sales(
        self,
        df: pd.DataFrame,
        chunk_size: int = None,
    ) -> tuple[int, int]:
        """
        Upsert fact_sales rows using PostgreSQL ON CONFLICT DO UPDATE.
        Conflict key: (source_system, source_record_id, invoice_line)

        Returns: (rows_inserted, rows_updated)
        """
        if df.empty:
            logger.info("upsert_fact_sales: empty DataFrame, nothing to load")
            return 0, 0

        chunk_size = chunk_size or self.CHUNK_SIZE
        rows_inserted = 0
        rows_updated  = 0

        # Coerce types
        int_cols = ["date_key", "product_key", "client_key", "city_key",
                    "zone_key", "distributor_key", "laboratory_key",
                    "salesperson_key", "invoice_line"]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        float_cols = ["quantity", "unit_price", "gross_amount", "discount_pct",
                      "discount_amount", "net_amount", "cost_amount",
                      "margin_amount", "margin_pct"]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Chunked upsert
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            records = chunk.to_dict("records")

            with self._engine.begin() as conn:
                result = conn.execute(
                    sa.text("""
                        INSERT INTO dw.fact_sales (
                            date_key, product_key, client_key, city_key, zone_key,
                            distributor_key, laboratory_key, salesperson_key,
                            invoice_number, invoice_line,
                            quantity, unit_price, gross_amount,
                            discount_pct, discount_amount, net_amount,
                            cost_amount, margin_amount, margin_pct,
                            source_system, source_record_id,
                            load_timestamp, is_deleted
                        )
                        VALUES (
                            :date_key, :product_key, :client_key, :city_key, :zone_key,
                            :distributor_key, :laboratory_key, :salesperson_key,
                            :invoice_number, :invoice_line,
                            :quantity, :unit_price, :gross_amount,
                            :discount_pct, :discount_amount, :net_amount,
                            :cost_amount, :margin_amount, :margin_pct,
                            :source_system, :source_record_id,
                            :load_timestamp, :is_deleted
                        )
                        ON CONFLICT (source_system, source_record_id, invoice_line)
                        DO UPDATE SET
                            quantity       = EXCLUDED.quantity,
                            unit_price     = EXCLUDED.unit_price,
                            gross_amount   = EXCLUDED.gross_amount,
                            discount_pct   = EXCLUDED.discount_pct,
                            discount_amount= EXCLUDED.discount_amount,
                            net_amount     = EXCLUDED.net_amount,
                            cost_amount    = EXCLUDED.cost_amount,
                            margin_amount  = EXCLUDED.margin_amount,
                            margin_pct     = EXCLUDED.margin_pct,
                            load_timestamp = EXCLUDED.load_timestamp,
                            is_deleted     = EXCLUDED.is_deleted
                        RETURNING (xmax = 0) AS is_insert
                    """),
                    records,
                )
                insert_flags = [row[0] for row in result]
                rows_inserted += sum(insert_flags)
                rows_updated  += len(insert_flags) - sum(insert_flags)

            logger.debug("Upsert chunk {}/{}: {} rows", start // chunk_size + 1,
                         (len(df) - 1) // chunk_size + 1, len(chunk))

        logger.info(
            "fact_sales upsert: {} inserted, {} updated",
            rows_inserted, rows_updated,
        )
        return rows_inserted, rows_updated

    # ── Client Upsert ─────────────────────────────────────────

    def upsert_clients(self, df: pd.DataFrame) -> int:
        """Upsert client dimension rows."""
        if df.empty:
            return 0
        required = ["client_id", "client_name", "client_type"]
        if not all(c in df.columns for c in required):
            logger.warning("upsert_clients: missing required columns")
            return 0

        loaded = 0
        for _, row in df.iterrows():
            with self._engine.begin() as conn:
                conn.execute(
                    sa.text("""
                        INSERT INTO dw.dim_client
                            (client_id, client_name, client_name_norm,
                             client_type, rnc, address, phone, email,
                             credit_limit, payment_terms,
                             city_key, zone_key, salesperson_key)
                        VALUES
                            (:cid, :cname, lower(unaccent(:cname)),
                             :ctype, :rnc, :addr, :phone, :email,
                             :credit, :terms,
                             :city_key, :zone_key, :sp_key)
                        ON CONFLICT (client_id) DO UPDATE SET
                            client_name = EXCLUDED.client_name,
                            client_type = EXCLUDED.client_type,
                            rnc         = EXCLUDED.rnc,
                            updated_at  = NOW()
                    """),
                    {
                        "cid":      row.get("client_id"),
                        "cname":    str(row.get("client_name", ""))[:200],
                        "ctype":    str(row.get("client_type", "FARMACIA_INDEPENDIENTE"))[:40],
                        "rnc":      row.get("rnc"),
                        "addr":     row.get("address"),
                        "phone":    row.get("phone"),
                        "email":    row.get("email"),
                        "credit":   float(row.get("credit_limit", 50000)),
                        "terms":    int(row.get("payment_terms", 30)),
                        "city_key": int(row.get("city_key", 1)),
                        "zone_key": int(row.get("zone_key", 1)),
                        "sp_key":   int(row.get("salesperson_key", 1)),
                    },
                )
                loaded += 1

        logger.info("upsert_clients: {} clients upserted", loaded)
        return loaded

    # ── Materialized View Refresh ─────────────────────────────

    def refresh_materialized_views(self) -> None:
        """Refresh all analytical materialized views (CONCURRENTLY)."""
        logger.info("Refreshing materialized views...")
        with self._engine.begin() as conn:
            conn.execute(sa.text("CALL mart.refresh_all_mvs()"))
        logger.info("Materialized views refreshed")

    def refresh_commissions(self, year: int, month: int) -> None:
        """Recalculate commissions for the given year/month."""
        logger.info("Recalculating commissions for {}-{:02d}", year, month)
        with self._engine.begin() as conn:
            conn.execute(
                sa.text("CALL dw.recalculate_commissions(:y, :m)"),
                {"y": year, "m": month},
            )
        logger.info("Commission recalculation complete for {}-{:02d}", year, month)

    # ── Bulk Load from CSV (Initial Load) ────────────────────

    def bulk_load_from_csv(
        self,
        csv_path: str,
        resolver,
        source_system: str = "SAP_HANA",
    ) -> tuple[int, int]:
        """
        Load synthetic/CSV data directly into fact_sales.
        Used for initial demo data population.
        """
        from etl.transformers.normalizer import normalize_for_load
        from etl.transformers.cleansing import cleanse_dataframe

        logger.info("Bulk loading from CSV: {}", csv_path)
        df = pd.read_csv(csv_path, parse_dates=["full_date"])

        # Filter to requested source system
        if "source_system" in df.columns:
            df = df[df["source_system"] == source_system].copy()

        if df.empty:
            logger.info("No rows for source_system={} in {}", source_system, csv_path)
            return 0, 0

        # Rename columns to expected format
        rename_map = {"full_date": "sale_date"}
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # Cleanse
        df, rejected = cleanse_dataframe(df, source_system)

        # Normalize (resolve dimension keys)
        fact_df, norm_rejected = normalize_for_load(df, source_system, resolver)

        # Load
        inserted, updated = self.upsert_fact_sales(fact_df)
        return inserted, updated

    def dispose(self):
        self._engine.dispose()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.dispose()
