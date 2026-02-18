"""
Pharma DR · Full Load Pipeline
================================
Initial data population pipeline. Runs once:
  1. Generate synthetic data (if not present)
  2. Load clients into dim_client
  3. Load all sales data (SAP HANA + SQL Server + 6 Distributors)
  4. Recalculate commissions for all months
  5. Refresh all materialized views

Run: python etl/pipelines/full_load_pipeline.py
"""

import sys
from pathlib import Path
from datetime import datetime

from loguru import logger

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from etl.config.settings import settings
from etl.extractors.excel_extractor import ExcelExtractor
from etl.transformers.cleansing import cleanse_dataframe
from etl.transformers.mapping import map_product_codes
from etl.transformers.normalizer import DimensionResolver, normalize_for_load
from etl.loaders.postgres_loader import PostgresLoader


def run_full_load():
    """Execute full data load pipeline."""
    logger.info("=" * 60)
    logger.info("FULL LOAD PIPELINE STARTED: {}", datetime.now().isoformat())
    logger.info("=" * 60)

    loader = PostgresLoader()
    resolver = DimensionResolver()
    run_id = loader.start_run("full_load_pipeline", "ALL_SOURCES")

    total_inserted = 0
    total_rejected = 0

    try:
        # ── Step 1: Check / Generate Synthetic Data ────────────
        sales_csv    = ROOT / "data" / "synthetic" / "sales.csv"
        clients_csv  = ROOT / "data" / "synthetic" / "clients.csv"

        if not sales_csv.exists():
            logger.info("Synthetic data not found. Generating...")
            import subprocess
            subprocess.run(
                [sys.executable, str(ROOT / "data" / "synthetic" / "generate_data.py")],
                check=True,
            )

        # ── Step 2: Load Clients ────────────────────────────────
        logger.info("Loading clients into dim_client...")
        import pandas as pd
        clients_df = pd.read_csv(clients_csv)

        # Resolve city_key and zone_key for clients
        cities_df = resolver.get_city_keys()
        city_merge = clients_df.merge(
            cities_df[["city_key", "city_name"]],
            on="city_name",
            how="left",
        )
        zones_df = resolver.get_zone_keys()
        city_merge = city_merge.merge(
            zones_df[["zone_key", "zone_code"]],
            left_on="zone_code",
            right_on="zone_code",
            how="left",
        )
        city_merge["zone_key"] = city_merge["zone_key"].fillna(1).astype(int)
        city_merge["city_key"] = city_merge["city_key"].fillna(1).astype(int)
        city_merge["salesperson_key"] = 1  # Default SP

        clients_loaded = loader.upsert_clients(city_merge)
        logger.info("Clients loaded: {}", clients_loaded)

        # Invalidate client cache after loading
        resolver.invalidate_cache("dw.dim_client")

        # ── Step 3: Load SAP HANA Data ─────────────────────────
        logger.info("Loading SAP HANA sales data...")
        ins, upd = loader.bulk_load_from_csv(
            str(sales_csv), resolver, source_system="SAP_HANA"
        )
        total_inserted += ins
        logger.info("SAP HANA: {} inserted, {} updated", ins, upd)

        # ── Step 4: Load SQL Server Data ───────────────────────
        logger.info("Loading SQL Server historical data...")
        from etl.extractors.sqlserver_extractor import SqlServerExtractor
        from etl.transformers.cleansing import cleanse_dataframe
        from etl.transformers.normalizer import normalize_for_load

        with SqlServerExtractor() as ss_ext:
            for batch_df in ss_ext.extract_historical_sales():
                if batch_df.empty:
                    continue
                batch_df["sale_date"] = pd.to_datetime(
                    batch_df.get("full_date", batch_df.get("sale_date")), errors="coerce"
                )
                clean_df, rejected_df = cleanse_dataframe(batch_df, "SQL_SERVER")
                total_rejected += len(rejected_df)
                if clean_df.empty:
                    continue
                fact_df, norm_rejected = normalize_for_load(clean_df, "SQL_SERVER", resolver)
                total_rejected += len(norm_rejected)
                ins, upd = loader.upsert_fact_sales(fact_df)
                total_inserted += ins
                logger.info("SQL Server batch: {} inserted, {} updated", ins, upd)

        # ── Step 5: Load Excel Distributor Data ────────────────
        logger.info("Loading Excel distributor data...")
        excel_dir = ROOT / "data" / "excel_samples"
        file_groups = ExcelExtractor.scan_landing_directory(str(excel_dir))

        for dist_code, file_list in file_groups.items():
            for file_path in file_list:
                try:
                    extractor = ExcelExtractor(dist_code)
                    raw_df = extractor.extract(file_path)

                    if raw_df.empty:
                        logger.warning("{}: empty extract from {}", dist_code, file_path.name)
                        continue

                    # Map product codes
                    raw_df = map_product_codes(raw_df, dist_code)

                    # Cleanse
                    clean_df, rejected_df = cleanse_dataframe(raw_df, dist_code)
                    total_rejected += len(rejected_df)

                    # Normalize
                    fact_df, norm_rejected = normalize_for_load(clean_df, dist_code, resolver)
                    total_rejected += len(norm_rejected)

                    # Load
                    ins, upd = loader.upsert_fact_sales(fact_df)
                    total_inserted += ins
                    logger.info("{} | {}: {} inserted, {} updated", dist_code, file_path.name, ins, upd)

                except Exception as exc:
                    logger.error("Error processing {} {}: {}", dist_code, file_path.name, exc)
                    total_rejected += 1

        # ── Step 6: Recalculate Commissions ───────────────────
        logger.info("Recalculating commissions for all months...")
        for year in range(2021, 2025):
            for month in range(1, 13):
                try:
                    loader.refresh_commissions(year, month)
                except Exception as exc:
                    logger.warning("Commission calc {}-{}: {}", year, month, exc)

        # ── Step 7: Refresh Materialized Views ─────────────────
        logger.info("Refreshing materialized views...")
        try:
            loader.refresh_materialized_views()
        except Exception as exc:
            logger.warning("MV refresh error: {}", exc)

        # ── Complete ────────────────────────────────────────────
        loader.complete_run(
            records_loaded=total_inserted,
            records_rejected=total_rejected,
            status="SUCCESS",
        )

        logger.info("=" * 60)
        logger.info("FULL LOAD COMPLETE")
        logger.info("  Total Inserted: {:,}", total_inserted)
        logger.info("  Total Rejected: {:,}", total_rejected)
        logger.info("=" * 60)

    except Exception as exc:
        logger.exception("FULL LOAD FAILED: {}", exc)
        loader.fail_run(str(exc))
        raise
    finally:
        loader.dispose()


if __name__ == "__main__":
    run_full_load()
