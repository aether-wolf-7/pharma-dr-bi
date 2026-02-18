"""
Pharma DR · Incremental Pipeline
===================================
Runs on schedule (every 15 min for SAP HANA, hourly for Excel distributors).
Uses watermarks to extract only new/changed records.

Called by: etl/orchestration/scheduler.py
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

from loguru import logger

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from etl.extractors.sap_hana_extractor import SapHanaExtractor
from etl.extractors.excel_extractor import ExcelExtractor
from etl.transformers.cleansing import cleanse_dataframe
from etl.transformers.mapping import map_product_codes
from etl.transformers.normalizer import DimensionResolver, normalize_for_load
from etl.loaders.postgres_loader import PostgresLoader


def run_sap_hana_incremental():
    """Extract and load incremental SAP HANA data."""
    loader = PostgresLoader()
    resolver = DimensionResolver()
    run_id = loader.start_run("sap_hana_incremental", "SAP_HANA")

    total_inserted = total_rejected = 0

    try:
        with SapHanaExtractor() as extractor:
            for batch_df in extractor.extract_sales():
                if batch_df.empty:
                    continue

                clean_df, rejected_df = cleanse_dataframe(batch_df, "SAP_HANA")
                total_rejected += len(rejected_df)

                if clean_df.empty:
                    continue

                fact_df, norm_rejected = normalize_for_load(clean_df, "SAP_HANA", resolver)
                total_rejected += len(norm_rejected)

                ins, upd = loader.upsert_fact_sales(fact_df)
                total_inserted += ins
                logger.info("SAP HANA incremental batch: {} inserted, {} updated", ins, upd)

        # Refresh commissions for current month
        now = datetime.now()
        loader.refresh_commissions(now.year, now.month)

        # Refresh MVs
        loader.refresh_materialized_views()

        loader.complete_run(
            records_loaded=total_inserted,
            records_rejected=total_rejected,
        )
        logger.info("SAP HANA incremental complete: {} inserted", total_inserted)

    except Exception as exc:
        logger.exception("SAP HANA incremental failed: {}", exc)
        loader.fail_run(str(exc))
    finally:
        loader.dispose()


def run_excel_incremental():
    """Scan landing directory and process any new Excel distributor files."""
    loader = PostgresLoader()
    resolver = DimensionResolver()
    run_id = loader.start_run("excel_incremental", "DISTRIBUTORS")

    total_inserted = total_rejected = 0

    try:
        file_groups = ExcelExtractor.scan_landing_directory()

        if not file_groups:
            logger.info("No new Excel files found")
            loader.complete_run(status="SUCCESS")
            return

        for dist_code, file_list in file_groups.items():
            for file_path in file_list:
                try:
                    extractor = ExcelExtractor(dist_code)
                    raw_df = extractor.extract(file_path)

                    if raw_df.empty:
                        ExcelExtractor.move_to_rejected(file_path, "empty_file")
                        continue

                    raw_df = map_product_codes(raw_df, dist_code)
                    clean_df, rejected_df = cleanse_dataframe(raw_df, dist_code)
                    total_rejected += len(rejected_df)

                    fact_df, norm_rejected = normalize_for_load(clean_df, dist_code, resolver)
                    total_rejected += len(norm_rejected)

                    ins, upd = loader.upsert_fact_sales(fact_df)
                    total_inserted += ins
                    logger.info("{} | {}: {} inserted", dist_code, file_path.name, ins)

                    ExcelExtractor.move_to_processed(file_path)

                except Exception as exc:
                    logger.error("Error processing {}: {}", file_path.name, exc)
                    ExcelExtractor.move_to_rejected(file_path, str(exc)[:500])

        if total_inserted > 0:
            loader.refresh_materialized_views()

        loader.complete_run(records_loaded=total_inserted, records_rejected=total_rejected)

    except Exception as exc:
        logger.exception("Excel incremental failed: {}", exc)
        loader.fail_run(str(exc))
    finally:
        loader.dispose()


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "sap"
    if cmd == "sap":
        run_sap_hana_incremental()
    elif cmd == "excel":
        run_excel_incremental()
    else:
        logger.error("Usage: python incremental_pipeline.py [sap|excel]")
