"""
Pharma DR · ETL Scheduler
===========================
APScheduler-based orchestration for all ETL jobs.
For production, upgrade to Apache Airflow (DAGs provided in architecture docs).

Jobs:
  - sap_hana_incremental    : every 15 minutes
  - excel_distributor_scan  : every 1 hour
  - mv_refresh              : nightly at 02:00
  - commission_monthly      : 1st of month at 06:00
  - sqlserver_delta         : 1st of month at 07:00

Run: python etl/orchestration/scheduler.py
"""

import sys
import signal
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from loguru import logger

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from etl.config.settings import settings


# ── Job Functions (thin wrappers with error isolation) ─────────

def job_sap_hana_incremental():
    """SAP HANA incremental extraction — every 15 min."""
    try:
        from etl.pipelines.incremental_pipeline import run_sap_hana_incremental
        run_sap_hana_incremental()
    except Exception as exc:
        logger.error("job_sap_hana_incremental failed: {}", exc)
        _send_alert("SAP HANA Incremental Failed", str(exc))


def job_excel_distributor_scan():
    """Excel distributor file scan — every 1 hour."""
    try:
        from etl.pipelines.incremental_pipeline import run_excel_incremental
        run_excel_incremental()
    except Exception as exc:
        logger.error("job_excel_distributor_scan failed: {}", exc)
        _send_alert("Excel Distributor Scan Failed", str(exc))


def job_mv_refresh():
    """Nightly materialized view refresh — 02:00."""
    try:
        from etl.loaders.postgres_loader import PostgresLoader
        with PostgresLoader() as loader:
            loader.refresh_materialized_views()
        logger.info("Nightly MV refresh complete")
    except Exception as exc:
        logger.error("job_mv_refresh failed: {}", exc)
        _send_alert("MV Refresh Failed", str(exc))


def job_commission_monthly():
    """Monthly commission recalculation — 1st of month 06:00."""
    try:
        from etl.loaders.postgres_loader import PostgresLoader
        now = datetime.now()
        with PostgresLoader() as loader:
            loader.refresh_commissions(now.year, now.month)
        logger.info("Commission recalculation complete: {}-{:02d}", now.year, now.month)
    except Exception as exc:
        logger.error("job_commission_monthly failed: {}", exc)
        _send_alert("Commission Calculation Failed", str(exc))


def job_sqlserver_delta():
    """SQL Server monthly delta — 1st of month 07:00."""
    try:
        from etl.extractors.sqlserver_extractor import SqlServerExtractor
        from etl.transformers.cleansing import cleanse_dataframe
        from etl.transformers.normalizer import DimensionResolver, normalize_for_load
        from etl.loaders.postgres_loader import PostgresLoader
        import pandas as pd
        from datetime import timedelta

        now = datetime.now()
        first_of_prev_month = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
        first_of_curr_month = now.replace(day=1)

        loader = PostgresLoader()
        resolver = DimensionResolver()
        total_inserted = 0

        with SqlServerExtractor() as extractor:
            for batch_df in extractor.extract_historical_sales(
                from_date=first_of_prev_month,
                to_date=first_of_curr_month,
            ):
                if batch_df.empty:
                    continue
                clean_df, _ = cleanse_dataframe(batch_df, "SQL_SERVER")
                fact_df, _ = normalize_for_load(clean_df, "SQL_SERVER", resolver)
                ins, _ = loader.upsert_fact_sales(fact_df)
                total_inserted += ins

        loader.dispose()
        logger.info("SQL Server delta: {} rows inserted", total_inserted)
    except Exception as exc:
        logger.error("job_sqlserver_delta failed: {}", exc)
        _send_alert("SQL Server Delta Failed", str(exc))


# ── Alert Helper ───────────────────────────────────────────────

def _send_alert(subject: str, body: str):
    """Send email alert on job failure."""
    if not settings.smtp_host or not settings.alert_recipient_list:
        return
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(f"{body}\n\nTime: {datetime.now().isoformat()}")
        msg["Subject"] = f"[Pharma DR ETL] {subject}"
        msg["From"]    = settings.smtp_user
        msg["To"]      = ", ".join(settings.alert_recipient_list)
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as smtp:
            smtp.starttls()
            smtp.login(settings.smtp_user, settings.smtp_password)
            smtp.send_message(msg)
        logger.info("Alert sent: {}", subject)
    except Exception as exc:
        logger.warning("Failed to send alert email: {}", exc)


# ── APScheduler Event Listener ────────────────────────────────

def on_job_event(event):
    if event.exception:
        logger.error("Job {} raised: {}", event.job_id, event.exception)
    else:
        logger.info("Job {} executed successfully (scheduled: {})",
                    event.job_id, event.scheduled_run_time)


# ── Main Scheduler Setup ──────────────────────────────────────

def create_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="America/Santo_Domingo")
    scheduler.add_listener(on_job_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

    # SAP HANA incremental: every 15 minutes
    scheduler.add_job(
        job_sap_hana_incremental,
        trigger=IntervalTrigger(minutes=15),
        id="sap_hana_incremental",
        name="SAP HANA Incremental Extraction",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Excel distributor scan: every hour at :05
    scheduler.add_job(
        job_excel_distributor_scan,
        trigger=CronTrigger(minute=5),
        id="excel_distributor_scan",
        name="Excel Distributor Feed Scan",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )

    # Nightly MV refresh: 02:00
    scheduler.add_job(
        job_mv_refresh,
        trigger=CronTrigger(hour=2, minute=0),
        id="mv_refresh",
        name="Nightly Materialized View Refresh",
        max_instances=1,
        misfire_grace_time=3600,
    )

    # Monthly commission calc: 1st of month at 06:00
    scheduler.add_job(
        job_commission_monthly,
        trigger=CronTrigger(day=1, hour=6, minute=0),
        id="commission_monthly",
        name="Monthly Commission Calculation",
        max_instances=1,
        misfire_grace_time=7200,
    )

    # Monthly SQL Server delta: 1st of month at 07:00
    scheduler.add_job(
        job_sqlserver_delta,
        trigger=CronTrigger(day=1, hour=7, minute=0),
        id="sqlserver_delta",
        name="SQL Server Monthly Delta",
        max_instances=1,
        misfire_grace_time=7200,
    )

    return scheduler


def main():
    scheduler = create_scheduler()

    def shutdown_handler(signum, frame):
        logger.info("Shutdown signal received. Stopping scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    logger.info("=" * 60)
    logger.info("Pharma DR ETL Scheduler starting...")
    logger.info("Timezone: America/Santo_Domingo")
    logger.info("Environment: {}", settings.environment)
    logger.info("=" * 60)

    for job in scheduler.get_jobs():
        logger.info("  Scheduled: {} | Next run: {}", job.name, job.next_run_time)

    scheduler.start()


if __name__ == "__main__":
    main()
