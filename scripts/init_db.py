"""
Pharma DR · Database Initialization Script
============================================
Runs all SQL schema files in order against the PostgreSQL instance.
Run after: docker compose up -d postgres

Usage: python scripts/init_db.py
"""

import sys
import time
from pathlib import Path

import sqlalchemy as sa
from loguru import logger

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from etl.config.settings import settings

SQL_FILES = [
    ROOT / "database" / "01_schema.sql",
    ROOT / "database" / "02_dimensions.sql",
    ROOT / "database" / "03_facts.sql",
    ROOT / "database" / "04_indexes.sql",
    ROOT / "database" / "05_views.sql",
    ROOT / "database" / "06_rbac.sql",
]


def wait_for_postgres(engine: sa.Engine, max_wait: int = 60) -> bool:
    """Poll until PostgreSQL is ready."""
    logger.info("Waiting for PostgreSQL to be ready...")
    for attempt in range(max_wait):
        try:
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            logger.info("PostgreSQL is ready!")
            return True
        except Exception:
            if attempt % 5 == 0:
                logger.info("  Attempt {}/{}...", attempt + 1, max_wait)
            time.sleep(1)
    logger.error("PostgreSQL not ready after {} seconds", max_wait)
    return False


def run_sql_file(conn: sa.Connection, sql_file: Path) -> None:
    """Execute a SQL file, splitting on ';' for multi-statement support."""
    logger.info("  Running: {}", sql_file.name)
    sql_content = sql_file.read_text(encoding="utf-8")

    # Remove SQL comments for cleaner parsing
    import re
    sql_no_comments = re.sub(r"--[^\n]*", "", sql_content)
    sql_no_comments = re.sub(r"/\*.*?\*/", "", sql_no_comments, flags=re.DOTALL)

    # Split on semicolons (naive but works for our structured SQL files)
    statements = [s.strip() for s in sql_no_comments.split(";") if s.strip()]

    success = 0
    errors  = 0
    for stmt in statements:
        try:
            conn.execute(sa.text(stmt))
            success += 1
        except Exception as exc:
            # Ignore "already exists" errors
            err_str = str(exc).lower()
            if any(phrase in err_str for phrase in [
                "already exists", "does not exist",
                "duplicate", "relation already"
            ]):
                pass  # Expected during re-runs
            else:
                logger.warning("    SQL error: {}", str(exc)[:200])
                errors += 1

    logger.info("    {} statements OK, {} errors", success, errors)


def initialize_database() -> None:
    """Run all SQL initialization files."""
    logger.info("=" * 60)
    logger.info("Pharma DR · Database Initialization")
    logger.info("Host: {}:{}/{}", settings.postgres_host, settings.postgres_port, settings.postgres_db)
    logger.info("=" * 60)

    # Use direct psycopg2 connection (bypass PgBouncer for DDL)
    dsn = (
        f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
    engine = sa.create_engine(dsn, isolation_level="AUTOCOMMIT")

    if not wait_for_postgres(engine):
        sys.exit(1)

    for sql_file in SQL_FILES:
        if not sql_file.exists():
            logger.warning("SQL file not found: {}", sql_file)
            continue
        with engine.connect() as conn:
            run_sql_file(conn, sql_file)

    logger.info("=" * 60)
    logger.info("Database initialization complete!")

    # Quick sanity check
    with engine.connect() as conn:
        zones = conn.execute(sa.text("SELECT COUNT(*) FROM dw.dim_zone")).scalar()
        cities = conn.execute(sa.text("SELECT COUNT(*) FROM dw.dim_city")).scalar()
        products = conn.execute(sa.text("SELECT COUNT(*) FROM dw.dim_product")).scalar()
        labs = conn.execute(sa.text("SELECT COUNT(*) FROM dw.dim_laboratory")).scalar()
        sps = conn.execute(sa.text("SELECT COUNT(*) FROM dw.dim_salesperson")).scalar()
        date_rows = conn.execute(sa.text("SELECT COUNT(*) FROM dw.dim_date")).scalar()

    logger.info("Dimension counts:")
    logger.info("  dim_zone:        {}", zones)
    logger.info("  dim_city:        {}", cities)
    logger.info("  dim_product:     {}", products)
    logger.info("  dim_laboratory:  {}", labs)
    logger.info("  dim_salesperson: {}", sps)
    logger.info("  dim_date:        {} rows (2018–2030)", date_rows)
    logger.info("=" * 60)

    engine.dispose()


if __name__ == "__main__":
    initialize_database()
