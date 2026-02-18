-- ============================================================
-- Pharma DR · Database Schema Setup
-- Run order: 01 → 02 → 03 → 04 → 05 → 06
-- PostgreSQL 16
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "unaccent";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";       -- Fuzzy text search
CREATE EXTENSION IF NOT EXISTS "btree_gin";     -- GIN composite indexes
CREATE EXTENSION IF NOT EXISTS "pgcrypto";      -- Password hashing for app users

-- ── Schemas ──────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;   -- Raw data from sources (transient)
CREATE SCHEMA IF NOT EXISTS dw;        -- Data Warehouse (dimensions + facts)
CREATE SCHEMA IF NOT EXISTS mart;      -- Analytical views and materialized views
CREATE SCHEMA IF NOT EXISTS audit;     -- ETL audit log

COMMENT ON SCHEMA staging IS 'Raw extracted data — transient, overwritten each cycle';
COMMENT ON SCHEMA dw      IS 'Star schema data warehouse — single source of truth';
COMMENT ON SCHEMA mart     IS 'Analytical layer — materialized views for BI consumption';
COMMENT ON SCHEMA audit    IS 'ETL run logs and data quality metrics';

-- ── Audit / ETL Log Tables ────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.etl_run_log (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    source_system   VARCHAR(50)  NOT NULL,
    start_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    end_time        TIMESTAMPTZ,
    status          VARCHAR(20)  NOT NULL DEFAULT 'RUNNING',  -- RUNNING|SUCCESS|FAILED|PARTIAL
    records_read    INT          DEFAULT 0,
    records_loaded  INT          DEFAULT 0,
    records_rejected INT         DEFAULT 0,
    watermark_from  TIMESTAMPTZ,
    watermark_to    TIMESTAMPTZ,
    error_message   TEXT,
    run_metadata    JSONB
);

CREATE TABLE IF NOT EXISTS audit.data_quality_log (
    dq_id           BIGSERIAL PRIMARY KEY,
    run_id          BIGINT REFERENCES audit.etl_run_log(run_id),
    check_name      VARCHAR(100) NOT NULL,
    table_name      VARCHAR(100),
    records_checked INT,
    records_failed  INT,
    failure_pct     NUMERIC(6,3),
    severity        VARCHAR(10),   -- INFO|WARN|ERROR|CRITICAL
    details         TEXT,
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit.unmapped_products (
    unmapped_id         BIGSERIAL PRIMARY KEY,
    source_system       VARCHAR(50),
    distributor_code    VARCHAR(100),
    product_description VARCHAR(500),
    suggested_match     VARCHAR(200),
    similarity_score    NUMERIC(5,3),
    resolved_flag       BOOLEAN DEFAULT FALSE,
    resolved_by         VARCHAR(100),
    resolved_at         TIMESTAMPTZ,
    first_seen          TIMESTAMPTZ DEFAULT NOW(),
    occurrence_count    INT DEFAULT 1
);

-- ── Staging Tables ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.sap_raw (
    id              BIGSERIAL PRIMARY KEY,
    source_key      VARCHAR(200),
    raw_data        JSONB NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_flag  BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS staging.sqlsrv_raw (
    id              BIGSERIAL PRIMARY KEY,
    source_key      VARCHAR(200),
    raw_data        JSONB NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_flag  BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS staging.excel_raw (
    id              BIGSERIAL PRIMARY KEY,
    distributor_id  VARCHAR(10),   -- DIST_A through DIST_F
    file_name       VARCHAR(500),
    row_num         INT,
    raw_data        JSONB NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_flag  BOOLEAN DEFAULT FALSE,
    rejection_reason TEXT
);

-- ── Watermark Table (Incremental Extraction State) ────────────
CREATE TABLE IF NOT EXISTS audit.watermarks (
    source_system   VARCHAR(50) PRIMARY KEY,
    last_watermark  TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO audit.watermarks(source_system, last_watermark) VALUES
    ('SAP_HANA',   '2018-01-01 00:00:00+00'),
    ('SQL_SERVER', '2018-01-01 00:00:00+00'),
    ('DIST_A',     '2018-01-01 00:00:00+00'),
    ('DIST_B',     '2018-01-01 00:00:00+00'),
    ('DIST_C',     '2018-01-01 00:00:00+00'),
    ('DIST_D',     '2018-01-01 00:00:00+00'),
    ('DIST_E',     '2018-01-01 00:00:00+00'),
    ('DIST_F',     '2018-01-01 00:00:00+00')
ON CONFLICT (source_system) DO NOTHING;
