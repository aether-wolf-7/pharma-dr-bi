-- ============================================================
-- Pharma DR · Indexes & Partitioning Strategy
-- ============================================================

-- ── fact_sales Foreign Key Indexes ───────────────────────────
CREATE INDEX IF NOT EXISTS idx_fs_date_key        ON dw.fact_sales (date_key);
CREATE INDEX IF NOT EXISTS idx_fs_product_key     ON dw.fact_sales (product_key);
CREATE INDEX IF NOT EXISTS idx_fs_client_key      ON dw.fact_sales (client_key);
CREATE INDEX IF NOT EXISTS idx_fs_city_key        ON dw.fact_sales (city_key);
CREATE INDEX IF NOT EXISTS idx_fs_zone_key        ON dw.fact_sales (zone_key);
CREATE INDEX IF NOT EXISTS idx_fs_distributor_key ON dw.fact_sales (distributor_key);
CREATE INDEX IF NOT EXISTS idx_fs_laboratory_key  ON dw.fact_sales (laboratory_key);
CREATE INDEX IF NOT EXISTS idx_fs_salesperson_key ON dw.fact_sales (salesperson_key);

-- ── Composite Indexes (Query Pattern Optimized) ───────────────
-- YoY / trend queries: zone + date
CREATE INDEX IF NOT EXISTS idx_fs_zone_date
    ON dw.fact_sales (zone_key, date_key)
    INCLUDE (net_amount, margin_amount);

-- Product performance: product + date
CREATE INDEX IF NOT EXISTS idx_fs_product_date
    ON dw.fact_sales (product_key, date_key)
    INCLUDE (net_amount, quantity, margin_amount);

-- Client ranking: client + date range
CREATE INDEX IF NOT EXISTS idx_fs_client_date
    ON dw.fact_sales (client_key, date_key)
    INCLUDE (net_amount, quantity);

-- Distributor comparison: distributor + zone + date
CREATE INDEX IF NOT EXISTS idx_fs_dist_zone_date
    ON dw.fact_sales (distributor_key, zone_key, date_key);

-- Source system dedup (ETL idempotency)
CREATE UNIQUE INDEX IF NOT EXISTS idx_fs_source_dedup
    ON dw.fact_sales (source_system, source_record_id, invoice_line)
    WHERE is_deleted = FALSE;

-- ── BRIN Index for Time-Series Load Tracking ─────────────────
-- Extremely small footprint (~few KB) for time-ordered append data
CREATE INDEX IF NOT EXISTS idx_fs_load_brin
    ON dw.fact_sales USING BRIN (load_timestamp)
    WITH (pages_per_range = 128);

-- ── Partial Index: Active Records Only ───────────────────────
CREATE INDEX IF NOT EXISTS idx_fs_active
    ON dw.fact_sales (date_key, zone_key, product_key)
    WHERE is_deleted = FALSE;

-- ── Dimension Table Indexes ───────────────────────────────────
-- Product fuzzy search (pg_trgm)
CREATE INDEX IF NOT EXISTS idx_prod_name_trgm
    ON dw.dim_product USING GIN (product_name_norm gin_trgm_ops);

-- City fuzzy search
CREATE INDEX IF NOT EXISTS idx_city_name_trgm
    ON dw.dim_city USING GIN (city_name_norm gin_trgm_ops);

-- Client fuzzy search
CREATE INDEX IF NOT EXISTS idx_client_name_trgm
    ON dw.dim_client USING GIN (client_name_norm gin_trgm_ops);

-- RNC/NIT lookup
CREATE INDEX IF NOT EXISTS idx_client_rnc ON dw.dim_client (rnc);
CREATE INDEX IF NOT EXISTS idx_product_id  ON dw.dim_product (product_id);

-- Staging processing flag
CREATE INDEX IF NOT EXISTS idx_sap_raw_proc
    ON staging.sap_raw (processed_flag, extracted_at)
    WHERE processed_flag = FALSE;

CREATE INDEX IF NOT EXISTS idx_excel_raw_dist_proc
    ON staging.excel_raw (distributor_id, processed_flag, extracted_at)
    WHERE processed_flag = FALSE;

-- ── Table Statistics & Autovacuum Tuning ─────────────────────
-- For large fact table: more aggressive autovacuum
ALTER TABLE dw.fact_sales SET (
    autovacuum_vacuum_scale_factor    = 0.01,
    autovacuum_analyze_scale_factor   = 0.005,
    autovacuum_vacuum_cost_delay      = 2,
    fillfactor                        = 85     -- Leave 15% free for HOT updates
);

-- ── Cluster Hint (run periodically to physically sort data) ──
-- CLUSTER dw.fact_sales USING idx_fs_date_key;
-- Schedule monthly: pg_cron or maintenance script

-- ── Analyze after initial load ────────────────────────────────
-- ANALYZE dw.fact_sales;
-- ANALYZE dw.dim_product;
-- ANALYZE dw.dim_client;
-- ANALYZE dw.dim_city;
