-- ============================================================
-- Pharma DR · Analytical Views & Materialized Views
-- These power Apache Superset dashboards
-- ============================================================

-- ── 1. Materialized View: Monthly Sales by Zone ───────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_sales_monthly_zone AS
SELECT
    dd.year_month,
    dd.year,
    dd.month_num,
    dd.month_name,
    dz.zone_code,
    dz.zone_name,
    dl.distributor_code,
    dl.distributor_name,
    dl.distributor_type,
    COUNT(DISTINCT fs.invoice_number)       AS invoice_count,
    COUNT(*)                                AS line_count,
    SUM(fs.quantity)                        AS total_quantity,
    ROUND(SUM(fs.gross_amount), 2)          AS total_gross,
    ROUND(SUM(fs.discount_amount), 2)       AS total_discount,
    ROUND(SUM(fs.net_amount), 2)            AS total_net,
    ROUND(SUM(fs.cost_amount), 2)           AS total_cost,
    ROUND(SUM(fs.margin_amount), 2)         AS total_margin,
    ROUND(AVG(fs.margin_pct), 3)            AS avg_margin_pct,
    COUNT(DISTINCT fs.client_key)           AS active_clients,
    ROUND(SUM(fs.net_amount) /
          NULLIF(COUNT(DISTINCT fs.client_key),0), 2) AS avg_ticket
FROM dw.fact_sales fs
JOIN dw.dim_date        dd ON dd.date_key       = fs.date_key
JOIN dw.dim_zone        dz ON dz.zone_key       = fs.zone_key
JOIN dw.dim_distributor dl ON dl.distributor_key = fs.distributor_key
WHERE fs.is_deleted = FALSE
GROUP BY dd.year_month, dd.year, dd.month_num, dd.month_name,
         dz.zone_code, dz.zone_name,
         dl.distributor_code, dl.distributor_name, dl.distributor_type
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_smz
    ON mart.mv_sales_monthly_zone (year_month, zone_code, distributor_code);

-- ── 2. Materialized View: Year-over-Year Comparison ──────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_sales_yoy AS
SELECT
    cur.year_month,
    cur.year,
    cur.month_num,
    cur.zone_code,
    cur.zone_name,
    cur.total_net                           AS current_sales,
    prev.total_net                          AS prior_year_sales,
    ROUND(cur.total_net - COALESCE(prev.total_net, 0), 2)       AS yoy_variance,
    ROUND(
        CASE WHEN COALESCE(prev.total_net, 0) > 0
             THEN (cur.total_net - prev.total_net) / prev.total_net * 100
             ELSE NULL
        END, 2)                                                  AS yoy_growth_pct,
    cur.active_clients,
    cur.avg_ticket,
    cur.avg_margin_pct
FROM mart.mv_sales_monthly_zone cur
LEFT JOIN mart.mv_sales_monthly_zone prev
       ON prev.year       = cur.year - 1
      AND prev.month_num  = cur.month_num
      AND prev.zone_code  = cur.zone_code
      AND prev.distributor_code = cur.distributor_code
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_yoy
    ON mart.mv_sales_yoy (year, month_num, zone_code);

-- ── 3. Materialized View: Top Clients ────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_top_clients AS
SELECT
    dc.client_key,
    dc.client_id,
    dc.client_name,
    dc.client_type,
    dz.zone_name,
    dci.city_name,
    dsp.full_name                                   AS salesperson,
    dd.year,
    dd.year_month,
    COUNT(DISTINCT fs.invoice_number)               AS invoice_count,
    SUM(fs.quantity)                                AS total_quantity,
    ROUND(SUM(fs.net_amount), 2)                    AS total_net,
    ROUND(SUM(fs.margin_amount), 2)                 AS total_margin,
    ROUND(AVG(fs.margin_pct), 3)                    AS avg_margin_pct,
    ROUND(SUM(fs.net_amount) /
          NULLIF(COUNT(DISTINCT fs.invoice_number),0), 2) AS avg_ticket,
    RANK() OVER (PARTITION BY dd.year
                 ORDER BY SUM(fs.net_amount) DESC)  AS rank_by_year
FROM dw.fact_sales fs
JOIN dw.dim_date        dd  ON dd.date_key       = fs.date_key
JOIN dw.dim_client      dc  ON dc.client_key     = fs.client_key
JOIN dw.dim_zone        dz  ON dz.zone_key       = fs.zone_key
JOIN dw.dim_city        dci ON dci.city_key      = fs.city_key
JOIN dw.dim_salesperson dsp ON dsp.salesperson_key = fs.salesperson_key
WHERE fs.is_deleted = FALSE
GROUP BY dc.client_key, dc.client_id, dc.client_name, dc.client_type,
         dz.zone_name, dci.city_name, dsp.full_name,
         dd.year, dd.year_month
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_tc_year ON mart.mv_top_clients (year, rank_by_year);

-- ── 4. Materialized View: Product Margin Analysis ─────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_product_margin AS
SELECT
    dp.product_key,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,
    dp.presentation,
    dp.rx_otc_flag,
    dl.lab_name,
    dl.lab_country,
    dd.year,
    dd.year_month,
    dz.zone_name,
    SUM(fs.quantity)                                AS total_quantity,
    ROUND(SUM(fs.net_amount), 2)                    AS total_net,
    ROUND(SUM(fs.cost_amount), 2)                   AS total_cost,
    ROUND(SUM(fs.margin_amount), 2)                 AS total_margin,
    ROUND(AVG(fs.margin_pct), 3)                    AS avg_margin_pct,
    ROUND(AVG(fs.unit_price), 4)                    AS avg_unit_price,
    RANK() OVER (PARTITION BY dd.year, dz.zone_name
                 ORDER BY SUM(fs.net_amount) DESC)  AS rank_in_zone_year
FROM dw.fact_sales fs
JOIN dw.dim_date        dd ON dd.date_key    = fs.date_key
JOIN dw.dim_product     dp ON dp.product_key = fs.product_key
JOIN dw.dim_laboratory  dl ON dl.lab_key     = fs.laboratory_key
JOIN dw.dim_zone        dz ON dz.zone_key    = fs.zone_key
WHERE fs.is_deleted = FALSE
GROUP BY dp.product_key, dp.product_id, dp.product_name,
         dp.category, dp.subcategory, dp.presentation, dp.rx_otc_flag,
         dl.lab_name, dl.lab_country,
         dd.year, dd.year_month, dz.zone_name
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_pm_cat  ON mart.mv_product_margin (category, year);
CREATE INDEX IF NOT EXISTS idx_mv_pm_lab  ON mart.mv_product_margin (lab_name, year);

-- ── 5. Materialized View: Commission Summary ─────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_commission_monthly AS
SELECT
    dd.year_month,
    dd.year,
    dd.month_num,
    dsp.salesperson_id,
    dsp.full_name                                   AS salesperson_name,
    dz.zone_name,
    fc.sales_amount,
    fc.target_amount,
    fc.achievement_pct,
    fc.commission_rate * 100                        AS commission_rate_pct,
    fc.commission_amount,
    fc.bonus_amount,
    fc.total_payout,
    fc.paid_flag,
    fc.paid_date,
    RANK() OVER (PARTITION BY dd.year_month
                 ORDER BY fc.sales_amount DESC)     AS rank_in_month
FROM dw.fact_commission fc
JOIN dw.dim_date        dd  ON dd.date_key         = fc.date_key
JOIN dw.dim_salesperson dsp ON dsp.salesperson_key = fc.salesperson_key
JOIN dw.dim_zone        dz  ON dz.zone_key         = fc.zone_key
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_cm_ym ON mart.mv_commission_monthly (year_month);

-- ── Regular Views (no materialization — always fresh) ────────

-- City-level sales for map dashboard
CREATE OR REPLACE VIEW mart.vw_sales_by_city AS
SELECT
    dci.city_name,
    dci.province,
    dci.latitude,
    dci.longitude,
    dz.zone_name,
    dd.year,
    dd.year_month,
    COUNT(DISTINCT fs.invoice_number)   AS invoice_count,
    ROUND(SUM(fs.net_amount), 2)        AS total_net,
    COUNT(DISTINCT fs.client_key)       AS active_clients
FROM dw.fact_sales fs
JOIN dw.dim_date   dd  ON dd.date_key  = fs.date_key
JOIN dw.dim_city   dci ON dci.city_key = fs.city_key
JOIN dw.dim_zone   dz  ON dz.zone_key  = fs.zone_key
WHERE fs.is_deleted = FALSE
GROUP BY dci.city_name, dci.province, dci.latitude, dci.longitude,
         dz.zone_name, dd.year, dd.year_month;

-- Category breakdown for sunburst / pie chart
CREATE OR REPLACE VIEW mart.vw_sales_by_category AS
SELECT
    dp.category,
    dp.subcategory,
    dp.rx_otc_flag,
    dz.zone_name,
    dd.year,
    dd.year_month,
    SUM(fs.quantity)                    AS total_quantity,
    ROUND(SUM(fs.net_amount), 2)        AS total_net,
    ROUND(SUM(fs.margin_amount), 2)     AS total_margin,
    ROUND(AVG(fs.margin_pct), 3)        AS avg_margin_pct
FROM dw.fact_sales fs
JOIN dw.dim_date    dd ON dd.date_key    = fs.date_key
JOIN dw.dim_product dp ON dp.product_key = fs.product_key
JOIN dw.dim_zone    dz ON dz.zone_key    = fs.zone_key
WHERE fs.is_deleted = FALSE
GROUP BY dp.category, dp.subcategory, dp.rx_otc_flag, dz.zone_name, dd.year, dd.year_month;

-- Average ticket per client type
CREATE OR REPLACE VIEW mart.vw_avg_ticket AS
SELECT
    dc.client_type,
    dz.zone_name,
    dd.year,
    dd.year_month,
    COUNT(DISTINCT fs.invoice_number)   AS invoice_count,
    COUNT(DISTINCT fs.client_key)       AS client_count,
    ROUND(SUM(fs.net_amount), 2)        AS total_net,
    ROUND(SUM(fs.net_amount) /
          NULLIF(COUNT(DISTINCT fs.invoice_number), 0), 2) AS avg_ticket
FROM dw.fact_sales fs
JOIN dw.dim_date   dd ON dd.date_key  = fs.date_key
JOIN dw.dim_client dc ON dc.client_key= fs.client_key
JOIN dw.dim_zone   dz ON dz.zone_key  = fs.zone_key
WHERE fs.is_deleted = FALSE
GROUP BY dc.client_type, dz.zone_name, dd.year, dd.year_month;

-- Distributor vs Internal comparison
CREATE OR REPLACE VIEW mart.vw_distributor_vs_internal AS
SELECT
    dl.distributor_type,
    dl.distributor_name,
    dz.zone_name,
    dd.year,
    dd.year_month,
    COUNT(DISTINCT fs.invoice_number)   AS invoice_count,
    SUM(fs.quantity)                    AS total_quantity,
    ROUND(SUM(fs.net_amount), 2)        AS total_net,
    ROUND(SUM(fs.margin_amount), 2)     AS total_margin,
    ROUND(AVG(fs.margin_pct), 3)        AS avg_margin_pct,
    COUNT(DISTINCT fs.client_key)       AS active_clients
FROM dw.fact_sales fs
JOIN dw.dim_date        dd ON dd.date_key        = fs.date_key
JOIN dw.dim_distributor dl ON dl.distributor_key = fs.distributor_key
JOIN dw.dim_zone        dz ON dz.zone_key        = fs.zone_key
WHERE fs.is_deleted = FALSE
GROUP BY dl.distributor_type, dl.distributor_name, dz.zone_name, dd.year, dd.year_month;

-- ── Refresh Function (called nightly by ETL) ──────────────────
CREATE OR REPLACE PROCEDURE mart.refresh_all_mvs()
LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_sales_monthly_zone;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_sales_yoy;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_top_clients;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_product_margin;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_commission_monthly;
    RAISE NOTICE 'All materialized views refreshed at %', NOW();
END;
$$;
