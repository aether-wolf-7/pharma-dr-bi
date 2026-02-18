-- ============================================================
-- Pharma DR · Fact Tables
-- ============================================================

-- ── fact_sales (Partitioned by year via date_key range) ───────
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    sale_key            BIGSERIAL,
    date_key            INT          NOT NULL,
    product_key         INT          NOT NULL,
    client_key          INT          NOT NULL,
    city_key            INT          NOT NULL,
    zone_key            INT          NOT NULL,
    distributor_key     INT          NOT NULL,
    laboratory_key      INT          NOT NULL,
    salesperson_key     INT          NOT NULL,
    invoice_number      VARCHAR(50)  NOT NULL,
    invoice_line        SMALLINT     NOT NULL DEFAULT 1,
    quantity            NUMERIC(12,3) NOT NULL,
    unit_price          NUMERIC(12,4) NOT NULL,
    gross_amount        NUMERIC(15,2) NOT NULL,
    discount_pct        NUMERIC(5,2)  NOT NULL DEFAULT 0,
    discount_amount     NUMERIC(15,2) NOT NULL DEFAULT 0,
    net_amount          NUMERIC(15,2) NOT NULL,
    cost_amount         NUMERIC(15,2),
    margin_amount       NUMERIC(15,2),
    margin_pct          NUMERIC(6,3),
    source_system       VARCHAR(20)  NOT NULL,
    source_record_id    VARCHAR(200),
    load_timestamp      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_deleted          BOOLEAN      NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_fact_sales PRIMARY KEY (sale_key),
    CONSTRAINT fk_fs_date        FOREIGN KEY (date_key)        REFERENCES dw.dim_date(date_key),
    CONSTRAINT fk_fs_product     FOREIGN KEY (product_key)     REFERENCES dw.dim_product(product_key),
    CONSTRAINT fk_fs_client      FOREIGN KEY (client_key)      REFERENCES dw.dim_client(client_key),
    CONSTRAINT fk_fs_city        FOREIGN KEY (city_key)        REFERENCES dw.dim_city(city_key),
    CONSTRAINT fk_fs_zone        FOREIGN KEY (zone_key)        REFERENCES dw.dim_zone(zone_key),
    CONSTRAINT fk_fs_dist        FOREIGN KEY (distributor_key) REFERENCES dw.dim_distributor(distributor_key),
    CONSTRAINT fk_fs_lab         FOREIGN KEY (laboratory_key)  REFERENCES dw.dim_laboratory(lab_key),
    CONSTRAINT fk_fs_sp          FOREIGN KEY (salesperson_key) REFERENCES dw.dim_salesperson(salesperson_key),
    CONSTRAINT uq_fs_source      UNIQUE (source_system, source_record_id, invoice_line)
);

COMMENT ON TABLE dw.fact_sales IS
'Transactional sales fact table — grain: one row per invoice line item.
Source systems: SAP_HANA, SQL_SERVER, DIST_A through DIST_F.
Partitioned by date_key (YYYYMMDD integer ranges) — see 04_indexes.sql.';

-- ── fact_commission ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.fact_commission (
    commission_key      SERIAL PRIMARY KEY,
    date_key            INT          NOT NULL,    -- First day of month YYYYMM01
    salesperson_key     INT          NOT NULL,
    zone_key            INT          NOT NULL,
    sales_amount        NUMERIC(15,2) NOT NULL DEFAULT 0,
    target_amount       NUMERIC(15,2) NOT NULL DEFAULT 0,
    achievement_pct     NUMERIC(6,3)  NOT NULL DEFAULT 0,
    commission_rate     NUMERIC(5,3)  NOT NULL DEFAULT 0,
    commission_amount   NUMERIC(15,2) NOT NULL DEFAULT 0,
    bonus_amount        NUMERIC(15,2) NOT NULL DEFAULT 0,
    total_payout        NUMERIC(15,2) NOT NULL DEFAULT 0,
    paid_flag           BOOLEAN       NOT NULL DEFAULT FALSE,
    paid_date           DATE,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_fc_date FOREIGN KEY (date_key)        REFERENCES dw.dim_date(date_key),
    CONSTRAINT fk_fc_sp   FOREIGN KEY (salesperson_key) REFERENCES dw.dim_salesperson(salesperson_key),
    CONSTRAINT fk_fc_zone FOREIGN KEY (zone_key)        REFERENCES dw.dim_zone(zone_key),
    CONSTRAINT uq_fc      UNIQUE (date_key, salesperson_key)
);

COMMENT ON TABLE dw.fact_commission IS
'Monthly commission fact — grain: one row per salesperson per month.
Computed by ETL pipeline after fact_sales load. Includes bonus for >100%% achievement.';

-- ── Stored Procedure: Recalculate Monthly Commissions ─────────
CREATE OR REPLACE PROCEDURE dw.recalculate_commissions(p_year INT, p_month INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_date_key INT := (p_year * 10000 + p_month * 100 + 1);
BEGIN
    INSERT INTO dw.fact_commission (
        date_key, salesperson_key, zone_key,
        sales_amount, target_amount, achievement_pct,
        commission_rate, commission_amount, bonus_amount, total_payout
    )
    SELECT
        v_date_key,
        sp.salesperson_key,
        sp.zone_key,
        COALESCE(SUM(fs.net_amount), 0)                             AS sales_amount,
        sp.monthly_target                                           AS target_amount,
        CASE WHEN sp.monthly_target > 0
             THEN ROUND(COALESCE(SUM(fs.net_amount),0) / sp.monthly_target * 100, 3)
             ELSE 0 END                                             AS achievement_pct,
        sp.commission_rate,
        ROUND(COALESCE(SUM(fs.net_amount),0) * sp.commission_rate, 2) AS commission_amount,
        -- Bonus: 10% extra on commission for >100% achievement
        CASE
            WHEN sp.monthly_target > 0
                 AND COALESCE(SUM(fs.net_amount),0) >= sp.monthly_target
            THEN ROUND(COALESCE(SUM(fs.net_amount),0) * sp.commission_rate * 0.10, 2)
            ELSE 0
        END                                                         AS bonus_amount,
        ROUND(COALESCE(SUM(fs.net_amount),0) * sp.commission_rate
            + CASE
                WHEN sp.monthly_target > 0
                     AND COALESCE(SUM(fs.net_amount),0) >= sp.monthly_target
                THEN COALESCE(SUM(fs.net_amount),0) * sp.commission_rate * 0.10
                ELSE 0
              END, 2)                                               AS total_payout
    FROM dw.dim_salesperson sp
    LEFT JOIN dw.fact_sales fs
           ON fs.salesperson_key = sp.salesperson_key
          AND fs.date_key / 100 = p_year * 100 + p_month   -- YYYYMM match
          AND fs.is_deleted = FALSE
    WHERE sp.active_flag = TRUE
    GROUP BY sp.salesperson_key, sp.zone_key, sp.monthly_target, sp.commission_rate
    ON CONFLICT (date_key, salesperson_key)
    DO UPDATE SET
        sales_amount      = EXCLUDED.sales_amount,
        target_amount     = EXCLUDED.target_amount,
        achievement_pct   = EXCLUDED.achievement_pct,
        commission_rate   = EXCLUDED.commission_rate,
        commission_amount = EXCLUDED.commission_amount,
        bonus_amount      = EXCLUDED.bonus_amount,
        total_payout      = EXCLUDED.total_payout,
        updated_at        = NOW();

    RAISE NOTICE 'Commission recalculation complete for %-%', p_year, p_month;
END;
$$;
