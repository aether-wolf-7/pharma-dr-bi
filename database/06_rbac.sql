-- ============================================================
-- Pharma DR · Role-Based Access Control (RBAC)
-- PostgreSQL roles aligned with Superset roles
-- ============================================================

-- ── Database Roles ────────────────────────────────────────────

-- ETL write role (used by Python ETL worker)
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pharma_etl') THEN
        CREATE ROLE pharma_etl LOGIN PASSWORD 'CHANGE_IN_ENV';
    END IF;
END $$;

-- Superset read-only role (base for all BI users)
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pharma_bi_reader') THEN
        CREATE ROLE pharma_bi_reader;
    END IF;
END $$;

-- Zone manager role template
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pharma_zona_manager') THEN
        CREATE ROLE pharma_zona_manager;
    END IF;
END $$;

-- Auditor role
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pharma_auditor') THEN
        CREATE ROLE pharma_auditor;
    END IF;
END $$;

-- ── Schema Permissions ────────────────────────────────────────

-- ETL worker: full access to staging, dw; read audit
GRANT USAGE  ON SCHEMA staging      TO pharma_etl;
GRANT ALL    ON ALL TABLES IN SCHEMA staging    TO pharma_etl;
GRANT ALL    ON ALL SEQUENCES IN SCHEMA staging TO pharma_etl;
GRANT USAGE  ON SCHEMA dw           TO pharma_etl;
GRANT ALL    ON ALL TABLES IN SCHEMA dw         TO pharma_etl;
GRANT ALL    ON ALL SEQUENCES IN SCHEMA dw      TO pharma_etl;
GRANT USAGE  ON SCHEMA mart         TO pharma_etl;
GRANT ALL    ON ALL TABLES IN SCHEMA mart       TO pharma_etl;
GRANT USAGE  ON SCHEMA audit        TO pharma_etl;
GRANT ALL    ON ALL TABLES IN SCHEMA audit      TO pharma_etl;
GRANT ALL    ON ALL SEQUENCES IN SCHEMA audit   TO pharma_etl;

-- BI Reader: SELECT on dw + mart only (no staging, no cost data by default)
GRANT USAGE  ON SCHEMA dw           TO pharma_bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dw         TO pharma_bi_reader;
GRANT USAGE  ON SCHEMA mart         TO pharma_bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mart       TO pharma_bi_reader;

-- Auditor: SELECT on all schemas
GRANT pharma_bi_reader TO pharma_auditor;
GRANT USAGE  ON SCHEMA staging      TO pharma_auditor;
GRANT SELECT ON ALL TABLES IN SCHEMA staging    TO pharma_auditor;
GRANT USAGE  ON SCHEMA audit        TO pharma_auditor;
GRANT SELECT ON ALL TABLES IN SCHEMA audit      TO pharma_auditor;

-- Zone manager inherits bi_reader
GRANT pharma_bi_reader TO pharma_zona_manager;

-- ── Row-Level Security (RLS) ──────────────────────────────────
-- Enable RLS on fact_sales for zone filtering
ALTER TABLE dw.fact_sales ENABLE ROW LEVEL SECURITY;
ALTER TABLE dw.fact_sales FORCE ROW LEVEL SECURITY;

-- ETL and admin bypass RLS
CREATE POLICY rls_etl_bypass ON dw.fact_sales
    FOR ALL TO pharma_etl USING (TRUE);

-- Admin bypass (pharma_admin is superuser — RLS bypassed automatically)

-- Zone manager: see only own zone
-- Superset passes current_setting('app.zone_code') via connection attribute
CREATE POLICY rls_zona_manager ON dw.fact_sales
    FOR SELECT TO pharma_zona_manager
    USING (
        zone_key IN (
            SELECT zone_key FROM dw.dim_zone
            WHERE zone_code = current_setting('app.zone_code', TRUE)
        )
    );

-- BI reader (no RLS restriction — sees all zones by default)
CREATE POLICY rls_bi_reader ON dw.fact_sales
    FOR SELECT TO pharma_bi_reader USING (TRUE);

-- ── Superset Virtual Roles (documented for configuration) ─────
/*
Superset Role Mapping:
─────────────────────────────────────────────────────────────────
| Superset Role    | DB Role              | Row Filter Applied   |
|──────────────────|──────────────────────|─────────────────────|
| Admin            | pharma_admin         | None (all data)      |
| Gerente Nacional | pharma_bi_reader     | None (all zones)     |
| Gerente Zona CAP | pharma_zona_manager  | zone_code = 'CAP'    |
| Gerente Zona NOR | pharma_zona_manager  | zone_code = 'NOR'    |
| Gerente Zona EST | pharma_zona_manager  | zone_code = 'EST'    |
| Gerente Zona SUR | pharma_zona_manager  | zone_code = 'SUR'    |
| Gerente Zona OES | pharma_zona_manager  | zone_code = 'OES'    |
| Vendedor         | pharma_bi_reader     | salesperson_key = X  |
| Auditor          | pharma_auditor       | None (all, read-only)|
| Distribuidor Ext | pharma_bi_reader     | distributor_key = X  |
─────────────────────────────────────────────────────────────────

In Superset, Row-Level Security is configured via:
Settings → Security → Row Level Security
Dataset: fact_sales (or derived datasets)
Filter: zone_key in (SELECT zone_key FROM dw.dim_zone WHERE zone_code = '{{ current_user_zone }}')
*/

-- ── Default Privileges (future tables automatically inherit) ──
ALTER DEFAULT PRIVILEGES IN SCHEMA dw
    GRANT SELECT ON TABLES TO pharma_bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart
    GRANT SELECT ON TABLES TO pharma_bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw
    GRANT ALL ON TABLES TO pharma_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT ALL ON TABLES TO pharma_etl;
