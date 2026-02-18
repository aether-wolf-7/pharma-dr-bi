# Technical Architecture Diagram
## Pharma DR · Production Data Platform

---

## Infrastructure Layout (Azure)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AZURE RESOURCE GROUP: rg-pharma-dr                  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │          Azure VM: Standard D4s v3  (4 vCPU / 16 GB RAM)            │  │
│  │          Ubuntu 22.04 LTS · Public IP · NSG (443, 8088, 5432*)       │  │
│  │                                                                      │  │
│  │   ┌─────────────────────────────────────────────────────────────┐   │  │
│  │   │              Docker Compose Stack                           │   │  │
│  │   │                                                             │   │  │
│  │   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │   │  │
│  │   │  │  PostgreSQL  │  │    Redis     │  │ Apache Superset  │  │   │  │
│  │   │  │  16 (port    │  │  7 (cache +  │  │  4.x (port 8088) │  │   │  │
│  │   │  │  5432)       │  │  Celery)     │  │  + Nginx 443     │  │   │  │
│  │   │  └──────┬───────┘  └──────────────┘  └──────────────────┘  │   │  │
│  │   │         │                                                    │   │  │
│  │   │  ┌──────▼───────┐  ┌──────────────┐                        │   │  │
│  │   │  │  PgBouncer   │  │  ETL Worker  │                        │   │  │
│  │   │  │ (conn pool)  │  │  (Python +   │                        │   │  │
│  │   │  └──────────────┘  │  APScheduler)│                        │   │  │
│  │   │                    └──────────────┘                        │   │  │
│  │   └─────────────────────────────────────────────────────────────┘   │  │
│  │                                                                      │  │
│  │   ┌─────────────────────────────────────────────────────────────┐   │  │
│  │   │         Azure Managed Disks                                 │   │  │
│  │   │  /data/postgres  (P30 512GB SSD)                            │   │  │
│  │   │  /data/etl       (P10 128GB SSD)                            │   │  │
│  │   └─────────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  Azure Blob Storage (Cold Archive + Excel Landing Zone)              │  │
│  │  Container: raw-excel-feeds  |  Container: etl-logs  |  Backups     │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  Azure Key Vault  (Secrets: DB passwords, SAP HANA key, SMTP creds)  │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
          ▲                ▲                         ▲
          │                │                         │
   VPN/ExpressRoute   SQL Server               End Users
   SAP HANA (on-prem)  (on-prem/Azure SQL)    (HTTPS / browser)
```

---

## Data Flow Diagram

```
EXTRACTION PHASE
────────────────
SAP HANA ──[hdbcli/JDBC]──►  staging.sap_raw          (incremental, 15 min)
SQL Server ─[pyodbc]──────►  staging.sqlsrv_raw        (one-time migration)
Excel A-F ─[openpyxl]─────►  staging.excel_raw_<dist>  (scheduled per dist)

STAGING PHASE (DuckDB scratch)
───────────────────────────────
staging.* → DuckDB in-memory profiling → quality report → reject table if fail

TRANSFORMATION PHASE
─────────────────────
cleansing.py:
  · Normalize city names  (fuzzy match → dim_city.city_name)
  · Normalize product names (Levenshtein → dim_product.product_name)
  · Deduplicate invoices  (composite key: source + invoice + line)
  · Standardize RNC/NIT client codes
  · Fix numeric fields (comma vs dot decimal, text "1,234.56" → 1234.56)

mapping.py:
  · Distributor product code → SAP product_id  (CSV lookup table)
  · Unmapped codes → staging.unmapped_products (alert + manual review)

normalizer.py:
  · Assign zone_key from city_key
  · Assign laboratory_key from product_key
  · Calculate margin from cost + price
  · Validate referential integrity vs all dimension tables

LOAD PHASE
───────────
dw.fact_sales ← UPSERT on (source_system, source_record_id, invoice_line)
dw.fact_commission ← Recalculate monthly after sales load

MART REFRESH
─────────────
REFRESH MATERIALIZED VIEW mart.mv_sales_monthly_zone;
REFRESH MATERIALIZED VIEW mart.mv_sales_yoy;
... (all 5 MVs)

SUPERSET CACHE INVALIDATION
─────────────────────────────
POST /api/v1/cache/invalidate  (Superset API)
```

---

## Deployment Architecture (Docker Compose)

```yaml
Services:
  postgres:   PostgreSQL 16 — primary data store
  redis:      Redis 7     — Superset cache + Celery broker
  superset:   Apache Superset 4.x — BI layer
  nginx:      Reverse proxy — SSL termination (port 443→8088)
  etl:        Python ETL worker — APScheduler
  pgbouncer:  Connection pooler (max_client_conn=200, pool_size=20)

Networks:
  pharma-internal: postgres, redis, superset, etl, pgbouncer
  pharma-public:   nginx

Volumes:
  postgres-data:   /var/lib/postgresql/data
  superset-home:   /app/superset_home
  etl-data:        /app/data
```

---

## Scheduling Architecture

```
APScheduler (MVP)                  Airflow DAGs (Production upgrade)
─────────────────                  ────────────────────────────────
Every 15 min:                      DAG: sap_hana_incremental
  sap_hana_incremental_job           Task: extract → Task: stage →
                                     Task: transform → Task: load →
                                     Task: refresh_mvs
Every 1 h:
  excel_distributor_scan_job       DAG: excel_distributors_daily
                                     6 parallel Tasks (one per dist)
Every night 02:00:
  mv_refresh_job                   DAG: mart_refresh_nightly
  commission_calc_job

Monthly (1st day, 06:00):
  commission_payment_job           DAG: commission_monthly
  sqlserver_delta_job
```

---

## Security Architecture

```
Internet → Nginx (SSL/TLS 1.3, Let's Encrypt) → Superset (port 8088)
                                                     │
                                              Superset AuthDB (Postgres)
                                                     │
                                              Row-Level Security (RLS)
                                              per role per dataset
                                                     │
                                              PgBouncer (auth_type=scram-sha-256)
                                                     │
                                              PostgreSQL (pg_hba.conf: local only)

Secrets: Azure Key Vault → etl container env vars (MSI auth)
Backups: pg_dump nightly → Azure Blob (GRS) → 30-day retention
Audit:   PostgreSQL pgaudit extension → Azure Log Analytics
```
