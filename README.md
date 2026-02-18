# Pharma DR · BI Platform
## República Dominicana · Pharmaceutical Sales Intelligence

A production-ready data platform integrating SAP HANA, SQL Server, and 6 Excel
distributor formats into a single web-based BI layer — built for pharmaceutical
sales analytics across the Dominican Republic.

---

## Table of Contents
1. [How to Run (Super Easy!)](#how-to-run-super-easy)
2. [Architecture Overview](#architecture-overview)
3. [Technology Stack](#technology-stack)
4. [Data Sources](#data-sources)
5. [Data Model](#data-model)
6. [ETL Pipeline](#etl-pipeline)
7. [BI Dashboards](#bi-dashboards)
8. [Security & RBAC](#security--rbac)
9. [Performance Tuning](#performance-tuning)
10. [Project Structure](#project-structure)

---

## How to Run (Super Easy!)

> **No Docker. No servers. No accounts. Just Python.**
> If you can double-click a file, you can run this.

---

### Step 1 — Make sure Python is installed

Open the Start Menu, search for **Command Prompt**, and type:

```
python --version
```

You should see something like `Python 3.12.0` or higher.
If you see an error, download Python from https://www.python.org/downloads/ and install it.
During installation, **check the box that says "Add Python to PATH"**.

---

### Step 2 — Install the required tools (one-time only)

In the same Command Prompt window, paste this and press Enter:

```
pip install duckdb pandas streamlit plotly openpyxl rapidfuzz unidecode faker loguru rich tqdm python-dotenv
```

This downloads everything the project needs. It only takes a few minutes.
You only need to do this **once**.

---

### Step 3 — Go to the project folder

In Command Prompt, type:

```
cd "D:\@@@@test@@@@\Dominica"
```

*(Replace the path above with wherever you saved this project.)*

---

### Step 4 — Create the database (one-time only)

Run this to build the data warehouse file:

```
python local_setup\db_init.py
```

Then run this to fill it with 64,000+ realistic sales records:

```
python local_setup\load_data.py
```

You will see a progress bar. It takes about 10 seconds.

---

### Step 5 — Start the dashboard!

```
streamlit run local_setup\app.py
```

Your web browser will open automatically at **http://localhost:8501**

That's it. You are done! 🎉

---

### Or: Just double-click `run.bat`

If you already completed Steps 1 and 2, next time you only need to
**double-click** the file called **`run.bat`** in the project folder.
It does everything automatically and opens your browser.

---

### What you will see

A dashboard with **15 interactive screens** showing pharmaceutical sales data
for the Dominican Republic:

| # | Dashboard |
|---|---|
| 1 | Resumen General — overall KPIs |
| 2 | Mapa de Ventas — sales map by city |
| 3 | Ventas por Zona — by sales zone |
| 4 | Top Clientes — best clients |
| 5 | Top Productos — best-selling medicines |
| 6 | Comisiones — salesperson commissions |
| 7 | Evolución Mensual — monthly trends |
| 8 | Comparativo Año Anterior — year-over-year |
| 9 | Ventas por Laboratorio — by pharmaceutical lab |
| 10 | Distribuidor vs Interno — external vs internal sales |
| 11 | Margen por Producto — profit margins |
| 12 | Ticket Promedio — average order value |
| 13 | Ventas por Categoría — by medicine category |
| 14 | Ranking Vendedores — salesperson leaderboard |
| 15 | Cumplimiento de Metas — target achievement |

Use the **sidebar on the left** to switch between dashboards.
Use the **filters** (Year, Zone, Category) to explore the data.

---

### Troubleshooting

| Problem | Solution |
|---|---|
| `python` is not recognized | Install Python from python.org and check "Add to PATH" |
| `pip` is not recognized | Re-install Python and check "Add to PATH" |
| `streamlit` is not recognized | Run Step 2 again |
| Port 8501 already in use | Run `streamlit run local_setup\app.py --server.port 8502` |
| Database not found error | Run Steps 4 before Step 5 |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  SAP HANA    │  │  SQL Server  │  │  6x Excel Distributor    │  │
│  │ (Incremental │  │ (Historical  │  │  Formats (SFTP / Email / │  │
│  │  CDC/TS)     │  │  One-Time)   │  │  SharePoint)             │  │
│  └──────┬───────┘  └──────┬───────┘  └────────────┬─────────────┘  │
└─────────┼─────────────────┼───────────────────────┼────────────────┘
          │                 │                       │
          ▼                 ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ETL / ELT LAYER  (Python + SQL)              │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  extract/ -> stage/ -> cleanse/ -> map/ -> transform/ -> load/│  │
│  │  Orchestrated by APScheduler  (upgradeable to Airflow)       │   │
│  └──────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────┬─────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE  ·  DuckDB (local) / PostgreSQL (prod)  │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Star Schema: fact_sales, fact_commission                   │    │
│  │  8 dimension tables + analytical views                      │    │
│  └─────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────┬─────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│              BI LAYER  ·  Streamlit + Plotly                        │
│  15 Dashboards · Dynamic Filters · Maps · Drill-Down · Charts       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Local (No Docker) | Production |
|---|---|---|
| **Dashboard** | Streamlit + Plotly | Streamlit / Apache Superset |
| **Data Warehouse** | DuckDB (embedded file) | PostgreSQL 16 |
| **ETL Orchestration** | Python 3.12 + APScheduler | Apache Airflow |
| **Containerization** | Not required | Docker Compose |
| **Cloud** | Runs on any laptop | Azure VM (Standard D4s v3) |
| **SAP HANA Driver** | hdbcli (demo mode w/o real HANA) | hdbcli |
| **SQL Server Driver** | pyodbc + SQLAlchemy | pyodbc + SQLAlchemy |
| **Excel Ingestion** | openpyxl + pandas | openpyxl + pandas |

### Why DuckDB for local, PostgreSQL for production?

- **DuckDB** is a single file — no installation, no server, no configuration.
  Perfect for running locally or sharing the project.
- **PostgreSQL** supports dozens of simultaneous BI tool connections without
  degradation, required for multi-user production environments.

---

## Data Sources

| # | Source | Method | Frequency |
|---|---|---|---|
| 1 | SAP HANA (private cloud) | Incremental CDC via `_CHANGED_AT` timestamp | Every 15 min |
| 2 | SQL Server (historical) | Full one-time migration via pyodbc | One-time + monthly delta |
| 3 | Distributor A — Ramos | Excel (.xlsx) Row-per-invoice format | Daily SFTP |
| 4 | Distributor B — Medifar | Excel (.xls) Pivot-style with merged cells | Weekly email |
| 5 | Distributor C — Farmacorp | CSV disguised as .xlsx | Daily SFTP |
| 6 | Distributor D — AlphaFarma | Multi-sheet Excel workbook | Weekly SharePoint |
| 7 | Distributor E — BioPharma | Excel with dynamic header row | Bi-weekly |
| 8 | Distributor F — MedDist | Excel with combined city+zone column | Weekly |

---

## Data Model

Star Schema — see `architecture/data_model.md` for the full ERD.

**Fact Tables**
- `fact_sales` — transactional sales grain (one row per invoice line)
- `fact_commission` — commission grain (one row per salesperson per month)

**Dimension Tables**
- `dim_date` — calendar spine 2018–2030 with DR holidays
- `dim_product` — 50 pharma SKUs with SAP master codes
- `dim_client` — 200 pharmacies / hospitals / clinics
- `dim_city` — 30 Dominican Republic cities with geo coordinates
- `dim_zone` — 5 sales zones (Norte, Sur, Este, Oeste, Capital)
- `dim_distributor` — 1 internal + 6 external distributors
- `dim_laboratory` — 16 pharmaceutical laboratories
- `dim_salesperson` — 15 sales reps with territory and commission rate

---

## ETL Pipeline

```
etl/
├── config/settings.py          # All connection strings & params
├── extractors/
│   ├── sap_hana_extractor.py   # Incremental CDC/timestamp extraction
│   ├── sqlserver_extractor.py  # One-time + delta migration
│   └── excel_extractor.py      # 6-format adaptive parser
├── transformers/
│   ├── cleansing.py            # Name normalization, dedup, fuzzy matching
│   ├── normalizer.py           # Canonical product/city/client codes
│   └── mapping.py              # Distributor code -> SAP master mapping
├── loaders/
│   └── postgres_loader.py      # Upsert logic + staging -> DW promotion
├── pipelines/
│   ├── full_load_pipeline.py   # Initial / full-refresh run
│   └── incremental_pipeline.py # Scheduled incremental run
└── orchestration/
    └── scheduler.py            # APScheduler job definitions
```

---

## BI Dashboards

| # | Dashboard | Key Charts |
|---|---|---|
| 1 | **Resumen General** | KPI tiles + area chart + treemap |
| 2 | **Mapa de Ventas** | Scatter mapbox by city + top 10 table |
| 3 | **Ventas por Zona** | Stacked bar + grouped bar + pivot heatmap |
| 4 | **Top Clientes** | Ranked bar + scatter (ventas vs ticket) |
| 5 | **Top Productos** | Treemap + horizontal bar + RX vs OTC pie |
| 6 | **Comisiones** | Achievement gauge + dual-axis trend |
| 7 | **Evolución Mensual** | Area + bar dual-axis + ticket trend |
| 8 | **Comparativo Año Anterior** | YoY grouped bar + growth % bar |
| 9 | **Ventas por Laboratorio** | Pie + scatter + line trend |
| 10 | **Distribuidor vs Interno** | Area + pie + KPI table |
| 11 | **Margen por Producto** | Scatter + horizontal bar + heatmap |
| 12 | **Ticket Promedio** | Bar by type + bar by zone + line trend |
| 13 | **Ventas por Categoría** | Sunburst + RX/OTC pie + line trend |
| 14 | **Ranking Vendedores** | Horizontal bar + leaderboard table |
| 15 | **Cumplimiento de Metas** | Achievement bar + 100% target line + trend |

---

## Security & RBAC

| Role | Access |
|---|---|
| `admin` | Full access, all data, all dashboards |
| `gerente_nacional` | All zones, no cost/margin data |
| `gerente_zona` | Own zone only, filtered datasets |
| `vendedor` | Own clients only |
| `distribuidor_ext` | Own sales data only |
| `auditor` | Read-only, all data |

PostgreSQL Row-Level Security (RLS) policies enforce zone-level data isolation
at the database layer — not just the application layer.

---

## Performance Tuning

- `fact_sales` partitioned by year (PostgreSQL declarative partitioning)
- BRIN indexes on date columns for fast range scans
- Composite indexes on `(zone_key, date_key)` and `(product_key, date_key)`
- Materialized views for YoY and commission aggregations (refreshed nightly)
- Superset query result caching via Redis (TTL 5 min operational / 1 h historical)
- Connection pooling via PgBouncer (production only)

---

## Project Structure

```
Dominica/
├── README.md
├── run.bat                      # Double-click to start (Windows)
├── run.ps1                      # PowerShell launcher
├── requirements_local.txt       # Dependencies for local (no Docker) run
├── requirements.txt             # Full production dependencies
├── docker-compose.yml           # Production Docker stack
├── .env.example                 # Environment variable template
├── local_setup/
│   ├── db_init.py               # Creates DuckDB schema
│   ├── load_data.py             # Generates + loads 64 000+ sales rows
│   ├── app.py                   # Streamlit BI dashboard (15 screens)
│   └── pharma_dr.duckdb         # Auto-generated warehouse file
├── architecture/
│   ├── architecture_diagram.md
│   ├── data_model.md
│   └── deployment_guide.md
├── database/
│   ├── 01_schema.sql
│   ├── 02_dimensions.sql
│   ├── 03_facts.sql
│   ├── 04_indexes.sql
│   ├── 05_views.sql
│   └── 06_rbac.sql
├── etl/
│   ├── config/settings.py
│   ├── extractors/
│   ├── transformers/
│   ├── loaders/
│   ├── pipelines/
│   └── orchestration/
├── data/
│   ├── synthetic/generate_data.py
│   ├── excel_samples/
│   └── mappings/product_mapping.csv
├── superset/
│   ├── superset_config.py
│   └── dashboards/
├── scripts/
│   ├── init_db.py
│   ├── deploy_azure.sh
│   └── setup_superset.sh
└── tests/
    ├── test_cleansing.py
    └── test_loaders.py
```
