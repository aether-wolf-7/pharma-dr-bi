# Apache Superset · Dashboard Definitions
## Pharma DR · 15 Dashboards

---

## Setup Instructions

### 1. Connect Database
1. Settings → Database Connections → + Database
2. Name: `Pharma DR Warehouse`
3. SQLAlchemy URI: `postgresql+psycopg2://pharma_admin:PASSWORD@pgbouncer:6432/pharma_dr`
4. Enable: Async queries, Allow DML, Allow CREATE TABLE AS

### 2. Create Datasets
For each dataset listed below, go to: Data → Datasets → + Dataset
Select `Pharma DR Warehouse` and the schema/table/view.

### 3. Create Charts → Assemble Dashboards

---

## Datasets Required

| Dataset Name | Schema.Object | Type |
|---|---|---|
| `ds_fact_sales` | `dw.fact_sales` | Physical table |
| `ds_sales_monthly_zone` | `mart.mv_sales_monthly_zone` | Materialized view |
| `ds_sales_yoy` | `mart.mv_sales_yoy` | Materialized view |
| `ds_top_clients` | `mart.mv_top_clients` | Materialized view |
| `ds_product_margin` | `mart.mv_product_margin` | Materialized view |
| `ds_commission` | `mart.mv_commission_monthly` | Materialized view |
| `ds_city_sales` | `mart.vw_sales_by_city` | View |
| `ds_category_sales` | `mart.vw_sales_by_category` | View |
| `ds_avg_ticket` | `mart.vw_avg_ticket` | View |
| `ds_dist_vs_internal` | `mart.vw_distributor_vs_internal` | View |

---

## Dashboard 1 — Mapa de Ventas (DR Map)

**Purpose:** Geographic view of sales across Dominican Republic cities.

**Charts:**
1. **Choropleth Map** (Country Map chart type)
   - Dataset: `ds_city_sales`
   - Metric: `SUM(total_net)`
   - Dimension: `city_name`
   - Color scheme: Blue-to-green sequential
   - Tooltip: city_name, total_net, active_clients

2. **Bubble Map**
   - Dataset: `ds_city_sales`
   - Latitude: `latitude`, Longitude: `longitude`
   - Point size: `total_net`
   - Point color: `zone_name`

3. **Bar Chart — Top 10 Cities**
   - Dataset: `ds_city_sales`
   - Metric: `SUM(total_net)`
   - Group by: `city_name`
   - Sort: DESC, limit 10

**Filters:**
- Date range: `full_date`
- Zone: `zone_name`
- Year: `year`

---

## Dashboard 2 — Ventas por Zona

**Charts:**
1. **Stacked Bar** — Monthly sales by zone
   - Dataset: `ds_sales_monthly_zone`
   - X-axis: `year_month`
   - Metric: `SUM(total_net)`
   - Group by: `zone_name`

2. **Donut Chart** — Zone share (%)
   - Dataset: `ds_sales_monthly_zone`
   - Metric: `SUM(total_net)`
   - Dimensions: `zone_name`

3. **Big Number** — Total sales per zone (4 tiles)
4. **Table** — Zone KPI table (Net, Margin %, Clients, Invoices)

---

## Dashboard 3 — Top Clientes

**Charts:**
1. **Table (ranked)** — Client leaderboard
   - Dataset: `ds_top_clients`
   - Columns: rank_by_year, client_name, client_type, zone_name, total_net, avg_ticket, avg_margin_pct

2. **Pareto Chart** (Bar + Line)
   - Dataset: `ds_top_clients`
   - Bar: `total_net` by `client_name` (top 20)
   - Line: cumulative % (calculated field)

3. **Pie Chart** — Client type distribution
   - Dataset: `ds_top_clients`
   - Metric: `SUM(total_net)`
   - Group by: `client_type`

4. **Scatter Plot** — Net Sales vs Avg Ticket
   - Dataset: `ds_top_clients`
   - X: `avg_ticket`, Y: `total_net`
   - Size: `invoice_count`
   - Color: `client_type`

---

## Dashboard 4 — Top Productos Farmacéuticos

**Charts:**
1. **Treemap** — Products by net sales
   - Dataset: `ds_product_margin`
   - Metric: `SUM(total_net)`
   - Group by: `category`, `product_name`
   - Color: `avg_margin_pct`

2. **Horizontal Bar** — Top 15 products by revenue
3. **Table** — Full product ranking with margin %
4. **Pie** — Sales by category (ANTIBIOTICO, CARDIOVASCULAR, etc.)

---

## Dashboard 5 — Comisiones

**Charts:**
1. **Table** — Commission leaderboard by month
   - Dataset: `ds_commission`
   - Columns: rank_in_month, salesperson_name, zone_name, sales_amount, target_amount, achievement_pct, total_payout, paid_flag

2. **Bullet Chart / Gauge** — Achievement % per salesperson
   - Custom: `achievement_pct` vs 100% target

3. **Big Number** — Total commission payout (month)

4. **Bar** — Commission amount by salesperson

5. **Heatmap** — Monthly achievement by salesperson (rows) × month (cols)

---

## Dashboard 6 — Evolución Mensual

**Charts:**
1. **Time-Series Line** — Net sales trend 2021–2024
   - Dataset: `ds_sales_monthly_zone`
   - X: `year_month`, Y: `SUM(total_net)`
   - Multiple series: one per zone

2. **Area Chart** — Stacked monthly sales by distributor type

3. **Bar + Line combo** — Invoices (bar) + Avg Ticket (line)

4. **Big Numbers** — Current month vs prior month

---

## Dashboard 7 — Comparativo Año Anterior (YoY)

**Charts:**
1. **Grouped Bar** — Current year vs prior year by month
   - Dataset: `ds_sales_yoy`
   - X: `month_num` (label: `month_name`)
   - Series: `current_sales` vs `prior_year_sales`
   - Color coding: current = #2ECC71, prior = #95A5A6

2. **Big Number** — YoY growth % (current year total)

3. **Line Chart** — YoY growth % trend line

4. **Table** — Month-by-month YoY comparison with variance

---

## Dashboard 8 — Ventas por Laboratorio

**Charts:**
1. **Pie / Donut** — Lab revenue share
   - Dataset: `ds_product_margin`
   - Metric: `SUM(total_net)`
   - Group by: `lab_name`

2. **Stacked Bar** — Lab sales trend by month

3. **Table** — Lab KPIs: Revenue, Margin %, Top Product, Unit Volume

4. **Scatter** — Lab revenue vs avg margin %
   - X: `avg_margin_pct`, Y: `SUM(total_net)`
   - Point label: `lab_name`

---

## Dashboard 9 — Distribuidor vs Ventas Internas

**Charts:**
1. **Side-by-Side Bar** — Internal vs External by month
   - Dataset: `ds_dist_vs_internal`
   - X: `year_month`
   - Group by: `distributor_type`
   - Metric: `SUM(total_net)`

2. **Pie** — Share: Internal vs External

3. **Table** — Per-distributor KPIs (Revenue, Margin, Clients, Invoices)

4. **Waterfall Chart** — Contribution of each distributor

---

## Dashboard 10 — Margen por Producto

**Charts:**
1. **Waterfall** — Margin contribution by category
2. **Scatter** — Unit price vs margin %
   - Dataset: `ds_product_margin`
   - X: `avg_unit_price`, Y: `avg_margin_pct`
   - Point size: `total_quantity`
   - Color: `category`

3. **Heatmap** — Margin % by product × zone

4. **Table** — Full product margin table sorted by avg_margin_pct DESC

---

## Dashboard 11 — Ticket Promedio por Cliente

**Charts:**
1. **Box Plot** — Avg ticket distribution by client type
   - Dataset: `ds_avg_ticket`
   - X: `client_type`
   - Y: `avg_ticket`

2. **Line** — Avg ticket trend by month

3. **Bar** — Top 20 clients by avg ticket

4. **Big Number** — Overall avg ticket (current month)

---

## Dashboard 12 — Ventas por Categoría (OTC/Rx/Vitaminas)

**Charts:**
1. **Sunburst** — Category → Subcategory → Product
   - Dataset: `ds_category_sales`
   - Hierarchy: `category` → `subcategory`
   - Metric: `SUM(total_net)`

2. **Pie** — RX vs OTC split

3. **Stacked Bar** — Category trend by month

4. **Table** — Category KPIs

---

## Dashboard 13 — Ranking Vendedores

**Charts:**
1. **Leaderboard Table** — Salesperson ranking
   - Dataset: `ds_commission`
   - Columns: rank, salesperson_name, zone_name, sales_amount, achievement_pct, total_payout

2. **Bar** — Monthly sales by salesperson

3. **Gauge per salesperson** — Achievement % (15 gauges or scrollable)

4. **Scatter** — Salesperson: sales vs achievement %

---

## Dashboard 14 — Cumplimiento de Metas

**Charts:**
1. **Bullet Chart** (or Gauge) — Overall company achievement %
   - Metric: `SUM(sales_amount) / SUM(target_amount) * 100`
   - Color: Red (<80%), Yellow (80–99%), Green (≥100%)

2. **Bar** — Achievement % by zone (horizontal)

3. **Table** — Monthly target vs actual by salesperson

4. **Trend Line** — Achievement % over time (last 12 months)

---

## Dashboard 15 — Drill-Down General (Master Dashboard)

**Purpose:** Cross-filter hub dashboard combining all KPIs with drill-down navigation.

**Charts (full-screen layout):**
1. **Row 1 — KPI Tiles (4 Big Numbers)**
   - Total Net Sales (YTD)
   - YoY Growth %
   - Avg Margin %
   - Avg Ticket

2. **Row 2 — Primary Charts**
   - Left: Monthly trend (Line, 2 years)
   - Right: Zone breakdown (Donut)

3. **Row 3 — Supporting Charts**
   - Top Products (Treemap)
   - Top Clients (Table)

4. **Row 4 — Secondary Charts**
   - Lab breakdown (Pie)
   - Salesperson ranking (Bar)

**Cross-Filter Configuration:**
- All charts are cross-filter sources AND targets
- Clicking a zone → all charts filter to that zone
- Clicking a product category → all charts filter to that category
- Clicking a month → all charts filter to that month

**Native Filters:**
- Date range picker
- Zone multi-select
- Distributor type selector
- Product category multi-select
- Client type multi-select

**Drill-Down Navigation:**
- Each chart has "Go to dashboard" links to detailed dashboards
- E.g., Click product → Dashboard 4 (Top Productos)
- E.g., Click client → Dashboard 3 (Top Clientes)

---

## Global Native Filter Configuration

Apply these filters to ALL dashboards using Superset's Native Filters:

```
Filter 1: Fecha (Date Range)
  Type: Time Range
  Column: full_date (or year_month)
  Default: Last 12 months
  Scope: All charts

Filter 2: Zona
  Type: Select
  Column: zone_name / zone_code
  Options: Capital, Norte, Este, Sur, Oeste
  Allow multiple: Yes
  Default: All

Filter 3: Distribuidor Type
  Type: Select
  Column: distributor_type
  Options: INTERNO, EXTERNO
  Allow multiple: Yes

Filter 4: Categoría Producto
  Type: Select
  Column: category
  Options: (from dim_product)
  Allow multiple: Yes

Filter 5: Año
  Type: Select
  Column: year
  Options: 2021, 2022, 2023, 2024
  Allow multiple: Yes
```

---

## Role-Based Dashboard Visibility

| Role | Accessible Dashboards |
|---|---|
| `Admin` | All 15 |
| `Gerente Nacional` | All 15 |
| `Gerente Zona` | 1, 2, 3, 4, 6, 7, 8, 9, 12, 15 (own zone data only) |
| `Vendedor` | 3, 11, 13, 14 (own clients only) |
| `Distribuidor Externo` | 9 (own data only) |
| `Auditor` | All 15 (read-only) |

---

## Performance Optimization Tips

1. **Cache timeout by dashboard type:**
   - Real-time (ops dashboards): 5 min
   - Historical analysis: 1 hour
   - YoY / annual: 24 hours

2. **Async queries enabled** for all heavy dashboards

3. **Use materialized views** (`mart.*`) for all aggregation charts — never query `fact_sales` directly for summary charts

4. **Dashboard filter pre-fetch:** Enable in dashboard settings to pre-compute filter values on load

5. **Limit time range defaults** to 12 months to avoid full-table scans on first load
