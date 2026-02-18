# Data Model — Star Schema
## Pharma DR · Centralized Reporting Platform

---

## Entity-Relationship Diagram (Star Schema)

```
                              ┌─────────────────────┐
                              │     dim_date         │
                              │─────────────────────│
                              │ PK date_key (int)    │
                              │    full_date         │
                              │    year              │
                              │    quarter           │
                              │    month_num         │
                              │    month_name        │
                              │    week_num          │
                              │    day_of_week       │
                              │    is_weekend        │
                              │    is_holiday_dr     │
                              └──────────┬──────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
   ┌──────────▼──────────┐   ┌──────────▼──────────┐   ┌──────────▼──────────┐
   │    dim_product       │   │     fact_sales       │   │    dim_client        │
   │─────────────────────│   │─────────────────────│   │─────────────────────│
   │ PK product_key       │◄──│ PK sale_key (bigint) │──►│ PK client_key        │
   │    product_id (SAP)  │   │ FK date_key          │   │    client_id         │
   │    product_name      │   │ FK product_key       │   │    client_name       │
   │    generic_name      │   │ FK client_key        │   │    client_type       │
   │    category          │   │ FK city_key          │   │    nit_rnc           │
   │    subcategory       │   │ FK zone_key          │   │    address           │
   │    presentation      │   │ FK distributor_key   │   │    phone             │
   │    concentration     │   │ FK laboratory_key    │   │    email             │
   │    lab_key (FK)      │   │ FK salesperson_key   │   │    credit_limit      │
   │    rx_otc_flag       │   │    invoice_number    │   │    payment_terms     │
   │    controlled_flag   │   │    quantity          │   │ FK city_key          │
   │    unit_cost         │   │    unit_price        │   │ FK zone_key          │
   │    sat_code          │   │    gross_amount      │   │ FK salesperson_key   │
   │    active_flag       │   │    discount_amount   │   │    active_flag       │
   └─────────────────────┘   │    net_amount        │   └─────────────────────┘
                              │    cost_amount       │
   ┌─────────────────────┐   │    margin_amount     │   ┌─────────────────────┐
   │   dim_laboratory     │   │    margin_pct        │   │    dim_zone          │
   │─────────────────────│   │    source_system     │   │─────────────────────│
   │ PK lab_key           │◄──│    load_timestamp    │   │ PK zone_key          │
   │    lab_name          │   └──────────┬──────────┘   │    zone_code         │
   │    lab_country       │              │               │    zone_name         │
   │    lab_type          │   ┌──────────▼──────────┐   │    zone_manager      │
   │    contact_email     │   │  fact_commission     │   │    region            │
   │    active_flag       │   │─────────────────────│   └──────────┬──────────┘
   └─────────────────────┘   │ PK commission_key    │              │
                              │ FK date_key (month)  │   ┌──────────▼──────────┐
   ┌─────────────────────┐   │ FK salesperson_key   │   │     dim_city         │
   │  dim_distributor     │   │ FK zone_key          │   │─────────────────────│
   │─────────────────────│   │    sales_amount      │   │ PK city_key          │
   │ PK distributor_key   │◄──│    target_amount     │   │    city_name         │
   │    distributor_code  │   │    achievement_pct   │   │    province          │
   │    distributor_name  │   │    commission_rate   │   │    region            │
   │    distributor_type  │   │    commission_amount │   │    latitude          │
   │    contact_name      │   │    bonus_amount      │   │    longitude         │
   │    excel_format_id   │   │    total_payout      │   │    population        │
   │    active_flag       │   │    paid_flag         │   │ FK zone_key          │
   └─────────────────────┘   └─────────────────────┘   └─────────────────────┘

   ┌─────────────────────┐
   │  dim_salesperson     │
   │─────────────────────│
   │ PK salesperson_key   │
   │    salesperson_id    │
   │    full_name         │
   │    email             │
   │    phone             │
   │ FK zone_key          │
   │    hire_date         │
   │    commission_rate   │
   │    monthly_target    │
   │    active_flag       │
   └─────────────────────┘
```

---

## Table Specifications

### Fact Tables

#### `dw.fact_sales`
| Column | Type | Description |
|---|---|---|
| `sale_key` | BIGSERIAL PK | Surrogate key |
| `date_key` | INT FK | Reference to dim_date |
| `product_key` | INT FK | Reference to dim_product |
| `client_key` | INT FK | Reference to dim_client |
| `city_key` | INT FK | Reference to dim_city |
| `zone_key` | INT FK | Reference to dim_zone |
| `distributor_key` | INT FK | Reference to dim_distributor |
| `laboratory_key` | INT FK | Reference to dim_laboratory |
| `salesperson_key` | INT FK | Reference to dim_salesperson |
| `invoice_number` | VARCHAR(50) | Source invoice/document number |
| `invoice_line` | SMALLINT | Line number within invoice |
| `quantity` | NUMERIC(12,3) | Units sold |
| `unit_price` | NUMERIC(12,4) | Price per unit (RD$) |
| `gross_amount` | NUMERIC(15,2) | quantity × unit_price |
| `discount_pct` | NUMERIC(5,2) | Discount percentage |
| `discount_amount` | NUMERIC(15,2) | Computed discount in RD$ |
| `net_amount` | NUMERIC(15,2) | gross - discount |
| `cost_amount` | NUMERIC(15,2) | Cost from SAP HANA |
| `margin_amount` | NUMERIC(15,2) | net - cost |
| `margin_pct` | NUMERIC(6,3) | margin / net × 100 |
| `source_system` | VARCHAR(20) | 'SAP_HANA','SQL_SERVER','DIST_A'…'DIST_F' |
| `source_record_id` | VARCHAR(100) | Natural key from source |
| `load_timestamp` | TIMESTAMPTZ | ETL load time |
| `is_deleted` | BOOLEAN | Soft-delete flag |

**Grain:** One row per invoice line item.
**Estimated volume:** ~2M rows/year, ~10M rows over 5-year history.

#### `dw.fact_commission`
| Column | Type | Description |
|---|---|---|
| `commission_key` | SERIAL PK | Surrogate key |
| `date_key` | INT FK | Month-level date key |
| `salesperson_key` | INT FK | Reference to dim_salesperson |
| `zone_key` | INT FK | Reference to dim_zone |
| `sales_amount` | NUMERIC(15,2) | Actual monthly sales |
| `target_amount` | NUMERIC(15,2) | Monthly sales target |
| `achievement_pct` | NUMERIC(6,3) | sales / target × 100 |
| `commission_rate` | NUMERIC(5,3) | Applied commission % |
| `commission_amount` | NUMERIC(15,2) | sales × commission_rate |
| `bonus_amount` | NUMERIC(15,2) | Bonus for >100% achievement |
| `total_payout` | NUMERIC(15,2) | commission + bonus |
| `paid_flag` | BOOLEAN | Payment status |
| `paid_date` | DATE | Payment date |

---

### Dimension Tables

#### `dw.dim_date` — Calendar spine (2018–2030)
Pre-populated with 4,383 rows. Includes Dominican Republic public holidays.

#### `dw.dim_product`
50 pharmaceutical products across 8 categories:
- `ANTIBIOTICO` — Amoxicilina, Ciprofloxacino, Azitromicina, Clindamicina, Ceftriaxona
- `ANALGESICO` — Ibuprofeno, Paracetamol, Naproxeno, Diclofenaco, Tramadol
- `VITAMINAS` — Vit C, Vit D3, Complejo B, Zinc, Calcio+D3
- `CARDIOVASCULAR` — Atorvastatina, Losartán, Metoprolol, Amlodipino, Enalapril
- `DIABETES` — Metformina, Glibenclamida, Insulina NPH, Sitagliptina
- `RESPIRATORIO` — Salbutamol, Loratadina, Montelukast, Fluticasona
- `GASTRO` — Omeprazol, Ranitidina, Metoclopramida, Loperamida
- `DERMATOLOGIA` — Hidrocortisona crema, Clotrimazol, Aciclovir

#### `dw.dim_client`
200 clients across DR — pharmacies, hospitals, clinics, wholesalers.
Types: `FARMACIA_INDEPENDIENTE`, `CADENA_FARMACIA`, `HOSPITAL_PUBLICO`,
       `CLINICA_PRIVADA`, `DISTRIBUIDORA_LOCAL`

#### `dw.dim_city`
32 cities in República Dominicana with coordinates, provinces, and zone assignments:
```
Capital: Santo Domingo, Santo Domingo Este, Santo Domingo Norte, Santo Domingo Oeste
Norte:   Santiago, Puerto Plata, Moca, La Vega, Bonao, San Francisco de Macorís
Este:    La Romana, Higüey, San Pedro de Macorís, Hato Mayor, El Seibo
Sur:     Barahona, Azua, San Cristóbal, Bani, Ocoa
Oeste:   Monte Cristi, Dajabón, Santiago Rodríguez, Valverde (Mao)
```

#### `dw.dim_zone`
| zone_code | zone_name | Provinces |
|---|---|---|
| CAP | Capital | Distrito Nacional, SD Este/Norte/Oeste |
| NOR | Norte | Santiago, Puerto Plata, La Vega, Espaillat, Duarte |
| EST | Este | La Altagracia, La Romana, San Pedro, Hato Mayor, El Seibo |
| SUR | Sur | Barahona, Azua, San Cristóbal, Peravia, Ocoa |
| OES | Oeste | Monte Cristi, Dajabón, Santiago Rodríguez, Valverde |

---

## Indexing Strategy

### Primary Indexes (automatic via PK)
- All surrogate keys are BIGSERIAL / SERIAL — B-tree by default

### Foreign Key Indexes
```sql
-- fact_sales: all FK columns
CREATE INDEX idx_fs_date     ON dw.fact_sales(date_key);
CREATE INDEX idx_fs_product  ON dw.fact_sales(product_key);
CREATE INDEX idx_fs_client   ON dw.fact_sales(client_key);
CREATE INDEX idx_fs_city     ON dw.fact_sales(city_key);
CREATE INDEX idx_fs_zone     ON dw.fact_sales(zone_key);
CREATE INDEX idx_fs_dist     ON dw.fact_sales(distributor_key);
CREATE INDEX idx_fs_lab      ON dw.fact_sales(laboratory_key);
CREATE INDEX idx_fs_sp       ON dw.fact_sales(salesperson_key);
```

### Range Scan Indexes (BRIN — very small footprint for date-ordered data)
```sql
CREATE INDEX idx_fs_load_brin ON dw.fact_sales USING BRIN (load_timestamp);
```

### Composite Indexes (most common query patterns)
```sql
-- YoY comparison: zone + product + date
CREATE INDEX idx_fs_zone_prod_date ON dw.fact_sales(zone_key, product_key, date_key);
-- Client ranking queries
CREATE INDEX idx_fs_client_date    ON dw.fact_sales(client_key, date_key);
-- Source system dedup
CREATE INDEX idx_fs_source         ON dw.fact_sales(source_system, source_record_id);
```

### Partitioning
```sql
-- Partition fact_sales by year (PostgreSQL declarative)
CREATE TABLE dw.fact_sales_2022 PARTITION OF dw.fact_sales
    FOR VALUES FROM (20220101) TO (20230101);  -- date_key range
```

---

## Materialized Views (Aggregation Layer)

| View | Refresh | Purpose |
|---|---|---|
| `mart.mv_sales_monthly_zone` | Nightly | Zone-level monthly aggregates for dashboards |
| `mart.mv_sales_yoy` | Nightly | Year-over-year comparison |
| `mart.mv_top_clients` | Hourly | Client ranking by net_amount |
| `mart.mv_product_margin` | Nightly | Margin % by product + lab |
| `mart.mv_commission_monthly` | Monthly | Commission calculations |
