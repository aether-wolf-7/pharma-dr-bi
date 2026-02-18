"""
Pharma DR · Data Generator + DuckDB Loader
============================================
Generates 100 000+ realistic pharmaceutical sales rows for República Dominicana
and loads them directly into the DuckDB warehouse.

Run: python local_setup/load_data.py
"""

import sys
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import random
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from faker import Faker
from tqdm import tqdm

DB_PATH = Path(__file__).parent / "pharma_dr.duckdb"

fake = Faker("es_MX")
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# ── Reference Data ────────────────────────────────────────────

CITIES = [
    (1,'Santo Domingo',1,0.22),(2,'Santo Domingo Este',1,0.10),
    (3,'Santo Domingo Norte',1,0.06),(4,'Santo Domingo Oeste',1,0.04),
    (5,'Boca Chica',1,0.02),(6,'Santiago',2,0.14),(7,'La Vega',2,0.04),
    (8,'Moca',2,0.03),(9,'Puerto Plata',2,0.03),(10,'San Francisco de Macorís',2,0.03),
    (11,'Bonao',2,0.02),(12,'Cotui',2,0.01),(13,'Nagua',2,0.02),
    (14,'La Romana',3,0.06),(15,'San Pedro de Macorís',3,0.05),
    (16,'Higüey',3,0.03),(17,'Hato Mayor',3,0.01),(18,'El Seibo',3,0.01),
    (19,'Barahona',4,0.04),(20,'San Cristóbal',4,0.04),
    (21,'Azua',4,0.02),(22,'Bani',4,0.02),(23,'Ocoa',4,0.01),
    (24,'Monte Cristi',5,0.03),(25,'Dajabón',5,0.01),
    (26,'Mao',5,0.02),(27,'Santiago Rodríguez',5,0.01),
    (28,'Samaná',2,0.01),(29,'Neiba',4,0.01),(30,'Pedernales',4,0.01),
]
CITY_IDS   = [c[0] for c in CITIES]
CITY_NAMES = [c[1] for c in CITIES]
CITY_ZONES = [c[2] for c in CITIES]
CITY_W     = np.array([c[3] for c in CITIES]); CITY_W /= CITY_W.sum()

PRODUCTS = [
    (1,'SAP-001','ANTIBIOTICO',14,28.50,58.00,6),
    (2,'SAP-002','ANTIBIOTICO', 1,95.00,195.00,4),
    (3,'SAP-003','ANTIBIOTICO', 2,145.00,295.00,3),
    (4,'SAP-004','ANTIBIOTICO', 2,82.00,168.00,2),
    (5,'SAP-005','ANTIBIOTICO', 4,185.00,380.00,2),
    (6,'SAP-006','ANTIBIOTICO',10,22.00,45.00,3),
    (7,'SAP-007','ANALGESICO',  3,18.00,38.00,8),
    (8,'SAP-008','ANALGESICO',  8, 8.50,18.00,10),
    (9,'SAP-009','ANALGESICO',  6,25.00,52.00,6),
    (10,'SAP-010','ANALGESICO', 4,32.00,65.00,4),
    (11,'SAP-011','ANALGESICO', 7,68.00,140.00,2),
    (12,'SAP-012','VITAMINAS',  1,35.00,72.00,7),
    (13,'SAP-013','VITAMINAS',  3,42.00,88.00,5),
    (14,'SAP-014','VITAMINAS',  7,38.00,78.00,6),
    (15,'SAP-015','VITAMINAS', 12,22.00,45.00,4),
    (16,'SAP-016','VITAMINAS',  3,65.00,135.00,4),
    (17,'SAP-017','VITAMINAS',  2,85.00,175.00,5),
    (18,'SAP-018','CARDIOVASCULAR',2,58.00,120.00,5),
    (19,'SAP-019','CARDIOVASCULAR',7,45.00,92.00,4),
    (20,'SAP-020','CARDIOVASCULAR',9,38.00,78.00,3),
    (21,'SAP-021','CARDIOVASCULAR',2,48.00,98.00,3),
    (22,'SAP-022','CARDIOVASCULAR',7,32.00,65.00,3),
    (23,'SAP-023','CARDIOVASCULAR',10,95.00,195.00,2),
    (24,'SAP-024','CARDIOVASCULAR',10,15.00,32.00,3),
    (25,'SAP-025','DIABETES',7,28.00,58.00,5),
    (26,'SAP-026','DIABETES',4,18.00,38.00,3),
    (27,'SAP-027','DIABETES',11,285.00,585.00,2),
    (28,'SAP-028','DIABETES',7,485.00,995.00,2),
    (29,'SAP-029','RESPIRATORIO',8,195.00,400.00,4),
    (30,'SAP-030','RESPIRATORIO',7,32.00,65.00,5),
    (31,'SAP-031','RESPIRATORIO',7,245.00,500.00,3),
    (32,'SAP-032','RESPIRATORIO',8,385.00,790.00,2),
    (33,'SAP-033','RESPIRATORIO',16,145.00,298.00,3),
    (34,'SAP-034','GASTRO',9,42.00,88.00,6),
    (35,'SAP-035','GASTRO',8,28.00,58.00,4),
    (36,'SAP-036','GASTRO',10,18.00,38.00,4),
    (37,'SAP-037','GASTRO',5,48.00,98.00,3),
    (38,'SAP-038','GASTRO',2,95.00,195.00,4),
    (39,'SAP-039','DERMATOLOGIA',5,55.00,115.00,3),
    (40,'SAP-040','DERMATOLOGIA',1,48.00,98.00,4),
    (41,'SAP-041','DERMATOLOGIA',8,145.00,298.00,2),
    (42,'SAP-042','DERMATOLOGIA',7,75.00,155.00,2),
    (43,'SAP-043','ANTIPARASITARIO',8,38.00,78.00,3),
    (44,'SAP-044','ANTIPARASITARIO',15,55.00,115.00,2),
    (45,'SAP-045','CORTICOIDE',7,18.00,38.00,3),
    (46,'SAP-046','CORTICOIDE',7,25.00,52.00,2),
    (47,'SAP-047','HORMONA',3,185.00,380.00,2),
    (48,'SAP-048','PSIQUIATRIA',2,68.00,140.00,1),
    (49,'SAP-049','PSIQUIATRIA',11,78.00,160.00,2),
    (50,'SAP-050','PSIQUIATRIA',12,22.00,45.00,2),
]
PROD_KEYS   = [p[0] for p in PRODUCTS]
PROD_LABS   = [p[2] for p in PRODUCTS]  # reuse as category for weight
PROD_LABS_K = [p[3] for p in PRODUCTS]
PROD_COSTS  = [p[4] for p in PRODUCTS]
PROD_PRICES = [p[5] for p in PRODUCTS]
PROD_W      = np.array([p[6] for p in PRODUCTS], dtype=float); PROD_W /= PROD_W.sum()

SALESPERSONS = [
    (1,1),(2,1),(3,1),(4,1),(5,2),(6,2),(7,2),(8,2),
    (9,3),(10,3),(11,3),(12,4),(13,4),(14,4),(15,5),
]
ZONE_SP = {}
for sp_key, zone_key in SALESPERSONS:
    ZONE_SP.setdefault(zone_key, []).append(sp_key)

DISTRIBUTORS = [(1,0.50),(2,0.10),(3,0.10),(4,0.08),(5,0.08),(6,0.07),(7,0.07)]
DIST_KEYS = [d[0] for d in DISTRIBUTORS]
DIST_W    = np.array([d[1] for d in DISTRIBUTORS]); DIST_W /= DIST_W.sum()

CLIENT_TYPES = [
    'FARMACIA_INDEPENDIENTE','FARMACIA_INDEPENDIENTE','FARMACIA_INDEPENDIENTE',
    'CADENA_FARMACIA','CADENA_FARMACIA',
    'HOSPITAL_PUBLICO','CLINICA_PRIVADA','DISTRIBUIDORA_LOCAL',
]

DR_SURNAMES = [
    'García','Rodríguez','Martínez','Hernández','López','González','Pérez',
    'Sánchez','Ramírez','Torres','Flores','Díaz','Cruz','Reyes','Morales',
    'Jiménez','Medina','Cabrera','Taveras','Almonte','Batista','Marte',
]

MONTH_NAMES_ES = {
    1:'Enero',2:'Febrero',3:'Marzo',4:'Abril',5:'Mayo',6:'Junio',
    7:'Julio',8:'Agosto',9:'Septiembre',10:'Octubre',11:'Noviembre',12:'Diciembre',
}

SEASONAL = {1:1.25,2:1.05,3:1.00,4:1.15,5:1.10,6:0.90,
            7:0.88,8:0.85,9:0.95,10:1.10,11:1.20,12:1.30}

YOY_GROWTH = {2021:1.00,2022:1.05,2023:1.10,2024:1.16}


def generate_clients(n=200):
    rows = []
    for i in range(1, n+1):
        city_idx = np.random.choice(len(CITIES), p=CITY_W)
        city_id   = CITY_IDS[city_idx]
        zone_key  = CITY_ZONES[city_idx]
        city_name = CITY_NAMES[city_idx]
        ctype = random.choice(CLIENT_TYPES)

        if ctype == 'FARMACIA_INDEPENDIENTE':
            name = f"Farmacia {random.choice(DR_SURNAMES)}"
        elif ctype == 'CADENA_FARMACIA':
            name = random.choice(['Carol Ana','Estrella','Cruz Verde','del Pueblo','Nacional']) + f" #{i}"
        elif ctype == 'HOSPITAL_PUBLICO':
            name = f"Hospital General {city_name}"
        elif ctype == 'CLINICA_PRIVADA':
            name = f"Clínica {random.choice(DR_SURNAMES)} & Asoc."
        else:
            name = f"Distribuidora Local {random.choice(DR_SURNAMES)}"

        credit = {'FARMACIA_INDEPENDIENTE':80000,'CADENA_FARMACIA':500000,
                  'HOSPITAL_PUBLICO':1200000,'CLINICA_PRIVADA':250000,
                  'DISTRIBUIDORA_LOCAL':200000}.get(ctype, 80000)

        rows.append((i, f"CLI-{i:04d}", name, ctype, city_id, zone_key, credit * random.uniform(0.5, 1.5)))
    return rows


def generate_sales(client_rows, n_invoices=25000):
    records = []
    total_days = (date(2024,12,31) - date(2021,1,1)).days
    invoice_num = 100000
    sale_key = 1

    # Build client lookup
    clients = [(r[0], r[4], r[5]) for r in client_rows]  # key, city_key, zone_key

    print(f"  Generating {n_invoices:,} invoices (~{n_invoices*4:,} rows)...")
    for _ in tqdm(range(n_invoices), unit="inv"):
        invoice_num += 1
        client_key, city_key, zone_key = random.choice(clients)

        # Seasonal date sampling
        while True:
            offset = random.randint(0, total_days)
            sale_date = date(2021,1,1) + timedelta(days=offset)
            if random.random() < SEASONAL[sale_date.month] / 1.30:
                break

        growth = YOY_GROWTH.get(sale_date.year, 1.0)
        dist_key = np.random.choice(DIST_KEYS, p=DIST_W)
        sp_key   = random.choice(ZONE_SP.get(zone_key, [1]))
        n_lines  = random.choices([1,2,3,4,5,6], weights=[30,25,20,12,8,5])[0]
        prod_idxs = np.random.choice(len(PRODUCTS), size=n_lines, p=PROD_W, replace=False)

        for line_num, pidx in enumerate(prod_idxs, 1):
            pk       = PROD_KEYS[pidx]
            lab_key  = PROD_LABS_K[pidx]
            cost_u   = PROD_COSTS[pidx]
            price_u  = PROD_PRICES[pidx]

            qty      = random.choices([1,2,3,5,10,20,50,100], weights=[10,15,20,20,15,10,7,3])[0]
            up       = round(price_u * random.uniform(0.90, 1.10), 2)
            disc_pct = round(random.uniform(0, 15), 2)
            gross    = round(qty * up, 2)
            disc_amt = round(gross * disc_pct / 100, 2)
            net      = round((gross - disc_amt) * growth, 2)
            cost     = round(cost_u * qty, 2)
            margin   = round(net - cost, 2)
            mpct     = round(margin / net * 100, 3) if net > 0 else 0

            ym       = f"{sale_date.year}-{sale_date.month:02d}"
            records.append((
                sale_key,
                int(sale_date.strftime("%Y%m%d")),
                sale_date,
                sale_date.year,
                sale_date.month,
                MONTH_NAMES_ES[sale_date.month],
                ym,
                pk, client_key, city_key, zone_key,
                dist_key, lab_key, sp_key,
                f"FAC-{invoice_num:08d}", line_num,
                float(qty), up, gross, disc_pct, disc_amt, net, cost, margin, mpct,
                'SAP_HANA' if dist_key == 1 else f'DIST_{chr(64+dist_key-1)}',
            ))
            sale_key += 1

    return records


def compute_commissions(con):
    SP_INFO = {
        1:(0.040,850000),2:(0.038,820000),3:(0.042,900000),4:(0.035,750000),
        5:(0.040,800000),6:(0.038,780000),7:(0.045,950000),8:(0.035,720000),
        9:(0.040,820000),10:(0.038,780000),11:(0.036,760000),
        12:(0.038,760000),13:(0.035,720000),14:(0.033,700000),15:(0.040,800000),
    }
    SP_ZONE = {k:v for k,v in SALESPERSONS}

    rows = []
    ck = 1
    for year in range(2021, 2025):
        for month in range(1, 13):
            ym = f"{year}-{month:02d}"
            monthly = con.execute(f"""
                SELECT salesperson_key, SUM(net_amount)
                FROM fact_sales
                WHERE year={year} AND month_num={month}
                GROUP BY salesperson_key
            """).fetchall()
            sales_map = {r[0]: r[1] for r in monthly}

            for sp_key, (rate, target) in SP_INFO.items():
                seasonal_target = target * SEASONAL[month] * random.uniform(0.95, 1.05)
                actual = sales_map.get(sp_key, 0)
                ach    = round(actual / seasonal_target * 100, 3) if seasonal_target else 0
                comm   = round(actual * rate, 2)
                bonus  = round(actual * rate * 0.10, 2) if actual >= seasonal_target else 0
                rows.append((ck, ym, sp_key, SP_ZONE[sp_key],
                             actual, seasonal_target, ach, rate, comm, bonus, round(comm+bonus, 2)))
                ck += 1
    return rows


def main():
    print("=" * 55)
    print("Pharma DR · Data Generator & DuckDB Loader")
    print("=" * 55)

    con = duckdb.connect(str(DB_PATH))

    # Clear tables in FK-safe order (children before parents)
    con.execute("DELETE FROM fact_commission")
    con.execute("DELETE FROM fact_sales")
    con.execute("DELETE FROM dim_client")

    # ── Clients ──────────────────────────────────────────────
    print("\n[1/3] Generating 200 clients...")
    client_rows = generate_clients(200)
    con.execute("DELETE FROM dim_client")
    con.executemany(
        "INSERT INTO dim_client VALUES (?,?,?,?,?,?,?)",
        client_rows,
    )
    print(f"  [OK] {len(client_rows)} clients loaded")

    # ── Sales ────────────────────────────────────────────────
    print("\n[2/3] Generating sales data (2021–2024)...")
    sales_rows = generate_sales(client_rows, n_invoices=25000)

    # Convert to pandas DataFrame and insert via DuckDB register
    import pandas as pd
    sales_df = pd.DataFrame(sales_rows, columns=[
        "sale_key","date_key","full_date","year","month_num","month_name","year_month",
        "product_key","client_key","city_key","zone_key",
        "distributor_key","laboratory_key","salesperson_key",
        "invoice_number","invoice_line",
        "quantity","unit_price","gross_amount","discount_pct","discount_amount",
        "net_amount","cost_amount","margin_amount","margin_pct","source_system",
    ])
    # Ensure correct types
    for col in ["sale_key","date_key","year","month_num","product_key","client_key",
                "city_key","zone_key","distributor_key","laboratory_key",
                "salesperson_key","invoice_line"]:
        sales_df[col] = sales_df[col].astype(int)
    for col in ["quantity","unit_price","gross_amount","discount_pct","discount_amount",
                "net_amount","cost_amount","margin_amount","margin_pct"]:
        sales_df[col] = sales_df[col].astype(float)
    sales_df["full_date"] = pd.to_datetime(sales_df["full_date"])

    con.register("sales_df_view", sales_df)
    con.execute("INSERT INTO fact_sales SELECT * FROM sales_df_view")
    con.unregister("sales_df_view")
    total = len(sales_rows)
    total_net = sum(r[21] for r in sales_rows)
    print(f"  [OK] {total:,} fact rows loaded")
    print(f"  [OK] Total Net Sales: RD$ {total_net:,.0f}")

    # ── Commissions ──────────────────────────────────────────
    print("\n[3/3] Computing monthly commissions...")
    comm_rows = compute_commissions(con)
    comm_df = pd.DataFrame(comm_rows, columns=[
        "commission_key","year_month","salesperson_key","zone_key",
        "sales_amount","target_amount","achievement_pct",
        "commission_rate","commission_amount","bonus_amount","total_payout",
    ])
    for col in ["commission_key","salesperson_key","zone_key"]:
        comm_df[col] = comm_df[col].astype(int)
    for col in ["sales_amount","target_amount","achievement_pct","commission_rate",
                "commission_amount","bonus_amount","total_payout"]:
        comm_df[col] = comm_df[col].astype(float)
    con.register("comm_df_view", comm_df)
    con.execute("INSERT INTO fact_commission SELECT * FROM comm_df_view")
    con.unregister("comm_df_view")
    print(f"  [OK] {len(comm_rows)} commission rows computed")

    # Summary
    print("\n" + "=" * 55)
    print("LOAD COMPLETE — Summary")
    print("=" * 55)
    rows_by_year = con.execute("""
        SELECT year, COUNT(*) AS row_count, ROUND(SUM(net_amount),0) AS total_net
        FROM fact_sales GROUP BY year ORDER BY year
    """).fetchall()
    for r in rows_by_year:
        print(f"  {r[0]}: {r[1]:>7,} rows  |  RD$ {r[2]:>15,.0f}")

    print("\nTop 5 Zones by Revenue:")
    for r in con.execute("""
        SELECT z.zone_name, ROUND(SUM(s.net_amount),0) net
        FROM fact_sales s JOIN dim_zone z ON z.zone_key=s.zone_key
        GROUP BY z.zone_name ORDER BY net DESC LIMIT 5
    """).fetchall():
        print(f"  {r[0]:<12}: RD$ {r[1]:>15,.0f}")

    con.close()
    print(f"\nDatabase: {DB_PATH}")


if __name__ == "__main__":
    main()

