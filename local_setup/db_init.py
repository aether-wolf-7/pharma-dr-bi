"""
Pharma DR · DuckDB Database Initializer
========================================
Creates the full star-schema warehouse inside a single DuckDB file.
No server required — database lives in: local_setup/pharma_dr.duckdb

Run: python local_setup/db_init.py
"""

import sys
import os
# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pathlib import Path
import duckdb

DB_PATH = Path(__file__).parent / "pharma_dr.duckdb"

def init_db():
    print("=" * 55)
    print("Pharma DR - DuckDB Initialization")
    print(f"Database: {DB_PATH}")
    print("=" * 55)

    # Drop existing DB file for clean re-init
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    # ── Zones ────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_zone (
            zone_key   INTEGER PRIMARY KEY,
            zone_code  VARCHAR(3) NOT NULL UNIQUE,
            zone_name  VARCHAR(50) NOT NULL,
            zone_manager VARCHAR(100),
            region     VARCHAR(50)
        )
    """)
    con.executemany("INSERT INTO dim_zone VALUES (?,?,?,?,?)", [
        (1, 'CAP', 'Capital',  'Roberto Méndez',   'Región Ozama'),
        (2, 'NOR', 'Norte',    'Carmen Valentín',  'Región Cibao'),
        (3, 'EST', 'Este',     'Luis Fermín',      'Región Este'),
        (4, 'SUR', 'Sur',      'Ana Polanco',      'Región Sur'),
        (5, 'OES', 'Oeste',    'Pedro Castillo',   'Región Noroeste'),
    ])

    # ── Cities ───────────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_city (
            city_key   INTEGER PRIMARY KEY,
            city_name  VARCHAR(100) NOT NULL,
            province   VARCHAR(100),
            latitude   DOUBLE,
            longitude  DOUBLE,
            population INTEGER,
            zone_key   INTEGER REFERENCES dim_zone(zone_key)
        )
    """)
    cities = [
        (1,  'Santo Domingo',           'Distrito Nacional',          18.4792, -69.9312, 1053222, 1),
        (2,  'Santo Domingo Este',       'Santo Domingo',             18.4894, -69.8534,  948448, 1),
        (3,  'Santo Domingo Norte',      'Santo Domingo',             18.5450, -69.9763,  625000, 1),
        (4,  'Santo Domingo Oeste',      'Santo Domingo',             18.4700, -70.0230,  390000, 1),
        (5,  'Boca Chica',              'Santo Domingo',              18.4541, -69.6048,   78000, 1),
        (6,  'Santiago',                'Santiago',                   19.4511, -70.6918,  850800, 2),
        (7,  'La Vega',                 'La Vega',                    19.2211, -70.5298,  248300, 2),
        (8,  'Moca',                    'Espaillat',                  19.3958, -70.5227,  189200, 2),
        (9,  'Puerto Plata',            'Puerto Plata',               19.7932, -70.6877,  246700, 2),
        (10, 'San Francisco de Macorís','Duarte',                     19.2972, -70.2546,  278400, 2),
        (11, 'Bonao',                   'Monseñor Nouel',             18.9442, -70.4088,  175600, 2),
        (12, 'Cotui',                   'Sánchez Ramírez',            19.0569, -70.1527,   95200, 2),
        (13, 'Nagua',                   'María Trinidad Sánchez',     19.3811, -69.8487,  131800, 2),
        (14, 'La Romana',               'La Romana',                  18.4277, -68.9727,  280400, 3),
        (15, 'San Pedro de Macorís',    'San Pedro de Macorís',       18.4558, -69.3054,  217200, 3),
        (16, 'Higüey',                  'La Altagracia',              18.6149, -68.7072,  187800, 3),
        (17, 'Hato Mayor',              'Hato Mayor',                 18.7639, -69.2587,   87400, 3),
        (18, 'El Seibo',                'El Seibo',                   18.7647, -69.0402,   75600, 3),
        (19, 'Barahona',                'Barahona',                   18.2064, -71.1023,  162000, 4),
        (20, 'San Cristóbal',           'San Cristóbal',              18.4157, -70.1087,  267200, 4),
        (21, 'Azua',                    'Azua',                       18.4550, -70.7351,  104600, 4),
        (22, 'Bani',                    'Peravia',                    18.2797, -70.3307,  102600, 4),
        (23, 'Ocoa',                    'San José de Ocoa',           18.5438, -70.5038,   64400, 4),
        (24, 'Monte Cristi',            'Monte Cristi',               19.8505, -71.6512,   74800, 5),
        (25, 'Dajabón',                 'Dajabón',                    19.5474, -71.7076,   48600, 5),
        (26, 'Mao',                     'Valverde',                   19.5560, -71.0791,   76200, 5),
        (27, 'Santiago Rodríguez',      'Santiago Rodríguez',         19.4799, -71.3391,   42800, 5),
        (28, 'Samaná',                  'Samaná',                     19.2068, -69.3357,   58400, 2),
        (29, 'Neiba',                   'Baoruco',                    18.4817, -71.4182,   48200, 4),
        (30, 'Pedernales',              'Pedernales',                 18.0384, -71.7444,   21600, 4),
    ]
    con.executemany("INSERT INTO dim_city VALUES (?,?,?,?,?,?,?)", cities)

    # ── Laboratories ────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_laboratory (
            lab_key   INTEGER PRIMARY KEY,
            lab_code  VARCHAR(20) NOT NULL UNIQUE,
            lab_name  VARCHAR(200) NOT NULL,
            lab_country VARCHAR(50),
            lab_type  VARCHAR(30)
        )
    """)
    con.executemany("INSERT INTO dim_laboratory VALUES (?,?,?,?,?)", [
        (1,  'BAYER',   'Bayer AG',                      'Alemania',  'MULTINACIONAL'),
        (2,  'PFIZER',  'Pfizer Inc.',                   'USA',       'MULTINACIONAL'),
        (3,  'ABBOTT',  'Abbott Laboratories',           'USA',       'MULTINACIONAL'),
        (4,  'ROCHE',   'F. Hoffmann-La Roche AG',       'Suiza',     'MULTINACIONAL'),
        (5,  'JNJ',     'Johnson & Johnson',             'USA',       'MULTINACIONAL'),
        (6,  'NOVARTIS','Novartis AG',                   'Suiza',     'MULTINACIONAL'),
        (7,  'MERCK',   'Merck & Co.',                   'USA',       'MULTINACIONAL'),
        (8,  'GSK',     'GlaxoSmithKline plc',           'UK',        'MULTINACIONAL'),
        (9,  'ASTRA',   'AstraZeneca PLC',               'UK',        'MULTINACIONAL'),
        (10, 'SANOFI',  'Sanofi S.A.',                   'Francia',   'MULTINACIONAL'),
        (11, 'LILLY',   'Eli Lilly and Company',         'USA',       'MULTINACIONAL'),
        (12, 'MEDCO',   'Medco Laboratorios S.A.',       'Rep. Dom.', 'NACIONAL'),
        (13, 'LABNAC',  'Laboratorio Nacional S.A.',     'Rep. Dom.', 'NACIONAL'),
        (14, 'GENFAR',  'Genfar S.A.',                   'Colombia',  'GENERICO'),
        (15, 'LAFRANCOL','La Franco-Colombiana S.A.',    'Colombia',  'GENERICO'),
        (16, 'BOEHRINGER','Boehringer Ingelheim',        'Alemania',  'MULTINACIONAL'),
    ])

    # ── Products ────────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_product (
            product_key  INTEGER PRIMARY KEY,
            product_id   VARCHAR(20) NOT NULL UNIQUE,
            product_name VARCHAR(200) NOT NULL,
            category     VARCHAR(50) NOT NULL,
            subcategory  VARCHAR(100),
            presentation VARCHAR(100),
            rx_otc_flag  VARCHAR(3) DEFAULT 'OTC',
            lab_key      INTEGER REFERENCES dim_laboratory(lab_key),
            unit_cost    DOUBLE,
            list_price   DOUBLE
        )
    """)
    products = [
        (1,  'SAP-001','Amoxicilina 500mg Cápsulas',    'ANTIBIOTICO',    'PENICILINAS',      'Cápsula',   'RX',  14, 28.50,  58.00),
        (2,  'SAP-002','Ciprofloxacino 500mg Tabletas', 'ANTIBIOTICO',    'FLUOROQUINOLONAS', 'Tableta',   'RX',   1, 95.00, 195.00),
        (3,  'SAP-003','Azitromicina 500mg Tabletas',   'ANTIBIOTICO',    'MACRÓLIDOS',       'Tableta',   'RX',   2,145.00, 295.00),
        (4,  'SAP-004','Clindamicina 300mg Cápsulas',   'ANTIBIOTICO',    'LINCOSÁMIDOS',     'Cápsula',   'RX',   2, 82.00, 168.00),
        (5,  'SAP-005','Ceftriaxona 1g Inyectable',     'ANTIBIOTICO',    'CEFALOSPORINAS',   'Inyectable','RX',   4,185.00, 380.00),
        (6,  'SAP-006','Metronidazol 500mg Tabletas',   'ANTIBIOTICO',    'NITROIMIDAZOLES',  'Tableta',   'RX',  10, 22.00,  45.00),
        (7,  'SAP-007','Ibuprofeno 400mg Tabletas',     'ANALGESICO',     'AINE',             'Tableta',   'OTC',  3, 18.00,  38.00),
        (8,  'SAP-008','Paracetamol 500mg Tabletas',    'ANALGESICO',     'ANALGESICO_CENTRAL','Tableta',  'OTC',  8,  8.50,  18.00),
        (9,  'SAP-009','Diclofenaco 50mg Tabletas',     'ANALGESICO',     'AINE',             'Tableta',   'OTC',  6, 25.00,  52.00),
        (10, 'SAP-010','Naproxeno 500mg Tabletas',      'ANALGESICO',     'AINE',             'Tableta',   'OTC',  4, 32.00,  65.00),
        (11, 'SAP-011','Tramadol 50mg Cápsulas',        'ANALGESICO',     'OPIÁCEO_DÉBIL',    'Cápsula',   'RX',   7, 68.00, 140.00),
        (12, 'SAP-012','Vitamina C 500mg Tabletas',     'VITAMINAS',      'VITAMINA_C',       'Tableta',   'OTC',  1, 35.00,  72.00),
        (13, 'SAP-013','Vitamina D3 1000UI Cápsulas',   'VITAMINAS',      'VITAMINA_D',       'Cápsula',   'OTC',  3, 42.00,  88.00),
        (14, 'SAP-014','Complejo B Tabletas',           'VITAMINAS',      'COMPLEJO_B',       'Tableta',   'OTC',  7, 38.00,  78.00),
        (15, 'SAP-015','Zinc 20mg Tabletas',            'VITAMINAS',      'MINERAL',          'Tableta',   'OTC', 12, 22.00,  45.00),
        (16, 'SAP-016','Calcio + D3 600mg Tabletas',    'VITAMINAS',      'MINERAL',          'Tableta',   'OTC',  3, 65.00, 135.00),
        (17, 'SAP-017','Multivitamínico Adultos',       'VITAMINAS',      'MULTIVITAMÍNICO',  'Tableta',   'OTC',  2, 85.00, 175.00),
        (18, 'SAP-018','Atorvastatina 20mg Tabletas',   'CARDIOVASCULAR', 'ESTATINA',         'Tableta',   'RX',   2, 58.00, 120.00),
        (19, 'SAP-019','Losartán 50mg Tabletas',        'CARDIOVASCULAR', 'ARA-II',           'Tableta',   'RX',   7, 45.00,  92.00),
        (20, 'SAP-020','Metoprolol 50mg Tabletas',      'CARDIOVASCULAR', 'BETABLOQ',         'Tableta',   'RX',   9, 38.00,  78.00),
        (21, 'SAP-021','Amlodipino 10mg Tabletas',      'CARDIOVASCULAR', 'BCC',              'Tableta',   'RX',   2, 48.00,  98.00),
        (22, 'SAP-022','Enalapril 10mg Tabletas',       'CARDIOVASCULAR', 'IECA',             'Tableta',   'RX',   7, 32.00,  65.00),
        (23, 'SAP-023','Clopidogrel 75mg Tabletas',     'CARDIOVASCULAR', 'ANTIAGREGANTE',    'Tableta',   'RX',  10, 95.00, 195.00),
        (24, 'SAP-024','Furosemida 40mg Tabletas',      'CARDIOVASCULAR', 'DIURETICO',        'Tableta',   'RX',  10, 15.00,  32.00),
        (25, 'SAP-025','Metformina 850mg Tabletas',     'DIABETES',       'BIGUANIDA',        'Tableta',   'RX',   7, 28.00,  58.00),
        (26, 'SAP-026','Glibenclamida 5mg Tabletas',    'DIABETES',       'SULFONILUREA',     'Tableta',   'RX',   4, 18.00,  38.00),
        (27, 'SAP-027','Insulina NPH 100UI/ml',         'DIABETES',       'INSULINA',         'Inyectable','RX',  11,285.00, 585.00),
        (28, 'SAP-028','Sitagliptina 100mg Tabletas',   'DIABETES',       'DPP-4',            'Tableta',   'RX',   7,485.00, 995.00),
        (29, 'SAP-029','Salbutamol 100mcg Inhalador',   'RESPIRATORIO',   'BRONCODILATADOR',  'Inhalador', 'RX',   8,195.00, 400.00),
        (30, 'SAP-030','Loratadina 10mg Tabletas',      'RESPIRATORIO',   'ANTIHISTAMÍNICO',  'Tableta',   'OTC',  7, 32.00,  65.00),
        (31, 'SAP-031','Montelukast 10mg Tabletas',     'RESPIRATORIO',   'ANTILEUCOTRIENO',  'Tableta',   'RX',   7,245.00, 500.00),
        (32, 'SAP-032','Fluticasona 50mcg Spray Nasal', 'RESPIRATORIO',   'CORTICOIDE_INH',   'Spray',     'RX',   8,385.00, 790.00),
        (33, 'SAP-033','Ambroxol 30mg Jarabe',          'RESPIRATORIO',   'MUCOLÍTICO',       'Jarabe',    'OTC', 16,145.00, 298.00),
        (34, 'SAP-034','Omeprazol 20mg Cápsulas',       'GASTRO',         'INHIBIDOR_BBA',    'Cápsula',   'OTC',  9, 42.00,  88.00),
        (35, 'SAP-035','Ranitidina 150mg Tabletas',     'GASTRO',         'ANTAGONISTA_H2',   'Tableta',   'OTC',  8, 28.00,  58.00),
        (36, 'SAP-036','Metoclopramida 10mg Tabletas',  'GASTRO',         'PROCINÉTICO',      'Tableta',   'OTC', 10, 18.00,  38.00),
        (37, 'SAP-037','Loperamida 2mg Cápsulas',       'GASTRO',         'ANTIDIARREICO',    'Cápsula',   'OTC',  5, 48.00,  98.00),
        (38, 'SAP-038','Pantoprazol 40mg Tabletas',     'GASTRO',         'INHIBIDOR_BBA',    'Tableta',   'RX',   2, 95.00, 195.00),
        (39, 'SAP-039','Hidrocortisona 1% Crema',       'DERMATOLOGIA',   'CORTICOIDE_TOP',   'Crema',     'OTC',  5, 55.00, 115.00),
        (40, 'SAP-040','Clotrimazol 1% Crema',          'DERMATOLOGIA',   'ANTIFÚNGICO',      'Crema',     'OTC',  1, 48.00,  98.00),
        (41, 'SAP-041','Aciclovir 5% Crema',            'DERMATOLOGIA',   'ANTIVIRAL',        'Crema',     'OTC',  8,145.00, 298.00),
        (42, 'SAP-042','Betametasona 0.05% Crema',      'DERMATOLOGIA',   'CORTICOIDE_TOP',   'Crema',     'RX',   7, 75.00, 155.00),
        (43, 'SAP-043','Albendazol 400mg Tabletas',     'ANTIPARASITARIO','BENCIMIDAZOL',     'Tableta',   'OTC',  8, 38.00,  78.00),
        (44, 'SAP-044','Ivermectina 6mg Tabletas',      'ANTIPARASITARIO','AVERMECTINA',      'Tableta',   'RX',  15, 55.00, 115.00),
        (45, 'SAP-045','Dexametasona 4mg Inyectable',   'CORTICOIDE',     'SISTÉMICO',        'Inyectable','RX',   7, 18.00,  38.00),
        (46, 'SAP-046','Prednisona 20mg Tabletas',      'CORTICOIDE',     'SISTÉMICO',        'Tableta',   'RX',   7, 25.00,  52.00),
        (47, 'SAP-047','Levotiroxina 100mcg Tabletas',  'HORMONA',        'TIROIDES',         'Tableta',   'RX',   3,185.00, 380.00),
        (48, 'SAP-048','Alprazolam 0.5mg Tabletas',     'PSIQUIATRIA',    'BENZODIAC',        'Tableta',   'RX',   2, 68.00, 140.00),
        (49, 'SAP-049','Fluoxetina 20mg Cápsulas',      'PSIQUIATRIA',    'ISRS',             'Cápsula',   'RX',  11, 78.00, 160.00),
        (50, 'SAP-050','Amitriptilina 25mg Tabletas',   'PSIQUIATRIA',    'ANTIDEPRESS',      'Tableta',   'RX',  12, 22.00,  45.00),
    ]
    con.executemany("INSERT INTO dim_product VALUES (?,?,?,?,?,?,?,?,?,?)", products)

    # ── Distributors ────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_distributor (
            distributor_key  INTEGER PRIMARY KEY,
            distributor_code VARCHAR(20) NOT NULL UNIQUE,
            distributor_name VARCHAR(200) NOT NULL,
            distributor_type VARCHAR(20) NOT NULL
        )
    """)
    con.executemany("INSERT INTO dim_distributor VALUES (?,?,?,?)", [
        (1, 'INT',    'Ventas Internas Pharma DR',     'INTERNO'),
        (2, 'DIST_A', 'Distribuidora Ramos S.R.L.',    'EXTERNO'),
        (3, 'DIST_B', 'MediFar Dominicana S.A.',       'EXTERNO'),
        (4, 'DIST_C', 'Farmacorp S.A.',                'EXTERNO'),
        (5, 'DIST_D', 'AlphaFarma Group S.R.L.',       'EXTERNO'),
        (6, 'DIST_E', 'BioPharma Distribution S.A.',   'EXTERNO'),
        (7, 'DIST_F', 'MedDist Nacional S.R.L.',       'EXTERNO'),
    ])

    # ── Salespersons ────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_salesperson (
            salesperson_key INTEGER PRIMARY KEY,
            salesperson_id  VARCHAR(20) NOT NULL UNIQUE,
            full_name       VARCHAR(100) NOT NULL,
            zone_key        INTEGER REFERENCES dim_zone(zone_key),
            commission_rate DOUBLE DEFAULT 0.038,
            monthly_target  DOUBLE DEFAULT 800000
        )
    """)
    con.executemany("INSERT INTO dim_salesperson VALUES (?,?,?,?,?,?)", [
        (1,  'SP-001','Carlos Batista Pérez',     1, 0.040, 850000),
        (2,  'SP-002','María González Sánchez',   1, 0.038, 820000),
        (3,  'SP-003','José Rodríguez Mejía',     1, 0.042, 900000),
        (4,  'SP-004','Ana Martínez Ortiz',       1, 0.035, 750000),
        (5,  'SP-005','Luis Fernández Taveras',   2, 0.040, 800000),
        (6,  'SP-006','Carmen Valdez Peralta',    2, 0.038, 780000),
        (7,  'SP-007','Roberto Jiménez Cruz',     2, 0.045, 950000),
        (8,  'SP-008','Patricia Almonte Rivas',   2, 0.035, 720000),
        (9,  'SP-009','Miguel Ángel Rosario',     3, 0.040, 820000),
        (10, 'SP-010','Sandra Corporán Díaz',     3, 0.038, 780000),
        (11, 'SP-011','Ramón Féliz Cepeda',       3, 0.036, 760000),
        (12, 'SP-012','Yolanda Tejeda Núñez',     4, 0.038, 760000),
        (13, 'SP-013','Eduardo Pichardo Reyes',   4, 0.035, 720000),
        (14, 'SP-014','Cecilia Marte Herrera',    4, 0.033, 700000),
        (15, 'SP-015','Francisco Castillo López', 5, 0.040, 800000),
    ])

    # ── Clients ─────────────────────────────────────────────
    con.execute("""
        CREATE TABLE dim_client (
            client_key   INTEGER PRIMARY KEY,
            client_id    VARCHAR(20) NOT NULL UNIQUE,
            client_name  VARCHAR(200) NOT NULL,
            client_type  VARCHAR(50) NOT NULL,
            city_key     INTEGER REFERENCES dim_city(city_key),
            zone_key     INTEGER REFERENCES dim_zone(zone_key),
            credit_limit DOUBLE DEFAULT 50000
        )
    """)

    # ── Fact Sales ───────────────────────────────────────────
    con.execute("""
        CREATE TABLE fact_sales (
            sale_key          BIGINT,
            date_key          INTEGER NOT NULL,
            full_date         DATE NOT NULL,
            year              INTEGER,
            month_num         INTEGER,
            month_name        VARCHAR(20),
            year_month        VARCHAR(7),
            product_key       INTEGER REFERENCES dim_product(product_key),
            client_key        INTEGER REFERENCES dim_client(client_key),
            city_key          INTEGER REFERENCES dim_city(city_key),
            zone_key          INTEGER REFERENCES dim_zone(zone_key),
            distributor_key   INTEGER REFERENCES dim_distributor(distributor_key),
            laboratory_key    INTEGER REFERENCES dim_laboratory(lab_key),
            salesperson_key   INTEGER REFERENCES dim_salesperson(salesperson_key),
            invoice_number    VARCHAR(50),
            invoice_line      SMALLINT DEFAULT 1,
            quantity          DOUBLE,
            unit_price        DOUBLE,
            gross_amount      DOUBLE,
            discount_pct      DOUBLE DEFAULT 0,
            discount_amount   DOUBLE DEFAULT 0,
            net_amount        DOUBLE,
            cost_amount       DOUBLE,
            margin_amount     DOUBLE,
            margin_pct        DOUBLE,
            source_system     VARCHAR(20),
            PRIMARY KEY (sale_key)
        )
    """)

    # ── Commission Fact ──────────────────────────────────────
    con.execute("""
        CREATE TABLE fact_commission (
            commission_key   INTEGER,
            year_month       VARCHAR(7),
            salesperson_key  INTEGER,
            zone_key         INTEGER,
            sales_amount     DOUBLE DEFAULT 0,
            target_amount    DOUBLE DEFAULT 0,
            achievement_pct  DOUBLE DEFAULT 0,
            commission_rate  DOUBLE DEFAULT 0,
            commission_amount DOUBLE DEFAULT 0,
            bonus_amount     DOUBLE DEFAULT 0,
            total_payout     DOUBLE DEFAULT 0,
            PRIMARY KEY (year_month, salesperson_key)
        )
    """)

    # ── Budget Fact ──────────────────────────────────────────────
    con.execute("""
        CREATE TABLE fact_budget (
            budget_key      INTEGER PRIMARY KEY,
            year_month      VARCHAR(7)  NOT NULL,
            year            INTEGER     NOT NULL,
            month_num       INTEGER     NOT NULL,
            zone_key        INTEGER REFERENCES dim_zone(zone_key),
            salesperson_key INTEGER REFERENCES dim_salesperson(salesperson_key),
            category        VARCHAR(50) NOT NULL,
            budget_amount   DOUBLE      NOT NULL DEFAULT 0
        )
    """)

    # ── Validation Queue ─────────────────────────────────────────
    con.execute("""
        CREATE TABLE validation_queue (
            id                 INTEGER PRIMARY KEY,
            entry_type         VARCHAR(20)  NOT NULL,
            raw_value          VARCHAR(200) NOT NULL,
            suggested_value    VARCHAR(200),
            mapped_key         INTEGER,
            confidence_pct     DOUBLE,
            source_distributor VARCHAR(50),
            source_file        VARCHAR(200),
            status             VARCHAR(20)  DEFAULT 'PENDIENTE',
            created_at         TIMESTAMP,
            resolved_at        TIMESTAMP,
            resolved_by        VARCHAR(100)
        )
    """)

    # ── ETL Run Log ──────────────────────────────────────────────
    con.execute("""
        CREATE TABLE etl_run_log (
            run_id           INTEGER PRIMARY KEY,
            run_type         VARCHAR(40) NOT NULL,
            source_system    VARCHAR(50),
            started_at       TIMESTAMP   NOT NULL,
            finished_at      TIMESTAMP,
            duration_sec     DOUBLE,
            status           VARCHAR(20),
            records_read     INTEGER DEFAULT 0,
            records_loaded   INTEGER DEFAULT 0,
            records_rejected INTEGER DEFAULT 0,
            error_message    VARCHAR(500)
        )
    """)

    con.close()
    print("[OK] Schema created: dim_zone, dim_city, dim_laboratory, dim_product")
    print("[OK] Schema created: dim_distributor, dim_salesperson, dim_client")
    print("[OK] Schema created: fact_sales, fact_commission")
    print("[OK] Schema created: fact_budget, validation_queue, etl_run_log")
    print("\nDone! Database ready at:", DB_PATH)


if __name__ == "__main__":
    init_db()



