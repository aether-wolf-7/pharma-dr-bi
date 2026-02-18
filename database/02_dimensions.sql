-- ============================================================
-- Pharma DR · Dimension Tables
-- ============================================================

-- ── dim_zone ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_zone (
    zone_key        SERIAL PRIMARY KEY,
    zone_code       CHAR(3)      NOT NULL UNIQUE,
    zone_name       VARCHAR(50)  NOT NULL,
    zone_manager    VARCHAR(100),
    region          VARCHAR(50),
    active_flag     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO dw.dim_zone(zone_code, zone_name, zone_manager, region) VALUES
    ('CAP', 'Capital',  'Roberto Méndez',    'Región Ozama'),
    ('NOR', 'Norte',    'Carmen Valentín',   'Región Cibao'),
    ('EST', 'Este',     'Luis Fermín',       'Región Este'),
    ('SUR', 'Sur',      'Ana Polanco',       'Región Sur'),
    ('OES', 'Oeste',    'Pedro Castillo',    'Región Noroeste')
ON CONFLICT (zone_code) DO NOTHING;

-- ── dim_city ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_city (
    city_key        SERIAL PRIMARY KEY,
    city_name       VARCHAR(100) NOT NULL,
    city_name_norm  VARCHAR(100) NOT NULL,   -- unaccented lowercase for matching
    province        VARCHAR(100) NOT NULL,
    region          VARCHAR(100),
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    population      INT,
    zone_key        INT REFERENCES dw.dim_zone(zone_key),
    active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dw.dim_city(city_name, city_name_norm, province, region, latitude, longitude, population, zone_key)
SELECT c.city_name,
       lower(unaccent(c.city_name)),
       c.province,
       c.region,
       c.lat,
       c.lon,
       c.pop,
       z.zone_key
FROM (VALUES
    ('Santo Domingo',           'Distrito Nacional',         'Región Ozama',      18.479200, -69.931200,  1053222, 'CAP'),
    ('Santo Domingo Este',      'Santo Domingo',             'Región Ozama',      18.489400, -69.853400,   948448, 'CAP'),
    ('Santo Domingo Norte',     'Santo Domingo',             'Región Ozama',      18.545000, -69.976300,   625000, 'CAP'),
    ('Santo Domingo Oeste',     'Santo Domingo',             'Región Ozama',      18.470000, -70.023000,   390000, 'CAP'),
    ('Boca Chica',              'Santo Domingo',             'Región Ozama',      18.454100, -69.604800,    78000, 'CAP'),
    ('Santiago',                'Santiago',                  'Región Cibao Norte',19.451100, -70.691800,   850800, 'NOR'),
    ('La Vega',                 'La Vega',                   'Región Cibao Norte',19.221100, -70.529800,   248300, 'NOR'),
    ('Moca',                    'Espaillat',                 'Región Cibao Norte',19.395800, -70.522700,   189200, 'NOR'),
    ('Puerto Plata',            'Puerto Plata',              'Región Cibao Norte',19.793200, -70.687700,   246700, 'NOR'),
    ('San Francisco de Macorís','Duarte',                    'Región Cibao Nordeste',19.297200,-70.254600, 278400, 'NOR'),
    ('Bonao',                   'Monseñor Nouel',            'Región Cibao Sur',  18.944200, -70.408800,   175600, 'NOR'),
    ('Cotui',                   'Sánchez Ramírez',           'Región Cibao Sur',  19.056900, -70.152700,    95200, 'NOR'),
    ('La Romana',               'La Romana',                 'Región Este',       18.427700, -68.972700,   280400, 'EST'),
    ('San Pedro de Macorís',    'San Pedro de Macorís',      'Región Este',       18.455800, -69.305400,   217200, 'EST'),
    ('Higüey',                  'La Altagracia',             'Región Este',       18.614900, -68.707200,   187800, 'EST'),
    ('Hato Mayor',              'Hato Mayor',                'Región Este',       18.763900, -69.258700,    87400, 'EST'),
    ('El Seibo',                'El Seibo',                  'Región Este',       18.764700, -69.040200,    75600, 'EST'),
    ('Barahona',                'Barahona',                  'Región Enriquillo', 18.206400, -71.102300,   162000, 'SUR'),
    ('San Cristóbal',           'San Cristóbal',             'Región Valdesia',   18.415700, -70.108700,   267200, 'SUR'),
    ('Azua',                    'Azua',                      'Región Valdesia',   18.455000, -70.735100,   104600, 'SUR'),
    ('Bani',                    'Peravia',                   'Región Valdesia',   18.279700, -70.330700,   102600, 'SUR'),
    ('Ocoa',                    'San José de Ocoa',          'Región Valdesia',   18.543800, -70.503800,    64400, 'SUR'),
    ('Monte Cristi',            'Monte Cristi',              'Región Noroeste',   19.850500, -71.651200,    74800, 'OES'),
    ('Dajabón',                 'Dajabón',                   'Región Noroeste',   19.547400, -71.707600,    48600, 'OES'),
    ('Mao',                     'Valverde',                  'Región Noroeste',   19.556000, -71.079100,    76200, 'OES'),
    ('Santiago Rodríguez',      'Santiago Rodríguez',        'Región Noroeste',   19.479900, -71.339100,    42800, 'OES'),
    ('Nagua',                   'María Trinidad Sánchez',    'Región Nordeste',   19.381100, -69.848700,   131800, 'NOR'),
    ('Samaná',                  'Samaná',                    'Región Nordeste',   19.206800, -69.335700,    58400, 'NOR'),
    ('Neiba',                   'Baoruco',                   'Región Enriquillo', 18.481700, -71.418200,    48200, 'SUR'),
    ('Pedernales',              'Pedernales',                'Región Enriquillo', 18.038400, -71.744400,    21600, 'SUR'),
    ('Guerra',                  'Santo Domingo',             'Región Ozama',      18.546000, -69.879000,    45000, 'CAP'),
    ('Los Alcarrizos',          'Santo Domingo',             'Región Ozama',      18.503100, -70.006400,   185000, 'CAP')
) AS c(city_name, province, region, lat, lon, pop, zone_code)
JOIN dw.dim_zone z ON z.zone_code = c.zone_code
ON CONFLICT DO NOTHING;

-- ── dim_laboratory ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_laboratory (
    lab_key         SERIAL PRIMARY KEY,
    lab_code        VARCHAR(20)  NOT NULL UNIQUE,
    lab_name        VARCHAR(200) NOT NULL,
    lab_country     VARCHAR(50),
    lab_type        VARCHAR(30),  -- MULTINACIONAL | NACIONAL | GENERICO
    contact_email   VARCHAR(200),
    active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dw.dim_laboratory(lab_code, lab_name, lab_country, lab_type) VALUES
    ('BAYER',   'Bayer AG',                         'Alemania',    'MULTINACIONAL'),
    ('PFIZER',  'Pfizer Inc.',                      'USA',         'MULTINACIONAL'),
    ('ABBOTT',  'Abbott Laboratories',              'USA',         'MULTINACIONAL'),
    ('ROCHE',   'F. Hoffmann-La Roche AG',          'Suiza',       'MULTINACIONAL'),
    ('JNJ',     'Johnson & Johnson',                'USA',         'MULTINACIONAL'),
    ('NOVARTIS','Novartis AG',                      'Suiza',       'MULTINACIONAL'),
    ('MERCK',   'Merck & Co.',                      'USA',         'MULTINACIONAL'),
    ('GSK',     'GlaxoSmithKline plc',              'UK',          'MULTINACIONAL'),
    ('ASTRA',   'AstraZeneca PLC',                  'UK',          'MULTINACIONAL'),
    ('SANOFI',  'Sanofi S.A.',                      'Francia',     'MULTINACIONAL'),
    ('LILLY',   'Eli Lilly and Company',            'USA',         'MULTINACIONAL'),
    ('MEDCO',   'Medco Laboratorios S.A.',          'Rep. Dom.',   'NACIONAL'),
    ('LABNAC',  'Laboratorio Nacional S.A.',        'Rep. Dom.',   'NACIONAL'),
    ('FARMEX',  'Farmex Dominicana S.R.L.',         'Rep. Dom.',   'NACIONAL'),
    ('GENFAR',  'Genfar S.A.',                      'Colombia',    'GENERICO'),
    ('LAFRANCOL','La Franco-Colombiana S.A.',       'Colombia',    'GENERICO')
ON CONFLICT (lab_code) DO NOTHING;

-- ── dim_product ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_id          VARCHAR(50)  NOT NULL UNIQUE,  -- SAP material code
    product_name        VARCHAR(200) NOT NULL,
    product_name_norm   VARCHAR(200) NOT NULL,         -- normalized for matching
    generic_name        VARCHAR(200),
    brand_name          VARCHAR(200),
    category            VARCHAR(50)  NOT NULL,
    subcategory         VARCHAR(100),
    presentation        VARCHAR(100),   -- Tableta, Cápsula, Jarabe, Inyectable, etc.
    concentration       VARCHAR(50),    -- 500mg, 10mg/5ml, etc.
    unit_of_measure     VARCHAR(20)  DEFAULT 'CAJA',
    units_per_pack      SMALLINT     DEFAULT 30,
    lab_key             INT REFERENCES dw.dim_laboratory(lab_key),
    rx_otc_flag         CHAR(3)      NOT NULL DEFAULT 'OTC', -- RX | OTC
    controlled_flag     BOOLEAN      NOT NULL DEFAULT FALSE,
    refrigerated_flag   BOOLEAN      NOT NULL DEFAULT FALSE,
    unit_cost           NUMERIC(12,4),
    list_price          NUMERIC(12,4),
    sat_code            VARCHAR(20),    -- ITBIS product code
    active_flag         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Load products (50 pharmaceutical SKUs)
INSERT INTO dw.dim_product(
    product_id, product_name, product_name_norm, generic_name, brand_name,
    category, subcategory, presentation, concentration, units_per_pack,
    lab_key, rx_otc_flag, controlled_flag, unit_cost, list_price
)
SELECT
    p.pid, p.pname, lower(unaccent(p.pname)), p.gname, p.bname,
    p.cat, p.subcat, p.pres, p.conc, p.upack,
    l.lab_key, p.rx_otc, p.ctrl,
    p.ucost, p.lprice
FROM (VALUES
-- ANTIBIOTICOS
('SAP-001','Amoxicilina 500mg Cápsulas','Amoxicilina','Amoxil','ANTIBIOTICO','PENICILINAS','Cápsula','500mg',30,'GENFAR','RX',FALSE,28.50,58.00),
('SAP-002','Ciprofloxacino 500mg Tabletas','Ciprofloxacino','Cipro','ANTIBIOTICO','FLUOROQUINOLONAS','Tableta','500mg',20,'BAYER','RX',FALSE,95.00,195.00),
('SAP-003','Azitromicina 500mg Tabletas','Azitromicina','Zithromax','ANTIBIOTICO','MACRÓLIDOS','Tableta','500mg',3,'PFIZER','RX',FALSE,145.00,295.00),
('SAP-004','Clindamicina 300mg Cápsulas','Clindamicina','Dalacin C','ANTIBIOTICO','LINCOSÁMIDOS','Cápsula','300mg',16,'PFIZER','RX',FALSE,82.00,168.00),
('SAP-005','Ceftriaxona 1g Inyectable','Ceftriaxona','Rocephin','ANTIBIOTICO','CEFALOSPORINAS','Inyectable','1g',1,'ROCHE','RX',FALSE,185.00,380.00),
('SAP-006','Metronidazol 500mg Tabletas','Metronidazol','Flagyl','ANTIBIOTICO','NITROIMIDAZOLES','Tableta','500mg',30,'SANOFI','RX',FALSE,22.00,45.00),
-- ANALGESICOS / ANTIINFLAMATORIOS
('SAP-007','Ibuprofeno 400mg Tabletas','Ibuprofeno','Advil','ANALGESICO','AINE','Tableta','400mg',30,'ABBOTT','OTC',FALSE,18.00,38.00),
('SAP-008','Paracetamol 500mg Tabletas','Paracetamol','Panadol','ANALGESICO','ANALGESICO_CENTRAL','Tableta','500mg',20,'GSK','OTC',FALSE,8.50,18.00),
('SAP-009','Diclofenaco 50mg Tabletas','Diclofenaco','Voltaren','ANALGESICO','AINE','Tableta','50mg',30,'NOVARTIS','OTC',FALSE,25.00,52.00),
('SAP-010','Naproxeno 500mg Tabletas','Naproxeno','Naprosyn','ANALGESICO','AINE','Tableta','500mg',30,'ROCHE','OTC',FALSE,32.00,65.00),
('SAP-011','Tramadol 50mg Cápsulas','Tramadol','Tramal','ANALGESICO','OPIÁCEO_DÉBIL','Cápsula','50mg',30,'GRUNENTHAL','RX',TRUE,68.00,140.00),
-- VITAMINAS / SUPLEMENTOS
('SAP-012','Vitamina C 500mg Tabletas','Vitamina C','Redoxon','VITAMINAS','VITAMINA_C','Tableta','500mg',30,'BAYER','OTC',FALSE,35.00,72.00),
('SAP-013','Vitamina D3 1000UI Cápsulas','Vitamina D3','DCalvit','VITAMINAS','VITAMINA_D','Cápsula','1000UI',30,'ABBOTT','OTC',FALSE,42.00,88.00),
('SAP-014','Complejo B Tabletas','Complejo B','Neurobión','VITAMINAS','COMPLEJO_B','Tableta','Compuesto',30,'MERCK','OTC',FALSE,38.00,78.00),
('SAP-015','Zinc 20mg Tabletas','Zinc','Zinc Plus','VITAMINAS','MINERAL','Tableta','20mg',30,'MEDCO','OTC',FALSE,22.00,45.00),
('SAP-016','Calcio + D3 600mg Tabletas','Calcio D3','Caltrate','VITAMINAS','MINERAL','Tableta','600mg+400UI',60,'ABBOTT','OTC',FALSE,65.00,135.00),
('SAP-017','Multivitamínico Adultos Tabletas','Multivitamínico','Centrum','VITAMINAS','MULTIVITAMÍNICO','Tableta','Compuesto',30,'PFIZER','OTC',FALSE,85.00,175.00),
-- CARDIOVASCULAR
('SAP-018','Atorvastatina 20mg Tabletas','Atorvastatina','Lipitor','CARDIOVASCULAR','ESTATINA','Tableta','20mg',30,'PFIZER','RX',FALSE,58.00,120.00),
('SAP-019','Losartán 50mg Tabletas','Losartán','Cozaar','CARDIOVASCULAR','ARA-II','Tableta','50mg',30,'MERCK','RX',FALSE,45.00,92.00),
('SAP-020','Metoprolol 50mg Tabletas','Metoprolol','Betaloc','CARDIOVASCULAR','BETABLOQ','Tableta','50mg',30,'ASTRA','RX',FALSE,38.00,78.00),
('SAP-021','Amlodipino 10mg Tabletas','Amlodipino','Norvasc','CARDIOVASCULAR','BCC','Tableta','10mg',30,'PFIZER','RX',FALSE,48.00,98.00),
('SAP-022','Enalapril 10mg Tabletas','Enalapril','Vasotec','CARDIOVASCULAR','IECA','Tableta','10mg',30,'MERCK','RX',FALSE,32.00,65.00),
('SAP-023','Clopidogrel 75mg Tabletas','Clopidogrel','Plavix','CARDIOVASCULAR','ANTIAGREGANTE','Tableta','75mg',30,'SANOFI','RX',FALSE,95.00,195.00),
('SAP-024','Furosemida 40mg Tabletas','Furosemida','Lasix','CARDIOVASCULAR','DIURET','Tableta','40mg',30,'SANOFI','RX',FALSE,15.00,32.00),
-- DIABETES
('SAP-025','Metformina 850mg Tabletas','Metformina','Glucophage','DIABETES','BIGUANIDA','Tableta','850mg',60,'MERCK','RX',FALSE,28.00,58.00),
('SAP-026','Glibenclamida 5mg Tabletas','Glibenclamida','Euglucon','DIABETES','SULFONILUREA','Tableta','5mg',30,'ROCHE','RX',FALSE,18.00,38.00),
('SAP-027','Insulina NPH 100UI/ml Inyectable','Insulina NPH','Humulin N','DIABETES','INSULINA','Inyectable','100UI/ml',1,'LILLY','RX',TRUE,285.00,585.00),
('SAP-028','Sitagliptina 100mg Tabletas','Sitagliptina','Januvia','DIABETES','DPP-4','Tableta','100mg',30,'MERCK','RX',FALSE,485.00,995.00),
-- RESPIRATORIO
('SAP-029','Salbutamol 100mcg Inhalador','Salbutamol','Ventolin','RESPIRATORIO','BRONCODILATADOR','Inhalador','100mcg',200,'GSK','RX',FALSE,195.00,400.00),
('SAP-030','Loratadina 10mg Tabletas','Loratadina','Claritin','RESPIRATORIO','ANTIHISTAMÍNICO','Tableta','10mg',10,'MERCK','OTC',FALSE,32.00,65.00),
('SAP-031','Montelukast 10mg Tabletas','Montelukast','Singulair','RESPIRATORIO','ANTILEUCOTRIENO','Tableta','10mg',30,'MERCK','RX',FALSE,245.00,500.00),
('SAP-032','Fluticasona 50mcg Spray Nasal','Fluticasona','Flixonase','RESPIRATORIO','CORTICOIDE_INH','Spray','50mcg',150,'GSK','RX',FALSE,385.00,790.00),
('SAP-033','Ambroxol 30mg Jarabe','Ambroxol','Mucosolvan','RESPIRATORIO','MUCOLÍTICO','Jarabe','30mg/5ml',120,'BOEHRINGER','OTC',FALSE,145.00,298.00),
-- GASTROENTEROLOGÍA
('SAP-034','Omeprazol 20mg Cápsulas','Omeprazol','Losec','GASTRO','INHIBIDOR_BBA','Cápsula','20mg',14,'ASTRA','OTC',FALSE,42.00,88.00),
('SAP-035','Ranitidina 150mg Tabletas','Ranitidina','Zantac','GASTRO','ANTAGONISTA_H2','Tableta','150mg',30,'GSK','OTC',FALSE,28.00,58.00),
('SAP-036','Metoclopramida 10mg Tabletas','Metoclopramida','Plasil','GASTRO','PROCINÉTICO','Tableta','10mg',30,'SANOFI','OTC',FALSE,18.00,38.00),
('SAP-037','Loperamida 2mg Cápsulas','Loperamida','Imodium','GASTRO','ANTIDIARREICO','Cápsula','2mg',12,'JNJ','OTC',FALSE,48.00,98.00),
('SAP-038','Pantoprazol 40mg Tabletas','Pantoprazol','Protonix','GASTRO','INHIBIDOR_BBA','Tableta','40mg',28,'PFIZER','RX',FALSE,95.00,195.00),
-- DERMATOLOGÍA
('SAP-039','Hidrocortisona 1% Crema','Hidrocortisona','Cortaid','DERMATOLOGIA','CORTICOIDE_TOP','Crema','1%',20,'JNJ','OTC',FALSE,55.00,115.00),
('SAP-040','Clotrimazol 1% Crema','Clotrimazol','Canestén','DERMATOLOGIA','ANTIFÚNGICO','Crema','1%',20,'BAYER','OTC',FALSE,48.00,98.00),
('SAP-041','Aciclovir 5% Crema','Aciclovir','Zovirax','DERMATOLOGIA','ANTIVIRAL','Crema','5%',5,'GSK','OTC',FALSE,145.00,298.00),
('SAP-042','Betametasona 0.05% Crema','Betametasona','Diprosone','DERMATOLOGIA','CORTICOIDE_TOP','Crema','0.05%',20,'MERCK','RX',FALSE,75.00,155.00),
-- ANTIPARASITARIO / OTROS
('SAP-043','Albendazol 400mg Tabletas','Albendazol','Zentel','ANTIPARASITARIO','BENCIMIDAZOL','Tableta','400mg',1,'GSK','OTC',FALSE,38.00,78.00),
('SAP-044','Ivermectina 6mg Tabletas','Ivermectina','Ivexterm','ANTIPARASITARIO','AVERMECTINA','Tableta','6mg',4,'LAFRANCOL','RX',FALSE,55.00,115.00),
('SAP-045','Dexametasona 4mg Inyectable','Dexametasona','Decadron','CORTICOIDE','SISTÉMICO','Inyectable','4mg/ml',25,'MERCK','RX',FALSE,18.00,38.00),
('SAP-046','Prednisona 20mg Tabletas','Prednisona','Meticorten','CORTICOIDE','SISTÉMICO','Tableta','20mg',20,'MERCK','RX',FALSE,25.00,52.00),
('SAP-047','Levotiroxina 100mcg Tabletas','Levotiroxina','Synthroid','HORMONA','TIROIDES','Tableta','100mcg',50,'ABBOTT','RX',FALSE,185.00,380.00),
('SAP-048','Alprazolam 0.5mg Tabletas','Alprazolam','Xanax','PSIQUIATRIA','BENZODIAC','Tableta','0.5mg',30,'PFIZER','RX',TRUE,68.00,140.00),
('SAP-049','Fluoxetina 20mg Cápsulas','Fluoxetina','Prozac','PSIQUIATRIA','ISRS','Cápsula','20mg',30,'LILLY','RX',FALSE,78.00,160.00),
('SAP-050','Amitriptilina 25mg Tabletas','Amitriptilina','Elavil','PSIQUIATRIA','ANTIDEPRESS','Tableta','25mg',30,'MEDCO','RX',FALSE,22.00,45.00)
) AS p(pid,pname,gname,bname,cat,subcat,pres,conc,upack,lcode,rx_otc,ctrl,ucost,lprice)
JOIN dw.dim_laboratory l ON l.lab_code = p.lcode
ON CONFLICT (product_id) DO NOTHING;

-- ── dim_distributor ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_distributor (
    distributor_key     SERIAL PRIMARY KEY,
    distributor_code    VARCHAR(20)  NOT NULL UNIQUE,
    distributor_name    VARCHAR(200) NOT NULL,
    distributor_type    VARCHAR(20)  NOT NULL, -- INTERNO | EXTERNO
    contact_name        VARCHAR(100),
    contact_email       VARCHAR(200),
    contact_phone       VARCHAR(30),
    excel_format_id     VARCHAR(10),   -- DIST_A..F for external
    rnc                 VARCHAR(20),
    active_flag         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dw.dim_distributor(distributor_code, distributor_name, distributor_type, excel_format_id, rnc) VALUES
    ('INT',     'Ventas Internas Pharma DR',      'INTERNO',  NULL,     '1-01-12345-6'),
    ('DIST_A',  'Distribuidora Ramos S.R.L.',     'EXTERNO',  'DIST_A', '1-01-23456-7'),
    ('DIST_B',  'MediFar Dominicana S.A.',        'EXTERNO',  'DIST_B', '1-01-34567-8'),
    ('DIST_C',  'Farmacorp S.A.',                 'EXTERNO',  'DIST_C', '1-01-45678-9'),
    ('DIST_D',  'AlphaFarma Group S.R.L.',        'EXTERNO',  'DIST_D', '1-01-56789-0'),
    ('DIST_E',  'BioPharma Distribution S.A.',    'EXTERNO',  'DIST_E', '1-01-67890-1'),
    ('DIST_F',  'MedDist Nacional S.R.L.',        'EXTERNO',  'DIST_F', '1-01-78901-2')
ON CONFLICT (distributor_code) DO NOTHING;

-- ── dim_salesperson ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_salesperson (
    salesperson_key     SERIAL PRIMARY KEY,
    salesperson_id      VARCHAR(20)  NOT NULL UNIQUE,
    full_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(200),
    phone               VARCHAR(30),
    zone_key            INT REFERENCES dw.dim_zone(zone_key),
    hire_date           DATE,
    base_salary         NUMERIC(12,2),
    commission_rate     NUMERIC(5,3) DEFAULT 0.035,  -- 3.5% default
    monthly_target      NUMERIC(15,2),
    supervisor_id       VARCHAR(20),
    active_flag         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dw.dim_salesperson(salesperson_id, full_name, email, zone_key, hire_date, commission_rate, monthly_target)
SELECT sp.sid, sp.fname, sp.email, z.zone_key, sp.hdate::DATE, sp.crate, sp.mtarget
FROM (VALUES
    ('SP-001','Carlos Batista Pérez',    'c.batista@pharmadr.com',   'CAP','2019-03-15',0.040,850000),
    ('SP-002','María González Sánchez',  'm.gonzalez@pharmadr.com',  'CAP','2020-06-01',0.038,820000),
    ('SP-003','José Rodríguez Mejía',    'j.rodriguez@pharmadr.com', 'CAP','2018-11-20',0.042,900000),
    ('SP-004','Ana Martínez Ortiz',      'a.martinez@pharmadr.com',  'CAP','2021-02-10',0.035,750000),
    ('SP-005','Luis Fernández Taveras',  'l.fernandez@pharmadr.com', 'NOR','2019-08-05',0.040,800000),
    ('SP-006','Carmen Valdez Peralta',   'c.valdez@pharmadr.com',    'NOR','2020-01-15',0.038,780000),
    ('SP-007','Roberto Jiménez Cruz',    'r.jimenez@pharmadr.com',   'NOR','2017-05-20',0.045,950000),
    ('SP-008','Patricia Almonte Rivas',  'p.almonte@pharmadr.com',   'NOR','2021-09-01',0.035,720000),
    ('SP-009','Miguel Ángel Rosario',    'm.rosario@pharmadr.com',   'EST','2018-03-10',0.040,820000),
    ('SP-010','Sandra Corporán Díaz',    's.corporan@pharmadr.com',  'EST','2019-11-25',0.038,780000),
    ('SP-011','Ramón Féliz Cepeda',      'r.feliz@pharmadr.com',     'EST','2020-07-15',0.036,760000),
    ('SP-012','Yolanda Tejeda Núñez',    'y.tejeda@pharmadr.com',    'SUR','2019-04-20',0.038,760000),
    ('SP-013','Eduardo Pichardo Reyes',  'e.pichardo@pharmadr.com',  'SUR','2020-10-01',0.035,720000),
    ('SP-014','Cecilia Marte Herrera',   'c.marte@pharmadr.com',     'SUR','2021-06-15',0.033,700000),
    ('SP-015','Francisco Castillo López','f.castillo@pharmadr.com',  'OES','2018-08-10',0.040,800000)
) AS sp(sid,fname,email,zcode,hdate,crate,mtarget)
JOIN dw.dim_zone z ON z.zone_code = sp.zcode
ON CONFLICT (salesperson_id) DO NOTHING;

-- ── dim_client ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_client (
    client_key          SERIAL PRIMARY KEY,
    client_id           VARCHAR(30)  NOT NULL UNIQUE,
    client_name         VARCHAR(200) NOT NULL,
    client_name_norm    VARCHAR(200) NOT NULL,
    client_type         VARCHAR(40)  NOT NULL,
    rnc                 VARCHAR(20),
    address             VARCHAR(300),
    phone               VARCHAR(30),
    email               VARCHAR(200),
    credit_limit        NUMERIC(15,2) DEFAULT 50000,
    payment_terms       SMALLINT DEFAULT 30,
    city_key            INT REFERENCES dw.dim_city(city_key),
    zone_key            INT REFERENCES dw.dim_zone(zone_key),
    salesperson_key     INT REFERENCES dw.dim_salesperson(salesperson_key),
    active_flag         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── dim_date ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key            INT PRIMARY KEY,       -- YYYYMMDD integer
    full_date           DATE NOT NULL UNIQUE,
    year                SMALLINT NOT NULL,
    quarter             SMALLINT NOT NULL,
    month_num           SMALLINT NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    month_name_short    CHAR(3) NOT NULL,
    week_num            SMALLINT NOT NULL,
    day_of_month        SMALLINT NOT NULL,
    day_of_week         SMALLINT NOT NULL,     -- 1=Mon ... 7=Sun
    day_name            VARCHAR(20) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday_dr       BOOLEAN NOT NULL DEFAULT FALSE,
    holiday_name        VARCHAR(100),
    fiscal_year         SMALLINT,
    fiscal_quarter      SMALLINT,
    year_month          CHAR(7)               -- 'YYYY-MM'
);

-- Generate calendar spine 2018-01-01 to 2030-12-31
INSERT INTO dw.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(YEAR  FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month_num,
    TO_CHAR(d, 'TMMonth') AS month_name,
    TO_CHAR(d, 'TMMon') AS month_name_short,
    EXTRACT(WEEK  FROM d)::SMALLINT AS week_num,
    EXTRACT(DAY   FROM d)::SMALLINT AS day_of_month,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,
    TO_CHAR(d, 'TMDay') AS day_name,
    EXTRACT(ISODOW FROM d) IN (6,7) AS is_weekend,
    FALSE AS is_holiday_dr,
    NULL AS holiday_name,
    EXTRACT(YEAR FROM d)::SMALLINT AS fiscal_year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS fiscal_quarter,
    TO_CHAR(d,'YYYY-MM') AS year_month
FROM generate_series('2018-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- Mark Dominican Republic public holidays
UPDATE dw.dim_date SET is_holiday_dr = TRUE, holiday_name = h.hname
FROM (VALUES
    ('01-01', 'Año Nuevo'),
    ('01-06', 'Día de Reyes'),
    ('01-21', 'Día de la Altagracia'),
    ('01-26', 'Día de Duarte'),
    ('02-27', 'Día de la Independencia'),
    ('05-01', 'Día del Trabajo'),
    ('06-11', 'Corpus Christi'),
    ('08-16', 'Día de la Restauración'),
    ('09-24', 'Día de las Mercedes'),
    ('11-06', 'Día de la Constitución'),
    ('12-25', 'Navidad')
) AS h(mday, hname)
WHERE TO_CHAR(full_date, 'MM-DD') = h.mday;
