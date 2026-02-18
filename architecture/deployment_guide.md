# Deployment Guide
## Pharma DR · Step-by-Step

---

## Demo Deployment (24-Hour MVP)

### Prerequisites
- Docker Desktop ≥ 24 + Docker Compose v2
- Python 3.12+
- 8 GB free disk space
- Windows 10/11 or Linux/macOS

### Steps

```bash
# 1. Configure environment
copy .env.example .env
# Edit .env — at minimum set POSTGRES_PASSWORD, SUPERSET_SECRET_KEY, REDIS_PASSWORD

# 2. Start database + cache
docker compose up -d postgres redis pgbouncer
# Wait 15 seconds for PostgreSQL to initialize

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Initialize database schema
python scripts/init_db.py
# Expected output:
#   dim_zone:        5
#   dim_city:        32
#   dim_product:     50
#   dim_laboratory:  16
#   dim_salesperson: 15
#   dim_date:        4748 rows (2018–2030)

# 5. Generate synthetic data (~100k rows)
python data/synthetic/generate_data.py
# Expected: 6 Excel files + sales.csv + clients.csv

# 6. Run full ETL load
python etl/pipelines/full_load_pipeline.py
# Expected: ~100k rows inserted into dw.fact_sales

# 7. Start Superset + Nginx
docker compose up -d superset superset-worker superset-beat nginx

# 8. Initialize Superset (first time)
# Windows:
docker exec pharma_superset superset db upgrade
docker exec pharma_superset superset fab create-admin --username admin --firstname Admin --lastname Pharma --email admin@pharmadr.com --password admin
docker exec pharma_superset superset init

# 9. Access Superset
# http://localhost:8088
# Username: admin  |  Password: admin
```

### Post-Setup: Configure BI (30 min)

1. **Add Database Connection**
   - Settings → Database Connections → + Database
   - PostgreSQL: `postgresql+psycopg2://pharma_admin:YOUR_PASS@pgbouncer:6432/pharma_dr`
   - Test connection → Save

2. **Add Datasets** (one per dataset in dashboard_definitions.md)
   - Data → Datasets → + Dataset
   - Add: `mart.mv_sales_monthly_zone`, `mart.mv_top_clients`, etc.

3. **Build Charts** (use dashboard_definitions.md as reference)

4. **Assemble Dashboards** (15 dashboards)

5. **Add Native Filters** (date range, zone, category)

---

## Production Deployment (Azure VM)

### Estimated Time: 2-4 hours

```bash
# Prerequisites: Azure CLI logged in
az login

# Run automated deployment script
bash scripts/deploy_azure.sh
```

### Manual Azure Steps

**1. Resource Group + VM**
```bash
az group create -n rg-pharma-dr -l eastus2
az vm create \
    -g rg-pharma-dr \
    -n vm-pharma-dr \
    --image Ubuntu2204 \
    --size Standard_D4s_v3 \
    --admin-username pharmaadmin \
    --generate-ssh-keys
```

**2. Open Firewall Ports**
```bash
az vm open-port -g rg-pharma-dr -n vm-pharma-dr --port 443 --priority 100
az vm open-port -g rg-pharma-dr -n vm-pharma-dr --port 80  --priority 110
```

**3. SSH to VM and Install Docker**
```bash
PUBLIC_IP=$(az vm show -d -g rg-pharma-dr -n vm-pharma-dr --query publicIps -o tsv)
ssh pharmaadmin@$PUBLIC_IP

# On VM:
sudo apt update
sudo apt install -y docker.io docker-compose-plugin python3.12 python3-pip
sudo usermod -aG docker $USER
```

**4. Clone/Copy Project**
```bash
# From local machine:
scp -r ./Dominica pharmaadmin@$PUBLIC_IP:/home/pharmaadmin/pharma-dr
```

**5. Configure .env and Start**
```bash
# On VM:
cd /home/pharmaadmin/pharma-dr
cp .env.example .env
nano .env   # Set strong passwords

docker compose up -d
python3 scripts/init_db.py
python3 data/synthetic/generate_data.py
python3 etl/pipelines/full_load_pipeline.py
bash scripts/setup_superset.sh
```

**6. SSL with Let's Encrypt**
```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d pharmadr.yourcompany.com
```

**7. Point DNS**
- Create A record: `pharmadr.yourcompany.com` → `$PUBLIC_IP`

---

## Connecting Real Data Sources

### SAP HANA
1. Edit `.env`: Set `SAP_HANA_HOST`, `SAP_HANA_USER`, `SAP_HANA_PASSWORD`
2. Install hdbcli in ETL container:
   ```bash
   docker exec pharma_etl pip install hdbcli
   ```
3. Verify the user `PHARMA_ETL_USER` has SELECT on `VBAP_ENRICHED`, `KNA1_DR`, `MARA_DR`
4. Run incremental pipeline test:
   ```bash
   docker exec pharma_etl python etl/pipelines/incremental_pipeline.py sap
   ```

### SQL Server
1. Edit `.env`: Set `SQLSRV_HOST`, `SQLSRV_USER`, `SQLSRV_PASSWORD`
2. ODBC Driver 18 is pre-installed in the ETL container
3. Run migration test:
   ```bash
   docker exec pharma_etl python etl/pipelines/full_load_pipeline.py
   ```

### Excel Distributors
1. Drop Excel files into `data/excel_samples/` directory
2. File naming convention (see ExcelExtractor.FORMAT_PROFILES):
   - `DIST_A_*.xlsx`
   - `DIST_B_*.xlsx`
   - etc.
3. Scheduler auto-processes hourly
4. Processed files → `data/excel_processed/`
5. Rejected files → `data/excel_rejected/` with reason file

---

## Monitoring & Maintenance

### Check ETL Status
```sql
-- Recent ETL runs
SELECT * FROM audit.etl_run_log ORDER BY start_time DESC LIMIT 20;

-- Unmapped products needing review
SELECT * FROM audit.unmapped_products WHERE resolved_flag = FALSE ORDER BY occurrence_count DESC;

-- Data quality issues
SELECT * FROM audit.data_quality_log WHERE severity IN ('ERROR','CRITICAL') ORDER BY checked_at DESC;
```

### Refresh Materialized Views Manually
```sql
CALL mart.refresh_all_mvs();
```

### Database Backup
```bash
# Nightly backup script
docker exec pharma_postgres pg_dump -U pharma_admin pharma_dr | gzip > backup_$(date +%Y%m%d).sql.gz

# Upload to Azure Blob
az storage blob upload \
    --account-name YOUR_STORAGE \
    --container-name backups \
    --name backup_$(date +%Y%m%d).sql.gz \
    --file backup_$(date +%Y%m%d).sql.gz
```

### Scale Up (if needed)
- Resize VM: `az vm resize -g rg-pharma-dr -n vm-pharma-dr --size Standard_D8s_v3`
- Add Airflow: Replace APScheduler with `docker compose -f docker-compose.airflow.yml up -d`
- Add TimescaleDB: Enable extension for time-series optimization

---

## Estimated Cost (Azure, USD/month)

| Component | Size | Monthly Cost |
|---|---|---|
| VM Standard D4s v3 | 4 vCPU / 16 GB | ~$140 |
| P30 Managed Disk (512 GB) | Premium SSD | ~$65 |
| Storage Account | GRS, 100 GB | ~$5 |
| Public IP | Standard | ~$4 |
| Bandwidth (100 GB out) | | ~$8 |
| **Total** | | **~$222/month** |

*Tip: Use Azure Reserved Instances (1-year) to save ~35% → ~$145/month*
