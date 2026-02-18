#!/bin/bash
# ============================================================
# Pharma DR · Azure VM Deployment Script
# Deploys full stack on Azure Standard D4s v3 (Ubuntu 22.04)
# ============================================================

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────
RESOURCE_GROUP="rg-pharma-dr"
VM_NAME="vm-pharma-dr"
VM_SIZE="Standard_D4s_v3"       # 4 vCPU / 16 GB RAM
LOCATION="eastus2"
ADMIN_USER="pharmaadmin"
STORAGE_ACCOUNT="stpharmadr$(date +%s | tail -c 6)"
DOMAIN_NAME="pharmadr"           # For Let's Encrypt SSL

# ── Check Prerequisites ───────────────────────────────────────
command -v az    >/dev/null 2>&1 || { echo "Azure CLI not found. Install: https://docs.microsoft.com/cli/azure/install-azure-cli"; exit 1; }
command -v ssh   >/dev/null 2>&1 || { echo "SSH not found."; exit 1; }
command -v scp   >/dev/null 2>&1 || { echo "SCP not found."; exit 1; }

echo "============================================================"
echo "Pharma DR · Azure Deployment"
echo "Resource Group: $RESOURCE_GROUP"
echo "VM: $VM_NAME ($VM_SIZE) in $LOCATION"
echo "============================================================"

# ── 1. Create Resource Group ──────────────────────────────────
echo "[1/10] Creating resource group..."
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

# ── 2. Create Storage Account (Excel landing zone + backups) ──
echo "[2/10] Creating storage account..."
az storage account create \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_GRS \
    --kind StorageV2

az storage container create \
    --account-name "$STORAGE_ACCOUNT" \
    --name "raw-excel-feeds"

az storage container create \
    --account-name "$STORAGE_ACCOUNT" \
    --name "etl-logs"

az storage container create \
    --account-name "$STORAGE_ACCOUNT" \
    --name "backups"

# ── 3. Create VM with SSH key ─────────────────────────────────
echo "[3/10] Creating VM..."
az vm create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VM_NAME" \
    --image Ubuntu2204 \
    --size "$VM_SIZE" \
    --admin-username "$ADMIN_USER" \
    --generate-ssh-keys \
    --public-ip-sku Standard \
    --os-disk-size-gb 128 \
    --os-disk-delete-option Delete

# Get public IP
PUBLIC_IP=$(az vm show -d -g "$RESOURCE_GROUP" -n "$VM_NAME" --query publicIps -o tsv)
echo "  VM Public IP: $PUBLIC_IP"

# ── 4. Configure NSG (open ports 80, 443, 8088 for demo) ──────
echo "[4/10] Configuring network security..."
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "${VM_NAME}NSG" \
    --name "AllowHTTPS" \
    --protocol tcp \
    --priority 100 \
    --destination-port-range 443

az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "${VM_NAME}NSG" \
    --name "AllowHTTP" \
    --protocol tcp \
    --priority 110 \
    --destination-port-range 80

az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "${VM_NAME}NSG" \
    --name "AllowSuperset" \
    --protocol tcp \
    --priority 120 \
    --destination-port-range 8088

# ── 5. Attach Data Disk (PostgreSQL data) ─────────────────────
echo "[5/10] Attaching data disk..."
az vm disk attach \
    --resource-group "$RESOURCE_GROUP" \
    --vm-name "$VM_NAME" \
    --name "${VM_NAME}-data" \
    --size-gb 512 \
    --sku Premium_LRS \
    --new

# ── 6. Configure VM via cloud-init ───────────────────────────
echo "[6/10] Copying project files to VM..."
sleep 30  # Wait for VM SSH to be ready

# Copy project directory
scp -o StrictHostKeyChecking=no -r "$(pwd)" "${ADMIN_USER}@${PUBLIC_IP}:/home/${ADMIN_USER}/pharma-dr"

# Run setup script on VM
ssh -o StrictHostKeyChecking=no "${ADMIN_USER}@${PUBLIC_IP}" << 'REMOTE_SCRIPT'
set -euo pipefail
echo "=== Remote Setup: Installing Docker ==="

# Mount data disk
sudo mkfs.ext4 /dev/sdc
sudo mkdir -p /data/postgres /data/etl
sudo mount /dev/sdc /data
echo '/dev/sdc /data ext4 defaults 0 0' | sudo tee -a /etc/fstab

# Install Docker
sudo apt-get update -y
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER

# Install Python 3.12
sudo apt-get install -y python3.12 python3.12-venv python3-pip

# Install nginx + certbot
sudo apt-get install -y nginx certbot python3-certbot-nginx

echo "=== Remote Setup: Docker installed ==="
REMOTE_SCRIPT

# ── 7. Start Docker Compose stack ────────────────────────────
echo "[7/10] Starting Docker Compose stack..."
ssh "${ADMIN_USER}@${PUBLIC_IP}" << REMOTE_SCRIPT2
cd /home/${ADMIN_USER}/pharma-dr
cp .env.example .env
echo "POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d '/' | head -c 32)" >> .env
echo "SUPERSET_SECRET_KEY=$(openssl rand -base64 42)" >> .env
echo "REDIS_PASSWORD=$(openssl rand -base64 24 | head -c 24)" >> .env
sudo docker compose up -d postgres redis pgbouncer
sleep 20
sudo docker compose up -d superset superset-worker superset-beat
sudo docker compose up -d etl
sudo docker compose up -d nginx
REMOTE_SCRIPT2

# ── 8. Initialize Database ────────────────────────────────────
echo "[8/10] Initializing database..."
ssh "${ADMIN_USER}@${PUBLIC_IP}" << REMOTE_SCRIPT3
cd /home/${ADMIN_USER}/pharma-dr
pip3 install -r requirements.txt
python3 scripts/init_db.py
python3 data/synthetic/generate_data.py
python3 etl/pipelines/full_load_pipeline.py
REMOTE_SCRIPT3

# ── 9. Setup Superset ─────────────────────────────────────────
echo "[9/10] Setting up Superset..."
ssh "${ADMIN_USER}@${PUBLIC_IP}" bash scripts/setup_superset.sh

# ── 10. Configure SSL (Let's Encrypt) ────────────────────────
echo "[10/10] DNS + SSL setup..."
echo ""
echo "============================================================"
echo "MANUAL STEP REQUIRED:"
echo "  1. Point your domain to: $PUBLIC_IP"
echo "  2. Run on VM:"
echo "     sudo certbot --nginx -d your-domain.com"
echo "============================================================"

echo ""
echo "============================================================"
echo "DEPLOYMENT COMPLETE!"
echo ""
echo "Superset (HTTP):  http://$PUBLIC_IP:8088"
echo "Superset (HTTPS): https://your-domain.com (after SSL setup)"
echo ""
echo "Admin credentials: see .env file on VM"
echo "============================================================"
