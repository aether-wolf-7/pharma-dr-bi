#!/bin/bash
# ============================================================
# Pharma DR · Apache Superset Setup Script
# Run AFTER docker compose up -d
# ============================================================

set -euo pipefail

SUPERSET_CONTAINER="pharma_superset"
ADMIN_USER="${SUPERSET_ADMIN_USERNAME:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${SUPERSET_ADMIN_EMAIL:-admin@pharmadr.com}"

echo "============================================================"
echo "Pharma DR · Superset Setup"
echo "============================================================"

# Wait for Superset to be healthy
echo "[1/6] Waiting for Superset to start..."
for i in $(seq 1 60); do
    if docker exec "$SUPERSET_CONTAINER" curl -sf http://localhost:8088/health > /dev/null 2>&1; then
        echo "  Superset is ready!"
        break
    fi
    echo "  Attempt $i/60..."
    sleep 3
done

# Initialize Superset DB and create admin
echo "[2/6] Initializing Superset database..."
docker exec "$SUPERSET_CONTAINER" superset db upgrade

echo "[3/6] Creating admin user..."
docker exec "$SUPERSET_CONTAINER" superset fab create-admin \
    --username "$ADMIN_USER" \
    --firstname "Admin" \
    --lastname "PharmaDB" \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASS" || true  # Ignore if already exists

echo "[4/6] Running Superset init (roles, permissions)..."
docker exec "$SUPERSET_CONTAINER" superset init

# Create additional roles
echo "[5/6] Creating custom roles..."
docker exec "$SUPERSET_CONTAINER" python -c "
from superset import app
from superset.extensions import security_manager
with app.app_context():
    roles_to_create = [
        'Gerente_Nacional',
        'Gerente_Zona_CAP',
        'Gerente_Zona_NOR',
        'Gerente_Zona_EST',
        'Gerente_Zona_SUR',
        'Gerente_Zona_OES',
        'Vendedor',
        'Distribuidor_Externo',
        'Auditor',
    ]
    for role_name in roles_to_create:
        try:
            role = security_manager.find_role(role_name)
            if role is None:
                security_manager.add_role(role_name)
                print(f'  Created role: {role_name}')
            else:
                print(f'  Role exists: {role_name}')
        except Exception as e:
            print(f'  Error creating {role_name}: {e}')
" || echo "  Role creation requires manual setup via Superset UI"

echo "[6/6] Setup complete!"
echo ""
echo "============================================================"
echo "Superset Access:"
echo "  URL:      http://localhost:8088"
echo "  Username: $ADMIN_USER"
echo "  Password: $ADMIN_PASS"
echo "============================================================"
echo ""
echo "Next Steps:"
echo "  1. Open http://localhost:8088"
echo "  2. Settings → Database Connections → Add Pharma DR Warehouse"
echo "     URI: postgresql+psycopg2://pharma_admin:PASSWORD@pgbouncer:6432/pharma_dr"
echo "  3. Data → Datasets → Add all datasets from mart.* and dw.*"
echo "  4. Follow superset/dashboards/dashboard_definitions.md to build charts"
echo "  5. Configure Row Level Security for zone/role filtering"
