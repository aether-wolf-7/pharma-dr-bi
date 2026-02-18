# ============================================================
# Pharma DR · One-Click Run Script (PowerShell)
# ============================================================
# Usage: Right-click → Run with PowerShell
#    OR: powershell -ExecutionPolicy Bypass -File run.ps1
# ============================================================

$env:PYTHONIOENCODING = "utf-8"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Pharma DR - BI Platform" -ForegroundColor Cyan
Write-Host "  Republica Dominicana - Industria Farmaceutica" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# Check Python
$pyVersion = python --version 2>&1
Write-Host "`n[1/4] Python: $pyVersion" -ForegroundColor Green

# Install/verify dependencies
Write-Host "`n[2/4] Checking dependencies..." -ForegroundColor Yellow
python -c "import duckdb, streamlit, plotly, pandas, faker" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "     Installing dependencies..." -ForegroundColor Yellow
    pip install duckdb pandas numpy streamlit plotly openpyxl rapidfuzz unidecode faker loguru rich tqdm python-dotenv --quiet
}
Write-Host "     Dependencies OK" -ForegroundColor Green

# Initialize DB if not exists
$dbPath = "local_setup\pharma_dr.duckdb"
if (-not (Test-Path $dbPath)) {
    Write-Host "`n[3/4] First-time setup: Creating database and loading data..." -ForegroundColor Yellow
    python local_setup\db_init.py
    python local_setup\load_data.py
    Write-Host "     Database ready!" -ForegroundColor Green
} else {
    Write-Host "`n[3/4] Database found: $dbPath" -ForegroundColor Green
    $size = [math]::Round((Get-Item $dbPath).Length / 1MB, 1)
    Write-Host "     Size: ${size} MB" -ForegroundColor Green
}

# Launch dashboard
Write-Host "`n[4/4] Launching BI Dashboard..." -ForegroundColor Cyan
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Dashboard URL: http://localhost:8501" -ForegroundColor White
Write-Host "  Press Ctrl+C to stop" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Open browser automatically
Start-Process "http://localhost:8501"

# Start Streamlit
streamlit run local_setup\app.py --server.port 8501
