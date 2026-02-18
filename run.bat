@echo off
REM ============================================================
REM Pharma DR · One-Click Run Script (CMD Batch)
REM Double-click this file to start the BI Dashboard
REM ============================================================
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

echo.
echo ============================================
echo   Pharma DR - BI Platform
echo   Republica Dominicana - Industria Farmaceutica
echo ============================================

cd /d "%~dp0"

REM Check if database exists
if exist "local_setup\pharma_dr.duckdb" goto launch

echo.
echo [Setup] First-time initialization...
echo [1/2] Creating database schema...
python local_setup\db_init.py
if errorlevel 1 goto error

echo [2/2] Generating and loading data...
python local_setup\load_data.py
if errorlevel 1 goto error

:launch
echo.
echo ============================================
echo   Opening dashboard at: http://localhost:8501
echo   Press Ctrl+C to stop
echo ============================================
echo.

start "" http://localhost:8501
streamlit run local_setup\app.py --server.port 8501
goto end

:error
echo.
echo ERROR: Setup failed. Check Python installation.
echo Make sure you ran: pip install -r requirements_local.txt
pause

:end
