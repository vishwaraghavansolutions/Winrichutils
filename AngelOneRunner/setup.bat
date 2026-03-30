@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"
title Angel One Daily Runner — Setup
if not "%1"=="LAUNCHED" (
    cmd /k "%~f0" LAUNCHED
    exit /b
)

echo ============================================================
echo   Angel One Daily Runner — First-time Setup
echo ============================================================
echo.

:: ── Resolve installation root (handles nested-folder installs) ───────────────
set ROOT=%~dp0
if not exist "%ROOT%credentials\" (
    if exist "%ROOT%..\credentials\" (
        set ROOT=%ROOT%..\
    )
)
echo [info] Installation root: %ROOT%
echo.

:: ── Check Python ────────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not on PATH.
    echo.
    echo Please install Python 3.11 or later from:
    echo   https://www.python.org/downloads/
    echo.
    echo Make sure to tick "Add Python to PATH" during installation.
    echo Then run this setup.bat again.
    goto :end
)

for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo [OK] Python %PYVER% found.
echo.

:: ── Install dependencies ─────────────────────────────────────────────────────
echo Installing required packages (this may take a few minutes)...
echo.
python -m pip install --upgrade pip --quiet
python -m pip install playwright pandas openpyxl xlrd google-cloud-storage python-dotenv requests msal --quiet

if errorlevel 1 (
    echo.
    echo [ERROR] Package installation failed.
    echo Please check your internet connection and try again.
    goto :end
)
echo [OK] Packages installed.
echo.

:: ── Create data folder ───────────────────────────────────────────────────────
if not exist "%ROOT%data\" mkdir "%ROOT%data"
echo [OK] data\ folder ready.

:: ── Check GCP credentials ────────────────────────────────────────────────────
if exist "%ROOT%credentials\gcp_key.json" (
    echo [OK] GCP credentials found.
) else (
    echo [ERROR] credentials\gcp_key.json is missing.
    echo Please contact your administrator for a fresh installation package.
    goto :end
)

:: ── Check .env credentials ───────────────────────────────────────────────────
echo [info] Looking for credentials\.env at: %ROOT%credentials\.env
if not exist "%ROOT%credentials\.env" (
    echo [ERROR] credentials\.env is missing.
    echo Please contact your administrator for a fresh installation package.
    goto :end
)
echo [OK] credentials\.env found.

python -c "import sys; from pathlib import Path; data=Path(r'%ROOT%credentials\.env').read_text(encoding='utf-8-sig'); found={k.strip():v.strip().strip(chr(34)).strip(chr(39)) for l in data.splitlines() if '=' in l.strip() and not l.strip().startswith('#') for k,_,v in [l.strip().partition('=')]}; req=['MS_CLIENT_ID','MS_CLIENT_SECRET','MS_TENANT_ID','MS_GRAPH_MAILBOX','UNITY_USERNAME','UNITY_PASSWORD','VESTED_EMAIL','VESTED_PASSWORD','ANGELONE_USERID','ANGELONE_PASSWORD','ASK_USERNAME','ASK_PASSCODE']; miss=[k for k in req if not found.get(k)]; [print('[ERROR] Missing: '+k) for k in miss]; print('[OK] All credentials validated.') if not miss else None; sys.exit(len(miss))"
if errorlevel 1 (
    echo.
    echo Please fill in the missing values in credentials\.env and run setup.bat again.
    goto :end
)

echo.
echo ============================================================
echo   Setup complete!
echo   Double-click run_daily.bat each morning to fetch data.
echo ============================================================

:end
echo.
echo Press any key to close this window...
pause >nul
cmd /k echo Setup finished. You may close this window.
