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
set PYTHON_OK=0
set PYTHON_EXE=python

python -c "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1
if not errorlevel 1 (
    set PYTHON_OK=1
    for /f "tokens=2" %%v in ('python --version 2^>^&1') do echo [OK] Python %%v found on PATH.
) else (
    python --version >nul 2>&1
    if not errorlevel 1 (
        for /f "tokens=2" %%v in ('python --version 2^>^&1') do echo [WARN] Python %%v is too old ^(need 3.11+^). Will install a newer version.
    )
)

:: ── Auto-install Python 3.13 if missing or too old ───────────────────────────
if "!PYTHON_OK!"=="1" goto :deps

set PY_VERSION=3.13.3
set PY_INSTALLER=python-%PY_VERSION%-amd64.exe
set PY_URL=https://www.python.org/ftp/python/%PY_VERSION%/%PY_INSTALLER%
set PY_DEST=%TEMP%\%PY_INSTALLER%

echo [info] Downloading Python %PY_VERSION% - this may take a minute...
echo [info] From: %PY_URL%
echo.

curl -L --progress-bar -o "%PY_DEST%" "%PY_URL%" 2>nul
if errorlevel 1 (
    echo [info] curl failed, trying PowerShell download...
    powershell -NoProfile -Command "Invoke-WebRequest -Uri '%PY_URL%' -OutFile '%PY_DEST%' -UseBasicParsing"
)

if not exist "%PY_DEST%" (
    echo.
    echo [ERROR] Could not download Python installer.
    echo Please install Python 3.13 manually from:
    echo   https://www.python.org/downloads/
    echo Make sure to tick "Add Python to PATH", then run setup.bat again.
    goto :end
)

echo.
echo [info] Installing Python %PY_VERSION% silently (current user, added to PATH)...
"%PY_DEST%" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0 Include_launcher=1
if errorlevel 1 (
    echo [ERROR] Python installer returned an error.
    echo Try running the installer manually: %PY_DEST%
    goto :end
)

del /f /q "%PY_DEST%" >nul 2>&1

:: Refresh PATH for this session so the new Python is visible
for /f "skip=2 tokens=3*" %%a in ('reg query "HKCU\Environment" /v PATH 2^>nul') do set "USER_PATH=%%a %%b"
set "PATH=%USER_PATH%;%PATH%"

python --version >nul 2>&1
if not errorlevel 1 goto :py_ok
py -3 --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_EXE=py -3
    goto :py_ok
)
echo.
echo [ERROR] Python was installed but is not yet on PATH.
echo Please close this window, open a new Command Prompt and run setup.bat again.
goto :end

:py_ok
for /f "tokens=2" %%v in ('python --version 2^>^&1') do echo [OK] Python %%v installed successfully.
echo.

:deps
:: ── Install dependencies ─────────────────────────────────────────────────────
echo Installing required packages (this may take a few minutes)...
echo.
!PYTHON_EXE! -m pip install --upgrade pip --quiet
!PYTHON_EXE! -m pip install playwright pandas openpyxl xlrd google-cloud-storage python-dotenv requests msal --quiet

if errorlevel 1 (
    echo.
    echo [ERROR] Package installation failed.
    echo Please check your internet connection and try again.
    goto :end
)
echo [OK] Packages installed.
echo.

:: ── Install Playwright browser (Chromium) ────────────────────────────────────
echo Installing Playwright Chromium browser...
!PYTHON_EXE! -m playwright install chromium --quiet
if errorlevel 1 (
    echo [WARN] Playwright browser install may have had issues. Continuing...
) else (
    echo [OK] Playwright Chromium ready.
)
echo.

:: ── Create data folder ───────────────────────────────────────────────────────
if not exist "%ROOT%data\" mkdir "%ROOT%data"
echo [OK] data\ folder ready.

:: ── Check GCP credentials ─────────────────────────────────────────────────────
if exist "%ROOT%credentials\gcp_key.json" (
    echo [OK] GCP credentials found.
) else (
    echo [ERROR] credentials\gcp_key.json is missing.
    echo Please contact your administrator for a fresh installation package.
    goto :end
)

:: ── Check .env credentials ────────────────────────────────────────────────────
echo [info] Looking for credentials\.env at: %ROOT%credentials\.env
if not exist "%ROOT%credentials\.env" (
    echo [ERROR] credentials\.env is missing.
    echo Please contact your administrator for a fresh installation package.
    goto :end
)
echo [OK] credentials\.env found.

!PYTHON_EXE! -c "import sys; from pathlib import Path; data=Path(r'%ROOT%credentials\.env').read_text(encoding='utf-8-sig'); found={k.strip():v.strip().strip(chr(34)).strip(chr(39)) for l in data.splitlines() if '=' in l.strip() and not l.strip().startswith('#') for k,_,v in [l.strip().partition('=')]}; req=['MS_CLIENT_ID','MS_CLIENT_SECRET','MS_TENANT_ID','MS_GRAPH_MAILBOX','UNITY_USERNAME','UNITY_PASSWORD','VESTED_EMAIL','VESTED_PASSWORD','ANGELONE_USERID','ANGELONE_PASSWORD','ASK_USERNAME','ASK_PASSCODE']; miss=[k for k in req if not found.get(k)]; [print('[ERROR] Missing: '+k) for k in miss]; print('[OK] All credentials validated.') if not miss else None; sys.exit(len(miss))"
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
