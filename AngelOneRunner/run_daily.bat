@echo off
setlocal
cd /d "%~dp0"

:: ── Check Python ─────────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please run setup.bat first.
    pause
    exit /b 1
)

:: ── GCP credentials ──────────────────────────────────────────────────────────
if exist "%~dp0credentials\gcp_key.json" (
    set GOOGLE_APPLICATION_CREDENTIALS=%~dp0credentials\gcp_key.json
) else (
    echo [ERROR] credentials\gcp_key.json not found.
    echo Please run setup.bat first.
    pause
    exit /b 1
)

:: ── Launch the runner ─────────────────────────────────────────────────────────
python scripts\ao_daily_runner.py
