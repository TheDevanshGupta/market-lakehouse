@echo off
REM Opens the dashboard at http://localhost:8501 (close this window to stop it).
cd /d "%~dp0"
if not exist .venv\Scripts\lakehouse.exe (
    echo Run setup.bat first.
    pause
    exit /b 1
)
.venv\Scripts\lakehouse dashboard
