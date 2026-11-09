@echo off
REM Incremental daily load: picks up from the last loaded date, then rebuilds dbt models.
cd /d "%~dp0"
if not exist .venv\Scripts\lakehouse.exe (
    echo Run setup.bat first.
    pause
    exit /b 1
)
.venv\Scripts\lakehouse run
pause
