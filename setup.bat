@echo off
REM One-time setup: creates a Python virtual environment (.venv), installs
REM dependencies and builds the warehouse from simulated data.
cd /d "%~dp0"

if not exist .venv\Scripts\python.exe (
    echo [1/3] Creating virtual environment in .venv ...
    python -m venv .venv || goto :error
) else (
    echo [1/3] Virtual environment already exists.
)

echo [2/3] Installing dependencies (first time takes a few minutes) ...
.venv\Scripts\python -m pip install --upgrade pip -q || goto :error
.venv\Scripts\python -m pip install -e ".[dashboard,dev]" -q || goto :error

echo [3/3] Building the data warehouse ...
.venv\Scripts\lakehouse run --start 2023-01-02 || goto :error

echo.
echo Setup complete. Double-click dashboard.bat to open the dashboard.
pause
exit /b 0

:error
echo.
echo Setup failed - see the messages above.
pause
exit /b 1
