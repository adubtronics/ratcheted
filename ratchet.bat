@echo off
REM =============================================================================
REM Ratchet Trading Bot Launcher (Windows)
REM Ensures all Python tracebacks and logs appear in this console window.
REM =============================================================================

REM --- Move to the folder containing this BAT file ---
cd /d "%~dp0"

echo.
echo ============================================================
echo   Ratchet Bot Launcher
echo   Working Directory: %cd%
echo ============================================================
echo.

REM --- Optional: activate virtual environment if present ---
if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call ".venv\Scripts\activate.bat"
) else (
    echo No local venv found (.venv). Using system Python...
)

echo.
echo Launching GUI: gui_main.py
echo.

REM --- Run with full traceback support + unbuffered output ---
python -X faulthandler -u gui_main.py

echo.
echo ============================================================
echo   Ratchet GUI exited.
echo   If there was an error, it will be shown above.
echo ============================================================
echo.

pause
