@echo off
cd /d "%~dp0"
"%~dp0.venv\Scripts\python.exe" "%~dp0launch_app.py"
if errorlevel 1 (
    echo Launch failed. See the message above.
    pause
    exit /b 1
)
pause
