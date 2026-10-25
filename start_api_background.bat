@echo off
rem Starts the headless API server (and the mirror) in the background. No window.
rem Logs: logs\crawler.log   Stop: stop_api_background.bat
cd /d "%~dp0"
if not exist ".venv\Scripts\pythonw.exe" (
  echo Virtual environment belum ada. Lihat README.md bagian Instalasi.
  pause
  exit /b 1
)
start "" /B ".venv\Scripts\pythonw.exe" api_server.py
echo API server dijalankan di latar belakang: http://127.0.0.1:8766/health
echo Cek status: .venv\Scripts\python manage.py api status
timeout /t 3 >nul
