@echo off
rem Starts the Telegram Mirror UI. Double-click this file. No PowerShell needed.
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Virtual environment belum ada. Lihat README.md bagian Instalasi.
  pause
  exit /b 1
)
".venv\Scripts\python.exe" ui.py
pause
