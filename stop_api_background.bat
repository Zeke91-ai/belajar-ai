@echo off
rem Asks the background API server (and the mirror it started) to stop cleanly.
cd /d "%~dp0"
".venv\Scripts\python.exe" manage.py api stop
