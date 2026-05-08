@echo off
REM =====================================================================
REM run_beat.bat
REM ---------------------------------------------------------------------
REM Sobe o Celery Beat - scheduler que enfileira tasks periodicas.
REM
REM Atualmente agenda apenas:
REM   sync-spreadsheet-to-drive   a cada SAMBA_BEAT_SYNC_MINUTES (default 10)
REM
REM IMPORTANTE: rode em terminal SEPARADO do worker. Beat nao executa
REM tasks - ele apenas as enfileira; o worker e quem consome.
REM =====================================================================

cd /d "%~dp0\.."

if exist ".venv\Scripts\activate.bat" call ".venv\Scripts\activate.bat"
if exist "venv\Scripts\activate.bat"  call "venv\Scripts\activate.bat"

echo [SAMBA] Iniciando Celery Beat (scheduler)...
echo.

celery -A core.celery_app beat -l info
