@echo off
REM =====================================================================
REM run_worker.bat
REM ---------------------------------------------------------------------
REM Sobe o worker Celery em modo --pool=solo (obrigatorio no Windows -
REM o Celery dropou suporte oficial a fork no Windows desde a versao 4).
REM
REM Pool=solo == 1 task por vez, sequencial. Suficiente para dev/smoke.
REM Em PROD (Linux/Docker) trocar por --pool=prefork --concurrency=N.
REM
REM Escuta as 3 filas dedicadas:
REM   queue_inbound    (webhook -> dispatch leve)
REM   queue_extractor  (LLM/RAG - lento)
REM   queue_sync       (sync Sheets->Drive - pesado)
REM =====================================================================

cd /d "%~dp0\.."

REM Ativa venv local se existir (.venv ou venv)
if exist ".venv\Scripts\activate.bat" call ".venv\Scripts\activate.bat"
if exist "venv\Scripts\activate.bat"  call "venv\Scripts\activate.bat"

echo [SAMBA] Iniciando Celery worker (pool=solo)...
echo [SAMBA] Filas: queue_inbound, queue_extractor, queue_sync
echo.

celery -A core.celery_app worker -l info --pool=solo -Q queue_inbound,queue_extractor,queue_sync
