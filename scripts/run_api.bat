@echo off
REM =====================================================================
REM run_api.bat
REM ---------------------------------------------------------------------
REM Sobe o FastAPI (webhook Twilio) via uvicorn em modo --reload (dev).
REM
REM Endpoints:
REM   GET  /health           - liveness
REM   POST /webhook/twilio   - ingestao de mensagens WhatsApp
REM
REM Em DEV usa TWILIO_VALIDATE_SIGNATURE=false (do .env), permitindo
REM curl/Postman direto. Em PROD a assinatura HMAC-SHA1 e obrigatoria.
REM =====================================================================

cd /d "%~dp0\.."

if exist ".venv\Scripts\activate.bat" call ".venv\Scripts\activate.bat"
if exist "venv\Scripts\activate.bat"  call "venv\Scripts\activate.bat"

echo [SAMBA] Iniciando FastAPI (uvicorn) em http://localhost:8000
echo [SAMBA] Webhook: POST http://localhost:8000/webhook/twilio
echo.

uvicorn api.webhook:app --host 0.0.0.0 --port 8000 --reload
