@echo off
REM =====================================================================
REM smoke_webhook.bat
REM ---------------------------------------------------------------------
REM Smoke test E2E: dispara um POST simulando uma mensagem do Twilio
REM WhatsApp e valida que o pipeline reage:
REM
REM   1) FastAPI recebe (200 OK + TwiML vazio)
REM   2) Mensagem persistida em `messages` (SQLite)
REM   3) Workflow Celery enfileirado em queue_inbound
REM   4) task_extract_message executa em queue_extractor
REM   5) task_sync_spreadsheet_to_drive executa em queue_sync
REM
REM Pre-requisitos (em terminais separados):
REM   - Redis rodando        (run_redis_wsl.bat ou Memurai)
REM   - Worker rodando       (run_worker.bat)
REM   - API rodando          (run_api.bat)
REM   - .env com TWILIO_VALIDATE_SIGNATURE=false
REM
REM Observe os logs do WORKER apos rodar este script - voce devera ver
REM as tres tasks pulsando em sequencia.
REM =====================================================================

setlocal

set URL=http://localhost:8000/webhook/twilio
set SID=SM_smoke_%RANDOM%

echo [SMOKE] Disparando POST para %URL%
echo [SMOKE] MessageSid=%SID%
echo.

curl -i -X POST %URL% ^
  -H "Content-Type: application/x-www-form-urlencoded" ^
  --data-urlencode "From=whatsapp:+5511999990001" ^
  --data-urlencode "To=whatsapp:+5513999990001" ^
  --data-urlencode "Body=Vendo 5000 ton soja FOB Santos a 420 USD entrega Maio" ^
  --data-urlencode "MessageSid=%SID%" ^
  --data-urlencode "ProfileName=Trader Smoke" ^
  --data-urlencode "NumMedia=0"

echo.
echo.
echo [SMOKE] Pronto. Verifique:
echo   - Resposta acima: HTTP/1.1 200 OK + ^<Response^>^</Response^>
echo   - Logs da API: 'webhook_twilio dispatched task=...'
echo   - Logs do WORKER: 'task_extract_message start' -^> 'task_sync_spreadsheet_to_drive start'
echo   - SQLite: linha em `messages` com attachment_name=twilio:%SID%

endlocal
