@echo off
REM =====================================================================
REM run_redis_wsl.bat
REM ---------------------------------------------------------------------
REM Sobe o Redis dentro do WSL (Ubuntu) e expoe na porta 6379 do Windows.
REM Pre-requisitos:
REM   1) WSL2 instalado (wsl --install)
REM   2) Dentro do WSL: sudo apt-get install -y redis-server
REM
REM Alternativa nativa Windows: instalar Memurai (https://www.memurai.com)
REM   - Memurai roda como servico Windows na mesma porta 6379, dispensa
REM     este script. Use "memurai-cli ping" para validar.
REM
REM Como validar: em outro terminal, rode `redis-cli ping` (ou
REM `wsl redis-cli ping`) - deve responder "PONG".
REM =====================================================================

echo [SAMBA] Subindo Redis no WSL...
echo [SAMBA] (Ctrl+C para parar)
echo.

wsl -e redis-server --bind 0.0.0.0 --protected-mode no
