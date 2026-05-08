"""
scripts/register_drive_webhook.py
===================================
Utilitário de registro do canal Push Notification da Google Drive API.

O que faz:
  1. Inicializa o StartPageToken na tabela `drive_sync_state` (evita processar
     ficheiros históricos na primeira notificação).
  2. Chama `drive_service.changes().watch()` para registrar a URL pública do
     servidor FastAPI como destino das notificações.
  3. Imprime o `channelId` e `expiration` (máx 7 dias) para monitoramento.

Expiração:
  O Google expira canais Push em até 7 dias. Configure um cron job ou Celery
  Beat para re-executar este script antes da expiração:
    0 6 */6 * *   python scripts/register_drive_webhook.py   # a cada 6 dias

  Ou adicione a task `task_renew_drive_webhook` ao Beat (Sprint J).

Variáveis de ambiente (requeridas):
  DRIVE_WEBHOOK_PUBLIC_URL   — URL pública do servidor FastAPI, ex.:
                               https://samba.example.com  (sem trailing slash)
                               https://abc123.ngrok.io    (desenvolvimento)
  DRIVE_WEBHOOK_TOKEN        — Token compartilhado para autenticar notificações.
                               Deve ser o mesmo valor em DRIVE_WEBHOOK_TOKEN do
                               servidor FastAPI (veja api/webhook.py).

Variáveis de ambiente (opcionais):
  DRIVE_COMERCIAL_FOLDER_ID  — Se setado, observa apenas a pasta COMERCIAL.
                               Se não setado, observa mudanças em todo o Drive.
  GOOGLE_SERVICE_ACCOUNT_FILE / GOOGLE_CREDENTIALS_FILE
                             — Credenciais Google (padrão: config/service_account.json)

Execução:
  python scripts/register_drive_webhook.py

  Saída esperada:
    [OK] StartPageToken inicializado: 1234567
    [OK] Canal registrado com sucesso:
         channelId   : samba-rag-20260423T094800
         resourceId  : AbCdEfGhIjKl...
         expiration  : 2026-04-30T09:48:00Z (em 7 dias)
         URL callback: https://samba.example.com/webhook/drive
"""
from __future__ import annotations

import os
import sys
import uuid
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("register_drive_webhook")

# ── Constantes ─────────────────────────────────────────────────────────────────

WEBHOOK_PATH = "/webhook/drive"
CHANNEL_TTL_MS = 7 * 24 * 60 * 60 * 1000   # 7 dias em milissegundos (máx permitido)


def main() -> None:
    # ── 1. Validação das variáveis de ambiente ─────────────────────────────────
    public_url = os.getenv("DRIVE_WEBHOOK_PUBLIC_URL", "").rstrip("/")
    token      = os.getenv("DRIVE_WEBHOOK_TOKEN", "")
    folder_id  = os.getenv("DRIVE_COMERCIAL_FOLDER_ID", "")

    if not public_url:
        logger.error(
            "DRIVE_WEBHOOK_PUBLIC_URL não definida. "
            "Exemplo: export DRIVE_WEBHOOK_PUBLIC_URL=https://samba.example.com"
        )
        sys.exit(1)

    if not token:
        logger.warning(
            "DRIVE_WEBHOOK_TOKEN não definido. "
            "Recomendado para validar notificações em api/webhook.py. "
            "Gerando token aleatório para esta sessão..."
        )
        token = uuid.uuid4().hex
        logger.info("Token gerado (cole no .env): DRIVE_WEBHOOK_TOKEN=%s", token)

    callback_url = f"{public_url}{WEBHOOK_PATH}"
    channel_id   = f"samba-rag-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    logger.info("Registrando canal Drive Push Notification...")
    logger.info("  URL callback: %s", callback_url)
    logger.info("  channelId  : %s", channel_id)

    # ── 2. Autenticação Drive ──────────────────────────────────────────────────
    from services.google_drive import drive_manager

    if not drive_manager.service:
        logger.error("Drive não autenticado. Verifique config/service_account.json ou config/credentials.json")
        sys.exit(1)

    svc = drive_manager.service

    # ── 3. Inicializa StartPageToken no banco ──────────────────────────────────
    try:
        from models.database import get_session, DriveSyncState

        resp_token = svc.changes().getStartPageToken(supportsAllDrives=True).execute()
        start_token = resp_token.get("startPageToken", "")

        session = get_session()
        try:
            row = session.query(DriveSyncState).filter_by(key="changes_page_token").first()
            if row:
                logger.info(
                    "StartPageToken já existia no banco: %s → substituindo por %s",
                    row.value, start_token,
                )
                row.value = start_token
                row.updated_at = datetime.utcnow()
            else:
                session.add(DriveSyncState(
                    key="changes_page_token",
                    value=start_token,
                    updated_at=datetime.utcnow(),
                ))
            session.commit()
            logger.info("[OK] StartPageToken inicializado: %s", start_token)
        finally:
            session.close()
    except Exception:
        logger.exception("Falha ao inicializar StartPageToken")
        sys.exit(1)

    # ── 4. Registra o canal Watch ──────────────────────────────────────────────
    body = {
        "id":      channel_id,
        "type":    "web_hook",
        "address": callback_url,
        "token":   token,
        "expiration": int(
            (datetime.now(timezone.utc) + timedelta(milliseconds=CHANNEL_TTL_MS))
            .timestamp() * 1000
        ),
    }

    try:
        if folder_id:
            # Observa apenas a pasta COMERCIAL (via Files.watch — alternativa)
            # A Changes API não filtra por pasta; o filtro acontece no task processor.
            # Usamos Changes.watch para consistência com o delta_scan.
            logger.info("  Pasta monitorada: %s", folder_id)

        watch_resp = svc.changes().watch(
            pageToken=start_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            body=body,
        ).execute()

        resource_id  = watch_resp.get("resourceId", "")
        exp_ms       = int(watch_resp.get("expiration", 0))
        exp_dt       = datetime.fromtimestamp(exp_ms / 1000, tz=timezone.utc)
        days_left    = (exp_dt - datetime.now(timezone.utc)).days

        # Persiste info do canal para renovação automática (task_renew_drive_webhook)
        from models.database import get_session as _get_sess, DriveSyncState as _DSS
        _sess = _get_sess()
        try:
            for _k, _v in [
                ("drive_channel_id",           channel_id),
                ("drive_resource_id",           resource_id),
                ("drive_channel_expiration_ms", str(exp_ms)),
            ]:
                _row = _sess.query(_DSS).filter_by(key=_k).first()
                if _row:
                    _row.value = _v
                    _row.updated_at = datetime.utcnow()
                else:
                    _sess.add(_DSS(key=_k, value=_v, updated_at=datetime.utcnow()))
            _sess.commit()
            logger.info("[OK] Info do canal persistida no banco (drive_channel_*).")
        finally:
            _sess.close()

        logger.info("[OK] Canal registrado com sucesso:")
        logger.info("     channelId   : %s", channel_id)
        logger.info("     resourceId  : %s", resource_id)
        logger.info("     expiration  : %s (em %d dias)", exp_dt.isoformat(), days_left)
        logger.info("     URL callback: %s", callback_url)
        logger.info("")
        logger.info("Renovacao automatica: task_renew_drive_webhook (Beat diario 06:00 BRT).")

    except Exception:
        logger.exception("Falha ao registrar canal Watch no Drive")
        sys.exit(1)


if __name__ == "__main__":
    main()
