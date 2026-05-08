"""
core/celery_app.py
==================
Instância única do Celery + Beat schedule do SAMBA CORE ENGINE.

Filas dedicadas isolam blast radius entre cargas com perfis muito diferentes:

  - queue_inbound   → ingestão do webhook (rápido, baixa latência, alta vazão)
  - queue_extractor → chamadas LLM/RAG (lentas, sujeitas a rate-limit do Gemini)
  - queue_sync      → sincronização Sheets→Drive (pesada, periódica via Beat)

Beat agenda `task_sync_spreadsheet_to_drive` a cada 10 minutos. O comportamento
do agente síncrono (`SpreadsheetSyncAgent.sincronizar_planilha_para_drive`)
permanece inalterado — Celery só substitui o **acionamento**.

Variáveis de ambiente:
  - REDIS_URL              (default: redis://localhost:6379/0)
  - SAMBA_BEAT_SYNC_MINUTES (default: 10) — período da sincronização

Como subir:

  Linux/WSL/Docker (default — pool prefork):
      celery -A core.celery_app worker -l info \\
          -Q queue_inbound,queue_extractor,queue_sync
      celery -A core.celery_app beat -l info

  Windows (worker dev only — Celery dropou suporte oficial em 4.x):
      celery -A core.celery_app worker -l info --pool=solo \\
          -Q queue_inbound,queue_extractor,queue_sync
"""
from __future__ import annotations

import logging
import os

from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger("samba.celery")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BEAT_SYNC_MINUTES = int(os.getenv("SAMBA_BEAT_SYNC_MINUTES", "10"))

# Nomes canônicos das tasks — referenciados em routes e beat_schedule.
TASK_PROCESS_INBOUND  = "tasks.agent_tasks.task_process_inbound_message"
TASK_EXTRACT_MESSAGE  = "tasks.agent_tasks.task_extract_message"
TASK_SYNC_SPREADSHEET = "tasks.agent_tasks.task_sync_spreadsheet_to_drive"
TASK_MORNING_BRIEF    = "tasks.email_tasks.task_morning_brief"
TASK_EOD_CLOSING      = "tasks.email_tasks.task_eod_closing"
TASK_INTRADAY_ALERT   = "tasks.email_tasks.task_intraday_alert"
TASK_EXTRACTOR_TAB        = "tasks.email_tasks.task_sync_extractor_tab"
TASK_DISPATCH_FOLLOWUPS   = "tasks.agent_tasks.task_dispatch_followups"
TASK_PROCESS_FU_RESPONSE  = "tasks.agent_tasks.task_process_followup_response"
TASK_PIPELINE_REPORT      = "tasks.agent_tasks.task_pipeline_report"
TASK_WPP_ENRICHMENT       = "tasks.agent_tasks.task_wpp_enrichment"
TASK_INGEST_DRIVE         = "tasks.agent_tasks.task_ingest_drive_files"
TASK_RENEW_DRIVE_WEBHOOK  = "tasks.agent_tasks.task_renew_drive_webhook"
# ── Sprint M — Agentes Ativos + Inteligência Geopolítica ─────────────────────
TASK_VOICE_ATA            = "tasks.agent_tasks.task_process_voice_meeting_minutes"
TASK_MORNING_PULSE        = "tasks.agent_tasks.task_morning_pulse"
TASK_DRIVE_STATUS_REPORT  = "tasks.agent_tasks.task_drive_status_report"
TASK_GEOPOLITICAL_MONITOR = "tasks.agent_tasks.task_geopolitical_monitor"
TASK_PROCESS_MENTION      = "tasks.agent_tasks.task_process_mention"

# Nomes das filas — exportados para teste e para o comando do worker.
QUEUE_INBOUND   = "queue_inbound"
QUEUE_EXTRACTOR = "queue_extractor"
QUEUE_SYNC      = "queue_sync"
QUEUE_NOTIFY    = "queue_notify"   # email + WhatsApp interno


celery_app = Celery(
    "samba_core",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["tasks.agent_tasks", "tasks.email_tasks"],
)

celery_app.conf.update(
    # Serialização — JSON evita pickle CVE e é debugável.
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Localização — datas/horários da operação no fuso de São Paulo.
    timezone="America/Sao_Paulo",
    enable_utc=True,
    # Confiabilidade: ack só após sucesso, evita perda em worker crash.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # IO/LLM-heavy — não pré-fetchar muito (evita travar workers em chamadas longas).
    worker_prefetch_multiplier=1,
    # Roteamento por nome canônico de task → fila dedicada.
    task_routes={
        TASK_PROCESS_INBOUND:  {"queue": QUEUE_INBOUND},
        TASK_EXTRACT_MESSAGE:  {"queue": QUEUE_EXTRACTOR},
        TASK_SYNC_SPREADSHEET: {"queue": QUEUE_SYNC},
        TASK_MORNING_BRIEF:    {"queue": QUEUE_NOTIFY},
        TASK_EOD_CLOSING:      {"queue": QUEUE_NOTIFY},
        TASK_INTRADAY_ALERT:   {"queue": QUEUE_NOTIFY},
        TASK_EXTRACTOR_TAB:       {"queue": QUEUE_SYNC},
        TASK_DISPATCH_FOLLOWUPS:  {"queue": QUEUE_EXTRACTOR},
        TASK_PROCESS_FU_RESPONSE: {"queue": QUEUE_NOTIFY},
        TASK_PIPELINE_REPORT:     {"queue": QUEUE_NOTIFY},
        TASK_WPP_ENRICHMENT:      {"queue": QUEUE_SYNC},
        TASK_INGEST_DRIVE:        {"queue": QUEUE_EXTRACTOR},  # LLM-heavy (embeddings)
        TASK_RENEW_DRIVE_WEBHOOK: {"queue": QUEUE_SYNC},
        # Sprint M
        TASK_VOICE_ATA:            {"queue": QUEUE_EXTRACTOR},  # Gemini multimodal (pesado)
        TASK_MORNING_PULSE:        {"queue": QUEUE_NOTIFY},
        TASK_DRIVE_STATUS_REPORT:  {"queue": QUEUE_NOTIFY},
        TASK_GEOPOLITICAL_MONITOR: {"queue": QUEUE_NOTIFY},
        TASK_PROCESS_MENTION:      {"queue": QUEUE_EXTRACTOR},  # LLM-heavy (Gemini cascade)
    },
    # Beat — agenda sincronização periódica + notificações do agente de email.
    beat_schedule={
        "sync-spreadsheet-to-drive": {
            "task": TASK_SYNC_SPREADSHEET,
            "schedule": crontab(minute=f"*/{BEAT_SYNC_MINUTES}"),
            "options": {"queue": QUEUE_SYNC},
        },
        # ── Notificações internas ─────────────────────────────────
        "morning-brief": {
            "task": TASK_MORNING_BRIEF,
            "schedule": crontab(hour=7, minute=30),   # 07:30 BRT
            "options": {"queue": QUEUE_NOTIFY},
        },
        "eod-closing": {
            "task": TASK_EOD_CLOSING,
            "schedule": crontab(hour=18, minute=30),  # 18:30 BRT
            "options": {"queue": QUEUE_NOTIFY},
        },
        "intraday-alert": {
            "task": TASK_INTRADAY_ALERT,
            "schedule": crontab(minute="*/30"),        # cada 30 min
            "options": {"queue": QUEUE_NOTIFY},
        },
        "sync-extractor-tab": {
            "task": TASK_EXTRACTOR_TAB,
            "schedule": crontab(minute="*/15"),        # cada 15 min
            "options": {"queue": QUEUE_SYNC},
        },
        # ── Follow-Up Agent ────────────────────────────────────────
        "dispatch-followups": {
            "task": TASK_DISPATCH_FOLLOWUPS,
            "schedule": crontab(minute="*/15"),        # cada 15 min
            "options": {"queue": QUEUE_EXTRACTOR},
        },
        # ── Drive Organic Knowledge — Full scan fallback 60 min ───────
        "drive-full-scan": {
            "task": TASK_INGEST_DRIVE,
            "schedule": crontab(minute=0),           # topo de cada hora
            "args": [],
            "kwargs": {"full_scan": True},
            "options": {"queue": QUEUE_EXTRACTOR},
        },
        # ── Drive Webhook Renewal — diário 06:00 BRT ─────────────────
        "renew-drive-webhook": {
            "task": TASK_RENEW_DRIVE_WEBHOOK,
            "schedule": crontab(hour=6, minute=0),   # 06:00 BRT diário
            "options": {"queue": QUEUE_SYNC},
        },
        # ── Sprint M — Agentes Ativos / Geopolítica ───────────────
        "morning-pulse-wpp": {
            "task":    TASK_MORNING_PULSE,
            "schedule": crontab(hour=7, minute=30),           # 07:30 BRT
            "options": {"queue": QUEUE_NOTIFY},
        },
        "drive-status-midday": {
            "task":    TASK_DRIVE_STATUS_REPORT,
            "schedule": crontab(hour=12, minute=0),           # 12:00 BRT
            "options": {"queue": QUEUE_NOTIFY},
        },
        "drive-status-eod": {
            "task":    TASK_DRIVE_STATUS_REPORT,
            "schedule": crontab(hour=18, minute=0),           # 18:00 BRT
            "options": {"queue": QUEUE_NOTIFY},
        },
        "geopolitical-monitor": {
            "task":    TASK_GEOPOLITICAL_MONITOR,
            "schedule": crontab(minute="*/30"),               # a cada 30 min (NewsData.io key rotation)
            "options": {"queue": QUEUE_NOTIFY},
        },
        # ── Pipeline Report — Sexta 16h + Domingo 21h BRT ─────────
        "pipeline-report-friday": {
            "task": TASK_PIPELINE_REPORT,
            "schedule": crontab(hour=16, minute=0, day_of_week=5),  # sexta-feira
            "options": {"queue": QUEUE_NOTIFY},
        },
        "pipeline-report-sunday": {
            "task": TASK_PIPELINE_REPORT,
            "schedule": crontab(hour=21, minute=0, day_of_week=0),  # domingo
            "options": {"queue": QUEUE_NOTIFY},
        },
    },
)

logger.info(
    "Celery configurado: broker=%s, beat_period=%dmin, queues=[%s, %s, %s]",
    REDIS_URL,
    BEAT_SYNC_MINUTES,
    QUEUE_INBOUND,
    QUEUE_EXTRACTOR,
    QUEUE_SYNC,
)
