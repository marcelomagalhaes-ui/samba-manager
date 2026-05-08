"""
tasks/email_tasks.py
====================
Tasks Celery para as notificações periódicas via email + WhatsApp interno.

Acionadas pelo Beat (core/celery_app.py):
  - task_morning_brief    → 07:30 (America/Sao_Paulo)
  - task_eod_closing      → 18:30
  - task_intraday_alert   → a cada 30 min (dispara só se houver pendências)

Todas as queries SQL são read-only e trabalham contra a engine SQLite padrão.
Falhas de rede / credencial nunca levantam para o Beat — retornam {"status":"error"}.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from core.celery_app import celery_app

logger = logging.getLogger("samba.email_tasks")

QUEUE_NOTIFY  = "queue_notify"
QUEUE_SHEETS  = "queue_sync"


# ──────────────────────────────────────────────────────────────────
# Helpers de consulta
# ──────────────────────────────────────────────────────────────────

def _get_session():
    from models.database import get_session
    return get_session()


def _pipeline_por_stage(session) -> dict[str, int]:
    from sqlalchemy import text
    rows = session.execute(
        text("SELECT stage, COUNT(*) FROM deals WHERE status='ativo' GROUP BY stage")
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _deals_criados_no_periodo(session, desde: datetime, ate: datetime) -> int:
    from sqlalchemy import text
    r = session.execute(
        text(
            "SELECT COUNT(*) FROM deals WHERE status='ativo' "
            "AND created_at >= :desde AND created_at < :ate"
        ),
        {"desde": desde, "ate": ate},
    ).scalar()
    return r or 0


def _deals_em_qualificacao(session) -> list[dict[str, Any]]:
    from sqlalchemy import text
    rows = session.execute(
        text(
            "SELECT name, commodity, assignee, created_at "
            "FROM deals WHERE status='ativo' AND stage='Qualificação' "
            "ORDER BY created_at ASC LIMIT 30"
        )
    ).fetchall()
    return [
        {"name": r[0], "commodity": r[1], "assignee": r[2], "created_at": r[3]}
        for r in rows
    ]


def _deals_em_stage_hoje(session, stage: str) -> int:
    from sqlalchemy import text
    hoje = datetime.utcnow().date()
    r = session.execute(
        text(
            "SELECT COUNT(*) FROM deals WHERE status='ativo' "
            "AND stage=:stage AND DATE(updated_at)=:hoje"
        ),
        {"stage": stage, "hoje": str(hoje)},
    ).scalar()
    return r or 0


def _followups_vencidos(session) -> int:
    from sqlalchemy import text
    agora = datetime.utcnow()
    r = session.execute(
        text(
            "SELECT COUNT(*) FROM followups "
            "WHERE status='pendente' AND due_at <= :agora"
        ),
        {"agora": agora},
    ).scalar()
    return r or 0


def _deals_avancados_hoje(session) -> list[dict[str, Any]]:
    """Deals cujo updated_at é hoje e stage != 'Lead Capturado'."""
    from sqlalchemy import text
    hoje = datetime.utcnow().date()
    rows = session.execute(
        text(
            "SELECT name, commodity, stage, assignee FROM deals "
            "WHERE status='ativo' AND stage NOT IN ('Lead Capturado','Qualificação') "
            "AND DATE(updated_at)=:hoje ORDER BY updated_at DESC LIMIT 20"
        ),
        {"hoje": str(hoje)},
    ).fetchall()
    return [
        {"name": r[0], "commodity": r[1], "stage": r[2], "assignee": r[3]}
        for r in rows
    ]


def _deals_stale_sem_followup(session, horas: int = 2) -> list[dict[str, Any]]:
    """
    Deals em Qualificação há mais de `horas` horas sem FollowUp pendente.
    Detecta deals que precisam de ação humana mas ainda não foram notificados.
    """
    from sqlalchemy import text
    limite = datetime.utcnow() - timedelta(hours=horas)
    rows = session.execute(
        text(
            """
            SELECT d.id, d.name, d.commodity, d.assignee, d.source_sender, d.source_group
            FROM deals d
            WHERE d.status = 'ativo'
              AND d.stage = 'Qualificação'
              AND d.created_at <= :limite
              AND NOT EXISTS (
                  SELECT 1 FROM followups f
                  WHERE f.deal_id = d.id AND f.status = 'pendente'
              )
            ORDER BY d.created_at ASC
            LIMIT 20
            """
        ),
        {"limite": limite},
    ).fetchall()
    return [
        {
            "id": r[0], "name": r[1], "commodity": r[2],
            "assignee": r[3], "source_sender": r[4], "source_group": r[5],
        }
        for r in rows
    ]


# ──────────────────────────────────────────────────────────────────
# Tasks
# ──────────────────────────────────────────────────────────────────

@celery_app.task(
    name="tasks.email_tasks.task_morning_brief",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def task_morning_brief(self) -> dict[str, Any]:
    """
    Briefing matinal (07:30 BRT) — resumo do dia anterior + estado atual.
    """
    logger.info("task_morning_brief iniciando…")
    session = _get_session()
    try:
        ontem_inicio = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        ontem_fim    = ontem_inicio + timedelta(days=1)

        stats = {
            "deals_ontem":       _deals_criados_no_periodo(session, ontem_inicio, ontem_fim),
            "fechados_ontem":    _deals_em_stage_hoje(session, "Fechado"),
            "qualificacoes":     _deals_em_qualificacao(session),
            "followups_vencidos": _followups_vencidos(session),
            "pipeline_por_stage": _pipeline_por_stage(session),
        }
    finally:
        session.close()

    from services.internal_notify import get_notifier
    ok = get_notifier().send_morning_brief(stats)
    logger.info("task_morning_brief ok=%s", ok)
    return {"status": "ok" if ok else "error", "stats_keys": list(stats.keys())}


@celery_app.task(
    name="tasks.email_tasks.task_eod_closing",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def task_eod_closing(self) -> dict[str, Any]:
    """
    Fechamento do dia (18:30 BRT) — atividade do dia + pendências abertas.
    """
    logger.info("task_eod_closing iniciando…")
    session = _get_session()
    try:
        hoje_inicio = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        hoje_fim    = hoje_inicio + timedelta(days=1)

        quals = _deals_em_qualificacao(session)
        stats = {
            "deals_hoje":           _deals_criados_no_periodo(session, hoje_inicio, hoje_fim),
            "fechados_hoje":        _deals_em_stage_hoje(session, "Fechado"),
            "perdidos_hoje":        _deals_em_stage_hoje(session, "Perdido"),
            "qualificacoes_abertas": len(quals),
            "avancados_hoje":        _deals_avancados_hoje(session),
        }
    finally:
        session.close()

    from services.internal_notify import get_notifier
    ok = get_notifier().send_eod_closing(stats)
    logger.info("task_eod_closing ok=%s", ok)
    return {"status": "ok" if ok else "error", "stats_keys": list(stats.keys())}


@celery_app.task(
    name="tasks.email_tasks.task_intraday_alert",
    bind=True,
    max_retries=1,
    acks_late=True,
)
def task_intraday_alert(self) -> dict[str, Any]:
    """
    Alerta intraday (a cada 30 min) — dispara só se houver deals parados
    em Qualificação há >2h sem nenhum follow-up pendente.
    """
    logger.info("task_intraday_alert iniciando…")
    session = _get_session()
    try:
        stale = _deals_stale_sem_followup(session, horas=2)
    finally:
        session.close()

    if not stale:
        logger.info("task_intraday_alert: pipeline limpo — nenhum alerta enviado.")
        return {"status": "ok", "stale_count": 0}

    from services.internal_notify import get_notifier
    ok = get_notifier().send_intraday_alert(stale)
    logger.info("task_intraday_alert ok=%s stale_count=%s", ok, len(stale))
    return {"status": "ok" if ok else "error", "stale_count": len(stale)}


@celery_app.task(
    name="tasks.email_tasks.task_sync_extractor_tab",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def task_sync_extractor_tab(self) -> dict[str, Any]:
    """
    Atualiza a aba 'extractor' na planilha oficial com todos os deals
    que estão em Qualificação ou com dados comerciais incompletos.

    Acionado pelo Beat a cada 15 minutos — rotativamente junto com a
    sincronização Sheets→Drive, mas em fila separada (queue_sync).
    """
    logger.info("task_sync_extractor_tab iniciando…")
    try:
        from services.extractor_sheet_tab import ExtractorSheetTab
        tab = ExtractorSheetTab()
        result = tab.sync()
        logger.info("task_sync_extractor_tab resultado: %s", result)
        return result
    except Exception as exc:
        logger.exception("task_sync_extractor_tab erro: %s", exc)
        raise self.retry(exc=exc, countdown=60)
