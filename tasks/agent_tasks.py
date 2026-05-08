"""
tasks/agent_tasks.py
====================
Tasks Celery que envolvem (sem reescrever) os agentes existentes.

Princípios:
  - Os agentes (`ExtractorAgent`, `SpreadsheetSyncAgent`) permanecem intocados.
    As tasks são apenas drivers Celery — instanciam e invocam o método público.
  - Idempotência é responsabilidade do agente:
      * `ExtractorAgent.process_single_message` faz lookup por `source_message_id`
        antes de criar Deal, então redelivery do Twilio (webhook retry) NÃO
        duplica o pipeline.
      * `SpreadsheetSyncAgent.sincronizar_planilha_para_drive` é coluna-O-aware
        (linhas com STATUS=OK são puladas), então execuções concorrentes pelo
        Beat são seguras.
  - Retry policy é por task, não global. Falhas transitórias (rede, 5xx,
    bloqueio temporário do Drive) merecem retry exponencial. Falhas semânticas
    (`LLMUnavailable`) NÃO devem retentar — o agente já degradou para
    PENDING_IA na planilha (regra inviolável do circuit breaker).

Workflow:
  - `task_process_inbound_message(msg_id)` é o entrypoint do webhook.
    Dispara um `chain` que executa extração e, em sequência (`.si()` =
    immutable signature → ignora retorno), a sincronização da planilha.
"""
from __future__ import annotations

import logging
from typing import Any

from celery import chain
from celery.exceptions import Retry

from core.celery_app import celery_app
from sync.exceptions import LLMUnavailable

logger = logging.getLogger("samba.tasks")

# Backoff em segundos para retry exponencial: 30s, 2min, 8min.
RETRY_BACKOFF_BASE = 30
RETRY_MAX = 3


# ----------------------------------------------------------------------------
# Tasks — wrappers finos sobre os agentes
# ----------------------------------------------------------------------------

@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_extract_message",
    max_retries=RETRY_MAX,
    acks_late=True,
)
def task_extract_message(self, msg_id: int) -> dict[str, Any]:
    """
    Extrai uma única mensagem (ID conhecido) usando `ExtractorAgent`.

    Retry policy:
      - Exceções inesperadas (rede, DB hiccup) → retry exponencial.
      - `LLMUnavailable` NÃO é retentada — a regra de negócio é degradar
        para PENDING_IA, não martelar a API esgotada.
    """
    logger.info("task_extract_message start msg_id=%s attempt=%s", msg_id, self.request.retries)

    from agents.extractor_agent import ExtractorAgent

    try:
        agent = ExtractorAgent()
        result = agent.process_single_message(msg_id)
        logger.info("task_extract_message ok msg_id=%s result=%s", msg_id, result)
        return result
    except LLMUnavailable as exc:
        # Regra inviolável: LLM degradado não retenta. O orquestrador interno
        # já marca a saída apropriada — apenas log e devolve sinalização.
        logger.warning(
            "task_extract_message degradado (LLMUnavailable) msg_id=%s: %s",
            msg_id, exc,
        )
        return {"msg_id": msg_id, "skipped": "llm_unavailable", "reason": str(exc)}
    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_extract_message erro msg_id=%s — agendando retry", msg_id)
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_sync_spreadsheet_to_drive",
    max_retries=RETRY_MAX,
    acks_late=True,
)
def task_sync_spreadsheet_to_drive(self) -> dict[str, Any]:
    """
    Dispara a sincronização Sheets→Drive completa.

    Acionado por:
      - Beat (a cada 10min — agendado em `core.celery_app.beat_schedule`)
      - Workflow chain após uma extração bem-sucedida (ver
        `task_process_inbound_message`)

    Toda a inteligência (LLM, circuit breaker, OK-skip, Search & Destroy,
    contract_type detection) vive dentro do `SpreadsheetSyncAgent` —
    a task é deliberadamente um wrapper magro.
    """
    logger.info("task_sync_spreadsheet_to_drive start attempt=%s", self.request.retries)

    from agents.spreadsheet_sync_agent import SpreadsheetSyncAgent

    try:
        agent = SpreadsheetSyncAgent()
        agent.sincronizar_planilha_para_drive()
        logger.info("task_sync_spreadsheet_to_drive ok")
        return {"status": "ok"}
    except LLMUnavailable as exc:
        # O agente já tratou a degradação (PENDING_IA na planilha) e re-levantou
        # apenas se o circuit breaker tripou na 1ª linha. Não retentamos.
        logger.warning("task_sync_spreadsheet_to_drive degradado: %s", exc)
        return {"status": "degraded", "reason": str(exc)}
    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_sync_spreadsheet_to_drive erro — agendando retry")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_process_inbound_message",
    max_retries=RETRY_MAX,
    acks_late=True,
)
def task_process_inbound_message(self, msg_id: int) -> dict[str, Any]:
    """
    Entrypoint do webhook — orquestra o workflow completo de uma mensagem.

    Usa `chain` com `.si()` (immutable signature) na sync para que ela rode
    em sequência sem receber o retorno de `task_extract_message` como input.
    Cada elo da cadeia roda em sua fila dedicada (extractor → sync) e pode
    reenfileirar/retentar independentemente.
    """
    logger.info("task_process_inbound_message dispatching workflow msg_id=%s", msg_id)
    workflow = chain(
        task_extract_message.s(msg_id),
        task_sync_spreadsheet_to_drive.si(),
    )
    async_result = workflow.apply_async()
    logger.info(
        "task_process_inbound_message workflow_id=%s msg_id=%s",
        async_result.id, msg_id,
    )
    return {"workflow_id": async_result.id, "msg_id": msg_id}


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_dispatch_followups",
    max_retries=2,
    acks_late=True,
)
def task_dispatch_followups(
    self,
    dry_run: bool = False,
    max_batch: int = 5,   # conservador: 5 chamadas LLM por ciclo de 15 min
) -> dict[str, Any]:
    """
    Ciclo completo do FollowUpAgent, acionado pelo Beat a cada 15 min:
      1. Processa respostas recebidas (response_received=True → status=respondido)
      2. Envia follow-ups vencidos (due_at <= agora, status=pendente) via WhatsApp
      3. Escalona casos sem resposta há ESCALATE_AFTER_DAYS para o Manager

    Roda em queue_extractor (invoca LLM para gerar mensagens personalizadas).
    """
    logger.info(
        "task_dispatch_followups start dry_run=%s max_batch=%s attempt=%s",
        dry_run, max_batch, self.request.retries,
    )
    try:
        from agents.followup_agent import FollowUpAgent

        agent = FollowUpAgent()
        result = agent.process({"dry_run": dry_run, "max_batch": max_batch})
        agent.session.close()
        logger.info("task_dispatch_followups ok result=%s", result)
        return result
    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_dispatch_followups erro — agendando retry")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_pipeline_report",
    max_retries=2,
    acks_late=True,
)
def task_pipeline_report(self) -> dict:
    """
    Gera e envia o Relatório de Pipeline agrupado por grupo WhatsApp.

    Acionado pelo Celery Beat:
      - Sexta-feira 16:00 BRT
      - Domingo 21:00 BRT

    Lê a planilha "todos andamento", filtra deals ativos (STATUS_AUTOMACAO
    não REJECTED/SKIPPED), agrupa por coluna D (GRUPO) e envia email HTML
    com paleta oficial Samba Export para todos os diretores.
    """
    logger.info("task_pipeline_report start attempt=%s", self.request.retries)
    try:
        from agents.pipeline_report_agent import PipelineReportAgent

        agent  = PipelineReportAgent()
        result = agent.run()
        logger.info("task_pipeline_report ok result=%s", result)
        return result
    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_pipeline_report erro — agendando retry")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_wpp_enrichment",
    max_retries=2,
    acks_late=True,
)
def task_wpp_enrichment(self) -> dict:
    """
    Enriquece a planilha 'todos andamento' com dados extraídos das conversas
    de WhatsApp. Preenche SOMENTE células em branco — nunca sobrescreve.

    Acionado manualmente ou via Beat quando necessário.
    """
    logger.info("task_wpp_enrichment start attempt=%s", self.request.retries)
    try:
        from agents.wpp_enrichment_agent import WppEnrichmentAgent

        agent  = WppEnrichmentAgent()
        result = agent.run()
        logger.info("task_wpp_enrichment ok result=%s", result)
        return result
    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_wpp_enrichment erro — agendando retry")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_ingest_drive_files",
    max_retries=3,
    acks_late=True,
    time_limit=1800,        # 30 min — full_scan pode ser lento em pastas grandes
    soft_time_limit=1500,   # aviso 25 min antes de matar
)
def task_ingest_drive_files(self, full_scan: bool = False) -> dict:
    """
    Motor de Ingestão Orgânica de Conhecimento — Sprint I.

    Dois modos de operação:
      full_scan=False (padrão)
          Delta via Drive Changes API (StartPageToken). Usado pelo webhook
          /webhook/drive logo após notificação push. Processa apenas ficheiros
          alterados desde o último token persistido. Latência: segundos.

      full_scan=True
          Varre toda a pasta COMERCIAL e reingeribe ficheiros cujo md5Checksum
          mudou. Chamado pelo Beat a cada 60 min como safety-net. Também serve
          como bootstrap antes do webhook estar registrado.

    Idempotência garantida em ambos os modos via DriveSyncState (hash por file_id).
    Embedding: paraphrase-multilingual-MiniLM-L12-v2 (Sprint H).
    """
    logger.info(
        "task_ingest_drive_files start full_scan=%s attempt=%s",
        full_scan, self.request.retries,
    )
    try:
        from services.drive_ingestion import run_delta_scan, run_full_scan

        if full_scan:
            result = run_full_scan()
        else:
            result = run_delta_scan()

        logger.info("task_ingest_drive_files ok result=%s", result)
        return result

    except Retry:
        raise
    except Exception as exc:
        logger.exception(
            "task_ingest_drive_files erro full_scan=%s — agendando retry", full_scan
        )
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_renew_drive_webhook",
    max_retries=3,
    acks_late=True,
)
def task_renew_drive_webhook(self, force: bool = False) -> dict:
    """
    Verifica se o canal Push Notification do Drive expira em < 48h e renova.

    Fluxo:
      1. Lê drive_channel_expiration_ms do DriveSyncState.
      2. Se expiração < agora + 48h (ou force=True), chama changes().watch()
         com um novo channelId para o mesmo callback URL.
      3. Atualiza drive_channel_id, drive_resource_id e drive_channel_expiration_ms
         no banco.
      4. Reinicializa o StartPageToken para evitar reprocessar o histórico.

    Chamado pelo Beat diariamente às 06:00 BRT. Com CHANNEL_TTL de 7 dias,
    isso garante que o canal é renovado no dia 6 antes de expirar no dia 7.

    force=True renova independente da expiração — útil para rotacionar tokens.
    """
    import os
    import uuid
    from datetime import datetime, timezone, timedelta

    logger.info("task_renew_drive_webhook start force=%s attempt=%s", force, self.request.retries)
    try:
        from models.database import DriveSyncState, get_session
        from services.google_drive import drive_manager

        if not drive_manager.service:
            logger.error("task_renew_drive_webhook: Drive não autenticado — abortado")
            return {"status": "error", "reason": "drive_not_authenticated"}

        session = get_session()
        try:
            def _get(key: str):
                row = session.query(DriveSyncState).filter_by(key=key).first()
                return row.value if row else None

            def _set(key: str, val: str):
                row = session.query(DriveSyncState).filter_by(key=key).first()
                if row:
                    row.value = val
                    row.updated_at = datetime.utcnow()
                else:
                    session.add(DriveSyncState(key=key, value=val, updated_at=datetime.utcnow()))

            exp_ms_str = _get("drive_channel_expiration_ms")
            if not exp_ms_str:
                logger.warning("task_renew_drive_webhook: sem canal registrado — nada a renovar")
                return {"status": "skipped", "reason": "no_channel"}

            exp_ms  = int(exp_ms_str)
            exp_dt  = datetime.fromtimestamp(exp_ms / 1000, tz=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            remaining = exp_dt - now_utc

            if not force and remaining > timedelta(hours=48):
                logger.info(
                    "task_renew_drive_webhook: canal OK, expira em %.1fh — nada a fazer",
                    remaining.total_seconds() / 3600,
                )
                return {"status": "ok_no_action", "expires_in_hours": remaining.total_seconds() / 3600}

            # Renova canal
            public_url   = os.getenv("DRIVE_WEBHOOK_PUBLIC_URL", "").rstrip("/")
            webhook_token = os.getenv("DRIVE_WEBHOOK_TOKEN", "")
            if not public_url:
                logger.error("task_renew_drive_webhook: DRIVE_WEBHOOK_PUBLIC_URL não definida")
                return {"status": "error", "reason": "missing_env"}

            new_channel_id = f"samba-rag-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
            ttl_ms = 7 * 24 * 60 * 60 * 1000

            # Busca page token atual para o watch
            page_token = _get("changes_page_token") or \
                drive_manager.service.changes().getStartPageToken(supportsAllDrives=True).execute().get("startPageToken", "")

            watch_resp = drive_manager.service.changes().watch(
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                body={
                    "id":         new_channel_id,
                    "type":       "web_hook",
                    "address":    f"{public_url}/webhook/drive",
                    "token":      webhook_token,
                    "expiration": int((now_utc + timedelta(milliseconds=ttl_ms)).timestamp() * 1000),
                },
            ).execute()

            new_resource_id = watch_resp.get("resourceId", "")
            new_exp_ms      = int(watch_resp.get("expiration", 0))
            new_exp_dt      = datetime.fromtimestamp(new_exp_ms / 1000, tz=timezone.utc)

            _set("drive_channel_id",           new_channel_id)
            _set("drive_resource_id",           new_resource_id)
            _set("drive_channel_expiration_ms", str(new_exp_ms))
            _set("changes_page_token",          page_token)
            session.commit()

            logger.info(
                "task_renew_drive_webhook: canal renovado — id=%s exp=%s",
                new_channel_id, new_exp_dt.isoformat(),
            )
            return {
                "status":     "renewed",
                "channel_id": new_channel_id,
                "expires":    new_exp_dt.isoformat(),
            }
        finally:
            session.close()

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_renew_drive_webhook erro — agendando retry")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_process_followup_response",
    max_retries=2,
    acks_late=True,
)
def task_process_followup_response(self, followup_id: int) -> dict[str, Any]:
    """
    Processa uma resposta específica de follow-up:
      - Avança o deal para "Em Negociação" se aplicável
      - Envia notificação interna ao assignee

    Acionado pelo webhook imediatamente após detectar a resposta.
    """
    logger.info("task_process_followup_response followup_id=%s", followup_id)
    try:
        from models.database import get_session, Deal, FollowUp
        from services.internal_notify import get_notifier

        # Valores capturados antes de fechar a sessão (evita DetachedInstanceError).
        fu_deal_id = None
        fu_target  = "—"
        fu_content = ""
        deal_name_val      = "—"
        deal_commodity_val = "—"
        deal_assignee_val  = "—"

        session = get_session()
        try:
            fu = session.query(FollowUp).filter(FollowUp.id == followup_id).first()
            if fu is None:
                logger.warning("task_process_followup_response: followup_id=%s não encontrado", followup_id)
                return {"status": "not_found", "followup_id": followup_id}

            # Marcar como respondido
            fu.status = "respondido"

            deal = None
            if fu.deal_id:
                deal = session.query(Deal).filter(Deal.id == fu.deal_id).first()
                if deal and deal.stage == "Lead Capturado":
                    deal.stage = "Em Negociação"
                    deal.updated_at = __import__("datetime").datetime.utcnow()
                    logger.info(
                        "deal #%s '%s' avançou para Em Negociação via FollowUp#%s",
                        deal.id, deal.name, followup_id,
                    )

            # Captura antes do close
            fu_deal_id = fu.deal_id
            fu_target  = fu.target_person or fu.target_group or "—"
            fu_content = fu.response_content or ""
            if deal:
                deal_name_val      = deal.name      or "—"
                deal_commodity_val = deal.commodity or "—"
                deal_assignee_val  = deal.assignee  or "—"

            session.commit()
        finally:
            session.close()

        # Notificação interna
        notifier = get_notifier()
        notify_result = notifier.alert_followup_responded(
            followup_id=followup_id,
            deal_id=fu_deal_id,
            deal_name=deal_name_val,
            commodity=deal_commodity_val,
            assignee=deal_assignee_val,
            target_person=fu_target,
            response_content=fu_content,
        )
        logger.info("task_process_followup_response notify=%s", notify_result)
        return {"status": "ok", "followup_id": followup_id, "notify": notify_result}

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_process_followup_response erro followup_id=%s", followup_id)
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (4 ** self.request.retries))


# ─────────────────────────────────────────────────────────────────────────────
# SPRINT M — AGENTES ATIVOS / MULTIMODALIDADE / INTELIGÊNCIA GEOPOLÍTICA
# ─────────────────────────────────────────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_process_voice_meeting_minutes",
    max_retries=2,
    acks_late=True,
    time_limit=300,
    soft_time_limit=240,
)
def task_process_voice_meeting_minutes(
    self,
    message_id: int,
    media_url: str,
    mime_type: str = "audio/ogg",
    sender: str = "",
    group: str = "",
) -> dict:
    """
    Processa áudio de reunião recebido via WhatsApp:
      1. Baixa áudio do Twilio (media_url autenticado).
      2. Envia ao Gemini 1.5 Pro multimodal para transcrição + extração de ATA.
      3. Persiste action items em meeting_action_items.
      4. Envia ATA formatada ao grupo interno INTERNAL_WPP_GROUP.

    Args:
        message_id: ID da Message original no banco.
        media_url:  URL Twilio do áudio (ex.: https://api.twilio.com/2010-04-01/...Media/...).
        mime_type:  MIME type detectado pelo Twilio (ex.: audio/ogg, audio/mpeg).
        sender:     remetente normalizado (+55...).
        group:      nome do grupo WPP de origem.
    """
    logger.info(
        "task_process_voice_meeting_minutes msg_id=%s sender=%s group=%s mime=%s",
        message_id, sender, group, mime_type,
    )
    try:
        from services.voice_ata import (
            download_audio,
            format_ata_for_wpp,
            persist_action_items,
            process_audio_to_ata,
        )
        from services.internal_notify import get_notifier

        # 1. Download
        audio_data = download_audio(media_url)

        # 2. Gemini multimodal → ATA
        result = process_audio_to_ata(
            audio_data=audio_data,
            mime_type=mime_type,
            sender=sender,
            group=group,
        )

        # 3. Persiste action items
        ata_snippet = (result.get("ata_text") or "")[:500]
        item_ids = persist_action_items(
            action_items=result.get("action_items", []),
            message_id=message_id,
            source_group=group,
            ata_snippet=ata_snippet,
        )

        # 4. Envia ATA ao grupo interno
        wpp_msg = format_ata_for_wpp(result)
        notifier = get_notifier()
        try:
            notifier.send_internal_wpp(wpp_msg)
            notify_status = "sent"
        except Exception as notify_exc:
            logger.warning("task_process_voice_meeting_minutes: falha ao enviar WPP — %s", notify_exc)
            notify_status = "wpp_failed"

        logger.info(
            "task_process_voice_meeting_minutes: OK action_items=%d item_ids=%s notify=%s",
            len(result.get("action_items", [])), item_ids, notify_status,
        )
        return {
            "status":        "ok",
            "message_id":    message_id,
            "action_items":  len(result.get("action_items", [])),
            "item_ids":      item_ids,
            "notify":        notify_status,
        }

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_process_voice_meeting_minutes erro msg_id=%s", message_id)
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * (2 ** self.request.retries))


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_morning_pulse",
    max_retries=2,
    acks_late=True,
    time_limit=120,
    soft_time_limit=90,
)
def task_morning_pulse(self) -> dict:
    """
    Morning Pulse — 07:30 BRT via WhatsApp para a diretoria.

    Conteúdo:
      - Market Data (CBOT soja/milho, ICE açúcar, PTAX)
      - Pendências críticas e altas do dia (action items em aberto)
      - Alertas geopolíticos recentes (últimas 12h)
    """
    logger.info("task_morning_pulse: iniciando")
    try:
        from services.market_data import market_data
        from services.news_intelligence import (
            format_morning_pulse,
            run_geopolitical_scan,
        )
        from services.internal_notify import get_notifier
        from models.database import MeetingActionItem, get_session
        from sqlalchemy import text as _text

        # Mercado
        try:
            overview = market_data.get_market_overview()
        except Exception:
            overview = {}
        ptax = float((overview.get("USD/BRL") or {}).get("valor", 0.0)) or 0.0

        # Pendências críticas/altas
        critical_items: list[dict] = []
        sess = get_session()
        try:
            rows = (
                sess.query(MeetingActionItem)
                .filter(
                    MeetingActionItem.status == "pendente",
                    MeetingActionItem.priority.in_(["critica", "alta"]),
                )
                .order_by(MeetingActionItem.priority, MeetingActionItem.created_at)
                .limit(10)
                .all()
            )
            critical_items = [
                {
                    "responsible": r.responsible or "?",
                    "action":      r.action or "",
                    "priority":    r.priority or "media",
                }
                for r in rows
            ]
        finally:
            sess.close()

        # Alertas geopolíticos das últimas 12h
        try:
            geo_alerts = run_geopolitical_scan(hours_back=12)
        except Exception:
            geo_alerts = []

        # Monta e envia
        msg = format_morning_pulse(
            market_data=overview,
            ptax=ptax,
            critical_items=critical_items,
            geo_alerts=geo_alerts,
        )
        notifier = get_notifier()
        notifier.send_internal_wpp(msg)

        logger.info(
            "task_morning_pulse: enviado market_items=%d critical=%d geo=%d",
            len(overview), len(critical_items), len(geo_alerts),
        )
        return {
            "status":       "ok",
            "market_items": len(overview),
            "critical":     len(critical_items),
            "geo_alerts":   len(geo_alerts),
        }

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_morning_pulse erro")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE)


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_drive_status_report",
    max_retries=2,
    acks_late=True,
    time_limit=120,
    soft_time_limit=90,
)
def task_drive_status_report(self) -> dict:
    """
    Drive Status Report — 12:00 e 18:00 BRT.

    Conteúdo:
      - Novos chunks ingeridos desde o último relatório
      - Documentos novos/modificados na pasta COMERCIAL
      - Alerta se o canal Drive Watch estiver próximo do vencimento
    """
    logger.info("task_drive_status_report: iniciando")
    try:
        from models.database import CorporateKnowledge, DriveSyncState, get_session
        from services.internal_notify import get_notifier
        from datetime import datetime, timezone, timedelta

        sess = get_session()
        try:
            # Chunks nas últimas 6h
            cutoff = datetime.utcnow() - timedelta(hours=6)
            # CorporateKnowledge não tem created_at — usa aproximação via count
            total_chunks = sess.query(CorporateKnowledge).count()
            n_docs = sess.query(CorporateKnowledge.document_name).distinct().count()

            # Estado do canal
            def _get(k):
                row = sess.query(DriveSyncState).filter_by(key=k).first()
                return row.value if row else None

            exp_ms_str = _get("drive_channel_expiration_ms")
            channel_id = _get("drive_channel_id") or "—"
            days_left  = None
            exp_warning = ""
            if exp_ms_str and exp_ms_str.isdigit():
                exp_dt    = datetime.fromtimestamp(int(exp_ms_str) / 1000, tz=timezone.utc)
                days_left = (exp_dt - datetime.now(timezone.utc)).days
                if days_left <= 2:
                    exp_warning = f"\n⚠️ *Canal Drive expira em {days_left}d!* Renovar urgente."
                elif days_left <= 4:
                    exp_warning = f"\n🟡 Canal Drive expira em {days_left}d."
        finally:
            sess.close()

        hora = datetime.utcnow().strftime("%H:%M UTC")
        msg_lines = [
            f"📁 *SAMBA DRIVE STATUS — {hora}*",
            "",
            f"📚 Base de conhecimento: *{total_chunks} chunks* · {n_docs} documentos",
            f"🔗 Canal Watch: `{channel_id[:30]}...`",
        ]
        if days_left is not None:
            msg_lines.append(f"⏱️ Expira em: *{days_left} dias*")
        if exp_warning:
            msg_lines.append(exp_warning)
        msg_lines += [
            "",
            "_Para forçar sync completo, use o painel Base de Conhecimento._",
            "_Samba Drive Intelligence_",
        ]

        notifier = get_notifier()
        notifier.send_internal_wpp("\n".join(msg_lines))

        logger.info(
            "task_drive_status_report: OK chunks=%d docs=%d days_left=%s",
            total_chunks, n_docs, days_left,
        )
        return {"status": "ok", "chunks": total_chunks, "docs": n_docs, "days_left": days_left}

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_drive_status_report erro")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE)


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_geopolitical_monitor",
    max_retries=2,
    acks_late=True,
    time_limit=180,
    soft_time_limit=150,
)
def task_geopolitical_monitor(self) -> dict:
    """
    Geopolitical Sentinel — a cada 30 min (NewsData.io key rotation).

    Cruza notícias de alto impacto com as commodities do pipeline ativo.
    Janela de busca: 1 hora (hours_back=1) para evitar duplicatas entre runs.
    Se houver alertas críticos ou altos, dispara mensagem ao grupo da diretoria.

    Silencioso se não houver alertas relevantes (sem spam / alert fatigue).
    """
    logger.info("task_geopolitical_monitor: iniciando varredura")
    try:
        from services.news_intelligence import (
            format_strategic_alert,
            run_geopolitical_scan,
        )
        from services.internal_notify import get_notifier

        alerts = run_geopolitical_scan(hours_back=1)   # schedule 30min → janela 1h

        # ── Persiste resultado no cache JSON para o frontend Streamlit ────────
        # O painel lê este arquivo (sem overhead de API) a cada 5 min via cache.
        try:
            import json as _json
            from pathlib import Path as _Path
            from datetime import datetime as _dt
            _cache_dir  = _Path("data")
            _cache_dir.mkdir(exist_ok=True)
            _cache_file = _cache_dir / "geo_alerts_cache.json"
            _cache_file.write_text(
                _json.dumps(
                    {"ts": _dt.utcnow().isoformat(), "alerts": alerts},
                    ensure_ascii=False,
                    default=str,
                ),
                encoding="utf-8",
            )
            logger.debug("task_geopolitical_monitor: cache JSON atualizado (%d alerts)", len(alerts))
        except Exception as _cache_err:
            logger.warning("task_geopolitical_monitor: falha ao gravar cache JSON — %s", _cache_err)

        # Só notifica WPP se houver algo crítico ou alto
        actionable = [a for a in alerts if a["impact"] in ("critica", "alta")]
        if not actionable:
            logger.info("task_geopolitical_monitor: nenhum alerta crítico/alto — silencioso")
            return {"status": "ok", "alerts": 0, "notified": False}

        msg = format_strategic_alert(alerts)
        if msg:
            notifier = get_notifier()
            notifier.send_internal_wpp(msg)
            logger.info(
                "task_geopolitical_monitor: %d alertas enviados (%d críticos)",
                len(alerts),
                len([a for a in alerts if a["impact"] == "critica"]),
            )

        return {
            "status":   "ok",
            "alerts":   len(alerts),
            "criticas": len([a for a in alerts if a["impact"] == "critica"]),
            "altas":    len([a for a in alerts if a["impact"] == "alta"]),
            "notified": bool(msg),
        }

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_geopolitical_monitor erro")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE * 2)


# ─────────────────────────────────────────────────────────────────────────────
# TASK: @MENTION INTELLIGENCE ROUTER
# ─────────────────────────────────────────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_process_mention",
    max_retries=2,
    acks_late=True,
    time_limit=180,
    soft_time_limit=150,
)
def task_process_mention(
    self,
    message_id: int,
    sender: str,
    group: str,
    question: str,
) -> dict:
    """
    Processa @mention no WhatsApp via Cascade de Inteligência (5 níveis).

    Fluxo:
      L0 Intent → L1 DB-First → L2 RAG → L3 Gemini Flash → L4 Gemini Pro → L5 Fallback

    FACTS ONLY: jamais responde com informação não recuperada do sistema.
    Resposta enviada de volta ao mesmo grupo/pessoa via AgentRole.MANAGER.
    """
    logger.info(
        "task_process_mention: msg_id=%s sender=%s group=%s question='%s'",
        message_id, sender, group, question[:80],
    )
    try:
        from agents.whatsapp_intelligence_router import get_router
        from services.whatsapp_api import get_whatsapp_manager, AgentRole

        router = get_router()
        result = router.route(question=question, sender=sender, message_id=message_id)
        response_text = router.format_whatsapp(result, question=question)

        # Responde ao grupo (preferido) ou ao remetente direto
        target = group or sender
        wpp = get_whatsapp_manager()
        wpp.send(AgentRole.MANAGER, target, response_text)

        logger.info(
            "task_process_mention: respondido level=%d source=%s conf=%s target=%s",
            result.level, result.source, result.confidence, target,
        )
        return {
            "status":     "ok",
            "level":      result.level,
            "source":     result.source,
            "confidence": result.confidence,
            "intent":     result.intent,
            "msg_id":     message_id,
        }

    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_process_mention erro")
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE)


@celery_app.task(
    bind=True,
    name="tasks.agent_tasks.task_handle_new_samba_group",
    max_retries=2,
    acks_late=True,
)
def task_handle_new_samba_group(
    self,
    group_name: str,
    group_id: str,
    deal_data: dict | None = None,
) -> dict:
    """
    Disparado quando um novo grupo WhatsApp com padrão 'Samba x [Nome]' é detectado.

    Fluxo:
      1. Envia mensagem de boas-vindas bilíngue (PT/EN) NO GRUPO DO CLIENTE
      2. Envia alerta interno ao INTERNAL_WPP_GROUP com:
           - Nome do lead
           - Status NCNDA: PENDENTE + checklist de dados necessários
           - Qualificação comercial: campos preenchidos vs. pendentes

    Regra de ouro:
      - Boas-vindas → grupo do cliente
      - Alertas, checklists, status → grupo INTERNO
      - NUNCA expor operações internas ao cliente

    Chamado pelo: api/webhook.py ao detectar primeira mensagem de grupo novo.
    """
    logger.info(
        "task_handle_new_samba_group start group='%s' group_id='%s'",
        group_name, group_id,
    )
    try:
        from services.whatsapp_group_welcome import handle_new_group

        result = handle_new_group(
            group_name=group_name,
            group_id=group_id,
            deal_data=deal_data or {},
        )
        logger.info("task_handle_new_samba_group ok result=%s", result)
        return result
    except Retry:
        raise
    except Exception as exc:
        logger.exception("task_handle_new_samba_group erro group='%s'", group_name)
        raise self.retry(exc=exc, countdown=RETRY_BACKOFF_BASE)
